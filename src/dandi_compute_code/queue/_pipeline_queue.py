"""
PipelineQueue — typed container for ``jobs.tsv``.

``jobs.tsv`` is a tab-separated table where each row is one job
capsule. The asset paths of each capsule are kept apart in a sibling ``paths.tsv`` table,
one row per path, so that ``jobs.tsv`` stays narrow enough to render as a table.
:class:`PipelineQueue` is the container over both, a list of
:class:`~._job_capsule.JobCapsule` objects (the typed row model, defined in
:mod:`._job_capsule`) with convenience helpers for filtering and round-trip I/O.

The container itself is pipeline agnostic. Everything that differs between pipelines
is reached through the hooks at the bottom of the class, which a pipeline specific
subclass overrides. See :class:`~._aind_ephys_pipeline_queue.AindEphysPipelineQueue`.
"""

from __future__ import annotations

import collections
import csv
import datetime
import importlib.metadata
import io
import json
import logging
import os
import pathlib
import random
import shutil
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import ClassVar, Literal

import beartype

from ._capsule_resources import read_capsule_resources
from ._dispatch import DispatchResult, dispatch_pipeline_jobs
from ._dispatch_config import DispatchConfig
from ._fetch_qualifying_lfp_content_ids import _fetch_qualifying_lfp_content_ids
from ._globals import _CONFIGS_REGISTRIES, _PARAMS_REGISTRIES
from ._job_capsule import (
    _JOBS_TSV_FIELD_NAMES,
    _PATHS_TSV_FIELD_NAMES,
    JobCapsule,
    JobStatus,
    _path_field_name,
)
from ._queue_utils import (
    _CapsuleProvenanceCache,
    _collect_job_capsules,
    _duration_string_to_seconds,
    _extract_error_lines,
    _extract_nextflow_timeline_data,
    _finalize_job_capsule_records,
    _list_capsule_log_directories,
    _load_pipeline_config,
    _order_content_ids_for_uniform_dandiset_sampling,
    _remove_empty_parents,
    _sort_key,
    _UpstreamMetadataCache,
)
from ..aind_ephys_pipeline import UnmappedContentIDError
from ..dandiset import move_job_capsule, write_dandiset_file
from ..dandiset._globals import (
    _FAILED_RUNS_ARCHIVE_DANDISET_ID,
    _JOB_CAPSULES_DANDISET_ID,
)
from ..dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    _build_asset_metadata,
    load_assets_jsonld_metadata,
)
from ..lfp_pipeline import prepare_lfp_job

_log = logging.getLogger(__name__)

#: Dandiset whose assets back the pending/submission queries (the job capsules Dandiset).
_DANDISET_ID = _JOB_CAPSULES_DANDISET_ID

#: Default subpath (relative to a Dandiset root) that ``jobs.tsv`` is written to.
_JOBS_TSV_RELATIVE_PATH = "derivatives/jobs.tsv"

#: File name of the ``paths.tsv`` table, which always sits beside its ``jobs.tsv``.
_PATHS_TSV_FILE_NAME = "paths.tsv"


@beartype.beartype
@dataclass
class PipelineQueue:
    """
    Container for all entries in ``jobs.tsv``.

    Also the base class every pipeline specific queue derives from. The base class
    itself owns any pipeline no subclass claims, which is how the LFP pipeline is
    served, since it ships inside this package and needs no behaviour of its own.
    """

    #: Pipeline names, as they appear in the packaged pipeline configuration, that this
    #: class owns the creation behaviour of. Empty on the base class, which is the fallback.
    pipelines: ClassVar[tuple[str, ...]] = ()

    entries: list[JobCapsule]

    def __iter__(self) -> Iterator[JobCapsule]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    def with_status(self, status: JobStatus, /) -> list[JobCapsule]:
        """Entries whose recorded :attr:`~._job_capsule.JobCapsule.status` is *status*."""
        return [entry for entry in self.entries if entry.status == status]

    @property
    def pending(self) -> list[JobCapsule]:
        """Entries with code prepared but not yet submitted."""
        return self.with_status("pending")

    @property
    def stalled(self) -> list[JobCapsule]:
        """Entries submitted to the scheduler with no logs or output yet."""
        return self.with_status("stalled")

    @property
    def successful(self) -> list[JobCapsule]:
        """Entries whose output directory is present."""
        return self.with_status("successful")

    @property
    def failed(self) -> list[JobCapsule]:
        """Entries with logs but no output."""
        return self.with_status("failed")

    @property
    def successful_asset_bytes_total(self) -> int:
        """Total source-asset bytes across successful entries with a known size."""
        return sum(
            entry.asset_size_bytes
            for entry in self.entries
            if entry.status == "successful"
            and isinstance(entry.asset_size_bytes, int)
            and not isinstance(entry.asset_size_bytes, bool)
        )

    def entry_for(self, *, dandi_path: str, config: str | None = None) -> JobCapsule:
        """
        Return the entry with the given ``dandi_path`` (and ``config``).

        Parameters
        ----------
        dandi_path : str
            The ``dandi_path`` recorded on the target entry.
        config : str, optional
            Disambiguates scenarios that hold more than one job capsule for the
            same asset. Any config matches when omitted.

        Raises
        ------
        KeyError
            If no entry matches *dandi_path* and *config*.
        """
        for entry in self.entries:
            if entry.job.dandi_path == dandi_path and config in (None, entry.job.config):
                return entry
        message = f"No entry with dandi_path={dandi_path!r} and config={config!r}"
        raise KeyError(message)

    @classmethod
    def for_pipeline(cls, pipeline: str, /) -> type[PipelineQueue]:
        """
        The queue class owning the pipeline specific behaviour of *pipeline*.

        A subclass claims a pipeline by naming it in :attr:`pipelines`. Subclasses are
        discovered rather than registered, so a new pipeline queue only has to be imported
        by :mod:`dandi_compute_code.queue` to take effect.

        Parameters
        ----------
        pipeline : str
            The pipeline name as it appears in the packaged pipeline
            configuration.

        Returns
        -------
        type of PipelineQueue
            The claiming subclass, or :class:`PipelineQueue` when no subclass
            claims it.
        """
        for subclass in PipelineQueue.__subclasses__():
            if pipeline in subclass.pipelines:
                return subclass
        return PipelineQueue

    @staticmethod
    def pending_code_dirs() -> list[str]:
        """
        Identify job capsule ``code`` directories awaiting submission from DANDI assets metadata.

        Loads the DANDI ``assets.jsonld`` metadata and collects every job capsule
        directory that contains a ``code/submit.sh`` asset but no adjacent
        submitted-marker asset. An entry is considered submitted when a sibling
        ``submitted`` asset exists, or when a sibling asset whose name starts with
        ``submitted_date-`` exists.

        Returns
        -------
        list of str
            Sorted list of ``code`` directory paths (relative to the Dandiset
            root) that are pending submission. Empty when nothing is awaiting
            submission.
        """
        metadata = load_assets_jsonld_metadata()
        paths = set(metadata.path_to_asset_metadata.keys())

        pending_entries: list[str] = []
        for asset_path in sorted(paths):
            if asset_path.endswith("/code/submit.sh"):
                code_dir_path = asset_path[: -len("/submit.sh")]
                submitted_marker_prefix = f"{code_dir_path}/submitted_date-"
                has_submitted_marker = any(
                    path == f"{code_dir_path}/submitted" or path.startswith(submitted_marker_prefix) for path in paths
                )
                if not has_submitted_marker:
                    pending_entries.append(code_dir_path)

        return pending_entries

    @classmethod
    def has_pending_jobs(cls) -> bool:
        """
        Report whether any queued jobs are awaiting submission.

        Lightweight check intended to gate queue dispatch: it inspects the DANDI
        assets metadata for job capsule directories that contain a ``code/submit.sh``
        asset without an adjacent submitted marker. It does not submit anything
        and does not require SLURM access.
        """
        pending_entries = cls.pending_code_dirs()
        _log.info("Found %d pending queue entries", len(pending_entries))
        return len(pending_entries) > 0

    @staticmethod
    def load_pipeline_config() -> dict:
        """
        Read and validate the packaged pipeline configuration.

        Always reads the pipeline configuration packaged with this repo -- there is no local
        override.

        Raises
        ------
        FileNotFoundError
            If the packaged pipeline configuration file is missing.
        ValueError
            If the pipeline configuration fails LinkML validation.
        """
        return _load_pipeline_config()

    @staticmethod
    def resolve_params_key_to_id(*, pipeline: str, params_key: str) -> str:
        """
        Resolve a human-readable parameters key to its 7-character hash ID.

        The lookup is performed against the pipeline's registered params registry. For a
        pipeline without one, or if the key is not found, *params_key* is returned unchanged
        so callers that already store raw hash IDs continue to work.
        """
        entry = _PARAMS_REGISTRIES.get(pipeline, {}).get(params_key)
        params_id = entry["md5"][:7] if entry else params_key
        return params_id

    @classmethod
    def resolve_config_key_to_id(cls, *, pipeline: str, config_key: str) -> str:
        """
        Resolve a human-readable config key to its 7-character hash ID.

        Delegates to the queue class that owns *pipeline* (see :meth:`for_pipeline`).
        """
        queue_class = cls.for_pipeline(pipeline)
        config_id = queue_class._resolve_config_key_to_id(pipeline=pipeline, config_key=config_key)
        return config_id

    @classmethod
    def from_metadata(cls, metadata: AssetsJsonldMetadata, /) -> PipelineQueue:
        """
        Build a queue state from indexed DANDI assets metadata.

        Each entry represents one job capsule inferred from the
        ``derivatives/dandiset-*/.../pipeline-*/job-*`` path structure, with ``content_id`` /
        ``asset_size_bytes`` resolved from the upstream source Dandiset's ``assets.jsonld``.

        A job capsule directory name carries only the job ID, so the pipeline version,
        codebase version, parameters and config of each capsule are read back from the
        provenance block in its ``dataset_description.json``.

        Parameters
        ----------
        metadata : AssetsJsonldMetadata
            Indexed assets metadata, as produced by :meth:`from_jsonld` or
            :meth:`from_dandi`.
        """
        collection = _collect_job_capsules(metadata)
        upstream_cache = _UpstreamMetadataCache()
        provenance_cache = _CapsuleProvenanceCache(metadata)
        records = _finalize_job_capsule_records(
            collection=collection,
            upstream_cache=upstream_cache,
            provenance_cache=provenance_cache,
        )
        records.sort(key=_sort_key)
        return cls(entries=[JobCapsule.from_dict(record) for record in records])

    @classmethod
    def from_jsonld(cls, *, file_path: pathlib.Path) -> PipelineQueue:
        """
        Build a queue state from a local DANDI ``assets.jsonld`` file.

        The file should be a JSON file whose content is a list of asset dicts
        with ``path``, ``contentSize``, ``dateModified``, and ``contentUrl``
        fields (matching the ``assets.jsonld`` layout from DANDI).  The
        ``.jsonld`` file is preferred over its ``assets.yaml`` counterpart at
        the same S3 location because JSON parsing is many times faster than
        YAML for identical content.

        Parameters
        ----------
        file_path : pathlib.Path
            Path to a local assets JSON-LD file.

        Raises
        ------
        ValueError
            If the file content is not a JSON array.
        """
        raw = json.loads(file_path.read_text())
        if not isinstance(raw, list):
            raise ValueError(f"Expected a JSON array in {file_path}, got {type(raw).__name__}")
        content_id_to_asset: dict[str, dict] = {}
        path_to_asset_metadata: dict[str, AssetMetadata] = {}
        for asset in raw:
            if not isinstance(asset, dict):
                continue
            try:
                content_id, metadata = _build_asset_metadata(asset)
            except ValueError as exception:
                _log.debug("Skipping malformed asset in %s: %s", file_path, exception)
                continue
            content_id_to_asset[content_id] = asset
            path_to_asset_metadata[metadata.path] = metadata
        return cls.from_metadata(
            AssetsJsonldMetadata(
                content_id_to_asset=content_id_to_asset,
                path_to_asset_metadata=path_to_asset_metadata,
            )
        )

    @classmethod
    def from_dandi(cls, *, dandiset_id: str = _JOB_CAPSULES_DANDISET_ID) -> PipelineQueue:
        """
        Build a queue state from a Dandiset's remote ``assets.jsonld`` metadata.

        Fetches ``assets.jsonld`` for *dandiset_id* from the DANDI S3 bucket
        over the network.

        Parameters
        ----------
        dandiset_id : str, optional
            The Dandiset whose ``assets.jsonld`` is read. Defaults to the job
            capsules Dandiset (``001697``).
        """
        return cls.from_metadata(load_assets_jsonld_metadata(dandiset_id=dandiset_id))

    def to_tsv_string(self) -> str:
        """Serialise all entries to a tab-separated ``jobs.tsv`` table (including header)."""
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=_JOBS_TSV_FIELD_NAMES, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for entry in self.entries:
            writer.writerow(entry.to_tsv_row())
        return buffer.getvalue()

    def to_paths_tsv_string(self) -> str:
        """Serialise the asset paths of all entries to a tab-separated ``paths.tsv`` table (including header)."""
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=_PATHS_TSV_FIELD_NAMES, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for entry in self.entries:
            writer.writerows(entry.to_paths_tsv_rows())
        return buffer.getvalue()

    def to_tsv(self, file_path: pathlib.Path, /) -> None:
        """
        Write all entries to *file_path* as a tab-separated ``jobs.tsv`` table.

        The asset paths of the entries are written to a ``paths.tsv`` table beside it.

        Parameters
        ----------
        file_path : pathlib.Path
            Destination path. The file, and the ``paths.tsv`` beside it, are
            overwritten if they already exist.
        """
        file_path.write_text(self.to_tsv_string())
        file_path.with_name(_PATHS_TSV_FILE_NAME).write_text(self.to_paths_tsv_string())

    @classmethod
    def write_dandiset_jobs_table(
        cls,
        *,
        dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
        relative_path: str = _JOBS_TSV_RELATIVE_PATH,
        processing_directory: pathlib.Path | None = None,
        test: bool = False,
    ) -> None:
        """
        Write this Dandiset's queue state as a ``jobs.tsv`` table within itself.

        Builds the state from *dandiset_id*'s remote ``assets.jsonld`` metadata (see
        :meth:`from_dandi`) and uploads it as a tab-separated table to *relative_path* within
        *dandiset_id* (default ``derivatives/jobs.tsv``) via
        :func:`~dandi_compute_code.dandiset.write_dandiset_file`. The asset paths of the
        entries are uploaded the same way to a ``paths.tsv`` beside it.

        Intended to be called once for the job capsules ("source") Dandiset and once for the
        failed runs archive ("archived") Dandiset. There is no local queue directory or local
        state file involved -- the state is always rebuilt fresh from *dandiset_id*'s remote
        ``assets.jsonld`` and rewritten directly.

        Parameters
        ----------
        dandiset_id : str, optional
            The Dandiset whose ``assets.jsonld`` portrays the state, and which
            the table is written into. Defaults to the job capsules Dandiset.
        relative_path : str, optional
            Path (relative to the Dandiset root) the ``jobs.tsv`` table is
            written to. The ``paths.tsv`` table is written beside it.
        processing_directory : pathlib.Path, optional
            Directory for the temporary working tree used to upload the table.
            Defaults to the system temporary location.
        test : bool, optional
            When ``True``, leave the temporary working tree on disk after a
            successful upload for debugging.

        Raises
        ------
        RuntimeError
            If ``DANDI_API_KEY`` is unset or blank, or if the upload fails.
        """
        state = cls.from_dandi(dandiset_id=dandiset_id)
        paths_relative_path = str(pathlib.PurePosixPath(relative_path).with_name(_PATHS_TSV_FILE_NAME))
        for table_relative_path, content in (
            (relative_path, state.to_tsv_string()),
            (paths_relative_path, state.to_paths_tsv_string()),
        ):
            write_dandiset_file(
                dandiset_id=dandiset_id,
                relative_path=table_relative_path,
                content=content,
                processing_directory=processing_directory,
                test=test,
            )

    def aggregate_statistics(
        self,
        *,
        dandiset_directory: pathlib.Path,
        dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
        relative_path: str = "derivatives/queue_stats.json",
        processing_directory: pathlib.Path | None = None,
        test: bool = False,
    ) -> dict:
        """
        Write aggregate queue statistics JSON into a Dandiset and return the computed payload.

        Nextflow timeline reports are still located by walking *dandiset_directory* (a local
        Dandiset clone) -- that part is unchanged. The resulting statistics are written to
        *relative_path* within *dandiset_id* (default ``derivatives/queue_stats.json``) via
        :func:`~dandi_compute_code.dandiset.write_dandiset_file`, rather than written to local
        disk.

        Parameters
        ----------
        dandiset_directory : pathlib.Path
            Local clone of the dandiset used to locate Nextflow timeline
            reports.
        dandiset_id : str, optional
            The Dandiset the statistics JSON is written into.
        relative_path : str, optional
            Path (relative to the Dandiset root) the statistics JSON is written
            to.
        processing_directory : pathlib.Path, optional
            Directory for the temporary working tree used to upload the
            statistics JSON. Defaults to the system temporary location.
        test : bool, optional
            When ``True``, leave the temporary working tree on disk after a
            successful upload for debugging.

        Raises
        ------
        RuntimeError
            If ``DANDI_API_KEY`` is unset or blank, or if the upload fails.
        """
        job_step_wall_time_seconds: collections.defaultdict[str, float] = collections.defaultdict(float)
        timeline_files_processed = 0
        for entry in self.entries:
            capsule_dir = entry.resolve_capsule_dir(dandiset_directory)
            timeline_file = capsule_dir / "logs" / "timeline.html"
            if not timeline_file.is_file():
                continue

            timeline_data = _extract_nextflow_timeline_data(timeline_html=timeline_file.read_text())
            if timeline_data is None:
                continue

            processes = timeline_data.get("processes")
            if not isinstance(processes, list):
                continue
            timeline_files_processed += 1

            for process in processes:
                if not isinstance(process, dict):
                    continue
                process_label = process.get("label")
                if not isinstance(process_label, str):
                    continue
                step_name = process_label.split(" (", 1)[0]
                times = process.get("times")
                if not isinstance(times, list):
                    continue
                for step in times:
                    if not isinstance(step, dict):
                        continue
                    duration_label = step.get("label")
                    if not isinstance(duration_label, str):
                        continue
                    duration_string = duration_label.split("/", 1)[0].strip()
                    duration_seconds = _duration_string_to_seconds(duration_string)
                    if duration_seconds > 0:
                        job_step_wall_time_seconds[step_name] += duration_seconds

        statistics = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "state_entry_count": len(self.entries),
            "successful_asset_bytes_total": self.successful_asset_bytes_total,
            "timeline_files_processed": timeline_files_processed,
            "job_step_wall_time_seconds": {
                key: value for key, value in sorted(job_step_wall_time_seconds.items(), key=lambda item: item[0])
            },
        }

        write_dandiset_file(
            dandiset_id=dandiset_id,
            relative_path=relative_path,
            content=json.dumps(statistics, indent=2, sort_keys=True) + "\n",
            processing_directory=processing_directory,
            test=test,
        )
        return statistics

    def clean_unsubmitted_capsules(self, *, dandiset_directory: pathlib.Path) -> list[pathlib.Path]:
        """
        Remove all queued (unsubmitted) capsule directories from the dandiset tree.

        A capsule is *queued* when its directory has a ``code/`` subdirectory but no
        ``logs/`` or ``derivatives/`` content and no submitted marker. Each matching
        job capsule directory is deleted from the DANDI archive (via ``dandi delete``)
        and the local filesystem.

        Parameters
        ----------
        dandiset_directory : pathlib.Path
            Local clone of the dandiset used to resolve and delete matching job
            capsule directories.

        Returns
        -------
        list of pathlib.Path
            Job capsule directory paths that were deleted.

        Raises
        ------
        RuntimeError
            If ``DANDI_API_KEY`` is not set or is blank.
        """
        if not os.environ.get("DANDI_API_KEY", "").strip():
            message = "`DANDI_API_KEY` environment variable is not set or is blank."
            raise RuntimeError(message)

        cleanable_capsule_dirs = [
            capsule_dir
            for entry in self.entries
            if (capsule_dir := entry.resolve_unsubmitted_capsule_dir(dandiset_directory)) is not None
        ]

        removed: list[pathlib.Path] = []
        for capsule_dir in cleanable_capsule_dirs:
            if capsule_dir.is_dir():
                parent_dir = capsule_dir.parent
                subprocess.run(
                    ["dandi", "delete", str(capsule_dir)],
                    input=b"y\n",
                    check=True,
                )
                shutil.rmtree(capsule_dir)
                _remove_empty_parents(start=parent_dir, stop=dandiset_directory / "derivatives")
                removed.append(capsule_dir)

        return removed

    def archive_by_status(
        self,
        *,
        status: Literal["failed", "pending", "stalled"],
        dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
        archive_dandiset_id: str = _FAILED_RUNS_ARCHIVE_DANDISET_ID,
        processing_directory: pathlib.Path | None = None,
        test: bool = False,
    ) -> list[str]:
        """
        Move every entry with the given *status* into the failed runs archive.

        *status* is the recorded :attr:`~._job_capsule.JobCapsule.status` selecting the
        entries to archive: ``"failed"`` (logs present, no output), ``"pending"`` (code
        prepared but never submitted), or ``"stalled"`` (submitted to the scheduler but
        no logs or output ever appeared). For each matching entry, resolves its capsule path
        against *dandiset_id*'s remote ``assets.jsonld`` (see
        :meth:`JobCapsule.resolve_capsule_path`) and moves the
        corresponding capsule from *dandiset_id* to *archive_dandiset_id* via
        :func:`~dandi_compute_code.dandiset.move_job_capsule`. Both Dandisets are
        addressed purely by ID -- everything is resolved and moved ephemerally over
        the network, with no local Dandiset clone required.

        Parameters
        ----------
        status : {"failed", "pending", "stalled"}
            Which subset of entries to archive.
        dandiset_id : str, optional
            Dandiset entries are archived *from*. Defaults to the job capsules
            Dandiset.
        archive_dandiset_id : str, optional
            Dandiset entries are archived *to*. Defaults to the failed runs
            archive Dandiset.
        processing_directory : pathlib.Path, optional
            Directory for the temporary working tree used by each move. Defaults
            to the system temporary location.
        test : bool, optional
            When ``True``, leave each temporary working tree on disk after a
            successful move for debugging.

        Returns
        -------
        list of str
            Capsule paths (relative to the Dandiset root) that were archived, in
            the order they were processed.

        Raises
        ------
        RuntimeError
            If ``DANDI_API_KEY`` is unset or blank, or if archiving any
            individual capsule fails (see :func:`move_job_capsule`). A failure
            leaves entries processed so far archived and stops before the rest.
        """
        if not os.environ.get("DANDI_API_KEY", "").strip():
            message = "`DANDI_API_KEY` environment variable is not set or is blank."
            raise RuntimeError(message)

        entries = self.with_status(status)
        archived: list[str] = []
        if not entries:
            return archived

        asset_paths = set(load_assets_jsonld_metadata(dandiset_id=dandiset_id).path_to_asset_metadata)
        for entry in entries:
            capsule_path = entry.resolve_capsule_path(asset_paths)
            move_job_capsule(
                capsule_path=capsule_path,
                source_dandiset_id=dandiset_id,
                target_dandiset_id=archive_dandiset_id,
                processing_directory=processing_directory,
                test=test,
            )
            archived.append(capsule_path)

        return archived

    @classmethod
    def process_queue(
        cls,
        *,
        processing_directory: pathlib.Path,
        only_pipeline: str | None = None,
        max_concurrent: int | None = None,
        jitter_seconds: float = 30.0,
        dandiset_id: str = _DANDISET_ID,
        test: bool = False,
    ) -> dict[str, DispatchResult]:
        """
        Hand every pending job capsule to its pipeline's SLURM array dispatcher.

        Each configured pipeline is run on the cluster by exactly one array job, which is the
        only thing that triggers actual capsule runs. How many of its tasks run at once is the
        pipeline's ``dispatch.max_concurrent`` setting in the packaged pipeline configuration,
        applied by SLURM itself as the array's throttle. A pipeline whose dispatcher is still
        working through its array is skipped, so repeated invocations from a crontab never
        stack a second array on top of a live one.

        The queue state is always fetched fresh. Pending capsules are read live from the DANDI
        assets metadata (see :meth:`pending_code_dirs`), so there is no local queue directory.

        Parameters
        ----------
        processing_directory : pathlib.Path
            Directory the per-pipeline dispatch directories are created in. Each
            holds a manifest, a dispatch script and the array's logs, so it has
            to stay readable from the compute nodes for as long as the array
            lives.
        only_pipeline : str, optional
            Dispatch only this pipeline instead of every configured one.
        max_concurrent : int, optional
            Overrides every dispatched pipeline's configured concurrency limit.
        jitter_seconds : float, optional
            Maximum random delay (seconds) before processing. ``0`` disables it.
            Spreads concurrent invocations out so they do not read the cluster
            state at once.
        dandiset_id : str, optional
            The Dandiset capsules are downloaded from and uploaded back to.
        test : bool, optional
            If ``True``, array tasks leave their working trees on disk for
            debugging.

        Returns
        -------
        dict of str to DispatchResult
            The dispatch outcome per pipeline, keyed by pipeline name.

        Raises
        ------
        ValueError
            If *jitter_seconds* is negative, if *max_concurrent* is less than 1,
            or if *only_pipeline* is not configured.
        """
        if jitter_seconds < 0:
            message = "jitter_seconds must be non-negative"
            raise ValueError(message)
        if max_concurrent is not None and max_concurrent < 1:
            message = "max_concurrent must be at least 1"
            raise ValueError(message)

        pipeline_config = _load_pipeline_config()
        pipelines = pipeline_config.get("pipelines", {})
        if only_pipeline is not None and only_pipeline not in pipelines:
            configured = list(pipelines.keys())
            message = f"Pipeline '{only_pipeline}' is not configured. Configured pipelines are: {configured}."
            raise ValueError(message)

        if jitter_seconds > 0:
            delay = random.uniform(0, jitter_seconds)
            _log.info("Sleeping %.2f seconds (jitter) before processing queue", delay)
            time.sleep(delay)

        code_dir_paths = cls.pending_code_dirs()
        _log.info("Found %d pending queue entries", len(code_dir_paths))
        capsule_resources = read_capsule_resources(code_dir_paths)

        results: dict[str, DispatchResult] = {}
        for pipeline_name in pipelines:
            if only_pipeline is not None and pipeline_name != only_pipeline:
                continue
            dispatch_config = DispatchConfig.from_pipeline_config(
                pipeline=pipeline_name,
                pipeline_config=pipeline_config,
                max_concurrent=max_concurrent,
            )
            results[pipeline_name] = dispatch_pipeline_jobs(
                pipeline=pipeline_name,
                code_dir_paths=code_dir_paths,
                processing_directory=processing_directory,
                dispatch_config=dispatch_config,
                dandiset_id=dandiset_id,
                capsule_resources=capsule_resources,
                test=test,
            )
        return results

    def existing_capsule_keys(self) -> set[tuple[str, str, str, str]]:
        """
        Keys of the job capsules that already exist, as
        ``(pipeline, params, config, content_id)`` tuples.

        Used to skip forming a capsule for an asset that already has one, whatever point of
        the lifecycle it has reached. The pipeline and codebase versions are deliberately
        left out: a capsule already covers its asset for a parameters and config combination
        whichever version formed it.
        """
        return {
            (entry.job.pipeline, entry.job.params, entry.job.config, entry.content_id)
            for entry in self.entries
            if entry.content_id
        }

    @classmethod
    def resolve_latest_pipeline_version(cls, *, pipeline: str, pipeline_directory: pathlib.Path | None = None) -> str:
        """
        The latest version of *pipeline* available on this machine.

        New job capsules always run the latest locally available pipeline version, so there is
        no version priority list to maintain. What "locally available" means depends on the
        pipeline, so the answer comes from the queue class that owns *pipeline* (see
        :meth:`for_pipeline`).

        Parameters
        ----------
        pipeline : str
            The pipeline name as it appears in the packaged pipeline
            configuration.
        pipeline_directory : pathlib.Path, optional
            Local checkout of the pipeline repository, for pipelines that live
            in one.

        Returns
        -------
        str
            The version string to form new job capsules against.
        """
        queue_class = cls.for_pipeline(pipeline)
        latest_version = queue_class._resolve_latest_pipeline_version(pipeline_directory=pipeline_directory)
        return latest_version

    @classmethod
    def _qualifying_content_ids(cls, pipeline: str, /) -> list[str]:
        """Content IDs qualifying for *pipeline*, ordered so a limit samples Dandisets uniformly."""
        queue_class = cls.for_pipeline(pipeline)
        ordered_content_ids = _order_content_ids_for_uniform_dandiset_sampling(
            content_ids=queue_class._fetch_qualifying_content_ids()
        )
        return ordered_content_ids

    @classmethod
    def create_job_capsules(
        cls,
        *,
        only_pipeline: str | None = None,
        config_key: str = "default",
        content_ids: list[str] | None = None,
        limit: int | None = None,
        force_latest_versions: bool = False,
        pipeline_directory: pathlib.Path | None = None,
    ) -> int:
        """
        Form new job capsules for qualifying assets that do not have one yet.

        The rule is deliberately narrow. Every pipeline and parameters combination declared in
        the packaged pipeline configuration (see :meth:`load_pipeline_config`) is crossed with the
        qualifying content IDs, and an asset that already has a capsule for that combination is
        skipped. What already exists is read from the live queue state (see :meth:`from_dandi`),
        which is why creation lives on this class. New capsules are formed against the latest
        versions available locally.

        ``force_latest_versions`` breaks that rule on purpose. It forms a fresh capsule against
        the latest versions for every qualifying asset, whether or not one already exists, which
        is how a new pipeline release gets rolled out over assets that have already been
        processed.

        Parameters
        ----------
        only_pipeline : str, optional
            Form capsules only for this pipeline instead of every pipeline in
            the configuration. Raises if the name is not configured.
        config_key : str, optional
            Key for a registered job configuration.
        content_ids : list of str, optional
            Explicit content IDs to form capsules for. The qualifying list is
            not fetched from the network when these are provided.
        limit : int, optional
            Form at most this many capsules in total. Unlimited when ``None``.
        force_latest_versions : bool, optional
            Form a capsule for every qualifying asset against the latest
            pipeline and codebase versions, whether or not one already exists.
        pipeline_directory : pathlib.Path, optional
            Local checkout of the AIND pipeline repository.

        Returns
        -------
        int
            The number of job capsules that were formed.
        """
        pipeline_config = _load_pipeline_config()
        pipelines = pipeline_config.get("pipelines", {})
        if only_pipeline is not None and only_pipeline not in pipelines:
            configured = list(pipelines.keys())
            message = f"Pipeline '{only_pipeline}' is not configured. Configured pipelines are: {configured}."
            raise ValueError(message)

        existing_capsule_keys = set() if force_latest_versions else cls.from_dandi().existing_capsule_keys()

        created_count = 0
        for pipeline_name, pipeline_data in pipelines.items():
            if only_pipeline is not None and pipeline_name != only_pipeline:
                continue
            if limit is not None and created_count >= limit:
                break

            version = cls.resolve_latest_pipeline_version(pipeline=pipeline_name, pipeline_directory=pipeline_directory)
            config_id = cls.resolve_config_key_to_id(pipeline=pipeline_name, config_key=config_key)
            pipeline_content_ids = (
                content_ids if content_ids is not None else cls._qualifying_content_ids(pipeline_name)
            )

            for params_key in pipeline_data.get("params", []):
                params_id = cls.resolve_params_key_to_id(pipeline=pipeline_name, params_key=params_key)

                for content_id in pipeline_content_ids:
                    if limit is not None and created_count >= limit:
                        _log.info(f"Reached the creation limit of {limit} job capsules.")
                        return created_count

                    label = f"{pipeline_name}/{version}/{params_key}/{content_id}"
                    if (pipeline_name, params_id, config_id, content_id) in existing_capsule_keys:
                        _log.info(f"Skipping {label}: a job capsule already exists.")
                        continue

                    _log.info(f"Creating a job capsule for {label}.")
                    try:
                        script_file_path = cls.for_pipeline(pipeline_name)._prepare_job(
                            content_id=content_id,
                            parameters_key=params_key,
                            pipeline_version=version,
                            pipeline_directory=pipeline_directory,
                            config_key=config_key,
                            force_new_capsule=force_latest_versions,
                        )
                    except UnmappedContentIDError as error:
                        _log.warning(f"Skipping {label}: {error}")
                        continue
                    if script_file_path is None:
                        _log.info(f"Skipped {label}: a job capsule already exists.")
                        continue
                    created_count += 1

        return created_count

    @staticmethod
    def dump_issues(
        *,
        dandiset_directory: pathlib.Path,
        dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
        relative_path: str = "derivatives/issues_dump.json",
        processing_directory: pathlib.Path | None = None,
        test: bool = False,
    ) -> list[dict]:
        """
        Scan nextflow/slurm logs and write per-capsule error lines into a Dandiset.

        Logs are still located by walking *dandiset_directory* (a local Dandiset clone) --
        that part is unchanged. The resulting records are written to *relative_path* within
        *dandiset_id* (default ``derivatives/issues_dump.json``) via
        :func:`~dandi_compute_code.dandiset.write_dandiset_file`, rather than written to local
        disk.
        """
        records: list[dict] = []
        for logs_dir in _list_capsule_log_directories(dandiset_directory=dandiset_directory):
            nextflow_log = logs_dir / "nextflow.log"
            slurm_logs = sorted(path for path in logs_dir.glob("*slurm.log") if path.is_file())

            nextflow_errors = _extract_error_lines(log_file=nextflow_log)
            slurm_errors = {log_file.name: _extract_error_lines(log_file=log_file) for log_file in slurm_logs}
            slurm_errors = {key: value for key, value in slurm_errors.items() if value}
            if not nextflow_errors and not slurm_errors:
                continue

            records.append(
                {
                    "capsule_path": logs_dir.parent.relative_to(dandiset_directory).as_posix(),
                    "nextflow_log": (
                        nextflow_log.relative_to(dandiset_directory).as_posix() if nextflow_log.is_file() else None
                    ),
                    "nextflow_errors": nextflow_errors,
                    "slurm_errors": {
                        log_name: errors for log_name, errors in sorted(slurm_errors.items(), key=lambda item: item[0])
                    },
                }
            )

        payload = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "capsule_count": len(records),
            "records": records,
        }
        write_dandiset_file(
            dandiset_id=dandiset_id,
            relative_path=relative_path,
            content=json.dumps(payload, indent=2, sort_keys=True) + "\n",
            processing_directory=processing_directory,
            test=test,
        )
        return records

    @staticmethod
    def summarize_issues(
        *,
        dandiset_directory: pathlib.Path,
        dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
        dump_relative_path: str = "derivatives/issues_dump.json",
        relative_path: str = "derivatives/issues_summary.json",
        processing_directory: pathlib.Path | None = None,
        test: bool = False,
    ) -> dict[str, list[str]]:
        """
        Write a descending error-frequency summary (keys are counts, values are error strings).

        Calls :meth:`dump_issues` (writing its own dump to *dump_relative_path* within
        *dandiset_id*), then writes the summary to *relative_path* within *dandiset_id*
        (default ``derivatives/issues_summary.json``) via
        :func:`~dandi_compute_code.dandiset.write_dandiset_file`.
        """
        records = PipelineQueue.dump_issues(
            dandiset_directory=dandiset_directory,
            dandiset_id=dandiset_id,
            relative_path=dump_relative_path,
            processing_directory=processing_directory,
            test=test,
        )

        counts: collections.Counter[str] = collections.Counter()
        for record in records:
            counts.update(record.get("nextflow_errors", []))
            for errors in record.get("slurm_errors", {}).values():
                counts.update(errors)

        errors_by_count: dict[str, list[str]] = collections.defaultdict(list)
        for message, count in sorted(counts.items(), key=lambda message_count: (-message_count[1], message_count[0])):
            errors_by_count[str(count)].append(message)

        summary = {
            count: messages
            for count, messages in sorted(
                errors_by_count.items(),
                key=lambda count_messages: int(count_messages[0]),
                reverse=True,
            )
        }
        output_payload = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "summary": summary,
        }
        write_dandiset_file(
            dandiset_id=dandiset_id,
            relative_path=relative_path,
            content=json.dumps(output_payload, indent=2, sort_keys=True) + "\n",
            processing_directory=processing_directory,
            test=test,
        )
        return summary

    @classmethod
    def from_tsv(cls, file_path: pathlib.Path, /) -> PipelineQueue:
        """
        Load from an existing ``jobs.tsv`` file.

        The inverse of :meth:`to_tsv`/:meth:`to_tsv_string`: parses the tab-separated
        table (via :class:`csv.DictReader`, using :data:`_JOBS_TSV_FIELD_NAMES` as
        the expected column order) back into :class:`JobCapsule` objects via
        :meth:`JobCapsule.from_tsv_row`.

        The asset paths are read from the ``paths.tsv`` beside it, and attached to each
        entry by ``job_id``. Which mapping a path belongs in is read from where it sits
        beneath the capsule directory. Entries are left without paths when that table is absent.

        Parameters
        ----------
        file_path : pathlib.Path
            Path to the ``jobs.tsv`` file to read.

        Raises
        ------
        FileNotFoundError
            If *file_path* does not exist.
        """
        if not file_path.exists():
            message = f"State file not found: {file_path}"
            raise FileNotFoundError(message)
        with file_path.open(newline="") as file_stream:
            reader = csv.DictReader(file_stream, delimiter="\t")
            entries = [JobCapsule.from_tsv_row(row) for row in reader]

        paths_file_path = file_path.with_name(_PATHS_TSV_FILE_NAME)
        if paths_file_path.exists():
            job_id_to_entry = {entry.job.job_id: entry for entry in entries}
            with paths_file_path.open(newline="") as file_stream:
                for row in csv.DictReader(file_stream, delimiter="\t"):
                    entry = job_id_to_entry.get(row["job_id"])
                    field_name = _path_field_name(job_id=row["job_id"], path=row["path"])
                    if entry is None or field_name is None:
                        _log.debug("Skipping unmatched row in %s: %s", paths_file_path, row)
                        continue
                    getattr(entry, field_name)[row["path"]] = row["content_id"]
        return cls(entries=entries)

    @classmethod
    def _resolve_latest_pipeline_version(cls, *, pipeline_directory: pathlib.Path | None = None) -> str:
        """
        The latest version of a pipeline that ships inside this package, which is this
        package's own version. *pipeline_directory* is unused for such a pipeline.
        """
        latest_version = f"v{importlib.metadata.version('dandi-compute-code')}"
        return latest_version

    @classmethod
    def _resolve_config_key_to_id(cls, *, pipeline: str, config_key: str) -> str:
        """
        Resolve *config_key* against the registered configs of *pipeline*.

        The LFP pipeline has no config of its own, so it resolves to the empty string, which
        is what its job capsules record. A pipeline with no registered configs, or a key that
        is already a raw hash ID, resolves to *config_key* unchanged.
        """
        if pipeline == "lfp":
            return ""
        entry = _CONFIGS_REGISTRIES.get(pipeline, {}).get(config_key)
        config_id = entry["md5"][:7] if entry else config_key
        return config_id

    @classmethod
    def _fetch_qualifying_content_ids(cls) -> list[str]:
        """Content IDs qualifying for this queue's pipelines, in no particular order."""
        content_ids = _fetch_qualifying_lfp_content_ids()
        return content_ids

    @classmethod
    def _prepare_job(
        cls,
        *,
        content_id: str,
        parameters_key: str,
        pipeline_version: str,
        pipeline_directory: pathlib.Path | None,
        config_key: str,
        force_new_capsule: bool,
    ) -> pathlib.Path | None:
        """
        Form one job capsule and return its submission script, or ``None`` when a capsule
        already exists on the archive. *pipeline_directory* and *config_key* are unused by
        pipelines that ship inside this package.
        """
        script_file_path = prepare_lfp_job(
            content_id=content_id,
            parameters_key=parameters_key,
            pipeline_version=pipeline_version,
            force_new_capsule=force_new_capsule,
            silent=True,
        )
        return script_file_path
