"""
QueueState — typed container for ``state.tsv``.

``state.tsv`` is a tab-separated table where each row is one job
capsule. :class:`QueueState` is the container over it — a list of
:class:`~._job_capsule.JobCapsule` objects (the typed row model, defined in
:mod:`._job_capsule`) with convenience helpers for filtering and round-trip I/O.
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
from typing import Literal

from ._capsule_resources import read_capsule_resources
from ._dispatch import DispatchResult, dispatch_pipeline_jobs
from ._dispatch_config import DispatchConfig
from ._fetch_qualifying_aind_content_ids import _fetch_qualifying_aind_content_ids
from ._fetch_qualifying_lfp_content_ids import _fetch_qualifying_lfp_content_ids
from ._globals import _CONFIGS_REGISTRIES, _DEFAULT_AIND_PIPELINE_DIRECTORY, _PARAMS_REGISTRIES
from ._job_capsule import _STATE_TSV_FIELD_NAMES, JobCapsule
from ._queue_utils import (
    _CapsuleProvenanceCache,
    _collect_job_capsules,
    _duration_string_to_seconds,
    _extract_error_lines,
    _extract_nextflow_timeline_data,
    _finalize_job_capsule_records,
    _latest_repository_version_tag,
    _list_capsule_log_directories,
    _load_pipeline_config,
    _order_content_ids_for_uniform_dandiset_sampling,
    _remove_empty_parents,
    _sort_key,
    _UpstreamMetadataCache,
)
from ..aind_ephys_pipeline import UnmappedContentIDError, prepare_aind_ephys_job
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

#: Default subpath (relative to a Dandiset root) that ``state.tsv`` is written to.
_STATE_TSV_RELATIVE_PATH = "derivatives/state.tsv"


@dataclass
class QueueState:
    """Container for all entries in ``state.tsv``."""

    entries: list[JobCapsule]

    def __iter__(self) -> Iterator[JobCapsule]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    @property
    def pending(self) -> list[JobCapsule]:
        """Entries with code prepared but not yet submitted."""
        return [e for e in self.entries if e.is_pending]

    @property
    def stalled(self) -> list[JobCapsule]:
        """Entries submitted to the scheduler with no logs or output yet."""
        return [e for e in self.entries if e.is_stalled]

    @property
    def running(self) -> list[JobCapsule]:
        """Entries with logs present but no output — likely still executing."""
        return [e for e in self.entries if e.is_running]

    @property
    def successful(self) -> list[JobCapsule]:
        """Entries whose output directory is present."""
        return [e for e in self.entries if e.is_successful]

    @property
    def failed(self) -> list[JobCapsule]:
        """Entries with code and logs but no output."""
        return [e for e in self.entries if e.is_failed]

    @property
    def successful_asset_bytes_total(self) -> int:
        """Total source-asset bytes across successful entries with a known size."""
        return sum(
            entry.asset_size_bytes
            for entry in self.entries
            if entry.is_successful
            and isinstance(entry.asset_size_bytes, int)
            and not isinstance(entry.asset_size_bytes, bool)
        )

    def entry_for(self, *, dandi_path: str, config: str | None = None) -> JobCapsule:
        """
        Return the entry with the given ``dandi_path`` (and ``config``).

        :param dandi_path: The ``dandi_path`` recorded on the target entry.
        :param config: Disambiguates scenarios that hold more than one job capsule for
            the same asset. Any config matches when omitted.
        :raises KeyError: If no entry matches *dandi_path* and *config*.
        """
        for entry in self.entries:
            if entry.job.dandi_path == dandi_path and config in (None, entry.job.config):
                return entry
        message = f"No entry with dandi_path={dandi_path!r} and config={config!r}"
        raise KeyError(message)

    @staticmethod
    def pending_code_dirs() -> list[str]:
        """
        Identify job capsule ``code`` directories awaiting submission from DANDI assets metadata.

        Loads the DANDI ``assets.jsonld`` metadata and collects every job capsule
        directory that contains a ``code/submit.sh`` asset but no adjacent
        submitted-marker asset. An entry is considered submitted when a sibling
        ``submitted`` asset exists, or when a sibling asset whose name starts with
        ``submitted_date-`` exists.

        :returns: Sorted list of ``code`` directory paths (relative to the
            Dandiset root) that are pending submission. Empty when nothing is
            awaiting submission.
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

        :raises FileNotFoundError: If the packaged pipeline configuration file is missing.
        :raises ValueError: If the pipeline configuration fails LinkML validation.
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

    @staticmethod
    def resolve_config_key_to_id(*, pipeline: str, config_key: str) -> str:
        """
        Resolve a human-readable config key to its 7-character hash ID.

        The LFP pipeline has no config of its own, so it resolves to the empty string, which
        is what its job capsules record. Otherwise the lookup mirrors
        :meth:`resolve_params_key_to_id`.
        """
        if pipeline == "lfp":
            return ""
        entry = _CONFIGS_REGISTRIES.get(pipeline, {}).get(config_key)
        config_id = entry["md5"][:7] if entry else config_key
        return config_id

    @classmethod
    def from_metadata(cls, metadata: AssetsJsonldMetadata, /) -> QueueState:
        """
        Build a queue state from indexed DANDI assets metadata.

        Each entry represents one job capsule inferred from the
        ``derivatives/dandiset-*/.../pipeline-*/job-*`` path structure, with ``content_id`` /
        ``asset_size_bytes`` resolved from the upstream source Dandiset's ``assets.jsonld``.

        A job capsule directory name carries only the job ID, so the pipeline version,
        codebase version, parameters and config of each capsule are read back from the
        provenance block in its ``dataset_description.json``.

        :param metadata: Indexed assets metadata, as produced by
            :meth:`from_jsonld` or :meth:`from_dandi`.
        :type metadata: AssetsJsonldMetadata
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
    def from_jsonld(cls, *, file_path: pathlib.Path) -> QueueState:
        """
        Build a queue state from a local DANDI ``assets.jsonld`` file.

        The file should be a JSON file whose content is a list of asset dicts
        with ``path``, ``contentSize``, ``dateModified``, and ``contentUrl``
        fields (matching the ``assets.jsonld`` layout from DANDI).  The
        ``.jsonld`` file is preferred over its ``assets.yaml`` counterpart at
        the same S3 location because JSON parsing is many times faster than
        YAML for identical content.

        :param file_path: Path to a local assets JSON-LD file.
        :type file_path: pathlib.Path
        :raises ValueError: If the file content is not a JSON array.
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
    def from_dandi(cls, *, dandiset_id: str = _JOB_CAPSULES_DANDISET_ID) -> QueueState:
        """
        Build a queue state from a Dandiset's remote ``assets.jsonld`` metadata.

        Fetches ``assets.jsonld`` for *dandiset_id* from the DANDI S3 bucket
        over the network.

        :param dandiset_id: The Dandiset whose ``assets.jsonld`` is read.
            Defaults to the job capsules Dandiset (``001697``).
        :type dandiset_id: str
        """
        return cls.from_metadata(load_assets_jsonld_metadata(dandiset_id=dandiset_id))

    def to_tsv_string(self) -> str:
        """Serialise all entries to a tab-separated ``state.tsv`` table (including header)."""
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=_STATE_TSV_FIELD_NAMES, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for entry in self.entries:
            writer.writerow(entry.to_tsv_row())
        return buffer.getvalue()

    def to_tsv(self, file_path: pathlib.Path, /) -> None:
        """
        Write all entries to *file_path* as a tab-separated ``state.tsv`` table.

        :param file_path: Destination path; the file is overwritten if it already exists.
        :type file_path: pathlib.Path
        """
        file_path.write_text(self.to_tsv_string())

    @classmethod
    def write_dandiset_state_table(
        cls,
        *,
        dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
        relative_path: str = _STATE_TSV_RELATIVE_PATH,
        processing_directory: pathlib.Path | None = None,
        test: bool = False,
    ) -> None:
        """
        Write this Dandiset's queue state as a ``state.tsv`` table within itself.

        Builds the state from *dandiset_id*'s remote ``assets.jsonld`` metadata (see
        :meth:`from_dandi`) and uploads it as a tab-separated table to *relative_path* within
        *dandiset_id* (default ``derivatives/state.tsv``) via
        :func:`~dandi_compute_code.dandiset.write_dandiset_file`.

        Intended to be called once for the job capsules ("source") Dandiset and once for the
        failed runs archive ("archived") Dandiset. There is no local queue directory or local
        state file involved -- the state is always rebuilt fresh from *dandiset_id*'s remote
        ``assets.jsonld`` and rewritten directly.

        :param dandiset_id: The Dandiset whose ``assets.jsonld`` portrays the state, and which
            the table is written into. Defaults to the job capsules Dandiset.
        :type dandiset_id: str
        :param relative_path: Path (relative to the Dandiset root) the table is written to.
        :type relative_path: str
        :param processing_directory: Directory for the temporary working tree used to upload
            the table (defaults to the system temporary location).
        :type processing_directory: pathlib.Path | None
        :param test: When ``True``, leave the temporary working tree on disk after a
            successful upload for debugging.
        :type test: bool
        :raises RuntimeError: If ``DANDI_API_KEY`` is unset or blank, or if the upload fails.
        """
        state = cls.from_dandi(dandiset_id=dandiset_id)
        write_dandiset_file(
            dandiset_id=dandiset_id,
            relative_path=relative_path,
            content=state.to_tsv_string(),
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

        :param dandiset_directory: Local clone of the dandiset used to locate Nextflow timeline
            reports.
        :type dandiset_directory: pathlib.Path
        :param dandiset_id: The Dandiset the statistics JSON is written into.
        :type dandiset_id: str
        :param relative_path: Path (relative to the Dandiset root) the statistics JSON is
            written to.
        :type relative_path: str
        :param processing_directory: Directory for the temporary working tree used to upload the
            statistics JSON (defaults to the system temporary location).
        :type processing_directory: pathlib.Path | None
        :param test: When ``True``, leave the temporary working tree on disk after a successful
            upload for debugging.
        :type test: bool
        :raises RuntimeError: If ``DANDI_API_KEY`` is unset or blank, or if the upload fails.
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

        :param dandiset_directory: Local clone of the dandiset used to resolve and
            delete matching job capsule directories.
        :type dandiset_directory: pathlib.Path
        :returns: Job capsule directory paths that were deleted.
        :rtype: list[pathlib.Path]
        :raises RuntimeError: If ``DANDI_API_KEY`` is not set or is blank.
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

        *status* names the :class:`QueueState` property selecting the entries to
        archive: ``"failed"`` (:attr:`failed` — code and logs present, no output),
        ``"pending"`` (:attr:`pending` — code prepared but never submitted), or
        ``"stalled"`` (:attr:`stalled` — submitted to the scheduler but no logs or
        output ever appeared). For each matching entry, resolves its capsule path
        against *dandiset_id*'s remote ``assets.jsonld`` (see
        :meth:`JobCapsule.resolve_capsule_path`) and moves the
        corresponding capsule from *dandiset_id* to *archive_dandiset_id* via
        :func:`~dandi_compute_code.dandiset.move_job_capsule`. Both Dandisets are
        addressed purely by ID -- everything is resolved and moved ephemerally over
        the network, with no local Dandiset clone required.

        :param status: Which subset of entries to archive.
        :type status: typing.Literal["failed", "pending", "stalled"]
        :param dandiset_id: Dandiset entries are archived *from*. Defaults to the job
            capsules Dandiset.
        :type dandiset_id: str
        :param archive_dandiset_id: Dandiset entries are archived *to*. Defaults to
            the failed runs archive Dandiset.
        :type archive_dandiset_id: str
        :param processing_directory: Directory for the temporary working tree used by
            each move (defaults to the system temporary location).
        :type processing_directory: pathlib.Path | None
        :param test: When ``True``, leave each temporary working tree on disk after a
            successful move for debugging.
        :type test: bool
        :returns: Capsule paths (relative to the Dandiset root) that were archived,
            in the order they were processed.
        :rtype: list[str]
        :raises RuntimeError: If ``DANDI_API_KEY`` is unset or blank, or if archiving
            any individual capsule fails (see :func:`move_job_capsule`). A failure
            leaves entries processed so far archived and stops before the rest.
        :raises ValueError: If *status* is not ``"failed"``, ``"pending"``, or
            ``"stalled"``.
        """
        if status not in ("failed", "pending", "stalled"):
            message = f"Unknown status {status!r}; expected 'failed', 'pending', or 'stalled'."
            raise ValueError(message)

        if not os.environ.get("DANDI_API_KEY", "").strip():
            message = "`DANDI_API_KEY` environment variable is not set or is blank."
            raise RuntimeError(message)

        entries = getattr(self, status)
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

        :param processing_directory: Directory the per-pipeline dispatch directories are
            created in. Each holds a manifest, a dispatch script and the array's logs, so it
            has to stay readable from the compute nodes for as long as the array lives.
        :param only_pipeline: Dispatch only this pipeline instead of every configured one.
        :param max_concurrent: Overrides every dispatched pipeline's configured concurrency
            limit.
        :param jitter_seconds: Maximum random delay (seconds) before processing; ``0`` disables.
            Spreads concurrent invocations out so they do not read the cluster state at once.
        :param dandiset_id: The Dandiset capsules are downloaded from and uploaded back to.
        :param test: If ``True``, array tasks leave their working trees on disk for debugging.
        :returns: The dispatch outcome per pipeline, keyed by pipeline name.
        :rtype: dict[str, DispatchResult]
        :raises ValueError: If *jitter_seconds* is negative, if *max_concurrent* is less than
            1, or if *only_pipeline* is not configured.
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

    @staticmethod
    def resolve_latest_pipeline_version(*, pipeline: str, pipeline_directory: pathlib.Path | None = None) -> str:
        """
        The latest version of *pipeline* available on this machine.

        New job capsules always run the latest locally available pipeline version, so there is
        no version priority list to maintain. What "locally available" means depends on the
        pipeline. The AIND ephys pipeline lives in its own repository, checked out next to
        this one on the cluster, so its latest version is the highest release tag in that
        checkout. The LFP pipeline ships inside this package, so its latest version is this
        package's own version.

        :param pipeline: The pipeline name as it appears in the packaged pipeline configuration.
        :param pipeline_directory: Local checkout of the pipeline repository, for pipelines
            that live in one. Defaults to the AIND ephys pipeline checkout on MIT Engaging.
        :return: The version string to form new job capsules against.
        :rtype: str
        """
        if pipeline == "lfp":
            latest_version = f"v{importlib.metadata.version('dandi-compute-code')}"
            return latest_version

        latest_version = _latest_repository_version_tag(pipeline_directory or _DEFAULT_AIND_PIPELINE_DIRECTORY)
        return latest_version

    @staticmethod
    def _qualifying_content_ids(pipeline: str, /) -> list[str]:
        """Content IDs qualifying for *pipeline*, ordered so a limit samples Dandisets uniformly."""
        fetch = _fetch_qualifying_lfp_content_ids if pipeline == "lfp" else _fetch_qualifying_aind_content_ids
        ordered_content_ids = _order_content_ids_for_uniform_dandiset_sampling(content_ids=fetch())
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

        :param only_pipeline: Form capsules only for this pipeline instead of every pipeline in
            the configuration. Raises if the name is not configured.
        :param config_key: Key for a registered job configuration.
        :param content_ids: Explicit content IDs to form capsules for. The qualifying list is
            not fetched from the network when these are provided.
        :param limit: Form at most this many capsules in total. Unlimited when ``None``.
        :param force_latest_versions: Form a capsule for every qualifying asset against the
            latest pipeline and codebase versions, whether or not one already exists.
        :param pipeline_directory: Local checkout of the AIND pipeline repository.
        :return: The number of job capsules that were formed.
        :rtype: int
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
                        if pipeline_name == "lfp":
                            script_file_path = prepare_lfp_job(
                                content_id=content_id,
                                parameters_key=params_key,
                                pipeline_version=version,
                                force_new_capsule=force_latest_versions,
                                silent=True,
                            )
                        else:
                            script_file_path = prepare_aind_ephys_job(
                                content_id=content_id,
                                parameters_key=params_key,
                                pipeline_version=version,
                                pipeline_directory=pipeline_directory,
                                config_key=config_key,
                                force_new_capsule=force_latest_versions,
                                silent=True,
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
        records = QueueState.dump_issues(
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
    def from_tsv(cls, file_path: pathlib.Path, /) -> QueueState:
        """
        Load from an existing ``state.tsv`` file.

        The inverse of :meth:`to_tsv`/:meth:`to_tsv_string`: parses the tab-separated
        table (via :class:`csv.DictReader`, using :data:`_STATE_TSV_FIELD_NAMES` as
        the expected column order) back into :class:`JobCapsule` objects via
        :meth:`JobCapsule.from_tsv_row`.

        :param file_path: Path to the ``state.tsv`` file to read.
        :type file_path: pathlib.Path
        :raises FileNotFoundError: If *file_path* does not exist.
        """
        if not file_path.exists():
            message = f"State file not found: {file_path}"
            raise FileNotFoundError(message)
        with file_path.open(newline="") as file_stream:
            reader = csv.DictReader(file_stream, delimiter="\t")
            entries = [JobCapsule.from_tsv_row(row) for row in reader]
        return cls(entries=entries)
