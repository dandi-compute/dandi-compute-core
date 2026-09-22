"""
QueueState — typed container for ``state.tsv``.

``state.tsv`` is a tab-separated table where each row is one job
capsule. This module provides the typed model over it:

- :class:`JobEntry` wraps an existing :class:`JobInfo` with the status fields
  (``has_code``, ``has_output``, ``has_logs``, ``content_id``, ...).
- :class:`QueueState` is the container — a list of ``JobEntry`` objects with
  convenience helpers for filtering and round-trip I/O.
"""

from __future__ import annotations

import collections
import csv
import datetime
import io
import json
import logging
import os
import pathlib
import random
import shutil
import subprocess
import tempfile
import time
from collections.abc import Collection, Iterator
from dataclasses import dataclass, field
from typing import Literal

from ._globals import _CONFIGS_REGISTRIES, _PARAMS_REGISTRIES
from ._job_info import JobInfo
from ._queue_utils import (
    _CapsuleProvenanceCache,
    _collect_job_capsules,
    _duration_string_to_seconds,
    _extract_error_lines,
    _extract_nextflow_timeline_data,
    _finalize_job_capsule_records,
    _list_capsule_log_directories,
    _load_queue_config,
    _remove_empty_parents,
    _sort_key,
    _UpstreamMetadataCache,
)
from ..dandiset import move_job_capsule, write_dandiset_file
from ..dandiset._globals import (
    _FAILED_RUNS_ARCHIVE_DANDISET_ID,
    _JOB_CAPSULES_DANDISET_ID,
    _dandiset_derivatives_relative_dir,
)
from ..dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    _build_asset_metadata,
    load_assets_jsonld_metadata,
)

_log = logging.getLogger(__name__)

#: Dandiset whose assets back the pending/submission queries (the job capsules Dandiset).
_DANDISET_ID = _JOB_CAPSULES_DANDISET_ID

#: Column order for the ``state.tsv`` table -- matches :meth:`JobEntry.to_dict` field order.
_STATE_TSV_FIELD_NAMES = [
    "job_id",
    "dandiset_id",
    "dandi_path",
    "pipeline",
    "version",
    "params",
    "config",
    "codebase",
    "content_id",
    "asset_size_bytes",
    "has_code",
    "has_been_submitted",
    "has_output",
    "has_logs",
    "dataset_description_path",
    "output_paths",
    "log_paths",
    "created_at",
    "job_completion_time",
]

#: Default subpath (relative to a Dandiset root) that ``state.tsv`` is written to.
_STATE_TSV_RELATIVE_PATH = "derivatives/state.tsv"


@dataclass
class JobEntry:
    """
    A :class:`JobInfo` (identity) plus the status fields written by
    ``write_queue_state`` and consumed across the queue module.
    """

    job: JobInfo
    content_id: str | None
    asset_size_bytes: int | None
    has_code: bool = False
    has_been_submitted: bool = False
    has_output: bool = False
    has_logs: bool = False
    created_at: str | None = None
    job_completion_time: str | None = None
    dataset_description_path: dict[str, str] = field(default_factory=dict)
    output_paths: dict[str, str] = field(default_factory=dict)
    log_paths: dict[str, str] = field(default_factory=dict)

    @property
    def is_pending(self) -> bool:
        """Code prepared but never submitted (no logs, no output yet)."""
        return self.has_code and not self.has_been_submitted and not self.has_logs and not self.has_output

    @property
    def is_stalled(self) -> bool:
        """Submitted to the scheduler but no logs or output have appeared yet — likely stuck or lost."""
        return self.has_been_submitted and not self.has_logs and not self.has_output

    @property
    def is_running(self) -> bool:
        """Logs present but no output yet — likely still executing."""
        return self.has_logs and not self.has_output

    @property
    def is_successful(self) -> bool:
        """Output directory present — job completed successfully."""
        return self.has_output

    @property
    def is_failed(self) -> bool:
        """Has code and logs but no output — the job ran but did not succeed."""
        return self.has_code and self.has_logs and not self.has_output

    @property
    def identity(self) -> tuple:
        """
        Stable key for matching queue/state/last-submitted entries.

        Excludes ``codebase`` deliberately: a capsule is the same logical job
        regardless of which codebase version produced it.
        """
        return (
            self.job.dandiset_id,
            self.job.dandi_path,
            self.job.pipeline,
            self.job.version,
            self.job.params,
            self.job.config,
        )

    def capsule_dir(self, base_dir: pathlib.Path, /) -> pathlib.Path:
        """
        Return this job capsule's directory path under *base_dir*.

        :param base_dir: Root of the local Dandiset tree to resolve paths under.
        :type base_dir: pathlib.Path
        :raises ValueError: If this entry's ``dandi_path`` is an empty string.
        """
        if self.job.dandi_path == "":
            message = f"Entry has invalid dandi_path field (empty): {self!r}"
            raise ValueError(message)
        normalized_dandi_path = self.job.dandi_path.removesuffix(".nwb")

        pipeline_dir = (
            base_dir
            / "derivatives"
            / pathlib.PurePosixPath(_dandiset_derivatives_relative_dir(self.job.dandiset_id))
            / pathlib.PurePosixPath(normalized_dandi_path)
            / f"pipeline-{self.job.pipeline}"
        )
        return pipeline_dir / self.job.job_id

    def resolve_capsule_dir(self, base_dir: pathlib.Path, /) -> pathlib.Path:
        """
        Resolve the on-disk job capsule directory path for this entry.

        Falls back to searching every ``pipeline-*`` directory in the Dandiset for this
        entry's job ID, which covers a recorded ``dandi_path`` that does not match the
        on-disk layout.
        """
        capsule_dir = self.capsule_dir(base_dir)
        if capsule_dir.is_dir():
            return capsule_dir

        dandiset_root = (
            base_dir / "derivatives" / pathlib.PurePosixPath(_dandiset_derivatives_relative_dir(self.job.dandiset_id))
        )
        if not dandiset_root.is_dir():
            return capsule_dir

        for pipeline_dir in sorted(dandiset_root.rglob(f"pipeline-{self.job.pipeline}")):
            fallback_capsule_dir = pipeline_dir / self.job.job_id
            if fallback_capsule_dir.is_dir():
                return fallback_capsule_dir

        return capsule_dir

    def capsule_path(self) -> str:
        """
        Return this job capsule's path relative to the Dandiset root (a POSIX string).

        The remote-metadata counterpart of :meth:`capsule_dir`, used when resolving a
        capsule's path against DANDI assets metadata rather than a local Dandiset clone.

        :raises ValueError: If this entry's ``dandi_path`` is an empty string.
        """
        if self.job.dandi_path == "":
            message = f"Entry has invalid dandi_path field (empty): {self!r}"
            raise ValueError(message)
        normalized_dandi_path = self.job.dandi_path.removesuffix(".nwb")

        pipeline_dir = (
            f"derivatives/{_dandiset_derivatives_relative_dir(self.job.dandiset_id)}"
            f"/{normalized_dandi_path}/pipeline-{self.job.pipeline}"
        )
        return f"{pipeline_dir}/{self.job.job_id}"

    def resolve_capsule_path(self, asset_paths: Collection[str], /) -> str:
        """
        Resolve the capsule path (relative to the Dandiset root) for this entry against a
        known set of remote asset paths, e.g. the keys of
        :attr:`~dandi_compute_code.dandiset.AssetsJsonldMetadata.path_to_asset_metadata`.

        The remote-metadata counterpart of :meth:`resolve_capsule_dir`: the same fallback
        search, but checked against *asset_paths* membership instead of the local
        filesystem -- so this works purely from DANDI metadata, without a local Dandiset
        clone.

        :param asset_paths: Asset paths (POSIX strings) for the Dandiset this
            entry's capsule lives in.
        :type asset_paths: collections.abc.Collection[str]
        """

        def _has_assets_under(prefix: str) -> bool:
            return any(path == prefix or path.startswith(f"{prefix}/") for path in asset_paths)

        capsule_path = self.capsule_path()
        if _has_assets_under(capsule_path):
            return capsule_path

        dandiset_prefix = f"derivatives/{_dandiset_derivatives_relative_dir(self.job.dandiset_id)}/"
        if not any(path.startswith(dandiset_prefix) for path in asset_paths):
            return capsule_path

        pipeline_dir_marker = f"/pipeline-{self.job.pipeline}/"
        pipeline_dirs = sorted(
            {
                path[: path.index(pipeline_dir_marker) + len(pipeline_dir_marker) - 1]
                for path in asset_paths
                if path.startswith(dandiset_prefix) and pipeline_dir_marker in path
            }
        )
        for pipeline_dir in pipeline_dirs:
            fallback_capsule_path = f"{pipeline_dir}/{self.job.job_id}"
            if _has_assets_under(fallback_capsule_path):
                return fallback_capsule_path

        return capsule_path

    def resolve_unsubmitted_capsule_dir(self, base_dir: pathlib.Path, /) -> pathlib.Path | None:
        """
        Resolve the job capsule directory only if this entry is queued but unsubmitted.

        Returns ``None`` when the entry is not pending (see :attr:`is_pending`) or
        when a submitted marker (``code/submitted`` or ``code/submitted_date-*``)
        is present on disk.
        """
        if not self.is_pending:
            return None

        capsule_dir = self.resolve_capsule_dir(base_dir)
        code_dir = capsule_dir / "code"
        if (code_dir / "submitted").exists() or any(code_dir.glob("submitted_date-*")):
            return None
        return capsule_dir

    @classmethod
    def from_dict(cls, data: dict, /) -> JobEntry:
        """Construct from a raw entry dict (as produced by :meth:`to_dict`)."""
        job = JobInfo(
            job_id=data["job_id"],
            dandiset_id=data["dandiset_id"],
            dandi_path=data["dandi_path"],
            pipeline=data["pipeline"],
            version=data["version"],
            params=data["params"],
            config=data["config"],
            codebase=data["codebase"],
        )
        return cls(
            job=job,
            content_id=data.get("content_id"),
            asset_size_bytes=data.get("asset_size_bytes"),
            has_code=bool(data.get("has_code", False)),
            has_been_submitted=bool(data.get("has_been_submitted", False)),
            has_output=bool(data.get("has_output", False)),
            has_logs=bool(data.get("has_logs", False)),
            created_at=data.get("created_at"),
            job_completion_time=data.get("job_completion_time"),
            dataset_description_path=dict(data.get("dataset_description_path") or {}),
            output_paths=dict(data.get("output_paths") or {}),
            log_paths=dict(data.get("log_paths") or {}),
        )

    def to_dict(self) -> dict:
        """Serialise back to the flat dict format underlying :meth:`to_tsv_row`."""
        return {
            **self.job.to_dict(),
            "content_id": self.content_id,
            "asset_size_bytes": self.asset_size_bytes,
            "has_code": self.has_code,
            "has_been_submitted": self.has_been_submitted,
            "has_output": self.has_output,
            "has_logs": self.has_logs,
            "dataset_description_path": self.dataset_description_path,
            "output_paths": self.output_paths,
            "log_paths": self.log_paths,
            "created_at": self.created_at,
            "job_completion_time": self.job_completion_time,
        }

    def to_tsv_row(self) -> dict[str, str]:
        """
        Flatten this entry to a single ``state.tsv`` row.

        Every value from :meth:`to_dict` is coerced to a plain string: ``None`` becomes an
        empty cell, and the nested path/content-id mappings (``dataset_description_path``,
        ``output_paths``, ``log_paths``) are serialised as compact JSON so the table stays
        strictly tabular (one row per job capsule).
        """
        raw = self.to_dict()
        row: dict[str, str] = {}
        for field_name in _STATE_TSV_FIELD_NAMES:
            value = raw[field_name]
            if isinstance(value, dict):
                row[field_name] = json.dumps(value, sort_keys=True) if value else ""
            elif value is None:
                row[field_name] = ""
            else:
                row[field_name] = str(value)
        return row

    @classmethod
    def from_tsv_row(cls, row: dict[str, str], /) -> JobEntry:
        """
        Construct from a single ``state.tsv`` row (the inverse of :meth:`to_tsv_row`).

        Reverses the coercions applied by :meth:`to_tsv_row`: empty cells become
        ``None`` (or ``{}`` for the JSON-encoded mapping fields), ``asset_size_bytes``
        is parsed back to ``int``, and the boolean fields (stored as the literal
        strings ``"True"``/``"False"``) are parsed back to ``bool``.
        """
        job = JobInfo(
            job_id=row["job_id"],
            dandiset_id=row["dandiset_id"],
            dandi_path=row["dandi_path"],
            pipeline=row["pipeline"],
            version=row["version"],
            params=row["params"],
            config=row["config"],
            codebase=row["codebase"],
        )

        def _parse_bool(value: str) -> bool:
            return value == "True"

        def _parse_optional_str(value: str) -> str | None:
            return value if value != "" else None

        def _parse_json_dict(value: str) -> dict[str, str]:
            return json.loads(value) if value != "" else {}

        content_id = row["content_id"] or None
        asset_size_bytes_raw = row["asset_size_bytes"]
        asset_size_bytes = int(asset_size_bytes_raw) if asset_size_bytes_raw != "" else None

        return cls(
            job=job,
            content_id=content_id,
            asset_size_bytes=asset_size_bytes,
            has_code=_parse_bool(row["has_code"]),
            has_been_submitted=_parse_bool(row["has_been_submitted"]),
            has_output=_parse_bool(row["has_output"]),
            has_logs=_parse_bool(row["has_logs"]),
            created_at=_parse_optional_str(row["created_at"]),
            job_completion_time=_parse_optional_str(row["job_completion_time"]),
            dataset_description_path=_parse_json_dict(row["dataset_description_path"]),
            output_paths=_parse_json_dict(row["output_paths"]),
            log_paths=_parse_json_dict(row["log_paths"]),
        )


@dataclass
class QueueState:
    """Container for all entries in ``state.tsv``."""

    entries: list[JobEntry]

    def __iter__(self) -> Iterator[JobEntry]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    @property
    def pending(self) -> list[JobEntry]:
        """Entries with code prepared but not yet submitted."""
        return [e for e in self.entries if e.is_pending]

    @property
    def stalled(self) -> list[JobEntry]:
        """Entries submitted to the scheduler with no logs or output yet."""
        return [e for e in self.entries if e.is_stalled]

    @property
    def running(self) -> list[JobEntry]:
        """Entries with logs present but no output — likely still executing."""
        return [e for e in self.entries if e.is_running]

    @property
    def successful(self) -> list[JobEntry]:
        """Entries whose output directory is present."""
        return [e for e in self.entries if e.is_successful]

    @property
    def failed(self) -> list[JobEntry]:
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

    def entry_for(self, *, dandi_path: str, config: str | None = None) -> JobEntry:
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

    @classmethod
    def submit_next(
        cls,
        *,
        processing_directory: pathlib.Path,
        max_submissions: int = 2,
        test: bool = False,
    ) -> bool:
        """
        Submit the next eligible pending entries from the DANDI assets metadata.

        Identifies all job capsule directories that contain a ``code/submit.sh`` asset
        but no adjacent submitted-marker asset (see :meth:`pending_code_dirs`). For
        each candidate (up to *max_submissions*), a temporary working directory is
        created inside *processing_directory*, the ``code/`` tree is downloaded via
        ``dandi download --preserve-tree``, the submission script is executed via
        ``sbatch``, a submitted marker is written adjacent to ``submit.sh``, the
        marker is pushed back to the archive via ``dandi upload --allow-any-path``,
        and the temporary directory is removed on success.

        :param processing_directory: Directory in which temporary per-job working
            trees are created.
        :type processing_directory: pathlib.Path
        :param max_submissions: Maximum number of pending jobs to submit.
        :type max_submissions: int
        :param test: When ``True``, leave temporary working directories on disk
            after successful submission for debugging.
        :type test: bool
        :returns: ``True`` if at least one job was submitted, ``False`` otherwise.
        :rtype: bool
        :raises RuntimeError: If ``dandi download``, ``sbatch``, or ``dandi upload``
            returns a non-zero exit code for any candidate.
        """
        if max_submissions < 1:
            return False

        candidates = cls.pending_code_dirs()

        if not candidates:
            _log.info("No eligible pending entries available for submission")
            return False

        for code_dir_path in candidates[:max_submissions]:
            dandi_url = f"dandi://dandi/{_DANDISET_ID}/{code_dir_path}/"
            # Temporary directory is intentionally left on disk when any step fails
            # so that it can be inspected for debugging.
            temp_dir = pathlib.Path(tempfile.mkdtemp(dir=processing_directory, prefix="submit-next-"))
            _log.info("Submitting job run for %s in %s", code_dir_path, temp_dir)

            result = subprocess.run(
                ["dandi", "download", "--preserve-tree", dandi_url],
                capture_output=True,
                text=True,
                cwd=temp_dir,
            )
            _log.info("dandi download returned code %d for %s", result.returncode, dandi_url)
            _log.debug("dandi download stdout: %s\nstderr: %s", result.stdout, result.stderr)
            if result.returncode != 0:
                _log.warning("dandi download stdout: %s\nstderr: %s", result.stdout, result.stderr)
                message = f"dandi download failed for {dandi_url}"
                raise RuntimeError(message)

            dandiset_directory = temp_dir / _DANDISET_ID
            submit_sh_path = dandiset_directory / code_dir_path / "submit.sh"
            result = subprocess.run(
                ["sbatch", str(submit_sh_path.absolute())],
                capture_output=True,
                text=True,
            )
            _log.info("sbatch returned code %d; stdout %s", result.returncode, result.stdout)
            _log.debug("sbatch stdout: %s\nstderr: %s", result.stdout, result.stderr)
            if result.returncode != 0:
                _log.warning("sbatch stdout: %s\nstderr: %s", result.stdout, result.stderr)
                message = "sbatch submission failed - please check the logs to see more details."
                raise RuntimeError(message)

            now = datetime.datetime.now()
            submitted_marker = submit_sh_path.parent / (
                f"submitted_date-{now.year:04d}+{now.month:02d}+{now.day:02d}"
                f"_time-{now.hour:02d}+{now.minute:02d}+{now.second:02d}"
            )
            submitted_marker.write_bytes(b"1")
            _log.info("Created `submitted` file at: %s", submitted_marker.absolute())

            result = subprocess.run(
                ["dandi", "upload", "--allow-any-path"],
                capture_output=True,
                text=True,
                cwd=dandiset_directory,
            )
            _log.info("dandi upload returned code %d", result.returncode)
            _log.debug("dandi upload stdout: %s\nstderr: %s", result.stdout, result.stderr)
            if result.returncode != 0:
                _log.warning("dandi upload stdout: %s\nstderr: %s", result.stdout, result.stderr)
                message = "dandi upload failed - please check the logs to see more details."
                raise RuntimeError(message)

            if test:
                _log.info("Leaving temporary directory in place for test mode: %s", temp_dir)
            else:
                shutil.rmtree(temp_dir)

        return True

    @staticmethod
    def count_running_aind_ephys_pipeline_jobs() -> int:
        """
        Count currently running AIND Ephys pipeline jobs via the SLURM scheduler.

        Calls ``squeue --me --format=%j`` and counts jobs whose name is exactly
        ``AIND-Ephys-Pipeline``.

        :raises RuntimeError: If the ``squeue`` invocation exits non-zero and writes
            to standard error.
        """
        command = ["squeue", "--me", "--format=%j"]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0 and result.stderr:
            message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
            raise RuntimeError(message)
        if result.stderr:
            _log.warning(result.stderr)
        return sum(1 for line in result.stdout.splitlines() if line.strip() == "AIND-Ephys-Pipeline")

    @staticmethod
    def load_queue_config() -> dict:
        """
        Read and validate the packaged pipeline configuration.

        Always reads the pipeline configuration packaged with this repo -- there is no local
        override.

        :raises FileNotFoundError: If the packaged pipeline configuration file is missing.
        :raises ValueError: If the pipeline configuration fails LinkML validation.
        """
        return _load_queue_config()

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
        return cls(entries=[JobEntry.from_dict(record) for record in records])

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
        :meth:`JobEntry.resolve_capsule_path`) and moves the
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
        max_concurrent_aind_jobs: int = 2,
        jitter_seconds: float = 30.0,
        test: bool = False,
    ) -> Literal["submitted", "no-pending", "slots-unavailable"]:
        """
        Submit jobs from the live queue state up to ``max_concurrent_aind_jobs`` total
        running ``AIND-Ephys-Pipeline`` SLURM jobs.

        The queue state is always fetched fresh -- there is no local queue directory. Presence of
        pending work is checked live via :meth:`has_pending_jobs`, and submission itself works
        entirely from the DANDI assets metadata (see :meth:`submit_next`).

        :param processing_directory: Directory for temporary working trees during submission.
        :param max_concurrent_aind_jobs: Maximum concurrent ``AIND-Ephys-Pipeline`` jobs.
        :param jitter_seconds: Maximum random delay (seconds) before processing; ``0`` disables.
        :param test: If ``True``, preserve temporary processing directories on success.
        :raises ValueError: If *jitter_seconds* is negative or *max_concurrent_aind_jobs* < 1.
        """
        if jitter_seconds < 0:
            message = "jitter_seconds must be non-negative"
            raise ValueError(message)
        if jitter_seconds > 0:
            delay = random.uniform(0, jitter_seconds)
            _log.info("Sleeping %.2f seconds (jitter) before processing queue", delay)
            time.sleep(delay)

        if max_concurrent_aind_jobs < 1:
            message = "max_concurrent_aind_jobs must be at least 1"
            raise ValueError(message)
        if not cls.has_pending_jobs():
            return "no-pending"

        running_count = cls.count_running_aind_ephys_pipeline_jobs()
        available_slots = max(0, max_concurrent_aind_jobs - running_count)
        if available_slots < 1:
            return "slots-unavailable"

        submitted_any = cls.submit_next(
            processing_directory=processing_directory,
            max_submissions=available_slots,
            test=test,
        )
        return "submitted" if submitted_any else "no-pending"

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
        the expected column order) back into :class:`JobEntry` objects via
        :meth:`JobEntry.from_tsv_row`.

        :param file_path: Path to the ``state.tsv`` file to read.
        :type file_path: pathlib.Path
        :raises FileNotFoundError: If *file_path* does not exist.
        """
        if not file_path.exists():
            message = f"State file not found: {file_path}"
            raise FileNotFoundError(message)
        with file_path.open(newline="") as file_stream:
            reader = csv.DictReader(file_stream, delimiter="\t")
            entries = [JobEntry.from_tsv_row(row) for row in reader]
        return cls(entries=entries)
