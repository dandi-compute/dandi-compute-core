"""
``JobCapsule`` — one row of ``jobs.tsv``: a job's identity plus its status.

Its asset path mappings are the one part kept apart, as rows of ``paths.tsv``.

Kept in its own module so the typed row model stays separable from
:mod:`._pipeline_queue`, which only containerises and round-trips these rows.
"""

from __future__ import annotations

import datetime
import json
import pathlib
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Literal

import beartype

from ._job_info import JobInfo
from ..dandiset._globals import _dandiset_derivatives_relative_dir

#: Where a job capsule has reached in its lifecycle. The one status field replaces the
#: presence flags (``has_code``, ``has_been_submitted``, ``has_logs``, ``has_output``)
#: the table used to carry, and names the same subsets the queue selects on.
JobStatus = Literal["pending", "stalled", "failed", "successful", "unknown"]

#: Every value :data:`JobStatus` allows, for validating a status read back from a table.
JOB_STATUSES: tuple[JobStatus, ...] = ("pending", "stalled", "failed", "successful", "unknown")

#: Column order for the ``jobs.tsv`` table. The asset path mappings are left out and kept in
#: ``paths.tsv`` instead, so each ``jobs.tsv`` row stays short enough to read as a table.
_JOBS_TSV_FIELD_NAMES = [
    "job_id",
    "dandiset_id",
    "within_dandiset_path",
    "pipeline",
    "version",
    "params",
    "config",
    "codebase",
    "content_id",
    "asset_size_bytes",
    "status",
    "created_at",
    "job_submission_time",
    "job_completion_time",
    "queue_wait_seconds",
    "run_duration_seconds",
]

#: The ``JobCapsule`` mapping fields that ``paths.tsv`` holds, in the order their rows are written.
_PATH_FIELD_NAMES = ("dataset_description_path", "output_paths", "log_paths")

#: Column order for the ``paths.tsv`` table. One row is one asset path of one job capsule.
_PATHS_TSV_FIELD_NAMES = ["job_id", "path", "content_id"]


@beartype.beartype
def _path_field_name(*, job_id: str, path: str) -> str | None:
    """
    The ``JobCapsule`` mapping field an asset path of the capsule *job_id* belongs in.

    Read from where the path sits beneath the capsule directory, on the same terms the
    mappings are built from DANDI metadata. ``None`` when *path* is not beneath a
    ``job_id`` directory or matches none of the mappings.
    """
    parts = pathlib.PurePosixPath(path).parts
    if job_id not in parts:
        return None
    subpath_parts = parts[parts.index(job_id) + 1 :]
    if subpath_parts == ("dataset_description.json",):
        return "dataset_description_path"
    if subpath_parts[:1] == ("derivatives",):
        return "output_paths"
    if subpath_parts[:1] == ("logs",) and len(subpath_parts) > 1:
        return "log_paths"
    return None


@beartype.beartype
def _coerce_status(value: object, /) -> JobStatus:
    """Accept a status read back from a table or dict, falling back to ``"unknown"``."""
    for status in JOB_STATUSES:
        if value == status:
            return status
    return "unknown"


@beartype.beartype
def _derive_job_status(*, has_code: bool, has_been_submitted: bool, has_logs: bool, has_output: bool) -> JobStatus:
    """
    Collapse the observed presence of a capsule's directories into a single status.

    The checks are ordered from the furthest point in the lifecycle backwards, so every
    capsule lands on exactly one status.

    ``"failed"`` covers what used to be reported as both running and failed. Logs without
    output can mean either, and nothing recorded about a capsule tells the two apart, so
    they are one status rather than two overlapping ones.

    Parameters
    ----------
    has_code : bool
        A ``code`` directory is present.
    has_been_submitted : bool
        A ``code/submitted*`` marker is present.
    has_logs : bool
        A ``logs`` directory holds something other than its dataset description.
    has_output : bool
        A ``derivatives`` directory is present.
    """
    if has_output:
        return "successful"
    if has_logs:
        return "failed"
    if has_been_submitted:
        return "stalled"
    if has_code:
        return "pending"
    return "unknown"


@beartype.beartype
def _parse_timestamp(value: str | None, /) -> datetime.datetime | None:
    """Parse an ISO 8601 timestamp cell, treating a naive timestamp as UTC."""
    if not value:
        return None
    try:
        timestamp = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=datetime.timezone.utc)
    return timestamp


@beartype.beartype
def _elapsed_seconds(*, start: str | None, end: str | None) -> int | None:
    """Whole seconds between two ISO 8601 timestamps, or ``None`` if either is missing or malformed."""
    start_timestamp = _parse_timestamp(start)
    end_timestamp = _parse_timestamp(end)
    if start_timestamp is None or end_timestamp is None:
        return None
    return round((end_timestamp - start_timestamp).total_seconds())


@beartype.beartype
@dataclass
class JobCapsule:
    """
    A :class:`JobInfo` (identity) plus the lifecycle status (see :data:`JobStatus`) and
    the timestamps and asset mappings consumed across the queue module.
    """

    job: JobInfo
    content_id: str | None
    asset_size_bytes: int | None
    status: JobStatus = "unknown"
    created_at: str | None = None
    job_submission_time: str | None = None
    job_completion_time: str | None = None
    dataset_description_path: dict[str, str] = field(default_factory=dict)
    output_paths: dict[str, str] = field(default_factory=dict)
    log_paths: dict[str, str] = field(default_factory=dict)

    @property
    def queue_wait_seconds(self) -> int | None:
        """
        Seconds this job waited between being queued and being submitted
        (``created_at`` to ``job_submission_time``).

        ``None`` when either timestamp is missing -- a job that was never submitted has
        no waiting time yet.
        """
        return _elapsed_seconds(start=self.created_at, end=self.job_submission_time)

    @property
    def run_duration_seconds(self) -> int | None:
        """
        Seconds between this job's submission and its completion
        (``job_submission_time`` to ``job_completion_time``).

        ``None`` when either timestamp is missing -- a job that has not completed (or
        whose submission time is unknown) has no duration yet.
        """
        return _elapsed_seconds(start=self.job_submission_time, end=self.job_completion_time)

    @property
    def identity(self) -> tuple:
        """
        Stable key for matching queue/state/last-submitted entries.

        Excludes ``codebase`` deliberately: a capsule is the same logical job
        regardless of which codebase version produced it.
        """
        return (
            self.job.dandiset_id,
            self.job.within_dandiset_path,
            self.job.pipeline,
            self.job.version,
            self.job.params,
            self.job.config,
        )

    def capsule_dir(self, base_dir: pathlib.Path, /) -> pathlib.Path:
        """
        Return this job capsule's directory path under *base_dir*.

        Parameters
        ----------
        base_dir : pathlib.Path
            Root of the local Dandiset tree to resolve paths under.

        Raises
        ------
        ValueError
            If this entry's ``within_dandiset_path`` is an empty string.
        """
        if self.job.within_dandiset_path == "":
            message = f"Entry has invalid within_dandiset_path field (empty): {self!r}"
            raise ValueError(message)
        normalized_within_dandiset_path = self.job.within_dandiset_path.removesuffix(".nwb")

        pipeline_dir = (
            base_dir
            / "derivatives"
            / pathlib.PurePosixPath(_dandiset_derivatives_relative_dir(self.job.dandiset_id))
            / pathlib.PurePosixPath(normalized_within_dandiset_path)
            / f"pipeline-{self.job.pipeline}"
        )
        return pipeline_dir / self.job.job_id

    def capsule_path(self) -> str:
        """
        Return this job capsule's path relative to the Dandiset root (a POSIX string).

        The remote-metadata counterpart of :meth:`capsule_dir`, used when resolving a
        capsule's path against DANDI assets metadata rather than a local Dandiset clone.

        Raises
        ------
        ValueError
            If this entry's ``within_dandiset_path`` is an empty string.
        """
        if self.job.within_dandiset_path == "":
            message = f"Entry has invalid within_dandiset_path field (empty): {self!r}"
            raise ValueError(message)
        normalized_within_dandiset_path = self.job.within_dandiset_path.removesuffix(".nwb")

        pipeline_dir = (
            f"derivatives/{_dandiset_derivatives_relative_dir(self.job.dandiset_id)}"
            f"/{normalized_within_dandiset_path}/pipeline-{self.job.pipeline}"
        )
        return f"{pipeline_dir}/{self.job.job_id}"

    def resolve_capsule_path(self, asset_paths: Collection[str], /) -> str:
        """
        Resolve the capsule path (relative to the Dandiset root) for this entry against a
        known set of remote asset paths, e.g. the keys of
        :attr:`~dandi_compute_code.dandiset.AssetsJsonldMetadata.path_to_asset_metadata`.

        Falls back to searching every ``pipeline-*`` directory in the Dandiset for this
        entry's job ID, which covers a recorded ``within_dandiset_path`` that does not match the
        remote layout. This works purely from DANDI metadata, without a local Dandiset
        clone.

        Parameters
        ----------
        asset_paths : collections.abc.Collection of str
            Asset paths (POSIX strings) for the Dandiset this entry's capsule
            lives in.
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

    @classmethod
    def from_dict(cls, data: dict, /) -> JobCapsule:
        """Construct from a raw entry dict (as produced by :meth:`to_dict`)."""
        job = JobInfo(
            job_id=data["job_id"],
            dandiset_id=data["dandiset_id"],
            within_dandiset_path=data["within_dandiset_path"],
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
            status=_coerce_status(data.get("status")),
            created_at=data.get("created_at"),
            job_submission_time=data.get("job_submission_time"),
            job_completion_time=data.get("job_completion_time"),
            dataset_description_path=dict(data.get("dataset_description_path") or {}),
            output_paths=dict(data.get("output_paths") or {}),
            log_paths=dict(data.get("log_paths") or {}),
        )

    def to_dict(self) -> dict:
        """
        Serialise back to the flat dict format underlying :meth:`to_tsv_row`.

        The two duration fields (``queue_wait_seconds`` and ``run_duration_seconds``) are
        derived from the timestamps rather than stored, so :meth:`from_dict` ignores them.
        """
        return {
            **self.job.to_dict(),
            "content_id": self.content_id,
            "asset_size_bytes": self.asset_size_bytes,
            "status": self.status,
            "dataset_description_path": self.dataset_description_path,
            "output_paths": self.output_paths,
            "log_paths": self.log_paths,
            "created_at": self.created_at,
            "job_submission_time": self.job_submission_time,
            "job_completion_time": self.job_completion_time,
            "queue_wait_seconds": self.queue_wait_seconds,
            "run_duration_seconds": self.run_duration_seconds,
        }

    def to_tsv_row(self) -> dict[str, str]:
        """
        Flatten this entry to a single ``jobs.tsv`` row.

        Every value from :meth:`to_dict` is coerced to a plain string, and ``None`` becomes
        an empty cell. The path mappings (``dataset_description_path``, ``output_paths``,
        ``log_paths``) are not part of the row. They are written to ``paths.tsv`` by
        :meth:`to_paths_tsv_rows`.
        """
        raw = self.to_dict()
        row: dict[str, str] = {}
        for field_name in _JOBS_TSV_FIELD_NAMES:
            value = raw[field_name]
            row[field_name] = "" if value is None else str(value)
        return row

    def to_paths_tsv_rows(self) -> list[dict[str, str]]:
        """
        Flatten this entry's path mappings to ``paths.tsv`` rows, one per asset path.

        Each row carries the ``job_id`` linking it back to this entry's ``jobs.tsv`` row. The
        mapping a path came from is not recorded, since the path itself tells them apart.
        """
        rows = [
            {"job_id": self.job.job_id, "path": path, "content_id": content_id}
            for field_name in _PATH_FIELD_NAMES
            for path, content_id in sorted(getattr(self, field_name).items())
        ]
        return rows

    @classmethod
    def from_tsv_row(cls, row: dict[str, str], /) -> JobCapsule:
        """
        Construct from a single ``jobs.tsv`` row (the inverse of :meth:`to_tsv_row`).

        Reverses the coercions applied by :meth:`to_tsv_row`: empty cells become
        ``None`` and ``asset_size_bytes`` is parsed back to ``int``.

        The path mappings are left empty, since they live in ``paths.tsv`` and are attached
        by :meth:`~._pipeline_queue.PipelineQueue.from_tsv`. A table written before they
        moved there still carries them as JSON columns, and those are read back when present.

        The derived duration columns are not read back -- they are recomputed from the
        timestamps. ``job_submission_time`` is read leniently so that tables written
        before that column existed still parse, and a ``status`` cell that is empty or
        holds an unrecognised value falls back to ``"unknown"``.
        """
        job = JobInfo(
            job_id=row["job_id"],
            dandiset_id=row["dandiset_id"],
            within_dandiset_path=row["within_dandiset_path"],
            pipeline=row["pipeline"],
            version=row["version"],
            params=row["params"],
            config=row["config"],
            codebase=row["codebase"],
        )

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
            status=_coerce_status(row.get("status")),
            created_at=_parse_optional_str(row["created_at"]),
            job_submission_time=_parse_optional_str(row.get("job_submission_time", "")),
            job_completion_time=_parse_optional_str(row["job_completion_time"]),
            dataset_description_path=_parse_json_dict(row.get("dataset_description_path", "")),
            output_paths=_parse_json_dict(row.get("output_paths", "")),
            log_paths=_parse_json_dict(row.get("log_paths", "")),
        )
