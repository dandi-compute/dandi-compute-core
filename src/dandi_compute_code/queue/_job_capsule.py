"""
``JobCapsule`` — one row of ``state.tsv``: a job's identity plus its status.

Kept in its own module so the typed row model stays separable from
:mod:`._pipeline_queue`, which only containerises and round-trips these rows.
"""

from __future__ import annotations

import json
import pathlib
from collections.abc import Collection
from dataclasses import dataclass, field

from ._job_info import JobInfo
from ..dandiset._globals import _dandiset_derivatives_relative_dir

#: Column order for the ``state.tsv`` table -- matches :meth:`JobCapsule.to_dict` field order.
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


@dataclass
class JobCapsule:
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
    def from_dict(cls, data: dict, /) -> JobCapsule:
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
    def from_tsv_row(cls, row: dict[str, str], /) -> JobCapsule:
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
