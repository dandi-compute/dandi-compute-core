"""
``JobInfo`` — the immutable identity of one job capsule.

Kept in its own module so both :mod:`._pipeline_queue` and its private helpers
(:mod:`._queue_utils`) can depend on it without coupling to each other.
"""

from __future__ import annotations

from dataclasses import dataclass

import beartype


@beartype.beartype
@dataclass(frozen=True)
class JobInfo:
    """Immutable identity of one job capsule."""

    #: The ``job-{YYMMDD}{hash}`` directory name of the capsule.
    job_id: str

    dandiset_id: str
    dandi_path: str
    pipeline: str
    version: str
    params: str
    config: str
    codebase: str

    def to_dict(self) -> dict[str, object]:
        """Serialise the identity fields to a plain dict."""
        return {
            "job_id": self.job_id,
            "dandiset_id": self.dandiset_id,
            "dandi_path": self.dandi_path,
            "pipeline": self.pipeline,
            "version": self.version,
            "params": self.params,
            "config": self.config,
            "codebase": self.codebase,
        }
