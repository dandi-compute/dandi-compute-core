"""
Per-pipeline settings for the SLURM array dispatcher.

Every pipeline is run on the cluster by exactly one array job. This module reads that
array job's settings out of the packaged pipeline configuration, filling in defaults for
a pipeline that declares no ``dispatch`` block of its own.

Only the two queue limits are configurable. The array task's own resource requests are
pinned in the dispatch template, since the task does nothing but run the capsule's
``submit.sh``.
"""

from __future__ import annotations

import dataclasses

from ._globals import _DISPATCH_JOB_NAME_PREFIX, _DISPATCH_JOB_NAME_SANITIZE_RE

#: How many array tasks SLURM may run at once, for a pipeline that does not set its own.
_DEFAULT_MAX_CONCURRENT = 2
#: How many capsules one array may hold, kept well inside the usual SLURM ``MaxArraySize``.
_DEFAULT_MAX_ARRAY_TASKS = 500


@dataclasses.dataclass(frozen=True)
class DispatchConfig:
    """
    The settings of one pipeline's array dispatcher.

    Both fields are queue limits rather than resource requests. What each array task asks
    SLURM for is pinned in the dispatch template and is not configurable per pipeline.
    """

    pipeline: str
    max_concurrent: int = _DEFAULT_MAX_CONCURRENT
    max_array_tasks: int | None = _DEFAULT_MAX_ARRAY_TASKS

    def __post_init__(self) -> None:
        if self.max_concurrent < 1:
            message = f"max_concurrent must be at least 1 for pipeline '{self.pipeline}', got {self.max_concurrent}."
            raise ValueError(message)
        if self.max_array_tasks is not None and self.max_array_tasks < 1:
            message = f"max_array_tasks must be at least 1 for pipeline '{self.pipeline}', got {self.max_array_tasks}."
            raise ValueError(message)

    @classmethod
    def from_queue_config(
        cls,
        *,
        pipeline: str,
        queue_config: dict,
        max_concurrent: int | None = None,
    ) -> DispatchConfig:
        """
        Read *pipeline*'s dispatcher settings out of a loaded queue configuration.

        Any setting the pipeline does not declare falls back to this module's default, so a
        pipeline with no ``dispatch`` block still dispatches.

        :param pipeline: The pipeline name as it appears in the queue configuration.
        :param queue_config: A loaded queue configuration, as returned by
            :meth:`~dandi_compute_code.queue.QueueState.load_queue_config`.
        :param max_concurrent: Overrides the configured concurrency limit when given.
        :raises ValueError: If *pipeline* is not present in *queue_config*.
        """
        pipelines = queue_config.get("pipelines", {})
        if pipeline not in pipelines:
            configured = list(pipelines.keys())
            message = f"Pipeline '{pipeline}' is not configured. Configured pipelines are: {configured}."
            raise ValueError(message)

        dispatch = pipelines[pipeline].get("dispatch") or {}
        configured_max_concurrent = max_concurrent if max_concurrent is not None else dispatch.get("max_concurrent")
        dispatch_config = cls(
            pipeline=pipeline,
            max_concurrent=configured_max_concurrent or _DEFAULT_MAX_CONCURRENT,
            # An explicit null means no upper bound, which is why this reads the key rather
            # than falling back on a falsy value the way max_concurrent does.
            max_array_tasks=dispatch.get("max_array_tasks", _DEFAULT_MAX_ARRAY_TASKS),
        )
        return dispatch_config

    @property
    def job_name(self) -> str:
        """
        The SLURM job name this pipeline's array dispatcher carries.

        The name is what identifies a live dispatcher, so it has to survive a round trip
        through SLURM unchanged. Characters a pipeline name may carry but a job name should
        not (``aind+ephys``) are folded to a hyphen.
        """
        sanitized_pipeline = _DISPATCH_JOB_NAME_SANITIZE_RE.sub("-", self.pipeline).strip("-")
        job_name = f"{_DISPATCH_JOB_NAME_PREFIX}-{sanitized_pipeline}"
        return job_name

    def array_specification(self, task_count: int, /) -> str:
        """
        The ``--array`` specification covering *task_count* capsules.

        Array indices are one-based so that a task index reads directly as a line number in
        the manifest. The ``%n`` suffix is what holds the cluster to this pipeline's
        concurrency limit, leaving the remaining tasks queued in SLURM rather than resubmitted
        by a later dispatch.
        """
        if task_count < 1:
            message = f"An array needs at least one task, got {task_count}."
            raise ValueError(message)
        specification = f"1-{task_count}%{self.max_concurrent}"
        return specification
