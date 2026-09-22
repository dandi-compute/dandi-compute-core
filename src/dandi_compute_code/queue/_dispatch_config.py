"""
Per-pipeline settings for the SLURM array dispatcher.

Every pipeline is run on the cluster by exactly one array job. This module reads that
array job's settings out of the packaged pipeline configuration, filling in defaults for
a pipeline that declares no ``dispatch`` block of its own.

Only the queue limits are configurable. An array task runs its capsule's ``submit.sh``
directly, so the task's allocation is the one the capsule actually gets. Those resource
requests are therefore read back out of the pipeline's own submission template rather than
configured a second time here, which keeps the template the single place they are written.
"""

from __future__ import annotations

import dataclasses
import logging
import pathlib

from ._capsule_resources import CapsuleResources
from ._globals import _DISPATCH_JOB_NAME_PREFIX, _DISPATCH_JOB_NAME_SANITIZE_RE, _SBATCH_DIRECTIVE_RE
from ..aind_ephys_pipeline._globals import _RAW_TEMPLATE_FILE_PATH as _AIND_TEMPLATE_FILE_PATH
from ..lfp_pipeline._globals import _RAW_TEMPLATE_FILE_PATH as _LFP_TEMPLATE_FILE_PATH

_log = logging.getLogger(__name__)

#: How many array tasks SLURM may run at once, for a pipeline that does not set its own.
_DEFAULT_MAX_CONCURRENT = 2
#: How many capsules one array may hold, kept well inside the usual SLURM ``MaxArraySize``.
_DEFAULT_MAX_ARRAY_TASKS = 500

#: Fallback requests for a pipeline whose submission template cannot be read. Deliberately
#: generous, since under-provisioning an array task means its capsule is killed mid-run.
_FALLBACK_PARTITION = "mit_normal"
_FALLBACK_MEMORY = "16GB"
_FALLBACK_CPUS_PER_TASK = 1
_FALLBACK_TIME_LIMIT = "48:00:00"

#: Submission template each pipeline's capsules are rendered from.
_PIPELINE_TEMPLATE_FILE_PATHS: dict[str, pathlib.Path] = {
    "aind+ephys": _AIND_TEMPLATE_FILE_PATH,
    "lfp": _LFP_TEMPLATE_FILE_PATH,
}


def _read_template_resources(pipeline: str, /) -> dict[str, str]:
    """
    The ``#SBATCH`` resource directives written into *pipeline*'s submission template.

    Only the resource directives are returned. ``--job-name`` and ``--output`` are left out
    because the array job carries its own, and because ``--output`` is the one directive that
    is rendered from a Jinja variable rather than written literally.

    Returns an empty mapping for a pipeline with no packaged template, or one that cannot be
    read, which leaves the caller on its fallbacks.
    """
    template_file_path = _PIPELINE_TEMPLATE_FILE_PATHS.get(pipeline)
    if template_file_path is None:
        _log.warning("Pipeline %s has no packaged submission template; using fallback requests", pipeline)
        return {}

    try:
        template = template_file_path.read_text()
    except OSError as exception:
        _log.warning("Unable to read the submission template for %s: %s", pipeline, exception)
        return {}

    wanted = {"mem", "cpus-per-task", "partition", "time"}
    resources = {
        match.group("name"): match.group("value")
        for match in _SBATCH_DIRECTIVE_RE.finditer(template)
        if match.group("name") in wanted
    }
    return resources


@dataclasses.dataclass(frozen=True)
class DispatchConfig:
    """
    The settings of one pipeline's array dispatcher.

    The two limits are configured. The resource requests are not: an array task runs its
    capsule's ``submit.sh`` directly, so they are read back out of the pipeline's own
    submission template by :meth:`from_pipeline_config` to match what the capsule asks for.
    """

    pipeline: str
    max_concurrent: int = _DEFAULT_MAX_CONCURRENT
    max_array_tasks: int | None = _DEFAULT_MAX_ARRAY_TASKS
    partition: str = _FALLBACK_PARTITION
    memory: str = _FALLBACK_MEMORY
    cpus_per_task: int = _FALLBACK_CPUS_PER_TASK
    time_limit: str = _FALLBACK_TIME_LIMIT

    def __post_init__(self) -> None:
        if self.max_concurrent < 1:
            message = f"max_concurrent must be at least 1 for pipeline '{self.pipeline}', got {self.max_concurrent}."
            raise ValueError(message)
        if self.max_array_tasks is not None and self.max_array_tasks < 1:
            message = f"max_array_tasks must be at least 1 for pipeline '{self.pipeline}', got {self.max_array_tasks}."
            raise ValueError(message)
        if self.cpus_per_task < 1:
            message = f"cpus_per_task must be at least 1 for pipeline '{self.pipeline}', got {self.cpus_per_task}."
            raise ValueError(message)

    @classmethod
    def from_pipeline_config(
        cls,
        *,
        pipeline: str,
        pipeline_config: dict,
        max_concurrent: int | None = None,
    ) -> DispatchConfig:
        """
        Read *pipeline*'s dispatcher settings out of a loaded pipeline configuration.

        The queue limits come from the configuration, and any the pipeline does not declare
        fall back to this module's default, so a pipeline with no ``dispatch`` block still
        dispatches. The resource requests come from the pipeline's own submission template
        instead, so that an array task is allocated exactly what the capsule it runs asks for.

        :param pipeline: The pipeline name as it appears in the pipeline configuration.
        :param pipeline_config: A loaded pipeline configuration, as returned by
            :meth:`~dandi_compute_code.queue.QueueState.load_pipeline_config`.
        :param max_concurrent: Overrides the configured concurrency limit when given.
        :raises ValueError: If *pipeline* is not present in *pipeline_config*.
        """
        pipelines = pipeline_config.get("pipelines", {})
        if pipeline not in pipelines:
            configured = list(pipelines.keys())
            message = f"Pipeline '{pipeline}' is not configured. Configured pipelines are: {configured}."
            raise ValueError(message)

        dispatch = pipelines[pipeline].get("dispatch") or {}
        configured_max_concurrent = max_concurrent if max_concurrent is not None else dispatch.get("max_concurrent")
        resources = _read_template_resources(pipeline)
        cpus_per_task = resources.get("cpus-per-task")
        dispatch_config = cls(
            pipeline=pipeline,
            max_concurrent=configured_max_concurrent or _DEFAULT_MAX_CONCURRENT,
            # An explicit null means no upper bound, which is why this reads the key rather
            # than falling back on a falsy value the way max_concurrent does.
            max_array_tasks=dispatch.get("max_array_tasks", _DEFAULT_MAX_ARRAY_TASKS),
            partition=resources.get("partition") or _FALLBACK_PARTITION,
            memory=resources.get("mem") or _FALLBACK_MEMORY,
            cpus_per_task=int(cpus_per_task) if cpus_per_task is not None else _FALLBACK_CPUS_PER_TASK,
            time_limit=resources.get("time") or _FALLBACK_TIME_LIMIT,
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

    def template_resources(self) -> CapsuleResources:
        """
        These settings expressed as a capsule's resource request.

        Capsules are grouped by what they ask SLURM for, and this is the group a capsule whose
        own script could not be read belongs to.
        """
        resources = CapsuleResources(
            memory=self.memory,
            cpus_per_task=self.cpus_per_task,
            partition=self.partition,
            time_limit=self.time_limit,
        )
        return resources

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
