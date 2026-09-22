"""
Array dispatch — one SLURM array job per pipeline, owning every run of that pipeline.

Capsules used to be submitted one ``sbatch`` at a time, with concurrency held down by
counting running jobs before each submission. That made every cron invocation a scheduler
of its own, racing the others. Here a pipeline's pending capsules are placed in a single
array job instead. SLURM holds the queue, the array's ``%n`` throttle holds the
concurrency limit, and each array task runs one capsule.

A dispatcher is identified on the cluster by its job name, so a pipeline whose array is
still working through its tasks is left alone rather than dispatched a second time.
"""

from __future__ import annotations

import dataclasses
import datetime
import logging
import pathlib
import subprocess
from typing import Literal

from ._dispatch_config import DispatchConfig
from ._globals import _ACTIVE_SLURM_JOB_STATES, _SBATCH_JOB_ID_RE
from ._handle_template import generate_array_dispatch_script

_log = logging.getLogger(__name__)

#: Name of the manifest listing the capsules an array covers, one per line.
_MANIFEST_FILE_NAME = "manifest.txt"
#: Name of the generated array dispatch script.
_DISPATCH_SCRIPT_FILE_NAME = "dispatch.sh"

DispatchStatus = Literal["dispatched", "no-pending", "dispatcher-active"]


@dataclasses.dataclass(frozen=True)
class DispatchResult:
    """The outcome of one pipeline's dispatch attempt."""

    pipeline: str
    status: DispatchStatus
    task_count: int = 0
    array_job_id: str | None = None
    dispatch_directory: pathlib.Path | None = None

    def summary(self) -> str:
        """A single human-readable line describing this outcome."""
        if self.status == "no-pending":
            return f"{self.pipeline}: no capsules are waiting to be submitted."
        if self.status == "dispatcher-active":
            return f"{self.pipeline}: the dispatcher is already running; its array has not been exhausted yet."
        noun = "capsule" if self.task_count == 1 else "capsules"
        return f"{self.pipeline}: dispatched {self.task_count} {noun} as array job {self.array_job_id}."


def _pending_code_dirs_for_pipeline(*, pipeline: str, code_dir_paths: list[str]) -> list[str]:
    """
    Select the capsule ``code`` directories belonging to *pipeline*.

    A capsule path spells its pipeline out as a ``pipeline-<name>`` segment, which is what is
    matched here.
    """
    pipeline_segment = f"/pipeline-{pipeline}/"
    selected = [code_dir_path for code_dir_path in code_dir_paths if pipeline_segment in code_dir_path]
    return selected


def _active_dispatcher_job_ids(job_name: str, /) -> list[str]:
    """
    The IDs of this user's active SLURM jobs carrying *job_name*.

    A non-empty result means a dispatcher still owns an array, whether it is running tasks or
    holding them back behind the concurrency throttle.

    :raises RuntimeError: If the ``squeue`` invocation exits non-zero and writes to standard
        error.
    """
    command = [
        "squeue",
        "--me",
        "--noheader",
        "--name",
        job_name,
        "--states",
        _ACTIVE_SLURM_JOB_STATES,
        "--format=%i",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)
    if result.stderr:
        _log.warning(result.stderr)

    job_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return job_ids


def _submit_array_job(script_file_path: pathlib.Path, /) -> str:
    """
    Submit a dispatch script with ``sbatch`` and return the array job ID it reports.

    :raises RuntimeError: If ``sbatch`` exits non-zero, or if its output carries no job ID.
    """
    command = ["sbatch", str(script_file_path.absolute())]
    result = subprocess.run(command, capture_output=True, text=True)
    _log.info("sbatch returned code %d; stdout: %s", result.returncode, result.stdout)
    if result.returncode != 0:
        _log.warning("sbatch stdout: %s\nstderr: %s", result.stdout, result.stderr)
        message = "sbatch submission of the array dispatcher failed - please check the logs for details."
        raise RuntimeError(message)

    match = _SBATCH_JOB_ID_RE.search(result.stdout)
    if match is None:
        message = f"Unable to read an array job ID out of the sbatch output: {result.stdout!r}"
        raise RuntimeError(message)
    array_job_id = match.group("job_id")
    return array_job_id


def dispatch_pipeline_jobs(
    *,
    pipeline: str,
    code_dir_paths: list[str],
    processing_directory: pathlib.Path,
    dispatch_config: DispatchConfig,
    dandiset_id: str,
    test: bool = False,
) -> DispatchResult:
    """
    Place *pipeline*'s pending capsules in a single SLURM array job.

    The capsules belonging to *pipeline* are taken out of *code_dir_paths*, written to a
    manifest, and covered by one array whose throttle is this pipeline's concurrency limit.
    Each array task reads its capsule out of the manifest by task index, downloads it,
    claims it with a submitted marker, and runs it.

    Nothing is dispatched while a dispatcher for *pipeline* is still active on the cluster.
    That array already holds the pending tasks, so a second one would only duplicate them.
    Capsules formed after it was submitted wait for it to be exhausted and go out with the
    next dispatch.

    :param pipeline: The pipeline to dispatch.
    :param code_dir_paths: Capsule ``code`` directory paths (relative to the Dandiset root)
        awaiting submission, across all pipelines. See
        :meth:`~dandi_compute_code.queue.QueueState.pending_code_dirs`.
    :param processing_directory: Directory the dispatch directory is created in. It holds the
        manifest, the dispatch script, and the array's logs, so it has to remain readable
        from the compute nodes for as long as the array lives.
    :param dispatch_config: This pipeline's dispatcher settings.
    :param dandiset_id: The Dandiset the capsules are downloaded from and uploaded back to.
    :param test: When ``True``, each array task leaves its working tree on disk for debugging.
    :returns: What was dispatched, or why nothing was.
    :rtype: DispatchResult
    :raises RuntimeError: If ``squeue`` or ``sbatch`` fails.
    """
    pipeline_code_dir_paths = _pending_code_dirs_for_pipeline(pipeline=pipeline, code_dir_paths=code_dir_paths)
    if not pipeline_code_dir_paths:
        _log.info("No pending capsules for pipeline %s", pipeline)
        return DispatchResult(pipeline=pipeline, status="no-pending")

    job_name = dispatch_config.job_name
    active_job_ids = _active_dispatcher_job_ids(job_name)
    if active_job_ids:
        _log.info(
            "Dispatcher %s is still active as job(s) %s; leaving its array to be exhausted",
            job_name,
            ", ".join(active_job_ids),
        )
        return DispatchResult(pipeline=pipeline, status="dispatcher-active", array_job_id=active_job_ids[0])

    dispatched_code_dir_paths = pipeline_code_dir_paths[: dispatch_config.max_array_tasks]
    if len(dispatched_code_dir_paths) < len(pipeline_code_dir_paths):
        _log.info(
            "Holding %d capsule(s) for pipeline %s back until the next dispatch; an array covers at most %d",
            len(pipeline_code_dir_paths) - len(dispatched_code_dir_paths),
            pipeline,
            dispatch_config.max_array_tasks,
        )

    now = datetime.datetime.now()
    timestamp = f"{now.year:04d}{now.month:02d}{now.day:02d}-{now.hour:02d}{now.minute:02d}{now.second:02d}"
    dispatch_directory = processing_directory / f"{job_name}-{timestamp}"
    dispatch_directory.mkdir(parents=True, exist_ok=True)

    manifest_file_path = dispatch_directory / _MANIFEST_FILE_NAME
    manifest_file_path.write_text("".join(f"{code_dir_path}\n" for code_dir_path in dispatched_code_dir_paths))

    script_file_path = dispatch_directory / _DISPATCH_SCRIPT_FILE_NAME
    generate_array_dispatch_script(
        script_file_path=script_file_path,
        job_name=job_name,
        dispatch_directory=str(dispatch_directory.absolute()),
        memory=dispatch_config.memory,
        cpus_per_task=dispatch_config.cpus_per_task,
        partition=dispatch_config.partition,
        time_limit=dispatch_config.time_limit,
        array_specification=dispatch_config.array_specification(len(dispatched_code_dir_paths)),
        dandiset_id=dandiset_id,
        manifest_file_path=str(manifest_file_path.absolute()),
        keep_task_directory=test,
    )

    _log.info(
        "Dispatching %d capsule(s) for pipeline %s as array %s (at most %d at a time)",
        len(dispatched_code_dir_paths),
        pipeline,
        job_name,
        dispatch_config.max_concurrent,
    )
    array_job_id = _submit_array_job(script_file_path)

    result = DispatchResult(
        pipeline=pipeline,
        status="dispatched",
        task_count=len(dispatched_code_dir_paths),
        array_job_id=array_job_id,
        dispatch_directory=dispatch_directory,
    )
    return result
