"""
Array dispatch — one SLURM array job per pipeline, owning every run of that pipeline.

Capsules used to be submitted one ``sbatch`` at a time, with concurrency held down by
counting running jobs before each submission. That made every cron invocation a scheduler
of its own, racing the others. Here a pipeline's pending capsules are placed in a single
array job instead. SLURM holds the queue, the array's ``%n`` throttle holds the
concurrency limit, and each array task runs one capsule.

A dispatcher is identified on the cluster by its job name, so a pipeline whose array is
still working through its tasks is left alone rather than dispatched a second time.

Every dispatcher keeps its record in one central log directory per pipeline, under the
base directory's ``processing/derivatives/logs/``. The minted manifests, the generated array scripts and the
array tasks' own output all land there and outlive the dispatch directory, which only holds
the tasks' working trees and is removed once the array is finished with it.
"""

from __future__ import annotations

import dataclasses
import datetime
import logging
import pathlib
import shutil
import subprocess
from typing import Literal

import beartype

from ._capsule_resources import CapsuleResources
from ._dispatch_config import DispatchConfig
from ._globals import (
    _ACTIVE_SLURM_JOB_STATES,
    _DISPATCH_DIRECTORY_RE,
    _DISPATCH_DIRECTORY_TIMESTAMP_FORMAT,
    _DISPATCH_LOG_DIRECTORY_RELATIVE_PATH,
    _SBATCH_JOB_ID_RE,
)
from ._handle_template import generate_array_dispatch_script
from .._base_directory import _processing_directory

_log = logging.getLogger(__name__)

#: Name of the manifest listing the capsules one array covers, one per line.
_MANIFEST_FILE_NAME_TEMPLATE = "{timestamp}-manifest-{index}.txt"
#: Name of one generated array dispatch script.
_DISPATCH_SCRIPT_FILE_NAME_TEMPLATE = "{timestamp}-dispatch-{index}.sh"
#: Name of one array task's output, with SLURM filling in the array job and task IDs.
_DISPATCH_LOG_FILE_NAME_TEMPLATE = "{timestamp}-dispatch-{index}-%A_%a.log"

DispatchStatus = Literal["dispatched", "no-pending", "dispatcher-active"]


@beartype.beartype
@dataclasses.dataclass(frozen=True)
class DispatchedArray:
    """One array job, covering the capsules of a pipeline that request the same resources."""

    array_job_id: str
    task_count: int
    resources: CapsuleResources
    max_concurrent: int
    manifest_file_path: pathlib.Path
    script_file_path: pathlib.Path

    def summary(self) -> str:
        """This array's size, throttle and requests on one line."""
        noun = "capsule" if self.task_count == 1 else "capsules"
        line = (
            f"array {self.array_job_id}: {self.task_count} {noun} requesting {self.resources.describe()}"
            f", at most {self.max_concurrent} at a time"
        )
        return line


@beartype.beartype
@dataclasses.dataclass(frozen=True)
class DispatchResult:
    """The outcome of one pipeline's dispatch attempt."""

    pipeline: str
    status: DispatchStatus
    arrays: tuple[DispatchedArray, ...] = ()
    active_job_ids: tuple[str, ...] = ()
    dispatch_directory: pathlib.Path | None = None
    log_directory: pathlib.Path | None = None

    @property
    def task_count(self) -> int:
        """How many capsules this dispatch placed in arrays, across every resource group."""
        return sum(array.task_count for array in self.arrays)

    def summary(self) -> str:
        """A single human-readable line describing this outcome."""
        if self.status == "no-pending":
            return f"{self.pipeline}: no capsules are waiting to be submitted."
        if self.status == "dispatcher-active":
            return f"{self.pipeline}: the dispatcher is already running; its array has not been exhausted yet."
        noun = "capsule" if self.task_count == 1 else "capsules"
        if len(self.arrays) == 1:
            return f"{self.pipeline}: dispatched {self.task_count} {noun} as array job {self.arrays[0].array_job_id}."
        return (
            f"{self.pipeline}: dispatched {self.task_count} {noun} as {len(self.arrays)} array jobs, "
            f"one per distinct set of requested resources."
        )

    def summary_lines(self) -> list[str]:
        """
        The outcome plus one line per array, so a dispatch shows what each group asked for.

        Capsules are grouped by the resources their submission scripts request, and the
        grouping is what decides how the pipeline's concurrency limit is shared out, so both
        are worth seeing rather than having to read back the generated scripts.
        """
        lines = [self.summary()]
        lines.extend(f"  {array.summary()}" for array in self.arrays)
        if self.log_directory is not None:
            lines.append(f"  logs, manifests and scripts: {self.log_directory}")
        return lines


@beartype.beartype
def clean_dispatch_directories(
    *,
    base_directory: pathlib.Path,
    minimum_age_hours: float = 24.0,
) -> list[pathlib.Path]:
    """
    Remove dispatch directories whose arrays are finished with them.

    A dispatch directory holds the working trees of its array tasks, so removing one while its
    array is still working would pull the ground out from under every task still running. Two
    things guard against that. A directory is kept while its pipeline still has a dispatcher
    on the cluster, and it is kept until it is at least *minimum_age_hours* old, which covers
    the window between submitting an array and SLURM reporting it.

    Only directories named like a dispatch directory are considered, so anything else sharing
    the ``processing/`` directory of *base_directory* is left alone. That includes the central
    ``derivatives/logs/`` directory, which keeps every dispatcher's manifests, scripts and output
    after its dispatch directory is gone.

    Parameters
    ----------
    base_directory : pathlib.Path
        The structured base directory. Dispatch directories are created in its
        ``processing/`` directory.
    minimum_age_hours : float
        Leave directories formed more recently than this alone.

    Returns
    -------
    list of pathlib.Path
        The dispatch directories that were removed.

    Raises
    ------
    RuntimeError
        If ``squeue`` fails, since a live dispatcher cannot be ruled out.
    """
    processing_directory = _processing_directory(base_directory)
    if not processing_directory.is_dir():
        message = f"The processing directory does not exist or is not a directory: {processing_directory}"
        raise NotADirectoryError(message)

    now = datetime.datetime.now()
    active_by_job_name: dict[str, bool] = {}
    removed: list[pathlib.Path] = []

    for candidate in sorted(processing_directory.iterdir()):
        if not candidate.is_dir():
            continue
        match = _DISPATCH_DIRECTORY_RE.fullmatch(candidate.name)
        if match is None:
            continue

        try:
            formed_at = datetime.datetime.strptime(match.group("timestamp"), _DISPATCH_DIRECTORY_TIMESTAMP_FORMAT)
        except ValueError:
            _log.warning("Unreadable timestamp on dispatch directory %s; leaving it alone", candidate)
            continue

        age_hours = (now - formed_at).total_seconds() / 3600.0
        if age_hours < minimum_age_hours:
            _log.info("Keeping %s; it is %.1f hours old", candidate.name, age_hours)
            continue

        job_name = match.group("job_name")
        if job_name not in active_by_job_name:
            active_by_job_name[job_name] = bool(_active_dispatcher_job_ids(job_name))
        if active_by_job_name[job_name]:
            _log.info("Keeping %s; its dispatcher %s is still active", candidate.name, job_name)
            continue

        shutil.rmtree(candidate)
        _log.info("Removed dispatch directory %s", candidate)
        removed.append(candidate)

    return removed


@beartype.beartype
def _pending_code_dirs_for_pipeline(*, pipeline: str, code_dir_paths: list[str]) -> list[str]:
    """
    Select the capsule ``code`` directories belonging to *pipeline*.

    A capsule path spells its pipeline out as a ``pipeline-<name>`` segment, which is what is
    matched here.
    """
    pipeline_segment = f"/pipeline-{pipeline}/"
    selected = [code_dir_path for code_dir_path in code_dir_paths if pipeline_segment in code_dir_path]
    return selected


@beartype.beartype
def _group_by_requested_resources(
    *,
    code_dir_paths: list[str],
    capsule_resources: dict[str, CapsuleResources],
    fallback: CapsuleResources,
) -> dict[CapsuleResources, list[str]]:
    """
    Split capsules into one group per distinct set of requested resources.

    An array carries a single ``#SBATCH`` header, so capsules asking for different things
    cannot share one without the smaller header truncating the larger job. A capsule whose
    own script could not be read falls into the *fallback* group, which is its pipeline's
    template.

    Groups keep the order their first capsule appeared in, so dispatch stays deterministic.
    """
    groups: dict[CapsuleResources, list[str]] = {}
    for code_dir_path in code_dir_paths:
        resources = capsule_resources.get(code_dir_path, fallback)
        groups.setdefault(resources, []).append(code_dir_path)
    return groups


@beartype.beartype
def _active_dispatcher_job_ids(job_name: str, /) -> list[str]:
    """
    The IDs of this user's active SLURM jobs carrying *job_name*.

    A non-empty result means a dispatcher still owns an array, whether it is running tasks or
    holding them back behind the concurrency throttle.

    Raises
    ------
    RuntimeError
        If the ``squeue`` invocation exits non-zero and writes to standard
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


@beartype.beartype
def _submit_array_job(script_file_path: pathlib.Path, /) -> str:
    """
    Submit a dispatch script with ``sbatch`` and return the array job ID it reports.

    Raises
    ------
    RuntimeError
        If ``sbatch`` exits non-zero, or if its output carries no job ID.
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


@beartype.beartype
def dispatch_pipeline_jobs(
    *,
    pipeline: str,
    code_dir_paths: list[str],
    base_directory: pathlib.Path,
    dispatch_config: DispatchConfig,
    dandiset_id: str,
    capsule_resources: dict[str, CapsuleResources] | None = None,
    test: bool = False,
) -> DispatchResult:
    """
    Place *pipeline*'s pending capsules in SLURM array jobs, grouped by requested resources.

    The capsules belonging to *pipeline* are taken out of *code_dir_paths* and grouped by
    what their submission scripts ask SLURM for. Each group gets an array of its own, sized
    for that group, because an array carries a single ``#SBATCH`` header and a task runs its
    capsule directly. Every capsule of a pipeline normally asks for the same thing, so this
    is one array per pipeline in practice. Each array task reads its capsule out of its
    manifest by task index, downloads it, claims it with a submitted marker, and runs it.

    The manifests, the array scripts and the array tasks' output are written to the
    pipeline's central log directory, ``processing/derivatives/logs/<job name>/`` under *base_directory*,
    and are named by when the dispatch was formed. They are kept as the record of what
    each dispatch covered. The per-dispatch directory holds only the tasks' working trees.

    The configured concurrency limit is what the pipeline may run at once in total, so it is
    shared out across the arrays rather than applied to each of them.

    Nothing is dispatched while a dispatcher for *pipeline* is still active on the cluster.
    That array already holds the pending tasks, so a second one would only duplicate them.
    Capsules formed after it was submitted wait for it to be exhausted and go out with the
    next dispatch.

    Parameters
    ----------
    pipeline : str
        The pipeline to dispatch.
    code_dir_paths : list of str
        Capsule ``code`` directory paths (relative to the Dandiset root)
        awaiting submission, across all pipelines. See
        :meth:`~dandi_compute_code.queue.PipelineQueue.pending_code_dirs`.
    base_directory : pathlib.Path
        The structured base directory. The dispatch directory and the central
        log directory are created in its ``processing/`` directory. The array
        tasks read their manifest from the log directory and work in the
        dispatch directory, so both have to remain reachable from the compute
        nodes for as long as the array lives.
    dispatch_config : DispatchConfig
        This pipeline's dispatcher settings.
    dandiset_id : str
        The Dandiset the capsules are downloaded from and uploaded back to.
    capsule_resources : dict of str to CapsuleResources
        What each capsule asks SLURM for, keyed by ``code`` directory path. See
        :func:`~dandi_compute_code.queue.read_capsule_resources`. A capsule
        missing from it is grouped with the pipeline's own template.
    test : bool
        When ``True``, each array task leaves its working tree on disk for
        debugging.

    Returns
    -------
    DispatchResult
        What was dispatched, or why nothing was.

    Raises
    ------
    RuntimeError
        If ``squeue`` or ``sbatch`` fails.
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
        return DispatchResult(pipeline=pipeline, status="dispatcher-active", active_job_ids=tuple(active_job_ids))

    groups = _group_by_requested_resources(
        code_dir_paths=pipeline_code_dir_paths,
        capsule_resources=capsule_resources or {},
        fallback=dispatch_config.template_resources(),
    )
    if len(groups) > 1:
        _log.info(
            "Pipeline %s has capsules asking for %d different sets of resources; dispatching one array each",
            pipeline,
            len(groups),
        )

    # The configured limit is what this pipeline may run at once in total, so it is shared out
    # across the arrays rather than applied to each of them.
    max_concurrent_per_group = max(1, dispatch_config.max_concurrent // len(groups))

    now = datetime.datetime.now()
    timestamp = f"{now.year:04d}{now.month:02d}{now.day:02d}-{now.hour:02d}{now.minute:02d}{now.second:02d}"
    processing_directory = _processing_directory(base_directory)
    dispatch_directory = processing_directory / f"{job_name}-{timestamp}"
    dispatch_directory.mkdir(parents=True, exist_ok=True)
    log_directory = processing_directory / _DISPATCH_LOG_DIRECTORY_RELATIVE_PATH / job_name
    log_directory.mkdir(parents=True, exist_ok=True)

    dispatched_arrays: list[DispatchedArray] = []
    for group_index, (resources, group_code_dir_paths) in enumerate(groups.items(), start=1):
        dispatched_code_dir_paths = group_code_dir_paths[: dispatch_config.max_array_tasks]
        if len(dispatched_code_dir_paths) < len(group_code_dir_paths):
            _log.info(
                "Holding %d capsule(s) for pipeline %s back until the next dispatch; an array covers at most %s",
                len(group_code_dir_paths) - len(dispatched_code_dir_paths),
                pipeline,
                dispatch_config.max_array_tasks,
            )

        manifest_file_path = log_directory / _MANIFEST_FILE_NAME_TEMPLATE.format(timestamp=timestamp, index=group_index)
        manifest_file_path.write_text("".join(f"{code_dir_path}\n" for code_dir_path in dispatched_code_dir_paths))

        script_file_path = log_directory / _DISPATCH_SCRIPT_FILE_NAME_TEMPLATE.format(
            timestamp=timestamp, index=group_index
        )
        log_file_path = log_directory / _DISPATCH_LOG_FILE_NAME_TEMPLATE.format(timestamp=timestamp, index=group_index)
        generate_array_dispatch_script(
            script_file_path=script_file_path,
            job_name=job_name,
            log_file_path=str(log_file_path.absolute()),
            dispatch_directory=str(dispatch_directory.absolute()),
            memory=resources.memory,
            cpus_per_task=resources.cpus_per_task,
            partition=resources.partition,
            time_limit=resources.time_limit,
            array_specification=dataclasses.replace(
                dispatch_config, max_concurrent=max_concurrent_per_group
            ).array_specification(len(dispatched_code_dir_paths)),
            dandiset_id=dandiset_id,
            manifest_file_path=str(manifest_file_path.absolute()),
            signal=resources.signal,
            keep_task_directory=test,
        )

        _log.info(
            "Dispatching %d capsule(s) for pipeline %s as array %s requesting %s / %d CPU / %s / %s"
            " (at most %d at a time)",
            len(dispatched_code_dir_paths),
            pipeline,
            job_name,
            resources.memory,
            resources.cpus_per_task,
            resources.partition,
            resources.time_limit,
            max_concurrent_per_group,
        )
        dispatched_arrays.append(
            DispatchedArray(
                array_job_id=_submit_array_job(script_file_path),
                task_count=len(dispatched_code_dir_paths),
                resources=resources,
                max_concurrent=max_concurrent_per_group,
                manifest_file_path=manifest_file_path,
                script_file_path=script_file_path,
            )
        )

    result = DispatchResult(
        pipeline=pipeline,
        status="dispatched",
        arrays=tuple(dispatched_arrays),
        dispatch_directory=dispatch_directory,
        log_directory=log_directory,
    )
    return result
