import logging
import os
import pathlib

import click

from ._clean_work_directory import clean_work_directory
from ._styled_echo import _styled_echo
from .._configure_logging import _configure_logging
from ..aind_ephys_pipeline import prepare_aind_ephys_job, submit_job
from ..dandiset import move_job_capsule
from ..dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID, _JOB_CAPSULES_DANDISET_ID
from ..jobs import create_job_capsules
from ..queue import TEST_QUEUE_CONTENT_ID, QueueState

logging.basicConfig(level=logging.INFO)


def _require_dandi_api_key() -> None:
    """
    Verify that the ``DANDI_API_KEY`` environment variable is set and non-empty.

    Raises
    ------
    click.ClickException
        If ``DANDI_API_KEY`` is unset or empty.  The CLI catches this and exits
        with a user-facing error message.
    """
    if "DANDI_API_KEY" not in os.environ or not os.environ["DANDI_API_KEY"]:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")


def _require_dandi_devel() -> None:
    """
    Verify that the ``DANDI_DEVEL`` environment variable is set and non-empty.

    Raises
    ------
    click.ClickException
        If ``DANDI_DEVEL`` is unset or empty.
    """
    if "DANDI_DEVEL" not in os.environ or not os.environ["DANDI_DEVEL"]:
        raise click.ClickException("`DANDI_DEVEL` environment variable is not set.")


# dandicompute
@click.group(name="dandicompute")
def _dandicompute_group():
    """Run compute workflows and queue management tasks for DANDI assets."""
    pass


# dandicompute clean [OPTIONS]
@_dandicompute_group.command(name="clean")
@click.option(
    "--directory",
    "directory",
    help="Path to the directory to clean (all contents except 'apptainer_cache' will be deleted).",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _clean_command(directory: pathlib.Path, silent: bool = False) -> None:
    """Remove all files and directories under a work directory except apptainer cache."""
    _configure_logging(silent=silent)
    clean_work_directory(directory=directory)
    if not silent:
        _styled_echo(text="\nWork directory cleaned!", color="green")


# dandicompute submit [OPTIONS]
@_dandicompute_group.command(name="submit")
@click.option(
    "--script",
    "script_file_path",
    help="Path to the submission script file.",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _submit_command(script_file_path: pathlib.Path, silent: bool = False) -> None:
    """Submit a previously prepared pipeline script via sbatch."""
    _configure_logging(silent=silent)
    submit_job(script_file_path=script_file_path)


# dandicompute prepare
@_dandicompute_group.group(name="prepare")
def _prepare_group() -> None:
    """Run preparation workflows that generate submission scripts."""
    pass


# dandicompute prepare aind [OPTIONS]
@_prepare_group.command(name="aind")
@click.option(
    "--test",
    "test",
    help="Create test job capsules for all configured AIND ephys params combinations.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--id",
    "content_id",
    help="The content ID for the data to be processed. Required if --dandiset and --dandipath are not provided.",
    required=False,
    type=None,
)
@click.option(
    "--dandiset",
    "dandiset_id",
    help="The Dandiset ID for the data to be processed (e.g., '000409'). Required if --id is not provided.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--dandipath",
    "dandiset_path",
    help="The local path to the Dandiset data to be processed. Required if --id is not provided (ignored with --test).",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--config",
    "config_key",
    help="Registered configuration key to use.",
    required=False,
    type=str,
    default="default",
)
@click.option(
    "--pipeline",
    "pipeline_directory",
    help="Local path to the AIND pipeline repository.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--version",
    "pipeline_version",
    help="The version of the pipeline to use (ignored with --test).",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--params",
    "parameters_key",
    help="The name of the parameters to use (ignored with --test).",
    required=False,
    type=str,
    default="default",
)
@click.option(
    "--submit",
    help="Automatically submit the job after preparation (ignored with --test).",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress output messages (ignored with --test).",
    required=False,
    is_flag=True,
    default=False,
)
def _prepare_aind_command(
    test: bool = False,
    pipeline_version: str | None = None,
    content_id: str | None = None,
    dandiset_id: str | None = None,
    dandiset_path: pathlib.Path | None = None,
    config_key: str = "default",
    pipeline_directory: pathlib.Path | None = None,
    parameters_key: str = "default",
    submit: bool = False,
    silent: bool = False,
) -> None:
    """Prepare an AIND ephys job, or create test job capsules with --test."""
    _configure_logging(silent=silent)
    if "DANDI_API_KEY" not in os.environ:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")

    if test:
        create_job_capsules(
            content_ids=[TEST_QUEUE_CONTENT_ID],
            pipeline_directory=pipeline_directory,
            config_key=config_key,
        )
        return

    if pipeline_version is None:
        raise click.UsageError("--version is required when not using --test.")

    script_file_path = prepare_aind_ephys_job(
        content_id=content_id,
        dandiset_id=dandiset_id,
        dandiset_path=dandiset_path,
        config_key=config_key,
        pipeline_directory=pipeline_directory,
        pipeline_version=pipeline_version,
        parameters_key=parameters_key,
        silent=silent,
    )

    if script_file_path is None:
        if not silent:
            _styled_echo(text="\nA job capsule already exists for this asset; nothing was prepared.", color="yellow")
        return

    if submit:
        submit_job(script_file_path=script_file_path)

    if silent:
        return

    _styled_echo(text="\nPreparation complete!", color="green")

    if submit and not silent:
        _styled_echo(text=f"\n\nProcessing script at: {script_file_path}\n\n", color="yellow")
        return

    _styled_echo(
        text=f"\n\nTo submit the job, run:\n\n\tdandicompute submit --script {script_file_path}\n\n",
        color="yellow",
    )


# dandicompute jobs
@_dandicompute_group.group(name="jobs")
def _jobs_group() -> None:
    """Create new job capsules for qualifying assets."""
    pass


# dandicompute jobs create [OPTIONS]
@_jobs_group.command(name="create")
@click.option(
    "--pipeline",
    "only_pipeline",
    help="Create capsules only for the named pipeline (e.g. 'lfp' or 'aind+ephys'). "
    "Defaults to all configured pipelines.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--config",
    "config_key",
    help="Registered configuration key to use.",
    required=False,
    type=str,
    default="default",
)
@click.option(
    "--limit",
    "limit",
    help="Create at most N job capsules in total. Defaults to no limit.",
    required=False,
    type=click.IntRange(min=1),
    default=None,
)
@click.option(
    "--latest",
    "force_latest_versions",
    help="Create a capsule for every qualifying asset against the latest locally available "
    "pipeline and codebase versions, even where one already exists.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _jobs_create_command(
    only_pipeline: str | None = None,
    config_key: str = "default",
    limit: int | None = None,
    force_latest_versions: bool = False,
    silent: bool = False,
) -> None:
    """Create a job capsule for every qualifying asset that does not have one yet."""
    _configure_logging(silent=silent)
    _require_dandi_api_key()

    created_count = create_job_capsules(
        config_key=config_key,
        limit=limit,
        only_pipeline=only_pipeline,
        force_latest_versions=force_latest_versions,
    )
    if not silent:
        noun = "job capsule" if created_count == 1 else "job capsules"
        _styled_echo(text=f"\nCreated {created_count} {noun}.", color="green" if created_count else "yellow")


# dandicompute queue
@_dandicompute_group.group(name="queue")
def _queue_group() -> None:
    """Manage queue ordering, inspection, and execution."""
    pass


# dandicompute queue refresh [OPTIONS]
@_queue_group.command(name="refresh")
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Source (job capsules) Dandiset ID whose state is refreshed.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--archive-dandiset-id",
    "archive_dandiset_id",
    help="Archived (failed runs archive) Dandiset ID whose state is refreshed.",
    required=False,
    type=str,
    default=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--processing",
    "processing_directory",
    help="Directory for the temporary working tree used to write each state.tsv "
    "(defaults to the system temporary location).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--test",
    "test",
    help="Preserve the temporary working tree used to write each state.tsv instead of cleaning it up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _queue_refresh_command(
    dandiset_id: str,
    archive_dandiset_id: str,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
    silent: bool = False,
) -> None:
    """
    Rewrite state.tsv into both Dandisets.

    Ephemerally rebuilds and rewrites derivatives/state.tsv within both the source and
    archived Dandisets themselves (see QueueState.write_dandiset_state_table), so each always
    reflects its current state fetched fresh from its own assets.jsonld.
    """
    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    for target_dandiset_id in (dandiset_id, archive_dandiset_id):
        QueueState.write_dandiset_state_table(
            dandiset_id=target_dandiset_id,
            processing_directory=processing_directory,
            test=test,
        )
        if not silent:
            _styled_echo(text=f"\nWrote derivatives/state.tsv to Dandiset {target_dandiset_id}.", color="green")


# dandicompute queue clean [OPTIONS]
@_queue_group.command(name="clean")
@click.option(
    "--dandiset",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan for queued capsules.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _queue_clean_command(
    dandiset_directory: pathlib.Path,
    silent: bool = False,
) -> None:
    """Delete unsubmitted capsules that are no longer present in the queue."""
    _configure_logging(silent=silent)
    _require_dandi_api_key()

    state = QueueState.from_dandi()
    removed = state.clean_unsubmitted_capsules(dandiset_directory=dandiset_directory)
    if removed:
        if not silent:
            for path in removed:
                _styled_echo(text=f"  Removed: {path}", color="yellow")
            noun = "capsule" if len(removed) == 1 else "capsules"
            _styled_echo(text=f"\nCleaned {len(removed)} unsubmitted {noun}.", color="green")
    elif not silent:
        _styled_echo(text="\nNo unsubmitted capsules found.", color="yellow")


# dandicompute queue stats [OPTIONS]
@_queue_group.command(name="stats")
@click.option(
    "--dandiset",
    "dandiset_directory",
    help="Path to a local dandiset clone used to locate Nextflow timeline reports.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID the aggregate statistics JSON is written into.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--processing",
    "processing_directory",
    help="Directory for the temporary working tree used to write the statistics JSON "
    "(defaults to the system temporary location).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--test",
    "test",
    help="Preserve the temporary working tree used to write the statistics JSON instead of cleaning it up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _queue_stats_command(
    dandiset_directory: pathlib.Path,
    dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Write aggregate queue statistics from the live queue state."""
    _configure_logging(silent=silent)

    state = QueueState.from_dandi(dandiset_id=dandiset_id)
    state.aggregate_statistics(
        dandiset_directory=dandiset_directory,
        dandiset_id=dandiset_id,
        processing_directory=processing_directory,
        test=test,
    )
    if not silent:
        _styled_echo(text=f"\nWrote derivatives/queue_stats.json to Dandiset {dandiset_id}.", color="green")


# dandicompute queue pending [OPTIONS]
@_queue_group.command(name="pending")
@click.option(
    "--silent",
    help="Suppress informational log output and the printed result.",
    required=False,
    is_flag=True,
    default=False,
)
@click.pass_context
def _queue_pending_command(context: click.Context, silent: bool = False) -> None:
    """Report whether any queued jobs are awaiting submission.

    Prints ``true`` and exits with code 0 when at least one job is pending.
    Prints ``false`` and exits with code 1 when nothing is pending. This lets a
    crontab skip the dispatch entirely when there is no work, for example:

        dandicompute queue pending --silent && dandicompute queue process ...
    """
    _configure_logging(silent=silent)
    pending = QueueState.has_pending_jobs()
    if not silent:
        _styled_echo(text="true" if pending else "false", color="green" if pending else "yellow")
    context.exit(0 if pending else 1)


# dandicompute queue process [OPTIONS]
@_queue_group.command(name="process")
@click.option(
    "--processing",
    "processing_directory",
    help="Path to the directory used for temporary working trees during job submission.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--max",
    "max_concurrent_aind_jobs",
    help="Maximum number of AIND jobs allowed to be running before submission is skipped.",
    required=False,
    type=click.IntRange(min=1),
    default=2,
    show_default=True,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--test",
    "test",
    help="Preserve temporary processing directories instead of cleaning them up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--jitter",
    "jitter_seconds",
    help="Maximum seconds of random sleep applied before processing to spread concurrent invocations.",
    required=False,
    type=click.FloatRange(min=0),
    default=30.0,
    show_default=True,
)
def _queue_process_command(
    processing_directory: pathlib.Path,
    max_concurrent_aind_jobs: int = 2,
    silent: bool = False,
    test: bool = False,
    jitter_seconds: float = 30.0,
) -> None:
    """Submit queued jobs when no active dandicompute jobs are running."""
    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    queue_status = QueueState.process_queue(
        processing_directory=processing_directory,
        max_concurrent_aind_jobs=max_concurrent_aind_jobs,
        jitter_seconds=jitter_seconds,
        test=test,
    )
    if not silent and queue_status == "no-pending":
        _styled_echo(text="\nNo jobs were found waiting to be submitted.", color="yellow")


# dandicompute issues
@_dandicompute_group.group(name="issues")
def _issues_group() -> None:
    """Scan logs and write per-capsule and aggregate issue reports."""
    pass


# dandicompute issues dump [OPTIONS]
@_issues_group.command(name="dump")
@click.option(
    "--directory",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan for logs.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID the issue dump JSON is written into.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--processing",
    "processing_directory",
    help="Directory for the temporary working tree used to write the issue dump JSON "
    "(defaults to the system temporary location).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--test",
    "test",
    help="Preserve the temporary working tree used to write the issue dump JSON instead of cleaning it up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _issues_dump_command(
    dandiset_directory: pathlib.Path,
    dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Scan nextflow and slurm logs and write per-capsule issue records."""
    _configure_logging(silent=silent)

    QueueState.dump_issues(
        dandiset_directory=dandiset_directory,
        dandiset_id=dandiset_id,
        processing_directory=processing_directory,
        test=test,
    )
    if not silent:
        _styled_echo(text=f"\nWrote derivatives/issues_dump.json to Dandiset {dandiset_id}.", color="green")


# dandicompute issues summarize [OPTIONS]
@_issues_group.command(name="summarize")
@click.option(
    "--directory",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan for logs.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID the issue summary JSON (and its issue dump) is written into.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--processing",
    "processing_directory",
    help="Directory for the temporary working tree used to write the issue summary JSON "
    "(defaults to the system temporary location).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--test",
    "test",
    help="Preserve the temporary working tree used to write the issue summary JSON instead of cleaning it up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _issues_summarize_command(
    dandiset_directory: pathlib.Path,
    dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Summarize discovered issue lines by descending occurrence count."""
    _configure_logging(silent=silent)

    QueueState.summarize_issues(
        dandiset_directory=dandiset_directory,
        dandiset_id=dandiset_id,
        processing_directory=processing_directory,
        test=test,
    )
    if not silent:
        _styled_echo(text=f"\nWrote derivatives/issues_summary.json to Dandiset {dandiset_id}.", color="green")


# dandicompute archive [OPTIONS]
@_dandicompute_group.command(name="archive")
@click.option(
    "--status",
    "status",
    help="Archive every job capsule with this status. Mutually exclusive with --job.",
    required=False,
    type=click.Choice(["failed", "pending", "stalled"]),
    default=None,
)
@click.option(
    "--job",
    "capsule_path",
    help="Path of a single job capsule folder (relative to the source Dandiset root) to archive directly. "
    "Mutually exclusive with --status.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID capsules are archived from.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--archive-dandiset-id",
    "archive_dandiset_id",
    help="Dandiset ID capsules are archived to.",
    required=False,
    type=str,
    default=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--processing",
    "processing_directory",
    help="Directory for the temporary working tree (defaults to the system temporary location).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--test",
    "test",
    help="Preserve the temporary working tree instead of cleaning it up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _archive_command(
    status: str | None,
    capsule_path: str | None,
    dandiset_id: str,
    archive_dandiset_id: str,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Archive one job capsule (--job) or every capsule with a --status."""
    if (status is None) == (capsule_path is None):
        message = "Provide exactly one of --status (failed|pending|stalled) or --job PATH."
        raise click.UsageError(message)

    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    if capsule_path is not None:
        move_job_capsule(
            capsule_path=capsule_path,
            source_dandiset_id=dandiset_id,
            target_dandiset_id=archive_dandiset_id,
            processing_directory=processing_directory,
            test=test,
        )
        if not silent:
            _styled_echo(text=f"\nArchived job capsule: {capsule_path}", color="green")
        return

    state = QueueState.from_dandi(dandiset_id=dandiset_id)
    archived = state.archive_by_status(
        status=status,
        dandiset_id=dandiset_id,
        archive_dandiset_id=archive_dandiset_id,
        processing_directory=processing_directory,
        test=test,
    )

    if not silent:
        if archived:
            _styled_echo(text=f"\nArchived {len(archived)} {status} job capsule(s):", color="green")
            for capsule_path in archived:
                _styled_echo(text=f"  {capsule_path}", color="green")
        else:
            _styled_echo(text=f"\nNo {status} job capsules to archive.", color="yellow")
