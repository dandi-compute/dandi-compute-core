import logging
import os
import pathlib

import click

from ._clean_work_directory import clean_work_directory
from ._styled_echo import _styled_echo
from .._base_directory import _DEFAULT_BASE_DIRECTORY
from .._configure_logging import _configure_logging
from ..aind_ephys_pipeline import generate_curation_script, prepare_aind_ephys_job, submit_job
from ..dandiset import move_job_capsule
from ..dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID, _JOB_CAPSULES_DANDISET_ID
from ..queue import TEST_QUEUE_CONTENT_ID, PipelineQueue, clean_dispatch_directories

logging.basicConfig(level=logging.INFO)

_base_option = click.option(
    "--base",
    "base_directory",
    help="Path to the structured base directory, which holds code/, processing/, work/ and aind-ephys-pipeline/.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=_DEFAULT_BASE_DIRECTORY,
    show_default=True,
)


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
    """Run compute workflows and job management tasks for DANDI assets."""
    pass


# dandicompute clean [OPTIONS]
@_dandicompute_group.command(name="clean")
@_base_option
@click.option(
    "--work",
    "work",
    help="Clean the base directory's work/ (all contents except 'apptainer_cache' will be deleted).",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--dispatch",
    "dispatch",
    help="Remove finished dispatch directories from the base directory's processing/.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--unsubmitted",
    "unsubmitted",
    help="Delete job capsules that were prepared but never submitted from the job capsules Dandiset.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--age",
    "minimum_age_hours",
    help="Leave dispatch directories formed more recently than this many hours alone.",
    required=False,
    type=click.FloatRange(min=0),
    default=24.0,
    show_default=True,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _clean_command(
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    work: bool = False,
    dispatch: bool = False,
    unsubmitted: bool = False,
    minimum_age_hours: float = 24.0,
    silent: bool = False,
) -> None:
    """Clean work, finished dispatch directories or unsubmitted capsules."""
    if not work and not dispatch and not unsubmitted:
        raise click.UsageError("Nothing to clean. Pass any combination of --work, --dispatch and --unsubmitted.")

    _configure_logging(silent=silent)
    if unsubmitted:
        _require_dandi_api_key()

    if work:
        clean_work_directory(base_directory)
        if not silent:
            _styled_echo(text="\nWork directory cleaned!", color="green")

    if dispatch:
        removed_directories = clean_dispatch_directories(
            base_directory=base_directory,
            minimum_age_hours=minimum_age_hours,
        )
        if not silent and removed_directories:
            noun = "directory" if len(removed_directories) == 1 else "directories"
            _styled_echo(text=f"\nRemoved {len(removed_directories)} finished dispatch {noun}.", color="green")
        elif not silent:
            _styled_echo(text="\nNo dispatch directories were ready to be removed.", color="yellow")

    if unsubmitted:
        removed_capsules = PipelineQueue.from_dandi().clean_unsubmitted_capsules()
        if not silent and removed_capsules:
            for capsule_path in removed_capsules:
                _styled_echo(text=f"  Removed: {capsule_path}", color="yellow")
            noun = "capsule" if len(removed_capsules) == 1 else "capsules"
            _styled_echo(text=f"\nCleaned {len(removed_capsules)} unsubmitted {noun}.", color="green")
        elif not silent:
            _styled_echo(text="\nNo unsubmitted capsules found.", color="yellow")


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
    help="The path of the asset within its Dandiset on the archive (e.g., 'sub-01/sub-01_ecephys.nwb'). "
    "Required if --id is not provided (ignored with --test).",
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
@_base_option
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
    dandiset_path: str | None = None,
    config_key: str = "default",
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    parameters_key: str = "default",
    submit: bool = False,
    silent: bool = False,
) -> None:
    """Prepare an AIND ephys job, or create test job capsules with --test."""
    _configure_logging(silent=silent)
    if "DANDI_API_KEY" not in os.environ:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")

    if test:
        PipelineQueue.create_job_capsules(
            content_ids=[TEST_QUEUE_CONTENT_ID],
            base_directory=base_directory,
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
        base_directory=base_directory,
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
    """Create, dispatch and report on job capsules."""
    pass


# dandicompute jobs create [OPTIONS]
@_jobs_group.command(name="create")
@_base_option
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
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    only_pipeline: str | None = None,
    config_key: str = "default",
    limit: int | None = None,
    force_latest_versions: bool = False,
    silent: bool = False,
) -> None:
    """Create a job capsule for every qualifying asset that does not have one yet."""
    _configure_logging(silent=silent)
    _require_dandi_api_key()

    created_count = PipelineQueue.create_job_capsules(
        config_key=config_key,
        limit=limit,
        only_pipeline=only_pipeline,
        force_latest_versions=force_latest_versions,
        base_directory=base_directory,
    )
    if not silent:
        noun = "job capsule" if created_count == 1 else "job capsules"
        _styled_echo(text=f"\nCreated {created_count} {noun}.", color="green" if created_count else "yellow")


# dandicompute jobs refresh [OPTIONS]
@_jobs_group.command(name="refresh")
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
@_base_option
@click.option(
    "--test",
    "test",
    help=(
        "Preserve the temporary working trees used to write each jobs.tsv, paths.tsv and their JSON sidecars "
        "instead of cleaning them up."
    ),
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
def _jobs_refresh_command(
    dandiset_id: str,
    archive_dandiset_id: str,
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    test: bool = False,
    silent: bool = False,
) -> None:
    """
    Rewrite jobs.tsv into both Dandisets.

    Ephemerally rebuilds and rewrites derivatives/jobs.tsv within both the source and
    archived Dandisets themselves (see PipelineQueue.write_dandiset_jobs_table), so each always
    reflects its current state fetched fresh from its own assets.jsonld. The asset paths of
    each job are written beside it to derivatives/paths.tsv, and each table is accompanied by
    its JSON sidecar (derivatives/jobs.json and derivatives/paths.json) describing its columns.
    """
    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    for target_dandiset_id in (dandiset_id, archive_dandiset_id):
        PipelineQueue.write_dandiset_jobs_table(
            dandiset_id=target_dandiset_id,
            base_directory=base_directory,
            test=test,
        )
        if not silent:
            _styled_echo(
                text=(
                    "\nWrote derivatives/jobs.tsv, derivatives/paths.tsv and their JSON sidecars "
                    f"to Dandiset {target_dandiset_id}."
                ),
                color="green",
            )


# dandicompute jobs pending [OPTIONS]
@_jobs_group.command(name="pending")
@click.option(
    "--silent",
    help="Suppress informational log output and the printed result.",
    required=False,
    is_flag=True,
    default=False,
)
@click.pass_context
def _jobs_pending_command(context: click.Context, silent: bool = False) -> None:
    """Report whether any queued jobs are awaiting submission.

    Prints ``true`` and exits with code 0 when at least one job is pending.
    Prints ``false`` and exits with code 1 when nothing is pending. This lets a
    crontab skip the dispatch entirely when there is no work, for example:

        dandicompute jobs pending --silent && dandicompute jobs dispatch ...
    """
    _configure_logging(silent=silent)
    pending = PipelineQueue.has_pending_jobs()
    if not silent:
        _styled_echo(text="true" if pending else "false", color="green" if pending else "yellow")
    context.exit(0 if pending else 1)


# dandicompute jobs dispatch [OPTIONS]
@_jobs_group.command(name="dispatch")
@_base_option
@click.option(
    "--pipeline",
    "only_pipeline",
    help="Dispatch only this pipeline instead of every configured one.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--max",
    "max_concurrent",
    help="Override how many capsules the dispatched pipeline may run at once. Requires --pipeline.",
    required=False,
    type=click.IntRange(min=1),
    default=None,
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
def _jobs_dispatch_command(
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    only_pipeline: str | None = None,
    max_concurrent: int | None = None,
    silent: bool = False,
    test: bool = False,
    jitter_seconds: float = 30.0,
) -> None:
    """Hand every pending job capsule to its pipeline's SLURM array dispatcher."""
    # The concurrency limit is a per-pipeline setting, so an override that silently applied to
    # every pipeline at once would not mean the same thing as the setting it overrides.
    if max_concurrent is not None and only_pipeline is None:
        raise click.UsageError("--max overrides one pipeline's concurrency limit, so it requires --pipeline.")

    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    results = PipelineQueue.dispatch_jobs(
        base_directory=base_directory,
        only_pipeline=only_pipeline,
        max_concurrent=max_concurrent,
        jitter_seconds=jitter_seconds,
        test=test,
    )
    if silent:
        return

    if not results:
        _styled_echo(text="\nNo pipelines are configured for dispatch.", color="yellow")
        return
    for result in results.values():
        color = "green" if result.status == "dispatched" else "yellow"
        _styled_echo(text="\n" + "\n".join(result.summary_lines()), color=color)


# dandicompute issues
@_dandicompute_group.group(name="issues")
def _issues_group() -> None:
    """Scan logs and write per-capsule and aggregate issue reports."""
    pass


# dandicompute issues dump [OPTIONS]
@_issues_group.command(name="dump")
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID the issue dump JSON is written into.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@_base_option
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
    dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Scan nextflow and slurm logs and write per-capsule issue records."""
    _configure_logging(silent=silent)

    PipelineQueue.dump_issues(
        dandiset_id=dandiset_id,
        base_directory=base_directory,
        test=test,
    )
    if not silent:
        _styled_echo(text=f"\nWrote derivatives/issues_dump.json to Dandiset {dandiset_id}.", color="green")


# dandicompute issues summarize [OPTIONS]
@_issues_group.command(name="summarize")
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID the issue summary JSON (and its issue dump) is written into.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@_base_option
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
    dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Summarize discovered issue lines by descending occurrence count."""
    _configure_logging(silent=silent)

    PipelineQueue.summarize_issues(
        dandiset_id=dandiset_id,
        base_directory=base_directory,
        test=test,
    )
    if not silent:
        _styled_echo(text=f"\nWrote derivatives/issues_summary.json to Dandiset {dandiset_id}.", color="green")


# dandicompute curate [OPTIONS]
@_dandicompute_group.command(name="curate")
@click.option(
    "--job",
    "capsule",
    help="Job ID of a successful AIND capsule (e.g. 'job-260916a1b2c3'), or its path relative to the Dandiset root.",
    required=True,
    type=str,
)
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID holding the capsule.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
def _curate_command(capsule: str, dandiset_id: str = _JOB_CAPSULES_DANDISET_ID) -> None:
    """Print a SpikeInterface GUI curation script for a successful AIND capsule.

    Redirect the output to a file and run it, for example:

    \b
        dandicompute curate --job job-260916a1b2c3 > curate.py
        python curate.py
    """
    try:
        script = generate_curation_script(capsule=capsule, dandiset_id=dandiset_id)
    except ValueError as exception:
        raise click.ClickException(str(exception)) from exception
    click.echo(script, nl=False)


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
    "--pipeline",
    "pipeline",
    help="Archive every job capsule of this pipeline (e.g. 'lfp' or 'aind+ephys'), whatever its status. "
    "Combine with --status to archive only that pipeline's capsules with that status. Mutually exclusive with --job.",
    required=False,
    type=str,
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
@_base_option
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
    pipeline: str | None,
    capsule_path: str | None,
    dandiset_id: str,
    archive_dandiset_id: str,
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Archive one capsule (--job) or every capsule matching --status or --pipeline."""
    has_filter = status is not None or pipeline is not None
    if has_filter == (capsule_path is not None):
        message = "Provide either --job PATH, or at least one of --status (failed|pending|stalled) and --pipeline NAME."
        raise click.UsageError(message)

    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    if capsule_path is not None:
        move_job_capsule(
            capsule_path=capsule_path,
            source_dandiset_id=dandiset_id,
            target_dandiset_id=archive_dandiset_id,
            base_directory=base_directory,
            test=test,
        )
        if not silent:
            _styled_echo(text=f"\nArchived job capsule: {capsule_path}", color="green")
        return

    state = PipelineQueue.from_dandi(dandiset_id=dandiset_id)
    archived = state.archive_capsules(
        status=status,
        pipeline=pipeline,
        dandiset_id=dandiset_id,
        archive_dandiset_id=archive_dandiset_id,
        base_directory=base_directory,
        test=test,
    )

    if not silent:
        description = " ".join(part for part in (status, pipeline) if part is not None)
        if archived:
            _styled_echo(text=f"\nArchived {len(archived)} {description} job capsule(s):", color="green")
            for capsule_path in archived:
                _styled_echo(text=f"  {capsule_path}", color="green")
        else:
            _styled_echo(text=f"\nNo {description} job capsules to archive.", color="yellow")
