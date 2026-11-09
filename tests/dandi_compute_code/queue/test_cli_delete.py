"""Unit tests for the ``dandicompute archive delete`` CLI command."""

import os
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.queue import JobCapsule, JobInfo, PipelineQueue

_GROUP = "dandi_compute_code._cli._dandicompute_group"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"

_CAPSULE_PATH = "derivatives/dandisets-001/dandiset-001371/sub-sole/ses-capsule/pipeline-aind+ephys/job-240101aa000c"
_ENTRY = JobCapsule(
    job=JobInfo(
        job_id="job-240101aa000c",
        dandiset_id="001371",
        within_dandiset_path="sub-sole/ses-capsule",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2",
        params="abc1234",
        config="cfgsole",
        codebase="v0.3.0",
    ),
    content_id=None,
    asset_size_bytes=None,
    status="failed",
)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "args",
    [
        pytest.param([], id="nothing"),
        pytest.param(["--version", "v1.4.0"], id="version-without-pipeline"),
        pytest.param(["--pipeline", "aind+ephys"], id="pipeline-without-version"),
        pytest.param(
            ["--job", "job-240101aa000c", "--version", "v1.4.0", "--pipeline", "aind+ephys"], id="job-and-version"
        ),
        pytest.param(["--job", "job-240101aa000c", "--pipeline", "aind+ephys"], id="job-and-pipeline"),
    ],
)
def test_cli_delete_requires_a_job_or_a_version_with_its_pipeline(args: list[str]) -> None:
    """A bare version is refused, so it cannot clobber every pipeline sharing it, and --job stands alone."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi:
        result = runner.invoke(_dandicompute_group, ["archive", "delete", *args, "--preview"])

    assert result.exit_code == 2
    assert "Provide either --job JOB_ID, or both --version VERSION and --pipeline NAME." in result.output
    mock_from_dandi.assert_not_called()


@pytest.mark.ai_generated
def test_cli_delete_fails_without_api_key() -> None:
    """Deleting errors before reading the archive when DANDI_API_KEY is missing."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        result = runner.invoke(_dandicompute_group, ["archive", "delete", "--job", "job-240101aa000c"])

    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output
    mock_from_dandi.assert_not_called()


@pytest.mark.ai_generated
def test_cli_delete_deletes_a_version_of_a_pipeline_from_the_failed_runs_archive() -> None:
    """`delete --version --pipeline` reads and deletes from 001873 and reports each deleted capsule."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        mock_delete = mock_from_dandi.return_value.delete_capsules
        mock_delete.return_value = {_CAPSULE_PATH: _ENTRY}
        result = runner.invoke(
            _dandicompute_group, ["archive", "delete", "--version", "v1.1.1", "--pipeline", "aind+ephys"]
        )

    assert result.exit_code == 0, result.output
    assert (
        f"Deleted 1 aind+ephys v1.1.1 job capsule(s) from Dandiset {_FAILED_RUNS_ARCHIVE_DANDISET_ID}" in result.output
    )
    assert _CAPSULE_PATH in result.output
    assert "codebase:" not in result.output
    assert "dandicompute jobs refresh" in result.output
    mock_from_dandi.assert_called_once_with(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)
    mock_delete.assert_called_once_with(
        job_id=None,
        version="v1.1.1",
        pipeline="aind+ephys",
        dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        dry_run=False,
    )


@pytest.mark.ai_generated
def test_cli_delete_deletes_one_job_by_id() -> None:
    """`delete --job` forwards the job ID alone and names it in the report."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        mock_delete = mock_from_dandi.return_value.delete_capsules
        mock_delete.return_value = {_CAPSULE_PATH: _ENTRY}
        result = runner.invoke(_dandicompute_group, ["archive", "delete", "--job", "job-240101aa000c"])

    assert result.exit_code == 0, result.output
    assert (
        f"Deleted 1 job-240101aa000c job capsule(s) from Dandiset {_FAILED_RUNS_ARCHIVE_DANDISET_ID}" in result.output
    )
    assert _CAPSULE_PATH in result.output
    mock_delete.assert_called_once_with(
        job_id="job-240101aa000c",
        version=None,
        pipeline=None,
        dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        dry_run=False,
    )


@pytest.mark.ai_generated
def test_cli_delete_preview_lists_provenance_without_an_api_key() -> None:
    """--preview runs a dry run, needs no DANDI_API_KEY, and shows each capsule's provenance, exact version included."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        mock_delete = mock_from_dandi.return_value.delete_capsules
        mock_delete.return_value = {_CAPSULE_PATH: _ENTRY}
        result = runner.invoke(
            _dandicompute_group, ["archive", "delete", "--version", "v1.1.1", "--pipeline", "aind+ephys", "--preview"]
        )

    assert result.exit_code == 0, result.output
    assert (
        f"Would delete 1 aind+ephys v1.1.1 job capsule(s) from Dandiset {_FAILED_RUNS_ARCHIVE_DANDISET_ID}"
        in result.output
    )
    assert _CAPSULE_PATH in result.output
    provenance = result.output.split(_CAPSULE_PATH, maxsplit=1)[1]
    expected_lines = [
        "job_id: job-240101aa000c",
        "status: failed",
        "asset: 001371/sub-sole/ses-capsule",
        "pipeline: aind+ephys",
        "version: v1.1.1+b268fd2",
        "params: abc1234",
        "config: cfgsole",
        "codebase: v0.3.0",
    ]
    assert [line.strip() for line in provenance.splitlines() if line.strip()] == expected_lines
    assert "dandicompute jobs refresh" not in result.output
    mock_delete.assert_called_once_with(
        job_id=None,
        version="v1.1.1",
        pipeline="aind+ephys",
        dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        dry_run=True,
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("args", "description"),
    [
        pytest.param(["--version", "v1.4.0", "--pipeline", "lfp"], "lfp v1.4.0", id="version"),
        pytest.param(["--job", "job-240101aa000c"], "job-240101aa000c", id="job"),
        pytest.param(["--job", "240101aa000c"], "job-240101aa000c", id="job_without_prefix"),
    ],
)
def test_cli_delete_reports_when_nothing_matches(args: list[str], description: str) -> None:
    """With no matching capsules, `delete` says so and succeeds."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        mock_from_dandi.return_value.delete_capsules.return_value = {}
        result = runner.invoke(_dandicompute_group, ["archive", "delete", *args])

    assert result.exit_code == 0, result.output
    assert f"No {description} job capsules in Dandiset {_FAILED_RUNS_ARCHIVE_DANDISET_ID}." in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["--version", " ", "--pipeline", "lfp"], id="blank-version"),
        pytest.param(["--version", "v1.4.0", "--pipeline", " "], id="blank-pipeline"),
        pytest.param(["--job", " "], id="blank-job"),
    ],
)
def test_cli_delete_rejects_a_blank_selection_as_a_usage_error(args: list[str]) -> None:
    """A blank selector is reported as a usage error rather than a traceback."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=PipelineQueue(entries=[])):
        result = runner.invoke(_dandicompute_group, ["archive", "delete", *args, "--preview"])

    assert result.exit_code == 2
    assert "must not be blank" in result.output
