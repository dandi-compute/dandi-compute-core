"""Unit tests for the ``dandicompute delete --version`` CLI command."""

import os
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.queue import PipelineQueue

_GROUP = "dandi_compute_code._cli._dandicompute_group"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"


@pytest.mark.ai_generated
def test_cli_delete_requires_a_version() -> None:
    """--version is required, so a bare `delete` cannot select every capsule."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi:
        result = runner.invoke(_dandicompute_group, ["delete"])

    assert result.exit_code != 0
    assert "Missing option '--version'" in result.output
    mock_from_dandi.assert_not_called()


@pytest.mark.ai_generated
def test_cli_delete_fails_without_api_key() -> None:
    """Deleting errors before reading the archive when DANDI_API_KEY is missing."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        result = runner.invoke(_dandicompute_group, ["delete", "--version", "v1.4.0"])

    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output
    mock_from_dandi.assert_not_called()


@pytest.mark.ai_generated
def test_cli_delete_deletes_from_the_failed_runs_archive() -> None:
    """`delete` reads and deletes from 001873 and reports each deleted capsule."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        mock_delete = mock_from_dandi.return_value.delete_capsules
        mock_delete.return_value = ["derivatives/example-capsule"]
        result = runner.invoke(_dandicompute_group, ["delete", "--version", "v1.4.0", "--pipeline", "aind+ephys"])

    assert result.exit_code == 0, result.output
    assert (
        f"Deleted 1 aind+ephys v1.4.0 job capsule(s) from Dandiset {_FAILED_RUNS_ARCHIVE_DANDISET_ID}" in result.output
    )
    assert "derivatives/example-capsule" in result.output
    assert "dandicompute jobs refresh" in result.output
    mock_from_dandi.assert_called_once_with(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)
    mock_delete.assert_called_once_with(
        version="v1.4.0",
        pipeline="aind+ephys",
        dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        dry_run=False,
    )


@pytest.mark.ai_generated
def test_cli_delete_preview_lists_without_an_api_key() -> None:
    """--preview runs a dry run, needs no DANDI_API_KEY and says nothing was deleted."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        mock_delete = mock_from_dandi.return_value.delete_capsules
        mock_delete.return_value = ["derivatives/example-capsule"]
        result = runner.invoke(_dandicompute_group, ["delete", "--version", "v1.4.0", "--preview"])

    assert result.exit_code == 0, result.output
    assert f"Would delete 1 v1.4.0 job capsule(s) from Dandiset {_FAILED_RUNS_ARCHIVE_DANDISET_ID}" in result.output
    assert "dandicompute jobs refresh" not in result.output
    mock_delete.assert_called_once_with(
        version="v1.4.0",
        pipeline=None,
        dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        dry_run=True,
    )


@pytest.mark.ai_generated
def test_cli_delete_reports_when_nothing_matches() -> None:
    """With no matching capsules, `delete` says so and succeeds."""
    runner = CliRunner()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi,
    ):
        mock_from_dandi.return_value.delete_capsules.return_value = []
        result = runner.invoke(_dandicompute_group, ["delete", "--version", "v1.4.0"])

    assert result.exit_code == 0, result.output
    assert f"No v1.4.0 job capsules in Dandiset {_FAILED_RUNS_ARCHIVE_DANDISET_ID}." in result.output


@pytest.mark.ai_generated
def test_cli_delete_rejects_a_blank_version_as_a_usage_error() -> None:
    """A blank --version is reported as a usage error rather than a traceback."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=PipelineQueue(entries=[])):
        result = runner.invoke(_dandicompute_group, ["delete", "--version", " ", "--preview"])

    assert result.exit_code == 2
    assert "must not be blank" in result.output
