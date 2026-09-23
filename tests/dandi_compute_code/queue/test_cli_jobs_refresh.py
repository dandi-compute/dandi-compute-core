import os
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"
_DANDI_ENV = {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}


@pytest.mark.ai_generated
def test_cli_jobs_refresh_writes_tables_to_source_and_archived(base_directory: pathlib.Path) -> None:
    """dandicompute jobs refresh rewrites derivatives/jobs.tsv into both Dandisets."""
    runner = CliRunner()
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.write_dandiset_jobs_table") as mock_write:
        result = runner.invoke(_dandicompute_group, ["jobs", "refresh", "--base", str(base_directory)], env=_DANDI_ENV)
    assert result.exit_code == 0, result.output
    called_dandiset_ids = {call.kwargs["dandiset_id"] for call in mock_write.call_args_list}
    assert called_dandiset_ids == {_JOB_CAPSULES_DANDISET_ID, _FAILED_RUNS_ARCHIVE_DANDISET_ID}


@pytest.mark.ai_generated
def test_cli_jobs_refresh_forwards_custom_dandiset_ids(base_directory: pathlib.Path) -> None:
    """dandicompute jobs refresh forwards --dandiset-id/--archive-dandiset-id to write_dandiset_jobs_table."""
    runner = CliRunner()
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.write_dandiset_jobs_table") as mock_write:
        result = runner.invoke(
            _dandicompute_group,
            [
                "jobs",
                "refresh",
                "--base",
                str(base_directory),
                "--dandiset-id",
                "000123",
                "--archive-dandiset-id",
                "000456",
            ],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output
    called_dandiset_ids = {call.kwargs["dandiset_id"] for call in mock_write.call_args_list}
    assert called_dandiset_ids == {"000123", "000456"}


@pytest.mark.ai_generated
def test_cli_jobs_refresh_forwards_base_and_test_flags(base_directory: pathlib.Path) -> None:
    """dandicompute jobs refresh forwards --base/--test to write_dandiset_jobs_table."""
    runner = CliRunner()
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.write_dandiset_jobs_table") as mock_write:
        result = runner.invoke(
            _dandicompute_group,
            ["jobs", "refresh", "--base", str(base_directory), "--test"],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output
    for call in mock_write.call_args_list:
        assert call.kwargs["base_directory"] == base_directory
        assert call.kwargs["test"] is True


@pytest.mark.ai_generated
def test_cli_jobs_refresh_fails_without_api_key(base_directory: pathlib.Path) -> None:
    """dandicompute jobs refresh errors immediately when DANDI_API_KEY is missing (it writes to DANDI)."""
    runner = CliRunner()
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(_dandicompute_group, ["jobs", "refresh", "--base", str(base_directory)])
    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_jobs_refresh_fails_without_dandi_devel(base_directory: pathlib.Path) -> None:
    """dandicompute jobs refresh errors when DANDI_DEVEL is missing."""
    runner = CliRunner()
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}, clear=True):
        result = runner.invoke(_dandicompute_group, ["jobs", "refresh", "--base", str(base_directory)])
    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output
