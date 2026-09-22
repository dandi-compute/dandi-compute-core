import os
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"
_DANDI_ENV = {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}


@pytest.mark.ai_generated
def test_cli_queue_refresh_writes_tables_to_source_and_archived() -> None:
    """dandicompute queue refresh rewrites derivatives/jobs.tsv into both Dandisets."""
    runner = CliRunner()
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.write_dandiset_jobs_table") as mock_write:
        result = runner.invoke(_dandicompute_group, ["queue", "refresh"], env=_DANDI_ENV)
    assert result.exit_code == 0, result.output
    called_dandiset_ids = {call.kwargs["dandiset_id"] for call in mock_write.call_args_list}
    assert called_dandiset_ids == {_JOB_CAPSULES_DANDISET_ID, _FAILED_RUNS_ARCHIVE_DANDISET_ID}


@pytest.mark.ai_generated
def test_cli_queue_refresh_forwards_custom_dandiset_ids() -> None:
    """dandicompute queue refresh forwards --dandiset-id/--archive-dandiset-id to write_dandiset_jobs_table."""
    runner = CliRunner()
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.write_dandiset_jobs_table") as mock_write:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "refresh",
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
def test_cli_queue_refresh_forwards_processing_and_test_flags() -> None:
    """dandicompute queue refresh forwards --processing/--test to write_dandiset_jobs_table."""
    runner = CliRunner()
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.write_dandiset_jobs_table") as mock_write:
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--test"],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output
    for call in mock_write.call_args_list:
        assert call.kwargs["test"] is True


@pytest.mark.ai_generated
def test_cli_queue_refresh_fails_without_api_key() -> None:
    """dandicompute queue refresh errors immediately when DANDI_API_KEY is missing (it writes to DANDI)."""
    runner = CliRunner()
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(_dandicompute_group, ["queue", "refresh"])
    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_fails_without_dandi_devel() -> None:
    """dandicompute queue refresh errors when DANDI_DEVEL is missing."""
    runner = CliRunner()
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}, clear=True):
        result = runner.invoke(_dandicompute_group, ["queue", "refresh"])
    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output
