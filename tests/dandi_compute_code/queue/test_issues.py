import json
import pathlib
from unittest import mock

import pytest
from testing_utilities import job_capsule_log_files, serve_remote_dandiset

from dandi_compute_code.queue import PipelineQueue

_JOB_CAPSULES_DANDISET_ID = "001697"


@pytest.mark.ai_generated
def test_dump_issues_writes_per_capsule_records(base_directory: pathlib.Path) -> None:
    files = job_capsule_log_files(
        dandiset_id="000001",
        subject="mouse01",
        job_id="job-240101aa0001",
        nextflow_lines=["INFO start", "ERROR ~ Process failed"],
        slurm_lines_by_file={"job-123_slurm.log": ["slurm ok", "srun: error: node failure"]},
    ) | job_capsule_log_files(
        dandiset_id="000001",
        subject="mouse02",
        job_id="job-240101aa0002",
        nextflow_lines=["INFO only"],
        slurm_lines_by_file={"job-456_slurm.log": ["all good"]},
    )

    with (
        serve_remote_dandiset(files),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        records = PipelineQueue.dump_issues(base_directory=base_directory)

    assert len(records) == 1
    assert records[0]["capsule_path"].endswith("job-240101aa0001")
    assert records[0]["nextflow_errors"] == ["ERROR ~ Process failed"]
    assert records[0]["slurm_errors"] == {"job-123_slurm.log": ["srun: error: node failure"]}

    mock_write_file.assert_called_once()
    call_kwargs = mock_write_file.call_args.kwargs
    assert call_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert call_kwargs["relative_path"] == "derivatives/issues_dump.json"
    dump_payload = json.loads(call_kwargs["content"])
    assert dump_payload["capsule_count"] == 1
    assert dump_payload["records"] == records


@pytest.mark.ai_generated
def test_dump_issues_forwards_dandiset_id_and_relative_path(base_directory: pathlib.Path) -> None:
    """dump_issues forwards dandiset_id/relative_path/base_directory/test to write_dandiset_file."""

    with mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file:
        PipelineQueue.dump_issues(
            dandiset_id="000123",
            relative_path="derivatives/custom_dump.json",
            base_directory=base_directory,
            test=True,
        )

    mock_write_file.assert_called_once_with(
        dandiset_id="000123",
        relative_path="derivatives/custom_dump.json",
        content=mock.ANY,
        base_directory=base_directory,
        test=True,
    )


@pytest.mark.ai_generated
def test_summarize_issues_writes_descending_frequency(base_directory: pathlib.Path) -> None:
    files = job_capsule_log_files(
        dandiset_id="000001",
        subject="mouse01",
        job_id="job-240101aa0001",
        nextflow_lines=["error: common failure", "error: unique failure"],
        slurm_lines_by_file={"job-001_slurm.log": ["error: common failure"]},
    ) | job_capsule_log_files(
        dandiset_id="000002",
        subject="mouse02",
        job_id="job-240101aa0002",
        nextflow_lines=["error: common failure"],
        slurm_lines_by_file={"job-002_slurm.log": ["done"]},
    )

    with (
        serve_remote_dandiset(files),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        summary = PipelineQueue.summarize_issues(base_directory=base_directory)

    assert summary == {"3": ["error: common failure"], "1": ["error: unique failure"]}

    # dump_issues (called internally) plus the summary itself are both written.
    assert mock_write_file.call_count == 2
    relative_paths = {call.kwargs["relative_path"] for call in mock_write_file.call_args_list}
    assert relative_paths == {"derivatives/issues_dump.json", "derivatives/issues_summary.json"}

    summary_call = next(
        call
        for call in mock_write_file.call_args_list
        if call.kwargs["relative_path"] == "derivatives/issues_summary.json"
    )
    summary_payload = json.loads(summary_call.kwargs["content"])
    assert summary_payload["summary"] == {"3": ["error: common failure"], "1": ["error: unique failure"]}


@pytest.mark.ai_generated
def test_summarize_issues_forwards_dandiset_id_to_dump_and_summary(base_directory: pathlib.Path) -> None:
    """summarize_issues forwards dandiset_id/base_directory/test to both writes."""

    with mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file:
        PipelineQueue.summarize_issues(
            dandiset_id="000123",
            base_directory=base_directory,
            test=True,
        )

    assert mock_write_file.call_count == 2
    for call in mock_write_file.call_args_list:
        assert call.kwargs["dandiset_id"] == "000123"
        assert call.kwargs["base_directory"] == base_directory
        assert call.kwargs["test"] is True
