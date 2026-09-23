import json
import pathlib
from datetime import datetime
from unittest import mock

import pytest
from testing_utilities import job_capsule_files, serve_remote_dandiset

from dandi_compute_code.queue import PipelineQueue

_EXAMPLE_TIMELINE_REPORTS = pathlib.Path(__file__).parent / "example_timeline_reports"
_JOB_CAPSULES_DANDISET_ID = "001697"


@pytest.fixture
def timeline_two_steps() -> str:
    """Nextflow timeline report with two process steps."""
    return (_EXAMPLE_TIMELINE_REPORTS / "two_steps.html").read_text()


@pytest.fixture
def timeline_one_step() -> str:
    """Nextflow timeline report with a single process step."""
    return (_EXAMPLE_TIMELINE_REPORTS / "one_step.html").read_text()


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_writes_queue_stats_json(
    example_pipeline_queue: PipelineQueue, base_directory: pathlib.Path, timeline_two_steps: str
) -> None:
    """aggregate_statistics writes queue_stats.json with byte and timeline aggregates."""
    # sub-successful is the only entry with both output and a known source-asset size.
    entry = example_pipeline_queue.entry_for(within_dandiset_path="sub-successful")
    files = job_capsule_files(entry=entry, with_logs=True)
    files[f"{entry.capsule_path()}/logs/timeline.html"] = timeline_two_steps

    with (
        serve_remote_dandiset(files),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        stats = example_pipeline_queue.aggregate_statistics(base_directory=base_directory)

    assert stats["state_entry_count"] == len(example_pipeline_queue)
    assert stats["successful_asset_bytes_total"] == 120
    assert stats["timeline_files_processed"] == 1
    assert datetime.fromisoformat(stats["generated_at"])
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(65.0)
    assert stats["job_step_wall_time_seconds"]["step_two"] == pytest.approx(150.0)

    mock_write_file.assert_called_once()
    call_kwargs = mock_write_file.call_args.kwargs
    assert call_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert call_kwargs["relative_path"] == "derivatives/queue_stats.json"
    assert json.loads(call_kwargs["content"]) == stats


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_skips_invalid_timeline_html(
    example_pipeline_queue: PipelineQueue, base_directory: pathlib.Path
) -> None:
    """aggregate_statistics ignores timeline files with malformed embedded JSON."""
    entry = example_pipeline_queue.entry_for(within_dandiset_path="sub-successful")
    files = job_capsule_files(entry=entry, with_logs=True)
    files[f"{entry.capsule_path()}/logs/timeline.html"] = "<script>window.data = {invalid json};</script>"

    with serve_remote_dandiset(files), mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file"):
        stats = example_pipeline_queue.aggregate_statistics(base_directory=base_directory)

    assert stats["timeline_files_processed"] == 0
    assert stats["job_step_wall_time_seconds"] == {}


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_found_timeline_via_fallback_capsule_resolution(
    example_pipeline_queue: PipelineQueue, base_directory: pathlib.Path, timeline_one_step: str
) -> None:
    """aggregate_statistics finds timeline files when state within_dandiset_path differs from the remote path."""
    # The "sourcedata" entry's remote capsule lives under sub-mouse01, so its timeline
    # must be located via fallback resolution rather than the recorded within_dandiset_path.
    capsule_path = "derivatives/dandisets-001/dandiset-001849/sub-mouse01/pipeline-aind+ephys/job-240101aa0009"
    files = {f"{capsule_path}/logs/timeline.html": timeline_one_step}

    with serve_remote_dandiset(files), mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file"):
        stats = example_pipeline_queue.aggregate_statistics(base_directory=base_directory)

    assert stats["timeline_files_processed"] == 1
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(1.0)


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_forwards_dandiset_id_and_relative_path(
    example_pipeline_queue: PipelineQueue, base_directory: pathlib.Path
) -> None:
    """aggregate_statistics forwards dandiset_id/relative_path/base_directory/test to write_dandiset_file."""

    with mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file:
        example_pipeline_queue.aggregate_statistics(
            dandiset_id="000123",
            relative_path="derivatives/custom_stats.json",
            base_directory=base_directory,
            test=True,
        )

    mock_write_file.assert_called_once_with(
        dandiset_id="000123",
        relative_path="derivatives/custom_stats.json",
        content=mock.ANY,
        base_directory=base_directory,
        test=True,
    )
