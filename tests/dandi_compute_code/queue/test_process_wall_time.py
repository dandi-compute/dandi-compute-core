import csv
import io
import pathlib
from unittest import mock

import pytest
from testing_utilities import job_capsule_files, serve_remote_dandiset

from dandi_compute_code.dandiset import AssetsJsonldMetadata
from dandi_compute_code.queue import PipelineQueue

_EXAMPLE_TIMELINE_REPORTS = pathlib.Path(__file__).parent / "example_timeline_reports"


def _refreshed_process_wall_times(files: dict[str, str]) -> dict[str, str]:
    """Refresh the jobs table over *files* and return each written row's process wall time, keyed by job ID."""
    empty_upstream_metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={})
    with (
        serve_remote_dandiset(files),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=empty_upstream_metadata,
        ),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        PipelineQueue.write_dandiset_jobs_table()

    jobs_tsv = next(
        call.kwargs["content"]
        for call in mock_write_file.call_args_list
        if call.kwargs["relative_path"] == "derivatives/jobs.tsv"
    )
    process_wall_times = {
        row["job_id"]: row["process_wall_time_seconds"] for row in csv.DictReader(io.StringIO(jobs_tsv), delimiter="\t")
    }
    return process_wall_times


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("timeline_html", "expected_wall_time"),
    [
        pytest.param((_EXAMPLE_TIMELINE_REPORTS / "two_steps.html").read_text(), "215", id="two-steps"),
        pytest.param((_EXAMPLE_TIMELINE_REPORTS / "one_step.html").read_text(), "1", id="one-step"),
        pytest.param("<script>window.data = {invalid json};</script>", "", id="malformed"),
    ],
)
def test_refresh_records_process_wall_time_from_timeline(
    example_pipeline_queue: PipelineQueue, timeline_html: str, expected_wall_time: str
) -> None:
    """The refreshed jobs.tsv sums every Nextflow process duration, leaving the cell empty for an unreadable report."""
    entry = example_pipeline_queue.entry_for(within_dandiset_path="sub-successful")
    files = job_capsule_files(entry=entry, with_logs=True, with_output=True)
    files[f"{entry.capsule_path()}/logs/timeline.html"] = timeline_html

    process_wall_times = _refreshed_process_wall_times(files)

    assert process_wall_times == {entry.job.job_id: expected_wall_time}


@pytest.mark.ai_generated
def test_refresh_leaves_process_wall_time_empty_without_timeline(example_pipeline_queue: PipelineQueue) -> None:
    """A capsule with no timeline report, such as one that has not run, has an empty process wall time."""
    ran_entry = example_pipeline_queue.entry_for(within_dandiset_path="sub-successful")
    pending_entry = example_pipeline_queue.entry_for(within_dandiset_path="sub-pending")
    files = {
        **job_capsule_files(entry=ran_entry, with_logs=True, with_output=True),
        **job_capsule_files(entry=pending_entry),
        f"{ran_entry.capsule_path()}/logs/timeline.html": (_EXAMPLE_TIMELINE_REPORTS / "two_steps.html").read_text(),
    }

    process_wall_times = _refreshed_process_wall_times(files)

    assert process_wall_times == {ran_entry.job.job_id: "215", pending_entry.job.job_id: ""}
