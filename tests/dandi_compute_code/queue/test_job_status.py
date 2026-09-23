"""The single ``status`` column, and the queue selectors reading it."""

import pathlib

import pytest

from dandi_compute_code.queue import JOB_STATUSES, JobCapsule, JobInfo, PipelineQueue


def _entry(status: str, /) -> JobCapsule:
    job = JobInfo(
        job_id=f"job-250101{status[:6]}",
        dandiset_id="001849",
        within_dandiset_path=f"sub-{status}/sub-{status}_ecephys.nwb",
        pipeline="aind+ephys",
        version="v1.0",
        params="abc1234",
        config="def5678",
        codebase="v0.3.0",
    )
    return JobCapsule(job=job, content_id=f"content-{status}", asset_size_bytes=1024, status=status)


@pytest.mark.ai_generated
def test_job_capsule_defaults_to_unknown() -> None:
    """A capsule built without a status has not been observed anywhere in its lifecycle."""
    job = JobInfo(
        job_id="job-250101abc123",
        dandiset_id="001849",
        within_dandiset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
        pipeline="aind+ephys",
        version="v1.0",
        params="abc1234",
        config="def5678",
        codebase="v0.3.0",
    )
    entry = JobCapsule(job=job, content_id=None, asset_size_bytes=None)

    assert entry.status == "unknown"


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", JOB_STATUSES)
def test_status_round_trips_through_the_table(status: str, tmp_path: pathlib.Path) -> None:
    """Every status survives a write and read of ``jobs.tsv``."""
    jobs_file = tmp_path / "jobs.tsv"
    PipelineQueue(entries=[_entry(status)]).to_tsv(jobs_file)

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert pipeline_queue.entries[0].status == status


@pytest.mark.ai_generated
@pytest.mark.parametrize("cell", ["", "running", "not-a-status"], ids=["empty", "retired", "unrecognised"])
def test_unreadable_status_cell_falls_back_to_unknown(cell: str, tmp_path: pathlib.Path) -> None:
    """A status cell this version does not recognise reads back as unknown rather than raising."""
    jobs_file = tmp_path / "jobs.tsv"
    PipelineQueue(entries=[_entry("failed")]).to_tsv(jobs_file)
    header, row = (line.split("\t") for line in jobs_file.read_text().splitlines())
    row[header.index("status")] = cell
    jobs_file.write_text("\n".join(["\t".join(header), "\t".join(row)]) + "\n")

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert pipeline_queue.entries[0].status == "unknown"


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", JOB_STATUSES)
def test_with_status_selects_only_that_status(status: str) -> None:
    """Each status names a disjoint subset of the queue."""
    queue = PipelineQueue(entries=[_entry(each) for each in JOB_STATUSES])

    selected = queue.with_status(status)

    assert [entry.status for entry in selected] == [status]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("selector", "expected_status"),
    [
        pytest.param("pending", "pending", id="pending"),
        pytest.param("stalled", "stalled", id="stalled"),
        pytest.param("failed", "failed", id="failed"),
        pytest.param("successful", "successful", id="successful"),
    ],
)
def test_named_selectors_read_the_status_field(selector: str, expected_status: str) -> None:
    """The named queue properties are the status subsets ``archive_by_status`` acts on."""
    queue = PipelineQueue(entries=[_entry(each) for each in JOB_STATUSES])

    selected = getattr(queue, selector)

    assert [entry.status for entry in selected] == [expected_status]


@pytest.mark.ai_generated
def test_successful_asset_bytes_total_counts_only_successful_entries() -> None:
    """Only capsules that produced output contribute their source-asset size."""
    queue = PipelineQueue(entries=[_entry(each) for each in JOB_STATUSES])

    assert queue.successful_asset_bytes_total == 1024
