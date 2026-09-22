import csv
import io
import json
import pathlib

import pytest

from dandi_compute_code.queue import JobCapsule, JobInfo, PipelineQueue

_CAPSULE_PATH = (
    "derivatives/dandisets-001/dandiset-001849/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/job-250101abc123"
)


def _make_entry(**overrides: object) -> JobCapsule:
    job_kwargs = {
        "job_id": "job-250101abc123",
        "dandiset_id": "001849",
        "dandi_path": "sub-mouse01/sub-mouse01_ecephys.nwb",
        "pipeline": "aind+ephys",
        "version": "v1.0",
        "params": "abc1234",
        "config": "def5678",
        "codebase": "v0.3.0",
    }
    entry_kwargs = {
        "content_id": "content-id-1",
        "asset_size_bytes": 1234,
        "status": "successful",
        "created_at": "2025-01-01T00:00:00+00:00",
        "job_submission_time": "2025-01-01T00:15:00+00:00",
        "job_completion_time": "2025-01-01T01:00:00+00:00",
        "dataset_description_path": {f"{_CAPSULE_PATH}/dataset_description.json": "dd-id"},
        "output_paths": {f"{_CAPSULE_PATH}/derivatives/output.nwb": "out-id"},
        "log_paths": {f"{_CAPSULE_PATH}/logs/stdout.txt": "log-id"},
    }
    for key, value in overrides.items():
        if key in job_kwargs:
            job_kwargs[key] = value
        else:
            entry_kwargs[key] = value
    return JobCapsule(job=JobInfo(**job_kwargs), **entry_kwargs)


@pytest.mark.ai_generated
def test_job_capsule_to_tsv_row_leaves_out_path_mappings() -> None:
    """JobCapsule.to_tsv_row keeps the path mappings out of the jobs.tsv row."""
    entry = _make_entry()
    row = entry.to_tsv_row()
    assert row["dandiset_id"] == "001849"
    assert row["status"] == "successful"
    assert row["asset_size_bytes"] == "1234"
    assert {"dataset_description_path", "output_paths", "log_paths"} & row.keys() == set()


@pytest.mark.ai_generated
def test_job_capsule_to_paths_tsv_rows_lists_one_row_per_path() -> None:
    """JobCapsule.to_paths_tsv_rows writes one paths.tsv row per asset path, keyed by job_id."""
    rows = _make_entry().to_paths_tsv_rows()
    assert rows == [
        {"job_id": "job-250101abc123", "path": f"{_CAPSULE_PATH}/dataset_description.json", "content_id": "dd-id"},
        {"job_id": "job-250101abc123", "path": f"{_CAPSULE_PATH}/derivatives/output.nwb", "content_id": "out-id"},
        {"job_id": "job-250101abc123", "path": f"{_CAPSULE_PATH}/logs/stdout.txt", "content_id": "log-id"},
    ]


@pytest.mark.ai_generated
def test_job_capsule_to_paths_tsv_rows_empty_without_paths() -> None:
    """JobCapsule.to_paths_tsv_rows writes nothing for an entry without any paths."""
    entry = _make_entry(status="pending", dataset_description_path={}, output_paths={}, log_paths={})
    assert entry.to_paths_tsv_rows() == []


@pytest.mark.ai_generated
def test_job_capsule_to_tsv_row_none_becomes_empty_cell() -> None:
    """JobCapsule.to_tsv_row writes an empty string for None fields."""
    entry = _make_entry(created_at=None, job_completion_time=None, content_id=None, asset_size_bytes=None)
    row = entry.to_tsv_row()
    assert row["created_at"] == ""
    assert row["job_completion_time"] == ""
    assert row["content_id"] == ""
    assert row["asset_size_bytes"] == ""


@pytest.mark.ai_generated
def test_pipeline_queue_to_tsv_string_is_tab_delimited_with_header() -> None:
    """PipelineQueue.to_tsv_string writes a tab-delimited table with a header row."""
    state = PipelineQueue(entries=[_make_entry(), _make_entry(dandi_path="sub-mouse02/sub-mouse02_ecephys.nwb")])
    tsv_text = state.to_tsv_string()

    reader = csv.DictReader(io.StringIO(tsv_text), delimiter="\t")
    rows = list(reader)
    assert reader.fieldnames is not None
    assert "dandiset_id" in reader.fieldnames
    assert "dandi_path" in reader.fieldnames
    assert len(rows) == 2
    assert {row["dandi_path"] for row in rows} == {
        "sub-mouse01/sub-mouse01_ecephys.nwb",
        "sub-mouse02/sub-mouse02_ecephys.nwb",
    }


@pytest.mark.ai_generated
def test_pipeline_queue_to_tsv_string_empty_state_has_only_header() -> None:
    """PipelineQueue.to_tsv_string writes only the header row when there are no entries."""
    state = PipelineQueue(entries=[])
    tsv_text = state.to_tsv_string()
    lines = tsv_text.splitlines()
    assert len(lines) == 1
    assert lines[0].split("\t")[0] == "job_id"


@pytest.mark.ai_generated
def test_pipeline_queue_to_tsv_writes_file(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.to_tsv writes the TSV table to the given file path."""
    state = PipelineQueue(entries=[_make_entry()])
    output_file = tmp_path / "jobs.tsv"
    state.to_tsv(output_file)
    assert output_file.exists()
    assert output_file.read_text() == state.to_tsv_string()


@pytest.mark.ai_generated
def test_pipeline_queue_from_tsv_preserves_dataset_description_path(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.from_tsv preserves dataset_description_path entries."""
    jobs_file = tmp_path / "jobs.tsv"
    dataset_description_path = {f"{_CAPSULE_PATH}/dataset_description.json": "dataset-description-id"}
    entry = _make_entry(dataset_description_path=dataset_description_path)
    PipelineQueue(entries=[entry]).to_tsv(jobs_file)

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert len(pipeline_queue) == 1
    assert pipeline_queue.entries[0].dataset_description_path == dataset_description_path


@pytest.mark.ai_generated
def test_pipeline_queue_to_tsv_writes_paths_table_beside_state(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.to_tsv writes paths.tsv next to jobs.tsv, and from_tsv reads every mapping back."""
    entry = _make_entry(
        output_paths={
            f"{_CAPSULE_PATH}/derivatives/output.nwb": "out-id",
            f"{_CAPSULE_PATH}/derivatives/other.nwb": "other-id",
        }
    )
    jobs_file = tmp_path / "jobs.tsv"
    PipelineQueue(entries=[entry]).to_tsv(jobs_file)

    paths_file = tmp_path / "paths.tsv"
    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert paths_file.read_text() == PipelineQueue(entries=[entry]).to_paths_tsv_string()
    assert paths_file.read_text().splitlines()[0].split("\t") == ["job_id", "path", "content_id"]
    assert pipeline_queue.entries[0].dataset_description_path == entry.dataset_description_path
    assert pipeline_queue.entries[0].output_paths == entry.output_paths
    assert pipeline_queue.entries[0].log_paths == entry.log_paths


@pytest.mark.ai_generated
def test_pipeline_queue_from_tsv_without_paths_table(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.from_tsv leaves the path mappings empty when there is no paths.tsv."""
    jobs_file = tmp_path / "jobs.tsv"
    jobs_file.write_text(PipelineQueue(entries=[_make_entry()]).to_tsv_string())

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert pipeline_queue.entries[0].output_paths == {}
    assert pipeline_queue.entries[0].log_paths == {}


@pytest.mark.ai_generated
def test_pipeline_queue_from_tsv_reads_legacy_json_path_columns(tmp_path: pathlib.Path) -> None:
    """A jobs.tsv written before the paths moved to paths.tsv still reads its JSON path columns."""
    entry = _make_entry()
    row = {
        **entry.to_tsv_row(),
        "dataset_description_path": json.dumps(entry.dataset_description_path),
        "output_paths": json.dumps(entry.output_paths),
        "log_paths": "",
    }
    jobs_file = tmp_path / "jobs.tsv"
    with jobs_file.open("w", newline="") as file_stream:
        writer = csv.DictWriter(file_stream, fieldnames=list(row), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerow(row)

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert pipeline_queue.entries[0].dataset_description_path == entry.dataset_description_path
    assert pipeline_queue.entries[0].output_paths == entry.output_paths
    assert pipeline_queue.entries[0].log_paths == {}


@pytest.mark.ai_generated
def test_pipeline_queue_empty_dataset_description_path_cell(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.from_tsv reads an entry without a dataset description path back as an empty dict."""
    jobs_file = tmp_path / "jobs.tsv"
    entry = _make_entry(dataset_description_path={})
    PipelineQueue(entries=[entry]).to_tsv(jobs_file)

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert len(pipeline_queue) == 1
    assert pipeline_queue.entries[0].dataset_description_path == {}


@pytest.mark.ai_generated
def test_job_capsule_durations_are_computed_from_timestamps() -> None:
    """JobCapsule derives the waiting and running durations from its three timestamps."""
    entry = _make_entry()
    assert entry.queue_wait_seconds == 900
    assert entry.run_duration_seconds == 2700


@pytest.mark.ai_generated
def test_job_capsule_to_tsv_row_writes_durations_in_seconds() -> None:
    """JobCapsule.to_tsv_row writes the two derived durations as whole seconds."""
    row = _make_entry().to_tsv_row()
    assert row["job_submission_time"] == "2025-01-01T00:15:00+00:00"
    assert row["queue_wait_seconds"] == "900"
    assert row["run_duration_seconds"] == "2700"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("overrides", "expected_wait_seconds", "expected_run_duration_seconds"),
    [
        ({"created_at": None}, None, 2700),
        ({"job_submission_time": None}, None, None),
        ({"job_completion_time": None}, 900, None),
        ({"created_at": "not-a-timestamp"}, None, 2700),
    ],
    ids=["no_created_at", "no_submission_time", "no_completion_time", "malformed_created_at"],
)
def test_job_capsule_durations_empty_when_a_timestamp_is_unusable(
    overrides: dict, expected_wait_seconds: int | None, expected_run_duration_seconds: int | None
) -> None:
    """A missing or malformed timestamp leaves the duration it feeds empty."""
    entry = _make_entry(**overrides)
    row = entry.to_tsv_row()
    assert entry.queue_wait_seconds == expected_wait_seconds
    assert entry.run_duration_seconds == expected_run_duration_seconds
    assert row["queue_wait_seconds"] == ("" if expected_wait_seconds is None else str(expected_wait_seconds))
    assert row["run_duration_seconds"] == (
        "" if expected_run_duration_seconds is None else str(expected_run_duration_seconds)
    )


@pytest.mark.ai_generated
def test_pipeline_queue_from_tsv_round_trips_submission_time(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.from_tsv reads job_submission_time back and recomputes the durations."""
    jobs_file = tmp_path / "jobs.tsv"
    PipelineQueue(entries=[_make_entry()]).to_tsv(jobs_file)

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert pipeline_queue.entries[0].job_submission_time == "2025-01-01T00:15:00+00:00"
    assert pipeline_queue.entries[0].queue_wait_seconds == 900
    assert pipeline_queue.entries[0].run_duration_seconds == 2700


@pytest.mark.ai_generated
def test_pipeline_queue_from_tsv_reads_table_without_submission_column(tmp_path: pathlib.Path) -> None:
    """A table written before job_submission_time existed still parses, without durations."""
    jobs_file = tmp_path / "jobs.tsv"
    tsv_text = PipelineQueue(entries=[_make_entry()]).to_tsv_string()
    header, row = (line.split("\t") for line in tsv_text.splitlines())
    dropped_columns = {"job_submission_time", "queue_wait_seconds", "run_duration_seconds"}
    keep = [index for index, name in enumerate(header) if name not in dropped_columns]
    legacy_lines = ["\t".join([line[index] for index in keep]) for line in (header, row)]
    jobs_file.write_text("\n".join(legacy_lines) + "\n")

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert pipeline_queue.entries[0].job_submission_time is None
    assert pipeline_queue.entries[0].queue_wait_seconds is None
    assert pipeline_queue.entries[0].run_duration_seconds is None


@pytest.mark.ai_generated
def test_example_queue_reads_paths_from_sibling_table(example_pipeline_queue: PipelineQueue) -> None:
    """The example queue picks up the asset paths recorded in the paths.tsv beside it."""
    entry = example_pipeline_queue.entry_for(dandi_path="sub-successful")
    capsule_path = entry.capsule_path()

    assert entry.output_paths == {f"{capsule_path}/derivatives/output.nwb": "95557b8d-acb3-59bb-bf23-d9fc29ff0eed"}
    assert entry.log_paths == {f"{capsule_path}/logs/stdout.txt": "a366e135-9bbf-53c4-88ae-a4f7d938e313"}
    assert all(capsule.dataset_description_path != {} for capsule in example_pipeline_queue)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "path",
    [f"{_CAPSULE_PATH}/code/submit.sh", "elsewhere/logs/stdout.txt"],
    ids=["unmapped_subpath", "outside_capsule"],
)
def test_pipeline_queue_from_tsv_skips_paths_it_cannot_place(tmp_path: pathlib.Path, path: str) -> None:
    """A paths.tsv row that falls under none of the mappings is left out rather than guessed at."""
    entry = _make_entry(dataset_description_path={}, output_paths={}, log_paths={})
    jobs_file = tmp_path / "jobs.tsv"
    PipelineQueue(entries=[entry]).to_tsv(jobs_file)
    (tmp_path / "paths.tsv").write_text(f"job_id\tpath\tcontent_id\njob-250101abc123\t{path}\tsome-id\n")

    pipeline_queue = PipelineQueue.from_tsv(jobs_file)

    assert pipeline_queue.entries[0].dataset_description_path == {}
    assert pipeline_queue.entries[0].output_paths == {}
    assert pipeline_queue.entries[0].log_paths == {}
