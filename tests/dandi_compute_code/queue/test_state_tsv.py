import csv
import io
import json
import pathlib

import pytest

from dandi_compute_code.queue import JobCapsule, JobInfo, PipelineQueue


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
        "has_code": True,
        "has_been_submitted": True,
        "has_output": True,
        "has_logs": True,
        "created_at": "2025-01-01T00:00:00+00:00",
        "job_completion_time": "2025-01-01T01:00:00+00:00",
        "dataset_description_path": {"a/dataset_description.json": "dd-id"},
        "output_paths": {"a/output.nwb": "out-id"},
        "log_paths": {"a/logs/stdout.txt": "log-id"},
    }
    for key, value in overrides.items():
        if key in job_kwargs:
            job_kwargs[key] = value
        else:
            entry_kwargs[key] = value
    return JobCapsule(job=JobInfo(**job_kwargs), **entry_kwargs)


@pytest.mark.ai_generated
def test_job_capsule_to_tsv_row_flattens_nested_dicts_as_json() -> None:
    """JobCapsule.to_tsv_row serialises nested path/content-id mappings as JSON strings."""
    entry = _make_entry()
    row = entry.to_tsv_row()
    assert row["dandiset_id"] == "001849"
    assert row["has_code"] == "True"
    assert row["asset_size_bytes"] == "1234"
    assert json.loads(row["output_paths"]) == {"a/output.nwb": "out-id"}
    assert json.loads(row["log_paths"]) == {"a/logs/stdout.txt": "log-id"}


@pytest.mark.ai_generated
def test_job_capsule_to_tsv_row_empty_dict_becomes_empty_cell() -> None:
    """JobCapsule.to_tsv_row writes an empty string for empty nested mappings."""
    entry = _make_entry(has_output=False, output_paths={}, has_logs=False, log_paths={})
    row = entry.to_tsv_row()
    assert row["output_paths"] == ""
    assert row["log_paths"] == ""


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
    output_file = tmp_path / "state.tsv"
    state.to_tsv(output_file)
    assert output_file.exists()
    assert output_file.read_text() == state.to_tsv_string()


@pytest.mark.ai_generated
def test_pipeline_queue_from_tsv_preserves_dataset_description_path(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.from_tsv preserves dataset_description_path entries."""
    state_file = tmp_path / "state.tsv"
    dataset_description_path = {
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678/"
        "dataset_description.json": "dataset-description-id"
    }
    entry = _make_entry(dataset_description_path=dataset_description_path)
    PipelineQueue(entries=[entry]).to_tsv(state_file)

    pipeline_queue = PipelineQueue.from_tsv(state_file)

    assert len(pipeline_queue) == 1
    assert pipeline_queue.entries[0].dataset_description_path == dataset_description_path


@pytest.mark.ai_generated
def test_pipeline_queue_empty_dataset_description_path_cell(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.from_tsv converts an empty dataset_description_path cell to an empty dict."""
    state_file = tmp_path / "state.tsv"
    entry = _make_entry(dataset_description_path={})
    PipelineQueue(entries=[entry]).to_tsv(state_file)

    pipeline_queue = PipelineQueue.from_tsv(state_file)

    assert len(pipeline_queue) == 1
    assert pipeline_queue.entries[0].dataset_description_path == {}
