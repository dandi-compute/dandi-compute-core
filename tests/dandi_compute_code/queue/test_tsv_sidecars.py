import json
import pathlib

import pytest

from dandi_compute_code.queue import JOB_STATUSES, PipelineQueue

_EXAMPLE_JOBS_FILES_DIR = pathlib.Path(__file__).parent / "example_jobs_files"


def _header(tsv_string: str) -> list[str]:
    return tsv_string.splitlines()[0].split("\t")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("sidecar_string", "tsv_string"),
    [
        pytest.param(PipelineQueue.to_tsv_sidecar_string(), PipelineQueue(entries=[]).to_tsv_string(), id="jobs"),
        pytest.param(
            PipelineQueue.to_paths_tsv_sidecar_string(), PipelineQueue(entries=[]).to_paths_tsv_string(), id="paths"
        ),
    ],
)
def test_sidecar_describes_every_column_in_order(sidecar_string: str, tsv_string: str) -> None:
    """Each sidecar has one entry per column of its table, in column order, and each has a description."""
    sidecar = json.loads(sidecar_string)

    assert list(sidecar) == _header(tsv_string)
    for column in sidecar.values():
        assert column["Description"].strip() != ""
        assert "\n" not in column["Description"]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("sidecar_string", "example_file_name"),
    [
        pytest.param(PipelineQueue.to_tsv_sidecar_string(), "jobs.json", id="jobs"),
        pytest.param(PipelineQueue.to_paths_tsv_sidecar_string(), "paths.json", id="paths"),
    ],
)
def test_sidecar_matches_committed_example(sidecar_string: str, example_file_name: str) -> None:
    """Each sidecar matches the committed example exactly, so any change to it shows up in review."""
    assert sidecar_string == (_EXAMPLE_JOBS_FILES_DIR / example_file_name).read_text()


@pytest.mark.ai_generated
def test_jobs_sidecar_lists_status_levels() -> None:
    """The jobs.json sidecar lists every job status, each described, as the levels of the status column."""
    levels = json.loads(PipelineQueue.to_tsv_sidecar_string())["status"]["Levels"]

    assert tuple(levels) == JOB_STATUSES
    assert all(description.strip() != "" for description in levels.values())


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("column", "units"),
    [("asset_size_bytes", "B"), ("queue_wait_seconds", "s"), ("run_duration_seconds", "s")],
)
def test_jobs_sidecar_records_units(column: str, units: str) -> None:
    """The size and duration columns of jobs.json carry their units."""
    sidecar = json.loads(PipelineQueue.to_tsv_sidecar_string())

    assert sidecar[column]["Units"] == units


@pytest.mark.ai_generated
def test_to_tsv_writes_sidecars_beside_tables(tmp_path: pathlib.Path) -> None:
    """PipelineQueue.to_tsv writes jobs.json and paths.json beside jobs.tsv and paths.tsv."""
    PipelineQueue(entries=[]).to_tsv(tmp_path / "jobs.tsv")

    assert sorted(path.name for path in tmp_path.iterdir()) == ["jobs.json", "jobs.tsv", "paths.json", "paths.tsv"]
    assert (tmp_path / "jobs.json").read_text() == PipelineQueue.to_tsv_sidecar_string()
    assert (tmp_path / "paths.json").read_text() == PipelineQueue.to_paths_tsv_sidecar_string()
