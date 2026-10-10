import pathlib
from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import PipelineQueue

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"


@pytest.mark.ai_generated
def test_write_dandiset_jobs_table_builds_state_and_uploads() -> None:
    """write_dandiset_jobs_table builds the state from the given Dandiset and uploads both tables and sidecars."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/"
        "job-240101def567/code/submit.sh"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            capsule_path: AssetMetadata(
                path=capsule_path,
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=1,
                content_id="capsule-code-id",
            )
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="content-id-1",
            )
        },
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        PipelineQueue.write_dandiset_jobs_table(dandiset_id=_JOB_CAPSULES_DANDISET_ID)

    state_kwargs, state_sidecar_kwargs, paths_kwargs, paths_sidecar_kwargs = (
        call.kwargs for call in mock_write_file.call_args_list
    )
    assert state_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert state_kwargs["relative_path"] == "derivatives/jobs.tsv"
    assert "dandiset_id\t" in state_kwargs["content"].splitlines()[0]
    assert source_path in state_kwargs["content"]
    assert paths_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert paths_kwargs["relative_path"] == "derivatives/paths.tsv"
    assert paths_kwargs["content"].splitlines()[0] == "job_id\tpath\tcontent_id"
    assert state_sidecar_kwargs["relative_path"] == "derivatives/jobs.json"
    assert state_sidecar_kwargs["content"] == PipelineQueue.to_tsv_sidecar_string()
    assert paths_sidecar_kwargs["relative_path"] == "derivatives/paths.json"
    assert paths_sidecar_kwargs["content"] == PipelineQueue.to_paths_tsv_sidecar_string()


@pytest.mark.ai_generated
def test_write_dandiset_jobs_table_empty_state_writes_header_only() -> None:
    """write_dandiset_jobs_table uploads header-only tables, and their sidecars, when there are no entries."""
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        PipelineQueue.write_dandiset_jobs_table(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)

    assert [call.kwargs["relative_path"] for call in mock_write_file.call_args_list] == [
        "derivatives/jobs.tsv",
        "derivatives/jobs.json",
        "derivatives/paths.tsv",
        "derivatives/paths.json",
    ]
    for call in mock_write_file.call_args_list:
        assert call.kwargs["dandiset_id"] == _FAILED_RUNS_ARCHIVE_DANDISET_ID
        if call.kwargs["relative_path"].endswith(".tsv"):
            assert len(call.kwargs["content"].splitlines()) == 1


def _write_empty_state(current_content_by_path: dict[str, str]) -> tuple[list[str], list[str]]:
    """Write the empty state's tables over *current_content_by_path*; the returned and the uploaded paths."""
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue._read_text_asset_at_path",
            side_effect=lambda *, metadata, path: current_content_by_path.get(path),
        ),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        returned_paths = PipelineQueue.write_dandiset_jobs_table(dandiset_id=_JOB_CAPSULES_DANDISET_ID)
    uploaded_paths = [call.kwargs["relative_path"] for call in mock_write_file.call_args_list]
    return returned_paths, uploaded_paths


@pytest.mark.ai_generated
def test_write_dandiset_jobs_table_skips_tables_already_up_to_date() -> None:
    """A refresh that finds every table unchanged uploads nothing."""
    up_to_date = {
        str(relative_path): content
        for relative_path, content in PipelineQueue(entries=[])
        ._tables_by_relative_path(pathlib.PurePosixPath("derivatives/jobs.tsv"))
        .items()
    }

    returned_paths, uploaded_paths = _write_empty_state(up_to_date)

    assert returned_paths == []
    assert uploaded_paths == []


@pytest.mark.ai_generated
def test_write_dandiset_jobs_table_uploads_only_changed_tables() -> None:
    """Only the tables whose content differs from the Dandiset's copy are uploaded, and returned."""
    current = {
        str(relative_path): content
        for relative_path, content in PipelineQueue(entries=[])
        ._tables_by_relative_path(pathlib.PurePosixPath("derivatives/jobs.tsv"))
        .items()
    }
    current["derivatives/jobs.tsv"] = "an older table\n"
    del current["derivatives/paths.tsv"]

    returned_paths, uploaded_paths = _write_empty_state(current)

    assert uploaded_paths == ["derivatives/jobs.tsv", "derivatives/paths.tsv"]
    assert returned_paths == uploaded_paths
