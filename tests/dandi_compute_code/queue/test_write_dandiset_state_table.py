from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import PipelineQueue

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"


@pytest.mark.ai_generated
def test_write_dandiset_state_table_builds_state_and_uploads() -> None:
    """write_dandiset_state_table builds the state from the given Dandiset and uploads state.tsv and paths.tsv."""
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
        PipelineQueue.write_dandiset_state_table(dandiset_id=_JOB_CAPSULES_DANDISET_ID)

    state_kwargs, paths_kwargs = (call.kwargs for call in mock_write_file.call_args_list)
    assert state_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert state_kwargs["relative_path"] == "derivatives/state.tsv"
    assert "dandiset_id\t" in state_kwargs["content"].splitlines()[0]
    assert source_path in state_kwargs["content"]
    assert paths_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert paths_kwargs["relative_path"] == "derivatives/paths.tsv"
    assert paths_kwargs["content"].splitlines()[0] == "job_id\tpath\tcontent_id"


@pytest.mark.ai_generated
def test_write_dandiset_state_table_empty_state_writes_header_only() -> None:
    """write_dandiset_state_table uploads header-only state and paths tables when there are no entries."""
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
        mock.patch("dandi_compute_code.queue._pipeline_queue.write_dandiset_file") as mock_write_file,
    ):
        PipelineQueue.write_dandiset_state_table(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)

    assert [call.kwargs["relative_path"] for call in mock_write_file.call_args_list] == [
        "derivatives/state.tsv",
        "derivatives/paths.tsv",
    ]
    for call in mock_write_file.call_args_list:
        assert call.kwargs["dandiset_id"] == _FAILED_RUNS_ARCHIVE_DANDISET_ID
        assert len(call.kwargs["content"].splitlines()) == 1
