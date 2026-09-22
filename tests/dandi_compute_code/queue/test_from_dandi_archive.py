from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID
from dandi_compute_code.queue import PipelineQueue

# PipelineQueue.from_dandi(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID) derives the archive
# counterpart of the queue state, from the failed runs archive Dandiset's assets.jsonld,
# fetched over the network. That loader is the one external boundary mocked here.


@pytest.mark.ai_generated
def test_from_dandi_reads_from_archive_dandiset_when_requested() -> None:
    """from_dandi(dandiset_id=archive) reads from the failed runs archive Dandiset metadata."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567/code/submit.sh"
    )
    load_metadata = mock.Mock(
        return_value=AssetsJsonldMetadata(
            content_id_to_asset={},
            path_to_asset_metadata={
                capsule_path: AssetMetadata(
                    path=capsule_path,
                    date_modified="2024-01-01T00:00:00+00:00",
                    content_size=1,
                    content_id="capsule-id",
                )
            },
        )
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            load_metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2024-01-01T00:00:00+00:00",
                        content_size=1234,
                        content_id="source-id",
                    )
                },
            ),
        ),
    ):
        state = PipelineQueue.from_dandi(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)

    # The archive metadata is read from the failed runs archive Dandiset, not the job capsules one.
    load_metadata.assert_called_once_with(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)

    archive_entries = [entry.to_dict() for entry in state]
    assert len(archive_entries) == 1
    assert archive_entries[0]["dandi_path"] == source_path
    assert archive_entries[0]["content_id"] == "source-id"
    assert archive_entries[0]["status"] == "pending"


@pytest.mark.ai_generated
def test_from_dandi_returns_empty_when_no_capsules_in_archive() -> None:
    """from_dandi(dandiset_id=archive) returns an empty state when no job capsules are present."""
    with mock.patch(
        "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
    ):
        state = PipelineQueue.from_dandi(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)

    assert len(state) == 0
