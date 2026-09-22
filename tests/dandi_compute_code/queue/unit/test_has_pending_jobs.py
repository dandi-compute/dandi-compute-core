from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import PipelineQueue


def _build_metadata(paths: list[str]) -> AssetsJsonldMetadata:
    """Build minimal assets metadata indexed by path for the given asset paths."""
    path_to_asset_metadata = {
        path: AssetMetadata(
            path=path,
            date_modified="2026-01-01T00:00:00Z",
            content_size=1,
            content_id=f"content-{index}",
        )
        for index, path in enumerate(paths)
    }
    return AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata=path_to_asset_metadata,
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("paths", "expected"),
    [
        pytest.param([], False, id="empty-metadata"),
        pytest.param(
            ["sub-1/capsule-a/code/submit.sh"],
            True,
            id="single-unsubmitted",
        ),
        pytest.param(
            ["sub-1/capsule-a/code/submit.sh", "sub-1/capsule-a/code/submitted"],
            False,
            id="submitted-plain-marker",
        ),
        pytest.param(
            [
                "sub-1/capsule-a/code/submit.sh",
                "sub-1/capsule-a/code/submitted_date-2026+01+01_time-00+00+00",
            ],
            False,
            id="submitted-dated-marker",
        ),
        pytest.param(
            [
                "sub-1/capsule-a/code/submit.sh",
                "sub-1/capsule-a/code/submitted",
                "sub-2/capsule-a/code/submit.sh",
            ],
            True,
            id="mixed-one-pending",
        ),
        pytest.param(
            ["sub-1/capsule-a/code/some_other_file.txt"],
            False,
            id="no-submit-script",
        ),
    ],
)
def test_has_pending_jobs(paths: list[str], expected: bool) -> None:
    """has_pending_jobs reflects whether any submit.sh lacks a submitted marker."""
    metadata = _build_metadata(paths)
    with mock.patch(
        "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
        return_value=metadata,
    ):
        result = PipelineQueue.has_pending_jobs()

    assert result is expected
