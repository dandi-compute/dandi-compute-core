"""
Shared fixtures for the job capsule creation tests.

Creation reaches four boundaries that cannot run in CI: the packaged pipeline configuration,
the latest locally available pipeline version, the qualifying-content-ids download, and the
per-asset job preparation. The fixtures here neutralise the ones every test needs.
"""

from collections.abc import Iterator
from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetsJsonldMetadata


@pytest.fixture(autouse=True)
def mock_latest_pipeline_version() -> Iterator[mock.MagicMock]:
    """
    Resolve every pipeline's latest version without touching a local repository checkout.

    Tests that care which version was used read it back from ``return_value``.
    """
    with mock.patch(
        "dandi_compute_code.jobs._create_job_capsules.resolve_latest_pipeline_version",
        return_value="v9.9.9",
    ) as mock_resolve:
        yield mock_resolve


@pytest.fixture(autouse=True)
def mock_dandi_assets_metadata() -> Iterator[None]:
    """Default the live queue state to empty so no test hits the DANDI archive."""
    empty_metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={})
    with mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=empty_metadata):
        yield
