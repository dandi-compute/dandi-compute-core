"""
Shared fixtures for the ``PipelineQueue`` model test suite.

The network guard targets the binding used by the model
(:mod:`dandi_compute_code.queue._pipeline_queue`).
"""

import os
import pathlib
from collections.abc import Iterator
from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetsJsonldMetadata
from dandi_compute_code.queue import PipelineQueue

#: The committed example queue used as ground truth across the model tests.
EXAMPLE_STATE_FILE = pathlib.Path(__file__).parent / "example_state_files" / "state.tsv"


@pytest.fixture(autouse=True)
def mock_dandi_assets_metadata() -> Iterator[None]:
    """
    Default the DANDI ``assets.jsonld`` loaders to empty so no test hits the network.

    ``PipelineQueue.from_dandi`` / ``pending_code_dirs`` fetch assets metadata from the DANDI
    archive via the ``_pipeline_queue`` binding. This guard makes that return empty by default.
    Tests that need specific metadata override these with their own ``mock.patch``.
    """
    empty_metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={})
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=empty_metadata),
        mock.patch(
            "dandi_compute_code.queue._capsule_resources.load_assets_jsonld_metadata",
            return_value=empty_metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=empty_metadata,
        ),
    ):
        yield


@pytest.fixture
def example_pipeline_queue() -> PipelineQueue:
    """The committed example queue (``example_state_files/state.tsv``) loaded into a fresh model."""
    return PipelineQueue.from_tsv(EXAMPLE_STATE_FILE)


@pytest.fixture
def processing_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A directory for the temporary per-job working trees used during submission."""
    directory = tmp_path / "processing"
    directory.mkdir()
    return directory


@pytest.fixture
def dandi_api_key() -> Iterator[None]:
    """Provide a dummy DANDI_API_KEY for helpers that require it to be set."""
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        yield
