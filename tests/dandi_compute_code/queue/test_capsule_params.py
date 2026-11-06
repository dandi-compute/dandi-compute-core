from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import read_capsule_params

_CAPSULE_PATH = "derivatives/dandiset-000409/sub-mouse01/pipeline-aind+ephys/job-240101abc123"
_CODE_DIR_PATH = f"{_CAPSULE_PATH}/code"


def _metadata_with_provenance(provenance: dict | None) -> tuple[AssetsJsonldMetadata, dict]:
    """Assets metadata holding one capsule, and the dataset_description.json carrying *provenance*."""
    description_path = f"{_CAPSULE_PATH}/dataset_description.json"
    asset = {"path": description_path, "contentUrl": ["https://example.invalid/blobs/desc"]}
    content = {"Name": "x"} if provenance is None else {"Name": "x", "DandiCompute": provenance}
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={"desc": asset},
        path_to_asset_metadata={
            description_path: AssetMetadata(
                path=description_path, date_modified="2024-01-01T00:00:00Z", content_size=1, content_id="desc"
            )
        },
    )
    return metadata, content


@pytest.mark.ai_generated
def test_reads_the_params_id_out_of_each_capsules_provenance() -> None:
    """The params ID is the one the capsule recorded when it was formed."""
    metadata, content = _metadata_with_provenance({"params": "1157294", "config": "c749cf9"})

    with mock.patch("dandi_compute_code.queue._queue_utils._read_asset_json", return_value=content):
        capsule_params = read_capsule_params([_CODE_DIR_PATH], metadata=metadata)

    assert capsule_params == {_CODE_DIR_PATH: "1157294"}


@pytest.mark.ai_generated
@pytest.mark.parametrize("provenance", [None, {}, {"config": "c749cf9"}, {"params": ""}])
def test_leaves_out_a_capsule_whose_parameters_cannot_be_read(provenance: dict | None) -> None:
    """A capsule recording no parameters is left out rather than guessed at."""
    metadata, content = _metadata_with_provenance(provenance)

    with mock.patch("dandi_compute_code.queue._queue_utils._read_asset_json", return_value=content):
        capsule_params = read_capsule_params([_CODE_DIR_PATH], metadata=metadata)

    assert capsule_params == {}


@pytest.mark.ai_generated
def test_leaves_out_a_capsule_with_no_dataset_description_asset() -> None:
    """A capsule the metadata does not describe is left out rather than guessed at."""
    empty_metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={})

    assert read_capsule_params([_CODE_DIR_PATH], metadata=empty_metadata) == {}


@pytest.mark.ai_generated
def test_nothing_to_read_fetches_nothing() -> None:
    """An empty request does not touch the archive."""
    with mock.patch("dandi_compute_code.queue._capsule_params.load_assets_jsonld_metadata") as mock_load:
        assert read_capsule_params([]) == {}

    mock_load.assert_not_called()
