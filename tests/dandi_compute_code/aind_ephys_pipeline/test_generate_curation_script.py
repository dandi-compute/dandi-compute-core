"""Tests for the SpikeInterface GUI curation script generated from a job capsule."""

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.aind_ephys_pipeline import generate_curation_script
from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata

_CAPSULE_PATH = (
    "derivatives/dandisets-000/dandiset-000409/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/job-260916a1b2c3"
)
_OTHER_CAPSULE_PATH = (
    "derivatives/dandisets-000/dandiset-000409/sub-mouse02/sub-mouse02_ecephys/pipeline-aind+ephys/job-260917d4e5f6"
)
_ASSET_PATHS_TO_ZARR_IDS = {
    f"{_CAPSULE_PATH}/derivatives/postprocessed/block0_ProbeB_recording1.zarr": "zarr-id-probe-b",
    f"{_CAPSULE_PATH}/derivatives/postprocessed/block0_ProbeA_recording1.zarr": "zarr-id-probe-a",
    f"{_CAPSULE_PATH}/derivatives/nwb/sub-mouse01_ecephys.nwb": "blob-id-nwb",
    f"{_CAPSULE_PATH}/code/submit.sh": "blob-id-submit",
    f"{_OTHER_CAPSULE_PATH}/code/submit.sh": "blob-id-other-submit",
}


@pytest.fixture
def assets_metadata(monkeypatch: pytest.MonkeyPatch) -> AssetsJsonldMetadata:
    path_to_asset_metadata = {
        path: AssetMetadata(path=path, date_modified="2026-09-17T00:00:00+00:00", content_size=1, content_id=content_id)
        for path, content_id in _ASSET_PATHS_TO_ZARR_IDS.items()
    }
    metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=path_to_asset_metadata)
    monkeypatch.setattr(
        "dandi_compute_code.aind_ephys_pipeline._generate_curation_script.load_assets_jsonld_metadata",
        lambda dandiset_id: metadata,
    )
    return metadata


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "capsule",
    [
        "job-260916a1b2c3",
        _CAPSULE_PATH,
        f"{_CAPSULE_PATH}/",
        "sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/job-260916a1b2c3",
    ],
)
def test_generate_curation_script_fills_in_zarr_ids(assets_metadata: AssetsJsonldMetadata, capsule: str) -> None:
    script = generate_curation_script(capsule=capsule)

    assert f"Job capsule: DANDI:001697 {_CAPSULE_PATH}/" in script
    assert '"block0_ProbeA_recording1": "s3://dandiarchive/zarr/zarr-id-probe-a/",' in script
    assert '"block0_ProbeB_recording1": "s3://dandiarchive/zarr/zarr-id-probe-b/",' in script
    assert 'STREAM = "block0_ProbeA_recording1"' in script
    assert 'pathlib.Path(f"job-260916a1b2c3_{STREAM}_curation.json")' in script
    assert "blob-id" not in script
    compile(script, "curate.py", "exec")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("capsule", "expected_message"),
    [
        ("sub-mouse01_ecephys", "is not a job ID"),
        ("job-260917d4e5f6", "No postprocessed outputs were found"),
        ("job-260101000000", "No postprocessed outputs were found"),
    ],
)
def test_generate_curation_script_rejects_capsules_without_analyzers(
    assets_metadata: AssetsJsonldMetadata, capsule: str, expected_message: str
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        generate_curation_script(capsule=capsule)


@pytest.mark.ai_generated
def test_generate_curation_script_rejects_ambiguous_job_ids(
    assets_metadata: AssetsJsonldMetadata,
) -> None:
    duplicate_path = _CAPSULE_PATH.replace("sub-mouse01", "sub-mouse03")
    duplicate_asset_path = f"{duplicate_path}/derivatives/postprocessed/block0_ProbeA_recording1.zarr"
    assets_metadata.path_to_asset_metadata[duplicate_asset_path] = AssetMetadata(
        path=duplicate_asset_path, date_modified="2026-09-17T00:00:00+00:00", content_size=1, content_id="zarr-id-dup"
    )

    with pytest.raises(ValueError, match="matches more than one capsule"):
        generate_curation_script(capsule="job-260916a1b2c3")

    script = generate_curation_script(capsule=duplicate_path)
    assert "s3://dandiarchive/zarr/zarr-id-dup/" in script


@pytest.mark.ai_generated
def test_curate_command_prints_script(assets_metadata: AssetsJsonldMetadata) -> None:
    result = CliRunner().invoke(_dandicompute_group, ["curate", "--job", "job-260916a1b2c3"])

    assert result.exit_code == 0
    assert result.output == generate_curation_script(capsule="job-260916a1b2c3")


@pytest.mark.ai_generated
def test_curate_command_reports_missing_outputs(assets_metadata: AssetsJsonldMetadata) -> None:
    result = CliRunner().invoke(_dandicompute_group, ["curate", "--job", "job-260917d4e5f6"])

    assert result.exit_code == 1
    assert "No postprocessed outputs were found" in result.output
