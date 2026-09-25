import re

import pytest

from dandi_compute_code.lfp_pipeline import build_lfp_job_hash, build_lfp_pipeline_path


@pytest.mark.ai_generated
def test_build_pipeline_path_layout() -> None:
    pipeline_path = build_lfp_pipeline_path(dandiset_id="000409", output_within_dandiset_path="sub-01/sub-01_ecephys")

    assert pipeline_path == "derivatives/dandisets-000/dandiset-000409/sub-01/sub-01_ecephys/pipeline-lfp"


@pytest.mark.ai_generated
def test_build_job_hash_is_a_short_hex_digest() -> None:
    job_hash = build_lfp_job_hash(
        dandiset_id="000409",
        within_dandiset_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id="2f6768c",
        content_id="content-aaa",
    )

    assert re.fullmatch(r"[0-9a-f]{6}", job_hash) is not None


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("params_id", "content_id"),
    [
        pytest.param("0000000", "content-aaa", id="different_params"),
        pytest.param("2f6768c", "content-bbb", id="different_asset"),
    ],
)
def test_build_job_hash_differs_per_identifying_field(params_id: str, content_id: str) -> None:
    baseline_job_hash = build_lfp_job_hash(
        dandiset_id="000409",
        within_dandiset_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id="2f6768c",
        content_id="content-aaa",
    )
    job_hash = build_lfp_job_hash(
        dandiset_id="000409",
        within_dandiset_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id=params_id,
        content_id=content_id,
    )

    assert job_hash != baseline_job_hash
