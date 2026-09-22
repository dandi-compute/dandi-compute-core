import hashlib
import json
import pathlib

import jsonschema
import pytest

from dandi_compute_code.lfp_pipeline import load_lfp_parameters, validate_lfp_parameters

_VALID_PARAMETERS = {
    "filter_family": "butter",
    "filter_band": [1, 400],
    "filter_order": 4,
    "filter_direction": "zero-phase",
    "reference_scheme": "CMR",
    "resample_target_fs": 1250,
    "spatial_factor": 1,
    "bad_channel_method": "coherence+psd",
}


@pytest.mark.ai_generated
def test_load_default_parameters() -> None:
    parameters = load_lfp_parameters()

    assert parameters == _VALID_PARAMETERS


@pytest.mark.ai_generated
def test_load_default_parameters_via_explicit_key() -> None:
    assert load_lfp_parameters("default") == _VALID_PARAMETERS


@pytest.mark.ai_generated
def test_unknown_key_raises() -> None:
    with pytest.raises(ValueError, match="is not registered"):
        load_lfp_parameters("does-not-exist")


@pytest.mark.ai_generated
def test_registered_params_md5_matches_files() -> None:
    repository_root = pathlib.Path(__file__).resolve().parents[3]
    pipeline_dir = repository_root / "src" / "dandi_compute_code" / "lfp_pipeline"
    registry = json.loads((pipeline_dir / "registries" / "registered_params.json").read_text())

    assert "default" in registry
    for entry in registry.values():
        params_path = pipeline_dir / "params" / entry["path"]
        assert params_path.exists()
        assert hashlib.md5(params_path.read_bytes()).hexdigest() == entry["md5"]  # noqa: S324


@pytest.mark.ai_generated
def test_validate_accepts_valid_parameters() -> None:
    assert validate_lfp_parameters(dict(_VALID_PARAMETERS)) == _VALID_PARAMETERS


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("filter_family", "chebyshev"),
        ("filter_band", [2, 400]),
        ("filter_order", 3),
        ("filter_direction", "backward"),
        ("reference_scheme", "average"),
        ("resample_target_fs", 30000),
        ("spatial_factor", 2),
        ("bad_channel_method", "std"),
    ],
)
def test_validate_rejects_invalid_values(field: str, value: object) -> None:
    parameters = dict(_VALID_PARAMETERS)
    parameters[field] = value

    with pytest.raises(jsonschema.ValidationError):
        validate_lfp_parameters(parameters)


@pytest.mark.ai_generated
def test_validate_rejects_missing_field() -> None:
    parameters = dict(_VALID_PARAMETERS)
    del parameters["filter_family"]

    with pytest.raises(jsonschema.ValidationError):
        validate_lfp_parameters(parameters)


@pytest.mark.ai_generated
def test_validate_rejects_unknown_field() -> None:
    parameters = dict(_VALID_PARAMETERS)
    parameters["highpass_spatial_filter"] = True

    with pytest.raises(jsonschema.ValidationError):
        validate_lfp_parameters(parameters)
