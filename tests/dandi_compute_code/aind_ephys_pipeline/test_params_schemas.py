"""
Every AIND ephys parameters file shipped with this package is checked here.

These are the CI guard against registering parameters the pipeline would reject or crash on.
A parameters file must conform to the schema the pipeline repository ships at the version the
file targets. The schema is fetched from the upstream repository, the only source of truth.
"""

import copy
import hashlib
import json
import pathlib

import pytest

import dandi_compute_code
from dandi_compute_code.aind_ephys_pipeline import InvalidParametersError, validate_aind_ephys_parameters

_PIPELINE_DIRECTORY = pathlib.Path(dandi_compute_code.__file__).parent / "aind_ephys_pipeline"
_PARAMS_REGISTRY = json.loads((_PIPELINE_DIRECTORY / "registries" / "registered_params.json").read_text())


def _load_params(key: str, /) -> dict:
    parameters = json.loads((_PIPELINE_DIRECTORY / "params" / _PARAMS_REGISTRY[key]["path"]).read_text())
    return parameters


@pytest.mark.ai_generated
@pytest.mark.parametrize("key", sorted(_PARAMS_REGISTRY))
def test_registered_params_md5_matches_files(key: str) -> None:
    """Every registered parameters file exists and still has its registered MD5."""
    params_file_path = _PIPELINE_DIRECTORY / "params" / _PARAMS_REGISTRY[key]["path"]

    assert hashlib.md5(params_file_path.read_bytes()).hexdigest() == _PARAMS_REGISTRY[key]["md5"]  # noqa: S324


@pytest.mark.ai_generated
@pytest.mark.parametrize("key", sorted(_PARAMS_REGISTRY))
def test_registered_params_conform_to_the_schema_of_their_own_pipeline_version(key: str) -> None:
    """A parameters file conforms to the upstream schema of the pipeline version it declares it was written for."""
    parameters = _load_params(key)

    validate_aind_ephys_parameters(
        parameters=parameters,
        pipeline_version=parameters["pipeline_version"],
        parameters_file_name=_PARAMS_REGISTRY[key]["path"],
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("path", "value", "expected_json_path"),
    [
        (("preprocessing", "motion_correction", "compute"), True, "$.preprocessing.motion_correction"),
        (("preprocessing", "motion_correction", "preset"), "not_a_preset", "$.preprocessing.motion_correction.preset"),
        (("pipeline_version",), "1.2.4", "$.pipeline_version"),
    ],
)
def test_invalid_params_raise_an_obvious_error(path: tuple[str, ...], value: object, expected_json_path: str) -> None:
    """Parameters that break the schema raise with a banner naming the file and where each problem is."""
    parameters = copy.deepcopy(_load_params("default"))
    container = parameters
    for part in path[:-1]:
        container = container[part]
    container[path[-1]] = value

    with pytest.raises(InvalidParametersError, match="INVALID AIND EPHYS PARAMETERS") as error_info:
        validate_aind_ephys_parameters(
            parameters=parameters, pipeline_version="1.3.3", parameters_file_name="name-example.json"
        )

    assert "NO JOB CAPSULE WAS CREATED" in str(error_info.value)
    assert "name-example.json" in str(error_info.value)
    assert expected_json_path in str(error_info.value)


@pytest.mark.ai_generated
@pytest.mark.parametrize("pipeline_version", ["1.3.3", "v1.3.3"])
def test_the_schema_is_fetched_with_or_without_a_v_prefix(pipeline_version: str) -> None:
    """Upstream tags carry no ``v`` prefix, but a prefixed version still finds the schema."""
    validate_aind_ephys_parameters(
        parameters=_load_params("default"), pipeline_version=pipeline_version, parameters_file_name="name-x.json"
    )


@pytest.mark.ai_generated
def test_a_pipeline_version_without_an_upstream_schema_is_refused() -> None:
    """A version the pipeline repository has no schema for cannot have its parameters checked, so it is refused."""
    with pytest.raises(ValueError, match="AIND EPHYS PARAMETERS SCHEMA NOT FOUND"):
        validate_aind_ephys_parameters(
            parameters=_load_params("default"), pipeline_version="v99.0.0", parameters_file_name="name-x.json"
        )


@pytest.mark.ai_generated
@pytest.mark.parametrize("pipeline_version", ["v1.1.0", "1.1.1", "v1.0.0-fixes"])
def test_pipeline_versions_before_the_upstream_schema_are_not_validated(pipeline_version: str) -> None:
    """Versions before v1.2.0, the first release to ship a parameters schema, skip validation."""
    validate_aind_ephys_parameters(
        parameters={"not": "valid"}, pipeline_version=pipeline_version, parameters_file_name="name-x.json"
    )
