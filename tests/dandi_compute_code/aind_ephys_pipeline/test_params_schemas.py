"""
Every AIND ephys parameters file and parameters schema shipped with this package is checked here.

These are the CI guard against registering parameters the pipeline would reject or crash on.
A parameters file must conform to the schema registered for the pipeline version it targets,
and the ``default`` key must conform to the newest registered schema, which is the version new
job capsules are formed against.
"""

import copy
import hashlib
import json
import pathlib

import jsonschema.validators
import pytest

import dandi_compute_code
from dandi_compute_code.aind_ephys_pipeline import InvalidParametersError, validate_aind_ephys_parameters

_PIPELINE_DIRECTORY = pathlib.Path(dandi_compute_code.__file__).parent / "aind_ephys_pipeline"
_PARAMS_REGISTRY = json.loads((_PIPELINE_DIRECTORY / "registries" / "registered_params.json").read_text())
_SCHEMAS_REGISTRY = json.loads((_PIPELINE_DIRECTORY / "registries" / "registered_params_schemas.json").read_text())
_NEWEST_SCHEMA_VERSION = max(_SCHEMAS_REGISTRY, key=lambda version: tuple(int(part) for part in version.split(".")))


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
@pytest.mark.parametrize("version", sorted(_SCHEMAS_REGISTRY))
def test_registered_params_schemas_are_valid_json_schemas(version: str) -> None:
    """Every registered parameters schema exists, still has its registered MD5, and is itself a valid JSON Schema."""
    schema_file_path = _PIPELINE_DIRECTORY / "params_schemas" / _SCHEMAS_REGISTRY[version]["path"]
    schema = json.loads(schema_file_path.read_text())

    assert hashlib.md5(schema_file_path.read_bytes()).hexdigest() == _SCHEMAS_REGISTRY[version]["md5"]  # noqa: S324
    jsonschema.validators.validator_for(schema).check_schema(schema)


@pytest.mark.ai_generated
@pytest.mark.parametrize("key", sorted(_PARAMS_REGISTRY))
def test_registered_params_conform_to_the_schema_of_their_own_pipeline_version(key: str) -> None:
    """A parameters file conforms to the schema of the pipeline version it declares it was written for."""
    parameters = _load_params(key)

    validate_aind_ephys_parameters(
        parameters=parameters,
        pipeline_version=parameters["pipeline_version"],
        parameters_file_name=_PARAMS_REGISTRY[key]["path"],
    )


@pytest.mark.ai_generated
def test_default_params_conform_to_the_newest_registered_schema() -> None:
    """The ``default`` key is usable with the newest pipeline version that has a registered schema."""
    validate_aind_ephys_parameters(
        parameters=_load_params("default"),
        pipeline_version=_NEWEST_SCHEMA_VERSION,
        parameters_file_name=_PARAMS_REGISTRY["default"]["path"],
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
            parameters=parameters, pipeline_version="v1.3.3", parameters_file_name="name-example.json"
        )

    assert "NO JOB CAPSULE WAS CREATED" in str(error_info.value)
    assert "name-example.json" in str(error_info.value)
    assert expected_json_path in str(error_info.value)


@pytest.mark.ai_generated
@pytest.mark.parametrize("pipeline_version", ["v1.3.4", "v1.4.0", "2.0.0"])
def test_pipeline_versions_newer_than_every_registered_schema_are_refused(pipeline_version: str) -> None:
    """A pipeline release cannot be run until the parameters schema it ships is registered."""
    with pytest.raises(ValueError, match="UNREGISTERED AIND EPHYS PARAMETERS SCHEMA"):
        validate_aind_ephys_parameters(
            parameters=_load_params("default"), pipeline_version=pipeline_version, parameters_file_name="name-x.json"
        )


@pytest.mark.ai_generated
@pytest.mark.parametrize("pipeline_version", ["v1.1.0", "v1.2.4", "v1.0.0-fixes"])
def test_pipeline_versions_older_than_every_registered_schema_are_not_validated(pipeline_version: str) -> None:
    """Versions that predate the upstream parameters schema skip validation."""
    validate_aind_ephys_parameters(
        parameters={"not": "valid"}, pipeline_version=pipeline_version, parameters_file_name="name-x.json"
    )
