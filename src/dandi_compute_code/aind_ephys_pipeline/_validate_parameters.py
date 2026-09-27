import hashlib
import json
import logging
import pathlib

import beartype
import jsonschema.validators

from ._pipeline_version import _parse_pipeline_version
from ..schemas import validate_registry

_log = logging.getLogger(__name__)

_PARAMS_SCHEMAS_DIRECTORY = pathlib.Path(__file__).parent / "params_schemas"
_PARAMS_SCHEMAS_REGISTRY_FILE_PATH = pathlib.Path(__file__).parent / "registries" / "registered_params_schemas.json"
_BANNER_RULE = "!" * 100


class InvalidParametersError(ValueError):
    """Raised when a parameters file does not conform to the schema of the requested pipeline version."""


@beartype.beartype
def _banner(*, title: str, body: str) -> str:
    message = f"\n{_BANNER_RULE}\n{title}\n{_BANNER_RULE}\n{body}\n{_BANNER_RULE}"
    return message


@beartype.beartype
def _load_params_schema(pipeline_version: str, /) -> tuple[str, dict] | None:
    """
    The name and contents of the parameters schema registered for *pipeline_version*.

    Returns ``None`` for versions older than every registered schema, which predate the
    upstream schema. A newer version with no registered schema is an error, so that a
    pipeline release cannot be run without first registering the schema it ships.
    """
    registry = json.loads(_PARAMS_SCHEMAS_REGISTRY_FILE_PATH.read_text())
    validate_registry(registry, description=f"registry '{_PARAMS_SCHEMAS_REGISTRY_FILE_PATH.name}'")
    registered_versions = {
        _parse_pipeline_version(version, label="registered parameters schema"): entry
        for version, entry in registry.items()
    }

    requested_version = _parse_pipeline_version(pipeline_version, label="requested pipeline")
    if requested_version < min(registered_versions):
        return None
    if requested_version not in registered_versions:
        body = (
            f"No parameters schema is registered for pipeline version {pipeline_version!r}.\n"
            f"Registered versions are: {sorted(registry)}.\n\n"
            "Every pipeline version from the oldest registered one onward must have its parameters schema "
            "registered before any job capsule can be formed against it.\n"
            "Copy `pipeline/default_params_schema.json` from that release of the AIND ephys pipeline into "
            "`params_schemas/` and add an entry to `registries/registered_params_schemas.json`."
        )
        message = _banner(title="UNREGISTERED AIND EPHYS PARAMETERS SCHEMA. NO JOB CAPSULE WAS CREATED.", body=body)
        raise ValueError(message)

    entry = registered_versions[requested_version]
    schema_file_path = _PARAMS_SCHEMAS_DIRECTORY / entry["path"]
    actual_md5 = hashlib.md5(schema_file_path.read_bytes()).hexdigest()
    if actual_md5 != entry["md5"]:
        message = (
            f"MD5 mismatch for parameters schema file '{schema_file_path.name}': "
            f"expected {entry['md5']!r}, got {actual_md5!r}. "
            "The file may have been modified. Update the `md5` in `registries/registered_params_schemas.json` "
            "to reflect the new file contents."
        )
        raise ValueError(message)
    schema = json.loads(schema_file_path.read_text())
    return schema_file_path.name, schema


@beartype.beartype
def validate_aind_ephys_parameters(*, parameters: dict, pipeline_version: str, parameters_file_name: str) -> None:
    """
    Validate AIND ephys parameters against the schema registered for a pipeline version.

    This runs before any job capsule is formed, so parameters the pipeline would reject or crash
    on never reach the queue. Versions older than every registered schema predate the upstream
    schema and are not validated.

    Parameters
    ----------
    parameters : dict
        The loaded contents of the parameters file.
    pipeline_version : str
        The pipeline version the parameters will be run with, such as ``"v1.3.3"``.
    parameters_file_name : str
        The name of the parameters file, used in the error message.

    Raises
    ------
    InvalidParametersError
        If the parameters do not conform to the schema. The message lists every problem found.
    ValueError
        If no schema is registered for a version at or after the oldest registered one, or the
        registered schema file does not match its MD5.
    """
    loaded_schema = _load_params_schema(pipeline_version)
    if loaded_schema is None:
        _log.warning(
            f"Pipeline version {pipeline_version!r} predates every registered parameters schema. "
            f"Skipping schema validation of '{parameters_file_name}'."
        )
        return
    schema_file_name, schema = loaded_schema

    validator_class = jsonschema.validators.validator_for(schema)
    validator_class.check_schema(schema)
    validator = validator_class(schema)
    errors = sorted(validator.iter_errors(parameters), key=lambda error: error.json_path)
    if not errors:
        return

    problems = "\n".join(f"  {index}. At '{error.json_path}': {error.message}" for index, error in enumerate(errors, 1))
    body = (
        f"Parameters file '{parameters_file_name}' does not conform to the parameters schema for pipeline "
        f"version {pipeline_version!r} ('{schema_file_name}').\n"
        f"Found {len(errors)} problem(s).\n\n"
        f"{problems}\n\n"
        "Write a parameters file for this pipeline version and register it under a new key in "
        "`registries/registered_params.json`, or pick a parameters key written for this pipeline version."
    )
    message = _banner(title="INVALID AIND EPHYS PARAMETERS. NO JOB CAPSULE WAS CREATED.", body=body)
    raise InvalidParametersError(message)
