import hashlib
import json

import beartype
import jsonschema

from ._globals import _PARAMETER_SCHEMA_FILE_PATH, _PARAMS_DIR, _PARAMS_REGISTRY_FILE_PATH
from ..schemas import validate_registry


@beartype.beartype
def validate_lfp_parameters(parameters: dict, /) -> dict:
    """
    Validate a set of LFP parameters against the pipeline JSON schema.

    Parameters
    ----------
    parameters : dict
        The parameter mapping to validate.

    Returns
    -------
    dict
        The validated parameters, unchanged.

    Raises
    ------
    jsonschema.ValidationError
        If the parameters do not conform to ``parameter_schema.json``.
    """
    schema = json.loads(_PARAMETER_SCHEMA_FILE_PATH.read_text())
    jsonschema.validate(instance=parameters, schema=schema)
    return parameters


@beartype.beartype
def load_lfp_parameters(parameters_key: str = "default", /) -> dict:
    """
    Resolve a registered parameters key to a validated set of LFP parameters.

    Mirrors the AIND ephys pipeline approach. The registry itself is validated against
    ``schemas/registry.linkml.yaml``, the key is looked up in
    ``registries/registered_params.json``, the referenced file under ``params/`` is checked
    against its recorded MD5, and the loaded parameters are validated against
    ``parameter_schema.json``.

    Parameters
    ----------
    parameters_key : str
        The short name of the parameters to load. Must be a key registered in
        ``registries/registered_params.json``.

    Returns
    -------
    dict
        The validated parameters loaded from the registered file.

    Raises
    ------
    ValueError
        If the registry does not conform to its schema, if ``parameters_key`` is not
        registered, or if the MD5 checksum of the resolved file does not match its
        registry entry.
    jsonschema.ValidationError
        If the loaded parameters do not conform to ``parameter_schema.json``.
    """
    registry = json.loads(_PARAMS_REGISTRY_FILE_PATH.read_text())
    validate_registry(registry, description=f"registry '{_PARAMS_REGISTRY_FILE_PATH.name}'")
    if parameters_key not in registry:
        registered_keys = list(registry.keys())
        message = (
            f"Parameters key '{parameters_key}' is not registered. "
            f"Registered keys are: {registered_keys}. "
            "To register a new parameters file, add the JSON file to the `params/` directory "
            "and add an entry to `registries/registered_params.json` mapping the short name to its "
            "relative `path` and full MD5 `md5`."
        )
        raise ValueError(message)

    parameters_file_path = _PARAMS_DIR / registry[parameters_key]["path"]
    actual_md5 = hashlib.md5(parameters_file_path.read_bytes()).hexdigest()
    expected_md5 = registry[parameters_key]["md5"]
    if actual_md5 != expected_md5:
        message = (
            f"MD5 mismatch for parameters file '{parameters_file_path.name}': "
            f"expected {expected_md5!r}, got {actual_md5!r}. "
            "The file may have been modified. Update the `md5` in `registries/registered_params.json` "
            "to reflect the new file contents."
        )
        raise ValueError(message)

    parameters = json.loads(parameters_file_path.read_text())
    validated_parameters = validate_lfp_parameters(parameters)
    return validated_parameters
