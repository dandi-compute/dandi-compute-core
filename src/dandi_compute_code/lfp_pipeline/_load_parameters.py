import hashlib
import json

from ._globals import _PARAMS_DIR, _PARAMS_REGISTRY_FILE_PATH
from ..schemas import numeric_enum_slots, permissible_values, validate_against_schema, validate_registry

#: The schema describing these parameters, and the class in it they take.
_SCHEMA = "lfp_parameters"
_SCHEMA_CLASS = "LfpParameters"


def _format_number(value, /) -> str:
    """Write one number the way the schema's enumerations write it."""
    return format(float(value), "g")


def _format_numeric_value(value, /) -> str:
    """Write a number, or a sequence of them, the way the schema's enumerations write it."""
    if isinstance(value, (list, tuple)):
        return "-".join(_format_number(element) for element in value)
    return _format_number(value)


def validate_lfp_parameters(parameters, /) -> dict:
    """
    Validate a set of LFP parameters against the pipeline LinkML schema.

    :param parameters: The parameter mapping to validate.
    :type parameters: dict
    :return: The validated parameters, unchanged.
    :rtype: dict

    Raises
    ------
    ValueError
        If the parameters do not conform to ``schemas/lfp_parameters.linkml.yaml``, or if a
        numeric parameter is outside the fixed set that schema enumerates for it.
    """
    validate_against_schema(parameters, schema=_SCHEMA, description="LFP parameters")

    # A fixed set of numbers is stated in the schema as an enumeration, since an enumeration's
    # values are text, and the slot it governs points at it. Both are read back here rather
    # than repeated, so the allowed values are written in exactly one place.
    for field, enum_name in numeric_enum_slots(schema=_SCHEMA, class_name=_SCHEMA_CLASS):
        allowed = permissible_values(schema=_SCHEMA, enum_name=enum_name)
        value = parameters[field]
        try:
            formatted_value = _format_numeric_value(value)
        except (TypeError, ValueError):
            formatted_value = None
        if formatted_value not in allowed:
            message = (
                f"Invalid LFP parameters: {field} {value!r} is not one of the supported values. "
                f"Supported values, as the '{enum_name}' enumeration writes them, are: {list(allowed)}."
            )
            raise ValueError(message)

    return parameters


def load_lfp_parameters(parameters_key: str = "default", /) -> dict:
    """
    Resolve a registered parameters key to a validated set of LFP parameters.

    Mirrors the AIND ephys pipeline approach. The registry itself is validated against
    ``schemas/registry.linkml.yaml``, the key is looked up in
    ``registries/registered_params.json``, the referenced file under ``params/`` is checked
    against its recorded MD5, and the loaded parameters are validated against
    ``schemas/lfp_parameters.linkml.yaml``.

    :param parameters_key: The short name of the parameters to load.
        Must be a key registered in ``registries/registered_params.json``.
    :type parameters_key: str
    :return: The validated parameters loaded from the registered file.
    :rtype: dict

    Raises
    ------
    ValueError
        If the registry does not conform to its schema, if ``parameters_key`` is not
        registered, if the MD5 checksum of the resolved file does not match its registry
        entry, or if the loaded parameters do not conform to their schema.
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
