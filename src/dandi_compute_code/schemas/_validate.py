"""Validation of in-memory structures against the packaged LinkML schemas."""

from __future__ import annotations

import functools
import pathlib

import linkml_runtime.processing.referencevalidator
import linkml_runtime.utils.schemaview

from ._globals import SCHEMA_PATHS, SCHEMA_TREE_ROOTS


@functools.lru_cache(maxsize=None)
def _validator_for(schema_path: pathlib.Path, /) -> linkml_runtime.processing.referencevalidator.ReferenceValidator:
    """Build (once per schema) the validator for a packaged schema file."""
    schema_view = linkml_runtime.utils.schemaview.SchemaView(str(schema_path))
    validator = linkml_runtime.processing.referencevalidator.ReferenceValidator(schema_view)
    return validator


def resolve_schema_path(schema: str, /) -> pathlib.Path:
    """
    Resolve a schema's short name to its packaged file path.

    Parameters
    ----------
    schema : str
        A key of :data:`SCHEMA_PATHS`.

    Returns
    -------
    pathlib.Path
        The path of the schema file.

    Raises
    ------
    ValueError
        If *schema* is not packaged.
    """
    if schema not in SCHEMA_PATHS:
        packaged = sorted(SCHEMA_PATHS)
        message = f"'{schema}' is not a packaged LinkML schema. Packaged schemas are: {packaged}."
        raise ValueError(message)
    return SCHEMA_PATHS[schema]


def validate_against_schema(instance: dict, /, *, schema: str, description: str = "instance") -> dict:
    """
    Validate a structure against one of the packaged LinkML schemas.

    Parameters
    ----------
    instance : dict
        The structure to validate.
    schema : str
        A key of :data:`SCHEMA_PATHS`.
    description : str, optional
        What is being validated, used to open the error message.

    Returns
    -------
    dict
        The instance, unchanged.

    Raises
    ------
    ValueError
        If *instance* does not conform to the schema.
    """
    schema_path = resolve_schema_path(schema)
    target_class = SCHEMA_TREE_ROOTS[schema]

    report = _validator_for(schema_path).validate(instance, target=target_class)
    errors = [result for result in report.results if not (result.normalized or result.repaired)]
    if errors:
        message = (
            f"Invalid {description}: LinkML validation against '{schema_path.name}' failed with "
            f"{len(errors)} error(s). First error: {errors[0]!r}"
        )
        raise ValueError(message)
    return instance


def validate_registry(registry: dict, /, *, description: str = "registry") -> dict:
    """
    Validate a loaded registry file against the packaged registry schema.

    A registry file's top level is the key-to-entry mapping itself, so it is wrapped in the
    ``entries`` slot the ``Registry`` class declares before being validated.

    Parameters
    ----------
    registry : dict
        The loaded registry, keyed by registry key.
    description : str, optional
        What is being validated, used to open the error message.

    Returns
    -------
    dict
        The registry, unchanged.

    Raises
    ------
    ValueError
        If *registry* does not conform to the registry schema.
    """
    validate_against_schema({"entries": registry}, schema="registry", description=description)
    return registry
