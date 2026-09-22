"""
Validation of in-memory structures against the packaged LinkML schemas.

Validation at runtime goes through ``linkml-runtime``, which is a light dependency this
package already carries. That validator is a normalizer first and a validator second, so it
reports structural problems (an attribute a class does not declare, a value outside a
declared bound) but quietly coerces some mismatched scalar types rather than rejecting them.
The stricter, JSON-Schema-backed validator in the full ``linkml`` distribution is what CI
runs over the same schemas and the same packaged files, so the looser checks here are a fast
guard rather than the only one.
"""

from __future__ import annotations

import functools
import pathlib

import linkml_runtime.processing.referencevalidator
import linkml_runtime.utils.schemaview

from ._globals import _REGISTRY_SCHEMA_PATH, SCHEMA_PATHS, SCHEMA_TREE_ROOTS


@functools.lru_cache(maxsize=None)
def _validator_for(schema_path: pathlib.Path, /) -> linkml_runtime.processing.referencevalidator.ReferenceValidator:
    """Build (once per schema) the validator for a packaged schema file."""
    schema_view = linkml_runtime.utils.schemaview.SchemaView(str(schema_path))
    validator = linkml_runtime.processing.referencevalidator.ReferenceValidator(schema_view)
    return validator


def resolve_schema_path(schema: str | pathlib.Path, /) -> pathlib.Path:
    """
    Resolve a schema's short name to its packaged file path.

    :param schema: A key of :data:`SCHEMA_PATHS`, or a path to a schema file.
    :type schema: str | pathlib.Path
    :return: The path of the schema file.
    :rtype: pathlib.Path
    :raises ValueError: If *schema* is a name that is not packaged.
    """
    if isinstance(schema, pathlib.Path):
        return schema
    if schema not in SCHEMA_PATHS:
        packaged = sorted(SCHEMA_PATHS)
        message = f"'{schema}' is not a packaged LinkML schema. Packaged schemas are: {packaged}."
        raise ValueError(message)
    return SCHEMA_PATHS[schema]


def validate_against_schema(
    instance: dict,
    /,
    *,
    schema: str | pathlib.Path,
    target_class: str | None = None,
    description: str = "instance",
) -> dict:
    """
    Validate a structure against one of the packaged LinkML schemas.

    :param instance: The structure to validate.
    :type instance: dict
    :param schema: A key of :data:`SCHEMA_PATHS`, or a path to a schema file.
    :type schema: str | pathlib.Path
    :param target_class: The class in the schema to validate against. Defaults to the
        schema's tree root, which is the class its instances normally take.
    :type target_class: str | None
    :param description: What is being validated, used to open the error message.
    :type description: str
    :return: The instance, unchanged.
    :rtype: dict
    :raises ValueError: If *instance* does not conform to the schema.
    """
    schema_path = resolve_schema_path(schema)
    resolved_target = target_class
    if resolved_target is None:
        if isinstance(schema, pathlib.Path):
            message = "target_class is required when validating against a schema given by path."
            raise ValueError(message)
        resolved_target = SCHEMA_TREE_ROOTS[schema]

    validator = _validator_for(schema_path)
    try:
        report = validator.validate(instance, target=resolved_target)
    except (TypeError, ValueError) as exception:
        # A scalar of the wrong type can reach a comparison against a declared bound, where
        # the validator raises rather than reporting. Treat that as the failure it describes.
        message = (
            f"Invalid {description}: LinkML validation of '{resolved_target}' against "
            f"'{schema_path.name}' could not be completed ({exception})."
        )
        raise ValueError(message) from exception

    errors = [result for result in report.results if not (result.normalized or result.repaired)]
    if errors:
        message = (
            f"Invalid {description}: LinkML validation against '{schema_path.name}' failed with "
            f"{len(errors)} error(s). First error: {errors[0]!r}"
        )
        raise ValueError(message)

    if isinstance(schema, str):
        _validate_numeric_enums(
            instance,
            schema=schema,
            class_name=resolved_target,
            description=description,
        )
    return instance


def _format_number(value, /) -> str:
    """Write one number the way a schema's enumerations write it."""
    return format(float(value), "g")


def _format_numeric_value(value, /) -> str:
    """Write a number, or a sequence of them, the way a schema's enumerations write it."""
    if isinstance(value, (list, tuple)):
        return "-".join(_format_number(element) for element in value)
    return _format_number(value)


def _validate_numeric_enums(instance: dict, /, *, schema: str, class_name: str, description: str) -> None:
    """
    Check every slot whose allowed values are a fixed numeric set.

    LinkML cannot attach such a set to the slot it governs, because an enumeration's values
    are text. The schema therefore declares the set as an enumeration and points at it from
    the slot with a ``numeric_enum`` annotation. This resolves that annotation, so a schema
    carrying one is enforced wherever it is validated rather than only where a caller
    remembers to check.

    :raises ValueError: If a value is outside the set its slot's enumeration names.
    """
    for field, enum_name in _numeric_enum_slots(schema=schema, class_name=class_name):
        if field not in instance:
            continue
        allowed = permissible_values(schema=schema, enum_name=enum_name)
        value = instance[field]
        try:
            formatted_value = _format_numeric_value(value)
        except (TypeError, ValueError):
            formatted_value = None
        if formatted_value not in allowed:
            message = (
                f"Invalid {description}: {field} {value!r} is not one of the supported values. "
                f"Supported values, as the '{enum_name}' enumeration writes them, are: {list(allowed)}."
            )
            raise ValueError(message)


@functools.lru_cache(maxsize=None)
def permissible_values(*, schema: str, enum_name: str) -> tuple[str, ...]:
    """
    The values one of a packaged schema's enumerations allows.

    Reading these back out of the schema keeps it the only place such a set is written, for
    the cases a constraint cannot be attached to the slot it governs.

    :param schema: A key of :data:`SCHEMA_PATHS`.
    :type schema: str
    :param enum_name: The name of an enumeration in that schema.
    :type enum_name: str
    :return: Every value the enumeration permits.
    :rtype: tuple[str, ...]
    :raises ValueError: If the schema declares no such enumeration.
    """
    schema_path = resolve_schema_path(schema)
    schema_view = linkml_runtime.utils.schemaview.SchemaView(str(schema_path))
    enum_definition = schema_view.get_enum(enum_name)
    if enum_definition is None:
        message = f"'{enum_name}' is not an enumeration in '{schema_path.name}'."
        raise ValueError(message)
    values = tuple(str(value) for value in enum_definition.permissible_values)
    return values


@functools.lru_cache(maxsize=None)
def _numeric_enum_slots(*, schema: str, class_name: str) -> tuple[tuple[str, str], ...]:
    """
    The slots of a class whose allowed values are a fixed numeric set.

    A LinkML enumeration's values are text, so a numeric set is declared as an enumeration
    and pointed at from the slot it governs by a ``numeric_enum`` annotation. Reading that
    annotation back is what lets a loader and the JSON Schema generator work from one
    declaration rather than each repeating the set.

    :param schema: A key of :data:`SCHEMA_PATHS`.
    :type schema: str
    :param class_name: The class whose slots to inspect.
    :type class_name: str
    :return: Pairs of slot name and the enumeration naming its allowed values.
    :rtype: tuple[tuple[str, str], ...]
    """
    schema_view = linkml_runtime.utils.schemaview.SchemaView(str(resolve_schema_path(schema)))
    annotated = tuple(
        (slot.name, str(slot.annotations["numeric_enum"].value))
        for slot in schema_view.class_induced_slots(class_name)
        if "numeric_enum" in slot.annotations
    )
    return annotated


def validate_registry(registry: dict, /, *, description: str = "registry") -> dict:
    """
    Validate a loaded registry file against the packaged registry schema.

    A registry file's top level is the key-to-entry mapping itself, so it is wrapped in the
    ``entries`` slot the ``Registry`` class declares before being validated.

    :param registry: The loaded registry, keyed by registry key.
    :type registry: dict
    :param description: What is being validated, used to open the error message.
    :type description: str
    :return: The registry, unchanged.
    :rtype: dict
    :raises ValueError: If *registry* does not conform to the registry schema.
    """
    validate_against_schema(
        {"entries": registry},
        schema=_REGISTRY_SCHEMA_PATH,
        target_class="Registry",
        description=description,
    )
    return registry
