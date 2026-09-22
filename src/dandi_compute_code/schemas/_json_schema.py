"""
JSON Schema generated from a packaged LinkML parameter schema.

A pipeline's parameter schema is published as JSON Schema as well, because that is what the
documentation website renders. The LinkML schema stays the source of truth and the JSON
Schema is generated from it, so the two cannot drift. A test asserts that the committed file
is exactly what this module produces.

The output is deliberately flat and self-contained, with every enumeration inlined rather
than referenced through ``$defs``. A renderer can then show one field's allowed values
without resolving anything. This covers the shape a parameter schema takes, which is one
class of scalar and small list fields, rather than LinkML in general.

Some allowed value sets are numeric, and a LinkML enumeration's values are text. Those sets
are declared as enumerations and pointed at from the slot they govern by a ``numeric_enum``
annotation, which is what lets both this generator and the loader read one declaration.
"""

from __future__ import annotations

import json
import pathlib

import linkml_runtime.utils.schemaview

from ._globals import PARAMETER_JSON_SCHEMAS
from ._validate import resolve_schema_path

#: The JSON Schema dialect the generated files declare.
_JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"

#: How a LinkML type maps onto a JSON Schema type.
_RANGE_TO_JSON_TYPE = {
    "string": "string",
    "integer": "integer",
    "float": "number",
    "double": "number",
    "decimal": "number",
    "boolean": "boolean",
}


def _parse_number(token: str, /) -> int | float:
    """Read one number out of an enumeration value, keeping a whole number whole."""
    return float(token) if "." in token else int(token)


def _numeric_enum_values(
    *,
    schema_view: linkml_runtime.utils.schemaview.SchemaView,
    enum_name: str,
    multivalued: bool,
) -> list:
    """
    The allowed values of a numeric enumeration, as JSON Schema should carry them.

    A multivalued slot's enumeration states each allowed combination as its parts joined by
    a hyphen, so those come back as lists.
    """
    enum_definition = schema_view.get_enum(enum_name)
    if enum_definition is None:
        message = f"'{enum_name}' is not an enumeration in the schema."
        raise ValueError(message)

    values = []
    for permissible_value in enum_definition.permissible_values:
        tokens = str(permissible_value).split("-")
        parsed = [_parse_number(token) for token in tokens]
        values.append(parsed if multivalued else parsed[0])
    return values


def _slot_schema(*, schema_view: linkml_runtime.utils.schemaview.SchemaView, slot) -> dict:
    """The JSON Schema for one slot, with any enumeration inlined."""
    property_schema: dict = {}
    if slot.description:
        property_schema["description"] = " ".join(slot.description.split())

    annotation = slot.annotations["numeric_enum"] if "numeric_enum" in slot.annotations else None
    if annotation is not None:
        property_schema["enum"] = _numeric_enum_values(
            schema_view=schema_view,
            enum_name=str(annotation.value),
            multivalued=bool(slot.multivalued),
        )
        return property_schema

    if schema_view.get_enum(slot.range) is not None:
        property_schema["enum"] = [str(value) for value in schema_view.get_enum(slot.range).permissible_values]
        return property_schema

    json_type = _RANGE_TO_JSON_TYPE.get(str(slot.range), "string")
    bounds = {}
    if slot.minimum_value is not None:
        bounds["minimum"] = slot.minimum_value
    if slot.maximum_value is not None:
        bounds["maximum"] = slot.maximum_value

    if slot.multivalued:
        property_schema["type"] = "array"
        property_schema["items"] = {"type": json_type, **bounds}
    else:
        property_schema["type"] = json_type
        property_schema.update(bounds)
    return property_schema


def build_parameter_json_schema(schema: str, /) -> dict:
    """
    Generate the published JSON Schema for a packaged LinkML parameter schema.

    :param schema: A key of :data:`PARAMETER_JSON_SCHEMAS`.
    :type schema: str
    :return: The JSON Schema, ready to be written out.
    :rtype: dict
    :raises ValueError: If *schema* is not published as JSON Schema.
    """
    if schema not in PARAMETER_JSON_SCHEMAS:
        published = sorted(PARAMETER_JSON_SCHEMAS)
        message = f"'{schema}' is not published as JSON Schema. Published parameter schemas are: {published}."
        raise ValueError(message)

    schema_view = linkml_runtime.utils.schemaview.SchemaView(str(resolve_schema_path(schema)))
    root_class = next(
        class_definition for class_definition in schema_view.all_classes().values() if class_definition.tree_root
    )

    slots = schema_view.class_induced_slots(root_class.name)
    json_schema = {
        "$schema": _JSON_SCHEMA_DIALECT,
        "$id": PARAMETER_JSON_SCHEMAS[schema].schema_id,
        "title": schema_view.schema.title or root_class.name,
        "description": " ".join((root_class.description or "").split()),
        "type": "object",
        "additionalProperties": False,
        "required": [slot.name for slot in slots if slot.required],
        "properties": {slot.name: _slot_schema(schema_view=schema_view, slot=slot) for slot in slots},
    }
    return json_schema


def write_parameter_json_schema(schema: str, /) -> pathlib.Path:
    """
    Regenerate one published parameter JSON Schema in place.

    :param schema: A key of :data:`PARAMETER_JSON_SCHEMAS`.
    :type schema: str
    :return: The path that was written.
    :rtype: pathlib.Path
    """
    json_schema = build_parameter_json_schema(schema)
    output_path = PARAMETER_JSON_SCHEMAS[schema].output_path
    output_path.write_text(json.dumps(json_schema, indent=4) + "\n")
    return output_path
