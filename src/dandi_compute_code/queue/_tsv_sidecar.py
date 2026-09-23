"""
BIDS-style JSON sidecars describing the columns of ``jobs.tsv`` and ``paths.tsv``.

Every column description is read from the packaged job capsule LinkML schema, so a sidecar
never drifts from the schema its table is validated against.
"""

from __future__ import annotations

import functools
import json
from collections.abc import Sequence

import beartype
import linkml_runtime.utils.schemaview

from ..schemas import resolve_schema_path


@functools.lru_cache(maxsize=None)
def _job_capsule_schema_view() -> linkml_runtime.utils.schemaview.SchemaView:
    """Load (once) the packaged job capsule schema."""
    schema_view = linkml_runtime.utils.schemaview.SchemaView(str(resolve_schema_path("job_capsule")))
    return schema_view


@beartype.beartype
def _collapse_whitespace(text: str, /) -> str:
    """Join a folded YAML description onto a single line."""
    return " ".join(text.split())


@beartype.beartype
def _tsv_sidecar_string(*, class_name: str, field_names: Sequence[str]) -> str:
    """
    Serialise the BIDS-style sidecar of a table whose rows are instances of *class_name*.

    Each column in *field_names* is described by its slot in the job capsule schema. An enum
    valued slot lists its permissible values as ``Levels``, and a slot declaring a unit
    records its symbol as ``Units``.

    Parameters
    ----------
    class_name : str
        The schema class one table row is an instance of.
    field_names : sequence of str
        The table's columns, in the order they are written.
    """
    schema_view = _job_capsule_schema_view()
    sidecar: dict[str, dict] = {}
    for field_name in field_names:
        slot = schema_view.induced_slot(field_name, class_name)
        column: dict = {"Description": _collapse_whitespace(slot.description)}
        enum = schema_view.get_enum(slot.range) if slot.range else None
        if enum is not None:
            column["Levels"] = {
                name: _collapse_whitespace(value.description) for name, value in enum.permissible_values.items()
            }
        if slot.unit is not None and slot.unit.symbol:
            column["Units"] = slot.unit.symbol
        sidecar[field_name] = column
    return json.dumps(sidecar, indent=2) + "\n"
