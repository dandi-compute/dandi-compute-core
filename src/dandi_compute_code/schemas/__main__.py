"""
Regenerate the published parameter JSON Schemas from their LinkML sources.

Run after changing a parameter schema::

    python -m dandi_compute_code.schemas

A test asserts that the committed files match what this writes, so CI fails if a LinkML
parameter schema changes without its JSON Schema being regenerated.
"""

from ._globals import PARAMETER_JSON_SCHEMAS
from ._json_schema import write_parameter_json_schema


def main() -> None:
    """Write every published parameter JSON Schema in place."""
    for schema in sorted(PARAMETER_JSON_SCHEMAS):
        output_path = write_parameter_json_schema(schema)
        print(f"Wrote {output_path} from '{schema}'.")


if __name__ == "__main__":
    main()
