"""
Strict validation of the packaged LinkML schemas and of every data file shipped against them.

The runtime validator in ``linkml-runtime`` normalizes as much as it validates, so it lets a
scalar of the wrong type through where a schema declares another. The full ``linkml``
distribution compiles a schema to JSON Schema and validates against that instead, which
rejects those. That distribution is heavy, so it is not a runtime dependency. It is installed
by the schema validation CI job, which sets
``DANDI_COMPUTE_REQUIRE_STRICT_SCHEMA_VALIDATION`` so that these fail rather than skip if it
ever goes missing there.
"""

import json
import os
import pathlib

import pytest

from dandi_compute_code.schemas import SCHEMA_PATHS, SCHEMA_TREE_ROOTS

_REQUIRE_STRICT = os.environ.get("DANDI_COMPUTE_REQUIRE_STRICT_SCHEMA_VALIDATION") == "1"
_REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[3]
_PACKAGE_ROOT = _REPOSITORY_ROOT / "src" / "dandi_compute_code"

_REGISTRY_FILE_PATHS = [
    _PACKAGE_ROOT / "aind_ephys_pipeline" / "registries" / "registered_configs.json",
    _PACKAGE_ROOT / "aind_ephys_pipeline" / "registries" / "registered_params.json",
    _PACKAGE_ROOT / "lfp_pipeline" / "registries" / "registered_params.json",
]


def _import_linkml(module_name: str, /):
    """
    Import part of the full ``linkml`` distribution.

    Skips the test when it is absent, unless CI has declared that it must be present, in
    which case a missing install is the failure it would otherwise hide.
    """
    import importlib

    try:
        return importlib.import_module(module_name)
    except ImportError as exception:
        message = (
            f"The full 'linkml' distribution is required for strict schema validation but "
            f"'{module_name}' could not be imported ({exception}). Install it with "
            "`pip install --group schemas`."
        )
        if _REQUIRE_STRICT:
            pytest.fail(message)
        pytest.skip(message)


def _validate_strictly(instance: dict, *, schema_name: str, target_class: str | None = None) -> None:
    """Validate an instance against a packaged schema, failing with every message reported."""
    validator_module = _import_linkml("linkml.validator")
    resolved_target = target_class if target_class is not None else SCHEMA_TREE_ROOTS[schema_name]

    report = validator_module.validate(
        instance,
        schema=str(SCHEMA_PATHS[schema_name]),
        target_class=resolved_target,
    )

    messages = [result.message for result in report.results]
    assert messages == []


@pytest.mark.ai_generated
@pytest.mark.parametrize("schema_name", sorted(SCHEMA_PATHS))
def test_schema_compiles_to_json_schema(schema_name: str) -> None:
    """
    Each schema compiles to JSON Schema.

    This is what catches a schema that ``linkml-runtime`` loads but that does not express what
    it appears to, such as an inlined mapping whose range cannot actually be inlined.
    """
    generator_module = _import_linkml("linkml.generators.jsonschemagen")

    generated = json.loads(generator_module.JsonSchemaGenerator(str(SCHEMA_PATHS[schema_name])).serialize())

    assert SCHEMA_TREE_ROOTS[schema_name] in generated["$defs"]


@pytest.mark.ai_generated
def test_packaged_pipeline_config_validates_strictly() -> None:
    pipeline_config = json.loads((_PACKAGE_ROOT / "queue" / "pipeline_configs.json").read_text())

    _validate_strictly(pipeline_config, schema_name="pipeline_config")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "registry_file_path", _REGISTRY_FILE_PATHS, ids=lambda path: f"{path.parent.parent.name}/{path.name}"
)
def test_packaged_registries_validate_strictly(registry_file_path: pathlib.Path) -> None:
    registry = json.loads(registry_file_path.read_text())

    _validate_strictly({"entries": registry}, schema_name="registry")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_concurrent", "two"),
        ("max_concurrent", 0),
        ("max_array_tasks", 0),
    ],
)
def test_strict_validation_rejects_bad_dispatch_limits(field: str, value: object) -> None:
    """
    The strict validator rejects the scalar mismatches the runtime one lets through.

    This is the reason the schemas are validated twice rather than only at runtime.
    """
    validator_module = _import_linkml("linkml.validator")
    invalid = {"pipelines": {"test": {"dispatch": {field: value}}}}

    report = validator_module.validate(
        invalid,
        schema=str(SCHEMA_PATHS["pipeline_config"]),
        target_class="PipelinesConfig",
    )

    assert len(report.results) > 0
