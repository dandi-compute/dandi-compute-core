"""
The packaged LinkML schemas load, describe the models they are meant to describe, and accept
every data file shipped with this repository.

These run against ``linkml-runtime``, which is a hard dependency of this package, so they run
everywhere the rest of the suite does. The stricter checks live in
``test_strict_linkml_validation.py`` and need the full ``linkml`` distribution.
"""

import dataclasses
import json
import pathlib

import linkml_runtime.utils.schemaview
import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.lfp_pipeline import validate_lfp_parameters
from dandi_compute_code.queue import (
    JOB_STATUSES,
    CapsuleResources,
    DispatchConfig,
    DispatchedArray,
    DispatchResult,
    JobCapsule,
    JobInfo,
)
from dandi_compute_code.schemas import (
    PARAMETER_JSON_SCHEMAS,
    SCHEMA_PATHS,
    SCHEMA_TREE_ROOTS,
    build_parameter_json_schema,
    validate_against_schema,
    validate_registry,
)

_REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[3]
_PACKAGE_ROOT = _REPOSITORY_ROOT / "src" / "dandi_compute_code"

#: Every registry file packaged with a pipeline. All of them share one schema.
_REGISTRY_FILE_PATHS = [
    _PACKAGE_ROOT / "aind_ephys_pipeline" / "registries" / "registered_configs.json",
    _PACKAGE_ROOT / "aind_ephys_pipeline" / "registries" / "registered_params.json",
    _PACKAGE_ROOT / "lfp_pipeline" / "registries" / "registered_params.json",
]

#: Every LFP parameters file packaged with the pipeline.
_LFP_PARAMS_FILE_PATHS = sorted((_PACKAGE_ROOT / "lfp_pipeline" / "params").glob("name-*.json"))

_EXAMPLE_JOB_INFO = JobInfo(
    job_id="job-250101abcdef",
    dandiset_id="000001",
    dandi_path="sub-A/sub-A_ses-1_ecephys.nwb",
    pipeline="lfp",
    version="v1.2.2",
    params="default",
    config="default",
    codebase="0.7.6",
)


def _schema_view(schema_name: str, /) -> linkml_runtime.utils.schemaview.SchemaView:
    return linkml_runtime.utils.schemaview.SchemaView(str(SCHEMA_PATHS[schema_name]))


def _slot_names(schema_name: str, class_name: str) -> set[str]:
    """Every slot a class carries, including the ones it inherits."""
    schema_view = _schema_view(schema_name)
    return {slot.name for slot in schema_view.class_induced_slots(class_name)}


def _dataclass_field_names(model, /) -> set[str]:
    return {field.name for field in dataclasses.fields(model)}


@pytest.mark.ai_generated
def test_every_packaged_schema_is_enumerated() -> None:
    """Every schema file in the schemas directory is registered in SCHEMA_PATHS."""
    schemas_dir = next(iter(SCHEMA_PATHS.values())).parent
    on_disk = {path.name for path in schemas_dir.glob("*.linkml.yaml")}
    enumerated = {path.name for path in SCHEMA_PATHS.values()}

    assert on_disk == enumerated


@pytest.mark.ai_generated
@pytest.mark.parametrize("schema_name", sorted(SCHEMA_PATHS))
def test_schema_loads_and_declares_its_tree_root(schema_name: str) -> None:
    """Each schema parses, and the class its instances are validated against exists in it."""
    schema_view = _schema_view(schema_name)

    assert schema_view.get_class(SCHEMA_TREE_ROOTS[schema_name]) is not None


@pytest.mark.ai_generated
def test_packaged_pipeline_config_validates() -> None:
    """The pipeline configuration shipped with this repository conforms to its schema."""
    pipeline_config = json.loads((_PACKAGE_ROOT / "queue" / "pipeline_configs.json").read_text())

    assert validate_against_schema(pipeline_config, schema="pipeline_config") == pipeline_config


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "registry_file_path", _REGISTRY_FILE_PATHS, ids=lambda path: f"{path.parent.parent.name}/{path.name}"
)
def test_packaged_registries_validate(registry_file_path: pathlib.Path) -> None:
    """Every packaged registry file conforms to the registry schema."""
    registry = json.loads(registry_file_path.read_text())

    assert validate_registry(registry) == registry


@pytest.mark.ai_generated
@pytest.mark.parametrize("params_file_path", _LFP_PARAMS_FILE_PATHS, ids=lambda path: path.name)
def test_packaged_lfp_parameters_validate(params_file_path: pathlib.Path) -> None:
    """Every packaged LFP parameters file conforms to its schema."""
    parameters = json.loads(params_file_path.read_text())

    assert validate_against_schema(parameters, schema="lfp_parameters") == parameters


@pytest.mark.ai_generated
def test_job_capsule_record_validates() -> None:
    """A serialised job capsule conforms to the job capsule schema."""
    capsule = JobCapsule(
        job=_EXAMPLE_JOB_INFO,
        content_id="048d1ee9-83b7-491f-8f02-1ca615b1d455",
        asset_size_bytes=1024,
        status="successful",
        created_at="2026-01-01T00:00:00+00:00",
        job_submission_time="2026-01-01T01:00:00+00:00",
        job_completion_time="2026-01-01T02:00:00+00:00",
        output_paths={"derivatives/out.nwb": "abc"},
    )

    record = capsule.to_dict()

    assert validate_against_schema(record, schema="job_capsule") == record


@pytest.mark.ai_generated
def test_assets_metadata_validates() -> None:
    """Indexed assets metadata conforms to the assets metadata schema."""
    asset = {
        "path": "sub-A/sub-A_ses-1_ecephys.nwb",
        "contentSize": 1024,
        "dateModified": "2026-01-01T00:00:00+00:00",
        "contentUrl": ["https://api.dandiarchive.org/api/assets/blobs/abc/"],
    }
    metadata = {
        "path_to_asset_metadata": {
            asset["path"]: {
                "path": asset["path"],
                "date_modified": asset["dateModified"],
                "content_size": asset["contentSize"],
                "content_id": "abc",
            }
        },
        "content_id_to_asset": {"abc": asset},
    }

    assert validate_against_schema(metadata, schema="assets_metadata") == metadata


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("schema_name", "class_name", "model"),
    [
        ("assets_metadata", "AssetMetadata", AssetMetadata),
        ("assets_metadata", "AssetsJsonldMetadata", AssetsJsonldMetadata),
        ("dispatch", "CapsuleResources", CapsuleResources),
        ("dispatch", "DispatchConfig", DispatchConfig),
        ("dispatch", "DispatchedArray", DispatchedArray),
        ("dispatch", "DispatchResult", DispatchResult),
        ("job_capsule", "JobInfo", JobInfo),
    ],
)
def test_schema_class_matches_its_dataclass(schema_name: str, class_name: str, model: type) -> None:
    """
    Each schema class carries exactly the fields of the dataclass it describes.

    This is what keeps a schema from drifting once the model it was written for changes.
    """
    assert _slot_names(schema_name, class_name) == _dataclass_field_names(model)


@pytest.mark.ai_generated
def test_job_capsule_record_matches_the_serialised_capsule() -> None:
    """The job capsule schema carries exactly the fields a serialised capsule holds."""
    record = JobCapsule(job=_EXAMPLE_JOB_INFO, content_id=None, asset_size_bytes=None).to_dict()

    assert _slot_names("job_capsule", "JobCapsuleRecord") == set(record)


@pytest.mark.ai_generated
def test_job_status_enum_matches_the_statuses_the_queue_uses() -> None:
    """The schema's JobStatus enumeration lists exactly the statuses the queue selects on."""
    schema_view = _schema_view("job_capsule")
    permissible = set(schema_view.get_enum("JobStatus").permissible_values)

    assert permissible == set(JOB_STATUSES)


@pytest.mark.ai_generated
def test_pipeline_config_rejects_an_undeclared_attribute() -> None:
    """Validation is actually applied, rather than accepting anything handed to it."""
    invalid = {"pipelines": {"test": {"params": ["default"], "retries": 3}}}

    with pytest.raises(ValueError, match="LinkML validation"):
        validate_against_schema(invalid, schema="pipeline_config")


@pytest.mark.ai_generated
def test_registry_rejects_an_entry_missing_its_checksum() -> None:
    """A registry entry without an md5 is rejected."""
    invalid = {"default": {"path": "name-default.json"}}

    with pytest.raises(ValueError, match="LinkML validation"):
        validate_registry(invalid)


@pytest.mark.ai_generated
@pytest.mark.parametrize("schema_name", sorted(PARAMETER_JSON_SCHEMAS))
def test_published_json_schema_is_up_to_date(schema_name: str) -> None:
    """
    The committed JSON Schema is exactly what its LinkML source generates.

    The documentation website renders the JSON Schema, so it is committed rather than built
    on demand. This is what stops it drifting from the LinkML schema it comes from. Run
    ``python -m dandi_compute_code.schemas`` to regenerate it.
    """
    committed = json.loads(PARAMETER_JSON_SCHEMAS[schema_name].output_path.read_text())

    assert committed == build_parameter_json_schema(schema_name)


@pytest.mark.ai_generated
@pytest.mark.parametrize("schema_name", sorted(PARAMETER_JSON_SCHEMAS))
def test_published_json_schema_constrains_every_field(schema_name: str) -> None:
    """
    Every field the generated JSON Schema publishes carries its allowed values or a type.

    A field that generated as neither would render on the website as an unconstrained value,
    which is the failure mode of describing a numeric set in a schema whose enumerations are
    text.
    """
    json_schema = build_parameter_json_schema(schema_name)

    unconstrained = [
        name
        for name, property_schema in json_schema["properties"].items()
        if "enum" not in property_schema and "type" not in property_schema
    ]

    assert unconstrained == []


@pytest.mark.ai_generated
def test_published_json_schema_accepts_the_packaged_parameters() -> None:
    """The generated JSON Schema accepts the parameters files shipped alongside it."""
    jsonschema = pytest.importorskip("jsonschema")
    json_schema = build_parameter_json_schema("lfp_parameters")

    for params_file_path in _LFP_PARAMS_FILE_PATHS:
        jsonschema.validate(instance=json.loads(params_file_path.read_text()), schema=json_schema)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("filter_family", "chebyshev"),
        ("filter_band", [2, 400]),
        ("filter_order", 3),
        ("filter_direction", "backward"),
        ("reference_scheme", "average"),
        ("resample_target_fs", 30000),
        ("spatial_factor", 2),
        ("bad_channel_method", "std"),
    ],
)
def test_published_json_schema_rejects_what_the_loader_rejects(field: str, value: object) -> None:
    """
    The published JSON Schema and the runtime LinkML path agree on what is invalid.

    The website renders one and the pipeline enforces the other, so a value the two disagreed
    about would be documented as allowed while being refused, or the reverse.
    """
    jsonschema = pytest.importorskip("jsonschema")
    json_schema = build_parameter_json_schema("lfp_parameters")
    parameters = json.loads((_PACKAGE_ROOT / "lfp_pipeline" / "params" / "name-default.json").read_text())
    parameters[field] = value

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=parameters, schema=json_schema)

    with pytest.raises(ValueError):
        validate_lfp_parameters(parameters)


@pytest.mark.ai_generated
def test_unknown_schema_name_is_rejected() -> None:
    """Asking for a schema that is not packaged names the ones that are."""
    with pytest.raises(ValueError, match="is not a packaged LinkML schema"):
        validate_against_schema({}, schema="does-not-exist")
