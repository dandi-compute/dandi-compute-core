import dataclasses
import pathlib

_SCHEMAS_DIR = pathlib.Path(__file__).parent
_PACKAGE_DIR = _SCHEMAS_DIR.parent

_ASSETS_METADATA_SCHEMA_PATH = _SCHEMAS_DIR / "assets_metadata.linkml.yaml"
_DISPATCH_SCHEMA_PATH = _SCHEMAS_DIR / "dispatch.linkml.yaml"
_JOB_CAPSULE_SCHEMA_PATH = _SCHEMAS_DIR / "job_capsule.linkml.yaml"
_LFP_PARAMETERS_SCHEMA_PATH = _SCHEMAS_DIR / "lfp_parameters.linkml.yaml"
_PIPELINE_CONFIG_SCHEMA_PATH = _SCHEMAS_DIR / "pipeline_config.linkml.yaml"
_REGISTRY_SCHEMA_PATH = _SCHEMAS_DIR / "registry.linkml.yaml"

#: Every packaged LinkML schema, keyed by its short name. This is what the schema validation
#: tests and the CI validation job enumerate, so a schema added here is covered by both.
SCHEMA_PATHS: dict[str, pathlib.Path] = {
    "assets_metadata": _ASSETS_METADATA_SCHEMA_PATH,
    "dispatch": _DISPATCH_SCHEMA_PATH,
    "job_capsule": _JOB_CAPSULE_SCHEMA_PATH,
    "lfp_parameters": _LFP_PARAMETERS_SCHEMA_PATH,
    "pipeline_config": _PIPELINE_CONFIG_SCHEMA_PATH,
    "registry": _REGISTRY_SCHEMA_PATH,
}


@dataclasses.dataclass(frozen=True)
class PublishedParameterSchema:
    """Where one generated parameter JSON Schema is written, and what identifies it."""

    #: Where the generated file lives. It sits beside the parameters files it describes.
    output_path: pathlib.Path
    #: The ``$id`` the generated file declares. Kept stable, since it is a published URL
    #: that the documentation website and any external consumer resolve.
    schema_id: str


#: Parameter schemas that are also published as JSON Schema, because that is the form the
#: documentation website renders. The LinkML schema remains the source of truth and these
#: files are generated from it by ``python -m dandi_compute_code.schemas``.
PARAMETER_JSON_SCHEMAS: dict[str, PublishedParameterSchema] = {
    "lfp_parameters": PublishedParameterSchema(
        output_path=_PACKAGE_DIR / "lfp_pipeline" / "params" / "parameter_schema.json",
        schema_id=(
            "https://raw.githubusercontent.com/dandi-compute/code/main/"
            "src/dandi_compute_code/lfp_pipeline/params/parameter_schema.json"
        ),
    ),
}

#: The class each schema's instances are validated against by default.
SCHEMA_TREE_ROOTS: dict[str, str] = {
    "assets_metadata": "AssetsJsonldMetadata",
    "dispatch": "DispatchConfig",
    "job_capsule": "JobCapsuleRecord",
    "lfp_parameters": "LfpParameters",
    "pipeline_config": "PipelinesConfig",
    "registry": "Registry",
}
