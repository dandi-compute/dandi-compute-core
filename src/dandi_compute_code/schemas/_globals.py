import pathlib

_SCHEMAS_DIR = pathlib.Path(__file__).parent

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

#: The class each schema's instances are validated against by default.
SCHEMA_TREE_ROOTS: dict[str, str] = {
    "assets_metadata": "AssetsJsonldMetadata",
    "dispatch": "DispatchConfig",
    "job_capsule": "JobCapsuleRecord",
    "lfp_parameters": "LfpParameters",
    "pipeline_config": "PipelinesConfig",
    "registry": "Registry",
}
