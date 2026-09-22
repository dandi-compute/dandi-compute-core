import pathlib

_RAW_TEMPLATE_FILE_PATH = pathlib.Path(__file__).parent / "templates" / "submission_template.txt"
_PARAMETER_SCHEMA_FILE_PATH = pathlib.Path(__file__).parent / "params" / "parameter_schema.json"

_JOB_CAPSULES_DANDISET_ID = "001697"
_LFP_CONTAINER_NAME = "dandi-compute-lfp"
_LFP_CONTAINER_IMAGE_TEMPLATE = "ghcr.io/dandi-compute/dandi-compute-lfp:{version}"
_PARAMS_DIR = pathlib.Path(__file__).parent / "params"
_PARAMS_REGISTRY_FILE_PATH = pathlib.Path(__file__).parent / "registries" / "registered_params.json"
