import pathlib

_RAW_TEMPLATE_FILE_PATH = pathlib.Path(__file__).parent / "templates" / "submission_template.txt"

# Provisioned on the cluster by the Update codebase workflow of dandi-compute-runner. The pipeline
# needs Nextflow 25.10 or later from release 1.5.0. Capsules formed earlier keep the environment
# their own `submit.sh` names.
_NEXTFLOW_ENVIRONMENT_DIRECTORY = "/orcd/data/dandi/001/environments/name-nextflow_version-25+10+4_environment"

# Release 1.5.0 merged `main_multi_backend.nf`, the entry point for SLURM, into `main.nf`. Every
# earlier release ships both, and there `main.nf` is the one for Code Ocean.
_PIPELINE_FILE_NAMES = ("main_multi_backend.nf", "main.nf")

# Release 1.4.1 renamed the pinned step versions file.
_PINNED_STEP_VERSIONS_FILE_NAMES = ("process_versions.env", "capsule_versions.env")

# Release 1.5.0 validates the job dispatch arguments against a schema that has no `nwb_files`, so
# `--job_dispatch_args "--nwb-files ..."` fails the run before any step starts. From this release a
# capsule gives the pipeline an input folder holding the blob as its one `.nwb` file instead.
_FIRST_PIPELINE_VERSION_WITH_NWB_INPUT_FOLDER = (1, 5, 0)
