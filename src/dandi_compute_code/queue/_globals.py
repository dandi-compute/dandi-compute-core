import json
import pathlib
import re

_AIND_EPHYS_PARAMS_REGISTRY_PATH = (
    pathlib.Path(__file__).parent.parent / "aind_ephys_pipeline" / "registries" / "registered_params.json"
)
_AIND_EPHYS_CONFIGS_REGISTRY_PATH = (
    pathlib.Path(__file__).parent.parent / "aind_ephys_pipeline" / "registries" / "registered_configs.json"
)
_LFP_PARAMS_REGISTRY_PATH = (
    pathlib.Path(__file__).parent.parent / "lfp_pipeline" / "registries" / "registered_params.json"
)
_QUEUE_CONFIG_SCHEMA_PATH = pathlib.Path(__file__).parent / "schemas" / "queue_config.linkml.yaml"
_RAW_ARRAY_DISPATCH_TEMPLATE_FILE_PATH = pathlib.Path(__file__).parent / "templates" / "array_dispatch_template.txt"
#: Prefix of the SLURM job name carried by every pipeline's array dispatcher.
_DISPATCH_JOB_NAME_PREFIX = "dandicompute-dispatch"
#: Characters outside this set are replaced in a pipeline name to keep SLURM job names simple.
_DISPATCH_JOB_NAME_SANITIZE_RE = re.compile(r"[^A-Za-z0-9._-]+")
#: SLURM states in which a dispatcher still owns its array and must not be resubmitted.
_ACTIVE_SLURM_JOB_STATES = "PENDING,RUNNING,SUSPENDED,COMPLETING,CONFIGURING,RESIZING,REQUEUED"
#: Job ID line written by ``sbatch`` on a successful submission.
_SBATCH_JOB_ID_RE = re.compile(r"Submitted batch job (?P<job_id>\d+)")
#: One ``#SBATCH`` directive in a submission template. The separator is ``=`` or whitespace,
#: since the packaged templates use both.
_SBATCH_DIRECTIVE_RE = re.compile(r"^#SBATCH\s+--(?P<name>[A-Za-z-]+)(?:=|\s+)(?P<value>\S+)\s*$", re.MULTILINE)
# Packaged pipeline configuration, committed directly to this repo. This is the canonical
# source of truth for the queue's pipeline definitions. There is no local override for this
# file; see ``_load_queue_config``.
_PACKAGED_PIPELINE_CONFIGS_PATH = pathlib.Path(__file__).parent / "pipeline_configs.json"
_DURATION_PART_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ms|s|m|h|d)\b")
#: Release tags of the form ``v1.2.3``, optionally with a pre-release or build suffix.
_VERSION_TAG_RE = re.compile(r"v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?")
#: Local checkout of the AIND ephys pipeline repository on MIT Engaging.
_DEFAULT_AIND_PIPELINE_DIRECTORY = pathlib.Path("/orcd/data/dandi/001/dandi-compute/aind-ephys-pipeline")
TEST_QUEUE_CONTENT_ID = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
_QUALIFYING_AIND_CONTENT_IDS_URL = (
    "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/dist/"
    "derivatives/qualifying_aind_content_ids.jsonl.gz"
)
_QUALIFYING_LFP_CONTENT_IDS_URL = (
    "https://raw.githubusercontent.com/dandi-cache/qualifying-lfp-content-ids/derivatives/"
    "derivatives/qualifying_lfp_content_ids.jsonl"
)


def _load_registry(registry_path: pathlib.Path, /) -> dict:
    """Read a packaged registry file, falling back to an empty registry when it is unreadable."""
    try:
        registry: dict = json.loads(registry_path.read_text())
    except (OSError, json.JSONDecodeError):
        registry = {}
    return registry


#: Registered parameters per pipeline, keyed by the pipeline name used in job provenance.
_PARAMS_REGISTRIES: dict[str, dict] = {
    "aind+ephys": _load_registry(_AIND_EPHYS_PARAMS_REGISTRY_PATH),
    "lfp": _load_registry(_LFP_PARAMS_REGISTRY_PATH),
}

#: Registered configs per pipeline. The LFP pipeline has no config of its own.
_CONFIGS_REGISTRIES: dict[str, dict] = {"aind+ephys": _load_registry(_AIND_EPHYS_CONFIGS_REGISTRY_PATH)}
