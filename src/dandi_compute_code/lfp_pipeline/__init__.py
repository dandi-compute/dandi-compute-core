from ._handle_template import generate_lfp_submission_script
from ._prepare_job import build_lfp_job_hash, build_lfp_pipeline_path, prepare_lfp_job
from ._resolve import resolve_filter_kwargs, resolve_reference_spec

# The runtime processing modules require the LFP container environment
# (SpikeInterface, neuroconv, pynwb, jsonschema). Keep them optional so the
# orchestration side (job capsule creation) can import this package in the base
# environment without that heavy stack installed.
try:
    from ._load_parameters import load_lfp_parameters, validate_lfp_parameters
    from ._extract_lfp import extract_lfp
    from ._write_nwb import add_lfp_to_nwbfile, run_lfp_pipeline
except ImportError:
    pass

__all__ = [
    "generate_lfp_submission_script",
    "prepare_lfp_job",
    "build_lfp_job_hash",
    "build_lfp_pipeline_path",
    "resolve_filter_kwargs",
    "resolve_reference_spec",
    "load_lfp_parameters",
    "validate_lfp_parameters",
    "extract_lfp",
    "add_lfp_to_nwbfile",
    "run_lfp_pipeline",
]
