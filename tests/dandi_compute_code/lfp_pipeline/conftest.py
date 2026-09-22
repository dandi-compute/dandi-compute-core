import importlib.util

# Most of the LFP pipeline depends on the runtime environment provided by the pipeline
# container (see containers/) or `pip install .[lfp]`. When that environment is not present,
# skip collecting those tests instead of failing on import. Parameter loading is deliberately
# left out of this: it validates against a packaged LinkML schema using `linkml-runtime`,
# which the base install always carries, so it is collected everywhere.
_LFP_RUNTIME_MODULES = ("numpy", "spikeinterface", "neuroconv", "pynwb")
_TESTS_NEEDING_THE_RUNTIME_ENVIRONMENT = [
    "test_extract_lfp.py",
    "test_handle_template.py",
    "test_prepare_job.py",
    "test_resolve.py",
    "test_write_nwb.py",
]

if any(importlib.util.find_spec(module_name) is None for module_name in _LFP_RUNTIME_MODULES):
    collect_ignore = list(_TESTS_NEEDING_THE_RUNTIME_ENVIRONMENT)
