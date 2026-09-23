"""Tests verifying that the public API enforces input types via beartype."""

import pathlib
from collections.abc import Callable

import beartype.roar
import pytest

from dandi_compute_code.aind_ephys_pipeline import (
    generate_aind_ephys_submission_script,
    prepare_aind_ephys_job,
    submit_job,
)
from dandi_compute_code.dandiset import (
    AssetMetadata,
    AssetsJsonldMetadata,
    load_assets_jsonld_metadata,
    move_job_capsule,
    write_dandiset_file,
)
from dandi_compute_code.lfp_pipeline import (
    build_lfp_job_hash,
    build_lfp_pipeline_path,
    generate_lfp_submission_script,
    load_lfp_parameters,
    prepare_lfp_job,
    resolve_filter_kwargs,
    resolve_reference_spec,
    validate_lfp_parameters,
)
from dandi_compute_code.queue import (
    CapsuleResources,
    DispatchConfig,
    DispatchedArray,
    DispatchResult,
    JobCapsule,
    JobInfo,
    PipelineQueue,
    clean_dispatch_directories,
    dispatch_pipeline_jobs,
    read_capsule_resources,
)
from dandi_compute_code.schemas import resolve_schema_path, validate_against_schema, validate_registry

_PATH = pathlib.Path("/some/path")

# Every argument a valid call would need, with the one under test given the wrong type. Type
# checking happens before the function body runs, so none of these touch the network or disk.
_AIND_SCRIPT_KWARGS = {
    "script_file_path": "/some/submit.sh",
    "log_directory": "logs",
    "nwb_file_path": "nwb",
    "results_directory": "results",
    "work_directory": "work",
    "apptainer_cache_directory": "cache",
    "environment_directory": "env",
    "config_file_path": "config",
    "pipeline_file_path": "pipeline",
    "pipeline_repo_directory": "repo",
    "pipeline_version": "v1.0.1",
    "temp_name": "temp",
    "done_tracker_file_path": "done",
    "params_file_path": "params",
}
_LFP_SCRIPT_KWARGS = {
    "script_file_path": "/some/submit.sh",
    "log_directory": "logs",
    "dataset_directory": "dataset",
    "environment_directory": "env",
    "container_name": "container",
    "container_image": "image",
    "nwb_file_path": "nwb",
    "output_nwb_file_path": "output",
    "parameters_key": "default",
    "temp_name": "temp",
    "done_tracker_file_path": "done",
}
_JOB_INFO_KWARGS = {
    "job_id": 1,
    "dandiset_id": "000409",
    "within_dandiset_path": "sub-1/sub-1_ecephys.nwb",
    "pipeline": "lfp",
    "version": "v1.0.0",
    "params": "abcdef0",
    "config": "",
    "codebase": "v0.7.11",
}
_RESOURCES = CapsuleResources(memory="16GB", cpus_per_task=1, partition="mit_normal", time_limit="48:00:00")

_CASES = [
    pytest.param(prepare_aind_ephys_job, (), {"pipeline_version": 1}, id="prepare_aind_ephys_job-int-version"),
    pytest.param(
        prepare_aind_ephys_job,
        (),
        {"pipeline_version": "v1.0.1", "base_directory": "/some/path"},
        id="prepare_aind_ephys_job-str-directory",
    ),
    pytest.param(submit_job, (), {"script_file_path": "/some/submit.sh"}, id="submit_job-str-path"),
    pytest.param(
        generate_aind_ephys_submission_script, (), _AIND_SCRIPT_KWARGS, id="generate_aind_ephys_submission_script"
    ),
    pytest.param(
        build_lfp_pipeline_path,
        (),
        {"dandiset_id": 1, "output_within_dandiset_path": "sub-1"},
        id="build_lfp_pipeline_path",
    ),
    pytest.param(
        build_lfp_job_hash,
        (),
        {
            "dandiset_id": "000409",
            "within_dandiset_path": "x",
            "bidsy_version": "v1",
            "params_id": "a",
            "content_id": None,
        },
        id="build_lfp_job_hash-none-content-id",
    ),
    pytest.param(
        prepare_lfp_job, (), {"pipeline_version": "v1.0.0", "force_new_capsule": "yes"}, id="prepare_lfp_job-str-flag"
    ),
    pytest.param(generate_lfp_submission_script, (), _LFP_SCRIPT_KWARGS, id="generate_lfp_submission_script"),
    pytest.param(resolve_filter_kwargs, ("not a dict",), {}, id="resolve_filter_kwargs"),
    pytest.param(resolve_reference_spec, (["not", "a", "dict"],), {}, id="resolve_reference_spec"),
    pytest.param(load_lfp_parameters, (123,), {}, id="load_lfp_parameters"),
    pytest.param(validate_lfp_parameters, ("not a dict",), {}, id="validate_lfp_parameters"),
    pytest.param(
        write_dandiset_file,
        (),
        {"dandiset_id": 1697, "relative_path": "derivatives/jobs.tsv", "content": ""},
        id="write_dandiset_file-int-dandiset-id",
    ),
    pytest.param(move_job_capsule, (), {"capsule_path": _PATH}, id="move_job_capsule-path-for-str"),
    pytest.param(load_assets_jsonld_metadata, (1697,), {}, id="load_assets_jsonld_metadata"),
    pytest.param(
        AssetMetadata,
        (),
        {"path": "p", "date_modified": "d", "content_size": "12", "content_id": "c"},
        id="AssetMetadata-str-size",
    ),
    pytest.param(
        AssetsJsonldMetadata,
        (),
        {"content_id_to_asset": [], "path_to_asset_metadata": {}},
        id="AssetsJsonldMetadata-list-for-dict",
    ),
    pytest.param(PipelineQueue, (), {"entries": "not a list"}, id="PipelineQueue-str-entries"),
    pytest.param(PipelineQueue.from_jsonld, (), {"file_path": "/some/assets.jsonld"}, id="from_jsonld-str-path"),
    pytest.param(PipelineQueue.from_dandi, (), {"dandiset_id": 1697}, id="from_dandi-int-dandiset-id"),
    pytest.param(PipelineQueue.from_tsv, ("/some/jobs.tsv",), {}, id="from_tsv-str-path"),
    pytest.param(PipelineQueue.for_pipeline, (123,), {}, id="for_pipeline-int"),
    pytest.param(PipelineQueue(entries=[]).with_status, ("running",), {}, id="with_status-unknown-literal"),
    pytest.param(
        PipelineQueue.resolve_params_key_to_id, (), {"pipeline": "lfp", "params_key": None}, id="resolve_params_key"
    ),
    pytest.param(
        PipelineQueue.write_dandiset_jobs_table,
        (),
        {"base_directory": "/some/path"},
        id="write_dandiset_jobs_table-str-directory",
    ),
    pytest.param(
        PipelineQueue.dispatch_jobs,
        (),
        {"base_directory": _PATH, "jitter_seconds": "0"},
        id="dispatch_jobs-str-jitter",
    ),
    pytest.param(PipelineQueue.create_job_capsules, (), {"content_ids": "abc"}, id="create_job_capsules-str-ids"),
    pytest.param(JobCapsule.from_dict, ("not a dict",), {}, id="JobCapsule.from_dict"),
    pytest.param(JobCapsule.from_tsv_row, (["not", "a", "dict"],), {}, id="JobCapsule.from_tsv_row"),
    pytest.param(JobInfo, (), _JOB_INFO_KWARGS, id="JobInfo-int-job-id"),
    pytest.param(DispatchConfig, (), {"pipeline": "lfp", "max_concurrent": "2"}, id="DispatchConfig-str-limit"),
    pytest.param(
        DispatchConfig.from_pipeline_config,
        (),
        {"pipeline": "lfp", "pipeline_config": "not a dict"},
        id="DispatchConfig.from_pipeline_config",
    ),
    pytest.param(DispatchResult, (), {"pipeline": "lfp", "status": "bogus"}, id="DispatchResult-unknown-status"),
    pytest.param(
        DispatchedArray,
        (),
        {"array_job_id": 1, "task_count": 1, "resources": _RESOURCES, "max_concurrent": 1},
        id="DispatchedArray-int-job-id",
    ),
    pytest.param(
        CapsuleResources,
        (),
        {"memory": "16GB", "cpus_per_task": "1", "partition": "p", "time_limit": "t"},
        id="CapsuleResources-str-cpus",
    ),
    pytest.param(CapsuleResources.from_submission_script, (123,), {}, id="from_submission_script-int"),
    pytest.param(read_capsule_resources, ("code/dir",), {}, id="read_capsule_resources-str-for-list"),
    pytest.param(clean_dispatch_directories, (), {"base_directory": "/some/path"}, id="clean_dispatch_directories"),
    pytest.param(
        dispatch_pipeline_jobs,
        (),
        {
            "pipeline": 1,
            "code_dir_paths": [],
            "base_directory": _PATH,
            "dispatch_config": DispatchConfig(pipeline="lfp"),
            "dandiset_id": "001697",
        },
        id="dispatch_pipeline_jobs-int-pipeline",
    ),
    pytest.param(resolve_schema_path, (123,), {}, id="resolve_schema_path"),
    pytest.param(validate_against_schema, ("not a dict",), {"schema": "registry"}, id="validate_against_schema"),
    pytest.param(validate_registry, (["not", "a", "dict"],), {}, id="validate_registry"),
]


@pytest.mark.ai_generated
@pytest.mark.parametrize(("function", "args", "kwargs"), _CASES)
def test_rejects_bad_input_types(function: Callable, args: tuple, kwargs: dict) -> None:
    with pytest.raises(beartype.roar.BeartypeCallHintParamViolation):
        function(*args, **kwargs)
