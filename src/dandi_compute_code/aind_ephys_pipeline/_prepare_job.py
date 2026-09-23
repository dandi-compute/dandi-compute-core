import contextlib
import hashlib
import importlib.metadata
import io
import json
import logging
import os
import pathlib
import re
import subprocess
import tempfile
import urllib.request

import beartype
import dandi
import dandi.dandiapi
import dandi.download
import dandi.upload

from ._handle_template import generate_aind_ephys_submission_script
from ..dandiset._globals import (
    _JOB_CAPSULES_DANDISET_ID,
    _dandiset_derivatives_relative_dir,
)
from ..dandiset._job_id import (
    _PROVENANCE_KEY,
    _capsule_names_from_asset_paths,
    _compute_job_hash,
    _find_existing_capsule_path,
    _next_available_job_id,
)
from ..schemas import validate_registry

_log = logging.getLogger(__name__)


class UnmappedContentIDError(ValueError):
    """Raised when a content ID cannot be resolved to a unique Dandiset path."""


@beartype.beartype
def _parse_pipeline_version(version: str, *, label: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?:[-+][0-9A-Za-z.+-]+)?", version)
    if match is None:
        message = f"Unexpected {label} version format: {version!r}"
        raise ValueError(message)
    return int(match["major"]), int(match["minor"]), int(match["patch"])


@beartype.beartype
def prepare_aind_ephys_job(
    pipeline_version: str,
    content_id: str | None = None,
    dandiset_id: str | None = None,
    dandiset_path: str | None = None,
    config_key: str = "default",
    parameters_key: str = "default",
    pipeline_directory: pathlib.Path | None = None,
    force_new_capsule: bool = False,
    silent: bool = False,
) -> pathlib.Path | None:
    """
    Prepares an AIND ephys job by generating a submission script and returning the script file path.

    A job capsule is never formed twice. When one already exists on the archive for the
    requested pipeline version, parameters and config, nothing is prepared and ``None`` is
    returned. Pass ``force_new_capsule`` to form another capsule anyway, under a job ID
    disambiguated with a counter when the existing one was formed on the same day.

    Parameters
    ----------
    pipeline_version : str
        The version of the pipeline to use, which will be used to checkout a branch of the pipeline repository.
    content_id : str
        The content ID for the data to be processed.
    dandiset_id : str, optional
        The Dandiset ID for the data to be processed. Required if `content_id`
        is not provided and will be used to look up the content ID if `content_id` is not provided.
    dandiset_path : str, optional
        The local path to the Dandiset data to be processed. Required if `content_id
        is not provided and will be used to look up the content ID if `content_id` is not provided.
    config_key : str
        The short name of the configuration to use.
        Must be a key registered in `registries/registered_configs.json`.
    parameters_key : str
        The short name of the parameters to use.
        Must be a key registered in `registries/registered_params.json`.
    pipeline_directory : pathlib.Path, optional
        Local path to the AIND pipeline repository.
    force_new_capsule : bool, optional
        Whether to form a new job capsule even when one already exists for this job.
        Default is False.
    silent : bool, optional
        Whether to suppress output messages from the DANDI client.
        Default is False.

    Returns
    -------
    script_file_path : pathlib.Path or None
        The path to the generated submission script, or ``None`` when a job capsule
        already exists for this combination.

    Raises
    ------
    ValueError
        Raised in any of the following situations.

        - Neither ``content_id`` nor both ``dandiset_id`` and ``dandiset_path``
          are provided.
        - ``pipeline_version`` is an empty string.
        - ``pipeline_version`` equals the unsupported ``v1.0.0``.
        - ``config_key`` is not registered in ``registered_configs.json``.
        - ``parameters_key`` is not registered in ``registered_params.json``.
        - The resolved parameters file is missing a ``pipeline_version`` field.
        - The parameters file ``pipeline_version`` is in a different major
          series than the requested ``pipeline_version``.
        - The parameters file ``pipeline_version`` is newer than the requested
          ``pipeline_version``.
        - The MD5 checksum of the resolved config or parameters file does not
          match its registry entry.
        - ``content_id`` is not present in the content-id-to-Dandiset mapping.
        - The ``sub`` BIDS entity cannot be extracted from the resolved
          Dandiset path.
        - A resolved git commit hash does not match the expected
          40-character hexadecimal format.
    """
    if not content_id and not (dandiset_id and dandiset_path):
        message = "Either --id or both --dandiset and --dandipath must be provided."
        raise ValueError(message)
    if pipeline_version == "":
        message = f"Pipeline version passed for `{content_id=}` is empty!"
        raise ValueError(message)
    if pipeline_version == "v1.0.0":
        message = (
            "Version `v1.0.0` is incompatible with the new parameters file usage." "Please use `v1.0.0-fixes` instead."
        )
        raise ValueError(message)
    requested_pipeline_version = _parse_pipeline_version(pipeline_version, label="requested pipeline")
    config_registry_path = pathlib.Path(__file__).parent / "registries" / "registered_configs.json"
    config_registry = json.loads(config_registry_path.read_text())
    validate_registry(config_registry, description=f"registry '{config_registry_path.name}'")
    if config_key not in config_registry:
        registered_keys = list(config_registry.keys())
        message = (
            f"Config key '{config_key}' is not registered. "
            f"Registered keys are: {registered_keys}. "
            "To register a new config file, add the `.config` file to the `configs/` directory "
            "and add an entry to `registries/registered_configs.json` mapping the short name to its "
            "relative `path` and full MD5 `md5`."
        )
        raise ValueError(message)
    config_file_path = pathlib.Path(__file__).parent / "configs" / config_registry[config_key]["path"]
    expected_config_md5 = config_registry[config_key]["md5"]

    params_registry_path = pathlib.Path(__file__).parent / "registries" / "registered_params.json"
    params_registry = json.loads(params_registry_path.read_text())
    validate_registry(params_registry, description=f"registry '{params_registry_path.name}'")
    if parameters_key not in params_registry:
        registered_keys = list(params_registry.keys())
        message = (
            f"Parameters key '{parameters_key}' is not registered. "
            f"Registered keys are: {registered_keys}. "
            "To register a new parameters file, add the JSON file to the `params/` directory "
            "and add an entry to `registries/registered_params.json` mapping the short name to its "
            "relative `path` and full MD5 `md5`."
        )
        raise ValueError(message)
    parameters_file_path = pathlib.Path(__file__).parent / "params" / params_registry[parameters_key]["path"]
    actual_md5 = hashlib.md5(parameters_file_path.read_bytes()).hexdigest()
    expected_md5 = params_registry[parameters_key]["md5"]
    if actual_md5 != expected_md5:
        message = (
            f"MD5 mismatch for parameters file '{parameters_file_path.name}': "
            f"expected {expected_md5!r}, got {actual_md5!r}. "
            "The file may have been modified. Update the `md5` in `registries/registered_params.json` "
            "to reflect the new file contents."
        )
        raise ValueError(message)
    parameters = json.loads(parameters_file_path.read_text())
    if "pipeline_version" not in parameters:
        message = f"Parameters file '{parameters_file_path.name}' is missing required 'pipeline_version'."
        raise ValueError(message)
    parameters_pipeline_version = _parse_pipeline_version(
        parameters["pipeline_version"], label="parameters file pipeline"
    )
    different_major = parameters_pipeline_version[0] != requested_pipeline_version[0]
    params_too_new = parameters_pipeline_version > requested_pipeline_version
    if different_major:
        message = (
            f"Parameters file '{parameters_file_path.name}' targets pipeline version "
            f"{parameters['pipeline_version']!r}, which is in a different major series than requested "
            f"pipeline version {pipeline_version!r}. Requested pipeline version must be in the same major series."
        )
        raise ValueError(message)
    if params_too_new:
        message = (
            f"Parameters file '{parameters_file_path.name}' targets pipeline version "
            f"{parameters['pipeline_version']!r}, which is newer than requested "
            f"pipeline version {pipeline_version!r}. Requested pipeline version must be at least as new as the "
            "parameters file version."
        )
        raise ValueError(message)
    params_id = actual_md5[0:7]

    if content_id is None:
        client = dandi.dandiapi.DandiAPIClient()
        dandiset = client.get_dandiset(dandiset_id=dandiset_id)
        asset = dandiset.get_asset_by_path(path=dandiset_path)
        metadata = asset.get_raw_metadata()
        content_id = metadata["contentUrl"][1].split("/")[-1]

    actual_config_md5 = hashlib.md5(config_file_path.read_bytes()).hexdigest()
    if actual_config_md5 != expected_config_md5:
        message = (
            f"MD5 mismatch for config file '{config_file_path.name}': "
            f"expected {expected_config_md5!r}, got {actual_config_md5!r}. "
            "The file may have been modified. Update the `md5` in `registries/registered_configs.json` "
            "to reflect the new file contents."
        )
        raise ValueError(message)
    config_id = actual_config_md5[0:7]

    dandi_compute_dir = pathlib.Path("/orcd/data/dandi/001/dandi-compute")
    content_id_to_usage_dandiset_path_url = (
        "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/derivatives/"
        "derivatives/content_id_to_usage_dandiset_path.jsonl"
    )
    with urllib.request.urlopen(url=content_id_to_usage_dandiset_path_url) as response:
        decoded = response.read().decode()
    content_id_to_usage_dandiset_path = {
        content_id_key: dandiset_path
        for line in decoded.splitlines()
        if line.strip()
        for content_id_key, dandiset_path in json.loads(line).items()
    }

    if content_id not in content_id_to_usage_dandiset_path:
        message = (
            f"Content ID {content_id} not found in content ID to usage Dandiset path mapping. "
            "This likely means that the content ID is not associated with a Dandiset, "
            "or that the mapping file is out of date."
        )
        raise UnmappedContentIDError(message)

    dandiset_id, dandiset_path = next(iter(content_id_to_usage_dandiset_path[content_id].items()))
    output_dandi_path = dandiset_path.removesuffix(".nwb")
    # Parse BIDS entities from all path components (directory parts and filename stem),
    # and skip tokens that are modality suffixes (no "-" separator, e.g. "ecephys").
    # Directory parts are needed for AIND-style paths where "sub-" appears only in the folder name.
    entities = {}
    path_obj = pathlib.Path(dandiset_path)
    for part in [*path_obj.parts[:-1], path_obj.stem]:
        for token in part.split("_"):
            if "-" not in token:
                continue
            key, value = token.split("-", 1)
            entities[key] = value

    # Special case - inject missing subject label for testing asset
    if content_id == "048d1ee9-83b7-491f-8f02-1ca615b1d455":
        entities.setdefault("sub", "test")

    if "sub" not in entities:
        message = (
            f"Could not extract 'sub' BIDS entity from dandiset path: {dandiset_path!r}. "
            "The path must contain a directory component or filename token of the form 'sub-<label>'."
        )
        raise ValueError(message)

    # TODO: if first run for asset, skip below and add sourcedata

    pipeline_directory = (pipeline_directory or dandi_compute_dir / "aind-ephys-pipeline").absolute()
    pipeline_file_path = pipeline_directory / "pipeline" / "main_multi_backend.nf"
    dandi_compute_code_source_dir = dandi_compute_dir / "code"

    pipeline_commit_hash = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=pipeline_directory,
        text=True,
    ).strip()
    if not re.match(r"^[0-9a-f]{40}$", pipeline_commit_hash):
        message = f"Unexpected commit hash format: {pipeline_commit_hash}"
        raise ValueError(message)

    dandi_compute_code_commit_hash = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=dandi_compute_code_source_dir,
        text=True,
    ).strip()
    if not re.match(r"^[0-9a-f]{40}$", dandi_compute_code_commit_hash):
        message = f"Unexpected commit hash format: {dandi_compute_code_commit_hash}"
        raise ValueError(message)

    codebase_version = importlib.metadata.version("dandi-compute-code")
    bidsy_pipeline_version = pipeline_version.replace("-", "+")
    pipeline_dandiset_path = (
        f"derivatives/{_dandiset_derivatives_relative_dir(dandiset_id)}/{output_dandi_path}/pipeline-aind+ephys"
    )
    job_hash = _compute_job_hash(
        dandiset_id=dandiset_id,
        dandi_path=dandiset_path,
        pipeline="aind+ephys",
        version=bidsy_pipeline_version,
        params=params_id,
        config=config_id,
        content_id=content_id,
    )
    client = dandi.dandiapi.DandiAPIClient(token=os.environ["DANDI_API_KEY"])
    dandiset = client.get_dandiset(dandiset_id=_JOB_CAPSULES_DANDISET_ID)
    existing_capsule_names = _capsule_names_from_asset_paths(
        asset_paths=(asset.path for asset in dandiset.get_assets_with_path_prefix(path=f"{pipeline_dandiset_path}/")),
        pipeline_dandiset_path=pipeline_dandiset_path,
    )
    existing_capsule_path = _find_existing_capsule_path(
        capsule_names=existing_capsule_names,
        pipeline_dandiset_path=pipeline_dandiset_path,
        job_hash=job_hash,
    )
    if existing_capsule_path is not None and not force_new_capsule:
        _log.info(f"A job capsule already exists at {existing_capsule_path}; skipping preparation.")
        return None

    job_id = _next_available_job_id(job_hash=job_hash, taken_job_ids=existing_capsule_names)
    output_dandiset_path = f"{pipeline_dandiset_path}/{job_id}"

    blob_head = content_id[0]
    partition = "001" if ord(blob_head) - ord("0") <= 8 else "002"  # TODO: pull from source to keep up to date
    nwbfile_path = f"/orcd/data/dandi/{partition}/s3dandiarchive/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}"
    # TODO: figure out if Zarr or not - only supports blobs ATM

    # Create an empty copy of Dandiset
    processing_directory = dandi_compute_dir / "processing"
    temporary_processing_directory = pathlib.Path(tempfile.mkdtemp(dir=processing_directory, prefix="prepare-job-"))
    dandi.download.download(
        urls="DANDI:001697",
        output_dir=temporary_processing_directory,
        get_metadata=True,
        get_assets=False,
    )

    # Construct BIDS derivative content
    dandiset_output_dir = temporary_processing_directory / "001697" / output_dandiset_path

    code_dir = dandiset_output_dir / "code"
    script_file_path = code_dir / "submit.sh"
    code_config_file_path = code_dir / config_file_path.name
    code_parameters_file_path = code_dir / parameters_file_path.name
    code_pipeline_file_path = code_dir / pipeline_file_path.name
    code_capsule_versions_file_path = code_dir / "capsule_versions.env"
    dataset_description_file_path = dandiset_output_dir / "dataset_description.json"

    log_directory = dandiset_output_dir / "logs"

    code_dir.mkdir(parents=True)
    log_directory.mkdir()
    intermediate_dir = dandiset_output_dir / "intermediate"
    intermediate_dir.mkdir()

    work_directory = dandi_compute_dir / "work"
    apptainer_cache_directory = work_directory / "apptainer_cache"
    # NOTE: NUMBA_CACHE_DIR is also needed for the pipeline
    # but must be set in `~/.bashrc`, and must be the same as WORKDIR
    # NUMBA_CACHE_DIR = "/orcd/data/dandi/001/dandi-compute/work"
    environment_directory = "/orcd/data/dandi/001/environments/name-nextflow_environment"
    done_tracker_file_path = processing_directory / "done.txt"

    pipeline_repo_directory = pipeline_file_path.parent.parent
    capsule_versions_file_path = pipeline_repo_directory / "pipeline" / "capsule_versions.env"

    # TODO: could look up description, authors, license, etc. from source dandiset metadata
    pipeline_url = f"https://github.com/CodyCBakerPhD/aind-ephys-pipeline/tree/{pipeline_version.replace('+','%2B')}"
    dataset_description = {
        "Name": f"DANDI Compute: AIND Ephys pipeline output for Dandiset {dandiset_id}",
        "BIDSVersion": "1.10",
        "DatasetType": "study",
        "GeneratedBy": [
            {
                "Name": "AIND Ephys Pipeline",
                "Description": "A customized and version-locked branch of the main AIND ephys pipeline.",
                "Version": f"{pipeline_version}+{pipeline_commit_hash}",
                "CodeURL": pipeline_url,
            },
            {
                "Name": "DANDI Compute: Code",
                "Description": "The primary source code for orchestration of AIND on MIT Engaging.",
                "Version": f"v{codebase_version}+{dandi_compute_code_commit_hash}",
                "CodeURL": "https://github.com/dandi-compute/dandi-compute-core",
            },
        ],
        "SourceDatasets": [{"URL": f"https://dandiarchive.org/dandiset/{dandiset_id}/"}],
        # Everything the capsule directory name used to spell out. This block is what the
        # queue jobs table reads back to describe the job.
        _PROVENANCE_KEY: {
            "job_id": job_id,
            "dandiset_id": dandiset_id,
            "dandi_path": dandiset_path,
            "content_id": content_id,
            "pipeline": "aind+ephys",
            "version": bidsy_pipeline_version,
            "codebase": f"v{codebase_version}",
            "params": params_id,
            "config": config_id,
            "params_key": parameters_key,
            "config_key": config_key,
        },
    }

    # Construct submission script from template
    _log.info(f"Writing job files to {pipeline_file_path.parent.absolute()}")
    generate_aind_ephys_submission_script(
        script_file_path=script_file_path,
        log_directory=str(log_directory),
        nwb_file_path=nwbfile_path,
        results_directory=str(intermediate_dir),  # Start off the results in the intermediate folder and separate later
        work_directory=str(work_directory),
        apptainer_cache_directory=str(apptainer_cache_directory),
        environment_directory=environment_directory,
        config_file_path=str(code_config_file_path),
        pipeline_file_path=str(pipeline_file_path),
        pipeline_repo_directory=str(pipeline_repo_directory.absolute()),
        pipeline_version=pipeline_version,
        temp_name=temporary_processing_directory.name,
        done_tracker_file_path=str(done_tracker_file_path),
        params_file_path=str(code_parameters_file_path),
    )
    code_config_file_path.write_text(data=config_file_path.read_text())
    code_parameters_file_path.write_text(data=parameters_file_path.read_text())
    code_capsule_versions_file_path.write_text(data=capsule_versions_file_path.read_text())
    code_pipeline_file_path.write_text(data=pipeline_file_path.read_text())
    dataset_description_file_path.write_text(data=json.dumps(obj=dataset_description, indent=2))

    # Upload preparation to 'reserve' spot during processing
    if silent:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            dandi.upload.upload(
                paths=[dandiset_output_dir],
                allow_any_path=True,
                validation=dandi.upload.UploadValidation.SKIP,
            )
    else:
        dandi.upload.upload(
            paths=[dandiset_output_dir],
            allow_any_path=True,
            validation=dandi.upload.UploadValidation.SKIP,
        )

    return script_file_path
