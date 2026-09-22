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

import dandi
import dandi.dandiapi
import dandi.download
import dandi.upload

from ._globals import _JOB_CAPSULES_DANDISET_ID, _LFP_CONTAINER_IMAGE_TEMPLATE, _LFP_CONTAINER_NAME
from ._handle_template import generate_lfp_submission_script
from ..aind_ephys_pipeline import UnmappedContentIDError
from ..dandiset._job_id import (
    _PROVENANCE_KEY,
    _capsule_names_from_asset_paths,
    _compute_job_hash,
    _find_existing_capsule_path,
    _next_available_job_id,
)

_log = logging.getLogger(__name__)

_CONTENT_ID_TO_USAGE_DANDISET_PATH_URL = (
    "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/derivatives/"
    "derivatives/content_id_to_usage_dandiset_path.jsonl"
)


def build_lfp_pipeline_path(*, dandiset_id: str, output_dandi_path: str) -> str:
    """
    Build the path of the LFP pipeline directory that holds an asset's job capsules.

    Returns
    -------
    str
        The pipeline directory path under the job capsules Dandiset.
    """
    pipeline_path = f"derivatives/dandiset-{dandiset_id}/{output_dandi_path}/pipeline-lfp"
    return pipeline_path


def build_lfp_job_hash(
    *,
    dandiset_id: str,
    dandi_path: str,
    bidsy_version: str,
    params_id: str,
    content_id: str,
) -> str:
    """
    Build the six-character hash that identifies one LFP job.

    The LFP pipeline has no config, so an empty config takes part in the hash. The codebase
    version is left out for the same reason it is on the AIND side: a job is the same logical
    job whichever release of this package formed it.

    Returns
    -------
    str
        The hash half of the job ID naming the capsule directory.
    """
    job_hash = _compute_job_hash(
        dandiset_id=dandiset_id,
        dandi_path=dandi_path,
        pipeline="lfp",
        version=bidsy_version,
        params=params_id,
        config="",
        content_id=content_id,
    )
    return job_hash


def _resolve_parameters_file(parameters_key: str, /) -> tuple[pathlib.Path, str]:
    """Resolve a registered parameters key to its file path and short MD5 identifier."""
    params_registry_path = pathlib.Path(__file__).parent / "registries" / "registered_params.json"
    params_registry = json.loads(params_registry_path.read_text())
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
    return parameters_file_path, actual_md5[0:7]


def prepare_lfp_job(
    *,
    pipeline_version: str,
    content_id: str | None = None,
    dandiset_id: str | None = None,
    dandiset_path: str | None = None,
    parameters_key: str = "default",
    force_new_capsule: bool = False,
    silent: bool = False,
) -> pathlib.Path | None:
    """
    Prepare a single LFP job capsule for an asset, or skip it if one already exists.

    This mirrors :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job`
    but is much simpler. The job runs a container via datalad-containers and duct,
    so there is no pipeline repository checkout and no Nextflow config. Exactly one
    capsule is ever prepared per asset. When a capsule already exists for the
    resolved output path, the function returns ``None`` without preparing another.

    Parameters
    ----------
    pipeline_version : str
        The container image tag to run, for example ``v0.4.0``.
    content_id : str, optional
        The content ID for the data to be processed.
    dandiset_id : str, optional
        The Dandiset ID, used to look up the content ID if it is not provided.
    dandiset_path : str, optional
        The asset path, used to look up the content ID if it is not provided.
    parameters_key : str, optional
        The registered LFP parameters key.
    force_new_capsule : bool, optional
        Whether to form a new capsule even when one already exists for this job,
        under a job ID disambiguated with a counter when the existing one was
        formed on the same day.
    silent : bool, optional
        Whether to suppress DANDI client output.

    Returns
    -------
    pathlib.Path or None
        The path to the generated submission script, or ``None`` if a capsule
        already existed.
    """
    if not content_id and not (dandiset_id and dandiset_path):
        message = "Either content_id or both dandiset_id and dandiset_path must be provided."
        raise ValueError(message)
    if pipeline_version == "":
        message = f"Pipeline version passed for `{content_id=}` is empty!"
        raise ValueError(message)

    parameters_file_path, params_id = _resolve_parameters_file(parameters_key)

    if content_id is None:
        client = dandi.dandiapi.DandiAPIClient()
        source_dandiset = client.get_dandiset(dandiset_id=dandiset_id)
        asset = source_dandiset.get_asset_by_path(path=dandiset_path)
        metadata = asset.get_raw_metadata()
        content_id = metadata["contentUrl"][1].split("/")[-1]

    with urllib.request.urlopen(url=_CONTENT_ID_TO_USAGE_DANDISET_PATH_URL) as response:
        decoded = response.read().decode()
    content_id_to_usage_dandiset_path = {
        content_id_key: usage_dandiset_path
        for line in decoded.splitlines()
        if line.strip()
        for content_id_key, usage_dandiset_path in json.loads(line).items()
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

    dandi_compute_dir = pathlib.Path("/orcd/data/dandi/001/dandi-compute")
    dandi_compute_code_source_dir = dandi_compute_dir / "code"
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
    pipeline_dandiset_path = build_lfp_pipeline_path(dandiset_id=dandiset_id, output_dandi_path=output_dandi_path)
    job_hash = build_lfp_job_hash(
        dandiset_id=dandiset_id,
        dandi_path=dandiset_path,
        bidsy_version=bidsy_pipeline_version,
        params_id=params_id,
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
        _log.info(f"LFP capsule already exists at {existing_capsule_path}; skipping preparation.")
        return None

    job_id = _next_available_job_id(job_hash=job_hash, taken_job_ids=existing_capsule_names)
    output_dandiset_path_base = f"{pipeline_dandiset_path}/{job_id}"

    blob_head = content_id[0]
    partition = "001" if ord(blob_head) - ord("0") <= 8 else "002"
    nwbfile_path = f"/orcd/data/dandi/{partition}/s3dandiarchive/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}"

    processing_directory = dandi_compute_dir / "processing"
    temporary_processing_directory = pathlib.Path(tempfile.mkdtemp(dir=processing_directory, prefix="prepare-job-"))
    dandi.download.download(
        urls=f"DANDI:{_JOB_CAPSULES_DANDISET_ID}",
        output_dir=temporary_processing_directory,
        get_metadata=True,
        get_assets=False,
    )

    dandiset_output_dir = temporary_processing_directory / _JOB_CAPSULES_DANDISET_ID / output_dandiset_path_base
    code_dir = dandiset_output_dir / "code"
    script_file_path = code_dir / "submit.sh"
    code_parameters_file_path = code_dir / parameters_file_path.name
    dataset_description_file_path = dandiset_output_dir / "dataset_description.json"
    log_directory = dandiset_output_dir / "logs"
    output_nwb_directory = dandiset_output_dir / "nwb"

    code_dir.mkdir(parents=True)
    log_directory.mkdir()
    output_nwb_directory.mkdir()

    output_nwb_file_path = output_nwb_directory / f"{pathlib.Path(output_dandi_path).name}_desc-lfp"
    container_image = _LFP_CONTAINER_IMAGE_TEMPLATE.format(version=pipeline_version)
    environment_directory = "/orcd/data/dandi/001/environments/name-lfp_environment"
    done_tracker_file_path = processing_directory / "done.txt"

    dataset_description = {
        "Name": f"DANDI Compute: LFP pipeline output for Dandiset {dandiset_id}",
        "BIDSVersion": "1.10",
        "DatasetType": "derivative",
        "GeneratedBy": [
            {
                "Name": "DANDI Compute: LFP Pipeline",
                "Description": "A minimal SpikeInterface-based LFP extraction pipeline run from a container.",
                "Version": pipeline_version,
                "CodeURL": container_image,
            },
            {
                "Name": "DANDI Compute: Code",
                "Description": "The primary source code for orchestration on MIT Engaging.",
                "Version": f"v{codebase_version}+{dandi_compute_code_commit_hash}",
                "CodeURL": "https://github.com/dandi-compute/code",
            },
        ],
        "SourceDatasets": [{"URL": f"https://dandiarchive.org/dandiset/{dandiset_id}/"}],
        # Everything the capsule directory name used to spell out. This block is what the
        # queue state table reads back to describe the job.
        _PROVENANCE_KEY: {
            "job_id": job_id,
            "dandiset_id": dandiset_id,
            "dandi_path": dandiset_path,
            "content_id": content_id,
            "pipeline": "lfp",
            "version": bidsy_pipeline_version,
            "codebase": f"v{codebase_version}",
            "params": params_id,
            "config": "",
            "params_key": parameters_key,
        },
    }

    _log.info(f"Writing LFP job files to {dandiset_output_dir.absolute()}")
    generate_lfp_submission_script(
        script_file_path=script_file_path,
        log_directory=str(log_directory),
        dataset_directory=str(dandiset_output_dir),
        environment_directory=environment_directory,
        container_name=_LFP_CONTAINER_NAME,
        container_image=container_image,
        nwb_file_path=nwbfile_path,
        output_nwb_file_path=str(output_nwb_file_path),
        parameters_key=parameters_key,
        temp_name=temporary_processing_directory.name,
        done_tracker_file_path=str(done_tracker_file_path),
    )
    code_parameters_file_path.write_text(data=parameters_file_path.read_text())
    dataset_description_file_path.write_text(data=json.dumps(obj=dataset_description, indent=2))

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
