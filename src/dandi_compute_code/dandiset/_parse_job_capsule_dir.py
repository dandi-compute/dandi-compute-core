import datetime
import json
import pathlib

import beartype

from ._job_id import _JOB_ID_RE, _PROVENANCE_KEY
from ._parse_content_id_from_submission_script import _parse_content_id_from_submission_script
from ..queue._job_capsule import _derive_job_status


@beartype.beartype
def _parse_job_capsule_dir(capsule_dir: pathlib.Path, /) -> dict | None:
    """
    Parse a single job capsule directory into a flat record dict.

    The expected path structure (relative to
    ``derivatives/dandisets-{first 3 digits}/dandiset-{dandiset_id}/``) is::

        <dandi-path>/pipeline-{pipeline}/job-{YYMMDD}{hash}/

    The capsule directory name carries only the job ID, so the pipeline version, codebase
    version, parameters and config are read from its ``dataset_description.json`` provenance.

    Parameters
    ----------
    capsule_dir : pathlib.Path
        The job capsule directory.

    Returns
    -------
    dict or None
        A flat dict with all entities and the lifecycle status, or ``None`` if
        the path does not match the expected structure.
    """
    if _JOB_ID_RE.fullmatch(capsule_dir.name) is None:
        return None

    pipeline_dir = capsule_dir.parent
    if not pipeline_dir.name.startswith("pipeline-"):
        return None
    pipeline = pipeline_dir.name[len("pipeline-") :]

    dandiset_dir = next(
        (parent for parent in pipeline_dir.parents if parent.name.startswith("dandiset-")),
        None,
    )
    if dandiset_dir is None:
        return None
    dandiset_id = dandiset_dir.name[len("dandiset-") :]
    dandi_path_parts = pipeline_dir.relative_to(dandiset_dir).parts[:-1]
    if not dandi_path_parts:
        return None
    dandi_path = pathlib.PurePosixPath(*dandi_path_parts).as_posix()

    provenance = _read_capsule_provenance(capsule_dir)

    code_dir = capsule_dir / "code"
    has_code = code_dir.is_dir()
    has_been_submitted = has_code and ((code_dir / "submitted").exists() or any(code_dir.glob("submitted_date-*")))
    has_output = (capsule_dir / "derivatives").is_dir()
    logs_dir = capsule_dir / "logs"
    has_logs = logs_dir.is_dir() and any(f for f in logs_dir.iterdir() if f.name != "dataset_description.json")
    status = _derive_job_status(
        has_code=has_code,
        has_been_submitted=has_been_submitted,
        has_logs=has_logs,
        has_output=has_output,
    )
    created_at = datetime.datetime.fromtimestamp(capsule_dir.stat().st_ctime, tz=datetime.timezone.utc).isoformat()
    content_id = _parse_content_id_from_submission_script(capsule_dir)

    record = {
        "job_id": capsule_dir.name,
        "dandiset_id": dandiset_id,
        "content_id": content_id,
        "dandi_path": dandi_path,
        "pipeline": pipeline,
        "version": provenance.get("version", ""),
        "codebase": provenance.get("codebase", ""),
        "params": provenance.get("params", ""),
        "config": provenance.get("config", ""),
        "status": status,
        "created_at": created_at,
    }
    return record


@beartype.beartype
def _read_capsule_provenance(capsule_dir: pathlib.Path, /) -> dict:
    """Read the job provenance block from a capsule's local ``dataset_description.json``."""
    dataset_description_file = capsule_dir / "dataset_description.json"
    if not dataset_description_file.is_file():
        return {}
    try:
        dataset_description = json.loads(dataset_description_file.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    provenance = dataset_description.get(_PROVENANCE_KEY)
    return provenance if isinstance(provenance, dict) else {}
