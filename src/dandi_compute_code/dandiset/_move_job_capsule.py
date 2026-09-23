import logging
import os
import pathlib
import shutil
import subprocess
import tempfile

import beartype

from ._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID, _JOB_CAPSULES_DANDISET_ID

_log = logging.getLogger(__name__)


@beartype.beartype
def move_job_capsule(
    *,
    capsule_path: str,
    source_dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
    target_dandiset_id: str = _FAILED_RUNS_ARCHIVE_DANDISET_ID,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
) -> None:
    """
    Move a single job capsule folder from one Dandiset to another.

    The capsule subtree is downloaded from *source_dandiset_id* into a fresh
    temporary working tree via ``dandi download --preserve-tree``, copied under
    a locally materialized copy of the *target_dandiset_id* tree, uploaded via
    ``dandi upload --allow-any-path``, and only then removed from
    *source_dandiset_id* via ``dandi delete``. The temporary working tree is
    removed on success. Each step is restricted to the single capsule subtree
    so the operation touches the minimum number of assets.

    The path of the capsule inside the Dandiset is preserved exactly. Only the
    enclosing Dandiset changes. Deletion from the source happens only after the
    upload to the target succeeds, so a failed upload never destroys the
    original.

    Parameters
    ----------
    capsule_path : str
        Path of the job capsule folder relative to the Dandiset root (for
        example ``derivatives/dandisets-000/dandiset-000409/sub-mouse01/pipeline-aind+ephys/
        job-260916a1b2c3``).
    source_dandiset_id : str, optional
        Dandiset the capsule is moved from. Defaults to the job capsules
        Dandiset (``001697``).
    target_dandiset_id : str, optional
        Dandiset the capsule is moved to. Defaults to the failed runs archive
        Dandiset (``001873``).
    processing_directory : pathlib.Path, optional
        Directory in which the temporary per-capsule working tree is created.
        When ``None``, the system default temporary location is used.
    test : bool, optional
        When ``True``, leave the temporary working tree on disk after a
        successful move for debugging.

    Raises
    ------
    RuntimeError
        If ``DANDI_API_KEY`` is unset or blank, or if any ``dandi`` subprocess
        returns a non-zero exit code. The temporary working tree is
        intentionally left in place when any step fails so that it can be
        inspected.
    """
    if not os.environ.get("DANDI_API_KEY", "").strip():
        message = "`DANDI_API_KEY` environment variable is not set or is blank."
        raise RuntimeError(message)

    relative_capsule_path = capsule_path.strip("/")

    processing_root = pathlib.Path(tempfile.mkdtemp(dir=processing_directory, prefix="move-capsule-"))
    _log.info(
        "Moving job capsule %s from %s to %s in %s",
        relative_capsule_path,
        source_dandiset_id,
        target_dandiset_id,
        processing_root,
    )

    source_url = f"dandi://dandi/{source_dandiset_id}/{relative_capsule_path}/"
    download = subprocess.run(
        ["dandi", "download", "--preserve-tree", source_url],
        capture_output=True,
        text=True,
        cwd=processing_root,
    )
    _log.info("dandi download returned code %d for %s", download.returncode, source_url)
    _log.debug("dandi download stdout: %s\nstderr: %s", download.stdout, download.stderr)
    if download.returncode != 0:
        _log.warning("dandi download stdout: %s\nstderr: %s", download.stdout, download.stderr)
        message = f"dandi download failed for {source_url}"
        raise RuntimeError(message)

    target_url = f"dandi://dandi/{target_dandiset_id}/"
    metadata_download = subprocess.run(
        ["dandi", "download", "--download", "dandiset.yaml", target_url],
        capture_output=True,
        text=True,
        cwd=processing_root,
    )
    _log.info("dandi download of dandiset.yaml returned code %d for %s", metadata_download.returncode, target_url)
    _log.debug("dandi download stdout: %s\nstderr: %s", metadata_download.stdout, metadata_download.stderr)
    if metadata_download.returncode != 0:
        _log.warning("dandi download stdout: %s\nstderr: %s", metadata_download.stdout, metadata_download.stderr)
        message = f"dandi download of dandiset.yaml failed for {target_url}"
        raise RuntimeError(message)

    source_capsule_directory = processing_root / source_dandiset_id / relative_capsule_path
    target_capsule_directory = processing_root / target_dandiset_id / relative_capsule_path
    target_capsule_directory.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_capsule_directory, target_capsule_directory)

    target_dandiset_directory = processing_root / target_dandiset_id
    upload = subprocess.run(
        ["dandi", "upload", "--allow-any-path", relative_capsule_path],
        capture_output=True,
        text=True,
        cwd=target_dandiset_directory,
    )
    _log.info("dandi upload returned code %d for %s", upload.returncode, target_url)
    _log.debug("dandi upload stdout: %s\nstderr: %s", upload.stdout, upload.stderr)
    if upload.returncode != 0:
        _log.warning("dandi upload stdout: %s\nstderr: %s", upload.stdout, upload.stderr)
        message = f"dandi upload failed for {target_url}{relative_capsule_path}"
        raise RuntimeError(message)

    delete = subprocess.run(
        ["dandi", "delete", str(source_capsule_directory)],
        input=b"y\n",
        capture_output=True,
    )
    _log.info("dandi delete returned code %d for %s", delete.returncode, source_url)
    _log.debug("dandi delete stdout: %s\nstderr: %s", delete.stdout, delete.stderr)
    if delete.returncode != 0:
        _log.warning("dandi delete stdout: %s\nstderr: %s", delete.stdout, delete.stderr)
        message = (
            f"dandi delete failed for {source_url}; the capsule was uploaded to {target_dandiset_id} "
            f"but could not be removed from {source_dandiset_id}"
        )
        raise RuntimeError(message)

    if test:
        _log.info("Leaving temporary working tree in place for test mode: %s", processing_root)
    else:
        shutil.rmtree(processing_root)
