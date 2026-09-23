import logging
import os
import pathlib
import shutil
import subprocess
import tempfile

import beartype

_log = logging.getLogger(__name__)


@beartype.beartype
def write_dandiset_file(
    *,
    dandiset_id: str,
    relative_path: str,
    content: str,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
) -> None:
    """
    Create or overwrite a single text file at *relative_path* within a remote Dandiset.

    Only ``dandiset.yaml`` is downloaded from *dandiset_id* to obtain a locally materialized
    Dandiset tree; *content* is then written to *relative_path* under that tree and uploaded
    back via ``dandi upload --allow-any-path``. No other assets in the Dandiset are downloaded
    or otherwise touched.

    Parameters
    ----------
    dandiset_id : str
        Dandiset the file is written into.
    relative_path : str
        Path of the file, relative to the Dandiset root (POSIX string). Leading/trailing
        slashes are stripped.
    content : str
        Text content to write.
    processing_directory : pathlib.Path, optional
        Directory in which the temporary working tree is created. When ``None``, the system
        default temporary location is used.
    test : bool, optional
        When ``True``, leave the temporary working tree on disk after a successful upload
        for debugging.

    Raises
    ------
    RuntimeError
        If ``DANDI_API_KEY`` is unset or blank, or if any ``dandi`` subprocess returns a
        non-zero exit code. The temporary working tree is intentionally left in place when
        any step fails so that it can be inspected.
    """
    if not os.environ.get("DANDI_API_KEY", "").strip():
        message = "`DANDI_API_KEY` environment variable is not set or is blank."
        raise RuntimeError(message)

    relative_file_path = relative_path.strip("/")

    processing_root = pathlib.Path(tempfile.mkdtemp(dir=processing_directory, prefix="write-dandiset-file-"))
    _log.info("Writing %s into Dandiset %s in %s", relative_file_path, dandiset_id, processing_root)

    dandiset_url = f"dandi://dandi/{dandiset_id}/"
    metadata_download = subprocess.run(
        ["dandi", "download", "--download", "dandiset.yaml", dandiset_url],
        capture_output=True,
        text=True,
        cwd=processing_root,
    )
    _log.info("dandi download of dandiset.yaml returned code %d for %s", metadata_download.returncode, dandiset_url)
    _log.debug("dandi download stdout: %s\nstderr: %s", metadata_download.stdout, metadata_download.stderr)
    if metadata_download.returncode != 0:
        _log.warning("dandi download stdout: %s\nstderr: %s", metadata_download.stdout, metadata_download.stderr)
        message = f"dandi download of dandiset.yaml failed for {dandiset_url}"
        raise RuntimeError(message)

    dandiset_directory = processing_root / dandiset_id
    target_file = dandiset_directory / relative_file_path
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text(content)

    upload = subprocess.run(
        ["dandi", "upload", "--allow-any-path", relative_file_path],
        capture_output=True,
        text=True,
        cwd=dandiset_directory,
    )
    _log.info("dandi upload returned code %d for %s", upload.returncode, dandiset_url)
    _log.debug("dandi upload stdout: %s\nstderr: %s", upload.stdout, upload.stderr)
    if upload.returncode != 0:
        _log.warning("dandi upload stdout: %s\nstderr: %s", upload.stdout, upload.stderr)
        message = f"dandi upload failed for {dandiset_url}{relative_file_path}"
        raise RuntimeError(message)

    if test:
        _log.info("Leaving temporary working tree in place for test mode: %s", processing_root)
    else:
        shutil.rmtree(processing_root)
