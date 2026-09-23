import datetime
import logging
import os
import pathlib
import subprocess

import beartype

_log = logging.getLogger(__name__)


@beartype.beartype
def submit_job(script_file_path: pathlib.Path) -> None:
    """
    Submit a pipeline script via sbatch.

    Raises
    ------
    RuntimeError
        If the ``DANDI_API_KEY`` environment variable is not set, if the
        ``sbatch`` submission returns a non-zero exit code, or if the
        subsequent ``dandi upload`` invocation returns a non-zero exit code.
    """
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)

    absolute_script_file_path = script_file_path.absolute()
    command = ["sbatch", str(absolute_script_file_path)]
    _log.info(f"Submitting sbatch script: {absolute_script_file_path}")
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )
    _log.info(f"sbatch returned code: {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")
    if result.returncode != 0:
        message = "sbatch submission failed - please check the logs to see more details."
        raise RuntimeError(message)

    now = datetime.datetime.now()
    submitted_file_path = absolute_script_file_path.parent / (
        f"submitted_date-{now.year:04d}+{now.month:02d}+{now.day:02d}"
        f"_time-{now.hour:02d}+{now.minute:02d}+{now.second:02d}"
    )
    submitted_file_path.write_bytes(b"1")
    _log.info("Created `submitted` file at: %s", submitted_file_path.absolute())
    result = subprocess.run(
        ["dandi", "upload"],
        capture_output=True,
        text=True,
        cwd=absolute_script_file_path.parent,
    )
    _log.info(f"Upload to DANDI returned code: {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")
    if result.returncode != 0:
        message = "DANDI upload failed - please check the logs to see more details."
        raise RuntimeError(message)
