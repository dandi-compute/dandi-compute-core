"""
Plain helpers for working with the example queue (``example_state_files/state.tsv``).

These are ordinary functions, imported and called directly by the queue tests.
They need nothing from pytest, so they are deliberately not fixtures. The pytest
fixtures (temporary directories, environment and network setup) live in
``conftest.py``.
"""

import pathlib

from dandi_compute_code.dandiset._globals import _dandiset_derivatives_relative_dir
from dandi_compute_code.queue import JobCapsule


def create_job_capsule_directory(
    *,
    base_dir: pathlib.Path,
    entry: JobCapsule,
    with_code: bool = True,
    with_output: bool = False,
    with_logs: bool = False,
    submitted: bool = False,
) -> pathlib.Path:
    """
    Materialize the on-disk job capsule directory for *entry* under *base_dir*.

    The layout is derived from the entry itself (via the public ``JobCapsule.capsule_dir``)
    so the directory tree always matches the ground-truth example state rather than a
    separately specified set of coordinates.
    """
    capsule_dir = entry.capsule_dir(base_dir)
    capsule_dir.mkdir(parents=True)
    if with_code:
        code_dir = capsule_dir / "code"
        code_dir.mkdir()
        (code_dir / "submit.sh").write_text("#!/bin/bash\necho hello\n")
        if submitted:
            (code_dir / "submitted_date-date-2025+01+01_time-00+00+00").touch()
    if with_output:
        (capsule_dir / "derivatives").mkdir()
    if with_logs:
        logs_dir = capsule_dir / "logs"
        logs_dir.mkdir()
        (logs_dir / "run.log").write_text("job output\n")
    return capsule_dir


def write_job_capsule_logs(
    *,
    dandiset_directory: pathlib.Path,
    dandiset_id: str,
    subject: str,
    job_id: str,
    nextflow_lines: list[str],
    slurm_lines_by_file: dict[str, list[str]],
) -> pathlib.Path:
    """Materialize a job capsule ``logs/`` directory holding a nextflow log and slurm logs."""
    logs_dir = (
        dandiset_directory
        / "derivatives"
        / pathlib.PurePosixPath(_dandiset_derivatives_relative_dir(dandiset_id))
        / f"sub-{subject}"
        / "pipeline-test"
        / job_id
        / "logs"
    )
    logs_dir.mkdir(parents=True)
    (logs_dir / "nextflow.log").write_text("\n".join(nextflow_lines) + "\n")
    for file_name, lines in slurm_lines_by_file.items():
        (logs_dir / file_name).write_text("\n".join(lines) + "\n")
    return logs_dir
