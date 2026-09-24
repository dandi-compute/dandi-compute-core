import os
import pathlib
import shutil
import signal
import subprocess
import time

import pytest

from dandi_compute_code.aind_ephys_pipeline import generate_aind_ephys_submission_script

# The submission script is run for real under bash, with every cluster tool it calls replaced
# by a stub on PATH. The Nextflow stub records its arguments and then runs until it is
# terminated, standing in for a run whose steps are still queued when the time limit nears.

_NEXTFLOW_STUB = """#!/bin/bash
echo "$@" > "$STUB_RECORD_DIRECTORY/nextflow_arguments"
trap 'echo terminated > "$STUB_RECORD_DIRECTORY/nextflow_terminated"; exit 143' TERM
while true; do sleep 0.1; done
"""
_DANDI_STUB = """#!/bin/bash
echo "$PWD $*" >> "$STUB_RECORD_DIRECTORY/dandi_calls"
"""
_NO_OP_STUB = "#!/bin/bash\n"


@pytest.fixture
def capsule(tmp_path: pathlib.Path) -> dict[str, pathlib.Path]:
    """A rendered capsule script, its directories, and stubs standing in for the cluster tools."""
    capsule_directory = tmp_path / "001697" / "capsule"
    log_directory = capsule_directory / "logs"
    results_directory = capsule_directory / "intermediate"
    log_directory.mkdir(parents=True)
    results_directory.mkdir()
    (results_directory / "partial_output.txt").write_text("partial")

    script_file_path = capsule_directory / "submit.sh"
    generate_aind_ephys_submission_script(
        script_file_path=script_file_path,
        log_directory=str(log_directory),
        nwb_file_path=str(tmp_path / "input.nwb"),
        results_directory=str(results_directory),
        work_directory=str(tmp_path / "work"),
        apptainer_cache_directory=str(tmp_path / "work" / "apptainer_cache"),
        environment_directory=str(tmp_path / "environment"),
        config_file_path=str(tmp_path / "pipeline.config"),
        pipeline_file_path=str(tmp_path / "main_multi_backend.nf"),
        pipeline_repo_directory=str(tmp_path / "pipeline_repo"),
        pipeline_version="v1.1.0",
        temp_name="prepare-job-test",
        done_tracker_file_path=str(tmp_path / "done.txt"),
        params_file_path=str(tmp_path / "params.json"),
    )

    stub_directory = tmp_path / "stubs"
    stub_directory.mkdir()
    stubs = {"nextflow": _NEXTFLOW_STUB, "dandi": _DANDI_STUB, "pip": _NO_OP_STUB, "git": _NO_OP_STUB}
    stubs.update({"conda": _NO_OP_STUB, "module": _NO_OP_STUB, "tree": _NO_OP_STUB})
    for name, content in stubs.items():
        stub_file_path = stub_directory / name
        stub_file_path.write_text(content)
        stub_file_path.chmod(0o755)

    record_directory = tmp_path / "records"
    record_directory.mkdir()

    paths = {
        "script": script_file_path,
        "capsule": capsule_directory,
        "results": results_directory,
        "stubs": stub_directory,
        "records": record_directory,
        "done_tracker": tmp_path / "done.txt",
    }
    return paths


def _wait_for(file_path: pathlib.Path, /, *, timeout_seconds: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not file_path.exists():
        if time.monotonic() > deadline:
            message = f"{file_path} never appeared"
            raise TimeoutError(message)
        time.sleep(0.05)


@pytest.mark.ai_generated
def test_submission_script_asks_to_be_warned_ahead_of_its_time_limit(capsule: dict[str, pathlib.Path]) -> None:
    """The warning is what gives the script time to upload its logs before SLURM kills it."""
    script = capsule["script"].read_text()

    assert "#SBATCH --signal=B:USR1@600" in script


@pytest.mark.ai_generated
@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_submission_script_stops_nextflow_and_uploads_logs_when_warned(capsule: dict[str, pathlib.Path]) -> None:
    """
    A run cut off by its time limit reads as failed, with logs, rather than stalled without any.

    Partial results are removed first so that they are not uploaded as the capsule's output.
    """
    environment = {
        **os.environ,
        "PATH": f"{capsule['stubs']}{os.pathsep}{os.environ['PATH']}",
        "STUB_RECORD_DIRECTORY": str(capsule["records"]),
    }
    process = subprocess.Popen(
        ["bash", str(capsule["script"])],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for(capsule["records"] / "nextflow_arguments")
        process.send_signal(signal.SIGUSR1)
        output, _ = process.communicate(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()

    assert process.returncode == 1, output
    assert (capsule["records"] / "nextflow_terminated").exists()
    assert "-resume" in (capsule["records"] / "nextflow_arguments").read_text().split()
    assert not capsule["results"].exists()
    dandi_calls = (capsule["records"] / "dandi_calls").read_text().splitlines()
    assert dandi_calls == [f"{capsule['capsule']} upload --validation skip"]
    assert not capsule["done_tracker"].exists()
