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
def capsule(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> dict[str, pathlib.Path]:
    """
    A rendered capsule script, its directories, and stubs standing in for the cluster tools.

    The pipeline version is v1.1.0 unless a test parametrizes this fixture indirectly with another.
    """
    pipeline_version = getattr(request, "param", "v1.1.0")
    capsule_directory = tmp_path / "001697" / "capsule"
    log_directory = capsule_directory / "logs"
    results_directory = capsule_directory / "intermediate"
    log_directory.mkdir(parents=True)
    results_directory.mkdir()
    (results_directory / "partial_output.txt").write_text("partial")

    # Blobs in the archive mirror are named by content ID alone, with no `.nwb` suffix.
    nwb_file_path = tmp_path / "blobs" / "db5" / "42d" / "db542d25-ecb9-4a27-a784-548f38deffa4"
    nwb_file_path.parent.mkdir(parents=True)
    nwb_file_path.write_text("nwb")
    work_directory = tmp_path / "work" / "job-test"

    script_file_path = capsule_directory / "submit.sh"
    generate_aind_ephys_submission_script(
        script_file_path=script_file_path,
        log_directory=str(log_directory),
        nwb_file_path=str(nwb_file_path),
        results_directory=str(results_directory),
        work_directory=str(work_directory),
        apptainer_cache_directory=str(tmp_path / "work" / "apptainer_cache"),
        environment_directory=str(tmp_path / "environment"),
        config_file_path=str(tmp_path / "pipeline.config"),
        pipeline_file_path=str(tmp_path / "main_multi_backend.nf"),
        pipeline_repo_directory=str(tmp_path / "pipeline_repo"),
        pipeline_version=pipeline_version,
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
        "nwb_file": nwb_file_path,
        "work": work_directory,
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
@pytest.mark.parametrize(
    "stop_signal",
    [
        pytest.param(signal.SIGUSR1, id="time-limit-warning"),
        pytest.param(signal.SIGTERM, id="preempted-or-cancelled"),
    ],
)
def test_submission_script_stops_nextflow_and_uploads_logs_when_stopped(
    capsule: dict[str, pathlib.Path],
    stop_signal: signal.Signals,
) -> None:
    """
    A run cut off early reads as failed, with logs, rather than stalled without any.

    That holds whether SLURM warned it of its time limit or stopped it outright. Partial results
    are removed first so that they are not uploaded as the capsule's output.
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
        process.send_signal(stop_signal)
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


# Stands in for a run that finishes, leaving the outputs a 1.5.0 release publishes.
_COMPLETING_NEXTFLOW_STUB = """#!/bin/bash
echo "$NXF_SYNTAX_PARSER" > "$STUB_RECORD_DIRECTORY/nextflow_syntax_parser"
mkdir -p "$RESULTS_PATH/nwb" "$RESULTS_PATH/nextflow" "$RESULTS_PATH/visualization" "$RESULTS_PATH/postprocessed"
echo trace > "$RESULTS_PATH/nextflow/trace.txt"
echo provenance > "$RESULTS_PATH/provenance.json"
"""


def _run_to_completion(
    *, capsule: dict[str, pathlib.Path], nextflow_stub: str = _COMPLETING_NEXTFLOW_STUB
) -> subprocess.CompletedProcess:
    (capsule["stubs"] / "nextflow").write_text(nextflow_stub)
    environment = {
        **os.environ,
        "PATH": f"{capsule['stubs']}{os.pathsep}{os.environ['PATH']}",
        "STUB_RECORD_DIRECTORY": str(capsule["records"]),
    }
    completed = subprocess.run(
        ["bash", str(capsule["script"])], env=environment, capture_output=True, text=True, timeout=60
    )
    return completed


@pytest.mark.ai_generated
@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_submission_script_uploads_the_run_provenance_with_the_logs(capsule: dict[str, pathlib.Path]) -> None:
    """The `provenance.json` releases from 1.5.0 write beside their results is kept, not deleted with them."""
    completed = _run_to_completion(capsule=capsule)

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert (capsule["capsule"] / "logs" / "provenance.json").read_text() == "provenance\n"
    assert (capsule["capsule"] / "logs" / "trace.txt").read_text() == "trace\n"
    assert not capsule["results"].exists()


@pytest.mark.ai_generated
@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_submission_script_runs_nextflow_with_the_syntax_parser_the_pipeline_is_written_for(
    capsule: dict[str, pathlib.Path],
) -> None:
    """Nextflow 26.04 and later parse with a stricter syntax by default, which the pipeline does not follow."""
    completed = _run_to_completion(capsule=capsule)

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert (capsule["records"] / "nextflow_syntax_parser").read_text() == "v1\n"


# Records each argument on its own line, so a quoted argument holding spaces reads back whole.
_ARGUMENT_RECORDING_NEXTFLOW_STUB = """#!/bin/bash
printf '%s\\n' "$@" > "$STUB_RECORD_DIRECTORY/nextflow_arguments"
echo "$DATA_PATH" > "$STUB_RECORD_DIRECTORY/nextflow_data_path"
"""


@pytest.mark.ai_generated
@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("capsule", ["v1.4.1", "1.4.1"], indirect=True)
def test_submission_script_passes_the_blob_as_nwb_files_before_1_5_0(capsule: dict[str, pathlib.Path]) -> None:
    """Up to release 1.4.1 the job dispatch step takes the blob through `--nwb-files`, from its own directory."""
    completed = _run_to_completion(capsule=capsule, nextflow_stub=_ARGUMENT_RECORDING_NEXTFLOW_STUB)

    assert completed.returncode == 0, completed.stdout + completed.stderr
    arguments = (capsule["records"] / "nextflow_arguments").read_text().splitlines()
    assert arguments[-2:] == ["--job_dispatch_args", f"--nwb-files {capsule['nwb_file']}"]
    data_path = (capsule["records"] / "nextflow_data_path").read_text().strip()
    assert data_path == str(capsule["nwb_file"].parent)
    assert not (capsule["work"] / "input").exists()


@pytest.mark.ai_generated
@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("capsule", ["v1.5.0", "1.5.0", "1.6.0"], indirect=True)
def test_submission_script_links_the_blob_into_an_input_folder_from_1_5_0(capsule: dict[str, pathlib.Path]) -> None:
    """
    From release 1.5.0 the parameters schema rejects `--nwb-files`, which fails the run before any step starts.

    The pipeline is given an input folder instead, holding the blob as its one `.nwb` file.
    """
    completed = _run_to_completion(capsule=capsule, nextflow_stub=_ARGUMENT_RECORDING_NEXTFLOW_STUB)

    assert completed.returncode == 0, completed.stdout + completed.stderr
    arguments = (capsule["records"] / "nextflow_arguments").read_text().splitlines()
    assert not any("job_dispatch_args" in argument or "nwb-files" in argument for argument in arguments)
    data_path = pathlib.Path((capsule["records"] / "nextflow_data_path").read_text().strip())
    assert data_path == capsule["work"] / "input"
    assert sorted(path.name for path in data_path.iterdir()) == [f"{capsule['nwb_file'].name}.nwb"]
    link = data_path / f"{capsule['nwb_file'].name}.nwb"
    assert link.is_symlink()
    assert pathlib.Path(os.readlink(link)) == capsule["nwb_file"]


@pytest.mark.ai_generated
@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("capsule", ["v1.5.0"], indirect=True)
def test_submission_script_leaves_the_input_folder_unchanged_when_run_again(capsule: dict[str, pathlib.Path]) -> None:
    """A requeued run finds the input folder as the first run left it, so Nextflow can resume its cached steps."""
    first_run = _run_to_completion(capsule=capsule, nextflow_stub=_ARGUMENT_RECORDING_NEXTFLOW_STUB)
    input_folder = capsule["work"] / "input"
    link = input_folder / f"{capsule['nwb_file'].name}.nwb"
    folder_status_after_first_run = input_folder.stat()
    link_status_after_first_run = link.lstat()

    capsule["results"].mkdir()
    second_run = _run_to_completion(capsule=capsule, nextflow_stub=_ARGUMENT_RECORDING_NEXTFLOW_STUB)

    assert first_run.returncode == 0, first_run.stdout + first_run.stderr
    assert second_run.returncode == 0, second_run.stdout + second_run.stderr
    assert input_folder.stat().st_mtime_ns == folder_status_after_first_run.st_mtime_ns
    assert link.lstat().st_ino == link_status_after_first_run.st_ino
