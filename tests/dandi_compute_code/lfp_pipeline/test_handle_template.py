import dataclasses
import os
import pathlib
import re
import shutil
import signal
import subprocess
import time

import pytest

from dandi_compute_code.lfp_pipeline import generate_lfp_submission_script

_CONTAINER_IMAGE = "ghcr.io/dandi-compute/dandi-compute-lfp:v0.0.0"
_TEMP_NAME = "prepare-job-xyz"
_MODULES_INIT_FILE_PATH = "/etc/profile.d/modules.sh"

_requires_bash = pytest.mark.skipif(
    shutil.which("bash") is None or os.name != "posix", reason="The submission script runs under bash on POSIX."
)

# Stand-ins for the cluster's tools. Each records how it was called, and each can be told to fail.
_STUB_MODULES_INIT = """
module() {
    echo "module $*" >> "$STUB_CALLS"
    return "${STUB_MODULE_STATUS:-0}"
}
"""
# The apptainer stub runs the command it is given, as `apptainer exec` would inside the image. With
# --cleanenv only the stubs' own settings, PATH and HOME get through, plus the APPTAINERENV_ variables
# under their unprefixed names.
_STUB_EXECUTABLES = {
    "apptainer": """#!/bin/bash
echo "apptainer $*" >> "$STUB_CALLS"
if [ "$1" = "pull" ]; then
    echo "image" > "$3"
    exit "${STUB_PULL_STATUS:-0}"
fi
shift
clean=""
while [ "${1#--}" != "$1" ]; do
    case "$1" in
        --cleanenv) clean=1 ;;
        --bind) shift ;;
    esac
    shift
done
image="$1"
shift
[ -f "$image" ] || exit 255
if [ -n "$clean" ]; then
    kept=(PATH="$PATH" HOME="$HOME")
    while IFS= read -r line; do
        kept+=("$line")
    done < <(env | grep '^STUB_')
    if [ -n "${APPTAINERENV_DANDI_API_KEY:-}" ]; then
        kept+=(DANDI_API_KEY="$APPTAINERENV_DANDI_API_KEY")
    fi
    exec env -i "${kept[@]}" "$@"
fi
exec "$@"
""",
    "duct": """#!/bin/bash
echo "duct $*" >> "$STUB_CALLS"
shift 2
exec "$@"
""",
    "python": """#!/bin/bash
echo "python $*" >> "$STUB_CALLS"
output=""
while [ $# -gt 0 ]; do
    if [ "$1" = "--output" ]; then
        output="$2"
    fi
    shift
done
echo "partial" > "$output"
if [ -n "${STUB_RUN_STARTED_FILE_PATH:-}" ]; then
    touch "$STUB_RUN_STARTED_FILE_PATH"
    sleep 60
fi
exit "${STUB_RUN_STATUS:-0}"
""",
    "dandi": """#!/bin/bash
echo "dandi $* from $PWD with token ${DANDI_API_KEY:-none}" >> "$STUB_CALLS"
exit "${STUB_UPLOAD_STATUS:-0}"
""",
}
_PID_PLACEHOLDER = "PID"
_UPLOAD_CALL_TEMPLATE = "dandi upload --allow-any-path --validation skip from {dataset_directory} with token {token}"


def _mask_pid(call: str) -> str:
    """Replace the process ID a partial image is named after, which differs on every run."""
    masked_call = re.sub(pattern=r"\.partial-\d+", repl=f".partial-{_PID_PLACEHOLDER}", string=call)
    return masked_call


@dataclasses.dataclass(frozen=True)
class _Capsule:
    script_file_path: pathlib.Path
    dataset_directory: pathlib.Path
    output_nwb_file_path: pathlib.Path
    image_file_path: pathlib.Path
    done_tracker_file_path: pathlib.Path
    calls_file_path: pathlib.Path
    environment: dict[str, str]

    def calls(self) -> list[str]:
        if not self.calls_file_path.exists():
            return []
        return self.calls_file_path.read_text().splitlines()

    def upload_call(self) -> str:
        upload_call = _UPLOAD_CALL_TEMPLATE.format(dataset_directory=self.dataset_directory, token="secret")
        return upload_call

    def run(self, **stub_settings: str) -> subprocess.CompletedProcess:
        result = subprocess.run(
            ["bash", str(self.script_file_path)],
            env={**self.environment, **stub_settings},
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result


def _render(tmp_path: pathlib.Path) -> str:
    script_file_path = tmp_path / "submit.sh"
    generate_lfp_submission_script(
        script_file_path=script_file_path,
        log_directory="/dataset/logs",
        dataset_directory="/dataset",
        container_image=_CONTAINER_IMAGE,
        image_file_path="/work/apptainer_cache/image.img",
        nwb_file_path="/data/blobs/abc/def/0123456789",
        output_nwb_file_path="/dataset/derivatives/lfp/sub-01_lfp",
        parameters_key="default",
        temp_name=_TEMP_NAME,
        done_tracker_file_path="/processing/done.txt",
    )
    return script_file_path.read_text()


@pytest.fixture
def capsule(tmp_path: pathlib.Path) -> _Capsule:
    """A rendered capsule in a temporary tree, run against stubs of the cluster's tools."""
    dataset_directory = tmp_path / "001697" / "derivatives" / "job-abc"
    log_directory = dataset_directory / "logs"
    log_directory.mkdir(parents=True)
    output_nwb_file_path = dataset_directory / "derivatives" / "nwb" / "sub-01_desc-lfp"
    image_file_path = tmp_path / "work" / "apptainer_cache" / "image.img"
    done_tracker_file_path = tmp_path / "done.txt"
    script_file_path = dataset_directory / "code" / "submit.sh"
    script_file_path.parent.mkdir()

    generate_lfp_submission_script(
        script_file_path=script_file_path,
        log_directory=str(log_directory),
        dataset_directory=str(dataset_directory),
        container_image=_CONTAINER_IMAGE,
        image_file_path=str(image_file_path),
        nwb_file_path=str(tmp_path / "blob"),
        output_nwb_file_path=str(output_nwb_file_path),
        parameters_key="default",
        temp_name=_TEMP_NAME,
        done_tracker_file_path=str(done_tracker_file_path),
    )

    # The module system only exists on the cluster, so its init script is swapped for the stub.
    modules_init_file_path = tmp_path / "modules.sh"
    modules_init_file_path.write_text(_STUB_MODULES_INIT)
    script = script_file_path.read_text()
    script_file_path.write_text(script.replace(_MODULES_INIT_FILE_PATH, str(modules_init_file_path)))

    stub_directory = tmp_path / "bin"
    stub_directory.mkdir()
    for name, content in _STUB_EXECUTABLES.items():
        stub_file_path = stub_directory / name
        stub_file_path.write_text(content)
        stub_file_path.chmod(0o755)

    calls_file_path = tmp_path / "calls.txt"
    environment = {
        "PATH": f"{stub_directory}{os.pathsep}{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "STUB_CALLS": str(calls_file_path),
        "DANDI_API_KEY": "secret",
    }
    return _Capsule(
        script_file_path=script_file_path,
        dataset_directory=dataset_directory,
        output_nwb_file_path=output_nwb_file_path,
        image_file_path=image_file_path,
        done_tracker_file_path=done_tracker_file_path,
        calls_file_path=calls_file_path,
        environment=environment,
    )


@pytest.mark.ai_generated
def test_rendered_script_has_sbatch_header(tmp_path) -> None:
    script = _render(tmp_path)

    assert script.startswith("#!/bin/bash")
    assert "#SBATCH --job-name=DANDI-Compute-LFP" in script
    assert "#SBATCH --output=/dataset/logs/job-%j_slurm.log" in script


@pytest.mark.ai_generated
def test_rendered_script_runs_every_step_from_the_image(tmp_path) -> None:
    script = _render(tmp_path)

    assert "datalad" not in script
    assert "conda" not in script
    assert "miniforge" not in script
    assert "module load apptainer" in script
    assert 'duct --output-prefix "/dataset/logs/duct_"' in script
    assert f'apptainer pull --disable-cache "$PARTIAL_IMAGE_FILE_PATH" "docker://{_CONTAINER_IMAGE}"' in script
    assert 'IMAGE_FILE_PATH="/work/apptainer_cache/image.img"' in script
    assert 'BIND_PATHS="/orcd/data/dandi/001:/orcd/data/dandi/001,/orcd/data/dandi/002:/orcd/data/dandi/002"' in script
    assert 'apptainer exec --cleanenv --bind "$BIND_PATHS" "$IMAGE_FILE_PATH" "$@"' in script


@pytest.mark.ai_generated
def test_rendered_script_invokes_pipeline_on_input_path(tmp_path) -> None:
    script = _render(tmp_path)

    assert "python -m dandi_compute_code.lfp_pipeline" in script
    assert '--input "/data/blobs/abc/def/0123456789"' in script
    assert '--output "/dataset/derivatives/lfp/sub-01_lfp"' in script
    assert '--params "default"' in script


@pytest.mark.ai_generated
def test_rendered_script_uploads_from_an_exit_trap_set_before_any_setup(tmp_path) -> None:
    script = _render(tmp_path)

    assert script.index("trap upload_capsule EXIT") < script.index(f"source {_MODULES_INIT_FILE_PATH}")
    assert 'cd "/dataset" && run_in_image dandi upload --allow-any-path --validation skip' in script
    assert f'echo "{_TEMP_NAME}" >> /processing/done.txt' in script


@_requires_bash
@pytest.mark.ai_generated
def test_successful_run_keeps_its_output_and_uploads_the_capsule(capsule: _Capsule, tmp_path: pathlib.Path) -> None:
    result = capsule.run()

    assert result.returncode == 0, result.stdout + result.stderr
    assert capsule.output_nwb_file_path.read_text() == "partial\n"
    assert capsule.image_file_path.read_text() == "image\n"
    assert capsule.done_tracker_file_path.read_text() == f"{_TEMP_NAME}\n"
    exec_prefix = (
        "apptainer exec --cleanenv --bind "
        "/orcd/data/dandi/001:/orcd/data/dandi/001,/orcd/data/dandi/002:/orcd/data/dandi/002 "
        f"{capsule.image_file_path}"
    )
    pipeline_call = (
        f"python -m dandi_compute_code.lfp_pipeline --input {tmp_path / 'blob'} "
        f"--output {capsule.output_nwb_file_path} --params default"
    )
    duct_call = f"duct --output-prefix {capsule.dataset_directory / 'logs'}/duct_ {pipeline_call}"
    upload_call = "dandi upload --allow-any-path --validation skip"
    expected_calls = [
        "module load apptainer",
        f"apptainer pull --disable-cache {capsule.image_file_path}.partial-{_PID_PLACEHOLDER} docker://{_CONTAINER_IMAGE}",
        f"{exec_prefix} {duct_call}",
        duct_call,
        pipeline_call,
        f"{exec_prefix} {upload_call}",
        capsule.upload_call(),
    ]
    calls = [_mask_pid(call) for call in capsule.calls()]
    assert calls == expected_calls


@_requires_bash
@pytest.mark.ai_generated
def test_cached_image_is_not_pulled_again(capsule: _Capsule) -> None:
    capsule.image_file_path.parent.mkdir(parents=True)
    capsule.image_file_path.write_text("cached\n")

    result = capsule.run()

    assert result.returncode == 0, result.stdout + result.stderr
    assert [call for call in capsule.calls() if call.startswith("apptainer pull")] == []
    assert capsule.image_file_path.read_text() == "cached\n"


@_requires_bash
@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("stub_settings", "expected_status"),
    [
        pytest.param({"STUB_MODULE_STATUS": "5"}, 5, id="setup"),
        pytest.param({"STUB_RUN_STATUS": "3"}, 3, id="run"),
    ],
)
def test_failure_with_the_image_cached_still_uploads_the_logs(
    capsule: _Capsule, stub_settings: dict[str, str], expected_status: int
) -> None:
    capsule.image_file_path.parent.mkdir(parents=True)
    capsule.image_file_path.write_text("cached\n")

    result = capsule.run(**stub_settings)

    assert result.returncode == expected_status, result.stdout + result.stderr
    assert capsule.calls()[-1] == capsule.upload_call()
    assert capsule.output_nwb_file_path.exists() is False
    assert capsule.done_tracker_file_path.exists() is False


@_requires_bash
@pytest.mark.ai_generated
def test_failed_image_pull_leaves_no_partial_image_and_skips_the_upload(capsule: _Capsule) -> None:
    result = capsule.run(STUB_PULL_STATUS="4")

    assert result.returncode == 1, result.stdout + result.stderr
    assert "so the capsule cannot be uploaded" in result.stdout
    assert [call for call in capsule.calls() if call.startswith("dandi")] == []
    assert capsule.image_file_path.exists() is False
    assert list(capsule.image_file_path.parent.glob("*.partial-*")) == []
    assert capsule.done_tracker_file_path.exists() is False


@_requires_bash
@pytest.mark.ai_generated
def test_failed_upload_after_a_successful_run_is_not_recorded_as_done(capsule: _Capsule) -> None:
    result = capsule.run(STUB_UPLOAD_STATUS="2")

    assert result.returncode == 2, result.stdout + result.stderr
    assert capsule.done_tracker_file_path.exists() is False


@_requires_bash
@pytest.mark.ai_generated
def test_run_stopped_by_slurm_uploads_its_logs(capsule: _Capsule, tmp_path: pathlib.Path) -> None:
    run_started_file_path = tmp_path / "run_started"
    process = subprocess.Popen(
        ["bash", str(capsule.script_file_path)],
        env={**capsule.environment, "STUB_RUN_STARTED_FILE_PATH": str(run_started_file_path)},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    deadline = time.monotonic() + 30
    while not run_started_file_path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)

    # SLURM signals every process of a job it stops, not only the batch shell.
    os.killpg(process.pid, signal.SIGTERM)
    output, _ = process.communicate(timeout=30)

    assert process.returncode == 143, output
    assert capsule.calls()[-1] == capsule.upload_call()
    assert capsule.output_nwb_file_path.exists() is False
    assert capsule.done_tracker_file_path.exists() is False
