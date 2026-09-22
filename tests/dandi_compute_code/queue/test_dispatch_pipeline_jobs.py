import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import DispatchConfig, dispatch_pipeline_jobs

# dispatch_pipeline_jobs reaches the cluster through two subprocess calls that cannot run in
# CI: `squeue` (is a dispatcher still working through its array?) and `sbatch` (submit the
# array). Both are mocked here; the manifest and the generated script are the observable
# results and are asserted directly.

_AIND_CODE_DIR_PATHS = [
    "derivatives/dandiset-000409/sub-mouse01/pipeline-aind+ephys/job-240101abc123/code",
    "derivatives/dandiset-000409/sub-mouse02/pipeline-aind+ephys/job-240101abc124/code",
    "derivatives/dandiset-000409/sub-mouse03/pipeline-aind+ephys/job-240101abc125/code",
]
_LFP_CODE_DIR_PATH = "derivatives/dandiset-000409/sub-mouse01/pipeline-lfp/job-240101abc126/code"


def _mock_subprocess_run(*, squeue_stdout: str = "", sbatch_stdout: str = "Submitted batch job 4242\n"):
    """A subprocess.run replacement answering the squeue and sbatch calls dispatch makes."""

    def run(command: list[str], **_: object) -> mock.MagicMock:
        result = mock.MagicMock()
        result.returncode = 0
        result.stderr = ""
        result.stdout = squeue_stdout if command[0] == "squeue" else sbatch_stdout
        return result

    return run


def _dispatch(
    *,
    processing_directory: pathlib.Path,
    code_dir_paths: list[str],
    dispatch_config: DispatchConfig | None = None,
    squeue_stdout: str = "",
    test: bool = False,
):
    """Dispatch the aind+ephys pipeline with the cluster calls mocked, returning the result."""
    with mock.patch(
        "dandi_compute_code.queue._dispatch.subprocess.run",
        side_effect=_mock_subprocess_run(squeue_stdout=squeue_stdout),
    ):
        return dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=code_dir_paths,
            processing_directory=processing_directory,
            dispatch_config=dispatch_config or DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
            test=test,
        )


@pytest.mark.ai_generated
def test_dispatch_places_every_pending_capsule_of_the_pipeline_in_one_array(
    processing_directory: pathlib.Path,
) -> None:
    """All of a pipeline's pending capsules go out as a single array job."""
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    assert result.status == "dispatched"
    assert result.task_count == 3
    assert result.array_job_id == "4242"


@pytest.mark.ai_generated
def test_dispatch_writes_one_manifest_line_per_capsule(processing_directory: pathlib.Path) -> None:
    """The manifest maps array task index to capsule, one capsule per line."""
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    manifest_lines = (result.dispatch_directory / "manifest.txt").read_text().splitlines()
    assert manifest_lines == _AIND_CODE_DIR_PATHS


@pytest.mark.ai_generated
def test_dispatch_ignores_capsules_belonging_to_another_pipeline(processing_directory: pathlib.Path) -> None:
    """A pipeline's array covers only its own capsules."""
    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=[*_AIND_CODE_DIR_PATHS, _LFP_CODE_DIR_PATH],
    )

    manifest_lines = (result.dispatch_directory / "manifest.txt").read_text().splitlines()
    assert manifest_lines == _AIND_CODE_DIR_PATHS


@pytest.mark.ai_generated
def test_dispatch_script_carries_the_job_name_and_configured_throttle(
    processing_directory: pathlib.Path,
) -> None:
    """The name and the throttle are what the dispatch settings put into the array script."""
    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        dispatch_config=DispatchConfig(pipeline="aind+ephys", max_concurrent=2),
    )

    script = (result.dispatch_directory / "dispatch.sh").read_text()
    assert "#SBATCH --job-name=dandicompute-dispatch-aind-ephys" in script
    assert "#SBATCH --array=1-3%2" in script


@pytest.mark.ai_generated
def test_dispatch_script_pins_its_resource_requests(processing_directory: pathlib.Path) -> None:
    """
    An array task only runs the capsule's own submission script, so its requests are pinned.

    They are deliberately not configurable per pipeline, which is why these are asserted as
    fixed values rather than read back from the dispatch settings.
    """
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    script = (result.dispatch_directory / "dispatch.sh").read_text()
    assert "#SBATCH --mem=100MB" in script
    assert "#SBATCH --cpus-per-task=1" in script
    assert "#SBATCH --partition=mit_normal" in script
    assert "#SBATCH --time=12:00:00" in script


@pytest.mark.ai_generated
def test_dispatch_places_every_capsule_in_one_array_when_uncapped(
    processing_directory: pathlib.Path,
) -> None:
    """A null max_array_tasks holds nothing back for the next dispatch."""
    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        dispatch_config=DispatchConfig(pipeline="aind+ephys", max_array_tasks=None),
    )

    assert result.task_count == len(_AIND_CODE_DIR_PATHS)
    manifest_lines = (result.dispatch_directory / "manifest.txt").read_text().splitlines()
    assert manifest_lines == _AIND_CODE_DIR_PATHS


@pytest.mark.ai_generated
def test_dispatch_script_reproduces_the_capsules_own_slurm_log_path(
    processing_directory: pathlib.Path,
) -> None:
    """
    The capsule's `#SBATCH --output` is the one directive still honoured.

    Running the capsule inside an array task makes its `#SBATCH` header inert, but that path
    points into the capsule's own logs directory, which is where the capsule uploads its SLURM
    log from and where `issues dump` reads it back.
    """
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    script = (result.dispatch_directory / "dispatch.sh").read_text()
    assert "sed -n 's/^#SBATCH[[:space:]]\\+--output=//p'" in script
    assert 'CAPSULE_LOG_FILE_PATH="${CAPSULE_LOG_FILE_PATH//%j/${SLURM_JOB_ID}}"' in script
    assert 'bash "${CAPSULE_CODE_DIRECTORY}/submit.sh" 2>&1 | tee "$CAPSULE_LOG_FILE_PATH"' in script
    # pipefail is what keeps a failing capsule a failing array task through that pipe.
    assert "set -euo pipefail" in script


@pytest.mark.ai_generated
def test_dispatch_submits_the_generated_script_with_sbatch(processing_directory: pathlib.Path) -> None:
    """The array script is what gets handed to sbatch."""
    with mock.patch(
        "dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_mock_subprocess_run()
    ) as mock_run:
        result = dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=_AIND_CODE_DIR_PATHS,
            processing_directory=processing_directory,
            dispatch_config=DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
        )

    sbatch_command = mock_run.call_args_list[-1].args[0]
    assert sbatch_command == ["sbatch", str((result.dispatch_directory / "dispatch.sh").absolute())]


@pytest.mark.ai_generated
def test_dispatch_asks_squeue_only_about_this_pipelines_dispatcher(processing_directory: pathlib.Path) -> None:
    """The liveness check is scoped to this user and this pipeline's dispatcher job name."""
    with mock.patch(
        "dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_mock_subprocess_run()
    ) as mock_run:
        dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=_AIND_CODE_DIR_PATHS,
            processing_directory=processing_directory,
            dispatch_config=DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
        )

    squeue_command = mock_run.call_args_list[0].args[0]
    assert squeue_command[0] == "squeue"
    assert "--me" in squeue_command
    assert squeue_command[squeue_command.index("--name") + 1] == "dandicompute-dispatch-aind-ephys"


@pytest.mark.ai_generated
def test_dispatch_does_not_resubmit_while_the_dispatcher_is_still_active(
    processing_directory: pathlib.Path,
) -> None:
    """A live dispatcher already owns the pending capsules, so no second array is submitted."""
    with mock.patch(
        "dandi_compute_code.queue._dispatch.subprocess.run",
        side_effect=_mock_subprocess_run(squeue_stdout="1234_[3-12%2]\n"),
    ) as mock_run:
        result = dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=_AIND_CODE_DIR_PATHS,
            processing_directory=processing_directory,
            dispatch_config=DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
        )

    assert result.status == "dispatcher-active"
    assert result.array_job_id == "1234_[3-12%2]"
    assert mock_run.call_count == 1
    assert list(processing_directory.iterdir()) == []


@pytest.mark.ai_generated
def test_dispatch_reports_no_pending_without_touching_the_cluster(processing_directory: pathlib.Path) -> None:
    """With nothing pending for the pipeline, neither squeue nor sbatch is called."""
    with mock.patch("dandi_compute_code.queue._dispatch.subprocess.run") as mock_run:
        result = dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=[_LFP_CODE_DIR_PATH],
            processing_directory=processing_directory,
            dispatch_config=DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
        )

    assert result.status == "no-pending"
    assert result.task_count == 0
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_dispatch_holds_capsules_beyond_the_array_size_limit_back(processing_directory: pathlib.Path) -> None:
    """Capsules past max_array_tasks stay pending for the next dispatch rather than overflowing."""
    dispatch_config = DispatchConfig(pipeline="aind+ephys", max_array_tasks=2)

    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        dispatch_config=dispatch_config,
    )

    assert result.task_count == 2
    manifest_lines = (result.dispatch_directory / "manifest.txt").read_text().splitlines()
    assert manifest_lines == _AIND_CODE_DIR_PATHS[:2]


@pytest.mark.ai_generated
@pytest.mark.parametrize(("test_mode", "expected_removal"), [(False, True), (True, False)])
def test_dispatch_script_removes_task_directories_unless_running_in_test_mode(
    processing_directory: pathlib.Path, test_mode: bool, expected_removal: bool
) -> None:
    """Test mode leaves each array task's working tree on disk for debugging."""
    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        test=test_mode,
    )

    script = (result.dispatch_directory / "dispatch.sh").read_text()
    assert ('rm -rf "$TASK_DIRECTORY"' in script) is expected_removal


@pytest.mark.ai_generated
def test_dispatch_raises_when_sbatch_fails(processing_directory: pathlib.Path) -> None:
    """A failed array submission is surfaced rather than reported as dispatched."""
    failed_result = mock.MagicMock(returncode=1, stdout="", stderr="boom")

    def run(command: list[str], **_: object) -> mock.MagicMock:
        if command[0] == "squeue":
            return mock.MagicMock(returncode=0, stdout="", stderr="")
        return failed_result

    with (
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=run),
        pytest.raises(RuntimeError, match="sbatch submission of the array dispatcher failed"),
    ):
        dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=_AIND_CODE_DIR_PATHS,
            processing_directory=processing_directory,
            dispatch_config=DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
        )


@pytest.mark.ai_generated
def test_dispatch_raises_when_sbatch_reports_no_job_id(processing_directory: pathlib.Path) -> None:
    """Output without a job ID means the array cannot be tracked, so it is an error."""
    with (
        mock.patch(
            "dandi_compute_code.queue._dispatch.subprocess.run",
            side_effect=_mock_subprocess_run(sbatch_stdout="nothing useful\n"),
        ),
        pytest.raises(RuntimeError, match="Unable to read an array job ID"),
    ):
        dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=_AIND_CODE_DIR_PATHS,
            processing_directory=processing_directory,
            dispatch_config=DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
        )


@pytest.mark.ai_generated
def test_dispatch_raises_when_squeue_fails(processing_directory: pathlib.Path) -> None:
    """Without a usable answer from squeue there is no way to tell a live dispatcher apart."""
    failed_result = mock.MagicMock(returncode=1, stdout="", stderr="squeue: error")

    with (
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", return_value=failed_result),
        pytest.raises(RuntimeError, match="squeue: error"),
    ):
        dispatch_pipeline_jobs(
            pipeline="aind+ephys",
            code_dir_paths=_AIND_CODE_DIR_PATHS,
            processing_directory=processing_directory,
            dispatch_config=DispatchConfig(pipeline="aind+ephys"),
            dandiset_id="001697",
        )
