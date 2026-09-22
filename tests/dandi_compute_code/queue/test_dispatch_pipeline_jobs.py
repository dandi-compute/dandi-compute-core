import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import CapsuleResources, DispatchConfig, DispatchResult, dispatch_pipeline_jobs

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
    capsule_resources: dict | None = None,
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
            capsule_resources=capsule_resources,
            test=test,
        )


def _script(result, index: int = 1) -> str:
    """The generated dispatch script for one resource group."""
    return (result.dispatch_directory / f"dispatch-{index}.sh").read_text()


def _manifest(result, index: int = 1) -> list[str]:
    """The manifest lines for one resource group."""
    return (result.dispatch_directory / f"manifest-{index}.txt").read_text().splitlines()


@pytest.mark.ai_generated
def test_dispatch_places_every_pending_capsule_of_the_pipeline_in_one_array(
    processing_directory: pathlib.Path,
) -> None:
    """All of a pipeline's pending capsules go out as a single array job."""
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    assert result.status == "dispatched"
    assert result.task_count == 3
    assert tuple(a.array_job_id for a in result.arrays) == ("4242",)


@pytest.mark.ai_generated
def test_dispatch_writes_one_manifest_line_per_capsule(processing_directory: pathlib.Path) -> None:
    """The manifest maps array task index to capsule, one capsule per line."""
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    manifest_lines = _manifest(result)
    assert manifest_lines == _AIND_CODE_DIR_PATHS


@pytest.mark.ai_generated
def test_dispatch_ignores_capsules_belonging_to_another_pipeline(processing_directory: pathlib.Path) -> None:
    """A pipeline's array covers only its own capsules."""
    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=[*_AIND_CODE_DIR_PATHS, _LFP_CODE_DIR_PATH],
    )

    manifest_lines = _manifest(result)
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

    script = _script(result)
    assert "#SBATCH --job-name=dandicompute-dispatch-aind-ephys" in script
    assert "#SBATCH --array=1-3%2" in script


@pytest.mark.ai_generated
def test_dispatch_script_requests_what_the_capsule_itself_asks_for(
    processing_directory: pathlib.Path,
) -> None:
    """
    The array task's allocation is the one its capsule gets, so it matches the capsule's own.

    The requests come from the pipeline's submission template rather than the dispatch
    settings, which keeps the template the only place they are written.
    """
    dispatch_config = DispatchConfig(
        pipeline="aind+ephys",
        max_concurrent=2,
        partition="mit_preemptable",
        memory="16GB",
        cpus_per_task=4,
        time_limit="48:00:00",
    )

    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        dispatch_config=dispatch_config,
    )

    script = _script(result)
    assert "#SBATCH --mem=16GB" in script
    assert "#SBATCH --cpus-per-task=4" in script
    assert "#SBATCH --partition=mit_preemptable" in script
    assert "#SBATCH --time=48:00:00" in script


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
    manifest_lines = _manifest(result)
    assert manifest_lines == _AIND_CODE_DIR_PATHS


@pytest.mark.ai_generated
def test_dispatch_script_runs_each_capsule_inside_its_array_task(
    processing_directory: pathlib.Path,
) -> None:
    """
    The capsule runs in the task, so the task finishing is the work finishing.

    That is what makes the array's throttle a limit on concurrent pipeline runs without any
    waiting process to keep alive in between.
    """
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    script = _script(result)
    assert 'bash "${CAPSULE_CODE_DIRECTORY}/submit.sh"' in script
    assert "sbatch" not in script


@pytest.mark.ai_generated
def test_dispatch_script_reproduces_the_capsules_own_slurm_log_path(
    processing_directory: pathlib.Path,
) -> None:
    """
    The capsule's `#SBATCH --output` has to be reproduced by hand.

    Running the capsule inside an array task makes its `#SBATCH` header inert, but that path
    points into the capsule's own logs directory, which is where the capsule uploads its SLURM
    log from and where `issues dump` reads it back.
    """
    result = _dispatch(processing_directory=processing_directory, code_dir_paths=_AIND_CODE_DIR_PATHS)

    script = _script(result)
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
    assert sbatch_command == ["sbatch", str((result.dispatch_directory / "dispatch-1.sh").absolute())]


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
    assert result.active_job_ids == ("1234_[3-12%2]",)
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
    manifest_lines = _manifest(result)
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

    script = _script(result)
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


# Capsules of one pipeline normally agree on resources, since one template renders them all.
# They can diverge when a template changed between the releases that prepared them, and an
# array carries a single `#SBATCH` header, so a divergent capsule needs an array of its own.

_LIGHT = CapsuleResources(memory="1GB", cpus_per_task=1, partition="mit_normal", time_limit="12:00:00")
_HEAVY = CapsuleResources(memory="16GB", cpus_per_task=1, partition="mit_preemptable", time_limit="48:00:00")


@pytest.mark.ai_generated
def test_dispatch_splits_capsules_that_ask_for_different_resources(
    processing_directory: pathlib.Path,
) -> None:
    """Capsules asking for different things get an array each, so neither is truncated."""
    capsule_resources = {
        _AIND_CODE_DIR_PATHS[0]: _LIGHT,
        _AIND_CODE_DIR_PATHS[1]: _HEAVY,
        _AIND_CODE_DIR_PATHS[2]: _LIGHT,
    }

    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        capsule_resources=capsule_resources,
    )

    assert result.task_count == 3
    assert len(result.arrays) == 2
    assert _manifest(result, 1) == [_AIND_CODE_DIR_PATHS[0], _AIND_CODE_DIR_PATHS[2]]
    assert _manifest(result, 2) == [_AIND_CODE_DIR_PATHS[1]]
    assert "#SBATCH --mem=1GB" in _script(result, 1)
    assert "#SBATCH --mem=16GB" in _script(result, 2)
    assert "#SBATCH --partition=mit_preemptable" in _script(result, 2)


@pytest.mark.ai_generated
def test_dispatch_shares_the_concurrency_limit_across_resource_groups(
    processing_directory: pathlib.Path,
) -> None:
    """
    The limit is what the pipeline may run at once in total, not per array.

    Applying it to each array would let a pipeline that split into groups quietly run several
    times its configured limit.
    """
    capsule_resources = {_AIND_CODE_DIR_PATHS[0]: _LIGHT, _AIND_CODE_DIR_PATHS[1]: _HEAVY}

    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS[:2],
        dispatch_config=DispatchConfig(pipeline="aind+ephys", max_concurrent=4),
        capsule_resources=capsule_resources,
    )

    assert "#SBATCH --array=1-1%2" in _script(result, 1)
    assert "#SBATCH --array=1-1%2" in _script(result, 2)


@pytest.mark.ai_generated
def test_dispatch_keeps_one_array_when_every_capsule_agrees(processing_directory: pathlib.Path) -> None:
    """The common case stays a single array per pipeline rather than fragmenting."""
    capsule_resources = dict.fromkeys(_AIND_CODE_DIR_PATHS, _LIGHT)

    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        capsule_resources=capsule_resources,
    )

    assert len(result.arrays) == 1
    assert result.task_count == 3


@pytest.mark.ai_generated
def test_dispatch_groups_an_unreadable_capsule_with_the_pipeline_template(
    processing_directory: pathlib.Path,
) -> None:
    """A capsule whose own script could not be read falls back rather than being dropped."""
    dispatch_config = DispatchConfig(
        pipeline="aind+ephys",
        memory="1GB",
        cpus_per_task=1,
        partition="mit_normal",
        time_limit="12:00:00",
    )
    capsule_resources = {_AIND_CODE_DIR_PATHS[0]: _LIGHT}

    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS,
        dispatch_config=dispatch_config,
        capsule_resources=capsule_resources,
    )

    assert len(result.arrays) == 1
    assert _manifest(result) == _AIND_CODE_DIR_PATHS


@pytest.mark.ai_generated
def test_dispatch_summary_lines_name_each_arrays_group(processing_directory: pathlib.Path) -> None:
    """
    The summary shows one line per array with what that group asked for.

    The grouping decides how the pipeline's limit is shared out, so it is worth reading off
    the dispatch output rather than out of the generated scripts.
    """
    capsule_resources = {_AIND_CODE_DIR_PATHS[0]: _LIGHT, _AIND_CODE_DIR_PATHS[1]: _HEAVY}

    result = _dispatch(
        processing_directory=processing_directory,
        code_dir_paths=_AIND_CODE_DIR_PATHS[:2],
        dispatch_config=DispatchConfig(pipeline="aind+ephys", max_concurrent=4),
        capsule_resources=capsule_resources,
    )

    lines = result.summary_lines()
    assert lines[0] == "aind+ephys: dispatched 2 capsules as 2 array jobs, one per distinct set of requested resources."
    assert lines[1] == "  array 4242: 1 capsule requesting 1GB / 1 CPU / mit_normal / 12:00:00, at most 2 at a time"
    assert lines[2] == (
        "  array 4242: 1 capsule requesting 16GB / 1 CPU / mit_preemptable / 48:00:00, at most 2 at a time"
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["no-pending", "dispatcher-active"])
def test_dispatch_summary_lines_are_a_single_line_when_nothing_was_dispatched(status: str) -> None:
    """With no arrays submitted there are no group lines to show."""
    result = DispatchResult(pipeline="lfp", status=status)

    assert len(result.summary_lines()) == 1
