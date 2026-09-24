import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.queue import TEST_QUEUE_CONTENT_ID, CapsuleResources, DispatchedArray, DispatchResult

# These tests exercise CLI argument wiring. Each command delegates directly to
# the ``PipelineQueue`` model, so the model methods are mocked here to isolate the
# delegation (option parsing and forwarded kwargs) under test.

_GROUP = "dandi_compute_code._cli._dandicompute_group"


@pytest.mark.ai_generated
def test_cli_prepare_test_creates_capsules_for_the_test_content_id(base_directory: pathlib.Path) -> None:
    """dandicompute prepare aind --test creates job capsules for the known test content ID."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["prepare", "aind", "--test", "--base", str(base_directory)])

    assert result.exit_code == 0
    mock_create.assert_called_once_with(
        content_ids=[TEST_QUEUE_CONTENT_ID],
        base_directory=base_directory,
        config_key="default",
    )


@pytest.mark.ai_generated
def test_cli_jobs_create_forwards_pipeline_as_only_pipeline(base_directory: pathlib.Path) -> None:
    """dandicompute jobs create --pipeline <name> forwards only_pipeline to PipelineQueue.create_job_capsules."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(
            _dandicompute_group, ["jobs", "create", "--base", str(base_directory), "--pipeline", "lfp", "--limit", "5"]
        )

    assert result.exit_code == 0
    mock_create.assert_called_once_with(
        config_key="default",
        limit=5,
        only_pipeline="lfp",
        force_latest_versions=False,
        base_directory=base_directory,
    )


@pytest.mark.ai_generated
def test_cli_jobs_create_forwards_latest_flag(base_directory: pathlib.Path) -> None:
    """dandicompute jobs create --latest forwards force_latest_versions to PipelineQueue.create_job_capsules."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "create", "--base", str(base_directory), "--latest"])

    assert result.exit_code == 0
    assert mock_create.call_args.kwargs["force_latest_versions"] is True


@pytest.mark.ai_generated
def test_cli_jobs_create_defaults_to_no_limit(base_directory: pathlib.Path) -> None:
    """dandicompute jobs create with no --limit creates capsules without a cap."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "create", "--base", str(base_directory)])

    assert result.exit_code == 0
    assert mock_create.call_args.kwargs["limit"] is None


@pytest.mark.ai_generated
def test_cli_jobs_create_requires_a_dandi_api_key(base_directory: pathlib.Path) -> None:
    """dandicompute jobs create fails clearly when DANDI_API_KEY is not set."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": ""}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "create", "--base", str(base_directory)])

    assert result.exit_code != 0
    assert mock_create.call_count == 0


@pytest.mark.ai_generated
def test_cli_prepare_test_passes_config_key(base_directory: pathlib.Path) -> None:
    """dandicompute prepare aind --test forwards --config to PipelineQueue.create_job_capsules."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["prepare", "aind", "--test", "--base", str(base_directory), "--config", "mit+engaging+revision-1"],
        )

    assert result.exit_code == 0
    mock_create.assert_called_once_with(
        content_ids=[TEST_QUEUE_CONTENT_ID],
        base_directory=base_directory,
        config_key="mit+engaging+revision-1",
    )


@pytest.mark.ai_generated
def test_cli_aind_prepare_passes_config_key(base_directory: pathlib.Path) -> None:
    """dandicompute prepare aind forwards --config to prepare_aind_ephys_job."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_prepare.return_value = pathlib.Path("/tmp/submit.sh")
        result = runner.invoke(
            _dandicompute_group,
            [
                "prepare",
                "aind",
                "--base",
                str(base_directory),
                "--id",
                "abc123",
                "--version",
                "v1.2.3",
                "--config",
                "mit+engaging+revision-1",
            ],
        )

    assert result.exit_code == 0
    assert mock_prepare.call_args.kwargs["config_key"] == "mit+engaging+revision-1"


@pytest.mark.ai_generated
def test_cli_clean_unsubmitted_calls_helper(base_directory: pathlib.Path) -> None:
    """dandicompute clean --unsubmitted delegates to PipelineQueue and reports removed paths."""
    fake_removed = ["derivatives/dandisets-000/dandiset-000001/sub-mouse01/pipeline-test/job-240101aa0001"]
    mock_state = mock.Mock()
    mock_state.clean_unsubmitted_capsules.return_value = fake_removed
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=mock_state) as mock_from_dandi:
        result = runner.invoke(
            _dandicompute_group,
            ["clean", "--unsubmitted", "--base", str(base_directory)],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    mock_from_dandi.assert_called_once_with()
    mock_state.clean_unsubmitted_capsules.assert_called_once_with()
    assert "Cleaned 1 unsubmitted capsule" in result.output


@pytest.mark.ai_generated
def test_cli_clean_unsubmitted_requires_dandi_api_key(base_directory: pathlib.Path) -> None:
    """dandicompute clean --unsubmitted fails before reading the queue when DANDI_API_KEY is unset."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi") as mock_from_dandi:
        result = runner.invoke(
            _dandicompute_group, ["clean", "--unsubmitted", "--base", str(base_directory)], env={"DANDI_API_KEY": ""}
        )

    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output
    mock_from_dandi.assert_not_called()


@pytest.mark.ai_generated
def test_cli_clean_unsubmitted_reports_nothing_found(base_directory: pathlib.Path) -> None:
    """dandicompute clean --unsubmitted reports when no unsubmitted capsules are found."""
    mock_state = mock.Mock()
    mock_state.clean_unsubmitted_capsules.return_value = []
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=mock_state):
        result = runner.invoke(
            _dandicompute_group,
            ["clean", "--unsubmitted", "--base", str(base_directory)],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    assert "No unsubmitted capsules found" in result.output


@pytest.mark.ai_generated
def test_cli_issues_dump_calls_helper(base_directory: pathlib.Path) -> None:
    """dandicompute issues dump delegates to PipelineQueue.dump_issues and reports output."""
    runner = CliRunner()
    with mock.patch(f"{_GROUP}.PipelineQueue.dump_issues", return_value=[]) as mock_dump:
        result = runner.invoke(
            _dandicompute_group,
            ["issues", "dump", "--base", str(base_directory)],
        )

    assert result.exit_code == 0, result.output
    mock_dump.assert_called_once_with(
        dandiset_id="001697",
        base_directory=base_directory,
        test=False,
    )
    assert "Wrote derivatives/issues_dump.json" in result.output


@pytest.mark.ai_generated
def test_cli_issues_summarize_calls_helper(base_directory: pathlib.Path) -> None:
    """dandicompute issues summarize delegates to PipelineQueue.summarize_issues and reports output."""
    runner = CliRunner()
    with mock.patch(f"{_GROUP}.PipelineQueue.summarize_issues", return_value={}) as mock_summarize:
        result = runner.invoke(
            _dandicompute_group,
            ["issues", "summarize", "--base", str(base_directory)],
        )

    assert result.exit_code == 0, result.output
    mock_summarize.assert_called_once_with(
        dandiset_id="001697",
        base_directory=base_directory,
        test=False,
    )
    assert "Wrote derivatives/issues_summary.json" in result.output


@pytest.mark.ai_generated
def test_cli_jobs_dispatch_rejects_a_missing_base_directory(tmp_path: pathlib.Path) -> None:
    """Queue process command rejects a --base that does not exist."""
    runner = CliRunner()
    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch(f"{_GROUP}.PipelineQueue.dispatch_jobs", return_value={}) as mock_dispatch,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "dispatch", "--base", str(tmp_path / "missing")])
    assert result.exit_code != 0
    assert "Invalid value for '--base'" in result.output
    mock_dispatch.assert_not_called()


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("extra_arguments", "expected_keyword_arguments"),
    [
        pytest.param([], {}, id="defaults"),
        pytest.param(["--pipeline", "lfp"], {"only_pipeline": "lfp"}, id="pipeline"),
        pytest.param(
            ["--pipeline", "lfp", "--max", "4"],
            {"only_pipeline": "lfp", "max_concurrent": 4},
            id="pipeline-and-max",
        ),
        pytest.param(["--test"], {"test": True}, id="test"),
        pytest.param(["--jitter", "120.0"], {"jitter_seconds": 120.0}, id="jitter"),
        pytest.param(["--jitter", "0"], {"jitter_seconds": 0.0}, id="zero-jitter"),
        pytest.param(["--record"], {"record": True}, id="record"),
    ],
)
def test_cli_jobs_dispatch_forwards_its_options(
    base_directory: pathlib.Path, extra_arguments: list[str], expected_keyword_arguments: dict
) -> None:
    """dandicompute jobs dispatch forwards each of its options to PipelineQueue.dispatch_jobs."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.dispatch_jobs", return_value={}) as mock_dispatch:
        result = runner.invoke(
            _dandicompute_group,
            ["jobs", "dispatch", "--base", str(base_directory), *extra_arguments],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    mock_dispatch.assert_called_once_with(
        **{
            "base_directory": base_directory,
            "only_pipeline": None,
            "max_concurrent": None,
            "jitter_seconds": 30.0,
            "record": False,
            "test": False,
            **expected_keyword_arguments,
        }
    )


@pytest.mark.ai_generated
def test_cli_jobs_dispatch_reports_each_pipelines_dispatch_outcome(base_directory: pathlib.Path) -> None:
    """dandicompute jobs dispatch prints one summary line per configured pipeline."""
    runner = CliRunner()

    results = {
        "aind+ephys": DispatchResult(
            pipeline="aind+ephys",
            status="dispatched",
            arrays=(
                DispatchedArray(
                    array_job_id="4242",
                    task_count=3,
                    resources=CapsuleResources(
                        memory="1GB", cpus_per_task=1, partition="mit_normal", time_limit="12:00:00"
                    ),
                    max_concurrent=2,
                    manifest_file_path=base_directory
                    / "processing"
                    / "derivatives"
                    / "logs"
                    / "dispatch"
                    / "manifest-1.txt",
                    script_file_path=base_directory
                    / "processing"
                    / "derivatives"
                    / "logs"
                    / "dispatch"
                    / "dispatch-1.sh",
                ),
            ),
        ),
        "lfp": DispatchResult(pipeline="lfp", status="no-pending"),
    }

    with mock.patch(f"{_GROUP}.PipelineQueue.dispatch_jobs", return_value=results):
        result = runner.invoke(
            _dandicompute_group,
            ["jobs", "dispatch", "--base", str(base_directory)],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    assert "aind+ephys: dispatched 3 capsules as array job 4242." in result.output
    assert "lfp: no capsules are waiting to be submitted." in result.output


@pytest.mark.ai_generated
def test_cli_jobs_dispatch_reports_a_dispatcher_that_is_still_working(base_directory: pathlib.Path) -> None:
    """A pipeline left alone because its array is still live is reported as such."""
    runner = CliRunner()

    results = {"lfp": DispatchResult(pipeline="lfp", status="dispatcher-active", active_job_ids=("9001",))}

    with mock.patch(f"{_GROUP}.PipelineQueue.dispatch_jobs", return_value=results):
        result = runner.invoke(
            _dandicompute_group,
            ["jobs", "dispatch", "--base", str(base_directory)],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    assert "the dispatcher is already running" in result.output


@pytest.mark.ai_generated
def test_cli_jobs_dispatch_requires_dandi_devel(base_directory: pathlib.Path) -> None:
    """Queue process command exits non-zero when DANDI_DEVEL is not set."""
    runner = CliRunner()

    result = runner.invoke(
        _dandicompute_group,
        ["jobs", "dispatch", "--base", str(base_directory)],
        env={"DANDI_API_KEY": "test-key"},
    )

    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("pending", "expected_exit_code", "expected_output"),
    [
        pytest.param(True, 0, "true", id="pending-exits-zero"),
        pytest.param(False, 1, "false", id="not-pending-exits-one"),
    ],
)
def test_cli_jobs_pending_reports_and_sets_exit_code(
    pending: bool, expected_exit_code: int, expected_output: str
) -> None:
    """dandicompute jobs pending prints the boolean and exits 0 when pending, 1 otherwise."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.has_pending_jobs", return_value=pending) as mock_has_pending:
        result = runner.invoke(_dandicompute_group, ["jobs", "pending"])

    assert result.exit_code == expected_exit_code, result.output
    mock_has_pending.assert_called_once_with()
    assert expected_output in result.output


@pytest.mark.ai_generated
def test_cli_jobs_pending_silent_suppresses_output(tmp_path: pathlib.Path) -> None:
    """dandicompute jobs pending --silent still sets the exit code but prints nothing."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.has_pending_jobs", return_value=False):
        result = runner.invoke(_dandicompute_group, ["jobs", "pending", "--silent"])

    assert result.exit_code == 1, result.output
    assert "false" not in result.output


@pytest.mark.ai_generated
def test_cli_jobs_dispatch_reports_when_no_pipelines_are_configured(base_directory: pathlib.Path) -> None:
    """dandicompute jobs dispatch says so rather than staying silent with nothing to dispatch."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.dispatch_jobs", return_value={}):
        result = runner.invoke(
            _dandicompute_group,
            ["jobs", "dispatch", "--base", str(base_directory)],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    assert "No pipelines are configured for dispatch." in result.output


@pytest.mark.ai_generated
def test_cli_jobs_dispatch_rejects_max_without_pipeline(base_directory: pathlib.Path) -> None:
    """--max overrides a per-pipeline setting, so it may not be given for every pipeline at once."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.dispatch_jobs", return_value={}) as mock_dispatch:
        result = runner.invoke(
            _dandicompute_group,
            ["jobs", "dispatch", "--base", str(base_directory), "--max", "4"],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code != 0
    assert "requires --pipeline" in result.output
    mock_dispatch.assert_not_called()


@pytest.mark.ai_generated
def test_cli_clean_requires_something_to_clean(base_directory: pathlib.Path) -> None:
    """dandicompute clean with neither target is a usage error rather than a silent no-op."""
    runner = CliRunner()

    result = runner.invoke(_dandicompute_group, ["clean", "--base", str(base_directory)])

    assert result.exit_code != 0
    assert "Nothing to clean" in result.output


@pytest.mark.ai_generated
def test_cli_clean_forwards_the_base_directory_and_age(base_directory: pathlib.Path) -> None:
    """dandicompute clean --dispatch forwards the base directory and --age to the cleanup helper."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.clean_dispatch_directories", return_value=[]) as mock_clean:
        result = runner.invoke(
            _dandicompute_group,
            ["clean", "--base", str(base_directory), "--dispatch", "--age", "6"],
        )

    assert result.exit_code == 0, result.output
    mock_clean.assert_called_once_with(base_directory=base_directory, minimum_age_hours=6.0)


@pytest.mark.ai_generated
def test_cli_clean_reports_how_many_dispatch_directories_went(base_directory: pathlib.Path) -> None:
    """dandicompute clean --dispatch reports what it removed."""
    runner = CliRunner()

    removed = [base_directory / "processing" / "dandicompute-dispatch-lfp-20260101-000000"]
    with mock.patch(f"{_GROUP}.clean_dispatch_directories", return_value=removed):
        result = runner.invoke(_dandicompute_group, ["clean", "--base", str(base_directory), "--dispatch"])

    assert result.exit_code == 0, result.output
    assert "Removed 1 finished dispatch directory." in result.output


@pytest.mark.ai_generated
def test_cli_clean_reports_when_nothing_was_ready_to_remove(base_directory: pathlib.Path) -> None:
    """A dispatch cleanup that removed nothing says so rather than claiming success."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.clean_dispatch_directories", return_value=[]):
        result = runner.invoke(_dandicompute_group, ["clean", "--base", str(base_directory), "--dispatch"])

    assert result.exit_code == 0, result.output
    assert "No dispatch directories were ready to be removed." in result.output


@pytest.mark.ai_generated
def test_cli_clean_still_cleans_a_work_directory_on_its_own(base_directory: pathlib.Path) -> None:
    """--work keeps working by itself, and does not trigger a dispatch cleanup."""
    runner = CliRunner()

    with (
        mock.patch(f"{_GROUP}.clean_work_directory") as mock_work,
        mock.patch(f"{_GROUP}.clean_dispatch_directories") as mock_dispatch,
    ):
        result = runner.invoke(_dandicompute_group, ["clean", "--base", str(base_directory), "--work"])

    assert result.exit_code == 0, result.output
    mock_work.assert_called_once_with(base_directory)
    mock_dispatch.assert_not_called()
