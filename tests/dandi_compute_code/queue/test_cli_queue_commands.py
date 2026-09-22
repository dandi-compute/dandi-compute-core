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
def test_cli_prepare_test_creates_capsules_for_the_test_content_id() -> None:
    """dandicompute prepare aind --test creates job capsules for the known test content ID."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["prepare", "aind", "--test"])

    assert result.exit_code == 0
    mock_create.assert_called_once_with(
        content_ids=[TEST_QUEUE_CONTENT_ID],
        pipeline_directory=None,
        config_key="default",
    )


@pytest.mark.ai_generated
def test_cli_jobs_create_forwards_pipeline_as_only_pipeline() -> None:
    """dandicompute jobs create --pipeline <name> forwards only_pipeline to PipelineQueue.create_job_capsules."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "create", "--pipeline", "lfp", "--limit", "5"])

    assert result.exit_code == 0
    mock_create.assert_called_once_with(
        config_key="default",
        limit=5,
        only_pipeline="lfp",
        force_latest_versions=False,
    )


@pytest.mark.ai_generated
def test_cli_jobs_create_forwards_latest_flag() -> None:
    """dandicompute jobs create --latest forwards force_latest_versions to PipelineQueue.create_job_capsules."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "create", "--latest"])

    assert result.exit_code == 0
    assert mock_create.call_args.kwargs["force_latest_versions"] is True


@pytest.mark.ai_generated
def test_cli_jobs_create_defaults_to_no_limit() -> None:
    """dandicompute jobs create with no --limit creates capsules without a cap."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "create"])

    assert result.exit_code == 0
    assert mock_create.call_args.kwargs["limit"] is None


@pytest.mark.ai_generated
def test_cli_jobs_create_requires_a_dandi_api_key() -> None:
    """dandicompute jobs create fails clearly when DANDI_API_KEY is not set."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": ""}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(_dandicompute_group, ["jobs", "create"])

    assert result.exit_code != 0
    assert mock_create.call_count == 0


@pytest.mark.ai_generated
def test_cli_prepare_test_passes_config_key() -> None:
    """dandicompute prepare aind --test forwards --config to PipelineQueue.create_job_capsules."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.PipelineQueue.create_job_capsules") as mock_create,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["prepare", "aind", "--test", "--config", "mit+engaging+revision-1"],
        )

    assert result.exit_code == 0
    mock_create.assert_called_once_with(
        content_ids=[TEST_QUEUE_CONTENT_ID],
        pipeline_directory=None,
        config_key="mit+engaging+revision-1",
    )


@pytest.mark.ai_generated
def test_cli_aind_prepare_passes_config_key() -> None:
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
def test_cli_queue_clean_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute queue clean delegates to PipelineQueue and reports removed paths."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    fake_removed = [dandiset_dir / "derivatives" / "dandiset-000001" / "sub-mouse01" / "capsule-a"]
    mock_state = mock.Mock()
    mock_state.clean_unsubmitted_capsules.return_value = fake_removed
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=mock_state) as mock_from_dandi:
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "clean", "--dandiset", str(dandiset_dir)],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    mock_from_dandi.assert_called_once_with()
    mock_state.clean_unsubmitted_capsules.assert_called_once_with(dandiset_directory=dandiset_dir)
    assert "Cleaned 1 unsubmitted capsule" in result.output


@pytest.mark.ai_generated
def test_cli_queue_clean_reports_nothing_found(tmp_path: pathlib.Path) -> None:
    """dandicompute queue clean reports when no unsubmitted capsules are found."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    mock_state = mock.Mock()
    mock_state.clean_unsubmitted_capsules.return_value = []
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=mock_state):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "clean", "--dandiset", str(dandiset_dir)],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    assert "No unsubmitted capsules found" in result.output


@pytest.mark.ai_generated
def test_cli_queue_stats_calls_helper_and_reports_output(tmp_path: pathlib.Path) -> None:
    """dandicompute queue stats delegates to PipelineQueue.aggregate_statistics."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    mock_state = mock.Mock()
    mock_state.aggregate_statistics.return_value = {"successful_asset_bytes_total": 0}
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=mock_state) as mock_from_dandi:
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "stats", "--dandiset", str(dandiset_dir)],
        )

    assert result.exit_code == 0, result.output
    mock_from_dandi.assert_called_once_with(dandiset_id="001697")
    mock_state.aggregate_statistics.assert_called_once_with(
        dandiset_directory=dandiset_dir,
        dandiset_id="001697",
        processing_directory=None,
        test=False,
    )
    assert "Wrote derivatives/queue_stats.json" in result.output


@pytest.mark.ai_generated
def test_cli_queue_stats_forwards_custom_dandiset_id(tmp_path: pathlib.Path) -> None:
    """dandicompute queue stats forwards --dandiset-id to PipelineQueue.from_dandi/aggregate_statistics."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    mock_state = mock.Mock()
    mock_state.aggregate_statistics.return_value = {}
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.from_dandi", return_value=mock_state) as mock_from_dandi:
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "stats", "--dandiset", str(dandiset_dir), "--dandiset-id", "000123"],
        )

    assert result.exit_code == 0, result.output
    mock_from_dandi.assert_called_once_with(dandiset_id="000123")
    assert mock_state.aggregate_statistics.call_args.kwargs["dandiset_id"] == "000123"


@pytest.mark.ai_generated
def test_cli_issues_dump_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute issues dump delegates to PipelineQueue.dump_issues and reports output."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch(f"{_GROUP}.PipelineQueue.dump_issues", return_value=[]) as mock_dump:
        result = runner.invoke(
            _dandicompute_group,
            ["issues", "dump", "--directory", str(dandiset_dir)],
        )

    assert result.exit_code == 0, result.output
    mock_dump.assert_called_once_with(
        dandiset_directory=dandiset_dir,
        dandiset_id="001697",
        processing_directory=None,
        test=False,
    )
    assert "Wrote derivatives/issues_dump.json" in result.output


@pytest.mark.ai_generated
def test_cli_issues_summarize_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute issues summarize delegates to PipelineQueue.summarize_issues and reports output."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch(f"{_GROUP}.PipelineQueue.summarize_issues", return_value={}) as mock_summarize:
        result = runner.invoke(
            _dandicompute_group,
            ["issues", "summarize", "--directory", str(dandiset_dir)],
        )

    assert result.exit_code == 0, result.output
    mock_summarize.assert_called_once_with(
        dandiset_directory=dandiset_dir,
        dandiset_id="001697",
        processing_directory=None,
        test=False,
    )
    assert "Wrote derivatives/issues_summary.json" in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_requires_processing_directory() -> None:
    """Queue process command requires --processing."""
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, ["queue", "process"])
    assert result.exit_code != 0
    assert "Missing option '--processing'" in result.output


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
    ],
)
def test_cli_queue_process_forwards_its_options(
    tmp_path: pathlib.Path, extra_arguments: list[str], expected_keyword_arguments: dict
) -> None:
    """dandicompute queue process forwards each of its options to PipelineQueue.process_queue."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.process_queue", return_value={}) as mock_process:
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--processing", str(processing_dir), *extra_arguments],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    mock_process.assert_called_once_with(
        **{
            "processing_directory": processing_dir,
            "only_pipeline": None,
            "max_concurrent": None,
            "jitter_seconds": 30.0,
            "test": False,
            **expected_keyword_arguments,
        }
    )


@pytest.mark.ai_generated
def test_cli_queue_process_reports_each_pipelines_dispatch_outcome(tmp_path: pathlib.Path) -> None:
    """dandicompute queue process prints one summary line per configured pipeline."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
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
                ),
            ),
        ),
        "lfp": DispatchResult(pipeline="lfp", status="no-pending"),
    }

    with mock.patch(f"{_GROUP}.PipelineQueue.process_queue", return_value=results):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--processing", str(processing_dir)],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    assert "aind+ephys: dispatched 3 capsules as array job 4242." in result.output
    assert "lfp: no capsules are waiting to be submitted." in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_reports_a_dispatcher_that_is_still_working(tmp_path: pathlib.Path) -> None:
    """A pipeline left alone because its array is still live is reported as such."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    results = {"lfp": DispatchResult(pipeline="lfp", status="dispatcher-active", active_job_ids=("9001",))}

    with mock.patch(f"{_GROUP}.PipelineQueue.process_queue", return_value=results):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--processing", str(processing_dir)],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    assert "the dispatcher is already running" in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_requires_dandi_devel(tmp_path: pathlib.Path) -> None:
    """Queue process command exits non-zero when DANDI_DEVEL is not set."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        _dandicompute_group,
        ["queue", "process", "--processing", str(processing_dir)],
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
def test_cli_queue_pending_reports_and_sets_exit_code(
    pending: bool, expected_exit_code: int, expected_output: str
) -> None:
    """dandicompute queue pending prints the boolean and exits 0 when pending, 1 otherwise."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.has_pending_jobs", return_value=pending) as mock_has_pending:
        result = runner.invoke(_dandicompute_group, ["queue", "pending"])

    assert result.exit_code == expected_exit_code, result.output
    mock_has_pending.assert_called_once_with()
    assert expected_output in result.output


@pytest.mark.ai_generated
def test_cli_queue_pending_silent_suppresses_output(tmp_path: pathlib.Path) -> None:
    """dandicompute queue pending --silent still sets the exit code but prints nothing."""
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.has_pending_jobs", return_value=False):
        result = runner.invoke(_dandicompute_group, ["queue", "pending", "--silent"])

    assert result.exit_code == 1, result.output
    assert "false" not in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_reports_when_no_pipelines_are_configured(tmp_path: pathlib.Path) -> None:
    """dandicompute queue process says so rather than staying silent with nothing to dispatch."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.process_queue", return_value={}):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--processing", str(processing_dir)],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    assert "No pipelines are configured for dispatch." in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_rejects_max_without_pipeline(tmp_path: pathlib.Path) -> None:
    """--max overrides a per-pipeline setting, so it may not be given for every pipeline at once."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.PipelineQueue.process_queue", return_value={}) as mock_process:
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--processing", str(processing_dir), "--max", "4"],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code != 0
    assert "requires --pipeline" in result.output
    mock_process.assert_not_called()


@pytest.mark.ai_generated
def test_cli_clean_requires_something_to_clean() -> None:
    """dandicompute clean with neither target is a usage error rather than a silent no-op."""
    runner = CliRunner()

    result = runner.invoke(_dandicompute_group, ["clean"])

    assert result.exit_code != 0
    assert "Nothing to clean" in result.output


@pytest.mark.ai_generated
def test_cli_clean_forwards_the_dispatch_directory_and_age(tmp_path: pathlib.Path) -> None:
    """dandicompute clean --dispatch forwards its target and --age to the cleanup helper."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.clean_dispatch_directories", return_value=[]) as mock_clean:
        result = runner.invoke(
            _dandicompute_group,
            ["clean", "--dispatch", str(processing_dir), "--age", "6"],
        )

    assert result.exit_code == 0, result.output
    mock_clean.assert_called_once_with(processing_directory=processing_dir, minimum_age_hours=6.0)


@pytest.mark.ai_generated
def test_cli_clean_reports_how_many_dispatch_directories_went(tmp_path: pathlib.Path) -> None:
    """dandicompute clean --dispatch reports what it removed."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    removed = [processing_dir / "dandicompute-dispatch-lfp-20260101-000000"]
    with mock.patch(f"{_GROUP}.clean_dispatch_directories", return_value=removed):
        result = runner.invoke(_dandicompute_group, ["clean", "--dispatch", str(processing_dir)])

    assert result.exit_code == 0, result.output
    assert "Removed 1 finished dispatch directory." in result.output


@pytest.mark.ai_generated
def test_cli_clean_reports_when_nothing_was_ready_to_remove(tmp_path: pathlib.Path) -> None:
    """A dispatch cleanup that removed nothing says so rather than claiming success."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch(f"{_GROUP}.clean_dispatch_directories", return_value=[]):
        result = runner.invoke(_dandicompute_group, ["clean", "--dispatch", str(processing_dir)])

    assert result.exit_code == 0, result.output
    assert "No dispatch directories were ready to be removed." in result.output


@pytest.mark.ai_generated
def test_cli_clean_still_cleans_a_work_directory_on_its_own(tmp_path: pathlib.Path) -> None:
    """--directory keeps working by itself, and does not trigger a dispatch cleanup."""
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    runner = CliRunner()

    with (
        mock.patch(f"{_GROUP}.clean_work_directory") as mock_work,
        mock.patch(f"{_GROUP}.clean_dispatch_directories") as mock_dispatch,
    ):
        result = runner.invoke(_dandicompute_group, ["clean", "--directory", str(work_dir)])

    assert result.exit_code == 0, result.output
    mock_work.assert_called_once_with(directory=work_dir)
    mock_dispatch.assert_not_called()
