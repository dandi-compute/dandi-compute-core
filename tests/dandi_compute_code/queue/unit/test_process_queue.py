import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import QueueState


@pytest.mark.ai_generated
def test_process_queue_reports_no_pending_for_every_configured_pipeline(
    processing_directory: pathlib.Path,
) -> None:
    """With nothing pending, every configured pipeline reports back rather than dispatching."""
    with mock.patch("dandi_compute_code.queue._queue_state.QueueState.pending_code_dirs", return_value=[]):
        results = QueueState.process_queue(processing_directory=processing_directory, jitter_seconds=0)

    assert set(results) == set(QueueState.load_pipeline_config()["pipelines"])
    assert {result.status for result in results.values()} == {"no-pending"}


@pytest.mark.ai_generated
def test_process_queue_dispatches_only_the_requested_pipeline(processing_directory: pathlib.Path) -> None:
    """--pipeline narrows dispatch to a single pipeline's array."""
    with (
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.pending_code_dirs", return_value=[]),
        mock.patch("dandi_compute_code.queue._queue_state.dispatch_pipeline_jobs") as mock_dispatch,
    ):
        QueueState.process_queue(
            processing_directory=processing_directory,
            only_pipeline="lfp",
            jitter_seconds=0,
        )

    assert mock_dispatch.call_count == 1
    assert mock_dispatch.call_args.kwargs["pipeline"] == "lfp"


@pytest.mark.ai_generated
def test_process_queue_forwards_the_concurrency_override_to_the_dispatcher(
    processing_directory: pathlib.Path,
) -> None:
    """An explicit max_concurrent replaces each dispatched pipeline's configured limit."""
    with (
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.pending_code_dirs", return_value=[]),
        mock.patch("dandi_compute_code.queue._queue_state.dispatch_pipeline_jobs") as mock_dispatch,
    ):
        QueueState.process_queue(
            processing_directory=processing_directory,
            only_pipeline="lfp",
            max_concurrent=7,
            jitter_seconds=0,
        )

    assert mock_dispatch.call_args.kwargs["dispatch_config"].max_concurrent == 7


@pytest.mark.ai_generated
def test_process_queue_reads_the_pending_capsules_once_for_all_pipelines(
    processing_directory: pathlib.Path,
) -> None:
    """The pending capsules come from one metadata read shared across every pipeline."""
    code_dir_paths = ["derivatives/dandiset-000409/sub-mouse01/pipeline-lfp/job-240101abc123/code"]
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.pending_code_dirs",
            return_value=code_dir_paths,
        ) as mock_pending,
        mock.patch("dandi_compute_code.queue._queue_state.dispatch_pipeline_jobs") as mock_dispatch,
    ):
        QueueState.process_queue(processing_directory=processing_directory, jitter_seconds=0)

    mock_pending.assert_called_once_with()
    assert mock_dispatch.call_count == len(QueueState.load_pipeline_config()["pipelines"])
    for call in mock_dispatch.call_args_list:
        assert call.kwargs["code_dir_paths"] == code_dir_paths


@pytest.mark.ai_generated
def test_process_queue_forwards_test_flag(processing_directory: pathlib.Path) -> None:
    """Test mode reaches the dispatcher so array tasks keep their working trees."""
    with (
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.pending_code_dirs", return_value=[]),
        mock.patch("dandi_compute_code.queue._queue_state.dispatch_pipeline_jobs") as mock_dispatch,
    ):
        QueueState.process_queue(
            processing_directory=processing_directory,
            only_pipeline="lfp",
            jitter_seconds=0,
            test=True,
        )

    assert mock_dispatch.call_args.kwargs["test"] is True


@pytest.mark.ai_generated
def test_process_queue_rejects_non_positive_max_concurrent(processing_directory: pathlib.Path) -> None:
    """process_queue raises when max_concurrent is less than one."""
    with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
        QueueState.process_queue(
            processing_directory=processing_directory,
            max_concurrent=0,
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_rejects_an_unconfigured_pipeline(processing_directory: pathlib.Path) -> None:
    """process_queue raises when asked for a pipeline the configuration does not declare."""
    with pytest.raises(ValueError, match="Pipeline 'nope' is not configured"):
        QueueState.process_queue(
            processing_directory=processing_directory,
            only_pipeline="nope",
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_rejects_negative_jitter_seconds(processing_directory: pathlib.Path) -> None:
    """process_queue raises when jitter_seconds is negative."""
    with pytest.raises(ValueError, match="jitter_seconds must be non-negative"):
        QueueState.process_queue(
            processing_directory=processing_directory,
            jitter_seconds=-1.0,
        )


@pytest.mark.ai_generated
def test_process_queue_skips_sleep_when_jitter_is_zero(processing_directory: pathlib.Path) -> None:
    """process_queue does not sleep when jitter_seconds=0."""
    with (
        mock.patch("dandi_compute_code.queue._queue_state.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.pending_code_dirs", return_value=[]),
    ):
        QueueState.process_queue(processing_directory=processing_directory, jitter_seconds=0)

    mock_sleep.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_sleeps_within_jitter_range(processing_directory: pathlib.Path) -> None:
    """process_queue sleeps a duration in [0, jitter_seconds] when jitter_seconds > 0."""
    with (
        mock.patch("dandi_compute_code.queue._queue_state.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._queue_state.random.uniform", return_value=5.0) as mock_uniform,
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.pending_code_dirs", return_value=[]),
    ):
        QueueState.process_queue(processing_directory=processing_directory, jitter_seconds=30.0)

    mock_uniform.assert_called_once_with(0, 30.0)
    mock_sleep.assert_called_once_with(5.0)
