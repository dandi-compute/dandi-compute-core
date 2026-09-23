import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import DispatchResult, PipelineQueue


def _no_pending(*, pipeline: str, **_: object) -> DispatchResult:
    """A dispatch_pipeline_jobs stand-in reporting nothing to dispatch, typed as the real one is."""
    return DispatchResult(pipeline=pipeline, status="no-pending")


@pytest.mark.ai_generated
def test_dispatch_jobs_reports_no_pending_for_every_configured_pipeline(
    base_directory: pathlib.Path,
) -> None:
    """With nothing pending, every configured pipeline reports back rather than dispatching."""
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]):
        results = PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0)

    assert set(results) == set(PipelineQueue.load_pipeline_config()["pipelines"])
    assert {result.status for result in results.values()} == {"no-pending"}


@pytest.mark.ai_generated
def test_dispatch_jobs_dispatches_only_the_requested_pipeline(base_directory: pathlib.Path) -> None:
    """--pipeline narrows dispatch to a single pipeline's array."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.dispatch_pipeline_jobs", side_effect=_no_pending
        ) as mock_dispatch,
    ):
        PipelineQueue.dispatch_jobs(
            base_directory=base_directory,
            only_pipeline="lfp",
            jitter_seconds=0.0,
        )

    assert mock_dispatch.call_count == 1
    assert mock_dispatch.call_args.kwargs["pipeline"] == "lfp"


@pytest.mark.ai_generated
def test_dispatch_jobs_forwards_the_concurrency_override_to_the_dispatcher(
    base_directory: pathlib.Path,
) -> None:
    """An explicit max_concurrent replaces each dispatched pipeline's configured limit."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.dispatch_pipeline_jobs", side_effect=_no_pending
        ) as mock_dispatch,
    ):
        PipelineQueue.dispatch_jobs(
            base_directory=base_directory,
            only_pipeline="lfp",
            max_concurrent=7,
            jitter_seconds=0.0,
        )

    assert mock_dispatch.call_args.kwargs["dispatch_config"].max_concurrent == 7


@pytest.mark.ai_generated
def test_dispatch_jobs_reads_the_pending_capsules_once_for_all_pipelines(
    base_directory: pathlib.Path,
) -> None:
    """The pending capsules come from one metadata read shared across every pipeline."""
    code_dir_paths = ["derivatives/dandiset-000409/sub-mouse01/pipeline-lfp/job-240101abc123/code"]
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs",
            return_value=code_dir_paths,
        ) as mock_pending,
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.dispatch_pipeline_jobs", side_effect=_no_pending
        ) as mock_dispatch,
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0)

    mock_pending.assert_called_once_with()
    assert mock_dispatch.call_count == len(PipelineQueue.load_pipeline_config()["pipelines"])
    for call in mock_dispatch.call_args_list:
        assert call.kwargs["code_dir_paths"] == code_dir_paths


@pytest.mark.ai_generated
def test_dispatch_jobs_forwards_test_flag(base_directory: pathlib.Path) -> None:
    """Test mode reaches the dispatcher so array tasks keep their working trees."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.dispatch_pipeline_jobs", side_effect=_no_pending
        ) as mock_dispatch,
    ):
        PipelineQueue.dispatch_jobs(
            base_directory=base_directory,
            only_pipeline="lfp",
            jitter_seconds=0.0,
            test=True,
        )

    assert mock_dispatch.call_args.kwargs["test"] is True


@pytest.mark.ai_generated
def test_dispatch_jobs_rejects_non_positive_max_concurrent(base_directory: pathlib.Path) -> None:
    """dispatch_jobs raises when max_concurrent is less than one."""
    with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
        PipelineQueue.dispatch_jobs(
            base_directory=base_directory,
            max_concurrent=0,
            jitter_seconds=0.0,
        )


@pytest.mark.ai_generated
def test_dispatch_jobs_rejects_an_unconfigured_pipeline(base_directory: pathlib.Path) -> None:
    """dispatch_jobs raises when asked for a pipeline the configuration does not declare."""
    with pytest.raises(ValueError, match="Pipeline 'nope' is not configured"):
        PipelineQueue.dispatch_jobs(
            base_directory=base_directory,
            only_pipeline="nope",
            jitter_seconds=0.0,
        )


@pytest.mark.ai_generated
def test_dispatch_jobs_rejects_negative_jitter_seconds(base_directory: pathlib.Path) -> None:
    """dispatch_jobs raises when jitter_seconds is negative."""
    with pytest.raises(ValueError, match="jitter_seconds must be non-negative"):
        PipelineQueue.dispatch_jobs(
            base_directory=base_directory,
            jitter_seconds=-1.0,
        )


@pytest.mark.ai_generated
def test_dispatch_jobs_skips_sleep_when_jitter_is_zero(base_directory: pathlib.Path) -> None:
    """dispatch_jobs does not sleep when jitter_seconds=0."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0)

    mock_sleep.assert_not_called()


@pytest.mark.ai_generated
def test_dispatch_jobs_sleeps_within_jitter_range(base_directory: pathlib.Path) -> None:
    """dispatch_jobs sleeps a duration in [0, jitter_seconds] when jitter_seconds > 0."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._pipeline_queue.random.uniform", return_value=5.0) as mock_uniform,
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=30.0)

    mock_uniform.assert_called_once_with(0, 30.0)
    mock_sleep.assert_called_once_with(5.0)
