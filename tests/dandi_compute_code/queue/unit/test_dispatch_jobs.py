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


def _squeue_run(command: list[str], **_: object) -> mock.MagicMock:
    """A subprocess.run stand-in answering the snapshot's `squeue --me` call."""
    result = mock.MagicMock()
    result.returncode = 0
    result.stderr = ""
    result.stdout = "JOBID PARTITION NAME\n4242 mit_preemptable dandicompute-dispatch-aind-ephys\n"
    return result


def _records(base_directory: pathlib.Path, /) -> list[pathlib.Path]:
    return sorted((base_directory / "processing" / "derivatives" / "logs" / "squeue").glob("*-squeue.txt"))


@pytest.mark.ai_generated
def test_dispatch_jobs_records_the_attempt_locally_and_on_the_dandiset(base_directory: pathlib.Path) -> None:
    """A recorded attempt shows what each pipeline did and what squeue listed, even with nothing to do."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_squeue_run) as mock_run,
        mock.patch("dandi_compute_code.queue._dispatch.write_dandiset_file") as mock_post,
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0, record=True)

    [record_file_path] = _records(base_directory)
    record = record_file_path.read_text()
    assert "aind+ephys: no capsules are waiting to be submitted." in record
    assert "4242 mit_preemptable dandicompute-dispatch-aind-ephys" in record
    assert mock_run.call_args.args[0][:2] == ["squeue", "--me"]
    assert mock_post.call_args.kwargs["dandiset_id"] == "001697"
    assert mock_post.call_args.kwargs["relative_path"] == f"derivatives/logs/squeue/{record_file_path.name}"
    assert mock_post.call_args.kwargs["content"] == record


@pytest.mark.ai_generated
def test_dispatch_jobs_records_nothing_unless_asked(base_directory: pathlib.Path) -> None:
    """Recording posts to the archive, so a plain dispatch leaves no record."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
        mock.patch("dandi_compute_code.queue._dispatch.write_dandiset_file") as mock_post,
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0)

    assert _records(base_directory) == []
    mock_post.assert_not_called()


@pytest.mark.ai_generated
def test_dispatch_jobs_records_an_attempt_that_fails(base_directory: pathlib.Path) -> None:
    """A failed attempt is still one the cluster made, so it is recorded with its error and re-raised."""
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs",
            side_effect=RuntimeError("archive unreachable"),
        ),
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_squeue_run),
        mock.patch("dandi_compute_code.queue._dispatch.write_dandiset_file"),
        pytest.raises(RuntimeError, match="archive unreachable"),
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0, record=True)

    [record_file_path] = _records(base_directory)
    assert "The attempt stopped with an error: RuntimeError: archive unreachable" in record_file_path.read_text()


@pytest.mark.ai_generated
def test_dispatch_jobs_keeps_its_results_when_the_record_cannot_be_posted(base_directory: pathlib.Path) -> None:
    """A failed post is logged rather than raised, so it never masks the dispatch it records."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_squeue_run),
        mock.patch("dandi_compute_code.queue._dispatch.write_dandiset_file", side_effect=RuntimeError("upload failed")),
    ):
        results = PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0, record=True)

    assert {result.status for result in results.values()} == {"no-pending"}
    assert len(_records(base_directory)) == 1


@pytest.mark.ai_generated
def test_dispatch_jobs_records_why_squeue_could_not_be_read(base_directory: pathlib.Path) -> None:
    """A missing squeue is noted in the record instead of stopping it."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]),
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=FileNotFoundError("squeue")),
        mock.patch("dandi_compute_code.queue._dispatch.write_dandiset_file"),
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0, record=True)

    [record_file_path] = _records(base_directory)
    assert "(squeue could not be run:" in record_file_path.read_text()
