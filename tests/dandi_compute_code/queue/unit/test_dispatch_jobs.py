import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import DispatchResult, PipelineQueue


def _no_pending(*, pipeline: str, **_: object) -> DispatchResult:
    """A dispatch_pipeline_jobs stand-in reporting nothing to dispatch, typed as the real one is."""
    return DispatchResult(pipeline=pipeline, status="no-pending")


def _configured_dispatcher_labels() -> set[str]:
    """One label per dispatcher the packaged configuration declares: each pipeline and each dedicated parameter set."""
    pipelines = PipelineQueue.load_pipeline_config()["pipelines"]
    labels = set(pipelines)
    for pipeline_name, pipeline_data in pipelines.items():
        labels.update(f"{pipeline_name}/{params_key}" for params_key in pipeline_data.get("params_dispatch", {}))
    return labels


@pytest.mark.ai_generated
def test_dispatch_jobs_reports_no_pending_for_every_configured_dispatcher(
    base_directory: pathlib.Path,
) -> None:
    """With nothing pending, every configured dispatcher reports back rather than dispatching."""
    with mock.patch("dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=[]):
        results = PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0)

    assert set(results) == _configured_dispatcher_labels()
    assert {result.status for result in results.values()} == {"no-pending"}


@pytest.mark.ai_generated
def test_dispatch_jobs_gives_a_dedicated_parameter_set_its_own_dispatcher(base_directory: pathlib.Path) -> None:
    """
    Capsules of a parameter set with a dispatcher of its own go to that dispatcher, under its limits.

    The rest of the pipeline's capsules, including one whose parameters could not be read, go to
    the pipeline's own dispatcher. Capsules are matched by the params ID they record, not by name.
    """
    pipeline_config = {
        "pipelines": {
            "aind+ephys": {
                "params": ["default", "lupin"],
                "dispatch": {"max_concurrent": 2},
                "params_dispatch": {"lupin": {"max_concurrent": 10}},
            }
        }
    }
    lupin_id = PipelineQueue.resolve_params_key_to_id(pipeline="aind+ephys", params_key="lupin")
    default_id = PipelineQueue.resolve_params_key_to_id(pipeline="aind+ephys", params_key="default")
    code_dir_paths = [
        "derivatives/dandiset-000409/sub-a/pipeline-aind+ephys/job-240101aaaaaa/code",
        "derivatives/dandiset-000409/sub-b/pipeline-aind+ephys/job-240101bbbbbb/code",
        "derivatives/dandiset-000409/sub-c/pipeline-aind+ephys/job-240101cccccc/code",
        "derivatives/dandiset-000409/sub-d/pipeline-aind+ephys/job-240101dddddd/code",
    ]
    capsule_params = {code_dir_paths[0]: default_id, code_dir_paths[1]: lupin_id, code_dir_paths[3]: lupin_id}

    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue._load_pipeline_config", return_value=pipeline_config),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=code_dir_paths
        ),
        mock.patch("dandi_compute_code.queue._pipeline_queue.read_capsule_params", return_value=capsule_params),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.dispatch_pipeline_jobs", side_effect=_no_pending
        ) as mock_dispatch,
    ):
        results = PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0)

    assert list(results) == ["aind+ephys/lupin", "aind+ephys"]
    calls = {call.kwargs["dispatch_config"].params_key: call.kwargs for call in mock_dispatch.call_args_list}
    assert calls["lupin"]["code_dir_paths"] == [code_dir_paths[1], code_dir_paths[3]]
    assert calls["lupin"]["dispatch_config"].max_concurrent == 10
    assert calls["lupin"]["dispatch_config"].job_name == "dandicompute-dispatch-aind-ephys-lupin"
    assert calls[None]["code_dir_paths"] == [code_dir_paths[0], code_dir_paths[2]]
    assert calls[None]["dispatch_config"].max_concurrent == 2


@pytest.mark.ai_generated
def test_dispatch_jobs_does_not_read_capsule_parameters_for_a_pipeline_with_one_dispatcher(
    base_directory: pathlib.Path,
) -> None:
    """Reading each capsule's provenance costs a request, so it is skipped where nothing depends on it."""
    pipeline_config = {"pipelines": {"lfp": {"params": ["default"]}}}
    code_dir_paths = ["derivatives/dandiset-000409/sub-mouse01/pipeline-lfp/job-240101abc123/code"]

    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue._load_pipeline_config", return_value=pipeline_config),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs", return_value=code_dir_paths
        ),
        mock.patch("dandi_compute_code.queue._pipeline_queue.read_capsule_params") as mock_read_params,
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.dispatch_pipeline_jobs", side_effect=_no_pending
        ) as mock_dispatch,
    ):
        PipelineQueue.dispatch_jobs(base_directory=base_directory, jitter_seconds=0.0)

    mock_read_params.assert_not_called()
    assert mock_dispatch.call_args.kwargs["code_dir_paths"] == code_dir_paths


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
    assert mock_dispatch.call_count == len(_configured_dispatcher_labels())
    lfp_calls = [call for call in mock_dispatch.call_args_list if call.kwargs["pipeline"] == "lfp"]
    assert len(lfp_calls) == 1
    assert lfp_calls[0].kwargs["code_dir_paths"] == code_dir_paths


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
