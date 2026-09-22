import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import PipelineQueue

# These exercise process_queue through the real array dispatcher, with only the two cluster
# calls mocked: `squeue` (is a pipeline's dispatcher still working through its array?) and
# `sbatch` (submit the array). The pending capsules stand in for a metadata read.

_AIND_CODE_DIR_PATHS = [
    "derivatives/dandiset-000409/sub-mouse01/pipeline-aind+ephys/job-240101abc123/code",
    "derivatives/dandiset-000409/sub-mouse02/pipeline-aind+ephys/job-240101abc124/code",
]
_LFP_CODE_DIR_PATHS = ["derivatives/dandiset-000409/sub-mouse01/pipeline-lfp/job-240101abc125/code"]


def _cluster_calls(*, active_dispatcher_job_names: set[str] = frozenset()):
    """A subprocess.run replacement reporting the named dispatchers as still active."""

    def run(command: list[str], **_: object) -> mock.MagicMock:
        result = mock.MagicMock()
        result.returncode = 0
        result.stderr = ""
        if command[0] == "squeue":
            job_name = command[command.index("--name") + 1]
            result.stdout = "9001\n" if job_name in active_dispatcher_job_names else ""
        else:
            result.stdout = "Submitted batch job 4242\n"
        return result

    return run


@pytest.mark.ai_generated
def test_process_queue_dispatches_one_array_per_pipeline(processing_directory: pathlib.Path) -> None:
    """Each pipeline's pending capsules go out as that pipeline's own single array job."""
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs",
            return_value=[*_AIND_CODE_DIR_PATHS, *_LFP_CODE_DIR_PATHS],
        ),
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_cluster_calls()),
    ):
        results = PipelineQueue.process_queue(processing_directory=processing_directory, jitter_seconds=0)

    assert results["aind+ephys"].status == "dispatched"
    assert results["aind+ephys"].task_count == 2
    assert results["lfp"].status == "dispatched"
    assert results["lfp"].task_count == 1


@pytest.mark.ai_generated
def test_process_queue_leaves_a_live_dispatcher_to_exhaust_its_array(
    processing_directory: pathlib.Path,
) -> None:
    """A pipeline whose dispatcher is still active is skipped, while the others still dispatch."""
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs",
            return_value=[*_AIND_CODE_DIR_PATHS, *_LFP_CODE_DIR_PATHS],
        ),
        mock.patch(
            "dandi_compute_code.queue._dispatch.subprocess.run",
            side_effect=_cluster_calls(active_dispatcher_job_names={"dandicompute-dispatch-aind-ephys"}),
        ),
    ):
        results = PipelineQueue.process_queue(processing_directory=processing_directory, jitter_seconds=0)

    assert results["aind+ephys"].status == "dispatcher-active"
    assert results["lfp"].status == "dispatched"


@pytest.mark.ai_generated
def test_process_queue_does_not_dispatch_a_pipeline_without_pending_capsules(
    processing_directory: pathlib.Path,
) -> None:
    """A pipeline with nothing waiting gets no array of its own."""
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs",
            return_value=_LFP_CODE_DIR_PATHS,
        ),
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_cluster_calls()),
    ):
        results = PipelineQueue.process_queue(processing_directory=processing_directory, jitter_seconds=0)

    assert results["aind+ephys"].status == "no-pending"
    assert results["lfp"].status == "dispatched"


@pytest.mark.ai_generated
def test_process_queue_throttles_each_array_to_the_configured_limit(
    processing_directory: pathlib.Path,
) -> None:
    """The configured per-pipeline limit reaches SLURM as the array's concurrency throttle."""
    configured_limit = PipelineQueue.load_pipeline_config()["pipelines"]["aind+ephys"]["dispatch"]["max_concurrent"]

    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.PipelineQueue.pending_code_dirs",
            return_value=_AIND_CODE_DIR_PATHS,
        ),
        mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_cluster_calls()),
    ):
        results = PipelineQueue.process_queue(processing_directory=processing_directory, jitter_seconds=0)

    script = (results["aind+ephys"].dispatch_directory / "dispatch-1.sh").read_text()
    assert f"#SBATCH --array=1-2%{configured_limit}" in script
