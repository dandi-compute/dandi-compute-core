import os
from unittest import mock

import pytest
from testing_utilities import job_capsule_files, serve_remote_dandiset

from dandi_compute_code.queue import PipelineQueue

_JOB_CAPSULES_DANDISET_ID = "001697"


def _delete_call(capsule_path: str) -> mock._Call:
    return mock.call(
        ["dandi", "delete", f"dandi://dandi/{_JOB_CAPSULES_DANDISET_ID}/{capsule_path}/"], input=b"y\n", check=True
    )


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_raises_without_dandi_api_key() -> None:
    """clean_unsubmitted_capsules raises RuntimeError when DANDI_API_KEY is not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            PipelineQueue(entries=[]).clean_unsubmitted_capsules()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_queued_capsules(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules deletes capsules that are queued (code, no logs, no output) by URL."""
    queued_entry = example_pipeline_queue.entry_for(dandi_path="sub-pending")
    queued_path = queued_entry.capsule_path()

    with serve_remote_dandiset(job_capsule_files(entry=queued_entry)), mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules()

    assert removed == [queued_path]
    assert mock_run.call_args_list == [_delete_call(queued_path)]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dandi_path", "capsule_kwargs"),
    [
        pytest.param("sub-successful", {"with_logs": True, "with_output": True}, id="with-output"),
        pytest.param("sub-failed/ses-one", {"with_logs": True}, id="with-logs"),
        pytest.param("sub-pending", {"submitted": True}, id="with-submitted-marker"),
    ],
)
def test_clean_unsubmitted_capsules_skips_capsules_that_are_not_queued(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None, dandi_path: str, capsule_kwargs: dict
) -> None:
    """Capsules with output, logs, or a submitted marker are left on the archive."""
    files = job_capsule_files(entry=example_pipeline_queue.entry_for(dandi_path=dandi_path), **capsule_kwargs)

    with serve_remote_dandiset(files), mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules()

    assert removed == []
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_ignores_dataset_description_in_logs(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """A logs/ directory holding only dataset_description.json does not protect a queued capsule."""
    queued_entry = example_pipeline_queue.entry_for(dandi_path="sub-pending")
    queued_path = queued_entry.capsule_path()
    files = job_capsule_files(entry=queued_entry) | {f"{queued_path}/logs/dataset_description.json": "{}\n"}

    with serve_remote_dandiset(files), mock.patch("subprocess.run"):
        removed = example_pipeline_queue.clean_unsubmitted_capsules()

    assert removed == [queued_path]


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_returns_empty_list_when_nothing_is_on_the_archive(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules returns an empty list when no capsule has any remote assets."""
    with serve_remote_dandiset({}), mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules()

    assert removed == []
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_leaves_a_sibling_capsule_with_output(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """Only the queued capsule is deleted when a sibling under the same pipeline has output."""
    queued_entry = example_pipeline_queue.entry_for(dandi_path="sub-two/ses-capsules", config="cfgtwoa")
    remaining_entry = example_pipeline_queue.entry_for(dandi_path="sub-two/ses-capsules", config="cfgtwob")
    files = job_capsule_files(entry=queued_entry) | job_capsule_files(entry=remaining_entry, with_output=True)

    with serve_remote_dandiset(files), mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules()

    assert removed == [queued_entry.capsule_path()]
    assert mock_run.call_args_list == [_delete_call(queued_entry.capsule_path())]


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_only_queued_not_submitted(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules only deletes queued capsules, leaving submitted ones intact."""
    queued_entry = example_pipeline_queue.entry_for(dandi_path="sub-pending")
    submitted_entry = example_pipeline_queue.entry_for(dandi_path="sub-already/ses-submitted")
    files = job_capsule_files(entry=queued_entry) | job_capsule_files(entry=submitted_entry, submitted=True)

    with serve_remote_dandiset(files), mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules()

    assert removed == [queued_entry.capsule_path()]
    assert mock_run.call_args_list == [_delete_call(queued_entry.capsule_path())]


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_entry_via_fallback_capsule_resolution(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules deletes a queued entry whose dandi_path differs from its remote path."""
    # The "sourcedata" entry's remote capsule lives under sub-mouse01, so it must be
    # located via fallback resolution rather than the recorded dandi_path.
    capsule_path = "derivatives/dandisets-001/dandiset-001849/sub-mouse01/pipeline-aind+ephys/job-240101aa0009"

    with (
        serve_remote_dandiset({f"{capsule_path}/code/submit.sh": "#!/bin/bash\necho hello\n"}),
        mock.patch("subprocess.run") as mock_run,
    ):
        removed = example_pipeline_queue.clean_unsubmitted_capsules()

    assert removed == [capsule_path]
    assert mock_run.call_args_list == [_delete_call(capsule_path)]
