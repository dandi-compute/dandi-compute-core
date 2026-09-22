import os
import pathlib
from unittest import mock

import pytest
from testing_utilities import create_job_capsule_directory

from dandi_compute_code.queue import PipelineQueue


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_raises_without_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules raises RuntimeError when DANDI_API_KEY is not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            PipelineQueue(entries=[]).clean_unsubmitted_capsules(dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_queued_directories(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules removes capsule dirs that are queued (code, no logs, no output)."""
    dandiset_dir = tmp_path / "dandiset"
    queued_dir = create_job_capsule_directory(
        base_dir=dandiset_dir, entry=example_pipeline_queue.entry_for(dandi_path="sub-pending")
    )

    with mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    mock_run.assert_called_once_with(["dandi", "delete", str(queued_dir)], input=b"y\n", check=True)


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_output(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules does not remove capsules that already have output."""
    dandiset_dir = tmp_path / "dandiset"
    completed_dir = create_job_capsule_directory(
        base_dir=dandiset_dir,
        entry=example_pipeline_queue.entry_for(dandi_path="sub-successful"),
        with_logs=True,
        with_output=True,
    )

    with mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == []
    assert completed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_logs(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules does not remove capsules that have logs (already run)."""
    dandiset_dir = tmp_path / "dandiset"
    failed_dir = create_job_capsule_directory(
        base_dir=dandiset_dir,
        entry=example_pipeline_queue.entry_for(dandi_path="sub-failed/ses-one"),
        with_logs=True,
    )

    with mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == []
    assert failed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_ignores_dataset_description_in_logs(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """A logs/ directory holding only dataset_description.json does not protect a queued capsule."""
    dandiset_dir = tmp_path / "dandiset"
    queued_dir = create_job_capsule_directory(
        base_dir=dandiset_dir, entry=example_pipeline_queue.entry_for(dandi_path="sub-pending")
    )
    logs_dir = queued_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "dataset_description.json").write_text("{}\n")

    with mock.patch("subprocess.run"):
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_submitted_marker(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules does not remove capsules with a submitted marker file."""
    dandiset_dir = tmp_path / "dandiset"
    queued_dir = create_job_capsule_directory(
        base_dir=dandiset_dir, entry=example_pipeline_queue.entry_for(dandi_path="sub-pending"), submitted=True
    )

    with mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == []
    assert queued_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_returns_empty_list_when_nothing_queued(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules returns an empty list when nothing is materialized on disk."""
    dandiset_dir = tmp_path / "dandiset"

    removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == []


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_empty_parent_directories(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules removes empty pipeline/session dirs after last capsule removal."""
    dandiset_dir = tmp_path / "dandiset"
    queued_dir = create_job_capsule_directory(
        base_dir=dandiset_dir, entry=example_pipeline_queue.entry_for(dandi_path="sub-sole/ses-capsule")
    )
    pipeline_dir = queued_dir.parent
    session_dir = pipeline_dir.parent

    with mock.patch("subprocess.run"):
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == [queued_dir]
    assert not pipeline_dir.exists()
    assert not session_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_keeps_non_empty_parent_directories(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules keeps pipeline/version dirs when a sibling capsule remains."""
    dandiset_dir = tmp_path / "dandiset"
    queued_dir = create_job_capsule_directory(
        base_dir=dandiset_dir,
        entry=example_pipeline_queue.entry_for(dandi_path="sub-two/ses-capsules", config="cfgtwoa"),
    )
    remaining_dir = create_job_capsule_directory(
        base_dir=dandiset_dir,
        entry=example_pipeline_queue.entry_for(dandi_path="sub-two/ses-capsules", config="cfgtwob"),
        with_output=True,
    )
    pipeline_dir = queued_dir.parent

    with mock.patch("subprocess.run"):
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == [queued_dir]
    assert remaining_dir.exists()
    assert pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_prunes_empty_parents(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """Removing the sole capsule in a pipeline tree prunes the emptied parent directories."""
    dandiset_dir = tmp_path / "dandiset"
    queued_dir = create_job_capsule_directory(
        base_dir=dandiset_dir,
        entry=example_pipeline_queue.entry_for(dandi_path="sub-sole/ses-capsule"),
    )
    pipeline_dir = queued_dir.parent

    with mock.patch("subprocess.run"):
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert not pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_only_queued_not_submitted(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules only removes queued capsules, leaving submitted ones intact."""
    dandiset_dir = tmp_path / "dandiset"
    queued_dir = create_job_capsule_directory(
        base_dir=dandiset_dir, entry=example_pipeline_queue.entry_for(dandi_path="sub-pending")
    )
    submitted_dir = create_job_capsule_directory(
        base_dir=dandiset_dir,
        entry=example_pipeline_queue.entry_for(dandi_path="sub-already/ses-submitted"),
        submitted=True,
    )

    with mock.patch("subprocess.run"):
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert submitted_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removed_entry_via_fallback_capsule_resolution(
    example_pipeline_queue: PipelineQueue, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """clean_unsubmitted_capsules removes queued entry when dandi_path differs from the on-disk path."""
    dandiset_dir = tmp_path / "dandiset"

    # The "sourcedata" entry's on-disk capsule lives under sub-mouse01, so it must be
    # located via fallback resolution rather than the recorded dandi_path.
    capsule_dir = (
        dandiset_dir
        / "derivatives"
        / "dandisets-001"
        / "dandiset-001849"
        / "sub-mouse01"
        / "pipeline-aind+ephys"
        / "job-240101aa0009"
    )
    (capsule_dir / "code").mkdir(parents=True)
    (capsule_dir / "code" / "submit.sh").write_text("#!/bin/bash\necho hello\n")

    with mock.patch("subprocess.run") as mock_run:
        removed = example_pipeline_queue.clean_unsubmitted_capsules(dandiset_directory=dandiset_dir)

    assert removed == [capsule_dir]
    assert not capsule_dir.exists()
    mock_run.assert_called_once_with(["dandi", "delete", str(capsule_dir)], input=b"y\n", check=True)
