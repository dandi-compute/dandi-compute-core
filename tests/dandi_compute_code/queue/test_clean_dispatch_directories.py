import datetime
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import clean_dispatch_directories

# A dispatch directory holds the working trees of its array tasks, so removing one while its
# array is still working would pull them out from under every running task. The
# `squeue` call that rules that out cannot run in CI and is mocked here.


def _make_dispatch_directory(
    *,
    base_directory: pathlib.Path,
    job_name: str = "dandicompute-dispatch-lfp",
    age_hours: float = 48.0,
) -> pathlib.Path:
    """Create a dispatch directory named as though it were formed *age_hours* ago."""
    formed_at = datetime.datetime.now() - datetime.timedelta(hours=age_hours)
    directory = base_directory / "processing" / f"{job_name}-{formed_at.strftime('%Y%m%d-%H%M%S')}"
    (directory / "task-1-1").mkdir(parents=True)
    return directory


def _mock_squeue(*, active_job_names: set[str] = frozenset()):
    """A subprocess.run replacement reporting the named dispatchers as still on the cluster."""

    def run(command: list[str], **_: object) -> mock.MagicMock:
        job_name = command[command.index("--name") + 1]
        return mock.MagicMock(
            returncode=0,
            stderr="",
            stdout="9001\n" if job_name in active_job_names else "",
        )

    return run


@pytest.mark.ai_generated
def test_clean_removes_a_finished_dispatch_directory(base_directory: pathlib.Path) -> None:
    """An old directory whose dispatcher has left the cluster is removed outright."""
    directory = _make_dispatch_directory(base_directory=base_directory)

    with mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_mock_squeue()):
        removed = clean_dispatch_directories(base_directory=base_directory)

    assert removed == [directory]
    assert directory.exists() is False


@pytest.mark.ai_generated
def test_clean_keeps_a_directory_whose_dispatcher_is_still_active(
    base_directory: pathlib.Path,
) -> None:
    """
    A live dispatcher still needs its manifest.

    Its array tasks read the manifest as each one starts, so removing the directory would
    strand every task that had not begun yet.
    """
    directory = _make_dispatch_directory(base_directory=base_directory)

    with mock.patch(
        "dandi_compute_code.queue._dispatch.subprocess.run",
        side_effect=_mock_squeue(active_job_names={"dandicompute-dispatch-lfp"}),
    ):
        removed = clean_dispatch_directories(base_directory=base_directory)

    assert removed == []
    assert directory.is_dir()


@pytest.mark.ai_generated
@pytest.mark.parametrize(("age_hours", "expected_removal"), [(1.0, False), (48.0, True)])
def test_clean_keeps_a_directory_younger_than_the_age_floor(
    base_directory: pathlib.Path, age_hours: float, expected_removal: bool
) -> None:
    """The age floor covers the window between submitting an array and SLURM reporting it."""
    directory = _make_dispatch_directory(base_directory=base_directory, age_hours=age_hours)

    with mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_mock_squeue()):
        removed = clean_dispatch_directories(base_directory=base_directory, minimum_age_hours=24.0)

    assert (removed == [directory]) is expected_removal
    assert directory.is_dir() is not expected_removal


@pytest.mark.ai_generated
def test_clean_only_removes_the_pipelines_whose_dispatchers_have_finished(
    base_directory: pathlib.Path,
) -> None:
    """One pipeline still working does not hold up another pipeline's cleanup."""
    live = _make_dispatch_directory(base_directory=base_directory, job_name="dandicompute-dispatch-aind-ephys")
    finished = _make_dispatch_directory(base_directory=base_directory, job_name="dandicompute-dispatch-lfp")

    with mock.patch(
        "dandi_compute_code.queue._dispatch.subprocess.run",
        side_effect=_mock_squeue(active_job_names={"dandicompute-dispatch-aind-ephys"}),
    ):
        removed = clean_dispatch_directories(base_directory=base_directory)

    assert removed == [finished]
    assert live.is_dir()


@pytest.mark.ai_generated
def test_clean_leaves_anything_that_is_not_a_dispatch_directory(
    base_directory: pathlib.Path,
) -> None:
    """The processing directory may hold other things, and none of them are ours to remove."""
    unrelated_directory = base_directory / "processing" / "some-other-work"
    unrelated_directory.mkdir()
    unrelated_file = base_directory / "processing" / "notes.txt"
    unrelated_file.write_text("keep me")

    with mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_mock_squeue()) as mock_run:
        removed = clean_dispatch_directories(base_directory=base_directory)

    assert removed == []
    assert unrelated_directory.is_dir()
    assert unrelated_file.is_file()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_asks_squeue_once_per_dispatcher(base_directory: pathlib.Path) -> None:
    """Several directories of one pipeline cost a single liveness check between them."""
    for age_hours in (48.0, 72.0, 96.0):
        _make_dispatch_directory(base_directory=base_directory, age_hours=age_hours)

    with mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_mock_squeue()) as mock_run:
        removed = clean_dispatch_directories(base_directory=base_directory)

    assert len(removed) == 3
    assert mock_run.call_count == 1


@pytest.mark.ai_generated
def test_clean_raises_for_a_base_directory_without_processing(tmp_path: pathlib.Path) -> None:
    """A base directory without a processing directory is an error rather than a silent no-op."""
    with pytest.raises(NotADirectoryError, match="does not exist or is not a directory"):
        clean_dispatch_directories(base_directory=tmp_path / "nope")


@pytest.mark.ai_generated
def test_clean_keeps_the_central_log_directory(base_directory: pathlib.Path) -> None:
    """Manifests, scripts and array output are the record of past dispatches, so they outlive cleaning."""
    directory = _make_dispatch_directory(base_directory=base_directory)
    pipeline_log_directory = base_directory / "processing" / "derivatives" / "logs" / "dandicompute-dispatch-lfp"
    pipeline_log_directory.mkdir(parents=True)
    manifest_file_path = (
        pipeline_log_directory / f"{directory.name.removeprefix('dandicompute-dispatch-lfp-')}-manifest-1.txt"
    )
    manifest_file_path.write_text("some/capsule/code\n")

    with mock.patch("dandi_compute_code.queue._dispatch.subprocess.run", side_effect=_mock_squeue()):
        removed = clean_dispatch_directories(base_directory=base_directory)

    assert removed == [directory]
    assert manifest_file_path.read_text() == "some/capsule/code\n"
