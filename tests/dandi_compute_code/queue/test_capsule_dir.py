import pathlib

import pytest

from dandi_compute_code.queue import JobCapsule

_JOB_ID = "job-240101a1b2c3"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dandi_path", "relative_prefix"),
    [
        ("sub-mouse01", pathlib.Path("derivatives/dandisets-000/dandiset-000001/sub-mouse01/pipeline-test")),
        (
            "sub-mouse01/ses-01",
            pathlib.Path("derivatives/dandisets-000/dandiset-000001/sub-mouse01/ses-01/pipeline-test"),
        ),
        (
            "sourcedata/aind-sample.nwb",
            pathlib.Path("derivatives/dandisets-000/dandiset-000001/sourcedata/aind-sample/pipeline-test"),
        ),
    ],
)
def test_capsule_dir_is_the_job_id_under_the_pipeline_directory(
    dandi_path: str,
    relative_prefix: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """JobCapsule.capsule_dir names the capsule by its job ID alone."""
    entry = {
        "job_id": _JOB_ID,
        "dandiset_id": "000001",
        "dandi_path": dandi_path,
        "pipeline": "test",
        "version": "v1.0",
        "params": "abc1234",
        "config": "def5678",
        "codebase": "v0.3.0",
    }

    capsule_dir = JobCapsule.from_dict(entry).capsule_dir(tmp_path)

    assert capsule_dir == tmp_path / relative_prefix / _JOB_ID


@pytest.mark.ai_generated
def test_capsule_dir_name_carries_no_identity_entities(tmp_path: pathlib.Path) -> None:
    """The directory name spells out nothing beyond the job ID."""
    entry = {
        "job_id": _JOB_ID,
        "dandiset_id": "000001",
        "dandi_path": "sub-mouse01",
        "pipeline": "test",
        "version": "v1.1.1",
        "params": "4af6a25",
        "config": "0d4bf36",
        "codebase": "v0.3.17",
    }

    capsule_dir_name = JobCapsule.from_dict(entry).capsule_dir(tmp_path).name

    assert capsule_dir_name == _JOB_ID
    for entity in ("version-", "_codebase-", "_params-", "_config-", "_attempt-"):
        assert entity not in capsule_dir_name


@pytest.mark.ai_generated
def test_capsule_path_mirrors_capsule_dir(tmp_path: pathlib.Path) -> None:
    """capsule_path is the POSIX-string counterpart of capsule_dir."""
    entry = {
        "job_id": _JOB_ID,
        "dandiset_id": "000001",
        "dandi_path": "sub-mouse01/ses-01",
        "pipeline": "test",
        "version": "v1.0",
        "params": "abc1234",
        "config": "def5678",
        "codebase": "v0.3.0",
    }
    job_capsule = JobCapsule.from_dict(entry)

    assert job_capsule.capsule_path() == job_capsule.capsule_dir(tmp_path).relative_to(tmp_path).as_posix()


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("entry", "expected_exception", "expected_message"),
    [
        (
            {
                "job_id": _JOB_ID,
                "dandiset_id": "000001",
                "subject": "mouse01",
                "session": "01",
                "pipeline": "test",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "codebase": "v0.3.0",
            },
            # A JobCapsule always carries dandi_path, so a missing key fails at construction.
            KeyError,
            r"dandi_path",
        ),
        (
            {
                "job_id": _JOB_ID,
                "dandiset_id": "000001",
                "dandi_path": "",
                "pipeline": "test",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "codebase": "v0.3.0",
            },
            ValueError,
            r"Entry has invalid dandi_path field \(empty\)",
        ),
    ],
)
def test_capsule_dir_requires_valid_dandi_path(
    entry: dict,
    expected_exception: type[Exception],
    expected_message: str,
    tmp_path: pathlib.Path,
) -> None:
    """JobCapsule.capsule_dir requires a valid dandi_path value."""
    with pytest.raises(expected_exception, match=expected_message):
        JobCapsule.from_dict(entry).capsule_dir(tmp_path)
