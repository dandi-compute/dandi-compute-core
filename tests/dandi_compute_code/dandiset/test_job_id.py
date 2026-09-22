"""Tests for the job ID naming helpers behind a job capsule directory name."""

import datetime

import pytest

from dandi_compute_code.dandiset._job_id import (
    _capsule_names_from_asset_paths,
    _find_existing_capsule_path,
    _next_available_job_id,
)

_PIPELINE_PATH = "derivatives/dandisets-000/dandiset-000409/sub-01/sub-01_ecephys/pipeline-lfp"
_DATE = datetime.date(2026, 9, 22)


@pytest.mark.ai_generated
def test_capsule_names_are_read_off_asset_paths() -> None:
    """Every distinct capsule directory directly under the pipeline path is reported once."""
    asset_paths = [
        f"{_PIPELINE_PATH}/job-260922a1b2c3/code/submit.sh",
        f"{_PIPELINE_PATH}/job-260922a1b2c3/dataset_description.json",
        f"{_PIPELINE_PATH}/job-260922a1b2c3-2/code/submit.sh",
    ]

    capsule_names = _capsule_names_from_asset_paths(
        asset_paths=asset_paths, pipeline_dandiset_path=_PIPELINE_PATH
    )

    assert capsule_names == {"job-260922a1b2c3", "job-260922a1b2c3-2"}


@pytest.mark.ai_generated
def test_find_existing_capsule_matches_an_earlier_date() -> None:
    """A capsule formed on an earlier date is recognised by its job hash."""
    found = _find_existing_capsule_path(
        capsule_names={"job-200101a1b2c3"},
        pipeline_dandiset_path=_PIPELINE_PATH,
        job_hash="a1b2c3",
    )

    assert found == f"{_PIPELINE_PATH}/job-200101a1b2c3"


@pytest.mark.ai_generated
def test_find_existing_capsule_ignores_another_job() -> None:
    """A capsule for a different job is not mistaken for this one."""
    found = _find_existing_capsule_path(
        capsule_names={"job-200101ffffff"},
        pipeline_dandiset_path=_PIPELINE_PATH,
        job_hash="a1b2c3",
    )

    assert found is None


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("taken_job_ids", "expected_job_id"),
    [
        pytest.param(set(), "job-260922a1b2c3", id="first_capsule_carries_no_counter"),
        pytest.param({"job-260922a1b2c3"}, "job-260922a1b2c3-2", id="second_capsule_of_the_day"),
        pytest.param({"job-260922a1b2c3", "job-260922a1b2c3-2"}, "job-260922a1b2c3-3", id="third_capsule_of_the_day"),
        pytest.param({"job-200101a1b2c3"}, "job-260922a1b2c3", id="earlier_date_does_not_collide"),
    ],
)
def test_next_available_job_id_disambiguates_within_a_day(taken_job_ids: set[str], expected_job_id: str) -> None:
    """Forming the same job again on one day appends a counter to tell the capsules apart."""
    job_id = _next_available_job_id(job_hash="a1b2c3", taken_job_ids=taken_job_ids, date=_DATE)

    assert job_id == expected_job_id
