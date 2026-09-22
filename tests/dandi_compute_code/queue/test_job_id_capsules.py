"""
Tests for the ``job-{YYMMDD}{hash}`` job capsule layout.

A job capsule directory name carries only the job ID, so the pipeline version, codebase
version, parameters and config are read back from the provenance block written into the
capsule's ``dataset_description.json``.
"""

import datetime
from unittest import mock

import pytest

import dandi_compute_code.queue._queue_utils
from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.dandiset._job_id import _format_job_id, _parse_job_hash
from dandi_compute_code.queue import PipelineQueue

_JOB_ID = "job-240101a1b2c3"
_CAPSULE_PATH = f"derivatives/dandisets-001/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/{_JOB_ID}"
_SOURCE_PATH = "sub-mouse01/sub-mouse01_ecephys.nwb"

_PROVENANCE = {
    "job_id": _JOB_ID,
    "pipeline": "test",
    "version": "v1.1.0",
    "codebase": "v0.4.0",
    "params": "abc1234",
    "config": "def5678",
}


def _capsule_metadata() -> AssetsJsonldMetadata:
    paths = [f"{_CAPSULE_PATH}/code/submit.sh", f"{_CAPSULE_PATH}/dataset_description.json"]
    path_to_asset_metadata = {
        path: AssetMetadata(
            path=path,
            date_modified="2024-01-01T00:00:00+00:00",
            content_size=1,
            content_id=f"content-{index}",
        )
        for index, path in enumerate(paths)
    }
    content_id_to_asset = {
        f"content-{index}": {"path": path, "contentUrl": [f"https://example.org/blobs/content-{index}"]}
        for index, path in enumerate(paths)
    }
    return AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset,
        path_to_asset_metadata=path_to_asset_metadata,
    )


def _source_metadata() -> AssetsJsonldMetadata:
    return AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            _SOURCE_PATH: AssetMetadata(
                path=_SOURCE_PATH,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=512,
                content_id="source-asset",
            )
        },
    )


def _build_state(*, dataset_description: dict) -> PipelineQueue:
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=_capsule_metadata(),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=_source_metadata(),
        ),
        mock.patch.object(
            dandi_compute_code.queue._queue_utils,
            "_read_asset_json",
            return_value=dataset_description,
        ),
    ):
        state = PipelineQueue.from_dandi()
    return state


@pytest.mark.ai_generated
def test_from_dandi_reads_identity_from_capsule_provenance() -> None:
    """A job-ID capsule takes its version, codebase, params and config from its provenance."""
    state = _build_state(dataset_description={"Name": "example", "DandiCompute": _PROVENANCE})

    assert len(state) == 1
    entry = state.entries[0]
    assert entry.job.job_id == _JOB_ID
    assert entry.job.dandiset_id == "001697"
    assert entry.job.dandi_path == _SOURCE_PATH
    assert entry.job.pipeline == "test"
    assert entry.job.version == "v1.1.0"
    assert entry.job.codebase == "v0.4.0"
    assert entry.job.params == "abc1234"
    assert entry.job.config == "def5678"
    assert entry.content_id == "source-asset"
    assert entry.asset_size_bytes == 512
    assert entry.status == "pending"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "dataset_description",
    [
        pytest.param({}, id="no_dataset_description"),
        pytest.param({"Name": "example"}, id="no_provenance_block"),
        pytest.param({"DandiCompute": "not-a-mapping"}, id="malformed_provenance_block"),
    ],
)
def test_from_dandi_keeps_capsule_without_provenance(dataset_description: dict) -> None:
    """A job-ID capsule with unreadable provenance is still reported, with blank identity fields."""
    state = _build_state(dataset_description=dataset_description)

    assert len(state) == 1
    entry = state.entries[0]
    assert entry.job.job_id == _JOB_ID
    assert entry.job.version == ""
    assert entry.job.codebase == ""
    assert entry.job.params == ""
    assert entry.job.config == ""


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("job_id", "expected_hash"),
    [
        pytest.param("job-240101a1b2c3", "a1b2c3", id="no_counter"),
        pytest.param("job-240101a1b2c3-2", "a1b2c3", id="counter"),
        pytest.param("job-240101a1b2c3-12", "a1b2c3", id="two_digit_counter"),
        pytest.param("job-240101a1b2c3-1", None, id="counter_one_is_not_a_spelling"),
        pytest.param("job-240101a1b2c3-0", None, id="counter_zero_is_not_a_spelling"),
        pytest.param("version-v1.1.0_params-abc1234", None, id="legacy_name"),
    ],
)
def test_job_hash_reads_through_the_counter(job_id: str, expected_hash: str | None) -> None:
    """
    Capsules of one job prepared on one day are told apart by a counter, and still read back as
    the same job, so preparation recognises one of them as the job already being formed.

    The first capsule carries no counter, so ``-1`` and ``-0`` are not job IDs at all: each
    capsule has exactly one spelling.
    """
    assert _parse_job_hash(job_id) == expected_hash


@pytest.mark.ai_generated
def test_formatting_round_trips_through_the_counter() -> None:
    """Every index a migration can assign produces a name the package reads back."""
    date = datetime.date(2026, 9, 16)

    job_ids = [_format_job_id(job_hash="a1b2c3", date=date, index=index) for index in range(1, 13)]

    assert job_ids[0] == "job-260916a1b2c3"
    assert job_ids[1] == "job-260916a1b2c3-2"
    assert len(set(job_ids)) == len(job_ids)
    assert all(_parse_job_hash(job_id) == "a1b2c3" for job_id in job_ids)
