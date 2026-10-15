import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import JobCapsule, JobInfo, PipelineQueue

_MODULE = "dandi_compute_code.queue._pipeline_queue"
_AIND_MODULE = "dandi_compute_code.queue._aind_ephys_pipeline_queue"

_AIND_PIPELINE_CONFIG = {"pipelines": {"aind+ephys": {"params": ["default"]}}}
_MULTI_PIPELINE_CONFIG = {"pipelines": {"aind+ephys": {"params": ["default"]}, "lfp": {"params": ["default"]}}}


def _queue_with_capsule(*, content_id: str) -> PipelineQueue:
    """A live queue state holding one aind+ephys capsule (default params and config) for *content_id*."""
    job = JobInfo(
        job_id="job-240101aaaaaa",
        dandiset_id="000001",
        within_dandiset_path="sub-01/sub-01_ecephys.nwb",
        pipeline="aind+ephys",
        version="v0.0.1",
        params=PipelineQueue.resolve_params_key_to_id(pipeline="aind+ephys", params_key="default"),
        config=PipelineQueue.resolve_config_key_to_id(pipeline="aind+ephys", config_key="default"),
        codebase="v0.1.0",
    )
    return PipelineQueue(entries=[JobCapsule(job=job, content_id=content_id, asset_size_bytes=1, status="pending")])


def _creatable(
    *,
    pipeline_config: dict,
    qualifying: list[str],
    mapped: list[str],
    state: PipelineQueue | None = None,
    **kwargs,
) -> list[str]:
    with (
        mock.patch(f"{_MODULE}._load_pipeline_config", return_value=pipeline_config),
        mock.patch(f"{_MODULE}.PipelineQueue._qualifying_content_ids", return_value=qualifying),
        mock.patch(
            f"{_MODULE}._load_content_id_to_usage_dandiset_path",
            return_value={content_id: {"000001": "sub-01/sub-01_ecephys.nwb"} for content_id in mapped},
        ),
        mock.patch(f"{_MODULE}.PipelineQueue.from_dandi", return_value=state or PipelineQueue(entries=[])),
    ):
        return PipelineQueue.creatable_job_labels(**kwargs)


@pytest.mark.ai_generated
def test_lists_qualifying_mapped_assets_without_a_capsule() -> None:
    """An asset that already has a capsule, or that maps to no Dandiset path, is not creatable."""
    labels = _creatable(
        pipeline_config=_AIND_PIPELINE_CONFIG,
        qualifying=["asset-new", "asset-done", "asset-unmapped"],
        mapped=["asset-new", "asset-done"],
        state=_queue_with_capsule(content_id="asset-done"),
    )

    assert labels == ["aind+ephys/default/asset-new"]


@pytest.mark.ai_generated
def test_nothing_creatable_when_every_asset_has_a_capsule() -> None:
    """The gate stays shut when there is nothing new to form."""
    labels = _creatable(
        pipeline_config=_AIND_PIPELINE_CONFIG,
        qualifying=["asset-done"],
        mapped=["asset-done"],
        state=_queue_with_capsule(content_id="asset-done"),
    )

    assert labels == []


@pytest.mark.ai_generated
def test_only_pipeline_limits_the_check() -> None:
    """--pipeline checks a single pipeline."""
    labels = _creatable(
        pipeline_config=_MULTI_PIPELINE_CONFIG, qualifying=["asset-new"], mapped=["asset-new"], only_pipeline="lfp"
    )

    assert labels == ["lfp/default/asset-new"]


@pytest.mark.ai_generated
def test_unknown_pipeline_is_rejected() -> None:
    """An unconfigured pipeline name is an error, as it is for creation."""
    with pytest.raises(ValueError, match="not configured"):
        _creatable(pipeline_config=_AIND_PIPELINE_CONFIG, qualifying=[], mapped=[], only_pipeline="nope")


@pytest.mark.ai_generated
def test_agrees_with_what_creation_forms() -> None:
    """The check counts exactly the capsules that create_job_capsules then forms."""
    qualifying = ["asset-new-1", "asset-new-2", "asset-done", "asset-unmapped"]
    mapped = ["asset-new-1", "asset-new-2", "asset-done"]
    state = _queue_with_capsule(content_id="asset-done")
    labels = _creatable(pipeline_config=_AIND_PIPELINE_CONFIG, qualifying=qualifying, mapped=mapped, state=state)

    def prepare(**kwargs) -> pathlib.Path:
        if kwargs["content_id"] not in mapped:
            from dandi_compute_code.aind_ephys_pipeline import UnmappedContentIDError

            raise UnmappedContentIDError(kwargs["content_id"])
        return pathlib.Path("submit.sh")

    with (
        mock.patch(f"{_MODULE}._load_pipeline_config", return_value=_AIND_PIPELINE_CONFIG),
        mock.patch(f"{_MODULE}.PipelineQueue._qualifying_content_ids", return_value=qualifying),
        mock.patch(f"{_MODULE}.PipelineQueue.from_dandi", return_value=state),
        mock.patch(f"{_MODULE}.PipelineQueue.resolve_latest_pipeline_version", return_value="v9.9.9"),
        mock.patch(f"{_AIND_MODULE}.prepare_aind_ephys_job", side_effect=prepare),
    ):
        created_count = PipelineQueue.create_job_capsules()

    assert len(labels) == created_count == 2
