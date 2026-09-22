import gzip
import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.jobs import create_job_capsules
from dandi_compute_code.queue import JobEntry, JobInfo, QueueState

_MODULE = "dandi_compute_code.jobs._create_job_capsules"

_TEST_QUEUE_CONFIG = {"pipelines": {"test": {"params": ["default"]}}}
_LFP_QUEUE_CONFIG = {"pipelines": {"lfp": {"params": ["default"]}}}
_MULTI_QUEUE_CONFIG = {"pipelines": {"aind+ephys": {"params": ["default"]}, "lfp": {"params": ["default"]}}}


def _mock_urlopen_response(qualifying_content_ids: list[str]) -> mock.MagicMock:
    """Build a mock response matching the real ``{content_id: qualifies}``-per-line JSONL format."""
    mock_response = mock.MagicMock()
    jsonl = "\n".join(json.dumps({content_id: True}) for content_id in qualifying_content_ids)
    mock_response.read.return_value = gzip.compress(jsonl.encode())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False
    return mock_response


def _queue_state_with_capsule(*, content_id: str, params: str, config: str, version: str) -> QueueState:
    """A live queue state holding one job capsule for *content_id*."""
    job = JobInfo(
        job_id="job-240101aaaaaa",
        dandiset_id="000001",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        pipeline="test",
        version=version,
        params=params,
        config=config,
        codebase="v0.1.0",
    )
    entry = JobEntry(job=job, content_id=content_id, asset_size_bytes=1024, has_code=True)
    return QueueState(entries=[entry])


@pytest.mark.ai_generated
def test_creates_a_capsule_for_each_qualifying_asset() -> None:
    """A capsule is formed for every qualifying content ID that does not have one."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(["asset-bbb", "asset-ccc"])
        created_count = create_job_capsules()

    assert created_count == 2
    prepared_ids = {call.kwargs["content_id"] for call in mock_prepare.call_args_list}
    assert prepared_ids == {"asset-bbb", "asset-ccc"}


@pytest.mark.ai_generated
def test_forms_capsules_against_the_latest_pipeline_version(mock_latest_pipeline_version: mock.MagicMock) -> None:
    """The version handed to the job builder is the latest one available locally."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        create_job_capsules(content_ids=["asset-bbb"])

    assert mock_prepare.call_args.kwargs["pipeline_version"] == mock_latest_pipeline_version.return_value


@pytest.mark.ai_generated
def test_skips_an_asset_that_already_has_a_capsule(mock_latest_pipeline_version: mock.MagicMock) -> None:
    """An asset with a capsule for this params and config combination is left alone."""
    state = _queue_state_with_capsule(
        content_id="asset-bbb", params="default", config="default", version=mock_latest_pipeline_version.return_value
    )

    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.QueueState.from_dandi", return_value=state),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        created_count = create_job_capsules(content_ids=["asset-bbb", "asset-zzz"])

    assert created_count == 1
    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-zzz"]


@pytest.mark.ai_generated
def test_skips_an_existing_capsule_formed_against_an_older_version() -> None:
    """Existence does not depend on the version a capsule was formed against."""
    state = _queue_state_with_capsule(content_id="asset-bbb", params="default", config="default", version="v0.0.1")

    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.QueueState.from_dandi", return_value=state),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        created_count = create_job_capsules(content_ids=["asset-bbb"])

    assert created_count == 0
    assert mock_prepare.call_count == 0


@pytest.mark.ai_generated
def test_creates_a_capsule_for_different_params_on_the_same_asset(
    mock_latest_pipeline_version: mock.MagicMock,
) -> None:
    """A capsule for other parameters does not stand in for the requested combination."""
    state = _queue_state_with_capsule(
        content_id="asset-bbb", params="other", config="default", version=mock_latest_pipeline_version.return_value
    )

    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.QueueState.from_dandi", return_value=state),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        created_count = create_job_capsules(content_ids=["asset-bbb"])

    assert created_count == 1
    assert mock_prepare.call_count == 1


@pytest.mark.ai_generated
def test_latest_versions_forms_a_capsule_even_where_one_exists(mock_latest_pipeline_version: mock.MagicMock) -> None:
    """--latest bypasses the existence check and forces a new capsule."""
    state = _queue_state_with_capsule(
        content_id="asset-bbb", params="default", config="default", version=mock_latest_pipeline_version.return_value
    )

    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.QueueState.from_dandi", return_value=state) as mock_from_dandi,
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        created_count = create_job_capsules(content_ids=["asset-bbb"], force_latest_versions=True)

    assert created_count == 1
    assert mock_from_dandi.call_count == 0
    assert mock_prepare.call_args.kwargs["force_new_capsule"] is True


@pytest.mark.ai_generated
def test_does_not_force_a_new_capsule_by_default() -> None:
    """Without --latest the job builder keeps its own skip-if-it-exists behaviour."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        create_job_capsules(content_ids=["asset-bbb"])

    assert mock_prepare.call_args.kwargs["force_new_capsule"] is False


@pytest.mark.ai_generated
def test_limit_stops_after_n_assets() -> None:
    """Creation stops after exactly limit capsules when a limit is set."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        created_count = create_job_capsules(content_ids=["asset-aaa", "asset-bbb", "asset-ccc"], limit=2)

    assert mock_prepare.call_count == 2
    assert created_count == 2


@pytest.mark.ai_generated
def test_no_limit_creates_every_capsule() -> None:
    """A limit of None means every qualifying asset without a capsule gets one."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        created_count = create_job_capsules(content_ids=["asset-aaa", "asset-bbb", "asset-ccc"], limit=None)

    assert mock_prepare.call_count == 3
    assert created_count == 3


@pytest.mark.ai_generated
def test_capsule_that_already_existed_is_not_counted_or_limited() -> None:
    """A None from the job builder (capsule already on the archive) is not counted against the limit."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_prepare.side_effect = [None, pathlib.Path("submit.sh"), None]
        created_count = create_job_capsules(content_ids=["asset-bbb", "asset-ccc", "asset-ddd"], limit=1)

    assert mock_prepare.call_count == 2
    assert created_count == 1


@pytest.mark.ai_generated
def test_limit_samples_uniformly_over_dandisets() -> None:
    """Qualifying assets are interleaved so --limit is not biased by asset-rich Dandisets."""
    content_id_mapping = {
        "asset-a1": {"000001": "sub-a1/sub-a1_ecephys.nwb"},
        "asset-a2": {"000001": "sub-a2/sub-a2_ecephys.nwb"},
        "asset-b1": {"000002": "sub-b1/sub-b1_ecephys.nwb"},
    }

    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value=content_id_mapping,
        ),
        mock.patch("dandi_compute_code.queue._queue_utils.random.shuffle", side_effect=lambda items: None),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(["asset-a1", "asset-a2", "asset-b1"])
        create_job_capsules(limit=2)

    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-a1", "asset-b1"]


@pytest.mark.ai_generated
def test_excludes_non_qualifying_content_ids() -> None:
    """Only content IDs whose remote cache entry is True are given a capsule."""
    mock_response = mock.MagicMock()
    jsonl = "\n".join(
        json.dumps({content_id: qualifies}) for content_id, qualifies in [("asset-bbb", True), ("asset-ccc", False)]
    )
    mock_response.read.return_value = gzip.compress(jsonl.encode())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False

    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen", return_value=mock_response),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        create_job_capsules()

    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["content_id"] == "asset-bbb"


@pytest.mark.ai_generated
def test_explicit_content_ids_skip_the_network_fetch() -> None:
    """Provided content IDs are used directly instead of the qualifying-asset download."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        create_job_capsules(content_ids=["explicit-asset-001"])

    mock_urlopen.assert_not_called()
    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["content_id"] == "explicit-asset-001"


@pytest.mark.ai_generated
def test_passes_optional_args_through(tmp_path: pathlib.Path) -> None:
    """The pipeline directory and config key reach the job builder."""
    fake_pipeline_dir = tmp_path / "pipeline"
    fake_pipeline_dir.mkdir()

    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_prepare,
    ):
        create_job_capsules(
            content_ids=["asset-bbb"],
            pipeline_directory=fake_pipeline_dir,
            config_key="v1",
        )

    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["pipeline_directory"] == fake_pipeline_dir
    assert call_kwargs["config_key"] == "v1"


@pytest.mark.ai_generated
def test_dispatches_lfp_to_the_lfp_job_builder(mock_latest_pipeline_version: mock.MagicMock) -> None:
    """The 'lfp' pipeline is routed to prepare_lfp_job, not the AIND builder."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_LFP_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_lfp_job") as mock_lfp,
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_aind,
    ):
        create_job_capsules(content_ids=["asset-1", "asset-2"])

    assert mock_aind.call_count == 0
    assert mock_lfp.call_count == 2
    assert {call.kwargs["content_id"] for call in mock_lfp.call_args_list} == {"asset-1", "asset-2"}
    assert all(call.kwargs["pipeline_version"] == mock_latest_pipeline_version.return_value for call in mock_lfp.call_args_list)


@pytest.mark.ai_generated
def test_lfp_skip_does_not_count_toward_limit() -> None:
    """A prepare_lfp_job that returns None is not counted against --limit."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_LFP_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_lfp_job", return_value=None) as mock_lfp,
    ):
        create_job_capsules(content_ids=["asset-1", "asset-2", "asset-3"], limit=2)

    assert mock_lfp.call_count == 3


@pytest.mark.ai_generated
def test_only_pipeline_creates_just_that_pipeline() -> None:
    """only_pipeline restricts creation to the named pipeline."""
    with (
        mock.patch(f"{_MODULE}._load_queue_config", return_value=_MULTI_QUEUE_CONFIG),
        mock.patch(f"{_MODULE}.prepare_lfp_job") as mock_lfp,
        mock.patch(f"{_MODULE}.prepare_aind_ephys_job") as mock_aind,
    ):
        create_job_capsules(content_ids=["asset-1"], only_pipeline="lfp")

    assert mock_aind.call_count == 0
    assert mock_lfp.call_count == 1


@pytest.mark.ai_generated
def test_only_pipeline_unknown_raises() -> None:
    """A pipeline name that is not configured raises a clear error."""
    with mock.patch(f"{_MODULE}._load_queue_config", return_value=_LFP_QUEUE_CONFIG):
        with pytest.raises(ValueError, match="is not configured"):
            create_job_capsules(content_ids=["asset-1"], only_pipeline="does-not-exist")
