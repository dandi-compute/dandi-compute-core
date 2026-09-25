import os
import pathlib
from unittest import mock

import beartype.roar
import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID, _JOB_CAPSULES_DANDISET_ID
from dandi_compute_code.queue import PipelineQueue

#: For each archivable status, two distinct example entries (by within_dandiset_path)
#: that qualify for that status in the committed example queue.
_STATUS_EXAMPLE_SELECTORS = {
    "failed": [
        {"within_dandiset_path": "sub-failed/ses-one"},
        {"within_dandiset_path": "sub-failed/ses-two"},
    ],
    "pending": [
        {"within_dandiset_path": "sub-pending"},
        {"within_dandiset_path": "sub-fresh"},
    ],
    "stalled": [
        {"within_dandiset_path": "sub-stalled/ses-one"},
        {"within_dandiset_path": "sub-stalled/ses-two"},
    ],
}

_DUMMY_ASSET_METADATA = AssetMetadata(path="", date_modified="2025-01-01T00:00:00", content_size=1, content_id="dummy")


def _metadata_with_capsules_at(*capsule_paths: str) -> AssetsJsonldMetadata:
    """Build assets metadata whose paths include a ``code/submit.sh`` asset under each capsule path."""
    path_to_asset_metadata = {f"{capsule_path}/code/submit.sh": _DUMMY_ASSET_METADATA for capsule_path in capsule_paths}
    return AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=path_to_asset_metadata)


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending", "stalled"])
def test_archive_capsules_raises_without_dandi_api_key(status: str) -> None:
    """archive_capsules raises RuntimeError when DANDI_API_KEY is not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            PipelineQueue(entries=[]).archive_capsules(status=status)


@pytest.mark.ai_generated
def test_archive_capsules_raises_on_unknown_status(dandi_api_key: None) -> None:
    """archive_capsules rejects a status other than 'failed'/'pending'/'stalled' before doing anything."""
    with pytest.raises(beartype.roar.BeartypeCallHintParamViolation):
        PipelineQueue(entries=[]).archive_capsules(status="successful")


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending", "stalled"])
def test_archive_capsules_returns_empty_list_when_nothing_matches(status: str, dandi_api_key: None) -> None:
    """archive_capsules returns an empty list, without even fetching remote metadata, when nothing matches."""
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.move_job_capsule") as mock_move,
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata") as mock_load_metadata,
    ):
        archived = PipelineQueue(entries=[]).archive_capsules(status=status)

    assert archived == []
    mock_move.assert_not_called()
    mock_load_metadata.assert_not_called()


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending", "stalled"])
def test_archive_capsules_moves_every_matching_entry(
    status: str, example_pipeline_queue: PipelineQueue, base_directory: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_capsules calls move_job_capsule once per matching entry, with its resolved capsule path."""
    selectors = _STATUS_EXAMPLE_SELECTORS[status]
    entries = [example_pipeline_queue.entry_for(**selector) for selector in selectors]
    matching_state = PipelineQueue(entries=entries)
    expected_paths = [entry.capsule_path() for entry in entries]
    metadata = _metadata_with_capsules_at(*expected_paths)

    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.move_job_capsule") as mock_move,
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
    ):
        archived = matching_state.archive_capsules(status=status, base_directory=base_directory)

    assert archived == expected_paths
    assert mock_move.call_count == len(expected_paths)
    for expected_path in expected_paths:
        mock_move.assert_any_call(
            capsule_path=expected_path,
            source_dandiset_id=_JOB_CAPSULES_DANDISET_ID,
            target_dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
            base_directory=base_directory,
            test=False,
        )


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending", "stalled"])
def test_archive_capsules_ignores_non_matching_entries(
    status: str, example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """archive_capsules does not move capsules that don't have the requested status."""
    non_matching_state = PipelineQueue(
        entries=[entry for entry in example_pipeline_queue if entry not in getattr(example_pipeline_queue, status)]
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.move_job_capsule") as mock_move,
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata") as mock_load_metadata,
    ):
        archived = non_matching_state.archive_capsules(status=status)

    assert archived == []
    mock_move.assert_not_called()
    mock_load_metadata.assert_not_called()


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending", "stalled"])
def test_archive_capsules_forwards_base_directory_and_test_flag(
    status: str, example_pipeline_queue: PipelineQueue, base_directory: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_capsules forwards base_directory and test through to move_job_capsule."""
    single_entry = example_pipeline_queue.entry_for(**_STATUS_EXAMPLE_SELECTORS[status][0])
    single_matching_state = PipelineQueue(entries=[single_entry])
    expected_path = single_entry.capsule_path()
    metadata = _metadata_with_capsules_at(expected_path)

    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.move_job_capsule") as mock_move,
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
    ):
        single_matching_state.archive_capsules(status=status, base_directory=base_directory, test=True)

    mock_move.assert_called_once_with(
        capsule_path=expected_path,
        source_dandiset_id=_JOB_CAPSULES_DANDISET_ID,
        target_dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        base_directory=base_directory,
        test=True,
    )


@pytest.mark.ai_generated
def test_archive_capsules_forwards_dandiset_ids(
    example_pipeline_queue: PipelineQueue, base_directory: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_capsules forwards custom dandiset IDs to load_assets_jsonld_metadata and move_job_capsule."""
    single_entry = example_pipeline_queue.entry_for(**_STATUS_EXAMPLE_SELECTORS["failed"][0])
    single_matching_state = PipelineQueue(entries=[single_entry])
    expected_path = single_entry.capsule_path()
    metadata = _metadata_with_capsules_at(expected_path)

    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.move_job_capsule") as mock_move,
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata
        ) as mock_load_metadata,
    ):
        single_matching_state.archive_capsules(
            status="failed", dandiset_id="000123", archive_dandiset_id="000456", base_directory=base_directory
        )

    mock_load_metadata.assert_called_once_with(dandiset_id="000123")
    mock_move.assert_called_once_with(
        capsule_path=expected_path,
        source_dandiset_id="000123",
        target_dandiset_id="000456",
        base_directory=base_directory,
        test=False,
    )


@pytest.mark.ai_generated
def test_archive_capsules_requires_status_or_pipeline(dandi_api_key: None) -> None:
    """archive_capsules refuses to archive everything when neither status nor pipeline is given."""
    with pytest.raises(ValueError, match="status.*pipeline"):
        PipelineQueue(entries=[]).archive_capsules()


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("status", "pipeline", "expected_within_dandiset_paths"),
    [
        pytest.param(
            None, "aind+ephys", ["sub-sole/ses-capsule", "sourcedata/sub-test+bids"], id="pipeline_any_status"
        ),
        pytest.param(
            None,
            "test",
            [
                "sub-pending",
                "sub-successful",
                "sub-failed/ses-one",
                "sub-failed/ses-two",
                "sub-fresh",
                "sub-two/ses-capsules",
                "sub-two/ses-capsules",
                "sub-already/ses-submitted",
                "sub-stalled/ses-one",
                "sub-stalled/ses-two",
            ],
            id="pipeline_includes_successful",
        ),
        pytest.param("stalled", "test", ["sub-stalled/ses-one", "sub-stalled/ses-two"], id="pipeline_and_status"),
        pytest.param("stalled", "aind+ephys", [], id="pipeline_and_status_without_overlap"),
        pytest.param(None, "lfp", [], id="pipeline_without_capsules"),
    ],
)
def test_archive_capsules_filters_by_pipeline(
    status: str | None,
    pipeline: str,
    expected_within_dandiset_paths: list[str],
    example_pipeline_queue: PipelineQueue,
    base_directory: pathlib.Path,
    dandi_api_key: None,
) -> None:
    """archive_capsules moves every entry of the pipeline, narrowed by status when one is given."""
    expected_entries = [
        entry
        for entry in example_pipeline_queue
        if entry.job.pipeline == pipeline and (status is None or entry.status == status)
    ]
    expected_paths = [entry.capsule_path() for entry in expected_entries]
    metadata = _metadata_with_capsules_at(*expected_paths)

    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.move_job_capsule") as mock_move,
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
    ):
        archived = example_pipeline_queue.archive_capsules(
            status=status, pipeline=pipeline, base_directory=base_directory
        )

    assert [entry.job.within_dandiset_path for entry in expected_entries] == expected_within_dandiset_paths
    assert archived == expected_paths
    assert mock_move.call_count == len(expected_paths)
