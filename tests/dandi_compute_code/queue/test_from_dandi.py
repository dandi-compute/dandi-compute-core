from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import PipelineQueue

# PipelineQueue.from_dandi derives queue state from DANDI assets.jsonld metadata fetched over
# the network. The conftest _no_real_dandi_fetch guard defaults that loader to empty; tests
# that need specific metadata override it with their own mock.patch. The assets metadata built
# in each test is the ground-truth input under test.


def _entries(state: PipelineQueue) -> list[dict]:
    return [entry.to_dict() for entry in state]


@pytest.mark.ai_generated
def test_from_dandi_returns_empty_for_missing_metadata() -> None:
    """from_dandi returns an empty state when there is no assets metadata."""
    content_id_to_asset: dict[str, dict[str, object]] = {}
    with mock.patch(
        "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(content_id_to_asset=content_id_to_asset, path_to_asset_metadata={}),
    ):
        state = PipelineQueue.from_dandi()
    assert len(state) == 0


@pytest.mark.ai_generated
def test_from_dandi_returns_all_ordered_pending_entries() -> None:
    """from_dandi returns ordered pending entries from metadata."""
    capsule_metadata_by_path = {
        f"derivatives/dandiset-001697/sub-{i:02d}/sub-{i:02d}_ecephys/pipeline-test/"
        f"job-240101{i:06d}/code/submit.sh": AssetMetadata(
            path=(
                f"derivatives/dandiset-001697/sub-{i:02d}/sub-{i:02d}_ecephys/pipeline-test/"
                f"job-240101{i:06d}/code/submit.sh"
            ),
            date_modified="2024-01-01T00:00:00+00:00",
            content_size=1,
            content_id=f"capsule-{i}",
        )
        for i in range(1, 6)
    }
    source_metadata_by_path = {
        f"sub-{i:02d}/sub-{i:02d}_ecephys.nwb": AssetMetadata(
            path=f"sub-{i:02d}/sub-{i:02d}_ecephys.nwb",
            date_modified="2024-01-01T00:00:00+00:00",
            content_size=i,
            content_id=f"id-{i}",
        )
        for i in range(1, 6)
    }
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=capsule_metadata_by_path),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=source_metadata_by_path),
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 5
    assert all(entry["status"] == "pending" for entry in state_entries)


@pytest.mark.ai_generated
def test_from_dandi_includes_entries_with_submitted_markers() -> None:
    """from_dandi does not depend on local submitted marker files."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567/code/submit.sh"
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    capsule_path: AssetMetadata(
                        path=capsule_path,
                        date_modified="2024-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="capsule-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2024-01-01T00:00:00+00:00",
                        content_size=1234,
                        content_id="id-1",
                    )
                },
            ),
        ),
    ):
        state = PipelineQueue.from_dandi()
    state_entries = _entries(state)
    assert len(state_entries) == 1


@pytest.mark.ai_generated
def test_from_dandi_submitted_marker_sets_stalled_status() -> None:
    """from_dandi records a stalled status when code/submitted_date-* exists with no logs or output."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_prefix = "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567"
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{capsule_prefix}/code/submit.sh": AssetMetadata(
                path=f"{capsule_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="capsule-code-id",
            ),
            f"{capsule_prefix}/code/submitted_date-date-2025+01+01_time-00+00+00": AssetMetadata(
                path=f"{capsule_prefix}/code/submitted_date-date-2025+01+01_time-00+00+00",
                date_modified="2024-01-01T00:01:00+00:00",
                content_size=1,
                content_id="capsule-submitted-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()
    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["status"] == "stalled"


@pytest.mark.ai_generated
def test_from_dandi_records_submission_time_and_durations() -> None:
    """from_dandi times submission from the earliest submitted marker and derives both durations."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_prefix = "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567"
    asset_paths_and_times = {
        f"{capsule_prefix}/code/submit.sh": "2024-01-01T00:00:00+00:00",
        f"{capsule_prefix}/code/submitted_date-date-2024+01+01_time-00+30+00": "2024-01-01T00:30:00+00:00",
        f"{capsule_prefix}/code/submitted_date-date-2024+01+01_time-01+00+00": "2024-01-01T01:00:00+00:00",
        f"{capsule_prefix}/logs/stdout.txt": "2024-01-01T02:00:00+00:00",
    }
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            asset_path: AssetMetadata(
                path=asset_path,
                date_modified=date_modified,
                content_size=1,
                content_id=f"content-id-{index}",
            )
            for index, (asset_path, date_modified) in enumerate(asset_paths_and_times.items())
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["created_at"] == "2024-01-01T00:00:00+00:00"
    assert state_entries[0]["job_submission_time"] == "2024-01-01T00:30:00+00:00"
    assert state_entries[0]["job_completion_time"] == "2024-01-01T02:00:00+00:00"
    assert state_entries[0]["queue_wait_seconds"] == 1800
    assert state_entries[0]["run_duration_seconds"] == 5400


@pytest.mark.ai_generated
def test_from_dandi_parses_capsule_location_and_presence_flags_from_assets_paths() -> None:
    """from_dandi parses a capsule's location and lifecycle flags from derivatives asset paths."""
    source_path = "sub-mouse01/sourcedata/aind-sample.nwb"
    capsule_prefix = (
        "derivatives/dandiset-001849/sub-mouse01/sourcedata/aind-sample/pipeline-aind+ephys/job-2401010d4bf3"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={
            "source-content-id": {
                "path": source_path,
                "contentSize": 1234,
                "blobDateModified": "2026-05-24T10:00:00+00:00",
            },
            "code-content-id": {
                "path": f"{capsule_prefix}/code/submit.sh",
                "contentSize": 1,
                "dateModified": "2026-05-24T10:10:00+00:00",
            },
            "output-content-id": {
                "path": f"{capsule_prefix}/derivatives/output.nwb",
                "contentSize": 2,
                "dateModified": "2026-05-24T10:20:00+00:00",
            },
            "log-content-id": {
                "path": f"{capsule_prefix}/logs/stdout.txt",
                "contentSize": 3,
                "dateModified": "2026-05-24T10:30:00+00:00",
            },
        },
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2026-05-24T10:00:00+00:00",
                content_size=1234,
                content_id="source-content-id",
            ),
            f"{capsule_prefix}/code/submit.sh": AssetMetadata(
                path=f"{capsule_prefix}/code/submit.sh",
                date_modified="2026-05-24T10:10:00+00:00",
                content_size=1,
                content_id="code-content-id",
            ),
            f"{capsule_prefix}/derivatives/output.nwb": AssetMetadata(
                path=f"{capsule_prefix}/derivatives/output.nwb",
                date_modified="2026-05-24T10:20:00+00:00",
                content_size=2,
                content_id="output-content-id",
            ),
            f"{capsule_prefix}/logs/stdout.txt": AssetMetadata(
                path=f"{capsule_prefix}/logs/stdout.txt",
                date_modified="2026-05-24T10:30:00+00:00",
                content_size=3,
                content_id="log-content-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2026-05-24T10:00:00+00:00",
                content_size=1234,
                content_id="source-content-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["dandiset_id"] == "001849"
    assert state_entries[0]["within_dandiset_path"] == source_path
    assert state_entries[0]["pipeline"] == "aind+ephys"
    assert state_entries[0]["job_id"] == "job-2401010d4bf3"
    assert state_entries[0]["content_id"] == "source-content-id"
    assert state_entries[0]["asset_size_bytes"] == 1234
    assert state_entries[0]["status"] == "successful"
    assert state_entries[0]["job_completion_time"] == "2026-05-24T10:30:00+00:00"
    assert state_entries[0]["output_paths"] == {f"{capsule_prefix}/derivatives/output.nwb": "output-content-id"}
    assert state_entries[0]["log_paths"] == {f"{capsule_prefix}/logs/stdout.txt": "log-content-id"}


@pytest.mark.ai_generated
def test_from_dandi_resolves_within_dandiset_path_for_nested_asset() -> None:
    """from_dandi writes the assets.jsonld-resolved source path."""
    content_id = "0fbbca6a-0000-0000-0000-000000000001"
    source_path = "sub-mouse01/sub-mouse01_ses-ses001_obj-raw.nwb"
    asset_size_bytes = 1234
    capsule_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ses-ses001_obj-raw/"
        "pipeline-aind+ephys/job-240101222222/code/submit.sh"
    )

    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    capsule_path: AssetMetadata(
                        path=capsule_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="capsule-code-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=asset_size_bytes,
                        content_id=content_id,
                    )
                },
            ),
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["asset_size_bytes"] == asset_size_bytes
    assert state_entries[0]["within_dandiset_path"] == source_path


@pytest.mark.ai_generated
def test_from_dandi_resolves_within_dandiset_path_for_root_level_asset() -> None:
    """from_dandi writes the resolved within_dandiset_path even when the matched asset path is at dandiset root."""
    content_id = "0fbbca6a-0000-0000-0000-000000000002"
    asset_size_bytes = 4321
    root_asset_path = "sub-mouse01_ses-ses001_obj-raw.nwb"
    capsule_path = (
        "derivatives/dandiset-001697/sub-mouse01_ses-ses001_obj-raw/"
        "pipeline-aind+ephys/job-240101333333/code/submit.sh"
    )

    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    capsule_path: AssetMetadata(
                        path=capsule_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="capsule-code-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    root_asset_path: AssetMetadata(
                        path=root_asset_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=asset_size_bytes,
                        content_id=content_id,
                    )
                },
            ),
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["asset_size_bytes"] == asset_size_bytes
    assert state_entries[0]["within_dandiset_path"] == root_asset_path


@pytest.mark.ai_generated
def test_from_dandi_does_not_require_dandi_api_key() -> None:
    """from_dandi works when DANDI_API_KEY is not set."""
    with (
        mock.patch.dict("os.environ", {}, clear=True),
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
    ):
        PipelineQueue.from_dandi()


@pytest.mark.ai_generated
def test_from_dandi_includes_all_entries_derived_from_metadata() -> None:
    """from_dandi returns all entries derived from assets metadata."""
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test-pipeline/"
            "job-240101111111/code/submit.sh": AssetMetadata(
                path=(
                    "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test-pipeline/"
                    "job-240101111111/code/submit.sh"
                ),
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=1,
                content_id="capsule-1",
            ),
            "derivatives/dandiset-001697/sub-mouse02/sub-mouse02_ecephys/pipeline-test-pipeline/"
            "job-240101222222/code/submit.sh": AssetMetadata(
                path=(
                    "derivatives/dandiset-001697/sub-mouse02/sub-mouse02_ecephys/pipeline-test-pipeline/"
                    "job-240101222222/code/submit.sh"
                ),
                date_modified="2025-01-02T00:00:00+00:00",
                content_size=1,
                content_id="capsule-2",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            "sub-mouse01/sub-mouse01_ecephys.nwb": AssetMetadata(
                path="sub-mouse01/sub-mouse01_ecephys.nwb",
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=11,
                content_id="0fbbca6a-0000-0000-0000-000000000001",
            ),
            "sub-mouse02/sub-mouse02_ecephys.nwb": AssetMetadata(
                path="sub-mouse02/sub-mouse02_ecephys.nwb",
                date_modified="2025-01-02T00:00:00+00:00",
                content_size=22,
                content_id="0fbbca6a-0000-0000-0000-000000000002",
            ),
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 2
    assert {record["within_dandiset_path"] for record in state_entries} == {
        "sub-mouse01/sub-mouse01_ecephys.nwb",
        "sub-mouse02/sub-mouse02_ecephys.nwb",
    }


@pytest.mark.ai_generated
def test_from_dandi_is_independent_of_local_submitted_marker_files() -> None:
    """from_dandi output is independent of local submitted marker files."""
    capsule_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test-pipeline/"
        "job-240101999999/code/submit.sh"
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    capsule_path: AssetMetadata(
                        path=capsule_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="capsule-1",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    "sub-mouse01/sub-mouse01_ecephys.nwb": AssetMetadata(
                        path="sub-mouse01/sub-mouse01_ecephys.nwb",
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=11,
                        content_id="0fbbca6a-0000-0000-0000-000000000001",
                    )
                },
            ),
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["within_dandiset_path"] == "sub-mouse01/sub-mouse01_ecephys.nwb"


@pytest.mark.ai_generated
def test_from_dandi_output_paths_empty_when_no_output() -> None:
    """from_dandi returns output_paths as an empty dict when there is no output."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_prefix = "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567"
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{capsule_prefix}/code/submit.sh": AssetMetadata(
                path=f"{capsule_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["status"] == "pending"
    assert state_entries[0]["dataset_description_path"] == {}
    assert state_entries[0]["output_paths"] == {}


@pytest.mark.ai_generated
def test_from_dandi_log_paths_empty_when_no_logs() -> None:
    """from_dandi returns log_paths as an empty dict when there are no logs."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_prefix = "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567"
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{capsule_prefix}/code/submit.sh": AssetMetadata(
                path=f"{capsule_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["status"] == "pending"
    assert state_entries[0]["log_paths"] == {}


@pytest.mark.ai_generated
def test_from_dandi_output_paths_maps_asset_paths_to_blob_ids() -> None:
    """from_dandi populates output_paths with all derivatives asset paths mapped to their blob IDs."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_prefix = "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567"
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{capsule_prefix}/code/submit.sh": AssetMetadata(
                path=f"{capsule_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
            f"{capsule_prefix}/derivatives/output.nwb": AssetMetadata(
                path=f"{capsule_prefix}/derivatives/output.nwb",
                date_modified="2024-01-01T00:01:00+00:00",
                content_size=100,
                content_id="output-blob-id-1",
            ),
            f"{capsule_prefix}/derivatives/extra.json": AssetMetadata(
                path=f"{capsule_prefix}/derivatives/extra.json",
                date_modified="2024-01-01T00:02:00+00:00",
                content_size=10,
                content_id="output-blob-id-2",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["status"] == "successful"
    assert state_entries[0]["output_paths"] == {
        f"{capsule_prefix}/derivatives/output.nwb": "output-blob-id-1",
        f"{capsule_prefix}/derivatives/extra.json": "output-blob-id-2",
    }


@pytest.mark.ai_generated
def test_from_dandi_log_paths_map_asset_paths_to_blob_ids() -> None:
    """from_dandi populates log_paths with log asset paths mapped to their blob IDs."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_prefix = "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567"
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{capsule_prefix}/code/submit.sh": AssetMetadata(
                path=f"{capsule_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
            f"{capsule_prefix}/dataset_description.json": AssetMetadata(
                path=f"{capsule_prefix}/dataset_description.json",
                date_modified="2024-01-01T00:00:30+00:00",
                content_size=10,
                content_id="dataset-description-id",
            ),
            f"{capsule_prefix}/logs/stdout.txt": AssetMetadata(
                path=f"{capsule_prefix}/logs/stdout.txt",
                date_modified="2024-01-01T00:01:00+00:00",
                content_size=100,
                content_id="log-blob-id-1",
            ),
            f"{capsule_prefix}/logs/stderr.txt": AssetMetadata(
                path=f"{capsule_prefix}/logs/stderr.txt",
                date_modified="2024-01-01T00:02:00+00:00",
                content_size=10,
                content_id="log-blob-id-2",
            ),
            f"{capsule_prefix}/logs/dataset_description.json": AssetMetadata(
                path=f"{capsule_prefix}/logs/dataset_description.json",
                date_modified="2024-01-01T00:03:00+00:00",
                content_size=10,
                content_id="ignored-log-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["status"] == "failed"
    assert state_entries[0]["dataset_description_path"] == {
        f"{capsule_prefix}/dataset_description.json": "dataset-description-id"
    }
    assert state_entries[0]["log_paths"] == {
        f"{capsule_prefix}/logs/stdout.txt": "log-blob-id-1",
        f"{capsule_prefix}/logs/stderr.txt": "log-blob-id-2",
    }


@pytest.mark.ai_generated
def test_from_dandi_capsule_without_any_content_is_unknown() -> None:
    """A capsule holding only its dataset description has reached no point in the lifecycle."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    capsule_prefix = "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/job-240101def567"
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{capsule_prefix}/dataset_description.json": AssetMetadata(
                path=f"{capsule_prefix}/dataset_description.json",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="dataset-description-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = PipelineQueue.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["status"] == "unknown"
