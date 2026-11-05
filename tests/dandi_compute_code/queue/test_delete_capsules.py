import os
from unittest import mock

import pytest
from testing_utilities import job_capsule_files, serve_remote_dandiset

from dandi_compute_code.queue import PipelineQueue

_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"

_SOLE_AIND_PATH = "sub-sole/ses-capsule"  # recorded version v1.1.1+b268fd2
_SOURCEDATA_AIND_PATH = "sourcedata/sub-test+bids"  # recorded version v1.1.1+b268fd2+a66c8df


def _delete_call(capsule_path: str) -> mock._Call:
    return mock.call(
        ["dandi", "delete", f"dandi://dandi/{_FAILED_RUNS_ARCHIVE_DANDISET_ID}/{capsule_path}/"],
        input=b"y\n",
        check=True,
    )


def _files_for(queue: PipelineQueue, *within_dandiset_paths: str) -> dict[str, str]:
    files: dict[str, str] = {}
    for within_dandiset_path in within_dandiset_paths:
        files |= job_capsule_files(entry=queue.entry_for(within_dandiset_path=within_dandiset_path), with_logs=True)
    return files


@pytest.mark.ai_generated
def test_delete_capsules_deletes_every_capsule_of_the_version_on_the_archive(
    example_pipeline_queue: PipelineQueue, dandi_api_key: None
) -> None:
    """Every capsule of the version with assets on the archive is deleted by URL, and other versions are left."""
    failed_path = example_pipeline_queue.entry_for(within_dandiset_path="sub-failed/ses-one").capsule_path()
    stalled_path = example_pipeline_queue.entry_for(within_dandiset_path="sub-stalled/ses-one").capsule_path()
    files = _files_for(example_pipeline_queue, "sub-failed/ses-one", "sub-stalled/ses-one", _SOLE_AIND_PATH)

    with serve_remote_dandiset(files), mock.patch("subprocess.run") as mock_run:
        deleted = example_pipeline_queue.delete_capsules(version="v1.0")

    assert deleted == [failed_path, stalled_path]
    assert mock_run.call_args_list == [_delete_call(failed_path), _delete_call(stalled_path)]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("version", "expected_within_dandiset_paths"),
    [
        pytest.param("v1.1.1", [_SOLE_AIND_PATH, _SOURCEDATA_AIND_PATH], id="release-selects-its-builds"),
        pytest.param("1.1.1", [_SOLE_AIND_PATH, _SOURCEDATA_AIND_PATH], id="without-leading-v"),
        pytest.param("v1.1.1+b268fd2", [_SOLE_AIND_PATH], id="exact-build"),
        pytest.param("v1.1.1+b268fd2+a66c8df", [_SOURCEDATA_AIND_PATH], id="exact-longer-build"),
        pytest.param("v1.1", [], id="no-prefix-match"),
    ],
)
def test_delete_capsules_matches_versions(
    example_pipeline_queue: PipelineQueue,
    dandi_api_key: None,
    version: str,
    expected_within_dandiset_paths: list[str],
) -> None:
    """A release matches itself and its builds, and a build matches only itself."""
    files = _files_for(example_pipeline_queue, _SOLE_AIND_PATH, _SOURCEDATA_AIND_PATH)

    with serve_remote_dandiset(files), mock.patch("subprocess.run"):
        deleted = example_pipeline_queue.delete_capsules(version=version)

    expected = [
        example_pipeline_queue.entry_for(within_dandiset_path=path).capsule_path()
        for path in expected_within_dandiset_paths
    ]
    assert deleted == expected


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("pipeline", "expected_within_dandiset_paths"),
    [
        pytest.param("aind+ephys", [_SOLE_AIND_PATH, _SOURCEDATA_AIND_PATH], id="matching-pipeline"),
        pytest.param("lfp", [], id="other-pipeline"),
    ],
)
def test_delete_capsules_narrows_to_one_pipeline(
    example_pipeline_queue: PipelineQueue,
    dandi_api_key: None,
    pipeline: str,
    expected_within_dandiset_paths: list[str],
) -> None:
    """*pipeline* keeps only that pipeline's capsules of the version."""
    files = _files_for(example_pipeline_queue, _SOLE_AIND_PATH, _SOURCEDATA_AIND_PATH)

    with serve_remote_dandiset(files), mock.patch("subprocess.run") as mock_run:
        deleted = example_pipeline_queue.delete_capsules(version="v1.1.1", pipeline=pipeline)

    expected = [
        example_pipeline_queue.entry_for(within_dandiset_path=path).capsule_path()
        for path in expected_within_dandiset_paths
    ]
    assert deleted == expected
    assert mock_run.call_args_list == [_delete_call(path) for path in expected]


@pytest.mark.ai_generated
def test_delete_capsules_dry_run_lists_without_deleting_or_an_api_key(example_pipeline_queue: PipelineQueue) -> None:
    """A dry run returns what would be deleted, runs no `dandi delete` and needs no DANDI_API_KEY."""
    sole_path = example_pipeline_queue.entry_for(within_dandiset_path=_SOLE_AIND_PATH).capsule_path()
    files = _files_for(example_pipeline_queue, _SOLE_AIND_PATH)

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        serve_remote_dandiset(files),
        mock.patch("subprocess.run") as mock_run,
    ):
        deleted = example_pipeline_queue.delete_capsules(version="v1.1.1+b268fd2", dry_run=True)

    assert deleted == [sole_path]
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_delete_capsules_raises_without_dandi_api_key() -> None:
    """delete_capsules raises RuntimeError when DANDI_API_KEY is not set and this is not a dry run."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            PipelineQueue(entries=[]).delete_capsules(version="v1.0")


@pytest.mark.ai_generated
@pytest.mark.parametrize("version", ["", "   ", "v"])
def test_delete_capsules_rejects_a_blank_version(version: str, dandi_api_key: None) -> None:
    """A blank version is refused rather than matching nothing or everything."""
    with pytest.raises(ValueError, match="must not be blank"):
        PipelineQueue(entries=[]).delete_capsules(version=version)
