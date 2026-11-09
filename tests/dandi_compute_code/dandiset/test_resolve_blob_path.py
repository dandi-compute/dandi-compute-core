import logging
import pathlib

import pytest

from dandi_compute_code.dandiset import resolve_blob_path


def _blob_path(*, mirror_root: pathlib.Path, partition: str, content_id: str) -> pathlib.Path:
    return mirror_root / partition / "s3dandiarchive" / "blobs" / content_id[0:3] / content_id[3:6] / content_id


def _mirror_blob(*, mirror_root: pathlib.Path, partition: str, content_id: str) -> pathlib.Path:
    blob_path = _blob_path(mirror_root=mirror_root, partition=partition, content_id=content_id)
    blob_path.parent.mkdir(parents=True)
    blob_path.write_bytes(b"")
    return blob_path


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("content_id", "expected_partition"),
    [
        pytest.param("0a1b2c3d-0000-0000-0000-000000000000", "001", id="0"),
        pytest.param("8a1b2c3d-0000-0000-0000-000000000000", "001", id="8"),
        pytest.param("9a1b2c3d-0000-0000-0000-000000000000", "001", id="9"),
        pytest.param("aa1b2c3d-0000-0000-0000-000000000000", "002", id="a"),
        pytest.param("fa1b2c3d-0000-0000-0000-000000000000", "002", id="f"),
    ],
)
def test_resolve_blob_path_follows_the_backup_split_for_a_blob_not_yet_mirrored(
    content_id: str, expected_partition: str, tmp_path: pathlib.Path
) -> None:
    blob_path = resolve_blob_path(content_id=content_id, mirror_root=tmp_path)

    assert blob_path == _blob_path(mirror_root=tmp_path, partition=expected_partition, content_id=content_id)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("content_id", "partition"),
    [
        pytest.param("9a1b2c3d-0000-0000-0000-000000000000", "002", id="9-on-002"),
        pytest.param("aa1b2c3d-0000-0000-0000-000000000000", "001", id="a-on-001"),
    ],
)
def test_resolve_blob_path_finds_a_blob_on_the_other_partition(
    content_id: str, partition: str, tmp_path: pathlib.Path
) -> None:
    mirrored_path = _mirror_blob(mirror_root=tmp_path, partition=partition, content_id=content_id)

    blob_path = resolve_blob_path(content_id=content_id, mirror_root=tmp_path)

    assert blob_path == mirrored_path


@pytest.mark.ai_generated
def test_resolve_blob_path_prefers_the_backup_split_when_both_partitions_hold_the_blob(
    tmp_path: pathlib.Path,
) -> None:
    content_id = "9a1b2c3d-0000-0000-0000-000000000000"
    expected_path = _mirror_blob(mirror_root=tmp_path, partition="001", content_id=content_id)
    _mirror_blob(mirror_root=tmp_path, partition="002", content_id=content_id)

    blob_path = resolve_blob_path(content_id=content_id, mirror_root=tmp_path)

    assert blob_path == expected_path


@pytest.mark.ai_generated
def test_resolve_blob_path_warns_only_when_the_blob_is_not_mirrored(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    mirrored_content_id = "4825cfd1-bc75-4dd2-b4b7-77995ab57e0a"
    missing_content_id = "4825cfd1-0000-0000-0000-000000000000"
    _mirror_blob(mirror_root=tmp_path, partition="001", content_id=mirrored_content_id)

    with caplog.at_level(logging.WARNING):
        resolve_blob_path(content_id=mirrored_content_id, mirror_root=tmp_path)
        resolve_blob_path(content_id=missing_content_id, mirror_root=tmp_path)

    warned_about = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warned_about) == 1
    assert missing_content_id in warned_about[0]


@pytest.mark.ai_generated
@pytest.mark.parametrize("content_id", ["", "g0000000-0000-0000-0000-000000000000"])
def test_resolve_blob_path_rejects_a_content_id_without_a_leading_hex_digit(
    content_id: str, tmp_path: pathlib.Path
) -> None:
    with pytest.raises(ValueError, match="hex digit"):
        resolve_blob_path(content_id=content_id, mirror_root=tmp_path)
