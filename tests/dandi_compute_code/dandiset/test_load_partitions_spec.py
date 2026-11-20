"""
The blob mirror reads simple-s3-backup's ``partitions.json``, from the cluster checkout or GitHub.

The shared ``cluster_partitions_spec_path`` fixture puts a stand-in spec at the cluster path for every
test, so these tests move or remove it, and patch the fetch, to reach each source.
"""

import io
import json
import pathlib
import urllib.error

import pytest

from dandi_compute_code.dandiset import _blob_mirror, resolve_blob_path

_HEX_DIGITS = "0123456789abcdef"


def _fake_urlopen(*, spec: dict[str, object]):
    def urlopen(url, *, timeout):
        return io.BytesIO(json.dumps(spec).encode())

    return urlopen


def _failing_urlopen(url, *, timeout):
    raise urllib.error.URLError("no network")


@pytest.mark.ai_generated
def test_load_partitions_spec_prefers_the_cluster_checkout(
    monkeypatch: pytest.MonkeyPatch, blob_partitions_spec: dict[str, object]
) -> None:
    monkeypatch.setattr(_blob_mirror.urllib.request, "urlopen", _failing_urlopen)

    spec = _blob_mirror._load_partitions_spec()

    assert spec == blob_partitions_spec


@pytest.mark.ai_generated
def test_load_partitions_spec_falls_back_to_github(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, blob_partitions_spec: dict[str, object]
) -> None:
    fetched_spec = {**blob_partitions_spec, "mirror_root": "/fetched"}
    monkeypatch.setattr(_blob_mirror, "_CLUSTER_PARTITIONS_SPEC_PATH", tmp_path / "missing.json")
    monkeypatch.setattr(_blob_mirror.urllib.request, "urlopen", _fake_urlopen(spec=fetched_spec))

    spec = _blob_mirror._load_partitions_spec()

    assert spec == fetched_spec


@pytest.mark.ai_generated
def test_load_partitions_spec_names_both_places_when_neither_answers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    missing_path = tmp_path / "missing.json"
    monkeypatch.setattr(_blob_mirror, "_CLUSTER_PARTITIONS_SPEC_PATH", missing_path)
    monkeypatch.setattr(_blob_mirror.urllib.request, "urlopen", _failing_urlopen)

    with pytest.raises(RuntimeError) as exception_info:
        resolve_blob_path(content_id="0a1b2c3d-0000-0000-0000-000000000000")

    assert str(missing_path) in str(exception_info.value)
    assert _blob_mirror._PARTITIONS_SPEC_URL in str(exception_info.value)


@pytest.mark.ai_generated
def test_load_partitions_spec_is_read_once(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    first_spec = _blob_mirror._load_partitions_spec()
    monkeypatch.setattr(_blob_mirror, "_CLUSTER_PARTITIONS_SPEC_PATH", tmp_path / "missing.json")
    monkeypatch.setattr(_blob_mirror.urllib.request, "urlopen", _failing_urlopen)

    second_spec = _blob_mirror._load_partitions_spec()

    assert second_spec is first_spec


# A real fetch, so the daily tests catch simple-s3-backup moving or reshaping the file. It is not
# skipped on failure, since the file is on main and a failure here is one capsules would hit too.
@pytest.mark.ai_generated
def test_load_partitions_spec_from_github_has_the_expected_shape(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.setattr(_blob_mirror, "_CLUSTER_PARTITIONS_SPEC_PATH", tmp_path / "missing.json")

    spec = _blob_mirror._load_partitions_spec()

    assert isinstance(spec["mirror_root"], str)
    assert sorted(spec["blobs"]) == list(_HEX_DIGITS)
    assert set(spec["blobs"].values()) <= set(spec["partitions"])
