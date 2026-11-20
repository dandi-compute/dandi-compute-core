import json
import pathlib

import pytest

from dandi_compute_code.dandiset import _blob_mirror

_HEX_DIGITS = "0123456789abcdef"


@pytest.fixture
def base_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A structured base directory with its processing/ and work/ subdirectories created."""
    directory = tmp_path / "base"
    for subdirectory_name in ("processing", "work"):
        (directory / subdirectory_name).mkdir(parents=True)
    return directory


@pytest.fixture
def blob_partitions_spec() -> dict[str, object]:
    """A stand-in for simple-s3-backup's ``partitions.json``, so tests do not depend on its current values."""
    spec = {
        "mirror_root": "/orcd/data/dandi",
        "partitions": ["001", "002"],
        "blobs": {digit: "001" if digit.isdigit() else "002" for digit in _HEX_DIGITS},
        "zarr": {digit: "001" if digit.isdigit() else "002" for digit in _HEX_DIGITS},
        "not_in_bucket_grace_days": 183,
    }
    return spec


# Anything that resolves a blob path loads simple-s3-backup's partitions.json, from the cluster
# checkout or GitHub. Every test gets the stand-in spec at the cluster path instead, so tests need
# neither; the loader's cache is cleared around each test so a patched spec never leaks.
@pytest.fixture(autouse=True)
def cluster_partitions_spec_path(
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
    blob_partitions_spec: dict[str, object],
) -> pathlib.Path:
    # Kept out of `tmp_path`, which tests list or fill themselves.
    spec_path = tmp_path_factory.mktemp("simple-s3-backup") / "partitions.json"
    spec_path.write_text(json.dumps(blob_partitions_spec))
    monkeypatch.setattr(_blob_mirror, "_CLUSTER_PARTITIONS_SPEC_PATH", spec_path)
    _blob_mirror._load_partitions_spec.cache_clear()
    yield spec_path
    _blob_mirror._load_partitions_spec.cache_clear()
