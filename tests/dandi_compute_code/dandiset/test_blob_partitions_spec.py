"""
The vendored blob partition spec matches simple-s3-backup's ``partitions.json``.

simple-s3-backup decides which partition of the mirror each blob is copied to. This package
cannot import that decision, so it ships a copy, and these tests catch the copy drifting.
"""

import json
import pathlib
import urllib.error
import urllib.request

import pytest

import dandi_compute_code

_VENDORED_SPEC_PATH = pathlib.Path(dandi_compute_code.__file__).parent / "dandiset" / "blob_partitions.json"
_UPSTREAM_SPEC_URL = (
    "https://raw.githubusercontent.com/dandi/simple-s3-backup/main/src/simple_s3_backup/partitions.json"
)
_HEX_DIGITS = "0123456789abcdef"


@pytest.mark.ai_generated
def test_vendored_blob_partitions_spec_assigns_every_hex_digit_to_a_listed_partition() -> None:
    spec = json.loads(_VENDORED_SPEC_PATH.read_text())

    for kind in ("blobs", "zarr"):
        assert sorted(spec[kind]) == list(_HEX_DIGITS)
        assert set(spec[kind].values()) <= set(spec["partitions"])


# The upstream file is added by a simple-s3-backup change that may not be merged yet, so the URL
# can answer 404. Only that case (or no network at all) skips; once upstream publishes the file
# this becomes a real comparison with no edit needed here.
@pytest.mark.ai_generated
def test_vendored_blob_partitions_spec_matches_upstream() -> None:
    try:
        with urllib.request.urlopen(_UPSTREAM_SPEC_URL, timeout=30) as response:
            upstream_spec = json.loads(response.read())
    except urllib.error.HTTPError as error:
        if error.code == 404:
            pytest.skip("simple-s3-backup does not publish partitions.json on main yet")
        raise
    except urllib.error.URLError as error:
        pytest.skip(f"cannot reach GitHub to fetch the upstream spec: {error.reason}")

    vendored_spec = json.loads(_VENDORED_SPEC_PATH.read_text())
    assert vendored_spec == upstream_spec
