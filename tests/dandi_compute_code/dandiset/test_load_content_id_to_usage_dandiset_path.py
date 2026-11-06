import gzip
import io
import json
from collections.abc import Iterator
from unittest import mock

import pytest

from dandi_compute_code.dandiset._load_content_id_to_usage_dandiset_path import (
    _load_content_id_to_usage_dandiset_path,
)

_URLOPEN = "dandi_compute_code.dandiset._load_content_id_to_usage_dandiset_path.urllib.request.urlopen"


@pytest.fixture(autouse=True)
def clear_mapping_cache() -> Iterator[None]:
    _load_content_id_to_usage_dandiset_path.cache_clear()
    yield
    _load_content_id_to_usage_dandiset_path.cache_clear()


@pytest.mark.ai_generated
def test_mapping_is_merged_from_a_streamed_gzipped_json_lines_response() -> None:
    lines = [
        json.dumps({"content-id-1": {"000001": "sub-a/sub-a_ecephys.nwb"}}),
        "",
        json.dumps({"content-id-2": {"000002": "sub-b/sub-b_ecephys.nwb", "000003": "sub-c/sub-c_ecephys.nwb"}}),
    ]
    payload = gzip.compress("\n".join(lines).encode())

    with mock.patch(_URLOPEN, return_value=io.BytesIO(payload)):
        mapping = _load_content_id_to_usage_dandiset_path()

    assert mapping == {
        "content-id-1": {"000001": "sub-a/sub-a_ecephys.nwb"},
        "content-id-2": {"000002": "sub-b/sub-b_ecephys.nwb", "000003": "sub-c/sub-c_ecephys.nwb"},
    }


@pytest.mark.ai_generated
def test_mapping_is_downloaded_once_per_process() -> None:
    payload = gzip.compress(json.dumps({"content-id-1": {"000001": "sub-a/sub-a_ecephys.nwb"}}).encode())

    with mock.patch(_URLOPEN, return_value=io.BytesIO(payload)) as mock_urlopen:
        first = _load_content_id_to_usage_dandiset_path()
        second = _load_content_id_to_usage_dandiset_path()

    assert second is first
    assert mock_urlopen.call_count == 1


@pytest.mark.ai_generated
def test_an_undecodable_mapping_raises_runtime_error() -> None:
    with (
        mock.patch(_URLOPEN, return_value=io.BytesIO(b"not gzip")),
        pytest.raises(RuntimeError, match="Unable to load content-id-to-usage-dandiset-path mapping"),
    ):
        _load_content_id_to_usage_dandiset_path()
