"""
Every JSON file packaged with this repository has unique keys.

``json.loads`` keeps only the last of two identical keys without complaint, so a registry entry
added under a key that is already taken would silently replace the earlier one.
"""

import collections
import json
import pathlib

import pytest

import dandi_compute_code

_PACKAGE_ROOT = pathlib.Path(dandi_compute_code.__file__).parent
_PACKAGED_JSON_FILE_PATHS = sorted(_PACKAGE_ROOT.rglob("*.json"))


def _duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    counts = collections.Counter(key for key, _ in pairs)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    assert duplicates == []
    return dict(pairs)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "json_file_path",
    _PACKAGED_JSON_FILE_PATHS,
    ids=[str(path.relative_to(_PACKAGE_ROOT)) for path in _PACKAGED_JSON_FILE_PATHS],
)
def test_packaged_json_files_have_no_duplicate_keys(json_file_path: pathlib.Path) -> None:
    """No object in a packaged JSON file repeats a key."""
    json.loads(json_file_path.read_text(), object_pairs_hook=_duplicate_keys)
