import pathlib

import pytest


@pytest.fixture
def base_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A structured base directory with its processing/, work/ and dandi/ subdirectories created."""
    directory = tmp_path / "base"
    for subdirectory_name in ("processing", "work", "dandi"):
        (directory / subdirectory_name).mkdir(parents=True)
    return directory
