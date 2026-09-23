import pathlib
import subprocess
from unittest import mock

import pytest

from dandi_compute_code.queue import PipelineQueue


def _initialize_repository_with_tags(directory: pathlib.Path, /, *, tags: list[str]) -> None:
    """Create a git repository holding one commit carrying every tag in *tags*."""
    subprocess.run(["git", "init", "--quiet"], cwd=directory, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=directory, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=directory, check=True)
    (directory / "README.md").write_text("pipeline")
    subprocess.run(["git", "add", "README.md"], cwd=directory, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "initial"], cwd=directory, check=True)
    for tag in tags:
        subprocess.run(["git", "tag", tag], cwd=directory, check=True)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("tags", "expected_version"),
    [
        pytest.param(["v1.2.3"], "v1.2.3", id="single_tag"),
        pytest.param(["v1.2.3", "v1.10.0", "v1.9.0"], "v1.10.0", id="numeric_not_lexicographic"),
        pytest.param(["v1.2.3", "not-a-version", "latest"], "v1.2.3", id="ignores_non_version_tags"),
        pytest.param(["v0.9.0", "v1.0.0-fixes"], "v1.0.0-fixes", id="suffixed_tag"),
    ],
)
def test_resolve_latest_pipeline_version_from_repository_tags(
    tmp_path: pathlib.Path, tags: list[str], expected_version: str
) -> None:
    """The latest AIND pipeline version is the highest release tag in the base directory's local checkout."""
    pipeline_directory = tmp_path / "aind-ephys-pipeline"
    pipeline_directory.mkdir()
    _initialize_repository_with_tags(pipeline_directory, tags=tags)

    latest_version = PipelineQueue.resolve_latest_pipeline_version(pipeline="aind+ephys", base_directory=tmp_path)

    assert latest_version == expected_version


@pytest.mark.ai_generated
def test_resolve_latest_pipeline_version_raises_without_tags(tmp_path: pathlib.Path) -> None:
    """A checkout with no release tags cannot name a latest version."""
    pipeline_directory = tmp_path / "aind-ephys-pipeline"
    pipeline_directory.mkdir()
    _initialize_repository_with_tags(pipeline_directory, tags=[])

    with pytest.raises(ValueError, match="No release tags found"):
        PipelineQueue.resolve_latest_pipeline_version(pipeline="aind+ephys", base_directory=tmp_path)


@pytest.mark.ai_generated
def test_resolve_latest_lfp_version_is_the_installed_codebase_version() -> None:
    """The LFP pipeline ships in this package, so its latest version is this package's version."""
    with mock.patch(
        "dandi_compute_code.queue._pipeline_queue.importlib.metadata.version",
        return_value="1.2.3",
    ):
        latest_version = PipelineQueue.resolve_latest_pipeline_version(pipeline="lfp")

    assert latest_version == "v1.2.3"
