"""Tests for what `dandicompute clean --work` deletes from the base directory's work/."""

import pathlib

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group


@pytest.mark.ai_generated
def test_cli_clean_work_keeps_the_image_cache_and_bazel(base_directory: pathlib.Path) -> None:
    """Everything in work/ goes except apptainer_cache/, which every capsule reuses, and bazel/."""
    work_directory = base_directory / "work"
    (work_directory / "job-1" / "input").mkdir(parents=True)
    (work_directory / "job-1" / "input" / "asset.nwb").write_text("data")
    (work_directory / "stray.txt").write_text("stray")
    (work_directory / "apptainer_cache").mkdir()
    (work_directory / "apptainer_cache" / "image.sif").write_text("image")
    (work_directory / "bazel" / "0123abcd").mkdir(parents=True)
    (work_directory / "bazel" / "0123abcd" / "command.log").write_text("building")

    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["clean", "--base", str(base_directory), "--work"])

    assert result.exit_code == 0, result.output
    remaining = sorted(path.relative_to(work_directory).as_posix() for path in work_directory.rglob("*"))
    assert remaining == [
        "apptainer_cache",
        "apptainer_cache/image.sif",
        "bazel",
        "bazel/0123abcd",
        "bazel/0123abcd/command.log",
    ]
