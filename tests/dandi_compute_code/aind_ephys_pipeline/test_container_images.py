"""Tests for listing and caching the container images the AIND ephys pipeline runs its steps in."""

import os
import pathlib
import stat
import subprocess

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.aind_ephys_pipeline import (
    aind_ephys_container_images,
    apptainer_cache_file_name,
    cache_container_images,
    missing_container_images,
)

_PIPELINE_TEXT = """\
def versions = parse_capsule_versions()

// container tag
params.container_tag = "si-${versions['SPIKEINTERFACE_VERSION']}"

process job_dispatch {
    def container_name = "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:${params.container_tag}"
    container container_name
}
process spikesort_kilosort4 {
    def container_name = "ghcr.io/allenneuraldynamics/aind-ephys-spikesort-kilosort4:${params.container_tag}"
    container container_name
}
process preprocessing {
    def container_name = "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:${params.container_tag}"
    container container_name
}
process nwb_units {
    def container_name = "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-nwb:${params.container_tag}"
    container container_name
}
"""

_BASE_IMAGE = "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:si-0.104.9"
_KILOSORT4_IMAGE = "ghcr.io/allenneuraldynamics/aind-ephys-spikesort-kilosort4:si-0.104.9"
_NWB_IMAGE = "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-nwb:si-0.104.9"


def _git(*args: str, cwd: pathlib.Path) -> None:
    subprocess.run(
        ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


def _commit_pipeline_version(
    *, pipeline_directory: pathlib.Path, tag: str, spikeinterface_version: str, pipeline_text: str = _PIPELINE_TEXT
) -> None:
    (pipeline_directory / "pipeline").mkdir(parents=True, exist_ok=True)
    (pipeline_directory / "pipeline" / "main_multi_backend.nf").write_text(pipeline_text)
    (pipeline_directory / "pipeline" / "capsule_versions.env").write_text(
        f'JOB_DISPATCH_REPO="https://github.com/AllenNeuralDynamics/aind-ephys-job-dispatch"\n'
        f"SPIKEINTERFACE_VERSION={spikeinterface_version}\n"
        f'EXTRA_INSTALLS=""\n'
    )
    _git("add", ".", cwd=pipeline_directory)
    _git("commit", "-m", tag, cwd=pipeline_directory)
    _git("tag", tag, cwd=pipeline_directory)


@pytest.fixture
def pipeline_base_directory(base_directory: pathlib.Path) -> pathlib.Path:
    """A base directory whose pipeline checkout carries two release tags, v1.2.2 and v1.2.4."""
    pipeline_directory = base_directory / "aind-ephys-pipeline"
    pipeline_directory.mkdir()
    _git("init", "--quiet", cwd=pipeline_directory)
    _commit_pipeline_version(pipeline_directory=pipeline_directory, tag="v1.2.2", spikeinterface_version="0.103.0")
    _commit_pipeline_version(pipeline_directory=pipeline_directory, tag="v1.2.4", spikeinterface_version="0.104.9")
    return base_directory


@pytest.fixture
def fake_apptainer(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """An ``apptainer`` on PATH whose ``pull`` writes the image reference to the output file."""
    bin_directory = tmp_path / "bin"
    bin_directory.mkdir()
    calls_file_path = tmp_path / "apptainer_calls.txt"
    script_path = bin_directory / "apptainer"
    script_path.write_text(
        "#!/bin/sh\n"
        f'echo "$*" >> "{calls_file_path}"\n'
        'if [ -n "$FAKE_APPTAINER_FAIL" ]; then echo partial > "$3"; exit 255; fi\n'
        'echo "$4" > "$3"\n'
    )
    script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{bin_directory}{os.pathsep}{os.environ['PATH']}")
    return calls_file_path


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("pipeline_version", "expected_images"),
    [
        ("v1.2.4", [_BASE_IMAGE, _KILOSORT4_IMAGE, _NWB_IMAGE]),
        (
            "v1.2.2",
            [
                "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:si-0.103.0",
                "ghcr.io/allenneuraldynamics/aind-ephys-spikesort-kilosort4:si-0.103.0",
                "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-nwb:si-0.103.0",
            ],
        ),
    ],
)
def test_container_images_are_read_from_the_requested_tag(
    pipeline_base_directory: pathlib.Path, pipeline_version: str, expected_images: list[str]
) -> None:
    """Each step image appears once, tagged from that version's capsule_versions.env."""
    images = aind_ephys_container_images(pipeline_version=pipeline_version, base_directory=pipeline_base_directory)

    assert images == expected_images


@pytest.mark.ai_generated
def test_container_images_do_not_check_out_the_requested_tag(pipeline_base_directory: pathlib.Path) -> None:
    """Reading an older tag leaves the checkout on whatever it was on, since capsules share it."""
    pipeline_directory = pipeline_base_directory / "aind-ephys-pipeline"
    head_before = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=pipeline_directory, text=True)

    aind_ephys_container_images(pipeline_version="v1.2.2", base_directory=pipeline_base_directory)

    head_after = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=pipeline_directory, text=True)
    assert head_after == head_before


@pytest.mark.ai_generated
def test_container_images_raise_for_an_unknown_tag(pipeline_base_directory: pathlib.Path) -> None:
    """A version the checkout does not have is reported rather than read as having no images."""
    with pytest.raises(RuntimeError, match="v9.9.9"):
        aind_ephys_container_images(pipeline_version="v9.9.9", base_directory=pipeline_base_directory)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("pipeline_text", "expected_message"),
    [
        (_PIPELINE_TEXT.replace("params.container_tag = ", "params.other_tag = "), "No `params.container_tag`"),
        (_PIPELINE_TEXT.replace("SPIKEINTERFACE_VERSION", "UNSET_VERSION"), "UNSET_VERSION"),
        ('params.container_tag = "si-1"\n', "No step images"),
    ],
)
def test_container_images_raise_when_the_pipeline_cannot_be_read(
    pipeline_base_directory: pathlib.Path, pipeline_text: str, expected_message: str
) -> None:
    """A pipeline whose layout changed fails loudly rather than caching nothing."""
    _commit_pipeline_version(
        pipeline_directory=pipeline_base_directory / "aind-ephys-pipeline",
        tag="v1.3.0",
        spikeinterface_version="0.105.0",
        pipeline_text=pipeline_text,
    )

    with pytest.raises(ValueError, match=expected_message):
        aind_ephys_container_images(pipeline_version="v1.3.0", base_directory=pipeline_base_directory)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("image", "expected_file_name"),
    [
        (_BASE_IMAGE, "ghcr.io-allenneuraldynamics-aind-ephys-pipeline-base-si-0.104.9.img"),
        (f"docker://{_BASE_IMAGE}", "ghcr.io-allenneuraldynamics-aind-ephys-pipeline-base-si-0.104.9.img"),
        ("ubuntu:22.04", "ubuntu-22.04.img"),
    ],
)
def test_apptainer_cache_file_name_matches_nextflow(image: str, expected_file_name: str) -> None:
    """The cache file name is the one Nextflow looks for before pulling an image itself."""
    file_name = apptainer_cache_file_name(image)

    assert file_name == expected_file_name


@pytest.mark.ai_generated
def test_missing_container_images_skips_cached_images(base_directory: pathlib.Path) -> None:
    """An image already in work/apptainer_cache/ is not reported as missing."""
    cache_directory = base_directory / "work" / "apptainer_cache"
    cache_directory.mkdir()
    (cache_directory / apptainer_cache_file_name(_BASE_IMAGE)).write_text("sif")

    missing = missing_container_images(images=[_BASE_IMAGE, _NWB_IMAGE], base_directory=base_directory)

    assert missing == [_NWB_IMAGE]


@pytest.mark.ai_generated
def test_cache_container_images_pulls_only_missing_images(
    base_directory: pathlib.Path, fake_apptainer: pathlib.Path
) -> None:
    """Missing images are pulled under Nextflow's file name, and cached ones are left alone."""
    cache_directory = base_directory / "work" / "apptainer_cache"
    cache_directory.mkdir()
    (cache_directory / apptainer_cache_file_name(_BASE_IMAGE)).write_text("sif")

    pulled = cache_container_images(images=[_BASE_IMAGE, _NWB_IMAGE], base_directory=base_directory)

    assert pulled == [_NWB_IMAGE]
    assert (cache_directory / apptainer_cache_file_name(_NWB_IMAGE)).read_text() == f"docker://{_NWB_IMAGE}\n"
    assert (cache_directory / apptainer_cache_file_name(_BASE_IMAGE)).read_text() == "sif"
    assert len(fake_apptainer.read_text().splitlines()) == 1
    assert list(cache_directory.glob("*.pulling.*")) == []


@pytest.mark.ai_generated
def test_cache_container_images_leaves_no_partial_image_on_failure(
    base_directory: pathlib.Path, fake_apptainer: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed pull raises and leaves nothing Nextflow could mistake for a cached image."""
    monkeypatch.setenv("FAKE_APPTAINER_FAIL", "1")
    cache_directory = base_directory / "work" / "apptainer_cache"

    with pytest.raises(RuntimeError, match="exited with code 255"):
        cache_container_images(images=[_BASE_IMAGE], base_directory=base_directory)

    assert sorted(path.name for path in cache_directory.iterdir()) == [".dandicompute-image-cache.lock"]


@pytest.mark.ai_generated
def test_images_missing_command_defaults_to_the_latest_tag(pipeline_base_directory: pathlib.Path) -> None:
    """Without --version the images of the latest local tag are listed, one per line."""
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["images", "missing", "--base", str(pipeline_base_directory)])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [_BASE_IMAGE, _KILOSORT4_IMAGE, _NWB_IMAGE]


@pytest.mark.ai_generated
def test_images_missing_command_prints_nothing_when_everything_is_cached(
    pipeline_base_directory: pathlib.Path, fake_apptainer: pathlib.Path
) -> None:
    """Once `images cache` has run, `images missing` prints nothing and still exits 0."""
    runner = CliRunner()
    cache_result = runner.invoke(
        _dandicompute_group, ["images", "cache", "--base", str(pipeline_base_directory), "--version", "v1.2.4"]
    )
    missing_result = runner.invoke(
        _dandicompute_group, ["images", "missing", "--base", str(pipeline_base_directory), "--silent"]
    )

    assert cache_result.exit_code == 0, cache_result.output
    assert missing_result.exit_code == 0, missing_result.output
    assert missing_result.stdout == ""
