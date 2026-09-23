"""
Unit tests for prepare_aind_ephys_job in the AIND ephys pipeline module.

Tests focus on BIDS entity parsing from Dandiset path components, which is the
logic that resolves the ``sub-`` label used in the output directory hierarchy.
"""

import datetime
import importlib.metadata
import json
import os
import pathlib
import re
import subprocess
from unittest import mock

import pytest

import dandi_compute_code
from dandi_compute_code.aind_ephys_pipeline._prepare_job import prepare_aind_ephys_job

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_COMMIT_HASH = "a" * 40


def _make_urlopen_mock(mapping: dict) -> mock.MagicMock:
    """Build an ``urllib.request.urlopen`` mock returning the mapping as JSON Lines.

    Each content ID becomes its own single-entry ``{content_id: {...}}`` line,
    matching the remote content-id-to-usage-dandiset-path cache format.
    """
    payload = "\n".join(json.dumps({content_id: value}) for content_id, value in mapping.items()).encode()
    response = mock.MagicMock()
    response.read.return_value = payload
    response.__enter__ = lambda s: s
    response.__exit__ = mock.MagicMock(return_value=False)
    return mock.MagicMock(return_value=response)


def _git_check_output(cmd, *, cwd=None, text=False, **kwargs):
    """Return plausible fake git output based on the subcommand."""
    if "describe" in cmd:
        return "v1.0.0-0-gaaaaaaa\n"
    return _FAKE_COMMIT_HASH + "\n"


@pytest.fixture()
def fake_base_directory(base_directory: pathlib.Path) -> pathlib.Path:
    """Create a base directory holding a minimal fake pipeline directory structure."""
    pipeline_dir = base_directory / "aind-ephys-pipeline"
    pipeline_dir.mkdir()
    main_nf = pipeline_dir / "pipeline" / "main_multi_backend.nf"
    main_nf.parent.mkdir(parents=True)
    main_nf.write_text("// fake nextflow pipeline")
    (pipeline_dir / "pipeline" / "capsule_versions.env").write_text("CAPSULE_VERSION=1\n")
    return base_directory


# ---------------------------------------------------------------------------
# Tests for BIDS entity parsing via directory components
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dandiset_path", "expected_sub", "expected_output_path"),
    [
        # Standard BIDS filename with sub- in stem
        ("sub-mouse01_ses-01_ecephys.nwb", "mouse01", "sub-mouse01_ses-01_ecephys"),
        # AIND-style: sub- only in the first directory component
        (
            "sub-703986_2024-09-13_11-19-19"
            "/ecephys_703986_2024-09-13_11-19-19"
            "/ecephys_703986_2024-09-13_11-19-19.nwb",
            "703986",
            "sub-703986_2024-09-13_11-19-19/ecephys_703986_2024-09-13_11-19-19/ecephys_703986_2024-09-13_11-19-19",
        ),
        # sub- in directory alongside an explicit ses- directory
        (
            "sub-mouse01/ses-20240101/sub-mouse01_ses-20240101_ecephys.nwb",
            "mouse01",
            "sub-mouse01/ses-20240101/sub-mouse01_ses-20240101_ecephys",
        ),
    ],
)
def test_prepare_aind_ephys_job_extracts_sub_entity_from_path(
    dandiset_path: str,
    expected_sub: str,
    expected_output_path: str,
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """prepare_aind_ephys_job resolves the 'sub' entity from directory parts when absent in the filename."""
    content_id = "04000000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": dandiset_path}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)) as mock_mkdtemp,
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=content_id,
            config_key="default",
            parameters_key="original",
            base_directory=fake_base_directory,
        )

    mock_mkdtemp.assert_called_once_with(dir=fake_base_directory / "processing", prefix="prepare-job-")
    assert f"sub-{expected_sub}" in str(script_path)
    assert expected_output_path in str(script_path)


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_test_content_id_uses_sub_test(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """The test content ID (sourcedata/aind-sample.nwb) gets sub='test' injected as a special case."""
    # The production mapping for this ID is {'001849': 'sourcedata/aind-sample.nwb'},
    # which has no sub- entity in any path component.
    test_content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    mapping = {test_content_id: {"001849": "sourcedata/aind-sample.nwb"}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=test_content_id,
            config_key="default",
            parameters_key="original",
            base_directory=fake_base_directory,
        )

    assert "sourcedata/aind-sample" in str(script_path)
    # Older styles
    assert "sub-test/" not in str(script_path)
    assert "_date-" not in str(script_path)


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_raises_on_missing_sub_entity(tmp_path: pathlib.Path) -> None:
    """prepare_aind_ephys_job raises a clear ValueError when no 'sub' entity can be extracted from the path."""
    content_id = "05000000-0000-0000-0000-000000000000"
    no_sub_path = "ecephys_703986_2024-09-13_11-19-19/ecephys_703986_2024-09-13.nwb"
    mapping = {content_id: {"000001": no_sub_path}}

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        pytest.raises(ValueError, match="Could not extract 'sub' BIDS entity"),
    ):
        prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=content_id,
            config_key="default",
            parameters_key="original",
            base_directory=tmp_path,
        )


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_uses_simplified_job_id_format(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """prepare_aind_ephys_job names the job capsule directory `job-{YYMMDD}+{hash}` and nothing else."""
    content_id = "07000000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": "sub-mouse01/sub-mouse01_ecephys.nwb"}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=content_id,
            config_key="default",
            parameters_key="original",
            base_directory=fake_base_directory,
        )

    capsule_dir_name = pathlib.Path(str(script_path)).parent.parent.name
    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    assert re.fullmatch(rf"job-{today:%y%m%d}[0-9a-f]{{6}}", capsule_dir_name) is not None
    assert pathlib.Path(str(script_path)).parent.parent.parent.name == "pipeline-aind+ephys"


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_writes_job_provenance_in_dataset_description(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """dataset_description.json records everything the capsule directory name no longer spells out."""
    content_id = "07100000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": "sub-mouse01/sub-mouse01_ecephys.nwb"}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=content_id,
            config_key="default",
            parameters_key="original",
            base_directory=fake_base_directory,
        )

    capsule_directory = script_path.parent.parent
    dataset_description = json.loads((capsule_directory / "dataset_description.json").read_text())
    provenance = dataset_description["DandiCompute"]

    assert provenance["job_id"] == capsule_directory.name
    assert provenance["dandiset_id"] == "000001"
    assert provenance["within_dandiset_path"] == "sub-mouse01/sub-mouse01_ecephys.nwb"
    assert provenance["content_id"] == content_id
    assert provenance["pipeline"] == "aind+ephys"
    assert provenance["version"] == "v1.1.0"
    assert provenance["codebase"] == f"v{importlib.metadata.version('dandi-compute-code')}"
    assert provenance["params_key"] == "original"
    assert provenance["config_key"] == "default"
    assert re.fullmatch(r"[0-9a-f]{7}", provenance["params"]) is not None
    assert re.fullmatch(r"[0-9a-f]{7}", provenance["config"]) is not None


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_skips_when_a_capsule_already_exists(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """A capsule prepared on an earlier date is recognised by its job hash, so nothing is formed twice."""
    content_id = "07200000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": "sub-mouse01/sub-mouse01_ecephys.nwb"}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    def _run(existing_asset_paths: list[str]) -> pathlib.Path | None:
        mock_dandiset.get_assets_with_path_prefix.return_value = iter(
            [mock.MagicMock(path=path) for path in existing_asset_paths]
        )
        with (
            mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
            mock.patch("subprocess.check_output", side_effect=_git_check_output),
            mock.patch(
                "dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient"
            ) as mock_client,
            mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
            mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
            mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
            mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
        ):
            mock_client.return_value.get_dandiset.return_value = mock_dandiset
            return prepare_aind_ephys_job(
                pipeline_version="v1.1.0",
                content_id=content_id,
                config_key="default",
                parameters_key="original",
                base_directory=fake_base_directory,
            )

    script_path = _run([])
    assert script_path is not None

    capsule_directory = script_path.parent.parent
    pipeline_path = "derivatives/dandisets-000/dandiset-000001/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys"
    job_hash = capsule_directory.name.removeprefix("job-")[6:]
    existing_capsule = f"job-200101{job_hash}"

    assert _run([f"{pipeline_path}/{existing_capsule}/code/submit.sh"]) is None


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_writes_codebase_version_in_dataset_description(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """dataset_description.json records the installed codebase version with a leading v."""
    content_id = "07500000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": "sub-mouse01/sub-mouse01_ecephys.nwb"}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=content_id,
            config_key="default",
            parameters_key="original",
            base_directory=fake_base_directory,
        )

    output_directory = script_path.parent.parent
    dataset_description_path = output_directory / "dataset_description.json"
    dataset_description = json.loads(dataset_description_path.read_text())
    codebase_entry = next(
        (entry for entry in dataset_description["GeneratedBy"] if entry["Name"] == "DANDI Compute: Code"),
        None,
    )
    assert codebase_entry is not None
    expected_version = f"v{importlib.metadata.version('dandi-compute-code')}+{_FAKE_COMMIT_HASH}"
    assert codebase_entry["Version"] == expected_version


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_accepts_matching_minor_version_params(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """Parameters files remain compatible when only the pipeline patch version differs."""
    content_id = "08000000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": "sub-mouse01/sub-mouse01_ecephys.nwb"}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.1.5",
            content_id=content_id,
            config_key="default",
            parameters_key="original",
            base_directory=fake_base_directory,
        )

    assert script_path.exists()


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_accepts_newer_pipeline_minor_version_for_default_params(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """Parameters remain compatible with newer pipeline minor versions in the same major series."""
    content_id = "09000000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": "sub-mouse01/sub-mouse01_ecephys.nwb"}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.3.1",
            content_id=content_id,
            config_key="default",
            parameters_key="default",
            base_directory=fake_base_directory,
        )

    assert script_path.exists()


@pytest.mark.ai_generated
def test_newer_params_rejected_early(tmp_path: pathlib.Path) -> None:
    """Newer parameters versions are rejected before any content lookup work."""
    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        pytest.raises(ValueError, match="targets pipeline version .* newer than requested pipeline version"),
    ):
        prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            dandiset_id="000001",
            dandiset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
            config_key="default",
            parameters_key="default",
            base_directory=tmp_path,
        )

    mock_client.assert_not_called()
    mock_urlopen.assert_not_called()


@pytest.mark.ai_generated
def test_different_major_params_rejected_early(tmp_path: pathlib.Path) -> None:
    """Different-major parameters versions are rejected before any content lookup work."""
    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        pytest.raises(ValueError, match="different major series than requested pipeline version"),
    ):
        prepare_aind_ephys_job(
            pipeline_version="v2.0.0",
            dandiset_id="000001",
            dandiset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
            config_key="default",
            parameters_key="default",
            base_directory=tmp_path,
        )

    mock_client.assert_not_called()
    mock_urlopen.assert_not_called()


_PACKAGE_DIRECTORY = pathlib.Path(dandi_compute_code.__file__).parent


def _prepare_test_content_id_job(*, tmp_path: pathlib.Path, base_directory: pathlib.Path, check_output) -> None:
    """Prepare a job for the test content ID with every external call mocked, and *check_output* standing in for git."""
    test_content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    mapping = {test_content_id: {"001849": "sourcedata/aind-sample.nwb"}}
    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()
    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset
        prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=test_content_id,
            config_key="default",
            parameters_key="original",
            base_directory=base_directory,
        )


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_reads_the_codebase_commit_from_the_package_location(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """The codebase commit is read from the installed package's own checkout, not from a path in the base directory."""
    git_working_directories: list[pathlib.Path] = []

    def _recording_check_output(cmd, *, cwd=None, text=False, **kwargs):
        if "rev-parse" in cmd:
            git_working_directories.append(pathlib.Path(cwd))
        return _git_check_output(cmd, cwd=cwd, text=text, **kwargs)

    _prepare_test_content_id_job(
        tmp_path=tmp_path, base_directory=fake_base_directory, check_output=_recording_check_output
    )

    assert _PACKAGE_DIRECTORY in git_working_directories


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_explains_a_codebase_that_is_not_a_git_checkout(
    tmp_path: pathlib.Path,
    fake_base_directory: pathlib.Path,
) -> None:
    """A package not running from a git checkout fails with a message pointing at the editable install."""

    def _check_output_without_codebase_checkout(cmd, *, cwd=None, text=False, **kwargs):
        if "rev-parse" in cmd and pathlib.Path(cwd) == _PACKAGE_DIRECTORY:
            raise subprocess.CalledProcessError(returncode=128, cmd=cmd)
        return _git_check_output(cmd, cwd=cwd, text=text, **kwargs)

    with pytest.raises(RuntimeError, match="installed editable"):
        _prepare_test_content_id_job(
            tmp_path=tmp_path, base_directory=fake_base_directory, check_output=_check_output_without_codebase_checkout
        )
