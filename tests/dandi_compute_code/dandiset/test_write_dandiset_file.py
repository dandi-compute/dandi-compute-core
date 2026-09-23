"""
Unit tests for write_dandiset_file.
"""

import os
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.dandiset import write_dandiset_file

_DANDISET_ID = "001697"
_RELATIVE_PATH = "derivatives/jobs.tsv"
_CONTENT = "dandiset_id\twithin_dandiset_path\n001849\tsub-mouse01/sub-mouse01_ecephys.nwb\n"


def _make_run_side_effect(*, upload_returncode: int = 0):
    """Build a subprocess.run side effect that materializes the dandiset.yaml download."""

    def _side_effect(command: list, **kwargs: object) -> mock.MagicMock:
        result = mock.MagicMock()
        result.returncode = 0
        result.stdout = ""
        result.stderr = ""
        cwd = pathlib.Path(str(kwargs.get("cwd", ".")))
        if command[:4] == ["dandi", "download", "--download", "dandiset.yaml"]:
            (cwd / _DANDISET_ID).mkdir(parents=True, exist_ok=True)
            (cwd / _DANDISET_ID / "dandiset.yaml").write_text("identifier: DANDI:001697\n")
        elif command[:2] == ["dandi", "upload"]:
            result.returncode = upload_returncode
        return result

    return _side_effect


@pytest.mark.ai_generated
def test_write_dandiset_file_raises_without_dandi_api_key() -> None:
    """write_dandiset_file raises RuntimeError when DANDI_API_KEY is not set."""
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=_RELATIVE_PATH, content=_CONTENT)


@pytest.mark.ai_generated
def test_write_dandiset_file_downloads_only_dandiset_yaml(tmp_path: pathlib.Path) -> None:
    """write_dandiset_file downloads only dandiset.yaml before writing/uploading the file."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._write_dandiset_file.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=_RELATIVE_PATH, content=_CONTENT)

    expected_url = f"dandi://dandi/{_DANDISET_ID}/"
    download_call = mock_run.call_args_list[0]
    assert download_call.args[0] == ["dandi", "download", "--download", "dandiset.yaml", expected_url]
    assert download_call.kwargs.get("cwd") == scratch


@pytest.mark.ai_generated
def test_write_dandiset_file_writes_content_before_upload(tmp_path: pathlib.Path) -> None:
    """write_dandiset_file materializes *content* at *relative_path* before uploading."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._write_dandiset_file.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=_RELATIVE_PATH, content=_CONTENT)

    written_file = scratch / _DANDISET_ID / _RELATIVE_PATH
    assert written_file.read_text() == _CONTENT


@pytest.mark.ai_generated
def test_write_dandiset_file_uploads_with_allow_any_path(tmp_path: pathlib.Path) -> None:
    """write_dandiset_file uploads only the target file from the Dandiset root."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._write_dandiset_file.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=_RELATIVE_PATH, content=_CONTENT)

    upload_call = mock_run.call_args_list[1]
    assert upload_call.args[0] == ["dandi", "upload", "--allow-any-path", _RELATIVE_PATH]
    assert upload_call.kwargs.get("cwd") == scratch / _DANDISET_ID


@pytest.mark.ai_generated
def test_write_dandiset_file_cleans_up_scratch_on_success(tmp_path: pathlib.Path) -> None:
    """write_dandiset_file removes the scratch directory when the upload succeeds."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._write_dandiset_file.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = _make_run_side_effect()
        write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=_RELATIVE_PATH, content=_CONTENT)

    mock_rmtree.assert_called_once_with(scratch)


@pytest.mark.ai_generated
def test_write_dandiset_file_preserves_scratch_in_test_mode(tmp_path: pathlib.Path) -> None:
    """write_dandiset_file preserves the scratch directory after success in test mode."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._write_dandiset_file.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = _make_run_side_effect()
        write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=_RELATIVE_PATH, content=_CONTENT, test=True)

    mock_rmtree.assert_not_called()


@pytest.mark.ai_generated
def test_write_dandiset_file_raises_when_upload_fails(tmp_path: pathlib.Path) -> None:
    """write_dandiset_file raises RuntimeError and preserves scratch when upload fails."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._write_dandiset_file.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = _make_run_side_effect(upload_returncode=1)
        with pytest.raises(RuntimeError, match="dandi upload failed"):
            write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=_RELATIVE_PATH, content=_CONTENT)

    mock_rmtree.assert_not_called()


@pytest.mark.ai_generated
def test_write_dandiset_file_strips_surrounding_slashes(tmp_path: pathlib.Path) -> None:
    """write_dandiset_file normalizes a relative_path with surrounding slashes."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._write_dandiset_file.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._write_dandiset_file.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        write_dandiset_file(dandiset_id=_DANDISET_ID, relative_path=f"/{_RELATIVE_PATH}/", content=_CONTENT)

    upload_call = mock_run.call_args_list[1]
    assert upload_call.args[0] == ["dandi", "upload", "--allow-any-path", _RELATIVE_PATH]
