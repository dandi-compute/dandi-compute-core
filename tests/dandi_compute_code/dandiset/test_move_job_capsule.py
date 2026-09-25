"""
Unit tests for move_job_capsule and the ``dandicompute archive --job`` CLI option.
"""

import os
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import move_job_capsule

_SOURCE_DANDISET_ID = "001697"
_TARGET_DANDISET_ID = "001873"
_EXAMPLE_CAPSULE_PATH = (
    "derivatives/dandisets-000/dandiset-000409/sub-mouse01/pipeline-aind+ephys/"
    "version-v1.0_codebase-v0.3.0_params-default_config-abc123"
)


def _make_run_side_effect(*, upload_returncode: int = 0):
    """Build a subprocess.run side effect that materializes the downloaded trees."""

    def _side_effect(command: list, **kwargs: object) -> mock.MagicMock:
        result = mock.MagicMock()
        result.returncode = 0
        result.stdout = ""
        result.stderr = ""
        cwd = pathlib.Path(str(kwargs.get("cwd", ".")))
        if command[:3] == ["dandi", "download", "--preserve-tree"]:
            url = command[-1]
            _, relative = url.split(f"/{_SOURCE_DANDISET_ID}/", 1)
            capsule_dir = cwd / _SOURCE_DANDISET_ID / relative.strip("/")
            (capsule_dir / "code").mkdir(parents=True, exist_ok=True)
            (capsule_dir / "code" / "submit.sh").write_text("#!/bin/bash\n")
            (cwd / _SOURCE_DANDISET_ID / "dandiset.yaml").write_text("identifier: DANDI:001697\n")
        elif command[:4] == ["dandi", "download", "--download", "dandiset.yaml"]:
            (cwd / _TARGET_DANDISET_ID).mkdir(parents=True, exist_ok=True)
            (cwd / _TARGET_DANDISET_ID / "dandiset.yaml").write_text("identifier: DANDI:001873\n")
        elif command[:2] == ["dandi", "upload"]:
            result.returncode = upload_returncode
        return result

    return _side_effect


@pytest.mark.ai_generated
def test_move_job_capsule_raises_without_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """move_job_capsule raises RuntimeError when DANDI_API_KEY is not set."""
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH)


@pytest.mark.ai_generated
def test_move_job_capsule_downloads_source_with_preserve_tree(tmp_path: pathlib.Path) -> None:
    """move_job_capsule downloads the source capsule with --preserve-tree."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH)

    expected_url = f"dandi://dandi/{_SOURCE_DANDISET_ID}/{_EXAMPLE_CAPSULE_PATH}/"
    download_call = mock_run.call_args_list[0]
    assert download_call.args[0] == ["dandi", "download", "--preserve-tree", expected_url]
    assert download_call.kwargs.get("cwd") == scratch


@pytest.mark.ai_generated
def test_move_job_capsule_downloads_target_dandiset_yaml(tmp_path: pathlib.Path) -> None:
    """move_job_capsule fetches only the target dandiset.yaml before uploading."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH)

    metadata_call = mock_run.call_args_list[1]
    expected_url = f"dandi://dandi/{_TARGET_DANDISET_ID}/"
    assert metadata_call.args[0] == ["dandi", "download", "--download", "dandiset.yaml", expected_url]
    assert metadata_call.kwargs.get("cwd") == scratch


@pytest.mark.ai_generated
def test_move_job_capsule_copies_capsule_into_target_tree(tmp_path: pathlib.Path) -> None:
    """move_job_capsule copies the capsule subtree under the target Dandiset tree."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH, test=True)

    target_capsule = scratch / _TARGET_DANDISET_ID / _EXAMPLE_CAPSULE_PATH
    assert (target_capsule / "code" / "submit.sh").is_file()


@pytest.mark.ai_generated
def test_move_job_capsule_uploads_with_allow_any_path(tmp_path: pathlib.Path) -> None:
    """move_job_capsule uploads only the capsule subtree from the target Dandiset root."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH)

    upload_call = mock_run.call_args_list[2]
    assert upload_call.args[0] == ["dandi", "upload", "--allow-any-path", _EXAMPLE_CAPSULE_PATH]
    assert upload_call.kwargs.get("cwd") == scratch / _TARGET_DANDISET_ID


@pytest.mark.ai_generated
def test_move_job_capsule_deletes_source_after_successful_upload(tmp_path: pathlib.Path) -> None:
    """move_job_capsule deletes the source capsule only after a successful upload."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH)

    delete_call = mock_run.call_args_list[3]
    expected_source = scratch / _SOURCE_DANDISET_ID / _EXAMPLE_CAPSULE_PATH
    assert delete_call.args[0] == ["dandi", "delete", str(expected_source)]
    assert delete_call.kwargs.get("input") == b"y\n"


@pytest.mark.ai_generated
def test_move_job_capsule_cleans_up_scratch_on_success(tmp_path: pathlib.Path) -> None:
    """move_job_capsule removes the scratch directory when all steps succeed."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH)

    mock_rmtree.assert_called_once_with(scratch)


@pytest.mark.ai_generated
def test_move_job_capsule_preserves_scratch_in_test_mode(tmp_path: pathlib.Path) -> None:
    """move_job_capsule preserves the scratch directory after success in test mode."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH, test=True)

    mock_rmtree.assert_not_called()


@pytest.mark.ai_generated
def test_move_job_capsule_does_not_delete_source_when_upload_fails(tmp_path: pathlib.Path) -> None:
    """move_job_capsule never deletes the source nor cleans scratch when upload fails."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = _make_run_side_effect(upload_returncode=1)
        with pytest.raises(RuntimeError, match="dandi upload failed"):
            move_job_capsule(capsule_path=_EXAMPLE_CAPSULE_PATH)

    delete_calls = [call for call in mock_run.call_args_list if call.args[0][:2] == ["dandi", "delete"]]
    assert delete_calls == []
    mock_rmtree.assert_not_called()


@pytest.mark.ai_generated
def test_move_job_capsule_strips_surrounding_slashes(tmp_path: pathlib.Path) -> None:
    """move_job_capsule normalizes a path with surrounding slashes."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._move_job_capsule.tempfile.mkdtemp",
            return_value=str(scratch),
        ),
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.dandiset._move_job_capsule.shutil.rmtree"),
    ):
        mock_run.side_effect = _make_run_side_effect()
        move_job_capsule(capsule_path=f"/{_EXAMPLE_CAPSULE_PATH}/")

    expected_url = f"dandi://dandi/{_SOURCE_DANDISET_ID}/{_EXAMPLE_CAPSULE_PATH}/"
    assert mock_run.call_args_list[0].args[0][-1] == expected_url


@pytest.mark.ai_generated
def test_cli_archive_job_fails_without_api_key(base_directory: pathlib.Path) -> None:
    """CLI errors immediately when DANDI_API_KEY is missing."""
    runner = CliRunner()
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "--job", _EXAMPLE_CAPSULE_PATH, "--base", str(base_directory)],
        )
    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_archive_job_fails_without_dandi_devel(base_directory: pathlib.Path) -> None:
    """CLI errors when DANDI_DEVEL is missing, since move_job_capsule relies on `dandi upload`'s devel-only flags."""
    runner = CliRunner()
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "--job", _EXAMPLE_CAPSULE_PATH, "--base", str(base_directory)],
        )
    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output


@pytest.mark.ai_generated
def test_cli_archive_job_invokes_move(base_directory: pathlib.Path) -> None:
    """dandicompute archive --job calls move_job_capsule with the provided path."""
    runner = CliRunner()
    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.move_job_capsule") as mock_move,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "--job", _EXAMPLE_CAPSULE_PATH, "--base", str(base_directory)],
        )
    assert result.exit_code == 0, result.output
    assert "Archived job capsule" in result.output
    mock_move.assert_called_once_with(
        capsule_path=_EXAMPLE_CAPSULE_PATH,
        source_dandiset_id=_SOURCE_DANDISET_ID,
        target_dandiset_id=_TARGET_DANDISET_ID,
        base_directory=base_directory,
        test=False,
    )


@pytest.mark.ai_generated
def test_cli_archive_job_forwards_custom_dandiset_ids(base_directory: pathlib.Path) -> None:
    """dandicompute archive --job forwards --dandiset-id/--archive-dandiset-id to move_job_capsule."""
    runner = CliRunner()
    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.move_job_capsule") as mock_move,
    ):
        result = runner.invoke(
            _dandicompute_group,
            [
                "archive",
                "--job",
                _EXAMPLE_CAPSULE_PATH,
                "--dandiset-id",
                "000123",
                "--archive-dandiset-id",
                "000456",
                "--base",
                str(base_directory),
            ],
        )
    assert result.exit_code == 0, result.output
    mock_move.assert_called_once_with(
        capsule_path=_EXAMPLE_CAPSULE_PATH,
        source_dandiset_id="000123",
        target_dandiset_id="000456",
        base_directory=base_directory,
        test=False,
    )


@pytest.mark.ai_generated
def test_cli_archive_rejects_both_job_and_status(base_directory: pathlib.Path) -> None:
    """dandicompute archive rejects --job and --status given together."""
    runner = CliRunner()
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "--job", _EXAMPLE_CAPSULE_PATH, "--status", "failed", "--base", str(base_directory)],
        )
    assert result.exit_code != 0
    assert "Provide either --job PATH" in result.output


@pytest.mark.ai_generated
def test_cli_archive_rejects_neither_job_nor_status(base_directory: pathlib.Path) -> None:
    """dandicompute archive rejects being called with neither --job nor --status."""
    runner = CliRunner()
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, ["archive", "--base", str(base_directory)])
    assert result.exit_code != 0
    assert "Provide either --job PATH" in result.output
