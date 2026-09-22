import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import PipelineQueue
from dandi_compute_code.queue._globals import _PACKAGED_PIPELINE_CONFIGS_PATH


@pytest.mark.ai_generated
def test_packaged_pipeline_configs_file_exists_and_validates() -> None:
    """The pipeline_configs.json packaged with this repo exists and validates."""
    assert _PACKAGED_PIPELINE_CONFIGS_PATH.exists()
    loaded = PipelineQueue.load_pipeline_config()
    assert "pipelines" in loaded
    assert loaded["pipelines"]


@pytest.mark.ai_generated
def test_load_pipeline_config_raises_when_packaged_config_fails_linkml_validation(tmp_path: pathlib.Path) -> None:
    """load_pipeline_config raises when the packaged pipeline config violates LinkML constraints."""
    invalid_config_file = tmp_path / "pipeline_configs.json"
    invalid_pipeline_config = {
        "pipelines": {
            # 'retries' is not an attribute the Pipeline class declares.
            "test": {"params": ["default"], "retries": 3}
        }
    }
    invalid_config_file.write_text(json.dumps(invalid_pipeline_config))

    with (
        mock.patch("dandi_compute_code.queue._queue_utils._PACKAGED_PIPELINE_CONFIGS_PATH", invalid_config_file),
        pytest.raises(ValueError, match="LinkML validation"),
    ):
        PipelineQueue.load_pipeline_config()


@pytest.mark.ai_generated
def test_load_pipeline_config_raises_when_packaged_config_missing(tmp_path: pathlib.Path) -> None:
    """load_pipeline_config raises FileNotFoundError when the packaged pipeline config is missing."""
    missing_config_file = tmp_path / "does-not-exist.json"

    with (
        mock.patch("dandi_compute_code.queue._queue_utils._PACKAGED_PIPELINE_CONFIGS_PATH", missing_config_file),
        pytest.raises(FileNotFoundError),
    ):
        PipelineQueue.load_pipeline_config()
