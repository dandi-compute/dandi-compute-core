import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import PipelineQueue

_ISSUE_EXAMPLE_PIPELINE_CONFIG = {
    "pipelines": {
        "aind+ephys": {
            "params": ["default"],
            "asset_overrides": {"048d1ee9-83b7-491f-8f02-1ca615b1d455": None},
        }
    }
}


@pytest.mark.ai_generated
def test_load_pipeline_config_validates_issue_example_schema(tmp_path: pathlib.Path) -> None:
    """Issue-provided pipeline config validates against the LinkML schema."""
    config_file = tmp_path / "pipeline_configs.json"
    config_file.write_text(json.dumps(_ISSUE_EXAMPLE_PIPELINE_CONFIG))

    with mock.patch("dandi_compute_code.queue._queue_utils._PACKAGED_PIPELINE_CONFIGS_PATH", config_file):
        loaded = PipelineQueue.load_pipeline_config()

    assert loaded == _ISSUE_EXAMPLE_PIPELINE_CONFIG
