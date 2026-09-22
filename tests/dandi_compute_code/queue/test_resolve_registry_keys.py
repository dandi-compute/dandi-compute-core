import importlib.resources
import json

import pytest

from dandi_compute_code.queue import PipelineQueue


def _registry_id(*, package: str, registry_file_name: str, key: str) -> str:
    """The 7-character ID a registry records for *key*."""
    registry_path = importlib.resources.files(package).joinpath(f"registries/{registry_file_name}")
    return json.loads(registry_path.read_text())[key]["md5"][:7]


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_aind_ephys_default() -> None:
    """resolve_params_key_to_id returns the 7-char hash for a known aind+ephys key."""
    result = PipelineQueue.resolve_params_key_to_id(pipeline="aind+ephys", params_key="default")
    expected = _registry_id(
        package="dandi_compute_code.aind_ephys_pipeline", registry_file_name="registered_params.json", key="default"
    )

    assert result == expected


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_lfp_default() -> None:
    """resolve_params_key_to_id resolves LFP keys against the LFP registry."""
    result = PipelineQueue.resolve_params_key_to_id(pipeline="lfp", params_key="default")
    expected = _registry_id(
        package="dandi_compute_code.lfp_pipeline", registry_file_name="registered_params.json", key="default"
    )

    assert result == expected


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_unknown_pipeline_returns_key() -> None:
    """resolve_params_key_to_id returns the key unchanged for an unknown pipeline."""
    result = PipelineQueue.resolve_params_key_to_id(pipeline="unknown-pipeline", params_key="default")

    assert result == "default"


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_already_hash_passthrough() -> None:
    """resolve_params_key_to_id returns the value unchanged if it is already an ID (not a registered key)."""
    result = PipelineQueue.resolve_params_key_to_id(pipeline="aind+ephys", params_key="98fd947")

    assert result == "98fd947"


@pytest.mark.ai_generated
def test_resolve_config_key_to_id_aind_ephys_default() -> None:
    """resolve_config_key_to_id returns the 7-char hash for a known aind+ephys config key."""
    result = PipelineQueue.resolve_config_key_to_id(pipeline="aind+ephys", config_key="default")
    expected = _registry_id(
        package="dandi_compute_code.aind_ephys_pipeline", registry_file_name="registered_configs.json", key="default"
    )

    assert result == expected


@pytest.mark.ai_generated
def test_resolve_config_key_to_id_lfp_has_no_config() -> None:
    """The LFP pipeline has no config of its own, which its job capsules record as empty."""
    result = PipelineQueue.resolve_config_key_to_id(pipeline="lfp", config_key="default")

    assert result == ""


@pytest.mark.ai_generated
def test_resolve_config_key_to_id_unknown_pipeline_returns_key() -> None:
    """resolve_config_key_to_id returns the key unchanged for an unknown pipeline."""
    result = PipelineQueue.resolve_config_key_to_id(pipeline="unknown-pipeline", config_key="default")

    assert result == "default"
