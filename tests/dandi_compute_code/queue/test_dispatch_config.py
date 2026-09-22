import pytest

from dandi_compute_code.queue import DispatchConfig, QueueState

_QUEUE_CONFIG = {
    "pipelines": {
        "aind+ephys": {
            "params": ["default"],
            "dispatch": {
                "max_concurrent": 3,
                "max_array_tasks": 40,
                "partition": "mit_normal",
                "memory": "8GB",
                "cpus_per_task": 4,
                "time_limit": "06:00:00",
            },
        },
        "bare": {"params": ["default"]},
    }
}


@pytest.mark.ai_generated
def test_from_queue_config_reads_every_declared_setting() -> None:
    """A declared dispatch block is read field for field."""
    dispatch_config = DispatchConfig.from_queue_config(pipeline="aind+ephys", queue_config=_QUEUE_CONFIG)

    assert dispatch_config.max_concurrent == 3
    assert dispatch_config.max_array_tasks == 40
    assert dispatch_config.partition == "mit_normal"
    assert dispatch_config.memory == "8GB"
    assert dispatch_config.cpus_per_task == 4
    assert dispatch_config.time_limit == "06:00:00"


@pytest.mark.ai_generated
def test_from_queue_config_falls_back_to_defaults_without_a_dispatch_block() -> None:
    """A pipeline declaring no dispatch block still dispatches, on the built-in defaults."""
    dispatch_config = DispatchConfig.from_queue_config(pipeline="bare", queue_config=_QUEUE_CONFIG)

    assert dispatch_config == DispatchConfig(pipeline="bare")


@pytest.mark.ai_generated
def test_from_queue_config_max_concurrent_override_wins_over_the_configured_limit() -> None:
    """An explicit max_concurrent replaces the configured per-pipeline limit."""
    dispatch_config = DispatchConfig.from_queue_config(
        pipeline="aind+ephys", queue_config=_QUEUE_CONFIG, max_concurrent=9
    )

    assert dispatch_config.max_concurrent == 9
    assert dispatch_config.max_array_tasks == 40


@pytest.mark.ai_generated
def test_from_queue_config_raises_for_an_unconfigured_pipeline() -> None:
    """An unconfigured pipeline name is rejected, naming the ones that are configured."""
    with pytest.raises(ValueError, match="Pipeline 'nope' is not configured"):
        DispatchConfig.from_queue_config(pipeline="nope", queue_config=_QUEUE_CONFIG)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("pipeline", "expected_job_name"),
    [
        ("aind+ephys", "dandicompute-dispatch-aind-ephys"),
        ("lfp", "dandicompute-dispatch-lfp"),
        ("some pipeline", "dandicompute-dispatch-some-pipeline"),
    ],
)
def test_job_name_folds_characters_a_slurm_job_name_should_not_carry(pipeline: str, expected_job_name: str) -> None:
    """The dispatcher job name is the pipeline name with awkward characters folded to hyphens."""
    assert DispatchConfig(pipeline=pipeline).job_name == expected_job_name


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("task_count", "max_concurrent", "expected_specification"),
    [(1, 2, "1-1%2"), (12, 2, "1-12%2"), (500, 25, "1-500%25")],
)
def test_array_specification_is_one_based_and_carries_the_concurrency_throttle(
    task_count: int, max_concurrent: int, expected_specification: str
) -> None:
    """Array indices start at 1 so a task index reads as a manifest line number."""
    dispatch_config = DispatchConfig(pipeline="lfp", max_concurrent=max_concurrent)

    assert dispatch_config.array_specification(task_count) == expected_specification


@pytest.mark.ai_generated
def test_array_specification_rejects_an_empty_array() -> None:
    """An array with no tasks is not a valid specification."""
    with pytest.raises(ValueError, match="needs at least one task"):
        DispatchConfig(pipeline="lfp").array_specification(0)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("field_name", "expected_message"),
    [
        ("max_concurrent", "max_concurrent must be at least 1"),
        ("max_array_tasks", "max_array_tasks must be at least 1"),
        ("cpus_per_task", "cpus_per_task must be at least 1"),
    ],
)
def test_non_positive_counts_are_rejected(field_name: str, expected_message: str) -> None:
    """Counts that would make an unrunnable array are rejected at construction."""
    with pytest.raises(ValueError, match=expected_message):
        DispatchConfig(pipeline="lfp", **{field_name: 0})


@pytest.mark.ai_generated
@pytest.mark.parametrize("pipeline", ["aind+ephys", "lfp"])
def test_packaged_configuration_declares_dispatch_settings_for_every_pipeline(pipeline: str) -> None:
    """Every pipeline shipped in this repo carries its own dispatcher settings."""
    dispatch_config = DispatchConfig.from_queue_config(
        pipeline=pipeline, queue_config=QueueState.load_queue_config()
    )

    assert dispatch_config.max_concurrent >= 1
    assert dispatch_config.partition != ""
