import pathlib
import re

import pytest

import dandi_compute_code
from dandi_compute_code.queue import DispatchConfig, QueueState

_PIPELINE_CONFIG = {
    "pipelines": {
        "aind+ephys": {
            "params": ["default"],
            "dispatch": {"max_concurrent": 3, "max_array_tasks": 40},
        },
        "unbounded": {"params": ["default"], "dispatch": {"max_array_tasks": None}},
        "bare": {"params": ["default"]},
    }
}


@pytest.mark.ai_generated
def test_from_pipeline_config_reads_every_declared_setting() -> None:
    """A declared dispatch block is read field for field."""
    dispatch_config = DispatchConfig.from_pipeline_config(pipeline="aind+ephys", pipeline_config=_PIPELINE_CONFIG)

    assert dispatch_config.max_concurrent == 3
    assert dispatch_config.max_array_tasks == 40


@pytest.mark.ai_generated
def test_from_pipeline_config_falls_back_to_defaults_without_a_dispatch_block() -> None:
    """A pipeline declaring no dispatch block still dispatches, on the built-in defaults."""
    dispatch_config = DispatchConfig.from_pipeline_config(pipeline="bare", pipeline_config=_PIPELINE_CONFIG)

    assert dispatch_config == DispatchConfig(pipeline="bare")


@pytest.mark.ai_generated
def test_from_pipeline_config_reads_a_null_max_array_tasks_as_no_upper_bound() -> None:
    """An explicit null means no cap, as opposed to an omitted key which takes the default."""
    unbounded = DispatchConfig.from_pipeline_config(pipeline="unbounded", pipeline_config=_PIPELINE_CONFIG)
    omitted = DispatchConfig.from_pipeline_config(pipeline="bare", pipeline_config=_PIPELINE_CONFIG)

    assert unbounded.max_array_tasks is None
    assert omitted.max_array_tasks == 500


@pytest.mark.ai_generated
def test_max_array_tasks_accepts_none() -> None:
    """None is a valid value for the cap rather than a validation error."""
    assert DispatchConfig(pipeline="lfp", max_array_tasks=None).max_array_tasks is None


@pytest.mark.ai_generated
def test_from_pipeline_config_max_concurrent_override_wins_over_the_configured_limit() -> None:
    """An explicit max_concurrent replaces the configured per-pipeline limit."""
    dispatch_config = DispatchConfig.from_pipeline_config(
        pipeline="aind+ephys", pipeline_config=_PIPELINE_CONFIG, max_concurrent=9
    )

    assert dispatch_config.max_concurrent == 9
    assert dispatch_config.max_array_tasks == 40


@pytest.mark.ai_generated
def test_from_pipeline_config_raises_for_an_unconfigured_pipeline() -> None:
    """An unconfigured pipeline name is rejected, naming the ones that are configured."""
    with pytest.raises(ValueError, match="Pipeline 'nope' is not configured"):
        DispatchConfig.from_pipeline_config(pipeline="nope", pipeline_config=_PIPELINE_CONFIG)


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
    ],
)
def test_non_positive_counts_are_rejected(field_name: str, expected_message: str) -> None:
    """Counts that would make an unrunnable array are rejected at construction."""
    with pytest.raises(ValueError, match=expected_message):
        DispatchConfig(pipeline="lfp", **{field_name: 0})


@pytest.mark.ai_generated
@pytest.mark.parametrize("pipeline", ["aind+ephys", "lfp"])
def test_packaged_configuration_declares_dispatch_limits_for_every_pipeline(pipeline: str) -> None:
    """Every pipeline shipped in this repo carries its own dispatcher limits."""
    dispatch_config = DispatchConfig.from_pipeline_config(
        pipeline=pipeline, pipeline_config=QueueState.load_pipeline_config()
    )

    assert dispatch_config.max_concurrent >= 1


@pytest.mark.ai_generated
def test_packaged_configuration_declares_no_resource_settings() -> None:
    """Resources come from each pipeline's submission template, so the config must not carry them."""
    pipelines = QueueState.load_pipeline_config()["pipelines"]

    for pipeline_data in pipelines.values():
        assert set(pipeline_data.get("dispatch", {})) <= {"max_concurrent", "max_array_tasks"}


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("pipeline", "template_module"),
    [("aind+ephys", "aind_ephys_pipeline"), ("lfp", "lfp_pipeline")],
)
def test_resources_are_read_back_from_the_pipelines_submission_template(pipeline: str, template_module: str) -> None:
    """
    An array task runs its capsule, so its allocation has to match the capsule's own request.

    Reading the values back out of the template is what keeps the two from drifting apart, so
    this asserts against the template text rather than against hardcoded numbers.
    """
    template = (
        pathlib.Path(dandi_compute_code.__file__).parent / template_module / "templates" / "submission_template.txt"
    ).read_text()
    directives = dict(re.findall(r"^#SBATCH\s+--([A-Za-z-]+)(?:=|\s+)(\S+)\s*$", template, flags=re.MULTILINE))

    dispatch_config = DispatchConfig.from_pipeline_config(
        pipeline=pipeline, pipeline_config=QueueState.load_pipeline_config()
    )

    assert dispatch_config.memory == directives["mem"]
    assert dispatch_config.partition == directives["partition"]
    assert dispatch_config.time_limit == directives["time"]
    assert dispatch_config.cpus_per_task == int(directives["cpus-per-task"])


@pytest.mark.ai_generated
def test_resources_fall_back_when_a_pipeline_has_no_packaged_template() -> None:
    """
    A pipeline with no template still dispatches, on requests generous enough not to truncate.

    Under-provisioning an array task means the capsule inside it is killed mid-run, so the
    fallback deliberately errs high rather than low.
    """
    dispatch_config = DispatchConfig.from_pipeline_config(pipeline="bare", pipeline_config=_PIPELINE_CONFIG)

    assert dispatch_config.memory == "16GB"
    assert dispatch_config.time_limit == "48:00:00"
