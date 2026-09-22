import pytest

from dandi_compute_code.queue import AindEphysPipelineQueue, PipelineQueue


@pytest.mark.ai_generated
def test_aind_ephys_queue_derives_from_the_base_queue() -> None:
    """The AIND ephys queue is a pipeline specific flavour of the base queue."""
    assert issubclass(AindEphysPipelineQueue, PipelineQueue) is True


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("pipeline", "expected_class"),
    [
        pytest.param("aind+ephys", AindEphysPipelineQueue, id="aind_ephys"),
        pytest.param("lfp", PipelineQueue, id="lfp_falls_back_to_the_base"),
        pytest.param("does-not-exist", PipelineQueue, id="unclaimed_falls_back_to_the_base"),
    ],
)
def test_for_pipeline_resolves_the_owning_queue_class(pipeline: str, expected_class: type[PipelineQueue]) -> None:
    """A pipeline is owned by the subclass that claims it, and by the base class otherwise."""
    assert PipelineQueue.for_pipeline(pipeline) is expected_class


@pytest.mark.ai_generated
def test_subclass_inherits_the_container_behaviour() -> None:
    """Instantiating a subclass gives the same container the base class does."""
    queue = AindEphysPipelineQueue(entries=[])

    assert len(queue) == 0
    assert queue.pending == []
