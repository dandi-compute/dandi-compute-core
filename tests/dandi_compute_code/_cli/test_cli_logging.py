"""Logging behaviour shared by every CLI command."""

import logging
from collections.abc import Iterator
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group


@pytest.fixture
def linkml_runtime_logger() -> Iterator[logging.Logger]:
    """
    The ``linkml_runtime`` logger under a root logger at ``INFO``, as a command leaves it.

    pytest installs its own root handler, which makes the command's ``basicConfig`` a no-op,
    so the root level the command would set is applied here instead. Both levels are
    restored afterwards.
    """
    root_logger = logging.getLogger()
    original_root_level = root_logger.level
    root_logger.setLevel(logging.INFO)
    logger = logging.getLogger("linkml_runtime")
    logger.setLevel(logging.NOTSET)
    yield logger
    logger.setLevel(logging.NOTSET)
    root_logger.setLevel(original_root_level)


@pytest.mark.ai_generated
def test_cli_quiets_linkml_runtime_info_but_keeps_its_warnings(linkml_runtime_logger: logging.Logger) -> None:
    """A command hides linkml_runtime's schema import chatter while still surfacing its warnings."""
    with mock.patch("dandi_compute_code._cli._dandicompute_group.PipelineQueue.has_pending_jobs", return_value=True):
        result = CliRunner().invoke(_dandicompute_group, ["jobs", "pending"])

    schemaview_logger = logging.getLogger("linkml_runtime.utils.schemaview")
    assert result.exit_code == 0, result.output
    assert schemaview_logger.isEnabledFor(logging.INFO) is False
    assert schemaview_logger.isEnabledFor(logging.WARNING) is True
