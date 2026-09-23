"""
AindEphysPipelineQueue — the ``aind+ephys`` flavour of :class:`~._pipeline_queue.PipelineQueue`.

The AIND ephys pipeline lives in its own repository, checked out next to this one under the
structured base directory, and carries a config registry of its own. Those are the only things that set it
apart from the pipeline agnostic queue, so this subclass is just the hooks that differ.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import ClassVar

import beartype

from ._fetch_qualifying_aind_content_ids import _fetch_qualifying_aind_content_ids
from ._globals import _CONFIGS_REGISTRIES

# Sphinx resolves the inherited ``entries`` annotation against this module, so the name it
# refers to has to be importable from here.
from ._job_capsule import JobCapsule  # noqa: F401
from ._pipeline_queue import PipelineQueue
from ._queue_utils import _latest_repository_version_tag
from .._base_directory import _DEFAULT_BASE_DIRECTORY, _aind_pipeline_directory
from ..aind_ephys_pipeline import prepare_aind_ephys_job


@beartype.beartype
@dataclass
class AindEphysPipelineQueue(PipelineQueue):
    """Queue behaviour for the AIND ephys pipeline."""

    pipelines: ClassVar[tuple[str, ...]] = ("aind+ephys",)

    @classmethod
    def _resolve_latest_pipeline_version(cls, *, base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY) -> str:
        """
        The highest release tag in the local AIND ephys pipeline checkout.

        Parameters
        ----------
        base_directory : pathlib.Path, optional
            The structured base directory holding the AIND ephys pipeline
            checkout. Defaults to the one on MIT Engaging.
        """
        latest_version = _latest_repository_version_tag(_aind_pipeline_directory(base_directory))
        return latest_version

    @classmethod
    def _resolve_config_key_to_id(cls, *, pipeline: str, config_key: str) -> str:
        """
        Resolve *config_key* against the AIND ephys config registry.

        A key that is already a raw hash ID resolves to itself.
        """
        entry = _CONFIGS_REGISTRIES.get(pipeline, {}).get(config_key)
        config_id = entry["md5"][:7] if entry else config_key
        return config_id

    @classmethod
    def _fetch_qualifying_content_ids(cls) -> list[str]:
        """Content IDs qualifying for the AIND ephys pipeline, in no particular order."""
        content_ids = _fetch_qualifying_aind_content_ids()
        return content_ids

    @classmethod
    def _prepare_job(
        cls,
        *,
        content_id: str,
        parameters_key: str,
        pipeline_version: str,
        base_directory: pathlib.Path,
        config_key: str,
        force_new_capsule: bool,
    ) -> pathlib.Path | None:
        """
        Form one AIND ephys job capsule and return its submission script, or ``None`` when a
        capsule already exists on the archive.
        """
        script_file_path = prepare_aind_ephys_job(
            content_id=content_id,
            parameters_key=parameters_key,
            pipeline_version=pipeline_version,
            base_directory=base_directory,
            config_key=config_key,
            force_new_capsule=force_new_capsule,
            silent=True,
        )
        return script_file_path
