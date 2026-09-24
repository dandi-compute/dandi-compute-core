"""
The SLURM resources each pending job capsule asks for.

An array task runs its capsule directly, so the array's allocation has to cover what that
capsule's own ``#SBATCH`` header requests. Pipelines differ in shape here for real reasons.
The AIND submission script is a Nextflow driver that dispatches the heavy work to its own
jobs and needs very little itself, while the LFP script does its work in process and needs
a lot. Capsules can also differ inside one pipeline when a template changed between the
releases that prepared them.

This module reads each pending capsule's ``code/submit.sh`` back out of the archive so the
dispatcher can group capsules that agree and give each group an array sized for it.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import logging

import beartype

from ._globals import _SBATCH_DIRECTIVE_RE
from ._queue_utils import _read_asset_text
from ..dandiset._load_assets_jsonld_metadata import AssetsJsonldMetadata, load_assets_jsonld_metadata

_log = logging.getLogger(__name__)

#: How many capsule scripts to read at once. Each is a small file, so the cost is latency.
_MAX_READ_WORKERS = 8


@beartype.beartype
@dataclasses.dataclass(frozen=True)
class CapsuleResources:
    """
    What one capsule's submission script asks SLURM for.

    Frozen so that capsules requesting the same thing compare and hash equal, which is what
    the dispatcher groups them by.
    """

    memory: str
    cpus_per_task: int
    partition: str
    time_limit: str
    signal: str = ""

    def describe(self) -> str:
        """These requests on one line, for logs and the dispatch summary."""
        cpu_noun = "CPU" if self.cpus_per_task == 1 else "CPUs"
        description = f"{self.memory} / {self.cpus_per_task} {cpu_noun} / {self.partition} / {self.time_limit}"
        return description

    @classmethod
    def from_submission_script(cls, script: str, /) -> CapsuleResources | None:
        """
        Read the resource directives out of a submission script.

        Returns ``None`` when the script declares none of them, which leaves the caller on
        whatever default it would otherwise have used.
        """
        directives = {
            match.group("name"): match.group("value")
            for match in _SBATCH_DIRECTIVE_RE.finditer(script)
            if match.group("name") in {"mem", "cpus-per-task", "partition", "time", "signal"}
        }
        if not directives:
            return None

        cpus_per_task = directives.get("cpus-per-task")
        try:
            parsed_cpus_per_task = int(cpus_per_task) if cpus_per_task is not None else 1
        except ValueError:
            _log.warning("Unreadable cpus-per-task %r in a submission script; using 1", cpus_per_task)
            parsed_cpus_per_task = 1

        resources = cls(
            memory=directives.get("mem", ""),
            cpus_per_task=parsed_cpus_per_task,
            partition=directives.get("partition", ""),
            time_limit=directives.get("time", ""),
            signal=directives.get("signal", ""),
        )
        return resources


@beartype.beartype
def _read_one(*, code_dir_path: str, metadata: AssetsJsonldMetadata) -> CapsuleResources | None:
    """Read one capsule's requested resources out of its ``submit.sh`` asset."""
    asset_metadata = metadata.path_to_asset_metadata.get(f"{code_dir_path}/submit.sh")
    if asset_metadata is None:
        return None
    asset = metadata.content_id_to_asset.get(asset_metadata.content_id)
    if asset is None:
        return None
    script = _read_asset_text(asset)
    if script is None:
        return None
    return CapsuleResources.from_submission_script(script)


@beartype.beartype
def read_capsule_resources(
    code_dir_paths: list[str],
    /,
    *,
    metadata: AssetsJsonldMetadata | None = None,
) -> dict[str, CapsuleResources]:
    """
    The resources each of *code_dir_paths* asks for, keyed by capsule ``code`` directory path.

    A capsule whose script cannot be read is left out rather than guessed at, so the caller
    falls back to its pipeline's own defaults for it.

    Parameters
    ----------
    code_dir_paths : list of str
        Capsule ``code`` directory paths, relative to the Dandiset root.
    metadata : AssetsJsonldMetadata, optional
        Already loaded assets metadata. Fetched when not supplied.
    """
    if not code_dir_paths:
        return {}

    resolved_metadata = metadata if metadata is not None else load_assets_jsonld_metadata()

    resources_by_code_dir: dict[str, CapsuleResources] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(_MAX_READ_WORKERS, len(code_dir_paths))) as executor:
        read = [
            executor.submit(_read_one, code_dir_path=code_dir_path, metadata=resolved_metadata)
            for code_dir_path in code_dir_paths
        ]
        for code_dir_path, future in zip(code_dir_paths, read):
            resources = future.result()
            if resources is not None:
                resources_by_code_dir[code_dir_path] = resources

    unreadable = len(code_dir_paths) - len(resources_by_code_dir)
    if unreadable:
        _log.info(
            "Could not read requested resources for %d capsule(s); they fall back to pipeline defaults", unreadable
        )
    return resources_by_code_dir
