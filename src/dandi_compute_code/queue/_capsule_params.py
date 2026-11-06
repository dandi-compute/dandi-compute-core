"""
The parameter set each pending job capsule was formed with.

A pipeline can dispatch one of its parameter sets apart from the rest, under limits of its
own. Which dispatcher a pending capsule belongs to is therefore decided by its parameters,
which its path does not spell out. This module reads the parameters ID back out of each
pending capsule's ``dataset_description.json`` provenance, as the jobs table does.
"""

from __future__ import annotations

import logging

import beartype

from ._queue_utils import _CapsuleProvenanceCache
from ..dandiset._load_assets_jsonld_metadata import AssetsJsonldMetadata, load_assets_jsonld_metadata

_log = logging.getLogger(__name__)


@beartype.beartype
def read_capsule_params(
    code_dir_paths: list[str],
    /,
    *,
    metadata: AssetsJsonldMetadata | None = None,
) -> dict[str, str]:
    """
    The parameters ID each of *code_dir_paths* was formed with, keyed by capsule ``code`` directory path.

    The ID is the MD5 prefix recorded in the capsule's provenance, the same value the jobs
    table reports. A capsule whose provenance cannot be read, or records no parameters, is
    left out rather than guessed at, so the caller falls back to its pipeline's own
    dispatcher for it.

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
    provenance_cache = _CapsuleProvenanceCache(resolved_metadata)
    capsule_paths = {code_dir_path: code_dir_path.removesuffix("/code") for code_dir_path in code_dir_paths}
    provenance_cache.prefetch(capsule_paths.values())

    params_by_code_dir: dict[str, str] = {}
    for code_dir_path, capsule_path in capsule_paths.items():
        params = provenance_cache.get(capsule_path).get("params")
        if params:
            params_by_code_dir[code_dir_path] = str(params)

    unreadable = len(code_dir_paths) - len(params_by_code_dir)
    if unreadable:
        _log.info(
            "Could not read the parameter set of %d capsule(s); they fall back to their pipeline's own dispatcher",
            unreadable,
        )
    return params_by_code_dir
