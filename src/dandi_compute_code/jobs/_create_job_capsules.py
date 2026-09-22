"""
Creation of new job capsules for qualifying assets.

The rule is deliberately narrow. For every pipeline and parameters combination in the
packaged pipeline configuration, make sure each qualifying asset has a job capsule. A capsule
that already exists is left alone whichever pipeline or codebase version formed it, and new
capsules are formed against the latest versions available locally.

``--latest`` breaks that rule on purpose. It forms a fresh capsule against the latest
versions for every qualifying asset, whether or not one already exists, which is how a new
pipeline release gets rolled out over assets that have already been processed.
"""

import logging
import pathlib

from ._pipeline_versions import resolve_latest_pipeline_version
from ..aind_ephys_pipeline import UnmappedContentIDError, prepare_aind_ephys_job
from ..lfp_pipeline import prepare_lfp_job
from ..queue import QueueState
from ..queue._fetch_qualifying_aind_content_ids import _fetch_qualifying_aind_content_ids
from ..queue._fetch_qualifying_lfp_content_ids import _fetch_qualifying_lfp_content_ids
from ..queue._queue_utils import _load_queue_config, _order_content_ids_for_uniform_dandiset_sampling

_log = logging.getLogger(__name__)


def _qualifying_content_ids(pipeline: str, /) -> list[str]:
    """Content IDs qualifying for *pipeline*, ordered so a limit samples Dandisets uniformly."""
    fetch = _fetch_qualifying_lfp_content_ids if pipeline == "lfp" else _fetch_qualifying_aind_content_ids
    ordered_content_ids = _order_content_ids_for_uniform_dandiset_sampling(content_ids=fetch())
    return ordered_content_ids


def create_job_capsules(
    *,
    only_pipeline: str | None = None,
    config_key: str = "default",
    content_ids: list[str] | None = None,
    limit: int | None = None,
    force_latest_versions: bool = False,
    pipeline_directory: pathlib.Path | None = None,
) -> int:
    """
    Form new job capsules for qualifying assets that do not have one yet.

    Every pipeline and parameters combination declared in the packaged pipeline configuration
    (see :func:`_load_queue_config`) is crossed with the qualifying content IDs. An asset that
    already has a capsule for that combination is skipped, as read from the live queue state
    (see :meth:`~dandi_compute_code.queue.QueueState.from_dandi`).

    :param only_pipeline: Form capsules only for this pipeline instead of every pipeline in
        the configuration. Raises if the name is not configured.
    :param config_key: Key for a registered job configuration.
    :param content_ids: Explicit content IDs to form capsules for. The qualifying list is not
        fetched from the network when these are provided.
    :param limit: Form at most this many capsules in total. Unlimited when ``None``.
    :param force_latest_versions: Form a capsule for every qualifying asset against the latest
        pipeline and codebase versions, whether or not one already exists.
    :param pipeline_directory: Local checkout of the AIND pipeline repository.
    :return: The number of job capsules that were formed.
    :rtype: int
    """
    queue_config = _load_queue_config()
    pipelines = queue_config.get("pipelines", {})
    if only_pipeline is not None and only_pipeline not in pipelines:
        configured = list(pipelines.keys())
        message = f"Pipeline '{only_pipeline}' is not configured. Configured pipelines are: {configured}."
        raise ValueError(message)

    existing_capsule_keys = set() if force_latest_versions else QueueState.from_dandi().existing_capsule_keys()

    created_count = 0
    for pipeline_name, pipeline_data in pipelines.items():
        if only_pipeline is not None and pipeline_name != only_pipeline:
            continue
        if limit is not None and created_count >= limit:
            break

        version = resolve_latest_pipeline_version(pipeline=pipeline_name, pipeline_directory=pipeline_directory)
        config_id = QueueState.resolve_config_key_to_id(pipeline=pipeline_name, config_key=config_key)
        pipeline_content_ids = content_ids if content_ids is not None else _qualifying_content_ids(pipeline_name)

        for params_key in pipeline_data.get("params", []):
            params_id = QueueState.resolve_params_key_to_id(pipeline=pipeline_name, params_key=params_key)

            for content_id in pipeline_content_ids:
                if limit is not None and created_count >= limit:
                    _log.info(f"Reached the creation limit of {limit} job capsules.")
                    return created_count

                label = f"{pipeline_name}/{version}/{params_key}/{content_id}"
                if (pipeline_name, params_id, config_id, content_id) in existing_capsule_keys:
                    _log.info(f"Skipping {label}: a job capsule already exists.")
                    continue

                _log.info(f"Creating a job capsule for {label}.")
                try:
                    if pipeline_name == "lfp":
                        script_file_path = prepare_lfp_job(
                            content_id=content_id,
                            parameters_key=params_key,
                            pipeline_version=version,
                            force_new_capsule=force_latest_versions,
                            silent=True,
                        )
                    else:
                        script_file_path = prepare_aind_ephys_job(
                            content_id=content_id,
                            parameters_key=params_key,
                            pipeline_version=version,
                            pipeline_directory=pipeline_directory,
                            config_key=config_key,
                            force_new_capsule=force_latest_versions,
                            silent=True,
                        )
                except UnmappedContentIDError as error:
                    _log.warning(f"Skipping {label}: {error}")
                    continue
                if script_file_path is None:
                    _log.info(f"Skipped {label}: a job capsule already exists.")
                    continue
                created_count += 1

    return created_count
