"""
Private helpers for the :mod:`._pipeline_queue` model.

This companion module holds the lower-level utilities the ``PipelineQueue`` /
``JobCapsule`` model depends on (assets-path parsing, capsule-record construction,
upstream-metadata lookup, queue-config validation, content-id ordering, and log
parsing).
"""

from __future__ import annotations

import collections
import concurrent.futures
import json
import logging
import pathlib
import random
import subprocess
import urllib.error
import urllib.request
from collections.abc import Collection
from dataclasses import dataclass

import beartype

from ._globals import (
    _DURATION_PART_RE,
    _PACKAGED_PIPELINE_CONFIGS_PATH,
    _VERSION_TAG_RE,
)
from ._job_capsule import _derive_job_status
from ._job_info import JobInfo
from ..aind_ephys_pipeline._prepare_job import _parse_pipeline_version
from ..dandiset._job_id import _JOB_ID_RE, _PROVENANCE_KEY
from ..dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    _build_asset_metadata,
)
from ..dandiset._load_content_id_to_usage_dandiset_path import _load_content_id_to_usage_dandiset_path
from ..schemas import validate_against_schema

_log = logging.getLogger(__name__)

_UPSTREAM_JSONLD_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/dandisets/{dandiset_id}/draft/assets.jsonld"


@beartype.beartype
def _find_segment_index(parts: tuple[str, ...], prefix: str, start: int = 0) -> int | None:
    """Return the index of the first part starting with ``prefix`` at or after ``start``."""
    for index in range(start, len(parts)):
        if parts[index].startswith(prefix):
            return index
    return None


@beartype.beartype
@dataclass(frozen=True)
class _CapsuleLocation:
    """Where one job capsule sits in a Dandiset."""

    dandiset_id: str
    within_dandiset_path: str
    pipeline: str
    capsule_path: str
    job_id: str


@beartype.beartype
def _parse_capsule_location(asset_path: str, /) -> tuple[_CapsuleLocation, str] | None:
    """
    Parse an asset path of the form
    ``derivatives/dandiset-XXX/.../pipeline-NAME/job-{YYMMDD}{hash}/<subpath>`` into a
    :class:`_CapsuleLocation` and the subpath beneath the job capsule directory. Returns
    ``None`` if the path does not match that layout.
    """
    parts = pathlib.PurePosixPath(asset_path).parts

    dandiset_index = _find_segment_index(parts, "dandiset-")
    if dandiset_index is None:
        return None

    pipeline_index = _find_segment_index(parts, "pipeline-", start=dandiset_index + 1)
    if pipeline_index is None or pipeline_index <= dandiset_index + 1:
        return None

    pipeline = parts[pipeline_index][len("pipeline-") :]
    if not pipeline:
        return None

    capsule_dir_index = pipeline_index + 1
    if capsule_dir_index >= len(parts):
        return None

    job_id = parts[capsule_dir_index]
    if _JOB_ID_RE.fullmatch(job_id) is None:
        return None

    within_dandiset_path = "/".join(parts[dandiset_index + 1 : pipeline_index]) + ".nwb"
    subpath = "/".join(parts[capsule_dir_index + 1 :])

    location = _CapsuleLocation(
        dandiset_id=parts[dandiset_index][len("dandiset-") :],
        within_dandiset_path=within_dandiset_path,
        pipeline=pipeline,
        capsule_path="/".join(parts[: capsule_dir_index + 1]),
        job_id=job_id,
    )
    return location, subpath


@beartype.beartype
def _subpath_is_under(subpath: str, directory: str) -> bool:
    """True if ``subpath`` equals ``directory`` or lives inside it."""
    return subpath == directory or subpath.startswith(f"{directory}/")


@beartype.beartype
def _load_upstream_assets_jsonld_metadata(dandiset_id: str) -> AssetsJsonldMetadata:
    """Fetch and index ``assets.jsonld`` for another dandiset by id."""
    url = _UPSTREAM_JSONLD_URL_TEMPLATE.format(dandiset_id=dandiset_id)
    content_id_to_asset: dict[str, dict[str, object]] = {}
    path_to_asset_metadata: dict[str, AssetMetadata] = {}

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            assets = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exception:
        _log.warning("Unable to load upstream metadata from %s: %s", url, exception)
        return AssetsJsonldMetadata(
            content_id_to_asset=content_id_to_asset,
            path_to_asset_metadata=path_to_asset_metadata,
        )

    if not isinstance(assets, list):
        _log.warning("Expected a JSON array from %s, got %s", url, type(assets).__name__)
        return AssetsJsonldMetadata(
            content_id_to_asset=content_id_to_asset,
            path_to_asset_metadata=path_to_asset_metadata,
        )

    for asset in assets:
        if not isinstance(asset, dict):
            continue
        try:
            content_id, metadata = _build_asset_metadata(asset)
        except ValueError as exception:
            _log.debug("Skipping malformed upstream asset in %s: %s", url, exception)
            continue
        content_id_to_asset[content_id] = asset
        path_to_asset_metadata[metadata.path] = metadata

    return AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset,
        path_to_asset_metadata=path_to_asset_metadata,
    )


@beartype.beartype
class _UpstreamMetadataCache:
    """Per-call cache of upstream ``assets.jsonld`` lookups, keyed by dandiset id."""

    def __init__(self) -> None:
        self._cache: dict[str, AssetsJsonldMetadata] = {}

    def get(self, dandiset_id: str) -> AssetsJsonldMetadata:
        if dandiset_id not in self._cache:
            self._cache[dandiset_id] = _load_upstream_assets_jsonld_metadata(dandiset_id)
        return self._cache[dandiset_id]


@beartype.beartype
def _read_asset_json(asset: dict[str, object], /) -> dict:
    """Download and parse a small JSON asset from its DANDI blob URL."""
    content_urls = asset.get("contentUrl")
    for url in content_urls if isinstance(content_urls, list) else []:
        if not isinstance(url, str) or "/blobs/" not in url:
            continue
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                payload = json.load(response)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exception:
            _log.warning("Unable to read JSON asset from %s: %s", url, exception)
            continue
        if isinstance(payload, dict):
            return payload
    return {}


@beartype.beartype
class _CapsuleProvenanceCache:
    """
    Per-call cache of job provenance read from each capsule's ``dataset_description.json``.

    A capsule directory name carries only the job ID, so the pipeline version, codebase
    version, parameters and config are read back from the provenance block written into the
    capsule at preparation time.
    """

    def __init__(self, metadata: AssetsJsonldMetadata, /) -> None:
        self._metadata = metadata
        self._cache: dict[str, dict] = {}

    def prefetch(self, capsule_paths: Collection[str], /, *, max_workers: int = 8) -> None:
        """Warm the cache for many capsules at once, since each entry costs one HTTP request."""
        missing = [capsule_path for capsule_path in capsule_paths if capsule_path not in self._cache]
        if not missing:
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(missing))) as executor:
            for capsule_path, provenance in zip(missing, executor.map(self._load, missing)):
                self._cache[capsule_path] = provenance

    def get(self, capsule_path: str, /) -> dict:
        if capsule_path not in self._cache:
            self._cache[capsule_path] = self._load(capsule_path)
        return self._cache[capsule_path]

    def _load(self, capsule_path: str, /) -> dict:
        asset_metadata = self._metadata.path_to_asset_metadata.get(f"{capsule_path}/dataset_description.json")
        if asset_metadata is None:
            _log.warning("Job capsule %s has no dataset_description.json asset", capsule_path)
            return {}
        asset = self._metadata.content_id_to_asset.get(asset_metadata.content_id)
        if asset is None:
            return {}
        provenance = _read_asset_json(asset).get(_PROVENANCE_KEY)
        return provenance if isinstance(provenance, dict) else {}


@beartype.beartype
def _resolve_job_info(*, location: _CapsuleLocation, provenance_cache: _CapsuleProvenanceCache) -> JobInfo:
    """Build the full job identity for a capsule from its provenance."""
    fields = provenance_cache.get(location.capsule_path)
    if not fields:
        _log.warning(
            "No job provenance for capsule %s; version, codebase, params and config are left blank",
            location.capsule_path,
        )

    job_info = JobInfo(
        job_id=location.job_id,
        dandiset_id=location.dandiset_id,
        within_dandiset_path=location.within_dandiset_path,
        pipeline=location.pipeline,
        version=str(fields.get("version") or ""),
        params=str(fields.get("params") or ""),
        config=str(fields.get("config") or ""),
        codebase=str(fields.get("codebase") or ""),
    )
    return job_info


#: Presence markers accumulated while walking the assets, collapsed into ``status`` once
#: the walk is done (see :func:`_finalize_job_capsule_records`).
_PRESENCE_MARKERS = ("has_code", "has_been_submitted", "has_output", "has_logs")


def _new_capsule_record() -> dict[str, object]:
    return {
        **{marker: False for marker in _PRESENCE_MARKERS},
        "dataset_description_path": {},
        "output_paths": {},
        "log_paths": {},
    }


@beartype.beartype
@dataclass
class _JobCapsuleCollection:
    """Bookkeeping accumulated while walking ``assets.jsonld`` once, keyed by capsule path."""

    records_by_capsule: dict[str, dict[str, object]]
    locations_by_capsule: dict[str, _CapsuleLocation]
    log_timestamps_by_capsule: dict[str, list[str]]
    submit_sh_timestamps_by_capsule: dict[str, str]
    submitted_marker_timestamps_by_capsule: dict[str, list[str]]


@beartype.beartype
def _collect_job_capsules(local_metadata: AssetsJsonldMetadata, /) -> _JobCapsuleCollection:
    """
    Walk every asset path, group by job capsule directory, record presence flags, and
    capture the ``code/submit.sh`` timestamp per capsule (used for ``created_at``) along
    with the ``code/submitted*`` marker timestamps (used for ``job_submission_time``).
    """
    records_by_capsule: dict[str, dict[str, object]] = {}
    locations_by_capsule: dict[str, _CapsuleLocation] = {}
    log_timestamps_by_capsule: dict[str, list[str]] = {}
    submit_sh_timestamps_by_capsule: dict[str, str] = {}
    submitted_marker_timestamps_by_capsule: dict[str, list[str]] = {}

    for asset_path, asset_metadata in local_metadata.path_to_asset_metadata.items():
        parsed = _parse_capsule_location(asset_path)
        if parsed is None:
            continue
        location, subpath = parsed
        capsule_path = location.capsule_path

        locations_by_capsule.setdefault(capsule_path, location)
        record = records_by_capsule.setdefault(capsule_path, _new_capsule_record())

        if _subpath_is_under(subpath, "code"):
            record["has_code"] = True
        if subpath.startswith("code/submitted"):
            record["has_been_submitted"] = True
            submitted_marker_timestamps_by_capsule.setdefault(capsule_path, []).append(asset_metadata.date_modified)
        if subpath == "dataset_description.json":
            record["dataset_description_path"][asset_path] = asset_metadata.content_id
        elif _subpath_is_under(subpath, "derivatives"):
            record["has_output"] = True
            record["output_paths"][asset_path] = asset_metadata.content_id
        elif subpath.startswith("logs/"):
            log_relative_path = subpath.removeprefix("logs/")
            if log_relative_path and log_relative_path != "dataset_description.json":
                record["has_logs"] = True
                record["log_paths"][asset_path] = asset_metadata.content_id
                log_timestamps_by_capsule.setdefault(capsule_path, []).append(asset_metadata.date_modified)

        if subpath == "code/submit.sh":
            submit_sh_timestamps_by_capsule[capsule_path] = asset_metadata.date_modified

    return _JobCapsuleCollection(
        records_by_capsule=records_by_capsule,
        locations_by_capsule=locations_by_capsule,
        log_timestamps_by_capsule=log_timestamps_by_capsule,
        submit_sh_timestamps_by_capsule=submit_sh_timestamps_by_capsule,
        submitted_marker_timestamps_by_capsule=submitted_marker_timestamps_by_capsule,
    )


@beartype.beartype
def _finalize_job_capsule_records(
    *,
    collection: _JobCapsuleCollection,
    upstream_cache: _UpstreamMetadataCache,
    provenance_cache: _CapsuleProvenanceCache,
) -> list[dict[str, object]]:
    """
    Resolve each capsule's full identity, then attach source-asset fields (``content_id``,
    ``asset_size_bytes``) from the upstream dandiset's ``assets.jsonld``, and ``created_at`` /
    ``job_submission_time`` / ``job_completion_time`` from local timestamps.

    The presence markers gathered during the walk are collapsed into the single ``status``
    field the table carries.
    """
    provenance_cache.prefetch(collection.locations_by_capsule)

    finalized: list[dict[str, object]] = []
    for capsule_path, record in collection.records_by_capsule.items():
        location = collection.locations_by_capsule[capsule_path]
        job_info = _resolve_job_info(location=location, provenance_cache=provenance_cache)
        record.update(job_info.to_dict())

        upstream_metadata = upstream_cache.get(job_info.dandiset_id)
        source_metadata = upstream_metadata.path_to_asset_metadata.get(job_info.within_dandiset_path)

        if source_metadata is None:
            _log.warning(
                "Source asset not found in upstream dandiset %s for within_dandiset_path=%s; "
                "emitting record with null content_id/asset_size_bytes",
                job_info.dandiset_id,
                job_info.within_dandiset_path,
            )
            content_id: str | None = None
            asset_size_bytes: int | None = None
        else:
            content_id = source_metadata.content_id
            asset_size_bytes = source_metadata.content_size

        completion_times = collection.log_timestamps_by_capsule.get(capsule_path, [])
        # A capsule can carry several submitted markers (e.g. a resubmission); the earliest
        # one is when the job actually left the queue.
        submission_times = collection.submitted_marker_timestamps_by_capsule.get(capsule_path, [])
        presence = {marker: bool(record.pop(marker)) for marker in _PRESENCE_MARKERS}
        record.update(
            {
                "status": _derive_job_status(**presence),
                "content_id": content_id,
                "asset_size_bytes": asset_size_bytes,
                "created_at": collection.submit_sh_timestamps_by_capsule.get(capsule_path),
                "job_submission_time": min(submission_times) if submission_times else None,
                "job_completion_time": max(completion_times) if completion_times else None,
            }
        )
        finalized.append(record)
    return finalized


@beartype.beartype
def _sort_key(record: dict[str, object]) -> tuple[str, str, str, str]:
    # content_id may be None for capsules whose upstream source wasn't resolvable;
    # coerce to "" so sorting stays total.
    return (
        str(record["dandiset_id"]),
        str(record["within_dandiset_path"]),
        str(record["content_id"]) if record["content_id"] is not None else "",
        str(record["job_id"]),
    )


@beartype.beartype
def _validate_pipeline_config(*, pipeline_config: dict) -> None:
    """Validate the pipeline config (top-level ``pipelines`` mapping) against the LinkML schema."""
    validate_against_schema(pipeline_config, schema="pipeline_config", description="pipeline configuration")


@beartype.beartype
def _latest_repository_version_tag(pipeline_directory: pathlib.Path, /) -> str:
    """
    The highest release tag in a local pipeline repository checkout.

    Raises
    ------
    ValueError
        If the checkout carries no release tags.
    """
    tag_output = subprocess.check_output(["git", "tag", "--list"], cwd=pipeline_directory, text=True)
    version_tags = [tag.strip() for tag in tag_output.splitlines() if _VERSION_TAG_RE.fullmatch(tag.strip())]
    if not version_tags:
        message = (
            f"No release tags found in the pipeline repository at '{pipeline_directory}'. "
            "Fetch its tags (`git fetch --tags`) so the latest version can be resolved."
        )
        raise ValueError(message)

    latest_version_tag = max(version_tags, key=lambda tag: _parse_pipeline_version(tag, label="pipeline tag"))
    return latest_version_tag


def _load_pipeline_config() -> dict:
    """
    Read the packaged pipeline configuration and validate it against the LinkML schema.

    Always reads the pipeline configuration packaged with this repo (see
    ``_PACKAGED_PIPELINE_CONFIGS_PATH``) -- there is no local override.

    Raises
    ------
    FileNotFoundError
        If the packaged pipeline configuration file is missing.
    ValueError
        If the pipeline configuration fails LinkML validation.
    """
    if not _PACKAGED_PIPELINE_CONFIGS_PATH.exists():
        message = f"Packaged pipeline configuration is missing: '{_PACKAGED_PIPELINE_CONFIGS_PATH}'."
        raise FileNotFoundError(message)

    pipeline_config = json.loads(_PACKAGED_PIPELINE_CONFIGS_PATH.read_text())
    _validate_pipeline_config(pipeline_config=pipeline_config)
    return pipeline_config


@beartype.beartype
def _order_content_ids_for_uniform_dandiset_sampling(*, content_ids: list[str]) -> list[str]:
    """Return content IDs ordered by randomized round-robin across source Dandisets."""
    content_id_to_dandiset_path = _load_content_id_to_usage_dandiset_path()
    content_ids_by_dandiset: dict[str | tuple[str, str], list[str]] = {}
    for content_id in content_ids:
        content_id_mapping = content_id_to_dandiset_path.get(content_id, {})
        if len(content_id_mapping) == 1:
            dandiset_key: str | tuple[str, str] = next(iter(content_id_mapping))
        else:
            dandiset_key = ("content-id", content_id)
        content_ids_by_dandiset.setdefault(dandiset_key, []).append(content_id)

    grouped_content_ids = list(content_ids_by_dandiset.values())
    random.shuffle(grouped_content_ids)
    grouped_content_id_queues: list[collections.deque[str]] = []
    for content_ids_for_dandiset in grouped_content_ids:
        random.shuffle(content_ids_for_dandiset)
        grouped_content_id_queues.append(collections.deque(content_ids_for_dandiset))

    ordered_content_ids: list[str] = []
    while grouped_content_id_queues:
        next_grouped_content_id_queues: list[collections.deque[str]] = []
        for content_ids_for_dandiset in grouped_content_id_queues:
            ordered_content_ids.append(content_ids_for_dandiset.popleft())
            if content_ids_for_dandiset:
                next_grouped_content_id_queues.append(content_ids_for_dandiset)
        grouped_content_id_queues = next_grouped_content_id_queues
    return ordered_content_ids


@beartype.beartype
def _duration_string_to_seconds(duration_string: str) -> float:
    """Parse a Nextflow duration string (for example ``1m 5s``) into seconds."""
    total_seconds = 0.0
    for match in _DURATION_PART_RE.finditer(duration_string):
        value = float(match.group("value"))
        unit = match.group("unit")
        if unit == "ms":
            total_seconds += value / 1000.0
        elif unit == "s":
            total_seconds += value
        elif unit == "m":
            total_seconds += value * 60.0
        elif unit == "h":
            total_seconds += value * 3600.0
        elif unit == "d":
            total_seconds += value * 86400.0
    return total_seconds


@beartype.beartype
def _extract_nextflow_timeline_data(*, timeline_html: str) -> dict | None:
    """Extract the ``window.data`` JSON payload from a Nextflow timeline HTML report."""
    marker = "window.data ="
    marker_index = timeline_html.find(marker)
    if marker_index == -1:
        return None

    object_start = timeline_html.find("{", marker_index)
    if object_start == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    object_end = -1
    for index in range(object_start, len(timeline_html)):
        character = timeline_html[index]
        if in_string:
            if escape:
                escape = False
            elif character == "\\":
                escape = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                object_end = index
                break

    if object_end == -1:
        return None

    try:
        payload = json.loads(timeline_html[object_start : object_end + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


@beartype.beartype
def _read_asset_text(asset: dict[str, object], /) -> str | None:
    """Download a small text asset from its DANDI blob URL."""
    content_urls = asset.get("contentUrl")
    for url in content_urls if isinstance(content_urls, list) else []:
        if not isinstance(url, str) or "/blobs/" not in url:
            continue
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                return response.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError, UnicodeDecodeError) as exception:
            _log.warning("Unable to read a text asset from %s: %s", url, exception)
    return None


@beartype.beartype
def _read_text_asset_at_path(*, metadata: AssetsJsonldMetadata, path: str) -> str | None:
    """Download the text of the asset at *path*, or ``None`` when it is absent or unreadable."""
    asset_metadata = metadata.path_to_asset_metadata.get(path)
    if asset_metadata is None:
        return None
    asset = metadata.content_id_to_asset.get(asset_metadata.content_id)
    if asset is None:
        return None
    text = _read_asset_text(asset)
    return text


@beartype.beartype
def _extract_error_lines(text: str, /) -> list[str]:
    """Return non-empty log lines containing 'error' (case-insensitive)."""
    return [line.strip() for line in text.splitlines() if line.strip() and "error" in line.lower()]


@beartype.beartype
def _capsule_log_paths(asset_paths: Collection[str], /) -> dict[str, list[str]]:
    """Map each job capsule path to the sorted paths of the files directly in its ``logs/`` directory."""
    capsule_log_paths: collections.defaultdict[str, list[str]] = collections.defaultdict(list)
    for path in asset_paths:
        parts = pathlib.PurePosixPath(path).parts
        if len(parts) < 4 or parts[0] != "derivatives" or parts[-2] != "logs":
            continue
        if _JOB_ID_RE.fullmatch(parts[-3]) is None:
            continue
        capsule_log_paths[pathlib.PurePosixPath(*parts[:-2]).as_posix()].append(path)
    sorted_capsule_log_paths = {
        capsule_path: sorted(log_paths) for capsule_path, log_paths in sorted(capsule_log_paths.items())
    }
    return sorted_capsule_log_paths
