import dataclasses
import functools
import json
import logging
import urllib.request

from ._globals import _ASSETS_JSONLD_URL_TEMPLATE, _JOB_CAPSULES_DANDISET_ID

_log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class AssetMetadata:
    """Minimal indexed metadata for one asset path."""

    path: str
    date_modified: str
    content_size: int
    content_id: str


@dataclasses.dataclass(frozen=True)
class AssetsJsonldMetadata:
    """Indexed metadata loaded from DANDI ``assets.jsonld``."""

    content_id_to_asset: dict[str, dict[str, object]]
    path_to_asset_metadata: dict[str, AssetMetadata]


def _build_asset_metadata(asset: dict[str, object]) -> tuple[str, AssetMetadata]:
    """Validate and extract metadata for a single asset; raises ValueError on missing fields."""
    content_size = asset.get("contentSize")
    if isinstance(content_size, str) and content_size.isdigit():
        content_size = int(content_size)
        asset["contentSize"] = content_size
    if not isinstance(content_size, int):
        raise ValueError(f"Asset missing valid 'contentSize': {asset!r}")

    path = asset.get("path")
    if not isinstance(path, str):
        raise ValueError(f"Asset missing valid 'path': {asset!r}")

    date_modified = asset.get("dateModified")
    if not isinstance(date_modified, str):
        raise ValueError(f"Asset {path!r} missing valid 'dateModified'")

    content_urls = asset.get("contentUrl")
    content_id = next(
        (
            url.rstrip("/").rsplit("/", 1)[-1].split("?", 1)[0]
            for url in (content_urls if isinstance(content_urls, list) else [])
            if isinstance(url, str) and ("/blobs/" in url or "/zarr/" in url)
        ),
        None,
    )
    if not (isinstance(content_id, str) and content_id):
        raise ValueError(f"Asset at path {path!r} has no blob or zarr contentUrl: {content_urls!r}")

    return content_id, AssetMetadata(
        path=path,
        date_modified=date_modified,
        content_size=content_size,
        content_id=content_id,
    )


@functools.lru_cache(maxsize=None)
def load_assets_jsonld_metadata(dandiset_id: str = _JOB_CAPSULES_DANDISET_ID) -> AssetsJsonldMetadata:
    """
    Load content-id and path metadata from a DANDI draft ``assets.jsonld`` stream.

    Parameters
    ----------
    dandiset_id : str, optional
        The Dandiset whose draft ``assets.jsonld`` is loaded. Defaults to the
        job capsules Dandiset (``001697``), where jobs run. Pass the failed runs
        archive Dandiset (``001873``) to describe the archived state instead.

    Returns
    -------
    AssetsJsonldMetadata
        Indexed assets metadata.
    """
    assets_jsonld_url = _ASSETS_JSONLD_URL_TEMPLATE.format(dandiset_id=dandiset_id)
    content_id_to_asset: dict[str, dict[str, object]] = {}
    path_to_asset_metadata: dict[str, AssetMetadata] = {}
    try:
        with urllib.request.urlopen(assets_jsonld_url, timeout=30) as response:
            assets = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exception:
        _log.warning("Unable to load metadata from %s: %s", assets_jsonld_url, exception)
        return AssetsJsonldMetadata(
            content_id_to_asset=content_id_to_asset,
            path_to_asset_metadata=path_to_asset_metadata,
        )

    if not isinstance(assets, list):
        raise ValueError(f"Expected a JSON array from {assets_jsonld_url}, got {type(assets).__name__}")

    for asset in assets:
        if not isinstance(asset, dict):
            raise ValueError(f"Expected each asset to be a dict, got {type(asset).__name__}: {asset!r}")
        content_id, metadata = _build_asset_metadata(asset)
        content_id_to_asset[content_id] = asset
        path_to_asset_metadata[metadata.path] = metadata

    return AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset,
        path_to_asset_metadata=path_to_asset_metadata,
    )
