import logging

import dandi.dandiapi

from ._load_content_id_to_usage_dandiset_path import _load_content_id_to_usage_dandiset_path
from ._normalize_asset_path import _normalize_asset_path

_log = logging.getLogger(__name__)


# TODO: revise input arguments, logic, and combine with _lookup_job_completion_time
def _lookup_asset_size_bytes(
    *,
    api_token: str,
    dandiset_id: str,
    dandi_path: str,
    content_id: str,
) -> tuple[int | None, str | None]:
    """Lookup asset size and resolved source asset path from DANDI API.

    :param api_token: DANDI API token used for authenticated requests.
    :type api_token: str
    :param dandiset_id: DANDI dandiset identifier.
    :type dandiset_id: str
    :param dandi_path: Source path value from local scan, used only in mapped/scanned dandiset mismatch warnings.
    :type dandi_path: str
    :param content_id: Blob/content identifier extracted from ``NWB_FILE_PATH`` in ``code/submit.sh``.
    :type content_id: str
    :returns: A tuple ``(asset_size_bytes, resolved_dandi_path)``.
        The first value is ``int | None`` and the second value is ``str | None``.
        Either tuple value can be ``None`` when lookup conditions are not met.
    :rtype: tuple[int | None, str | None]
    """
    content_id_to_usage_dandiset_path = _load_content_id_to_usage_dandiset_path()
    if content_id not in content_id_to_usage_dandiset_path:
        _log.warning(
            (
                f"Unable to resolve asset_size_bytes for {content_id}. "
                "Content ID is not present in content-id-to-usage-dandiset-path mapping."
            ),
        )
        return None, None

    mapped_dandiset_path = content_id_to_usage_dandiset_path[content_id]
    if len(mapped_dandiset_path) != 1:
        _log.warning(
            (
                f"Unable to resolve asset_size_bytes for {content_id}. "
                f"Expected exactly 1 mapped dandiset/path pair but found {len(mapped_dandiset_path)}."
            ),
        )
        return None, None

    mapped_dandiset_id, mapped_asset_path = next(iter(mapped_dandiset_path.items()))
    if mapped_dandiset_id != dandiset_id:
        _log.warning(
            (
                f"Unable to resolve asset_size_bytes for {content_id}. "
                f"Mapped dandiset_id {mapped_dandiset_id} does not match scanned dandiset_id {dandiset_id} "
                f"for scanned path {dandi_path}."
            ),
        )
        return None, None

    client = dandi.dandiapi.DandiAPIClient(token=api_token)
    dandiset = client.get_dandiset(dandiset_id=dandiset_id)
    matching_assets = list(dandiset.get_assets_with_path_prefix(path=mapped_asset_path))
    if len(matching_assets) != 1:
        _log.warning(
            (
                f"Unable to resolve asset_size_bytes for {content_id}. "
                f"Expected exactly 1 asset at mapped path {mapped_asset_path} but found {len(matching_assets)}."
            ),
        )
        return None, None

    matching_metadata = matching_assets[0].get_raw_metadata()
    resolved_dandi_path = _normalize_asset_path(mapped_asset_path)

    content_size = matching_metadata.get("contentSize")
    if isinstance(content_size, int):
        return content_size, resolved_dandi_path
    if isinstance(content_size, str) and content_size.isdigit():
        return int(content_size), resolved_dandi_path
    _log.warning(
        f"Unable to resolve asset_size_bytes for {content_id}. Invalid or missing contentSize value {content_size!r}.",
    )
    return None, resolved_dandi_path
