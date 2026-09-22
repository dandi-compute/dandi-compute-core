import logging

import dandi.dandiapi

_log = logging.getLogger(__name__)


def _lookup_job_completion_time(
    *,
    api_token: str,
    dandiset_id: str,
    log_asset_path: str,
) -> str | None:
    """Look up the ``dateModified`` field for a log asset in the DANDI API."""
    client = dandi.dandiapi.DandiAPIClient(token=api_token)
    dandiset = client.get_dandiset(dandiset_id=dandiset_id)

    # TODO: offload this to helper function with LRU cache if this type of thing occurs anywhere else in codebase
    matching_assets = list(dandiset.get_assets_with_path_prefix(path=log_asset_path))
    if len(matching_assets) != 1:
        _log.warning(
            (
                f"Unable to resolve job_completion_time for log asset at {log_asset_path}. "
                f"Expected exactly 1 asset but found {len(matching_assets)}."
            ),
        )
        return None

    asset = matching_assets[0]
    metadata = asset.get_raw_metadata()
    date_modified = metadata.get("dateModified")
    if isinstance(date_modified, str):
        return date_modified
    _log.warning(
        (
            f"Unable to resolve job_completion_time for log asset at {log_asset_path}. "
            f"Invalid or missing dateModified value {date_modified!r}."
        ),
    )
    return None
