import gzip
import json
import urllib.request

from ._globals import _QUALIFYING_LFP_CONTENT_IDS_URL


def _fetch_qualifying_lfp_content_ids() -> list[str]:
    """
    Fetch the list of content IDs that currently qualify for the LFP pipeline.

    The remote cache is a gzip-compressed JSON Lines file where each line is a single-entry
    ``{content_id: qualifies}`` object (``qualifies`` is a ``bool``). The LFP cache
    is the looser superset of the AIND cache. The lines are merged into one mapping
    and only the content IDs whose value is ``True`` are returned.

    Raises
    ------
    RuntimeError
        If the mapping cannot be downloaded or decoded from the remote URL.
        The original exception is chained via ``raise ... from``.
    """
    try:
        with urllib.request.urlopen(url=_QUALIFYING_LFP_CONTENT_IDS_URL) as response:
            decoded = gzip.decompress(response.read()).decode()
        content_id_to_qualifies: dict[str, bool] = {}
        for line in decoded.splitlines():
            if line.strip():
                content_id_to_qualifies.update(json.loads(line))
        qualifying_content_ids = [content_id for content_id, qualifies in content_id_to_qualifies.items() if qualifies]
        return qualifying_content_ids
    except Exception as exception:
        message = f"Unable to load qualifying LFP content IDs from {_QUALIFYING_LFP_CONTENT_IDS_URL}"
        raise RuntimeError(message) from exception
