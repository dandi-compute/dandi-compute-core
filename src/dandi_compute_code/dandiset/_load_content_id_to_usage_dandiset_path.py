import functools
import gzip
import json
import urllib.request

from ._globals import _CONTENT_ID_TO_USAGE_DANDISET_PATH_URL


@functools.lru_cache(maxsize=1)
def _load_content_id_to_usage_dandiset_path() -> dict[str, dict[str, str]]:
    """
    Load the content ID to usage Dandiset path mapping.

    The remote cache is a gzip-compressed JSON Lines file where each line is a single-entry
    ``{content_id: {dandiset_id: path}}`` object; the lines are merged into one
    mapping.

    Raises
    ------
    RuntimeError
        If the mapping cannot be downloaded or decoded from the remote URL.
        The original exception is chained via ``raise ... from``.
    """
    try:
        with urllib.request.urlopen(url=_CONTENT_ID_TO_USAGE_DANDISET_PATH_URL) as response:
            decoded = gzip.decompress(response.read()).decode()
        return {
            content_id: dandiset_path
            for line in decoded.splitlines()
            if line.strip()
            for content_id, dandiset_path in json.loads(line).items()
        }
    except Exception as exception:
        message = (
            "Unable to load content-id-to-usage-dandiset-path mapping from " f"{_CONTENT_ID_TO_USAGE_DANDISET_PATH_URL}"
        )
        raise RuntimeError(message) from exception
