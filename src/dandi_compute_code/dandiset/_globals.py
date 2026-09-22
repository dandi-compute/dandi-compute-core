_RETIRED_DANDISET_ID = "214527"
_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"
_CONTENT_ID_TO_USAGE_DANDISET_PATH_URL = (
    "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/derivatives/"
    "derivatives/content_id_to_usage_dandiset_path.jsonl"
)
_ASSETS_JSONLD_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/dandisets/{dandiset_id}/draft/assets.jsonld"
_ASSETS_JSONLD_URL = _ASSETS_JSONLD_URL_TEMPLATE.format(dandiset_id=_JOB_CAPSULES_DANDISET_ID)


def _dandiset_derivatives_relative_dir(dandiset_id: str) -> str:
    """
    Return the ``dandisets-{first three digits}/dandiset-{dandiset_id}`` path segment
    used to nest a Dandiset's derivatives tree, e.g. ``"dandisets-001/dandiset-001234"``
    for dandiset_id ``"001234"``.
    """
    return f"dandisets-{dandiset_id[:3]}/dandiset-{dandiset_id}"
