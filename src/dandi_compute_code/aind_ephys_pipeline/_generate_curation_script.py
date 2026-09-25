import pathlib

import beartype
import jinja2

from ._globals import _CURATION_TEMPLATE_FILE_PATH
from ..dandiset import load_assets_jsonld_metadata
from ..dandiset._globals import _JOB_CAPSULES_DANDISET_ID
from ..dandiset._job_id import _JOB_ID_RE

_POSTPROCESSED_DIRECTORY = "derivatives/postprocessed"


@beartype.beartype
def generate_curation_script(*, capsule: str, dandiset_id: str = _JOB_CAPSULES_DANDISET_ID) -> str:
    """
    Generate a Python script that opens a successful AIND capsule's sorting in SpikeInterface GUI.

    The script streams each postprocessed SortingAnalyzer straight from the archive's S3 bucket,
    so the Zarr IDs of those assets are looked up here and written into it.

    Parameters
    ----------
    capsule : str
        The capsule's job ID (e.g. ``job-260916a1b2c3``), or its path relative to the Dandiset root.
    dandiset_id : str, optional
        The Dandiset holding the capsule. Defaults to the job capsules Dandiset (``001697``).

    Returns
    -------
    str
        The source of the script.

    Raises
    ------
    ValueError
        If no capsule, or more than one, matches ``capsule``, or it has no postprocessed outputs.
    """
    capsule = capsule.strip().strip("/")
    capsule_name = pathlib.PurePosixPath(capsule).name
    if _JOB_ID_RE.fullmatch(capsule_name) is None:
        message = f"{capsule!r} is not a job ID or a path ending in one (e.g. 'job-260916a1b2c3')."
        raise ValueError(message)

    metadata = load_assets_jsonld_metadata(dandiset_id)
    capsule_paths_to_analyzers: dict[str, dict[str, str]] = {}
    for asset_path, asset_metadata in metadata.path_to_asset_metadata.items():
        capsule_path, separator, analyzer_name = asset_path.partition(f"/{_POSTPROCESSED_DIRECTORY}/")
        if not separator or not analyzer_name.endswith(".zarr") or "/" in analyzer_name:
            continue
        if capsule_path != capsule and not capsule_path.endswith(f"/{capsule}"):
            continue
        stream_name = analyzer_name.removesuffix(".zarr")
        capsule_paths_to_analyzers.setdefault(capsule_path, {})[stream_name] = asset_metadata.content_id

    if not capsule_paths_to_analyzers:
        message = (
            f"No postprocessed outputs were found for {capsule!r} in Dandiset {dandiset_id}. "
            "Only successful 'aind+ephys' capsules have them."
        )
        raise ValueError(message)
    if len(capsule_paths_to_analyzers) > 1:
        matches = "\n".join(f"  {capsule_path}" for capsule_path in sorted(capsule_paths_to_analyzers))
        message = f"{capsule!r} matches more than one capsule. Pass its full path instead.\n{matches}"
        raise ValueError(message)

    ((capsule_path, analyzers),) = capsule_paths_to_analyzers.items()
    template = jinja2.Template(source=_CURATION_TEMPLATE_FILE_PATH.read_text())
    script = template.render(
        job_id=pathlib.PurePosixPath(capsule_path).name,
        dandiset_id=dandiset_id,
        capsule_path=capsule_path,
        analyzers=dict(sorted(analyzers.items())),
    )
    return f"{script}\n"
