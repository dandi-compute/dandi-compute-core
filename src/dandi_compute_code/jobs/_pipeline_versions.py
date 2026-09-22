"""
Resolution of the pipeline version a new job capsule is formed against.

New job capsules always run the latest pipeline version that is available locally, so there
is no version priority list to maintain. What "locally available" means depends on the
pipeline. The AIND ephys pipeline lives in its own repository, checked out next to this one
on the cluster, so its latest version is the highest release tag in that checkout. The LFP
pipeline ships inside this package, so its latest version is this package's own version.
"""

import importlib.metadata
import pathlib
import re
import subprocess

from ..aind_ephys_pipeline._prepare_job import _parse_pipeline_version

#: Release tags of the form ``v1.2.3``, optionally with a pre-release or build suffix.
_VERSION_TAG_RE = re.compile(r"v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?")

#: Local checkout of the AIND ephys pipeline repository on MIT Engaging.
_DEFAULT_AIND_PIPELINE_DIRECTORY = pathlib.Path("/orcd/data/dandi/001/dandi-compute/aind-ephys-pipeline")


def _latest_repository_version_tag(pipeline_directory: pathlib.Path, /) -> str:
    """
    The highest release tag in a local pipeline repository checkout.

    :raises ValueError: If the checkout carries no release tags.
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


def resolve_latest_pipeline_version(*, pipeline: str, pipeline_directory: pathlib.Path | None = None) -> str:
    """
    The latest version of *pipeline* available on this machine.

    :param pipeline: The pipeline name as it appears in the packaged pipeline configuration.
    :param pipeline_directory: Local checkout of the pipeline repository, for pipelines that
        live in one. Defaults to the AIND ephys pipeline checkout on MIT Engaging.
    :return: The version string to form new job capsules against.
    :rtype: str
    """
    if pipeline == "lfp":
        latest_version = f"v{importlib.metadata.version('dandi-compute-code')}"
        return latest_version

    latest_version = _latest_repository_version_tag(pipeline_directory or _DEFAULT_AIND_PIPELINE_DIRECTORY)
    return latest_version
