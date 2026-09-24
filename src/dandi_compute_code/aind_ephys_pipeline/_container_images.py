"""
The container images an AIND ephys pipeline version runs its steps in, cached ahead of its runs.

Nextflow pulls a step's image into its Apptainer cache the first time a step needs it. That pull
happens inside the capsule's own job, which asks for 1 GB. Turning a multi-gigabyte image into a
SIF file takes far more memory than that, so the pull is killed and the capsule fails before any
step runs. Images are therefore cached ahead of time instead, by a job that has the memory for it,
under the same file name Nextflow looks for. Nextflow then finds each image already in place and
never pulls one itself.
"""

from __future__ import annotations

import fcntl
import logging
import os
import pathlib
import re
import subprocess

import beartype

from .._base_directory import _DEFAULT_BASE_DIRECTORY, _aind_pipeline_directory, _apptainer_cache_directory

_log = logging.getLogger(__name__)

#: The pipeline's Nextflow script and the step versions it reads, relative to the checkout root.
_PIPELINE_FILE_RELATIVE_PATH = "pipeline/main_multi_backend.nf"
_CAPSULE_VERSIONS_FILE_RELATIVE_PATH = "pipeline/capsule_versions.env"
#: The tag every step image is pulled at, e.g. ``params.container_tag = "si-${versions['SPIKEINTERFACE_VERSION']}"``.
_CONTAINER_TAG_RE = re.compile(r'^\s*params\.container_tag\s*=\s*"(?P<template>[^"]*)"', re.MULTILINE)
#: A reference to one of the capsule versions inside the container tag.
_VERSIONS_REFERENCE_RE = re.compile(r"\$\{versions\['(?P<key>[A-Za-z0-9_]+)'\]\}")
#: A step image, written as a repository followed by the shared container tag.
_CONTAINER_IMAGE_RE = re.compile(r'"(?P<repository>[^"\s$]+):\$\{params\.container_tag\}"')
#: Held for the whole of a caching run so that two runs never pull into the cache at once.
_CACHE_LOCK_FILE_NAME = ".dandicompute-image-cache.lock"


@beartype.beartype
def _read_pipeline_file(*, pipeline_directory: pathlib.Path, pipeline_version: str, relative_path: str) -> str:
    """
    One file of the pipeline as it is at *pipeline_version*.

    The file is read out of the tag with ``git show`` rather than checked out, since capsules
    check out their own version of the same checkout while they run.
    """
    command = ["git", "show", f"{pipeline_version}:{relative_path}"]
    result = subprocess.run(command, cwd=pipeline_directory, capture_output=True, text=True)
    if result.returncode != 0:
        message = (
            f"Unable to read {relative_path} at version {pipeline_version!r} from the pipeline checkout "
            f"at '{pipeline_directory}'.\nstderr: {result.stderr}"
        )
        raise RuntimeError(message)
    return result.stdout


@beartype.beartype
def _parse_capsule_versions(text: str, /) -> dict[str, str]:
    """Read ``capsule_versions.env`` the way the pipeline does, one ``KEY=value`` per line."""
    versions: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        versions[key.strip()] = value.strip().strip("\"'")
    return versions


@beartype.beartype
def apptainer_cache_file_name(image: str, /) -> str:
    """
    The file name Nextflow looks for when *image* is in its Apptainer cache.

    Nextflow drops any ``docker://`` style prefix and folds ``/`` and ``:`` to ``-``, so
    ``ghcr.io/org/name:tag`` is cached as ``ghcr.io-org-name-tag.img``.
    """
    name = image.split("://", 1)[-1]
    file_name = name.replace(":", "-").replace("/", "-") + ".img"
    return file_name


@beartype.beartype
def aind_ephys_container_images(
    *,
    pipeline_version: str,
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
) -> list[str]:
    """
    The container images the AIND ephys pipeline runs its steps in at *pipeline_version*.

    Every step image shares one tag, which the pipeline builds from its ``capsule_versions.env``
    (``si-<SPIKEINTERFACE_VERSION>``). Both files are read out of the version's tag in the local
    pipeline checkout, so the images are the ones a capsule formed against that version will
    ask for. Every step image is listed, including sorters a given parameter set may not use.

    Parameters
    ----------
    pipeline_version : str
        The pipeline release tag, as capsules are formed against.
    base_directory : pathlib.Path, optional
        The structured base directory holding the pipeline checkout.

    Returns
    -------
    list of str
        Image references such as ``ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:si-0.104.9``,
        without duplicates, in the order the pipeline first mentions them.

    Raises
    ------
    RuntimeError
        If either file cannot be read at *pipeline_version*.
    ValueError
        If the container tag or the step images cannot be found in the pipeline, or the tag
        names a capsule version that is not set.
    """
    pipeline_directory = _aind_pipeline_directory(base_directory)
    pipeline_text = _read_pipeline_file(
        pipeline_directory=pipeline_directory,
        pipeline_version=pipeline_version,
        relative_path=_PIPELINE_FILE_RELATIVE_PATH,
    )
    versions = _parse_capsule_versions(
        _read_pipeline_file(
            pipeline_directory=pipeline_directory,
            pipeline_version=pipeline_version,
            relative_path=_CAPSULE_VERSIONS_FILE_RELATIVE_PATH,
        )
    )

    tag_match = _CONTAINER_TAG_RE.search(pipeline_text)
    if tag_match is None:
        message = f"No `params.container_tag` found in {_PIPELINE_FILE_RELATIVE_PATH} at {pipeline_version!r}."
        raise ValueError(message)

    template = tag_match.group("template")
    missing_keys = [key for key in _VERSIONS_REFERENCE_RE.findall(template) if not versions.get(key)]
    if missing_keys:
        message = (
            f"The container tag {template!r} at {pipeline_version!r} reads {missing_keys}, which "
            f"{_CAPSULE_VERSIONS_FILE_RELATIVE_PATH} does not set."
        )
        raise ValueError(message)
    container_tag = _VERSIONS_REFERENCE_RE.sub(lambda match: versions[match.group("key")], template)

    repositories = list(dict.fromkeys(_CONTAINER_IMAGE_RE.findall(pipeline_text)))
    if not repositories:
        message = f"No step images tagged with `params.container_tag` found in {_PIPELINE_FILE_RELATIVE_PATH}."
        raise ValueError(message)

    images = [f"{repository}:{container_tag}" for repository in repositories]
    return images


@beartype.beartype
def missing_container_images(
    *,
    images: list[str],
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
) -> list[str]:
    """
    The subset of *images* not yet in the Apptainer cache Nextflow reads from.

    Parameters
    ----------
    images : list of str
        Image references, as returned by :func:`aind_ephys_container_images`.
    base_directory : pathlib.Path, optional
        The structured base directory holding the cache.
    """
    cache_directory = _apptainer_cache_directory(base_directory)
    missing = [image for image in images if not (cache_directory / apptainer_cache_file_name(image)).is_file()]
    return missing


@beartype.beartype
def cache_container_images(
    *,
    images: list[str],
    base_directory: pathlib.Path = _DEFAULT_BASE_DIRECTORY,
) -> list[str]:
    """
    Pull every image in *images* that is not yet cached into the Apptainer cache Nextflow reads.

    Building a SIF file from a large image takes several gigabytes of memory, so this is meant
    to run in a job that asks for it. Each image is pulled to a temporary name next to its final
    one and renamed into place only once complete, so Nextflow never picks up a partial image.
    A lock on the cache is held throughout, so a second caching run waits for the first and
    then finds its images already in place.

    Parameters
    ----------
    images : list of str
        Image references, as returned by :func:`aind_ephys_container_images`.
    base_directory : pathlib.Path, optional
        The structured base directory holding the cache.

    Returns
    -------
    list of str
        The images that were pulled. Images already cached are left alone.

    Raises
    ------
    RuntimeError
        If ``apptainer pull`` fails for any image. Images pulled before it stay cached.
    """
    cache_directory = _apptainer_cache_directory(base_directory)
    cache_directory.mkdir(parents=True, exist_ok=True)

    pulled: list[str] = []
    with open(cache_directory / _CACHE_LOCK_FILE_NAME, "w") as lock_file:
        _log.info("Waiting for the lock on %s", cache_directory)
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        for image in missing_container_images(images=images, base_directory=base_directory):
            cache_file_path = cache_directory / apptainer_cache_file_name(image)
            partial_file_path = cache_file_path.with_name(f"{cache_file_path.name}.pulling.dandicompute-{os.getpid()}")
            partial_file_path.unlink(missing_ok=True)

            # --disable-cache keeps Apptainer from also keeping every layer in its own cache under $HOME.
            command = ["apptainer", "pull", "--disable-cache", str(partial_file_path), f"docker://{image}"]
            _log.info("Pulling %s into %s", image, cache_file_path)
            result = subprocess.run(command)
            if result.returncode != 0:
                partial_file_path.unlink(missing_ok=True)
                message = f"`apptainer pull` exited with code {result.returncode} for {image}."
                raise RuntimeError(message)

            os.replace(partial_file_path, cache_file_path)
            _log.info("Cached %s", image)
            pulled.append(image)

    return pulled
