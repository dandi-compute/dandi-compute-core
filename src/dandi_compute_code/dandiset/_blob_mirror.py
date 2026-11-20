"""
The cluster's mirror of the archive's S3 blobs.

The mirror is kept by the backup jobs of https://github.com/dandi/simple-s3-backup, which split the
blobs across partitions by the first hex digit of their content ID. That split and the mirror root
are specified by simple-s3-backup's ``partitions.json``, which is the one copy of them. simple-s3-backup
is not a dependency, so the file is read from its checkout on the cluster, which is the copy the
backup jobs run with. Elsewhere it is fetched from GitHub. It is read on first use, not on import.
The split has changed over time, so a blob is looked for on every partition before the current split
is assumed.
"""

import functools
import http.client
import json
import logging
import pathlib
import urllib.error
import urllib.request

import beartype

_log = logging.getLogger(__name__)

_CLUSTER_PARTITIONS_SPEC_PATH = pathlib.Path(
    "/orcd/data/dandi/001/backup/simple-s3-backup/src/simple_s3_backup/partitions.json"
)
_PARTITIONS_SPEC_URL = (
    "https://raw.githubusercontent.com/dandi/simple-s3-backup/main/src/simple_s3_backup/partitions.json"
)


@functools.cache
@beartype.beartype
def _load_partitions_spec() -> dict[str, object]:
    """
    Load simple-s3-backup's ``partitions.json``, from the cluster checkout if present, else from GitHub.

    Raises
    ------
    RuntimeError
        If the spec can be read from neither place. There is no fallback split, since a stale one
        would point capsules at the wrong partition.
    """
    if _CLUSTER_PARTITIONS_SPEC_PATH.is_file():
        try:
            spec = json.loads(_CLUSTER_PARTITIONS_SPEC_PATH.read_text())
            return spec
        except (OSError, json.JSONDecodeError) as exception:
            cluster_failure = f"{_CLUSTER_PARTITIONS_SPEC_PATH} could not be read ({exception})"
    else:
        cluster_failure = f"{_CLUSTER_PARTITIONS_SPEC_PATH} does not exist"

    try:
        with urllib.request.urlopen(_PARTITIONS_SPEC_URL, timeout=30) as response:
            spec = json.load(response)
    # OSError covers URLError, timeouts and dropped connections; ValueError covers bad JSON or encoding.
    except (OSError, http.client.HTTPException, ValueError) as exception:
        message = (
            "Unable to load simple-s3-backup's partitions.json, which specifies the blob mirror's partitions. "
            f"{cluster_failure}, and fetching {_PARTITIONS_SPEC_URL} failed ({exception})."
        )
        raise RuntimeError(message) from exception
    return spec


@beartype.beartype
def resolve_blob_path(*, content_id: str, mirror_root: pathlib.Path | None = None) -> pathlib.Path:
    """
    Find a blob in the cluster's mirror of the archive.

    The partition that holds the blob is used. When neither holds it, the blob has not been
    mirrored yet, and the path it is expected at under the current split is returned.

    Parameters
    ----------
    content_id : str
        The content ID of the blob.
    mirror_root : pathlib.Path, optional
        The directory the partitions of the mirror are mounted under.
        Defaults to the ``mirror_root`` of simple-s3-backup's ``partitions.json``.

    Returns
    -------
    pathlib.Path
        The path of the blob, ``{mirror_root}/{partition}/s3dandiarchive/blobs/{id[0:3]}/{id[3:6]}/{id}``.
    """
    spec = _load_partitions_spec()
    if mirror_root is None:
        mirror_root = pathlib.Path(spec["mirror_root"])

    expected_partition = spec["blobs"].get(content_id[0:1].lower())
    if expected_partition is None:
        message = f"Content ID {content_id!r} does not start with a hex digit."
        raise ValueError(message)

    partitions = sorted(spec["partitions"], key=lambda partition: partition != expected_partition)
    candidate_paths = [
        mirror_root / partition / "s3dandiarchive" / "blobs" / content_id[0:3] / content_id[3:6] / content_id
        for partition in partitions
    ]
    blob_path = next((path for path in candidate_paths if path.is_file()), None)
    if blob_path is None:
        blob_path = candidate_paths[0]
        _log.warning(f"Blob {content_id} is not in the cluster's mirror yet. It is expected at {blob_path}.")
    return blob_path
