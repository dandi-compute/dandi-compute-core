"""
The cluster's mirror of the archive's S3 blobs.

The mirror is kept by the backup jobs of https://github.com/dandi/simple-s3-backup, which split the
blobs across partitions by the first hex digit of their content ID. That split is specified by
simple-s3-backup's ``partitions.json``. It cannot be imported from there, since simple-s3-backup is
not a dependency, so ``blob_partitions.json`` is a copy of it that the tests check against upstream.
The split has changed over time, so a blob is looked for on every partition before the current split
is assumed.
"""

import json
import logging
import pathlib

import beartype

_log = logging.getLogger(__name__)

_BLOB_PARTITIONS_SPEC_PATH = pathlib.Path(__file__).parent / "blob_partitions.json"
_BLOB_PARTITIONS_SPEC = json.loads(_BLOB_PARTITIONS_SPEC_PATH.read_text())

#: Where the partitions of the blob mirror are mounted on MIT Engaging.
_BLOB_MIRROR_ROOT = pathlib.Path(_BLOB_PARTITIONS_SPEC["mirror_root"])
_BLOB_PARTITION_BY_FIRST_HEX_DIGIT: dict[str, str] = _BLOB_PARTITIONS_SPEC["blobs"]
_BLOB_PARTITIONS = tuple(_BLOB_PARTITIONS_SPEC["partitions"])


@beartype.beartype
def resolve_blob_path(*, content_id: str, mirror_root: pathlib.Path = _BLOB_MIRROR_ROOT) -> pathlib.Path:
    """
    Find a blob in the cluster's mirror of the archive.

    The partition that holds the blob is used. When neither holds it, the blob has not been
    mirrored yet, and the path it is expected at under the current split is returned.

    Parameters
    ----------
    content_id : str
        The content ID of the blob.
    mirror_root : pathlib.Path
        The directory the partitions of the mirror are mounted under.

    Returns
    -------
    pathlib.Path
        The path of the blob, ``{mirror_root}/{partition}/s3dandiarchive/blobs/{id[0:3]}/{id[3:6]}/{id}``.
    """
    expected_partition = _BLOB_PARTITION_BY_FIRST_HEX_DIGIT.get(content_id[0:1].lower())
    if expected_partition is None:
        message = f"Content ID {content_id!r} does not start with a hex digit."
        raise ValueError(message)

    partitions = sorted(_BLOB_PARTITIONS, key=lambda partition: partition != expected_partition)
    candidate_paths = [
        mirror_root / partition / "s3dandiarchive" / "blobs" / content_id[0:3] / content_id[3:6] / content_id
        for partition in partitions
    ]
    blob_path = next((path for path in candidate_paths if path.is_file()), None)
    if blob_path is None:
        blob_path = candidate_paths[0]
        _log.warning(f"Blob {content_id} is not in the cluster's mirror yet. It is expected at {blob_path}.")
    return blob_path
