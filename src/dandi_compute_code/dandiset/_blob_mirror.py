"""
The cluster's mirror of the archive's S3 blobs.

The mirror is kept by the backup jobs of https://github.com/dandi/simple-s3-backup, which split the
blobs across two partitions by the first hex digit of their content ID. That split has changed
over time, so a blob is looked for on every partition before the current split is assumed.
"""

import logging
import pathlib

import beartype

_log = logging.getLogger(__name__)

#: Where the partitions of the blob mirror are mounted on MIT Engaging.
_BLOB_MIRROR_ROOT = pathlib.Path("/orcd/data/dandi")

# The current split of `BLOBS_HEAD_TO_PARTITION` in simple-s3-backup.
_BLOB_PARTITION_BY_FIRST_HEX_DIGIT = {
    **{digit: "001" for digit in "0123456789"},
    **{digit: "002" for digit in "abcdef"},
}
_BLOB_PARTITIONS = tuple(sorted(set(_BLOB_PARTITION_BY_FIRST_HEX_DIGIT.values())))


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
