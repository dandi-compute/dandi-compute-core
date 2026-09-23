"""
Plain helpers for working with the example queue (``example_jobs_files/jobs.tsv``).

These are ordinary functions, imported and called directly by the queue tests.
They need nothing from pytest, so they are deliberately not fixtures. The pytest
fixtures (temporary directories, environment and network setup) live in
``conftest.py``.
"""

import contextlib
import io
from collections.abc import Iterator
from unittest import mock

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.dandiset._globals import _dandiset_derivatives_relative_dir
from dandi_compute_code.queue import JobCapsule


def job_capsule_files(
    *,
    entry: JobCapsule,
    with_code: bool = True,
    with_output: bool = False,
    with_logs: bool = False,
    submitted: bool = False,
) -> dict[str, str]:
    """
    The remote files of the job capsule for *entry*, keyed by asset path.

    The capsule path is derived from the entry itself (via the public ``JobCapsule.capsule_path``)
    so the files always match the ground-truth example state rather than a separately specified
    set of coordinates.
    """
    capsule_path = entry.capsule_path()
    files: dict[str, str] = {}
    if with_code:
        files[f"{capsule_path}/code/submit.sh"] = "#!/bin/bash\necho hello\n"
        if submitted:
            files[f"{capsule_path}/code/submitted_date-date-2025+01+01_time-00+00+00"] = ""
    if with_output:
        files[f"{capsule_path}/derivatives/output.nwb"] = ""
    if with_logs:
        files[f"{capsule_path}/logs/run.log"] = "job output\n"
    return files


def job_capsule_log_files(
    *,
    dandiset_id: str,
    subject: str,
    job_id: str,
    nextflow_lines: list[str],
    slurm_lines_by_file: dict[str, list[str]],
) -> dict[str, str]:
    """The remote ``logs/`` files of a job capsule holding a nextflow log and slurm logs, keyed by asset path."""
    logs_path = (
        f"derivatives/{_dandiset_derivatives_relative_dir(dandiset_id)}/sub-{subject}/pipeline-test/{job_id}/logs"
    )
    files = {f"{logs_path}/nextflow.log": "\n".join(nextflow_lines) + "\n"}
    for file_name, lines in slurm_lines_by_file.items():
        files[f"{logs_path}/{file_name}"] = "\n".join(lines) + "\n"
    return files


@contextlib.contextmanager
def serve_remote_dandiset(files: dict[str, str]) -> Iterator[None]:
    """
    Serve *files* (asset path to text) as the remote Dandiset the queue reads from.

    The queue's ``assets.jsonld`` loader returns an index of *files*, and each file's blob URL
    downloads its text, so the queue's own remote reading code runs unchanged.
    """
    content_id_to_asset: dict[str, dict[str, object]] = {}
    path_to_asset_metadata: dict[str, AssetMetadata] = {}
    blob_url_to_text: dict[str, str] = {}
    for index, (path, text) in enumerate(sorted(files.items())):
        content_id = f"00000000-0000-0000-0000-{index:012d}"
        blob_url = f"https://dandiarchive.s3.amazonaws.com/blobs/{content_id[:3]}/{content_id[3:6]}/{content_id}"
        content_id_to_asset[content_id] = {"path": path, "contentUrl": [blob_url]}
        path_to_asset_metadata[path] = AssetMetadata(
            path=path,
            date_modified="2025-01-01T00:00:00+00:00",
            content_size=len(text.encode()),
            content_id=content_id,
        )
        blob_url_to_text[blob_url] = text
    metadata = AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset, path_to_asset_metadata=path_to_asset_metadata
    )

    def _open_blob(url: str, timeout: float | None = None) -> io.BytesIO:
        return io.BytesIO(blob_url_to_text[url].encode())

    with (
        mock.patch("dandi_compute_code.queue._pipeline_queue.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch("dandi_compute_code.queue._queue_utils.urllib.request.urlopen", side_effect=_open_blob),
    ):
        yield
