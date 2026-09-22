"""
The job ID that names a job capsule directory.

A job capsule directory is named ``job-{YYMMDD}{hash}``, where ``hash`` is the first six
characters of the MD5 checksum of the fields that identify the job. The date makes the name
readable at a glance and separates re-attempts of the same job across days, while the hash
keeps capsules for different parameters, configs or assets apart within a single day.

Both halves are fixed width, six characters each, so the two are unambiguous without a
separator between them.

Two capsules can still land on one name, when they are the same logical job created on the
same day: re-runs differ only by details the hash deliberately ignores. A ``-2``, ``-3``
counter is appended to tell those apart. Ordinary creation never produces one, because it
does not form a capsule for a job that already has one, but ``--latest`` creation on a day
that already formed the same job does.

Everything the name used to spell out (pipeline version, codebase version, parameters and
config) is recorded in the capsule's ``dataset_description.json`` provenance and in the
``jobs.tsv`` summary table.
"""

import datetime
import hashlib
import re
from collections.abc import Collection, Iterable

#: Job capsule directory name, e.g. ``job-260916a1b2c3``, or ``job-260916a1b2c3-2`` for the
#: second capsule of one job on one day.
_JOB_ID_RE = re.compile(r"job-(?P<job_date>\d{6})(?P<job_hash>[0-9a-f]{6})(?:-(?P<job_index>[2-9]|\d{2,}))?")

#: Key under which job provenance is written into a capsule's ``dataset_description.json``.
_PROVENANCE_KEY = "DandiCompute"


def _compute_job_hash(
    *,
    dandiset_id: str,
    dandi_path: str,
    pipeline: str,
    version: str,
    params: str,
    config: str,
    content_id: str,
) -> str:
    """
    Six-character MD5 prefix over the fields that identify one job capsule.

    The codebase version is deliberately left out. A job is the same logical job no matter
    which release of this package formed it, which matches how the queue decides whether a
    capsule already exists.
    """
    payload = "|".join([dandiset_id, dandi_path, pipeline, version, params, config, content_id])
    job_hash = hashlib.md5(payload.encode("utf-8")).hexdigest()[:6]
    return job_hash


def _format_job_id(*, job_hash: str, date: datetime.date | None = None, index: int = 1) -> str:
    """
    Build the ``job-{YYMMDD}{hash}`` directory name, defaulting to today's date.

    Parameters
    ----------
    index : int, optional
        Which capsule this is among those sharing the name. The first carries no
        counter, so the common case reads as ``job-260916a1b2c3``. Later ones
        are suffixed ``-2``, ``-3`` and so on.
    """
    date = date if date is not None else datetime.datetime.now(tz=datetime.timezone.utc).date()
    counter = "" if index <= 1 else f"-{index}"
    job_id = f"job-{date:%y%m%d}{job_hash}{counter}"
    return job_id


def _parse_job_hash(job_id: str, /) -> str | None:
    """
    Return the hash portion of a job ID, or ``None`` when *job_id* is not a job ID.

    Any counter suffix is ignored, so capsules sharing a job read back as the same job.
    """
    match = _JOB_ID_RE.fullmatch(job_id)
    job_hash = match.group("job_hash") if match is not None else None
    return job_hash


def _capsule_names_from_asset_paths(*, asset_paths: Iterable[str], pipeline_dandiset_path: str) -> set[str]:
    """
    The distinct job capsule directory names sitting directly under *pipeline_dandiset_path*.

    Parameters
    ----------
    asset_paths : collections.abc.Iterable of str
        Asset paths of the form ``{pipeline_dandiset_path}/{job_id}/<subpath>``.
    pipeline_dandiset_path : str
        The ``.../pipeline-{name}`` path the capsules live under.
    """
    capsule_names = {asset_path.removeprefix(f"{pipeline_dandiset_path}/").split("/")[0] for asset_path in asset_paths}
    return capsule_names


def _find_existing_capsule_path(
    *,
    capsule_names: Collection[str],
    pipeline_dandiset_path: str,
    job_hash: str,
) -> str | None:
    """
    Find an already formed job capsule for this job among *capsule_names*, if there is one.

    Matching is on the job hash alone, so a capsule formed on an earlier date is still
    recognised.

    Returns
    -------
    str or None
        The capsule path, or ``None`` when no capsule exists for this job yet.
    """
    for capsule_name in sorted(capsule_names):
        if _parse_job_hash(capsule_name) == job_hash:
            return f"{pipeline_dandiset_path}/{capsule_name}"

    return None


def _next_available_job_id(*, job_hash: str, taken_job_ids: Collection[str], date: datetime.date | None = None) -> str:
    """
    The ``job-{YYMMDD}{hash}`` name for *date* that no existing capsule already carries.

    The first capsule of a job on a given day carries no counter. Forming another capsule for
    the same job on the same day appends ``-2``, then ``-3``, and so on.
    """
    index = 1
    job_id = _format_job_id(job_hash=job_hash, date=date, index=index)
    while job_id in taken_job_ids:
        index += 1
        job_id = _format_job_id(job_hash=job_hash, date=date, index=index)
    return job_id
