import pathlib
import shutil

import beartype

from .._base_directory import _work_directory

#: What ``clean --work`` keeps in ``work/``. ``apptainer_cache`` holds the container images every
#: capsule reuses. ``bazel`` is Bazel's output root, which the runner's ``launcher/clean_bazel.sh``
#: clears of what no Bazel command is using every 15 minutes. Deleting it whole would break the
#: builds running from it, and its read-only directories would stop the deletion part way.
_KEPT_NAMES = frozenset({"apptainer_cache", "bazel"})


@beartype.beartype
def clean_work_directory(base_directory: pathlib.Path, /) -> None:
    """
    Clean all contents of a base directory's ``work/`` except its 'apptainer_cache' and 'bazel'.

    Parameters
    ----------
    base_directory : pathlib.Path
        The structured base directory whose ``work/`` directory is cleaned.

    Raises
    ------
    NotADirectoryError
        If the ``work/`` directory does not exist or is not a directory.
    """
    directory = _work_directory(base_directory)
    if not directory.is_dir():
        message = f"The work directory does not exist or is not a directory: {directory}"
        raise NotADirectoryError(message)
    for item in directory.iterdir():
        if item.name in _KEPT_NAMES:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
