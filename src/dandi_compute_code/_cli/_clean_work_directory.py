import pathlib
import shutil

import beartype

from .._base_directory import _work_directory


@beartype.beartype
def clean_work_directory(base_directory: pathlib.Path, /) -> None:
    """
    Clean all contents of a base directory's ``work/`` except the 'apptainer_cache' subdirectory.

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
        if item.name == "apptainer_cache":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
