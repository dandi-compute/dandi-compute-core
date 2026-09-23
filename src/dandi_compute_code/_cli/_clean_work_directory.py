import pathlib
import shutil

import beartype


@beartype.beartype
def clean_work_directory(directory: pathlib.Path) -> None:
    """
    Clean all contents of a directory except the 'apptainer_cache' subdirectory.

    Parameters
    ----------
    directory : pathlib.Path
        Path to the directory to clean.

    Raises
    ------
    NotADirectoryError
        If *directory* does not exist or is not a directory.
    """
    if not directory.is_dir():
        message = f"The specified directory does not exist or is not a directory: {directory}"
        raise NotADirectoryError(message)
    for item in directory.iterdir():
        if item.name == "apptainer_cache":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
