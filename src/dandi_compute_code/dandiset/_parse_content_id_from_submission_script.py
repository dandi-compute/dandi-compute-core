import pathlib

import beartype


@beartype.beartype
def _parse_content_id_from_submission_script(capsule_dir: pathlib.Path, /) -> str:
    """
    Read a content ID from ``code/submit.sh``.

    Raises
    ------
    ValueError
        If ``code/submit.sh`` does not exist under *capsule_dir*, if the
        ``NWB_FILE_PATH`` assignment is missing or malformed, or if the
        derived content ID resolves to an empty string.
    """
    script_file = capsule_dir / "code" / "submit.sh"
    if not script_file.is_file():
        raise ValueError(f"Unable to determine content_id for {capsule_dir}: missing {script_file}")

    script_text = script_file.read_text()

    for line in script_text.splitlines():
        if line.startswith("NWB_FILE_PATH="):
            nwb_file_path = line.split("=", maxsplit=1)[1].strip().strip('"').strip("'")
            content_id = pathlib.PurePosixPath(nwb_file_path).name
            if not content_id:
                raise ValueError(
                    "Unable to determine content_id for "
                    f"{capsule_dir}: empty content_id from NWB_FILE_PATH in {script_file}"
                )
            return content_id
    raise ValueError(
        f"Unable to determine content_id for {capsule_dir}: missing/invalid NWB_FILE_PATH in {script_file}"
    )
