import re

import beartype


@beartype.beartype
def _parse_pipeline_version(version: str, *, label: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?:[-+][0-9A-Za-z.+-]+)?", version)
    if match is None:
        message = f"Unexpected {label} version format: {version!r}"
        raise ValueError(message)
    return int(match["major"]), int(match["minor"]), int(match["patch"])
