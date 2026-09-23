"""The commit of this repository that the running package was installed from."""

import pathlib
import re
import subprocess

_COMMIT_HASH_RE = re.compile(r"^[0-9a-f]{40}$")


def _codebase_commit_hash() -> str:
    """
    The commit hash of the git checkout this package is running from.

    The checkout is found from this file's own location, so it works wherever the checkout
    lives and whatever it is named. That requires an editable install (``pip install -e``)
    from a git checkout, which is how the package is installed on the cluster.

    Raises
    ------
    RuntimeError
        If the package is not running from a git checkout, as with a regular (non-editable)
        install, where there is no commit to record.
    ValueError
        If ``git rev-parse HEAD`` does not return a full commit hash.
    """
    package_directory = pathlib.Path(__file__).parent
    try:
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=package_directory,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError) as exception:
        message = (
            f"Could not read the commit of the codebase from {package_directory}. Preparing a job records "
            "that commit, so the package must be installed editable (`pip install -e`) from a git checkout."
        )
        raise RuntimeError(message) from exception

    if _COMMIT_HASH_RE.fullmatch(commit_hash) is None:
        message = f"Unexpected commit hash format: {commit_hash}"
        raise ValueError(message)
    return commit_hash
