"""
The structured base directory every command and API entry point operates on.

The base directory holds everything this package reads from or writes to on the cluster, under
a fixed layout.

- ``dandi-compute-core/`` is the checkout of this repository.
- ``processing/`` holds temporary working trees, dispatch directories and dispatch records.
- ``work/`` is the Nextflow work directory.
- ``aind-ephys-pipeline/`` is the checkout of the AIND ephys pipeline repository.
"""

import pathlib

import beartype

#: The base directory on MIT Engaging.
_DEFAULT_BASE_DIRECTORY = pathlib.Path("/orcd/data/dandi/001/dandi-compute")


@beartype.beartype
def _code_directory(base_directory: pathlib.Path, /) -> pathlib.Path:
    """The checkout of this repository under *base_directory*."""
    return base_directory / "dandi-compute-core"


@beartype.beartype
def _processing_directory(base_directory: pathlib.Path, /) -> pathlib.Path:
    """The directory for temporary working trees and dispatches under *base_directory*."""
    return base_directory / "processing"


@beartype.beartype
def _work_directory(base_directory: pathlib.Path, /) -> pathlib.Path:
    """The Nextflow work directory under *base_directory*."""
    return base_directory / "work"


@beartype.beartype
def _aind_pipeline_directory(base_directory: pathlib.Path, /) -> pathlib.Path:
    """The checkout of the AIND ephys pipeline repository under *base_directory*."""
    return base_directory / "aind-ephys-pipeline"
