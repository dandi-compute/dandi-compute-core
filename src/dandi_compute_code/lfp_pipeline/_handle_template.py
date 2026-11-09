import pathlib

import beartype
import jinja2

from ._globals import _RAW_TEMPLATE_FILE_PATH


@beartype.beartype
def generate_lfp_submission_script(
    *,
    script_file_path: pathlib.Path,
    log_directory: str,
    dataset_directory: str,
    environment_directory: str,
    container_image: str,
    image_file_path: str,
    nwb_file_path: str,
    output_nwb_file_path: str,
    parameters_key: str,
    temp_name: str,
    done_tracker_file_path: str,
) -> None:
    """
    Generate the LFP pipeline sbatch submission script from the template.

    The script runs the LFP runtime container with Apptainer on a local NWB file,
    wrapped in duct. It is intentionally much simpler than the AIND ephys
    submission script. Whichever way the script ends, it uploads the capsule, so
    a failed capsule always has its logs on the archive.

    Parameters
    ----------
    script_file_path : pathlib.Path
        Where to write the submission script.
    log_directory : str
        Directory for the slurm and duct logs.
    dataset_directory : str
        The capsule directory the script runs in and uploads from.
    environment_directory : str
        The conda environment to activate. It must provide con-duct and the
        DANDI CLI.
    container_image : str
        The container image reference, for example
        ``ghcr.io/dandi-compute/dandi-compute-lfp:latest``.
    image_file_path : str
        Where the image is kept once pulled, so every capsule of the same
        version runs the one copy. It is pulled there by the first capsule
        that finds it missing.
    nwb_file_path : str
        Path to the input NWB file. It is a valid NWB file even though it has no
        ``.nwb`` suffix.
    output_nwb_file_path : str
        Path to write the resulting NWB file.
    parameters_key : str
        The registered LFP parameters key to run.
    temp_name : str
        The name recorded in the done tracker file on completion.
    done_tracker_file_path : str
        The path to the done tracker file.
    """
    raw_template = _RAW_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        log_directory=log_directory,
        dataset_directory=dataset_directory,
        environment_directory=environment_directory,
        container_image=container_image,
        image_file_path=image_file_path,
        nwb_file_path=nwb_file_path,
        output_nwb_file_path=output_nwb_file_path,
        parameters_key=parameters_key,
        temp_name=temp_name,
        done_tracker_file_path=done_tracker_file_path,
    )
    script_file_path.write_text(data=f"{script}\n")
