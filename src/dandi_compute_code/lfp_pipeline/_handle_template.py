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
    container_name: str,
    container_image: str,
    nwb_file_path: str,
    output_nwb_file_path: str,
    parameters_key: str,
    temp_name: str,
    done_tracker_file_path: str,
) -> None:
    """
    Generate the LFP pipeline sbatch submission script from the template.

    The script uses datalad-containers and duct to run the LFP runtime container
    on a local NWB file. It is intentionally much simpler than the AIND ephys
    submission script.

    Parameters
    ----------
    script_file_path : pathlib.Path
        Where to write the submission script.
    log_directory : str
        Directory for the slurm and duct logs.
    dataset_directory : str
        The datalad dataset directory in which the container runs.
    environment_directory : str
        The conda environment to activate. It must provide datalad,
        datalad-container, con-duct, and apptainer.
    container_name : str
        The datalad container registration name.
    container_image : str
        The container image reference, for example
        ``ghcr.io/dandi-compute/dandi-compute-lfp:latest``.
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
        container_name=container_name,
        container_image=container_image,
        nwb_file_path=nwb_file_path,
        output_nwb_file_path=output_nwb_file_path,
        parameters_key=parameters_key,
        temp_name=temp_name,
        done_tracker_file_path=done_tracker_file_path,
    )
    script_file_path.write_text(data=f"{script}\n")
