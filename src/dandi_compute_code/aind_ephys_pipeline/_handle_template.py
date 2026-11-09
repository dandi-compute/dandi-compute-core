import pathlib

import beartype
import jinja2

from ._globals import _FIRST_PIPELINE_VERSION_WITH_NWB_INPUT_FOLDER, _RAW_TEMPLATE_FILE_PATH
from ._pipeline_version import _parse_pipeline_version


@beartype.beartype
def generate_aind_ephys_submission_script(
    script_file_path: pathlib.Path,
    log_directory: str,
    nwb_file_path: str,
    results_directory: str,
    work_directory: str,
    apptainer_cache_directory: str,
    environment_directory: str,
    config_file_path: str,
    pipeline_file_path: str,
    pipeline_repo_directory: str,
    pipeline_version: str,
    temp_name: str,
    done_tracker_file_path: str,
    params_file_path: str,
) -> None:
    """
    Generate AIND Ephys submission script from template.

    Arguments are ordered as they occur in the submission template.

    Parameters
    ----------
    script_file_path : pathlib.Path
        Where to write the submission script.
    log_directory : str
        The log directory.
    nwb_file_path : str
        The input NWB file path. Up to pipeline v1.4.1 it is passed to the job dispatch step as
        ``--nwb-files``. From v1.5.0 the script links it into an input folder under the work
        directory as the one ``.nwb`` file there, and the pipeline reads that folder.
    results_directory : str
        The results directory.
    work_directory : str
        The Nextflow work directory of this job.
    apptainer_cache_directory : str
        The Apptainer cache directory.
    environment_directory : str
        The conda environment to activate.
    config_file_path : str
        The configuration file path.
    pipeline_file_path : str
        The pipeline file path.
    pipeline_repo_directory : str
        Path to the base pipeline repository intended to be used.
    pipeline_version : str, optional
        The pipeline version, which is used to checkout a branch of the pipeline
        repository.
    temp_name : str
        The name of the temporary processing directory.
    done_tracker_file_path : str
        The path to the 'done' tracker file.
    params_file_path : str
        The parameters file path.
    """
    uses_nwb_input_folder = (
        _parse_pipeline_version(pipeline_version, label="pipeline") >= _FIRST_PIPELINE_VERSION_WITH_NWB_INPUT_FOLDER
    )
    if uses_nwb_input_folder:
        data_path = str(pathlib.Path(work_directory) / "input")
    else:
        data_path = str(pathlib.Path(nwb_file_path).parent)

    raw_template = _RAW_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        log_directory=log_directory,
        nwb_file_path=nwb_file_path,
        data_path=data_path,
        uses_nwb_input_folder=uses_nwb_input_folder,
        nwb_link_name=f"{pathlib.Path(nwb_file_path).name}.nwb",
        results_directory=results_directory,
        work_directory=work_directory,
        apptainer_cache_directory=apptainer_cache_directory,
        environment_directory=environment_directory,
        config_file_path=config_file_path,
        pipeline_file_path=pipeline_file_path,
        pipeline_repo_directory=pipeline_repo_directory,
        pipeline_version=pipeline_version,
        params_file_path=params_file_path,
        temp_name=temp_name,
        done_tracker_file_path=done_tracker_file_path,
    )
    script_file_path.write_text(data=f"{script}\n")  # Extra newline to prevent improper console wrapping in manual mode
