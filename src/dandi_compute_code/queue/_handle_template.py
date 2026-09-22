import pathlib

import jinja2
import pydantic

from ._globals import _RAW_ARRAY_DISPATCH_TEMPLATE_FILE_PATH


@pydantic.validate_call
def generate_array_dispatch_script(
    script_file_path: pathlib.Path,
    job_name: str,
    dispatch_directory: str,
    memory: str,
    cpus_per_task: int,
    partition: str,
    time_limit: str,
    array_specification: str,
    dandiset_id: str,
    manifest_file_path: str,
    keep_task_directory: bool = False,
) -> None:
    """
    Generate a pipeline's array dispatch script from the template.

    Arguments are ordered as they occur in the dispatch template.

    Parameters
    ----------
    script_file_path : pathlib.Path
        Where to write the dispatch script.
    job_name : str
        The SLURM job name identifying this pipeline's dispatcher.
    dispatch_directory : str
        Directory holding the manifest, the array logs, and each task's working
        tree.
    memory : str
        Memory requested per array task, taken from the pipeline's submission
        template so that it matches what the capsule asks for.
    cpus_per_task : int
        CPUs requested per array task.
    partition : str
        The SLURM partition the array is submitted to.
    time_limit : str
        Wall time requested per array task.
    array_specification : str
        The ``--array`` specification, including the concurrency throttle.
    dandiset_id : str
        The Dandiset each capsule is downloaded from and uploaded back to.
    manifest_file_path : str
        File listing one capsule ``code`` directory per line, read by array task
        index.
    keep_task_directory : bool, optional
        When ``True``, each task leaves its working tree on disk for debugging
        instead of removing it.
    """
    raw_template = _RAW_ARRAY_DISPATCH_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        job_name=job_name,
        dispatch_directory=dispatch_directory,
        memory=memory,
        cpus_per_task=cpus_per_task,
        partition=partition,
        time_limit=time_limit,
        array_specification=array_specification,
        dandiset_id=dandiset_id,
        manifest_file_path=manifest_file_path,
        keep_task_directory=keep_task_directory,
    )
    script_file_path.write_text(data=f"{script}\n")
