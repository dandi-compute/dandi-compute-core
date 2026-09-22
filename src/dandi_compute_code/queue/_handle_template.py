import pathlib

import jinja2
import pydantic

from ._globals import _RAW_ARRAY_DISPATCH_TEMPLATE_FILE_PATH


@pydantic.validate_call
def generate_array_dispatch_script(
    script_file_path: pathlib.Path,
    job_name: str,
    dispatch_directory: str,
    array_specification: str,
    dandiset_id: str,
    manifest_file_path: str,
    keep_task_directory: bool = False,
) -> None:
    """
    Generate a pipeline's array dispatch script from the template.

    Arguments are ordered as they occur in the dispatch template.

    :param script_file_path: Where to write the dispatch script.
    :type script_file_path: pathlib.Path
    :param job_name: The SLURM job name identifying this pipeline's dispatcher.
    :type job_name: str
    :param dispatch_directory: Directory holding the manifest, the array logs, and each
        task's working tree.
    :type dispatch_directory: str
    :param array_specification: The ``--array`` specification, including the concurrency
        throttle.
    :type array_specification: str
    :param dandiset_id: The Dandiset each capsule is downloaded from and uploaded back to.
    :type dandiset_id: str
    :param manifest_file_path: File listing one capsule ``code`` directory per line, read by
        array task index.
    :type manifest_file_path: str
    :param keep_task_directory: When ``True``, each task leaves its working tree on disk for
        debugging instead of removing it.
    :type keep_task_directory: bool
    """
    raw_template = _RAW_ARRAY_DISPATCH_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        job_name=job_name,
        dispatch_directory=dispatch_directory,
        array_specification=array_specification,
        dandiset_id=dandiset_id,
        manifest_file_path=manifest_file_path,
        keep_task_directory=keep_task_directory,
    )
    script_file_path.write_text(data=f"{script}\n")
