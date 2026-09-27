import functools
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

import beartype
import jsonschema.validators

from ._pipeline_version import _parse_pipeline_version

_log = logging.getLogger(__name__)

_PARAMS_SCHEMA_URL_TEMPLATE = "https://raw.githubusercontent.com/AllenNeuralDynamics/aind-ephys-pipeline/{ref}/pipeline/default_params_schema.json"
# Releases before v1.3.0 ship a schema that rejects `motion_correction.compute` and `apply`,
# which their preprocessing capsule accepts and the legacy parameter files rely on.
_MINIMUM_VALIDATED_PIPELINE_VERSION = (1, 3, 0)
_BANNER_RULE = "!" * 100


class InvalidParametersError(ValueError):
    """Raised when a parameters file does not conform to the schema of the requested pipeline version."""


@beartype.beartype
def _banner(*, title: str, body: str) -> str:
    message = f"\n{_BANNER_RULE}\n{title}\n{_BANNER_RULE}\n{body}\n{_BANNER_RULE}"
    return message


@functools.lru_cache(maxsize=None)
@beartype.beartype
def _fetch_params_schema(pipeline_version: str, /) -> tuple[str, dict]:
    """
    The URL and contents of the parameters schema the AIND ephys pipeline ships at *pipeline_version*.

    Upstream tags carry no ``v`` prefix while callers often pass one, so both spellings are tried.
    """
    bare_version = pipeline_version.removeprefix("v")
    refs = dict.fromkeys([pipeline_version, bare_version, f"v{bare_version}"])
    for ref in refs:
        url = _PARAMS_SCHEMA_URL_TEMPLATE.format(ref=urllib.parse.quote(ref, safe=""))
        try:
            with urllib.request.urlopen(url=url) as response:
                schema = json.loads(response.read().decode())
            return url, schema
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise

    body = (
        f"Could not find `pipeline/default_params_schema.json` for pipeline version {pipeline_version!r} "
        "in the AIND ephys pipeline repository.\n"
        f"Tried refs: {list(refs)}.\n\n"
        "Parameters cannot be checked against a release without its schema. "
        "Make sure the version names a release tag of https://github.com/AllenNeuralDynamics/aind-ephys-pipeline."
    )
    message = _banner(title="AIND EPHYS PARAMETERS SCHEMA NOT FOUND. NO JOB CAPSULE WAS CREATED.", body=body)
    raise ValueError(message)


@beartype.beartype
def validate_aind_ephys_parameters(*, parameters: dict, pipeline_version: str, parameters_file_name: str) -> None:
    """
    Validate AIND ephys parameters against the schema the pipeline ships at a version.

    The schema is fetched from the pipeline repository at that release tag, which is the source of
    truth for what the pipeline accepts. This runs before any job capsule is formed, so parameters
    the pipeline would reject or crash on never reach the queue. Versions before v1.3.0 are not
    validated.

    Parameters
    ----------
    parameters : dict
        The loaded contents of the parameters file.
    pipeline_version : str
        The pipeline version the parameters will be run with, such as ``"1.3.3"``.
    parameters_file_name : str
        The name of the parameters file, used in the error message.

    Raises
    ------
    InvalidParametersError
        If the parameters do not conform to the schema. The message lists every problem found.
    ValueError
        If the pipeline repository has no parameters schema at that version.
    urllib.error.URLError
        If the schema could not be fetched for any other reason.
    """
    if _parse_pipeline_version(pipeline_version, label="requested pipeline") < _MINIMUM_VALIDATED_PIPELINE_VERSION:
        _log.warning(
            f"Pipeline version {pipeline_version!r} predates v1.3.0. "
            f"Skipping schema validation of '{parameters_file_name}'."
        )
        return
    schema_url, schema = _fetch_params_schema(pipeline_version)

    validator_class = jsonschema.validators.validator_for(schema)
    validator_class.check_schema(schema)
    validator = validator_class(schema)
    errors = sorted(validator.iter_errors(parameters), key=lambda error: error.json_path)
    if not errors:
        return

    problems = "\n".join(f"  {index}. At '{error.json_path}': {error.message}" for index, error in enumerate(errors, 1))
    body = (
        f"Parameters file '{parameters_file_name}' does not conform to the parameters schema for pipeline "
        f"version {pipeline_version!r}.\n"
        f"Schema: {schema_url}\n"
        f"Found {len(errors)} problem(s).\n\n"
        f"{problems}\n\n"
        "Write a parameters file for this pipeline version and register it under a new key in "
        "`registries/registered_params.json`, or pick a parameters key written for this pipeline version."
    )
    message = _banner(title="INVALID AIND EPHYS PARAMETERS. NO JOB CAPSULE WAS CREATED.", body=body)
    raise InvalidParametersError(message)
