"""
Compose the parameters file of an end-to-end archetype from a registered parameters file.

An archetype names a key of `registries/registered_params.json` and the overrides layered on it.
The registered file is checked against its registered MD5, as a capsule would check it, and the
composed parameters are checked against the schema of the pipeline release they target.
"""

import copy
import hashlib
import json
import pathlib

import beartype
import click

import dandi_compute_code.aind_ephys_pipeline

_ARCHETYPES_FILE_PATH = pathlib.Path(__file__).parent / "archetypes.json"
_PIPELINE_PACKAGE_DIRECTORY = pathlib.Path(dandi_compute_code.aind_ephys_pipeline.__file__).parent


@beartype.beartype
def load_archetypes() -> dict:
    archetypes = json.loads(_ARCHETYPES_FILE_PATH.read_text())
    return archetypes


@beartype.beartype
def load_registered_parameters(parameters_key: str, /) -> dict:
    registry = json.loads((_PIPELINE_PACKAGE_DIRECTORY / "registries" / "registered_params.json").read_text())
    if parameters_key not in registry:
        message = f"Parameters key {parameters_key!r} is not registered. Registered keys are {list(registry)}."
        raise ValueError(message)

    entry = registry[parameters_key]
    parameters_file_path = _PIPELINE_PACKAGE_DIRECTORY / "params" / entry["path"]
    actual_md5 = hashlib.md5(parameters_file_path.read_bytes()).hexdigest()
    if actual_md5 != entry["md5"]:
        message = f"MD5 mismatch for {parameters_file_path.name}: expected {entry['md5']!r}, got {actual_md5!r}."
        raise ValueError(message)

    parameters = json.loads(parameters_file_path.read_text())
    return parameters


@beartype.beartype
def layer_overrides(*, base: dict, overrides: dict, path: str = "") -> dict:
    """
    Merge *overrides* into a copy of *base*.

    Every overridden key must already exist in the registered file. A misspelled key would
    otherwise pass silently wherever the schema allows extra properties.
    """
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        key_path = f"{path}.{key}" if path else key
        if key not in merged:
            message = f"Override '{key_path}' is not a key of the registered parameters."
            raise KeyError(message)
        if isinstance(value, dict) and isinstance(merged[key], dict):
            merged[key] = layer_overrides(base=merged[key], overrides=value, path=key_path)
        else:
            merged[key] = value
    return merged


@click.group()
def _main():
    pass


@_main.command(name="matrix")
def _matrix():
    """Print the archetype names as a JSON list, for a GitHub Actions matrix."""
    names = list(load_archetypes())
    click.echo(json.dumps(names))


@_main.command(name="compose")
@click.option("--archetype", required=True, help="The archetype name, a key of `archetypes.json`.")
@click.option("--output", required=True, type=click.Path(path_type=pathlib.Path), help="Where to write the file.")
def _compose(archetype: str, output: pathlib.Path):
    """Write the composed and validated parameters file of an archetype."""
    archetypes = load_archetypes()
    if archetype not in archetypes:
        message = f"Unknown archetype {archetype!r}. Known archetypes are {list(archetypes)}."
        raise click.BadParameter(message)

    entry = archetypes[archetype]
    base = load_registered_parameters(entry["params_key"])
    parameters = layer_overrides(base=base, overrides=entry["overrides"])
    dandi_compute_code.aind_ephys_pipeline.validate_aind_ephys_parameters(
        parameters=parameters,
        pipeline_version=parameters["pipeline_version"],
        parameters_file_name=f"{archetype} (from {entry['params_key']!r})",
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(parameters, indent=4) + "\n")
    click.echo(f"Wrote {archetype!r} for pipeline {parameters['pipeline_version']} to {output}")


if __name__ == "__main__":
    _main()
