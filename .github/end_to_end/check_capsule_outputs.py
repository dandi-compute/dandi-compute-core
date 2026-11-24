"""
Check that a pipeline run left what a job capsule needs, and summarize its resource use.

Upstream's `tests/check_pipeline_results.py` checks the science of the outputs. This checks the
contract with `submission_template.txt`, whose post-run moves fail on any missing path, and that
the pipeline ran the exact parameters it was given. It also writes each step's runtime and peak
memory from the Nextflow trace to the job summary.
"""

import csv
import json
import os
import pathlib
import sys

import beartype
import click

# The paths `submission_template.txt` moves out of the results directory after the run.
_CAPSULE_PATHS = (
    "nwb",
    "visualization",
    "visualization_output.json",
    "quality_control.json",
    "quality_control",
    "postprocessed",
    "nextflow/trace.txt",
    "provenance.json",
)


@beartype.beartype
def find_capsule_problems(*, results_directory: pathlib.Path, parameters: dict, sorter: str) -> list[str]:
    problems = [
        f"Missing '{relative_path}', which the capsule moves after the run."
        for relative_path in _CAPSULE_PATHS
        if not (results_directory / relative_path).exists()
    ]

    nwb_files = sorted((results_directory / "nwb").glob("*.nwb"))
    if len(nwb_files) != 1:
        problems.append(f"Expected one NWB file in 'nwb', found {[path.name for path in nwb_files]}.")

    provenance_file_path = results_directory / "provenance.json"
    if provenance_file_path.exists():
        provenance = json.loads(provenance_file_path.read_text())
        if provenance["sorter"] != sorter:
            problems.append(f"The run used sorter {provenance['sorter']!r}, not {sorter!r}.")
        if provenance["container_tag"] != parameters["pipeline_version"]:
            problems.append(
                f"The run used container tag {provenance['container_tag']!r}, "
                f"not the release {parameters['pipeline_version']!r} the parameters target."
            )
        if provenance["parameters"]["params_file_content"] != parameters:
            problems.append("The parameters in 'provenance.json' differ from the parameters file given to the run.")

    return problems


@beartype.beartype
def read_trace(trace_file_path: pathlib.Path, /) -> list[dict]:
    with trace_file_path.open(newline="") as trace_file:
        rows = list(csv.DictReader(trace_file, delimiter="\t"))
    return rows


@beartype.beartype
def format_trace_summary(*, archetype: str, rows: list[dict]) -> str:
    lines = [
        f"### Step resources for `{archetype}`",
        "",
        "| Step | Status | Exit | Realtime | %CPU | Peak RSS | Read | Written |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    lines.extend(
        f"| {row['name']} | {row['status']} | {row['exit']} | {row['realtime']} | {row['%cpu']} "
        f"| {row['peak_rss']} | {row['rchar']} | {row['wchar']} |"
        for row in rows
    )
    summary = "\n".join(lines) + "\n"
    return summary


@click.command()
@click.option("--results", required=True, type=click.Path(path_type=pathlib.Path, exists=True, file_okay=False))
@click.option("--params", required=True, type=click.Path(path_type=pathlib.Path, exists=True, dir_okay=False))
@click.option("--archetype", required=True)
@click.option("--sorter", default="lupin", show_default=True)
def _main(results: pathlib.Path, params: pathlib.Path, archetype: str, sorter: str):
    parameters = json.loads(params.read_text())

    trace_file_path = results / "nextflow" / "trace.txt"
    rows = read_trace(trace_file_path) if trace_file_path.exists() else []
    summary = format_trace_summary(archetype=archetype, rows=rows)
    click.echo(summary)
    if "GITHUB_STEP_SUMMARY" in os.environ:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as step_summary:
            step_summary.write(summary)

    problems = find_capsule_problems(results_directory=results, parameters=parameters, sorter=sorter)
    problems.extend(
        f"Step '{row['name']}' ended {row['status']} with exit {row['exit']}."
        for row in rows
        if row["status"] != "COMPLETED"
    )
    for problem in problems:
        click.echo(f"::error::{problem}")
    if problems:
        sys.exit(1)
    click.echo(f"The outputs of '{archetype}' hold everything a capsule needs.")


if __name__ == "__main__":
    _main()
