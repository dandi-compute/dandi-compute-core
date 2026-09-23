"""CLI help text tests for top-level groups and commands."""

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("args", "expected_text"),
    [
        (["--help"], "Run compute workflows and job management tasks for DANDI assets."),
        (["prepare", "--help"], "Run preparation workflows that generate submission scripts."),
        (["prepare", "aind", "--help"], "Prepare an AIND ephys job, or create test job capsules with --test."),
        (["submit", "--help"], "Submit a previously prepared pipeline script via sbatch."),
        (["clean", "--help"], "Clean work, finished dispatch directories or unsubmitted capsules."),
        (["jobs", "--help"], "Create, dispatch and report on job capsules."),
        (
            ["jobs", "create", "--help"],
            "Create a job capsule for every qualifying asset that does not have one yet.",
        ),
        (["jobs", "refresh", "--help"], "Rewrite jobs.tsv into both Dandisets."),
        (["jobs", "pending", "--help"], "Report whether any queued jobs are awaiting submission."),
        (["jobs", "process", "--help"], "Hand every pending job capsule to its pipeline's SLURM array dispatcher."),
        (["issues", "--help"], "Scan logs and write per-capsule and aggregate issue reports."),
        (["issues", "dump", "--help"], "Scan nextflow and slurm logs and write per-capsule issue records."),
        (["issues", "summarize", "--help"], "Summarize discovered issue lines by descending occurrence count."),
        (
            ["archive", "--help"],
            "Archive one job capsule (--job) or every capsule with a --status.",
        ),
    ],
)
def test_cli_help_includes_descriptions(args: list[str], expected_text: str) -> None:
    """CLI help output includes the configured command/group description text."""
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, args)

    assert result.exit_code == 0
    assert expected_text in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "args",
    [["queue", "--help"], ["jobs", "stats", "--help"], ["jobs", "clean", "--help"]],
)
def test_cli_removed_commands_are_gone(args: list[str]) -> None:
    """The retired queue group and its stats and clean commands are no longer registered."""
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, args)

    assert result.exit_code != 0
    assert "No such command" in result.output
