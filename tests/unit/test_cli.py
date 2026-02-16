"""Tests for CLI module."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from srdedupe.cli.main import cli


@pytest.fixture
def runner() -> CliRunner:
    """Provide Click test CLI runner."""
    return CliRunner()


# ---------------------------------------------------------------------------
# Top-level CLI
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cli_version_flag(runner: CliRunner) -> None:
    """Test --version flag outputs version string."""
    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert "srdedupe" in result.output


@pytest.mark.unit
def test_cli_help(runner: CliRunner) -> None:
    """Test --help output lists commands."""
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "srdedupe" in result.output
    assert "parse" in result.output
    assert "deduplicate" in result.output


@pytest.mark.unit
def test_cli_invalid_command(runner: CliRunner) -> None:
    """Test invalid command returns non-zero exit code."""
    result = runner.invoke(cli, ["invalid-command"])

    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# parse command
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_help(runner: CliRunner) -> None:
    """Test parse command help."""
    result = runner.invoke(cli, ["parse", "--help"])

    assert result.exit_code == 0
    assert "Parse bibliographic files" in result.output


@pytest.mark.integration
def test_parse_file(runner: CliRunner, tmp_path: Path) -> None:
    """Test parse command with a single file."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures" / "synthetic"
    sample_file = fixtures_dir / "sample.ris"

    if not sample_file.exists():
        pytest.skip("Sample file not found")

    output_file = tmp_path / "output.jsonl"

    result = runner.invoke(cli, ["parse", str(sample_file), "-o", str(output_file)])

    assert result.exit_code == 0
    assert output_file.exists()
    assert "Successfully wrote" in result.output


@pytest.mark.integration
def test_parse_folder(runner: CliRunner, tmp_path: Path) -> None:
    """Test parse command with a folder."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures" / "synthetic"

    if not fixtures_dir.exists():
        pytest.skip("Fixtures folder not found")

    output_file = tmp_path / "output.jsonl"

    result = runner.invoke(cli, ["parse", str(fixtures_dir), "-o", str(output_file)])

    assert result.exit_code == 0
    assert output_file.exists()
    assert "Successfully wrote" in result.output


# ---------------------------------------------------------------------------
# deduplicate command
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_deduplicate_help(runner: CliRunner) -> None:
    """Test deduplicate command help."""
    result = runner.invoke(cli, ["deduplicate", "--help"])

    assert result.exit_code == 0
    assert "deduplicate" in result.output.lower()
    assert "input_path" in result.output.lower()


@pytest.mark.unit
def test_deduplicate_nonexistent_file(runner: CliRunner) -> None:
    """Test deduplicate with nonexistent file."""
    result = runner.invoke(cli, ["deduplicate", "nonexistent.ris"])

    assert result.exit_code != 0


@pytest.mark.unit
def test_deduplicate_execution(runner: CliRunner, tmp_path: Path) -> None:
    """Test deduplicate command execution with minimal file."""
    test_file = tmp_path / "test.ris"
    test_file.write_text("TY  - JOUR\nER  -\n")

    result = runner.invoke(cli, ["deduplicate", str(test_file)])

    assert result.exit_code == 0
    assert "deduplicated" in result.output.lower()


@pytest.mark.unit
def test_deduplicate_verbose_flag(runner: CliRunner, tmp_path: Path) -> None:
    """Test verbose flag produces extra output."""
    test_file = tmp_path / "test.ris"
    test_file.write_text("TY  - JOUR\nER  -\n")

    result = runner.invoke(cli, ["deduplicate", str(test_file), "--verbose"])

    assert result.exit_code == 0
    assert "starting deduplication pipeline" in result.output.lower()
