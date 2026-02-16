"""Tests for audit helpers module."""

import subprocess
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from srdedupe.audit.helpers import (
    generate_run_id,
    get_dependency_versions,
    get_git_sha,
    get_package_version,
    get_platform_info,
    get_python_version,
    parse_iso_timestamp,
)


@pytest.mark.unit
def test_generate_run_id_format_and_uniqueness() -> None:
    """Test run ID has correct format and successive calls are unique."""
    rid1 = generate_run_id()
    rid2 = generate_run_id()

    # Format: ISO8601__hex8
    parts = rid1.split("__")
    assert len(parts) == 2
    assert parts[0].endswith("Z")
    assert len(parts[1]) == 8

    assert rid1 != rid2


@pytest.mark.unit
@pytest.mark.parametrize(
    ("iso_str", "expected_year"),
    [
        ("2026-02-03T12:00:00Z", 2026),
        ("2026-02-03T12:00:00+00:00", 2026),
        ("2026-02-03T12:00:00.123456Z", 2026),
    ],
)
def test_parse_iso_timestamp(iso_str: str, expected_year: int) -> None:
    """Test parsing both Z and +00:00 suffixed timestamps."""
    result = parse_iso_timestamp(iso_str)

    assert isinstance(result, datetime)
    assert result.year == expected_year
    assert result.tzinfo is not None


@pytest.mark.unit
def test_get_git_sha_success() -> None:
    """Test git SHA is truncated to 7 chars on success."""
    mock_result = Mock(stdout="abcdef1234567890\n")

    with patch("subprocess.run", return_value=mock_result):
        assert get_git_sha() == "abcdef1"


@pytest.mark.unit
@pytest.mark.parametrize(
    "side_effect",
    [subprocess.SubprocessError, FileNotFoundError, subprocess.TimeoutExpired("git", 5)],
)
def test_get_git_sha_returns_none_on_failure(side_effect: type | Exception) -> None:
    """Test git SHA returns None for all failure modes."""
    with patch("subprocess.run", side_effect=side_effect):
        assert get_git_sha() is None


@pytest.mark.unit
def test_environment_info_functions() -> None:
    """Test version/platform functions return non-empty strings."""
    assert "." in get_python_version()
    assert "-" in get_platform_info()
    # Package version is either semver or "unknown" when not installed
    version = get_package_version()
    assert version == "unknown" or "." in version


@pytest.mark.unit
def test_get_dependency_versions() -> None:
    """Test known packages return versions, unknown return 'unknown'."""
    versions = get_dependency_versions(["pytest", "nonexistent_xyz_pkg"])

    assert "." in versions["pytest"]
    assert versions["nonexistent_xyz_pkg"] == "unknown"
