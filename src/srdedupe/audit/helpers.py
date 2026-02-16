"""Helper utilities for audit logging.

Audit-specific utility functions: run ID generation, environment info,
and git/package/platform information.

For timestamp and hashing utilities, see srdedupe.utils.
"""

import secrets
import subprocess
import sys
from datetime import UTC, datetime

__all__ = [
    "generate_run_id",
    "get_git_sha",
    "get_package_version",
    "get_python_version",
    "get_platform_info",
    "get_dependency_versions",
    "parse_iso_timestamp",
]


def generate_run_id() -> str:
    """Generate unique run identifier.

    Returns
    -------
    str
        Run ID in format: ISO8601_timestamp__random_suffix.
    """
    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    suffix = secrets.token_hex(4)
    return f"{timestamp}__{suffix}"


def parse_iso_timestamp(iso_str: str) -> datetime:
    """Parse ISO8601 timestamp string to timezone-aware datetime.

    Handles both 'Z' and '+00:00' UTC suffixes.

    Parameters
    ----------
    iso_str : str
        ISO8601 timestamp string.

    Returns
    -------
    datetime
        Timezone-aware UTC datetime.
    """
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def get_git_sha() -> str | None:
    """Get current Git commit SHA if in repository.

    Returns
    -------
    str | None
        Short Git SHA (7 chars) or None if unavailable.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        sha = result.stdout.strip()
        return sha[:7] if sha else None
    except (subprocess.SubprocessError, FileNotFoundError, subprocess.TimeoutExpired):
        return None


def get_package_version() -> str:
    """Get srdedupe package version.

    Returns
    -------
    str
        Package version or "unknown".
    """
    try:
        import importlib.metadata

        return importlib.metadata.version("srdedupe")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def get_python_version() -> str:
    """Get Python version string.

    Returns
    -------
    str
        Python version (e.g., "3.12.3").
    """
    return sys.version.split()[0]


def get_platform_info() -> str:
    """Get platform information.

    Returns
    -------
    str
        Platform string (e.g., "Linux-6.8.0-x86_64").
    """
    import platform

    system = platform.system()
    release = platform.release()
    machine = platform.machine()
    return f"{system}-{release}-{machine}"


def get_dependency_versions(packages: list[str]) -> dict[str, str]:
    """Get versions of specified packages.

    Parameters
    ----------
    packages : list[str]
        List of package names to query.

    Returns
    -------
    dict[str, str]
        Mapping of package name to version.
    """
    import importlib.metadata

    versions: dict[str, str] = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "unknown"
    return versions
