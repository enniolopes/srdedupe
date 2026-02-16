"""Timestamp utilities for SR-Dedupe.

This module provides consistent timestamp functions across the codebase.
"""

from datetime import UTC, datetime
from pathlib import Path

__all__ = ["get_iso_timestamp", "get_file_mtime"]


def get_iso_timestamp() -> str:
    """Get current UTC timestamp in ISO8601 format with microseconds.

    Returns
    -------
    str
        ISO8601 timestamp with microseconds (e.g., "2026-02-03T12:34:56.123456Z").

    Notes
    -----
    This function includes microseconds for high-precision audit logging.
    Use this as the canonical timestamp function across the codebase.
    """
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def get_file_mtime(file_path: Path) -> str:
    """Get modification time of file as ISO8601 timestamp.

    Parameters
    ----------
    file_path : Path
        Path to file.

    Returns
    -------
    str
        ISO8601 timestamp (e.g., '2024-01-30T12:00:00Z'), or empty string if unable to read.

    Notes
    -----
    Uses file's st_mtime from stat(). Falls back to empty string if inaccessible.
    Microseconds are truncated for file modification times.
    """
    try:
        file_stat = file_path.stat()
        mtime = datetime.fromtimestamp(file_stat.st_mtime, UTC)
        return mtime.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except (OSError, ValueError):
        return ""
