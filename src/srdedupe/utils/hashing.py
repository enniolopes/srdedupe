"""Hashing utilities for SR-Dedupe.

This module provides consistent hashing functions for files and strings.
"""

import hashlib
from pathlib import Path

__all__ = [
    "format_sha256",
    "calculate_file_sha256",
    "calculate_file_digest",
    "calculate_string_sha256",
]


def format_sha256(hex_digest: str) -> str:
    """Format SHA256 hash with standard prefix.

    Parameters
    ----------
    hex_digest : str
        Raw hexadecimal digest.

    Returns
    -------
    str
        Formatted hash with "sha256:" prefix.
    """
    return f"sha256:{hex_digest}"


def calculate_file_sha256(path: Path) -> str:
    """Calculate SHA256 hash of file contents from file path.

    Parameters
    ----------
    path : Path
        Path to file.

    Returns
    -------
    str
        SHA256 hash with "sha256:" prefix.

    Raises
    ------
    FileNotFoundError
        If file does not exist.

    Notes
    -----
    This function reads files in chunks for memory efficiency.
    Use this when you have a file path.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    sha256_hash = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)

    return format_sha256(sha256_hash.hexdigest())


def calculate_file_digest(file_bytes: bytes) -> str:
    """Calculate SHA-256 digest of file bytes.

    Parameters
    ----------
    file_bytes : bytes
        Complete file content as bytes.

    Returns
    -------
    str
        SHA-256 digest in format "sha256:<hex>".

    Notes
    -----
    This function computes hash from bytes already in memory.
    Use this when you already have file contents loaded.
    """
    digest = hashlib.sha256(file_bytes).hexdigest()
    return format_sha256(digest)


def calculate_string_sha256(text: str) -> str:
    """Calculate SHA256 hash of string.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        SHA256 hash with "sha256:" prefix.
    """
    sha256_hash = hashlib.sha256(text.encode("utf-8"))
    return format_sha256(sha256_hash.hexdigest())
