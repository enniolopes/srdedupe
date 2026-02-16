"""Stable identifiers and digest calculation for canonical records.

This module implements deterministic RID (Record Identifier) and digest
calculation as specified in the canonical schema contract.
"""

import hashlib
import json
import uuid
from typing import Any

# Project-fixed namespace UUID for deterministic UUIDv5 generation
# This is a constant that should never change across versions
SRDEDUPE_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def calculate_record_digest(raw_tags: list[dict[str, Any]], source_format: str) -> str:
    """Calculate deterministic content fingerprint of a raw record.

    Parameters
    ----------
    raw_tags : list[dict[str, Any]]
        List of raw tag objects (with 'tag' and 'value' keys).
    source_format : str
        Source format ('ris', 'nbib', or 'unknown').

    Returns
    -------
    str
        SHA-256 digest in format "sha256:<hex>".

    Notes
    -----
    The digest is calculated from a canonical JSON representation that includes
    only the tag name and value (omitting line numbers to avoid platform drift).
    The source_format is included to prevent accidental collisions between
    different formats.
    """
    # Create canonical minimal representation (tag + value only, in order)
    canonical_raw = {
        "tags": [{"tag": t["tag"], "value": t["value"]} for t in raw_tags],
        "source_format": source_format,
    }

    # Serialize with deterministic settings
    json_bytes = json.dumps(
        canonical_raw,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),  # No whitespace
    ).encode("utf-8")

    # Calculate SHA-256
    digest = hashlib.sha256(json_bytes).hexdigest()
    return f"sha256:{digest}"


def calculate_source_digest(file_bytes: bytes) -> str:
    """Calculate SHA-256 digest of source file bytes.

    Parameters
    ----------
    file_bytes : bytes
        Complete source file content as bytes.

    Returns
    -------
    str
        SHA-256 digest in format "sha256:<hex>".
    """
    digest = hashlib.sha256(file_bytes).hexdigest()
    return f"sha256:{digest}"


def calculate_rid(source_digest: str, record_digest: str) -> str:
    """Calculate deterministic UUIDv5 record identifier.

    The RID is generated using UUIDv5 with a project-fixed namespace UUID
    and a name constructed from the source and record digests. This ensures:
    - Deterministic generation (same inputs -> same RID)
    - Uniqueness across different source files
    - Independence from file paths

    Parameters
    ----------
    source_digest : str
        SHA-256 digest of source file (format "sha256:<hex>").
    record_digest : str
        SHA-256 digest of record content (format "sha256:<hex>").

    Returns
    -------
    str
        UUIDv5 string in standard format.

    Notes
    -----
    RID Invariants:
    1. Same file bytes + same record -> same RID
    2. Same record in different files -> different RID (different source_digest)
    3. Renaming/moving file does NOT change RID
    4. Parser changes that alter raw content will change record_digest and RID
    """
    # Construct deterministic name from digests
    name = f"{source_digest}:{record_digest}"

    # Generate UUIDv5 using project namespace
    rid = uuid.uuid5(SRDEDUPE_NAMESPACE, name)

    return str(rid)


def validate_digest_format(digest: str) -> bool:
    """Validate that a digest string has the correct format.

    Parameters
    ----------
    digest : str
        Digest string to validate.

    Returns
    -------
    bool
        True if format is valid ("sha256:<64-hex-chars>"), False otherwise.
    """
    if not digest.startswith("sha256:"):
        return False

    hex_part = digest[7:]  # Remove "sha256:" prefix
    if len(hex_part) != 64:
        return False

    # Check if all characters are valid hex
    try:
        int(hex_part, 16)
        return True
    except ValueError:
        return False


def validate_rid_format(rid: str) -> bool:
    """Validate that a RID string is a valid UUIDv5.

    Parameters
    ----------
    rid : str
        RID string to validate.

    Returns
    -------
    bool
        True if valid UUIDv5 format, False otherwise.
    """
    try:
        parsed = uuid.UUID(rid)
        # Check if it's version 5
        return parsed.version == 5
    except (ValueError, AttributeError):
        return False
