"""Shared data types for srdedupe.

This package contains the core shared dataclasses, identifiers, and type
definitions consumed across the pipeline.

Domain-specific types live closer to their consumers:
- Audit types → srdedupe.audit.models
- Candidate types → srdedupe.candidates.models
"""

from srdedupe.models.identifiers import (
    SRDEDUPE_NAMESPACE,
    calculate_record_digest,
    calculate_rid,
    calculate_source_digest,
    validate_digest_format,
    validate_rid_format,
)
from srdedupe.models.records import (
    SCHEMA_VERSION,
    AuthorParsed,
    Canon,
    CanonicalRecord,
    Flags,
    Keys,
    Meta,
    Raw,
    RawTag,
)

__all__ = [
    # Schema version
    "SCHEMA_VERSION",
    # Record models
    "CanonicalRecord",
    "Meta",
    "Raw",
    "RawTag",
    "Canon",
    "AuthorParsed",
    "Keys",
    "Flags",
    # Identifiers
    "SRDEDUPE_NAMESPACE",
    "calculate_record_digest",
    "calculate_rid",
    "calculate_source_digest",
    "validate_digest_format",
    "validate_rid_format",
]
