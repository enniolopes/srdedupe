"""Audit logging and run manifest subsystem for srdedupe.

Main Components
---------------
- RunContext: High-level context manager for pipeline runs
- AuditLogger: JSONL event logger
- ManifestWriter: Run manifest builder
"""

from srdedupe.audit.context import RunContext
from srdedupe.audit.helpers import generate_run_id
from srdedupe.audit.logger import AuditLogger
from srdedupe.audit.manifest import ManifestWriter
from srdedupe.utils import (
    calculate_file_sha256,
    calculate_string_sha256,
    get_iso_timestamp,
)

__all__ = [
    "RunContext",
    "AuditLogger",
    "ManifestWriter",
    "generate_run_id",
    "get_iso_timestamp",
    "calculate_file_sha256",
    "calculate_string_sha256",
]
