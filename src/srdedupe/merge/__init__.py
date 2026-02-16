"""Canonical merge module for deduplication."""

from srdedupe.merge.models import (
    MergedRecord,
    MergePolicy,
    MergeProvenance,
    MergeProvenanceField,
    MergeSummary,
)
from srdedupe.merge.processor import process_canonical_merge
from srdedupe.merge.ris_writer import format_canon_as_ris, format_ris_record
from srdedupe.merge.survivor import select_survivor

__all__ = [
    "MergePolicy",
    "MergeProvenance",
    "MergeProvenanceField",
    "MergeSummary",
    "MergedRecord",
    "format_canon_as_ris",
    "format_ris_record",
    "process_canonical_merge",
    "select_survivor",
]
