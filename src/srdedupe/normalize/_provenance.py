"""Provenance tracking helpers for normalization.

This module provides utilities to build provenance entries consistently
across all normalization functions.
"""

from typing import Any

from srdedupe.models.records import RawTag

# Normalization version for provenance tracking
NORMALIZATION_VERSION = "1.0.0"


def build_provenance_entry(
    field_name: str,
    raw_tags: list[RawTag],
    tag_indices: list[int],
    source_format: str,
    transforms: list[dict[str, str]],
    confidence: str = "high",
) -> dict[str, Any]:
    """Build a single provenance entry.

    Parameters
    ----------
    field_name : str
        Canonical field name (e.g., 'canon.doi_norm').
    raw_tags : list[RawTag]
        All raw tags from record.
    tag_indices : list[int]
        Indices of tags used as sources.
    source_format : str
        Source format ('ris', 'nbib', etc.).
    transforms : list[dict[str, str]]
        List of transform dicts with 'name', 'version', 'notes'.
    confidence : str, optional
        Confidence level ('high', 'medium', 'low').

    Returns
    -------
    dict[str, Any]
        Provenance entry ready to add to provenance dict.
    """
    sources = []
    for idx in tag_indices:
        tag = raw_tags[idx]
        sources.append(
            {
                "path": f"raw.tags[{idx}]",
                "tag": tag.tag,
                "value_snippet": tag.value_raw_joined[:120],
                "source_format": source_format,
            }
        )

    return {
        field_name: {
            "sources": sources,
            "transforms": transforms,
            "confidence": confidence,
        }
    }


def add_transform(name: str, notes: str) -> dict[str, str]:
    """Create a transform dict.

    Parameters
    ----------
    name : str
        Transform name (e.g., 'lowercase_ascii').
    notes : str
        Description of what the transform does.

    Returns
    -------
    dict[str, str]
        Transform dict with name, version, and notes.
    """
    return {
        "name": name,
        "version": NORMALIZATION_VERSION,
        "notes": notes,
    }
