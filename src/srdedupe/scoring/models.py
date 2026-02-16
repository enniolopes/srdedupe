"""Data models for pairwise scoring.

This module defines the schema for pair scores and comparison results
produced by the Fellegi-Sunter scoring layer.
"""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class FieldComparison:
    """Comparison result for a single field.

    Attributes
    ----------
    level : str
        Agreement level (e.g., 'exact', 'high', 'missing').
    sim : float | None
        Similarity score if applicable (0.0-1.0), None otherwise.
    weight : float
        Log-likelihood ratio weight for this comparison.
    """

    level: str
    sim: float | None
    weight: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ModelInfo:
    """Model metadata.

    Attributes
    ----------
    name : str
        Model name (e.g., 'fs_v1').
    version : str
        Model version (e.g., '1.0.0').
    """

    name: str
    version: str

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


# Type alias for comparison results - flexible dict-based structure
ComparisonResult = dict[str, FieldComparison]


def comparison_to_dict(comparison: ComparisonResult) -> dict[str, dict[str, Any]]:
    """Convert ComparisonResult to dictionary for JSON serialization.

    Parameters
    ----------
    comparison : ComparisonResult
        Field comparisons mapping.

    Returns
    -------
    dict[str, dict[str, Any]]
        Dictionary with field names as keys and comparison dicts as values.
    """
    return {field: fc.to_dict() for field, fc in comparison.items()}


@dataclass(frozen=True, slots=True)
class PairScore:
    """Pairwise match score with explainability.

    Attributes
    ----------
    pair_id : str
        Deterministic pair identifier (format: "rid_a|rid_b").
    rid_a : str
        First record ID (lexicographically smaller).
    rid_b : str
        Second record ID (lexicographically larger).
    candidate_sources : tuple[dict[str, str], ...]
        Blocking sources that generated this pair (immutable).
    comparison : ComparisonResult
        Per-field comparison results.
    llr : float
        Log-likelihood ratio (total score).
    p_match : float
        Posterior probability of match (0.0-1.0).
    top_contributions : tuple[dict[str, Any], ...]
        Top contributing fields for explainability (immutable).
    warnings : tuple[str, ...]
        Warning codes (e.g., 'title_truncated') (immutable).
    model : ModelInfo
        Model metadata.
    """

    pair_id: str
    rid_a: str
    rid_b: str
    candidate_sources: tuple[dict[str, str], ...]
    comparison: ComparisonResult
    llr: float
    p_match: float
    top_contributions: tuple[dict[str, Any], ...]
    warnings: tuple[str, ...]
    model: ModelInfo

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict
            Complete dictionary representation.
        """
        return {
            "pair_id": self.pair_id,
            "rid_a": self.rid_a,
            "rid_b": self.rid_b,
            "candidate_sources": list(self.candidate_sources),
            "comparison": comparison_to_dict(self.comparison),
            "llr": self.llr,
            "p_match": self.p_match,
            "explain": {"top_contributions": list(self.top_contributions)},
            "warnings": list(self.warnings),
            "model": self.model.to_dict(),
        }
