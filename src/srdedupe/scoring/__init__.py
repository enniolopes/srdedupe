"""Pairwise scoring module using Fellegi-Sunter model.

This module implements the scoring layer that takes candidate pairs and produces
explainable probabilistic match scores using probabilistic record linkage.
"""

from srdedupe.scoring.comparators import FIELD_CONFIGS, FieldConfig
from srdedupe.scoring.fs_model import FSModel, load_model
from srdedupe.scoring.models import (
    ComparisonResult,
    FieldComparison,
    ModelInfo,
    PairScore,
    comparison_to_dict,
)
from srdedupe.scoring.score_pairs import score_all_pairs, score_pair

__all__ = [
    # Models
    "FieldComparison",
    "ComparisonResult",
    "comparison_to_dict",
    "PairScore",
    "ModelInfo",
    # Fellegi-Sunter
    "FSModel",
    "load_model",
    # Comparators
    "FieldConfig",
    "FIELD_CONFIGS",
    # Pipeline
    "score_pair",
    "score_all_pairs",
]
