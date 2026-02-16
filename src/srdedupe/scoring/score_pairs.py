"""Main pairwise scoring pipeline.

This module implements the scoring pipeline that:
1. Reads candidate pairs from candidates.jsonl
2. Loads canonical records
3. Compares each pair using field comparators
4. Computes Fellegi-Sunter scores
5. Writes pair_scores.jsonl with explainability
"""

import json
from pathlib import Path
from typing import Any

from srdedupe.audit.logger import AuditLogger
from srdedupe.models import CanonicalRecord
from srdedupe.scoring.comparators import FIELD_CONFIGS
from srdedupe.scoring.fs_model import FSModel
from srdedupe.scoring.models import (
    ComparisonResult,
    FieldComparison,
    ModelInfo,
    PairScore,
)

# ---------------------------------------------------------------------------
# Bucket calculation
# ---------------------------------------------------------------------------

_BUCKET_THRESHOLDS = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
_BUCKET_LABELS = (
    "0.0-0.1",
    "0.1-0.2",
    "0.2-0.3",
    "0.3-0.4",
    "0.4-0.5",
    "0.5-0.6",
    "0.6-0.7",
    "0.7-0.8",
    "0.8-0.9",
    "0.9-1.0",
)


def get_p_match_bucket(p_match: float) -> str:
    """Get bucket label for a p_match value.

    Parameters
    ----------
    p_match : float
        Probability of match (0.0-1.0).

    Returns
    -------
    str
        Bucket label (e.g., '0.5-0.6').
    """
    for i, threshold in enumerate(_BUCKET_THRESHOLDS):
        if p_match < threshold:
            return _BUCKET_LABELS[i]
    return _BUCKET_LABELS[-1]


# ---------------------------------------------------------------------------
# Core scoring
# ---------------------------------------------------------------------------


def score_pair(
    record_a: CanonicalRecord,
    record_b: CanonicalRecord,
    model: FSModel,
    candidate_sources: list[dict[str, str]],
) -> PairScore:
    """Score a single candidate pair.

    Parameters
    ----------
    record_a : CanonicalRecord
        First record.
    record_b : CanonicalRecord
        Second record.
    model : FSModel
        Fellegi-Sunter model.
    candidate_sources : list[dict[str, str]]
        Blocking sources that generated this pair.

    Returns
    -------
    PairScore
        Complete pair score with comparisons and explainability.
    """
    warnings: list[str] = []
    field_levels: dict[str, str] = {}
    field_weights: dict[str, tuple[str, float]] = {}
    comparison: ComparisonResult = {}

    # Compare all fields using registry
    for config in FIELD_CONFIGS:
        level, sim, field_warnings = config.compare(record_a, record_b)
        warnings.extend(field_warnings)

        weight = model.get_weight(config.name, level)
        field_levels[config.name] = level
        field_weights[config.name] = (level, weight)

        # Build comparison entry with rounded values
        comparison[config.name] = FieldComparison(
            level=level,
            sim=model.round_value(sim) if sim is not None else None,
            weight=model.round_value(weight),
        )

    # Compute scores
    llr = model.compute_llr(field_levels)
    p_match = model.compute_p_match(llr)

    return PairScore(
        pair_id=f"{record_a.rid}|{record_b.rid}",
        rid_a=record_a.rid,
        rid_b=record_b.rid,
        candidate_sources=tuple(candidate_sources),
        comparison=comparison,
        llr=model.round_value(llr),
        p_match=model.round_value(p_match),
        top_contributions=model.get_top_contributions(field_weights),
        warnings=tuple(dict.fromkeys(warnings)),  # Deduplicate, preserve order
        model=ModelInfo(name=model.name, version=model.version),
    )


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def load_candidates(candidates_path: Path) -> list[dict[str, Any]]:
    """Load candidate pairs from JSONL file.

    Parameters
    ----------
    candidates_path : Path
        Path to candidates.jsonl file.

    Returns
    -------
    list[dict[str, Any]]
        List of candidate pair dictionaries.
    """
    with candidates_path.open("r") as f:
        return [json.loads(line) for line in f]


def build_record_index(records: list[CanonicalRecord]) -> dict[str, CanonicalRecord]:
    """Build index of records by RID for fast lookup.

    Parameters
    ----------
    records : list[CanonicalRecord]
        List of canonical records.

    Returns
    -------
    dict[str, CanonicalRecord]
        Dictionary mapping RID to record.
    """
    return {record.rid: record for record in records}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

# Warning counter mapping
_WARNING_COUNTERS = {
    "title_truncated": "title_truncated_pairs",
    "pages_unreliable": "pages_unreliable_pairs",
    "both_present_id_conflicts": "both_present_id_conflicts",
}


def score_all_pairs(
    candidates_path: Path,
    records: list[CanonicalRecord],
    output_path: Path,
    model: FSModel,
    logger: AuditLogger | None = None,
) -> dict[str, Any]:
    """Score all candidate pairs and write results.

    Parameters
    ----------
    candidates_path : Path
        Path to candidates.jsonl file.
    records : list[CanonicalRecord]
        List of canonical records (post-Stage 1 normalization).
    output_path : Path
        Path to write pair_scores.jsonl.
    model : FSModel
        Fellegi-Sunter model.
    logger : AuditLogger | None, optional
        Audit logger for events, by default None.

    Returns
    -------
    dict[str, Any]
        Statistics dictionary with counters.

    Raises
    ------
    FileNotFoundError
        If candidates file not found.
    """
    # Initialize stats
    stats: dict[str, Any] = {
        "pairs_in": 0,
        "pairs_scored": 0,
        "pairs_skipped_missing_records": 0,
        "p_match_buckets": dict.fromkeys(_BUCKET_LABELS, 0),
        "warnings": dict.fromkeys(_WARNING_COUNTERS.values(), 0),
    }

    if logger:
        logger.event(
            "stage_started",
            data={
                "stage": "pairwise_scoring",
                "model": {"name": model.name, "version": model.version},
            },
        )

    record_index = build_record_index(records)
    candidates = load_candidates(candidates_path)
    stats["pairs_in"] = len(candidates)

    pair_scores: list[PairScore] = []

    for candidate in candidates:
        record_a = record_index.get(candidate["rid_a"])
        record_b = record_index.get(candidate["rid_b"])

        if not record_a or not record_b:
            stats["pairs_skipped_missing_records"] += 1
            continue

        pair_score = score_pair(record_a, record_b, model, candidate["sources"])
        pair_scores.append(pair_score)
        stats["pairs_scored"] += 1

        # Update warning counters
        for warning in pair_score.warnings:
            counter = _WARNING_COUNTERS.get(warning)
            if counter:
                stats["warnings"][counter] += 1

        # Update bucket
        stats["p_match_buckets"][get_p_match_bucket(pair_score.p_match)] += 1

    # Sort for determinism
    pair_scores.sort(key=lambda ps: ps.pair_id)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        for pair_score in pair_scores:
            json.dump(pair_score.to_dict(), f, sort_keys=True)
            f.write("\n")

    if logger:
        logger.event(
            "artifact_written",
            data={
                "artifact": "pair_scores.jsonl",
                "path": str(output_path),
                "pairs_count": stats["pairs_scored"],
            },
        )
        logger.event(
            "stage_finished",
            data={"stage": "pairwise_scoring", "stats": stats},
        )

    return stats
