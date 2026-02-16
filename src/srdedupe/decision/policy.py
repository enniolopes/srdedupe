"""Main three-way decision policy engine.

This module implements the decision pipeline that processes pair scores
and produces decisions with safety gates and FPR control.
"""

import json
from pathlib import Path
from typing import Any

from srdedupe.audit.logger import AuditLogger
from srdedupe.decision.conformal_calibration import ConformalCalibration
from srdedupe.decision.models import (
    Decision,
    DecisionSummary,
    NPCalibration,
    PairDecision,
    ReasonCode,
    Thresholds,
    categorize_forced_reason,
)
from srdedupe.decision.safety_gates import check_safety_gates
from srdedupe.models import CanonicalRecord


def make_decision(
    p_match: float,
    thresholds: Thresholds,
    forced_reasons: list[ReasonCode],
) -> tuple[Decision, list[dict[str, str]]]:
    """Make decision for a single pair.

    Parameters
    ----------
    p_match : float
        Posterior probability of match.
    thresholds : Thresholds
        Decision thresholds.
    forced_reasons : list[ReasonCode]
        Reasons for forced REVIEW from safety gates.

    Returns
    -------
    tuple[Decision, list[dict[str, str]]]
        Decision and list of reason codes.
    """
    if forced_reasons:
        return Decision.REVIEW, [{"code": r.value} for r in forced_reasons]

    if p_match >= thresholds.t_high:
        return Decision.AUTO_DUP, [{"code": ReasonCode.P_ABOVE_T_HIGH.value}]

    if p_match < thresholds.t_low:
        return Decision.AUTO_KEEP, [{"code": ReasonCode.P_BELOW_T_LOW.value}]

    return Decision.REVIEW, [{"code": ReasonCode.P_BETWEEN_THRESHOLDS.value}]


def make_pair_decisions(
    pair_scores_path: Path,
    records: list[CanonicalRecord],
    thresholds: Thresholds,
    np_calibration: NPCalibration,
    output_path: Path,
    logger: AuditLogger | None = None,
    conformal_calibration: ConformalCalibration | None = None,
) -> DecisionSummary:
    """Process pair scores and produce decisions.

    Parameters
    ----------
    pair_scores_path : Path
        Path to pair_scores.jsonl input.
    records : list[CanonicalRecord]
        List of canonical records for safety gate checks.
    thresholds : Thresholds
        Decision thresholds.
    np_calibration : NPCalibration
        NP calibration metadata.
    output_path : Path
        Path to write pair_decisions.jsonl.
    logger : AuditLogger | None, optional
        Audit logger for events, by default None.
    conformal_calibration : ConformalCalibration | None, optional
        Conformal calibration metadata (if enabled), by default None.

    Returns
    -------
    DecisionSummary
        Summary statistics for decisions.

    Raises
    ------
    FileNotFoundError
        If pair_scores file not found.
    """
    if not pair_scores_path.exists():
        raise FileNotFoundError(f"Pair scores file not found: {pair_scores_path}")

    record_index = {record.rid: record for record in records}

    # Counters
    decision_counts: dict[Decision, int] = dict.fromkeys(Decision, 0)
    forced_review_counts: dict[str, int] = {
        "conflicting_ids": 0,
        "special_records": 0,
        "data_quality": 0,
    }
    pairs_in = 0

    # Conformal metadata for output (if enabled)
    conformal_metadata: dict[str, Any] | None = None
    if conformal_calibration is not None:
        conformal_metadata = conformal_calibration.to_dict()

    # Log stage start
    _log_stage_start(logger, thresholds, np_calibration, conformal_calibration)

    # Process pairs
    pair_decisions: list[PairDecision] = []

    with pair_scores_path.open("r") as f:
        for line in f:
            data = json.loads(line)
            pairs_in += 1

            rid_a = data["rid_a"]
            rid_b = data["rid_b"]
            record_a = record_index.get(rid_a)
            record_b = record_index.get(rid_b)

            # Safety gates
            forced_reasons: list[ReasonCode] = []
            if record_a and record_b:
                forced_reasons = check_safety_gates(record_a, record_b, data.get("warnings", []))

            # Count forced reasons by category
            for reason in forced_reasons:
                category = categorize_forced_reason(reason)
                if category and category in forced_review_counts:
                    forced_review_counts[category] += 1

            # Decide
            p_match = data["p_match"]
            decision, reasons = make_decision(p_match, thresholds, forced_reasons)
            decision_counts[decision] += 1

            pair_decisions.append(
                PairDecision(
                    pair_id=data["pair_id"],
                    rid_a=rid_a,
                    rid_b=rid_b,
                    p_match=p_match,
                    decision=decision,
                    thresholds=thresholds,
                    np=np_calibration,
                    reasons=reasons,
                    warnings=data.get("warnings", []),
                    conformal=conformal_metadata,
                )
            )

    # Deterministic output
    pair_decisions.sort(key=lambda pd: pd.pair_id)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        for pd in pair_decisions:
            json.dump(pd.to_dict(), f, sort_keys=True)
            f.write("\n")

    # Log events
    _log_stage_finish(
        logger, pairs_in, decision_counts, np_calibration, conformal_calibration, output_path
    )

    return DecisionSummary(
        pairs_in=pairs_in,
        auto_dup=decision_counts[Decision.AUTO_DUP],
        review=decision_counts[Decision.REVIEW],
        auto_keep=decision_counts[Decision.AUTO_KEEP],
        forced_review_conflicting_ids=forced_review_counts["conflicting_ids"],
        forced_review_special_records=forced_review_counts["special_records"],
        forced_review_data_quality=forced_review_counts["data_quality"],
        estimated_fpr_at_t_high=np_calibration.estimated_fpr,
        alpha=np_calibration.alpha,
        thresholds=thresholds,
    )


def _log_stage_start(
    logger: AuditLogger | None,
    thresholds: Thresholds,
    np_calibration: NPCalibration,
    conformal_calibration: ConformalCalibration | None,
) -> None:
    if not logger:
        return

    stage_data: dict[str, Any] = {
        "stage": "decision_np",
        "thresholds": thresholds.to_dict(),
        "np_calibration": np_calibration.to_dict(),
    }
    if conformal_calibration is not None:
        stage_data["conformal_calibration"] = conformal_calibration.to_dict()

    logger.event("stage_started", data=stage_data)


def _log_stage_finish(
    logger: AuditLogger | None,
    pairs_in: int,
    decision_counts: dict[Decision, int],
    np_calibration: NPCalibration,
    conformal_calibration: ConformalCalibration | None,
    output_path: Path,
) -> None:
    if not logger:
        return

    logger.event(
        "artifact_written",
        data={
            "artifact": "pair_decisions.jsonl",
            "path": str(output_path),
            "pairs_count": pairs_in,
        },
    )

    calibration_data: dict[str, Any] = {
        "alpha": np_calibration.alpha,
        "calibration_size": np_calibration.calibration_size,
        "estimated_fpr": np_calibration.estimated_fpr,
    }
    if conformal_calibration is not None:
        calibration_data["conformal_enabled"] = True
        calibration_data["t_high_conformal"] = conformal_calibration.t_high_conformal
        calibration_data["conformal_feasible"] = conformal_calibration.feasible

    logger.event("np_calibration_completed", data=calibration_data)

    if conformal_calibration is not None:
        logger.event("conformal_calibration_completed", data=conformal_calibration.to_dict())

    counters = {
        "pairs_in": pairs_in,
        "auto_dup": decision_counts[Decision.AUTO_DUP],
        "review": decision_counts[Decision.REVIEW],
        "auto_keep": decision_counts[Decision.AUTO_KEEP],
    }
    logger.event("stage_finished", data={"stage": "decision_np", "counters": counters})


def write_decision_summary(summary: DecisionSummary, output_path: Path) -> None:
    """Write decision summary to JSON file.

    Parameters
    ----------
    summary : DecisionSummary
        Decision summary statistics.
    output_path : Path
        Path to write decision_summary.json.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(summary.to_dict(), f, indent=2, sort_keys=True)


def write_conformal_calibration_report(
    conformal_calibration: ConformalCalibration,
    output_path: Path,
) -> None:
    """Write conformal calibration report to JSON file.

    Parameters
    ----------
    conformal_calibration : ConformalCalibration
        Conformal calibration results.
    output_path : Path
        Path to write conformal_decision_calibration.json.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(conformal_calibration.to_dict(), f, indent=2, sort_keys=True)


def compute_final_threshold(
    t_high_np: float,
    conformal_calibration: ConformalCalibration | None,
) -> tuple[float, float | None, float | None]:
    """Compute final t_high from NP and conformal thresholds.

    The final threshold is max(t_high_np, t_high_conformal) when conformal
    is enabled, ensuring conformal can only make AUTO-DUP stricter.

    Parameters
    ----------
    t_high_np : float
        NP-calibrated threshold.
    conformal_calibration : ConformalCalibration | None
        Conformal calibration results (if enabled).

    Returns
    -------
    tuple[float, float | None, float | None]
        Tuple of (t_high_final, t_high_np, t_high_conformal).
        If conformal disabled, returns (t_high_np, t_high_np, None).
    """
    if conformal_calibration is not None:
        t_high_conformal = conformal_calibration.t_high_conformal
        t_high_final = max(t_high_np, t_high_conformal)
        return t_high_final, t_high_np, t_high_conformal

    return t_high_np, t_high_np, None
