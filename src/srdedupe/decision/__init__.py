"""Three-way decision policy with FPR control and conformal risk control.

This module implements the decision layer that converts pairwise scores into
AUTO-DUP, REVIEW, or AUTO-KEEP decisions with formal FPR control.
"""

from srdedupe.decision.conformal_calibration import (
    ConformalCalibration,
    calibrate_conformal_threshold,
)
from srdedupe.decision.models import (
    CalibrationPair,
    ConfusionMatrix,
    Decision,
    DecisionSummary,
    NPCalibration,
    PairDecision,
    ReasonCode,
    Thresholds,
    categorize_forced_reason,
)
from srdedupe.decision.np_calibration import (
    calibrate_np_threshold,
    load_calibration_set,
)
from srdedupe.decision.policy import (
    compute_final_threshold,
    make_pair_decisions,
    write_conformal_calibration_report,
    write_decision_summary,
)
from srdedupe.decision.safety_gates import check_safety_gates

__all__ = [
    "CalibrationPair",
    "ConformalCalibration",
    "ConfusionMatrix",
    "Decision",
    "DecisionSummary",
    "NPCalibration",
    "PairDecision",
    "ReasonCode",
    "Thresholds",
    "calibrate_conformal_threshold",
    "calibrate_np_threshold",
    "categorize_forced_reason",
    "check_safety_gates",
    "compute_final_threshold",
    "load_calibration_set",
    "make_pair_decisions",
    "write_conformal_calibration_report",
    "write_decision_summary",
]
