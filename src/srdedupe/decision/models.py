"""Data models for three-way decision policy.

This module defines the schema for decision results, thresholds,
calibration metadata, and confusion matrices.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Decision(StrEnum):
    """Three-way decision outcomes.

    Attributes
    ----------
    AUTO_DUP : str
        Automatic duplicate (high confidence).
    REVIEW : str
        Manual review required (medium confidence or safety concerns).
    AUTO_KEEP : str
        Automatic keep (low confidence).
    """

    AUTO_DUP = "AUTO_DUP"
    REVIEW = "REVIEW"
    AUTO_KEEP = "AUTO_KEEP"


class ReasonCode(StrEnum):
    """Reason codes for decision outcomes.

    Attributes
    ----------
    P_ABOVE_T_HIGH : str
        p_match >= t_high.
    P_BETWEEN_THRESHOLDS : str
        t_low <= p_match < t_high.
    P_BELOW_T_LOW : str
        p_match < t_low.
    FORCED_REVIEW_CONFLICTING_DOI : str
        Different DOI values present.
    FORCED_REVIEW_CONFLICTING_PMID : str
        Different PMID values present.
    FORCED_REVIEW_TITLE_TRUNCATED : str
        Title truncated without strong ID match.
    FORCED_REVIEW_PAGES_UNRELIABLE : str
        Pages unreliable without strong ID match.
    FORCED_REVIEW_ERRATUM_NOTICE : str
        Record is erratum notice.
    FORCED_REVIEW_RETRACTION_NOTICE : str
        Record is retraction notice.
    FORCED_REVIEW_CORRECTED_REPUBLISHED : str
        Record is corrected/republished.
    FORCED_REVIEW_LINKED_CITATION : str
        Record has linked citation.
    """

    P_ABOVE_T_HIGH = "p_above_t_high"
    P_BETWEEN_THRESHOLDS = "p_between_thresholds"
    P_BELOW_T_LOW = "p_below_t_low"
    FORCED_REVIEW_CONFLICTING_DOI = "forced_review_conflicting_doi"
    FORCED_REVIEW_CONFLICTING_PMID = "forced_review_conflicting_pmid"
    FORCED_REVIEW_TITLE_TRUNCATED = "forced_review_title_truncated"
    FORCED_REVIEW_PAGES_UNRELIABLE = "forced_review_pages_unreliable"
    FORCED_REVIEW_ERRATUM_NOTICE = "forced_review_erratum_notice"
    FORCED_REVIEW_RETRACTION_NOTICE = "forced_review_retraction_notice"
    FORCED_REVIEW_CORRECTED_REPUBLISHED = "forced_review_corrected_republished"
    FORCED_REVIEW_LINKED_CITATION = "forced_review_linked_citation"


# Single source of truth for forced-review reason categorization
_CONFLICTING_ID_REASONS: frozenset[ReasonCode] = frozenset(
    {
        ReasonCode.FORCED_REVIEW_CONFLICTING_DOI,
        ReasonCode.FORCED_REVIEW_CONFLICTING_PMID,
    }
)

_SPECIAL_RECORD_REASONS: frozenset[ReasonCode] = frozenset(
    {
        ReasonCode.FORCED_REVIEW_ERRATUM_NOTICE,
        ReasonCode.FORCED_REVIEW_RETRACTION_NOTICE,
        ReasonCode.FORCED_REVIEW_CORRECTED_REPUBLISHED,
        ReasonCode.FORCED_REVIEW_LINKED_CITATION,
    }
)

_DATA_QUALITY_REASONS: frozenset[ReasonCode] = frozenset(
    {
        ReasonCode.FORCED_REVIEW_TITLE_TRUNCATED,
        ReasonCode.FORCED_REVIEW_PAGES_UNRELIABLE,
    }
)


def categorize_forced_reason(reason: ReasonCode) -> str | None:
    """Return the forced-review category for a reason code.

    Parameters
    ----------
    reason : ReasonCode
        The reason code to categorize.

    Returns
    -------
    str | None
        Category name or None if not a forced-review reason.
    """
    if reason in _CONFLICTING_ID_REASONS:
        return "conflicting_ids"
    if reason in _SPECIAL_RECORD_REASONS:
        return "special_records"
    if reason in _DATA_QUALITY_REASONS:
        return "data_quality"
    return None


@dataclass(frozen=True)
class ConfusionMatrix:
    """Confusion matrix for calibration evaluation.

    Attributes
    ----------
    tp : int
        True positives.
    fp : int
        False positives.
    tn : int
        True negatives.
    fn : int
        False negatives.
    """

    tp: int
    fp: int
    tn: int
    fn: int

    def to_dict(self) -> dict[str, int]:
        """Convert to dictionary.

        Returns
        -------
        dict[str, int]
            Dictionary with tp, fp, tn, fn keys.
        """
        return {"tp": self.tp, "fp": self.fp, "tn": self.tn, "fn": self.fn}


@dataclass(frozen=True)
class Thresholds:
    """Decision thresholds.

    Attributes
    ----------
    t_high : float
        Upper threshold for AUTO-DUP.
    t_low : float
        Lower threshold for AUTO-KEEP.
    t_high_np : float | None
        NP-calibrated threshold (before conformal adjustment).
    t_high_conformal : float | None
        Conformal-calibrated threshold (if enabled).
    """

    t_high: float
    t_low: float
    t_high_np: float | None = None
    t_high_conformal: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        result: dict[str, Any] = {"t_high": self.t_high, "t_low": self.t_low}
        if self.t_high_np is not None:
            result["t_high_np"] = self.t_high_np
        if self.t_high_conformal is not None:
            result["t_high_conformal"] = self.t_high_conformal
        return result


@dataclass(frozen=True)
class NPCalibration:
    """Neyman-Pearson calibration metadata.

    Attributes
    ----------
    alpha : float
        Target FPR constraint (e.g., 0.001).
    calibration_set : str
        Calibration set identifier.
    method : str
        Calibration method (e.g., "neyman_pearson").
    calibration_size : int
        Number of calibration pairs used.
    estimated_fpr : float
        Estimated FPR at t_high.
    confusion_matrix : ConfusionMatrix
        Confusion matrix at calibrated threshold.
    """

    alpha: float
    calibration_set: str
    method: str
    calibration_size: int
    estimated_fpr: float
    confusion_matrix: ConfusionMatrix

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            "alpha": self.alpha,
            "calibration_set": self.calibration_set,
            "method": self.method,
            "calibration_size": self.calibration_size,
            "estimated_fpr": self.estimated_fpr,
            "confusion_matrix": self.confusion_matrix.to_dict(),
        }


@dataclass(frozen=True)
class CalibrationPair:
    """Calibration pair with ground truth.

    Attributes
    ----------
    pair_id : str
        Pair identifier.
    score : float
        Match score (p_match or llr).
    is_duplicate : bool
        Ground truth label (True = duplicate, False = non-duplicate).
    """

    pair_id: str
    score: float
    is_duplicate: bool


@dataclass(frozen=True)
class PairDecision:
    """Decision result for a single pair.

    Attributes
    ----------
    pair_id : str
        Pair identifier (format: "rid_a|rid_b").
    rid_a : str
        First record ID.
    rid_b : str
        Second record ID.
    p_match : float
        Posterior probability of match.
    decision : Decision
        Final decision (AUTO_DUP, REVIEW, AUTO_KEEP).
    thresholds : Thresholds
        Thresholds used for decision.
    np : NPCalibration
        Neyman-Pearson calibration metadata.
    reasons : list[dict[str, str]]
        List of reason codes explaining decision.
    warnings : list[str]
        Warnings from scoring stage.
    conformal : dict[str, Any] | None
        Conformal calibration metadata (if enabled).
    """

    pair_id: str
    rid_a: str
    rid_b: str
    p_match: float
    decision: Decision
    thresholds: Thresholds
    np: NPCalibration
    reasons: list[dict[str, str]]
    warnings: list[str]
    conformal: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        result: dict[str, Any] = {
            "pair_id": self.pair_id,
            "rid_a": self.rid_a,
            "rid_b": self.rid_b,
            "p_match": self.p_match,
            "decision": self.decision.value,
            "thresholds": self.thresholds.to_dict(),
            "np": self.np.to_dict(),
            "reasons": self.reasons,
            "warnings": self.warnings,
        }
        if self.conformal is not None:
            result["conformal"] = self.conformal
        return result


@dataclass(frozen=True)
class DecisionSummary:
    """Summary statistics for decision stage.

    Attributes
    ----------
    pairs_in : int
        Total pairs processed.
    auto_dup : int
        AUTO-DUP decisions.
    review : int
        REVIEW decisions.
    auto_keep : int
        AUTO-KEEP decisions.
    forced_review_conflicting_ids : int
        Forced REVIEW due to conflicting IDs.
    forced_review_special_records : int
        Forced REVIEW due to special record types.
    forced_review_data_quality : int
        Forced REVIEW due to data quality issues.
    estimated_fpr_at_t_high : float
        Estimated FPR at t_high threshold.
    alpha : float
        Target alpha for NP calibration.
    thresholds : Thresholds
        Thresholds used.
    """

    pairs_in: int
    auto_dup: int
    review: int
    auto_keep: int
    forced_review_conflicting_ids: int
    forced_review_special_records: int
    forced_review_data_quality: int
    estimated_fpr_at_t_high: float
    alpha: float
    thresholds: Thresholds

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            "pairs_in": self.pairs_in,
            "decisions": {
                "auto_dup": self.auto_dup,
                "review": self.review,
                "auto_keep": self.auto_keep,
            },
            "forced_review": {
                "conflicting_ids": self.forced_review_conflicting_ids,
                "special_records": self.forced_review_special_records,
                "data_quality": self.forced_review_data_quality,
            },
            "calibration": {
                "estimated_fpr_at_t_high": self.estimated_fpr_at_t_high,
                "alpha": self.alpha,
            },
            "thresholds": self.thresholds.to_dict(),
        }
