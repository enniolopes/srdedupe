"""Selective Conformal Risk Control (SCRC-I) for AUTO-DUP decisions.

This module implements a distribution-free risk control mechanism that provides
high-probability guarantees on the false-positive rate among AUTO-DUP pairs.
It follows the SCRC-I (inductive) variant with DKW-based concentration bounds.

References
----------
[1] Selective Conformal Risk Control: https://arxiv.org/pdf/2512.12844
[2] Conformal Risk Control: https://arxiv.org/pdf/2208.02814
"""

import math
from dataclasses import dataclass
from typing import Any

from srdedupe.decision.models import CalibrationPair, ConfusionMatrix


@dataclass(frozen=True)
class ConformalCalibration:
    """Conformal calibration results.

    Attributes
    ----------
    method : str
        Calibration method ("scrc_i" for SCRC-I).
    alpha : float
        Target selective FP risk (e.g., 0.001).
    delta : float
        Confidence level for high-probability guarantee (e.g., 0.05).
    n_calib : int
        Number of calibration pairs used.
    score_field : str
        Score field used ("p_match" or "llr").
    t_high_conformal : float
        Conformal-calibrated threshold.
    xi_hat : float
        Empirical acceptance rate at t_high_conformal.
    xi_lcb : float
        Conservative lower bound on acceptance rate (DKW-corrected).
    feasible : bool
        Whether a feasible threshold was found.
    n_thresholds_checked : int
        Number of candidate thresholds evaluated.
    confusion_matrix : ConfusionMatrix
        Confusion matrix at t_high_conformal.
    """

    method: str
    alpha: float
    delta: float
    n_calib: int
    score_field: str
    t_high_conformal: float
    xi_hat: float
    xi_lcb: float
    feasible: bool
    n_thresholds_checked: int
    confusion_matrix: ConfusionMatrix

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            "method": self.method,
            "alpha": self.alpha,
            "delta": self.delta,
            "n_calib": self.n_calib,
            "score_field": self.score_field,
            "t_high_conformal": self.t_high_conformal,
            "xi_hat": self.xi_hat,
            "xi_lcb": self.xi_lcb,
            "feasible": self.feasible,
            "n_thresholds_checked": self.n_thresholds_checked,
            "confusion_matrix": self.confusion_matrix.to_dict(),
        }


def calibrate_conformal_threshold(
    calibration_pairs: list[CalibrationPair],
    alpha: float,
    delta: float,
    score_field: str = "p_match",
) -> ConformalCalibration:
    """Calibrate conformal threshold using SCRC-I algorithm.

    Implements the SCRC-I (inductive) variant with DKW concentration bound.
    Selects the smallest threshold (maximum coverage) such that:
    FP(t) <= ceil((n+1) * alpha * xi_lcb(t)) - 1

    Parameters
    ----------
    calibration_pairs : list[CalibrationPair]
        Labeled calibration pairs with scores and ground truth.
    alpha : float
        Target selective FP risk (e.g., 0.001).
    delta : float
        Confidence level for high-probability guarantee (e.g., 0.05).
    score_field : str, optional
        Score field name ("p_match" or "llr"), by default "p_match".

    Returns
    -------
    ConformalCalibration
        Conformal calibration results and metadata.

    Raises
    ------
    ValueError
        If calibration set is empty or parameters are invalid.
    """
    if not calibration_pairs:
        raise ValueError("Calibration set is empty")
    if not 0 < alpha < 1:
        raise ValueError(f"alpha must be in (0, 1), got {alpha}")
    if not 0 < delta < 1:
        raise ValueError(f"delta must be in (0, 1), got {delta}")

    n = len(calibration_pairs)
    total_positives = sum(1 for p in calibration_pairs if p.is_duplicate)
    total_negatives = n - total_positives

    # DKW half-width: eps(n, delta) = sqrt((1/(2n)) * log(2/delta))
    eps = math.sqrt((1 / (2 * n)) * math.log(2 / delta))

    # Sort descending by score
    sorted_pairs = sorted(calibration_pairs, key=lambda p: p.score, reverse=True)

    # Extract unique thresholds, sorted descending
    unique_scores = sorted({p.score for p in calibration_pairs}, reverse=True)

    # O(n) sweep: running counters for accepted pairs
    best_threshold = float("inf")
    best_xi_hat = 0.0
    best_xi_lcb = 0.0
    best_tp = 0
    best_fp = 0
    feasible_found = False

    running_tp = 0
    running_fp = 0
    pair_idx = 0

    for threshold in unique_scores:
        # Advance pointer to include all pairs with score >= threshold
        while pair_idx < n and sorted_pairs[pair_idx].score >= threshold:
            if sorted_pairs[pair_idx].is_duplicate:
                running_tp += 1
            else:
                running_fp += 1
            pair_idx += 1

        n_accepted = running_tp + running_fp
        xi_hat = n_accepted / n
        xi_lcb = max(xi_hat - eps, 0.0)

        # Feasibility: (n+1) * alpha * xi_lcb >= 1
        if (n + 1) * alpha * xi_lcb < 1.0:
            continue

        # Conformal-safe condition: FP <= ceil((n+1) * alpha * xi_lcb) - 1
        conformal_bound = math.ceil((n + 1) * alpha * xi_lcb) - 1

        if running_fp <= conformal_bound:
            best_threshold = threshold
            best_xi_hat = xi_hat
            best_xi_lcb = xi_lcb
            best_tp = running_tp
            best_fp = running_fp
            feasible_found = True
            # Continue to find smallest safe threshold (most permissive)

    if not feasible_found:
        cm = ConfusionMatrix(tp=0, fp=0, tn=total_negatives, fn=total_positives)
        return ConformalCalibration(
            method="scrc_i",
            alpha=alpha,
            delta=delta,
            n_calib=n,
            score_field=score_field,
            t_high_conformal=float("inf"),
            xi_hat=0.0,
            xi_lcb=0.0,
            feasible=False,
            n_thresholds_checked=len(unique_scores),
            confusion_matrix=cm,
        )

    cm = ConfusionMatrix(
        tp=best_tp,
        fp=best_fp,
        tn=total_negatives - best_fp,
        fn=total_positives - best_tp,
    )
    return ConformalCalibration(
        method="scrc_i",
        alpha=alpha,
        delta=delta,
        n_calib=n,
        score_field=score_field,
        t_high_conformal=best_threshold,
        xi_hat=best_xi_hat,
        xi_lcb=best_xi_lcb,
        feasible=feasible_found,
        n_thresholds_checked=len(unique_scores),
        confusion_matrix=cm,
    )
