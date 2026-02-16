"""Neyman-Pearson calibration for FPR control.

This module implements empirical Neyman-Pearson threshold calibration,
selecting the most permissive threshold such that FPR(threshold) <= alpha.
"""

import json
from pathlib import Path

from srdedupe.decision.models import CalibrationPair, ConfusionMatrix, NPCalibration


def calibrate_np_threshold(
    calibration_pairs: list[CalibrationPair],
    alpha: float,
    calibration_set_id: str,
    min_calibration_pairs: int = 200,
) -> tuple[float, NPCalibration]:
    """Calibrate Neyman-Pearson threshold for FPR control.

    Selects the most permissive (lowest) threshold tau such that
    empirical FPR(tau) <= alpha. Sweeps from high to low score using
    O(n) running counters and groups tied scores.

    Parameters
    ----------
    calibration_pairs : list[CalibrationPair]
        Labeled calibration pairs with ground truth.
    alpha : float
        Target FPR constraint (e.g., 0.001).
    calibration_set_id : str
        Identifier for calibration set.
    min_calibration_pairs : int, optional
        Minimum required calibration pairs, by default 200.

    Returns
    -------
    tuple[float, NPCalibration]
        Calibrated threshold and calibration metadata.

    Raises
    ------
    ValueError
        If calibration set is too small or empty.
    """
    if not calibration_pairs:
        raise ValueError("Calibration set is empty")

    if len(calibration_pairs) < min_calibration_pairs:
        raise ValueError(
            f"Calibration set too small: {len(calibration_pairs)} pairs "
            f"(minimum required: {min_calibration_pairs})"
        )

    total_positives = sum(1 for p in calibration_pairs if p.is_duplicate)
    total_negatives = len(calibration_pairs) - total_positives

    # Edge case: no negatives — any threshold satisfies FPR = 0
    if total_negatives == 0:
        lowest = min(p.score for p in calibration_pairs)
        return lowest, _build_np_calibration(
            alpha=alpha,
            calibration_set_id=calibration_set_id,
            n=len(calibration_pairs),
            fpr=0.0,
            cm=ConfusionMatrix(tp=total_positives, fp=0, tn=0, fn=0),
        )

    # Sort descending by score
    sorted_pairs = sorted(calibration_pairs, key=lambda p: p.score, reverse=True)

    # Sweep from highest to lowest with running counters.
    # We group pairs with the same score so all ties are included at once.
    best_threshold = float("inf")
    best_fpr = 0.0
    best_tp = 0
    best_fp = 0

    running_tp = 0
    running_fp = 0
    i = 0
    n = len(sorted_pairs)

    while i < n:
        # Consume all pairs at the same score
        current_score = sorted_pairs[i].score
        while i < n and sorted_pairs[i].score == current_score:
            if sorted_pairs[i].is_duplicate:
                running_tp += 1
            else:
                running_fp += 1
            i += 1

        fpr = running_fp / total_negatives

        if fpr <= alpha:
            # This threshold is valid; keep it and continue to find
            # the most permissive (lowest score) that still satisfies
            best_threshold = current_score
            best_fpr = fpr
            best_tp = running_tp
            best_fp = running_fp
        else:
            # FPR exceeded alpha; stop (monotonically non-decreasing)
            break

    # Build result
    if best_threshold == float("inf"):
        # No safe threshold found — everything rejected
        cm = ConfusionMatrix(tp=0, fp=0, tn=total_negatives, fn=total_positives)
        return float("inf"), _build_np_calibration(
            alpha=alpha,
            calibration_set_id=calibration_set_id,
            n=n,
            fpr=0.0,
            cm=cm,
        )

    cm = ConfusionMatrix(
        tp=best_tp,
        fp=best_fp,
        tn=total_negatives - best_fp,
        fn=total_positives - best_tp,
    )
    return best_threshold, _build_np_calibration(
        alpha=alpha,
        calibration_set_id=calibration_set_id,
        n=n,
        fpr=best_fpr,
        cm=cm,
    )


def _build_np_calibration(
    *,
    alpha: float,
    calibration_set_id: str,
    n: int,
    fpr: float,
    cm: ConfusionMatrix,
) -> NPCalibration:
    return NPCalibration(
        alpha=alpha,
        calibration_set=calibration_set_id,
        method="neyman_pearson",
        calibration_size=n,
        estimated_fpr=fpr,
        confusion_matrix=cm,
    )


def load_calibration_set(calibration_path: str) -> list[CalibrationPair]:
    """Load calibration pairs from JSONL file.

    Parameters
    ----------
    calibration_path : str
        Path to calibration JSONL file.

    Returns
    -------
    list[CalibrationPair]
        List of calibration pairs with ground truth.

    Raises
    ------
    FileNotFoundError
        If calibration file not found.
    ValueError
        If calibration data is malformed.
    """
    path = Path(calibration_path)
    if not path.exists():
        raise FileNotFoundError(f"Calibration file not found: {calibration_path}")

    calibration_pairs: list[CalibrationPair] = []
    with path.open("r") as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                pair = CalibrationPair(
                    pair_id=data["pair_id"],
                    score=data["p_match"],
                    is_duplicate=data["is_duplicate"],
                )
                calibration_pairs.append(pair)
            except (KeyError, json.JSONDecodeError) as e:
                raise ValueError(f"Malformed calibration data at line {line_num}: {e}") from e

    return calibration_pairs
