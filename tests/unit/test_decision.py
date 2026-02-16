"""Unit tests for the decision module."""

import json
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from srdedupe.decision import (
    CalibrationPair,
    ConfusionMatrix,
    Decision,
    DecisionSummary,
    ReasonCode,
    Thresholds,
    calibrate_conformal_threshold,
    calibrate_np_threshold,
    check_safety_gates,
    compute_final_threshold,
    load_calibration_set,
    write_conformal_calibration_report,
    write_decision_summary,
)
from srdedupe.decision.conformal_calibration import ConformalCalibration
from srdedupe.decision.models import categorize_forced_reason
from srdedupe.decision.policy import make_decision
from srdedupe.models import CanonicalRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_separated_pairs(
    n_dup: int = 100,
    n_neg: int = 100,
    dup_score: float = 0.95,
    neg_score: float = 0.05,
) -> list[CalibrationPair]:
    """Generate well-separated calibration pairs."""
    pairs: list[CalibrationPair] = []
    for i in range(n_dup):
        pairs.append(CalibrationPair(f"dup_{i}", dup_score + i / 100_000, True))
    for i in range(n_neg):
        pairs.append(CalibrationPair(f"neg_{i}", neg_score + i / 100_000, False))
    return pairs


def _make_conformal(
    t_high: float = 0.95,
    feasible: bool = True,
    **overrides: Any,
) -> ConformalCalibration:
    """Build ConformalCalibration with sensible defaults."""
    defaults: dict[str, Any] = {
        "method": "scrc_i",
        "alpha": 0.001,
        "delta": 0.05,
        "n_calib": 200,
        "score_field": "p_match",
        "t_high_conformal": t_high,
        "xi_hat": 0.5,
        "xi_lcb": 0.48,
        "feasible": feasible,
        "n_thresholds_checked": 50,
        "confusion_matrix": ConfusionMatrix(tp=50, fp=0, tn=100, fn=50),
    }
    defaults.update(overrides)
    return ConformalCalibration(**defaults)


# ---------------------------------------------------------------------------
# NP Calibration — behavioral contracts
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_np_perfect_separation_satisfies_fpr() -> None:
    """Separated data yields a valid threshold with FPR <= alpha."""
    pairs = _make_separated_pairs(100, 100)
    t_high, cal = calibrate_np_threshold(pairs, 0.001, "test")

    assert cal.estimated_fpr <= 0.001
    assert t_high < float("inf")


@pytest.mark.unit
def test_np_overlapping_data_returns_infinity() -> None:
    """Heavily overlapping scores with strict alpha yield no safe threshold."""
    pairs = [CalibrationPair(f"p{i}", 0.5 + (i % 50) / 100, i % 3 == 0) for i in range(200)]
    t_high, _ = calibrate_np_threshold(pairs, 0.001, "test")

    assert t_high == float("inf")


@pytest.mark.unit
def test_np_all_positives_yields_fpr_zero() -> None:
    """When all pairs are duplicates, FPR is trivially zero."""
    pairs = [CalibrationPair(f"p{i}", 0.5 + i / 1000, True) for i in range(200)]
    t_high, cal = calibrate_np_threshold(pairs, 0.001, "test")

    assert cal.estimated_fpr == 0.0
    assert t_high < float("inf")


@pytest.mark.unit
def test_np_empty_set_raises() -> None:
    """Empty calibration set raises ValueError."""
    with pytest.raises(ValueError, match="empty"):
        calibrate_np_threshold([], 0.001, "test", min_calibration_pairs=10)


@pytest.mark.unit
def test_np_too_small_set_raises() -> None:
    """Calibration set below minimum size raises ValueError."""
    pairs = _make_separated_pairs(1, 1)
    with pytest.raises(ValueError, match="too small"):
        calibrate_np_threshold(pairs, 0.001, "test", min_calibration_pairs=200)


# ---------------------------------------------------------------------------
# Safety Gates — parametrized
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kwargs_a", "kwargs_b", "warnings", "expected_reason"),
    [
        pytest.param(
            {"doi_norm": "10.1/a"},
            {"doi_norm": "10.2/b"},
            [],
            ReasonCode.FORCED_REVIEW_CONFLICTING_DOI,
            id="conflicting_doi",
        ),
        pytest.param(
            {"pmid_norm": "111"},
            {"pmid_norm": "222"},
            [],
            ReasonCode.FORCED_REVIEW_CONFLICTING_PMID,
            id="conflicting_pmid",
        ),
        pytest.param(
            {"title_truncated": True},
            {},
            ["title_truncated"],
            ReasonCode.FORCED_REVIEW_TITLE_TRUNCATED,
            id="title_truncated_no_strong_id",
        ),
        pytest.param(
            {},
            {"pages_unreliable": True},
            ["pages_unreliable"],
            ReasonCode.FORCED_REVIEW_PAGES_UNRELIABLE,
            id="pages_unreliable_no_strong_id",
        ),
        pytest.param(
            {"is_erratum_notice": True},
            {},
            [],
            ReasonCode.FORCED_REVIEW_ERRATUM_NOTICE,
            id="erratum",
        ),
        pytest.param(
            {},
            {"is_retraction_notice": True},
            [],
            ReasonCode.FORCED_REVIEW_RETRACTION_NOTICE,
            id="retraction",
        ),
    ],
)
def test_safety_gate_triggers(
    make_record: Callable[..., CanonicalRecord],
    kwargs_a: dict[str, Any],
    kwargs_b: dict[str, Any],
    warnings: list[str],
    expected_reason: ReasonCode,
) -> None:
    """Each safety gate fires for its specific condition."""
    rec_a = make_record("a", **kwargs_a)
    rec_b = make_record("b", **kwargs_b)
    reasons = check_safety_gates(rec_a, rec_b, warnings)

    assert expected_reason in reasons


@pytest.mark.unit
def test_matching_doi_does_not_trigger_conflict(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Matching DOI must not produce a conflicting-DOI reason."""
    rec_a = make_record("a", doi_norm="10.1/same")
    rec_b = make_record("b", doi_norm="10.1/same")

    assert ReasonCode.FORCED_REVIEW_CONFLICTING_DOI not in check_safety_gates(rec_a, rec_b, [])


@pytest.mark.unit
def test_strong_id_suppresses_data_quality_gate(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Matching DOI suppresses title_truncated data-quality gate."""
    rec_a = make_record("a", doi_norm="10.1/x", title_truncated=True)
    rec_b = make_record("b", doi_norm="10.1/x")
    reasons = check_safety_gates(rec_a, rec_b, ["title_truncated"])

    assert ReasonCode.FORCED_REVIEW_TITLE_TRUNCATED not in reasons


# ---------------------------------------------------------------------------
# make_decision — parametrized three zones + forced review
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("score", "expected"),
    [
        pytest.param(0.99, Decision.AUTO_DUP, id="above_t_high"),
        pytest.param(0.95, Decision.AUTO_DUP, id="at_t_high_boundary"),
        pytest.param(0.50, Decision.REVIEW, id="between_thresholds"),
        pytest.param(0.20, Decision.REVIEW, id="at_t_low_boundary"),
        pytest.param(0.10, Decision.AUTO_KEEP, id="below_t_low"),
    ],
)
def test_decision_zones(score: float, expected: Decision) -> None:
    """Score maps to the correct decision zone."""
    thresholds = Thresholds(t_high=0.95, t_low=0.20)
    decision, _ = make_decision(score, thresholds, [])

    assert decision == expected


@pytest.mark.unit
def test_forced_review_overrides_high_score() -> None:
    """Forced reasons override even a high-confidence score."""
    thresholds = Thresholds(t_high=0.95, t_low=0.20)
    decision, reasons = make_decision(0.99, thresholds, [ReasonCode.FORCED_REVIEW_CONFLICTING_DOI])

    assert decision == Decision.REVIEW
    assert reasons[0]["code"] == ReasonCode.FORCED_REVIEW_CONFLICTING_DOI.value


# ---------------------------------------------------------------------------
# categorize_forced_reason
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        (ReasonCode.FORCED_REVIEW_CONFLICTING_DOI, "conflicting_ids"),
        (ReasonCode.FORCED_REVIEW_ERRATUM_NOTICE, "special_records"),
        (ReasonCode.FORCED_REVIEW_TITLE_TRUNCATED, "data_quality"),
        (ReasonCode.P_ABOVE_T_HIGH, None),
    ],
)
def test_categorize_forced_reason(reason: ReasonCode, expected: str | None) -> None:
    """Reason codes map to the correct forced-review category."""
    assert categorize_forced_reason(reason) == expected


# ---------------------------------------------------------------------------
# Conformal Calibration — behavioral contracts
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_conformal_separated_data_is_feasible() -> None:
    """Well-separated data yields a feasible threshold with controlled FP."""
    pairs = _make_separated_pairs(250, 250)
    result = calibrate_conformal_threshold(pairs, alpha=0.01, delta=0.05)

    assert result.feasible
    assert result.t_high_conformal < float("inf")
    assert result.confusion_matrix.fp <= 10


@pytest.mark.unit
def test_conformal_strict_params_yield_infeasible() -> None:
    """Overlapping scores with very strict params produce no feasible threshold."""
    pairs = [CalibrationPair(f"p{i}", 0.7 + i / 200, i % 2 == 0) for i in range(50)]
    result = calibrate_conformal_threshold(pairs, alpha=0.0001, delta=0.01)

    assert not result.feasible
    assert result.t_high_conformal == float("inf")


@pytest.mark.unit
def test_conformal_deterministic_regardless_of_order() -> None:
    """Shuffling input order does not change the result."""
    pairs = _make_separated_pairs(50, 50, dup_score=0.90, neg_score=0.10)

    r1 = calibrate_conformal_threshold(pairs.copy(), 0.01, 0.05)
    shuffled = pairs.copy()
    random.seed(42)
    random.shuffle(shuffled)
    r2 = calibrate_conformal_threshold(shuffled, 0.01, 0.05)

    assert r1.t_high_conformal == r2.t_high_conformal
    assert r1.feasible == r2.feasible


@pytest.mark.unit
@pytest.mark.parametrize(
    ("alpha", "delta", "match"),
    [
        (0.0, 0.05, "alpha"),
        (1.0, 0.05, "alpha"),
        (0.01, 0.0, "delta"),
        (0.01, 1.0, "delta"),
    ],
)
def test_conformal_invalid_params_raise(alpha: float, delta: float, match: str) -> None:
    """Out-of-range alpha or delta raises ValueError."""
    pairs = [CalibrationPair("p1", 0.5, True)]
    with pytest.raises(ValueError, match=match):
        calibrate_conformal_threshold(pairs, alpha, delta)


@pytest.mark.unit
def test_conformal_empty_set_raises() -> None:
    """Empty calibration set raises ValueError."""
    with pytest.raises(ValueError, match="empty"):
        calibrate_conformal_threshold([], 0.01, 0.05)


# ---------------------------------------------------------------------------
# compute_final_threshold — parametrized
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("t_np", "t_conf", "feasible", "expected_final"),
    [
        pytest.param(0.90, 0.95, True, 0.95, id="conformal_stricter"),
        pytest.param(0.98, 0.90, True, 0.98, id="np_stricter"),
        pytest.param(0.90, float("inf"), False, float("inf"), id="infeasible"),
    ],
)
def test_final_threshold_with_conformal(
    t_np: float, t_conf: float, feasible: bool, expected_final: float
) -> None:
    """Final threshold is max(NP, conformal) when conformal is active."""
    conformal = _make_conformal(t_high=t_conf, feasible=feasible)
    final, _, _ = compute_final_threshold(t_np, conformal)

    assert final == expected_final


@pytest.mark.unit
def test_final_threshold_without_conformal() -> None:
    """Without conformal, final threshold equals NP threshold."""
    final, _, t_conf = compute_final_threshold(0.92, None)

    assert final == 0.92
    assert t_conf is None


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decision_summary_roundtrip(tmp_path: Path) -> None:
    """DecisionSummary serializes to JSON with expected structure."""
    summary = DecisionSummary(
        pairs_in=100,
        auto_dup=30,
        review=40,
        auto_keep=30,
        forced_review_conflicting_ids=5,
        forced_review_special_records=3,
        forced_review_data_quality=2,
        estimated_fpr_at_t_high=0.001,
        alpha=0.001,
        thresholds=Thresholds(t_high=0.95, t_low=0.20),
    )
    out = tmp_path / "summary.json"
    write_decision_summary(summary, out)

    data = json.loads(out.read_text())
    assert data["pairs_in"] == 100
    assert data["decisions"]["auto_dup"] == 30
    assert data["forced_review"]["data_quality"] == 2


@pytest.mark.unit
def test_conformal_report_roundtrip(tmp_path: Path) -> None:
    """ConformalCalibration serializes to JSON with expected structure."""
    conformal = _make_conformal()
    out = tmp_path / "reports" / "conformal.json"
    write_conformal_calibration_report(conformal, out)

    data = json.loads(out.read_text())
    assert data["method"] == "scrc_i"
    assert isinstance(data["confusion_matrix"], dict)


# ---------------------------------------------------------------------------
# load_calibration_set
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_calibration_set_from_fixture() -> None:
    """Calibration pairs load correctly from JSONL fixture."""
    fixture = Path(__file__).parent.parent / "fixtures" / "decision" / "calibration_sample.jsonl"
    if not fixture.exists():
        pytest.skip("Calibration fixture not available")

    pairs = load_calibration_set(str(fixture))
    assert len(pairs) == 10
    assert all(isinstance(p, CalibrationPair) for p in pairs)


@pytest.mark.unit
def test_load_calibration_set_missing_file_raises() -> None:
    """Missing calibration file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_calibration_set("nonexistent.jsonl")
