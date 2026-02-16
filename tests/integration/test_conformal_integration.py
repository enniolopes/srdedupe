"""Integration tests for the decision pipeline."""

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from srdedupe.decision import (
    CalibrationPair,
    NPCalibration,
    Thresholds,
    calibrate_conformal_threshold,
    calibrate_np_threshold,
    compute_final_threshold,
    make_pair_decisions,
)
from srdedupe.models import CanonicalRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_pair_scores(path: Path, pairs: list[dict[str, object]]) -> None:
    """Write pair-score dicts to JSONL."""
    with path.open("w") as f:
        for p in pairs:
            f.write(json.dumps(p) + "\n")


def _score_row(
    pair_id: str,
    rid_a: str,
    rid_b: str,
    p_match: float,
) -> dict[str, object]:
    return {
        "pair_id": pair_id,
        "rid_a": rid_a,
        "rid_b": rid_b,
        "p_match": p_match,
        "warnings": [],
    }


def _quick_np_calibration() -> tuple[float, NPCalibration]:
    """Calibrate NP with a minimal well-separated set."""
    pairs = [CalibrationPair(f"p{i}", 0.9 if i < 50 else 0.1, i < 50) for i in range(100)]
    return calibrate_np_threshold(pairs, 0.01, "test", min_calibration_pairs=100)


# ---------------------------------------------------------------------------
# Pipeline tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pipeline_produces_correct_decision_counts(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """Three pairs at different scores yield one of each decision."""
    scores_path = tmp_path / "pair_scores.jsonl"
    _write_pair_scores(
        scores_path,
        [
            _score_row("a|b", "a", "b", 0.99),
            _score_row("c|d", "c", "d", 0.50),
            _score_row("e|f", "e", "f", 0.10),
        ],
    )
    records = [make_record(rid) for rid in "abcdef"]
    t_high, np_cal = _quick_np_calibration()
    thresholds = Thresholds(t_high=t_high, t_low=0.20)

    out = tmp_path / "decisions.jsonl"
    summary = make_pair_decisions(scores_path, records, thresholds, np_cal, out)

    assert summary.auto_dup == 1
    assert summary.review == 1
    assert summary.auto_keep == 1


@pytest.mark.integration
def test_pipeline_output_is_sorted_by_pair_id(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """Output JSONL is deterministically sorted by pair_id."""
    scores_path = tmp_path / "pair_scores.jsonl"
    _write_pair_scores(
        scores_path,
        [
            _score_row("z|z2", "z", "z2", 0.5),
            _score_row("a|a2", "a", "a2", 0.5),
            _score_row("m|m2", "m", "m2", 0.5),
        ],
    )
    records = [make_record(rid) for rid in ("a", "a2", "m", "m2", "z", "z2")]
    t_high, np_cal = _quick_np_calibration()
    thresholds = Thresholds(t_high=t_high, t_low=0.20)

    out = tmp_path / "decisions.jsonl"
    make_pair_decisions(scores_path, records, thresholds, np_cal, out)

    decisions = [json.loads(line) for line in out.read_text().splitlines()]
    pair_ids = [d["pair_id"] for d in decisions]
    assert pair_ids == sorted(pair_ids)


@pytest.mark.integration
def test_pipeline_with_conformal_includes_metadata(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """Pipeline with conformal calibration writes conformal metadata to output."""
    pairs = [
        CalibrationPair(
            f"p{i}",
            0.95 + i / 100_000 if i < 250 else 0.10 + i / 100_000,
            i < 250,
        )
        for i in range(500)
    ]
    t_np, np_cal = calibrate_np_threshold(pairs[:], 0.01, "test", min_calibration_pairs=100)
    conformal = calibrate_conformal_threshold(pairs[:], 0.01, 0.05)
    t_final, _, _ = compute_final_threshold(t_np, conformal)

    scores_path = tmp_path / "pair_scores.jsonl"
    _write_pair_scores(scores_path, [_score_row("a|b", "a", "b", 0.98)])
    records = [make_record("a"), make_record("b")]
    thresholds = Thresholds(t_high=t_final, t_low=0.20)

    out = tmp_path / "decisions.jsonl"
    make_pair_decisions(
        scores_path,
        records,
        thresholds,
        np_cal,
        out,
        conformal_calibration=conformal,
    )

    decisions = [json.loads(line) for line in out.read_text().splitlines()]
    assert decisions[0]["conformal"] is not None
    assert decisions[0]["conformal"]["method"] == "scrc_i"


@pytest.mark.integration
def test_pipeline_without_conformal_omits_metadata(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """Pipeline without conformal does not write conformal metadata."""
    t_high, np_cal = _quick_np_calibration()
    scores_path = tmp_path / "pair_scores.jsonl"
    _write_pair_scores(scores_path, [_score_row("a|b", "a", "b", 0.95)])
    records = [make_record("a"), make_record("b")]
    thresholds = Thresholds(t_high=t_high, t_low=0.20)

    out = tmp_path / "decisions.jsonl"
    make_pair_decisions(scores_path, records, thresholds, np_cal, out)

    decisions = [json.loads(line) for line in out.read_text().splitlines()]
    assert "conformal" not in decisions[0] or decisions[0]["conformal"] is None
