"""Tests for pairwise scoring module."""

import json
import tempfile
from collections.abc import Callable
from pathlib import Path

import pytest

from srdedupe.models import CanonicalRecord
from srdedupe.scoring import (
    FSModel,
    score_all_pairs,
    score_pair,
)
from srdedupe.scoring.comparators import (
    compare_authors,
    compare_doi,
    compare_journal,
    compare_pages,
    compare_pmid,
    compare_title,
    compare_year,
    jaccard_similarity,
)
from srdedupe.scoring.fs_model import logit, sigmoid
from srdedupe.scoring.models import (
    FieldComparison,
    ModelInfo,
    comparison_to_dict,
)

# ========== Math helpers ==========


@pytest.mark.unit
def test_sigmoid() -> None:
    """Test sigmoid function at key points."""
    assert sigmoid(0.0) == 0.5
    assert sigmoid(100.0) > 0.999
    assert sigmoid(-100.0) < 0.001


@pytest.mark.unit
def test_logit() -> None:
    """Test logit function (sigmoid inverse)."""
    assert abs(logit(0.5)) < 1e-6
    assert logit(0.9) > 0
    assert logit(0.1) < 0


@pytest.mark.unit
def test_logit_raises_on_boundary() -> None:
    """Test logit rejects probabilities outside (0, 1)."""
    with pytest.raises(ValueError):
        logit(0.0)
    with pytest.raises(ValueError):
        logit(1.0)


# ========== Model loading ==========


@pytest.mark.unit
def test_load_model(fs_model: FSModel) -> None:
    """Test FS model loads with expected metadata."""
    assert fs_model.name == "fs_v1"
    assert fs_model.version == "1.0.0"
    assert fs_model.lambda_prior == 0.01
    assert fs_model.round_decimals == 6


@pytest.mark.unit
def test_model_weight_lookup(fs_model: FSModel) -> None:
    """Test weight lookup for known field levels."""
    assert fs_model.get_weight("doi", "exact") > 0
    assert fs_model.get_weight("doi", "both_present_mismatch") < 0
    assert fs_model.get_weight("doi", "missing") == 0.0


@pytest.mark.unit
def test_model_weight_raises_on_unknown(fs_model: FSModel) -> None:
    """Test weight lookup raises for unknown field or level."""
    with pytest.raises(KeyError):
        fs_model.get_weight("nonexistent", "exact")
    with pytest.raises(KeyError):
        fs_model.get_weight("doi", "nonexistent")


@pytest.mark.unit
def test_model_compute_llr_and_p_match(fs_model: FSModel) -> None:
    """Test LLR computation and posterior probability."""
    field_levels = {
        "doi": "exact",
        "pmid": "missing",
        "title": "exact",
        "authors": "strong",
        "year": "exact",
        "journal": "high",
        "pages": "exact",
    }
    llr = fs_model.compute_llr(field_levels)
    p_match = fs_model.compute_p_match(llr)

    # All-match scenario should yield very high p(match)
    assert llr > 0
    assert p_match > 0.999


@pytest.mark.unit
def test_model_get_top_contributions(fs_model: FSModel) -> None:
    """Test top contributions sorting by absolute weight."""
    field_comparisons = {
        "doi": ("exact", 6.9067),
        "title": ("missing", 0.0),
        "year": ("far", -4.9416),
    }
    top = fs_model.get_top_contributions(field_comparisons, top_k=2)

    assert len(top) == 2
    assert top[0]["field"] == "doi"
    assert top[1]["field"] == "year"


# ========== Comparators — DOI ==========


@pytest.mark.unit
@pytest.mark.parametrize(
    ("doi_a", "doi_b", "expected_level", "expected_warning"),
    [
        ("10.1234/test", "10.1234/test", "exact", None),
        ("10.1234/a", "10.1234/b", "both_present_mismatch", "both_present_id_conflicts"),
        (None, "10.1234/test", "missing", None),
        ("10.1234/test", None, "missing", None),
        (None, None, "missing", None),
    ],
    ids=["exact", "mismatch", "a_missing", "b_missing", "both_missing"],
)
def test_compare_doi(
    doi_a: str | None,
    doi_b: str | None,
    expected_level: str,
    expected_warning: str | None,
) -> None:
    """Test DOI comparison levels."""
    level, sim, warnings = compare_doi(doi_a, doi_b)
    assert level == expected_level
    assert sim is None
    if expected_warning:
        assert expected_warning in warnings


# ========== Comparators — PMID ==========


@pytest.mark.unit
@pytest.mark.parametrize(
    ("pmid_a", "pmid_b", "expected_level", "expected_warning"),
    [
        ("12345", "12345", "exact", None),
        ("12345", "67890", "both_present_mismatch", "both_present_id_conflicts"),
        (None, "12345", "missing", None),
        (None, None, "missing", None),
    ],
    ids=["exact", "mismatch", "missing", "both_missing"],
)
def test_compare_pmid(
    pmid_a: str | None,
    pmid_b: str | None,
    expected_level: str,
    expected_warning: str | None,
) -> None:
    """Test PMID comparison levels."""
    level, sim, warnings = compare_pmid(pmid_a, pmid_b)
    assert level == expected_level
    assert sim is None
    if expected_warning:
        assert expected_warning in warnings


# ========== Comparators — Title ==========


@pytest.mark.unit
def test_compare_title_exact() -> None:
    """Test title exact match returns level and sim=1.0."""
    level, sim, warnings = compare_title("test title", "test title", None, None, False, False)
    assert level == "exact"
    assert sim == 1.0
    assert warnings == []


@pytest.mark.unit
def test_compare_title_high_similarity_via_shingles() -> None:
    """Test title similarity computed via Jaccard on shingles."""
    shingles_a = ["machine", "learning", "applications", "healthcare"]
    shingles_b = ["machine", "learning", "applications", "healthcare", "systems"]
    level, sim, _ = compare_title(
        "machine learning applications healthcare",
        "machine learning applications healthcare systems",
        shingles_a,
        shingles_b,
        False,
        False,
    )
    # Jaccard = 4/5 = 0.8 → "low" level (0.75 <= sim < 0.85)
    assert level == "low"
    assert sim is not None
    assert abs(sim - 0.8) < 1e-6


@pytest.mark.unit
def test_compare_title_truncated_caps_at_medium() -> None:
    """Test truncation warning and high→medium cap."""
    level, sim, warnings = compare_title("test title", "test title", None, None, True, False)
    assert "title_truncated" in warnings
    # Exact match is NOT capped — only "high" would be capped to "medium"
    assert level == "exact"


@pytest.mark.unit
def test_compare_title_missing() -> None:
    """Test missing title returns missing level."""
    level, sim, warnings = compare_title(None, "some title", None, None, False, False)
    assert level == "missing"
    assert sim is None


@pytest.mark.unit
def test_compare_title_low_similarity_below_threshold() -> None:
    """Test title similarity below 0.75 returns missing."""
    level, sim, _ = compare_title("alpha beta", "gamma delta epsilon", None, None, False, False)
    assert level == "missing"
    assert sim is not None
    assert sim < 0.75


# ========== Comparators — Authors ==========


@pytest.mark.unit
def test_compare_authors_strong() -> None:
    """Test strong: first author matches + Jaccard >= 0.5."""
    level, sim, _ = compare_authors(
        "smith_j", "smith_j", ["smith_j", "jones_a"], ["smith_j", "jones_a", "brown_b"]
    )
    assert level == "strong"
    assert sim is not None
    assert sim >= 0.5


@pytest.mark.unit
def test_compare_authors_weak_first_author_only() -> None:
    """Test weak: first author matches but no author lists."""
    level, sim, _ = compare_authors("smith_j", "smith_j", None, None)
    assert level == "weak"
    assert sim is None


@pytest.mark.unit
def test_compare_authors_weak_sim_zero() -> None:
    """Test weak: first author matches but Jaccard=0.0 (disjoint lists).

    Regression test for bug where sim=0.0 was treated as falsy.
    """
    level, sim, _ = compare_authors("smith_j", "smith_j", ["aaa_x"], ["bbb_y"])
    assert level == "weak"
    assert sim is not None
    assert sim == 0.0


@pytest.mark.unit
def test_compare_authors_mismatch() -> None:
    """Test mismatch: different first authors with no overlap."""
    level, sim, _ = compare_authors("smith_j", "jones_a", ["smith_j"], ["jones_a"])
    assert level == "mismatch"


@pytest.mark.unit
def test_compare_authors_missing() -> None:
    """Test missing: no first author signatures."""
    level, sim, _ = compare_authors(None, "smith_j", None, None)
    assert level == "missing"
    assert sim is None


# ========== Comparators — Year ==========


@pytest.mark.unit
@pytest.mark.parametrize(
    ("year_a", "year_b", "expected_level"),
    [
        (2020, 2020, "exact"),
        (2020, 2021, "pm1"),
        (2020, 2022, "pm2"),
        (2020, 2025, "far"),
        (None, 2020, "missing"),
        (None, None, "missing"),
    ],
    ids=["exact", "pm1", "pm2", "far", "missing", "both_missing"],
)
def test_compare_year(year_a: int | None, year_b: int | None, expected_level: str) -> None:
    """Test year comparison at each delta threshold."""
    level, sim, _ = compare_year(year_a, year_b)
    assert level == expected_level
    assert sim is None


# ========== Comparators — Journal ==========


@pytest.mark.unit
@pytest.mark.parametrize(
    ("journal_a", "journal_b", "expected_level"),
    [
        ("nature", "nature", "high"),
        ("nature", "science", "low"),
        (None, "nature", "missing"),
        (None, None, "missing"),
    ],
    ids=["exact", "different", "missing", "both_missing"],
)
def test_compare_journal(journal_a: str | None, journal_b: str | None, expected_level: str) -> None:
    """Test journal comparison levels."""
    level, _, _ = compare_journal(journal_a, journal_b)
    assert level == expected_level


# ========== Comparators — Pages ==========


@pytest.mark.unit
def test_compare_pages_unreliable() -> None:
    """Test unreliable flag short-circuits comparison."""
    level, sim, warnings = compare_pages(
        pages_norm_long_a="123-456",
        pages_norm_long_b="123-456",
        page_first_a=None,
        page_first_b=None,
        page_last_a=None,
        page_last_b=None,
        article_number_a=None,
        article_number_b=None,
        pages_unreliable_a=True,
        pages_unreliable_b=False,
    )
    assert level == "unreliable"
    assert "pages_unreliable" in warnings


@pytest.mark.unit
def test_compare_pages_exact() -> None:
    """Test exact match on pages_norm_long."""
    level, _, _ = compare_pages(
        pages_norm_long_a="123-456",
        pages_norm_long_b="123-456",
        page_first_a="123",
        page_first_b="123",
        page_last_a=None,
        page_last_b=None,
        article_number_a=None,
        article_number_b=None,
        pages_unreliable_a=False,
        pages_unreliable_b=False,
    )
    assert level == "exact"


@pytest.mark.unit
def test_compare_pages_compatible_first_page() -> None:
    """Test compatible: first pages match without full pagination."""
    level, _, _ = compare_pages(
        pages_norm_long_a=None,
        pages_norm_long_b=None,
        page_first_a="123",
        page_first_b="123",
        page_last_a=None,
        page_last_b=None,
        article_number_a=None,
        article_number_b=None,
        pages_unreliable_a=False,
        pages_unreliable_b=False,
    )
    assert level == "compatible"


@pytest.mark.unit
def test_compare_pages_mismatch_different_long() -> None:
    """Test mismatch: pages_norm_long differ, no page_first.

    Regression test for bug where this returned 'missing'.
    """
    level, _, _ = compare_pages(
        pages_norm_long_a="123-456",
        pages_norm_long_b="789-012",
        page_first_a=None,
        page_first_b=None,
        page_last_a=None,
        page_last_b=None,
        article_number_a=None,
        article_number_b=None,
        pages_unreliable_a=False,
        pages_unreliable_b=False,
    )
    assert level == "mismatch"


@pytest.mark.unit
def test_compare_pages_mismatch_first_pages_differ() -> None:
    """Test mismatch: first pages differ."""
    level, _, _ = compare_pages(
        pages_norm_long_a=None,
        pages_norm_long_b=None,
        page_first_a="100",
        page_first_b="200",
        page_last_a=None,
        page_last_b=None,
        article_number_a=None,
        article_number_b=None,
        pages_unreliable_a=False,
        pages_unreliable_b=False,
    )
    assert level == "mismatch"


@pytest.mark.unit
def test_compare_pages_missing() -> None:
    """Test missing: no pagination data at all."""
    level, _, _ = compare_pages(
        pages_norm_long_a=None,
        pages_norm_long_b=None,
        page_first_a=None,
        page_first_b=None,
        page_last_a=None,
        page_last_b=None,
        article_number_a=None,
        article_number_b=None,
        pages_unreliable_a=False,
        pages_unreliable_b=False,
    )
    assert level == "missing"


@pytest.mark.unit
def test_compare_pages_article_number_exact() -> None:
    """Test exact match via article number."""
    level, _, _ = compare_pages(
        pages_norm_long_a=None,
        pages_norm_long_b=None,
        page_first_a=None,
        page_first_b=None,
        page_last_a=None,
        page_last_b=None,
        article_number_a="e12345",
        article_number_b="e12345",
        pages_unreliable_a=False,
        pages_unreliable_b=False,
    )
    assert level == "exact"


# ========== Comparators — Jaccard ==========


@pytest.mark.unit
def test_jaccard_similarity() -> None:
    """Test Jaccard similarity computation."""
    assert abs(jaccard_similarity({"a", "b", "c"}, {"b", "c", "d"}) - 0.5) < 1e-6
    assert jaccard_similarity({"a"}, {"b"}) == 0.0
    assert jaccard_similarity({"a", "b"}, {"a", "b"}) == 1.0


# ========== Data models ==========


@pytest.mark.unit
def test_field_comparison_to_dict() -> None:
    """Test FieldComparison serialization."""
    fc = FieldComparison(level="exact", sim=1.0, weight=6.9)
    d = fc.to_dict()
    assert d == {"level": "exact", "sim": 1.0, "weight": 6.9}


@pytest.mark.unit
def test_comparison_to_dict() -> None:
    """Test ComparisonResult serialization via free function."""
    comp = {
        "doi": FieldComparison(level="exact", sim=None, weight=6.9),
        "title": FieldComparison(level="missing", sim=None, weight=0.0),
    }
    d = comparison_to_dict(comp)
    assert d["doi"]["level"] == "exact"
    assert d["title"]["weight"] == 0.0


@pytest.mark.unit
def test_model_info_to_dict() -> None:
    """Test ModelInfo serialization."""
    info = ModelInfo(name="fs_v1", version="1.0.0")
    assert info.to_dict() == {"name": "fs_v1", "version": "1.0.0"}


# ========== End-to-end scoring ==========


@pytest.mark.unit
def test_score_pair_doi_exact_match(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test DOI exact match dominates score → high p(match)."""
    record_a = make_record(
        "rid_001",
        doi_norm="10.1234/test",
        title_norm="sample title",
        title_shingles=["sample", "title"],
    )
    record_b = make_record(
        "rid_002",
        doi_norm="10.1234/test",
        title_norm="slightly different title",
        title_shingles=["slightly", "different", "title"],
    )

    pair = score_pair(record_a, record_b, fs_model, [])

    assert pair.p_match > 0.9
    assert pair.comparison["doi"].level == "exact"
    assert pair.top_contributions[0]["field"] == "doi"


@pytest.mark.unit
def test_score_pair_doi_conflict(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test DOI conflict penalizes score despite matching title."""
    record_a = make_record(
        "rid_001",
        doi_norm="10.1234/test1",
        title_norm="same title",
        title_shingles=["same", "title"],
    )
    record_b = make_record(
        "rid_002",
        doi_norm="10.1234/test2",
        title_norm="same title",
        title_shingles=["same", "title"],
    )

    pair = score_pair(record_a, record_b, fs_model, [])

    assert pair.p_match < 0.5
    assert pair.comparison["doi"].level == "both_present_mismatch"
    assert "both_present_id_conflicts" in pair.warnings


@pytest.mark.unit
def test_score_pair_no_ids_similar(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test scoring with no IDs but similar title + authors + year."""
    record_a = make_record(
        "rid_001",
        title_norm="machine learning applications in healthcare",
        title_shingles=["machine", "learning", "applications", "healthcare"],
        first_author_sig="smith_j",
        author_sig_strict=["smith_j", "jones_a"],
        year_norm=2020,
    )
    record_b = make_record(
        "rid_002",
        title_norm="machine learning applications in healthcare systems",
        title_shingles=["machine", "learning", "applications", "healthcare", "systems"],
        first_author_sig="smith_j",
        author_sig_strict=["smith_j", "jones_a"],
        year_norm=2020,
    )

    pair = score_pair(record_a, record_b, fs_model, [])

    assert pair.p_match > 0.5
    assert pair.comparison["title"].level in ("high", "medium", "low")
    assert pair.comparison["authors"].level in ("strong", "weak")


@pytest.mark.unit
def test_score_pair_title_truncated(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test title truncated propagates as warning."""
    record_a = make_record(
        "rid_001",
        title_norm="sample title",
        title_shingles=["sample", "title"],
        title_truncated=True,
    )
    record_b = make_record(
        "rid_002",
        title_norm="sample title",
        title_shingles=["sample", "title"],
    )

    pair = score_pair(record_a, record_b, fs_model, [])

    assert "title_truncated" in pair.warnings


@pytest.mark.unit
def test_score_pair_pages_unreliable(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test pages unreliable flag produces correct level and warning."""
    record_a = make_record("rid_001", pages_norm_long="123-456", pages_unreliable=True)
    record_b = make_record("rid_002", pages_norm_long="123-456")

    pair = score_pair(record_a, record_b, fs_model, [])

    assert pair.comparison["pages"].level == "unreliable"
    assert "pages_unreliable" in pair.warnings


@pytest.mark.unit
def test_score_pair_all_missing(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test scoring with all fields missing yields low p(match)."""
    record_a = make_record("rid_001")
    record_b = make_record("rid_002")

    pair = score_pair(record_a, record_b, fs_model, [])

    # All missing → prior dominates, p_match should be near lambda_prior
    assert pair.p_match < 0.1


@pytest.mark.unit
def test_score_pair_to_dict(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test PairScore serialization to dict."""
    record_a = make_record("rid_001", doi_norm="10.1234/test")
    record_b = make_record("rid_002", doi_norm="10.1234/test")

    pair = score_pair(record_a, record_b, fs_model, [])
    d = pair.to_dict()

    assert d["pair_id"] == "rid_001|rid_002"
    assert "p_match" in d
    assert "llr" in d
    assert "comparison" in d
    assert "doi" in d["comparison"]
    assert d["model"]["name"] == "fs_v1"
    assert isinstance(d["explain"]["top_contributions"], list)
    assert isinstance(d["warnings"], list)


@pytest.mark.unit
def test_determinism(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test that scoring the same pair twice yields identical results."""
    record_a = make_record(
        "rid_001",
        doi_norm="10.1234/test",
        title_norm="test title",
        title_shingles=["test", "title"],
    )
    record_b = make_record(
        "rid_002",
        doi_norm="10.1234/test",
        title_norm="test title",
        title_shingles=["test", "title"],
    )

    score1 = score_pair(record_a, record_b, fs_model, [])
    score2 = score_pair(record_a, record_b, fs_model, [])

    assert score1.llr == score2.llr
    assert score1.p_match == score2.p_match
    assert score1.comparison == score2.comparison


# ========== Pipeline ==========


@pytest.mark.unit
def test_score_all_pairs(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test scoring multiple pairs from candidates file."""
    records = [
        make_record("rid_001", doi_norm="10.1234/test", title_norm="title one"),
        make_record("rid_002", doi_norm="10.1234/test", title_norm="title two"),
        make_record("rid_003", pmid_norm="12345", title_norm="title three"),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        candidates = Path(tmpdir) / "candidates.jsonl"
        output = Path(tmpdir) / "pair_scores.jsonl"

        candidates.write_text(
            json.dumps(
                {
                    "pair_id": "rid_001|rid_002",
                    "rid_a": "rid_001",
                    "rid_b": "rid_002",
                    "sources": [
                        {
                            "blocker": "DOIExact",
                            "block_key": "10.1234/test",
                            "match_key": "doi_norm",
                        }
                    ],
                }
            )
            + "\n"
        )

        stats = score_all_pairs(candidates, records, output, fs_model)

        assert stats["pairs_in"] == 1
        assert stats["pairs_scored"] == 1
        assert stats["pairs_skipped_missing_records"] == 0

        scores = [json.loads(line) for line in output.read_text().splitlines()]
        assert len(scores) == 1
        assert scores[0]["pair_id"] == "rid_001|rid_002"
        assert "p_match" in scores[0]
        assert "llr" in scores[0]


@pytest.mark.unit
def test_output_deterministic_ordering(
    fs_model: FSModel,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test that output is sorted by pair_id regardless of input order."""
    records = [
        make_record("rid_001", title_norm="title one"),
        make_record("rid_002", title_norm="title two"),
        make_record("rid_003", title_norm="title three"),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        candidates = Path(tmpdir) / "candidates.jsonl"
        output = Path(tmpdir) / "pair_scores.jsonl"

        # Write candidates in reverse order
        candidates.write_text(
            json.dumps(
                {
                    "pair_id": "rid_002|rid_003",
                    "rid_a": "rid_002",
                    "rid_b": "rid_003",
                    "sources": [],
                }
            )
            + "\n"
            + json.dumps(
                {
                    "pair_id": "rid_001|rid_002",
                    "rid_a": "rid_001",
                    "rid_b": "rid_002",
                    "sources": [],
                }
            )
            + "\n"
        )

        score_all_pairs(candidates, records, output, fs_model)

        scores = [json.loads(line) for line in output.read_text().splitlines()]
        assert scores[0]["pair_id"] == "rid_001|rid_002"
        assert scores[1]["pair_id"] == "rid_002|rid_003"
