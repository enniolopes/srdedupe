"""Tests for candidate pair generation orchestrator."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from srdedupe.audit.logger import AuditLogger
from srdedupe.candidates import DOIExactBlocker, PMIDExactBlocker, generate_candidates
from srdedupe.models import SCHEMA_VERSION, Canon, CanonicalRecord, Flags, Keys, Meta, Raw, RawTag

# ============================================================================
# Minimal record builder
# ============================================================================

_RAW = Raw(
    record_lines=["TY  - JOUR"],
    tags=[
        RawTag(
            tag="TY",
            value_lines=["JOUR"],
            value_raw_joined="JOUR",
            occurrence=0,
            line_start=0,
            line_end=0,
        )
    ],
    unknown_lines=[],
)
_META = Meta(
    source_file="t.ris",
    source_format="ris",
    source_db=None,
    source_record_index=0,
    ingested_at="2024-01-01T00:00:00Z",
)


def _record(
    rid: str, *, doi_norm: str | None = None, pmid_norm: str | None = None
) -> CanonicalRecord:
    """Build a minimal record with optional identifiers."""
    return CanonicalRecord(
        schema_version=SCHEMA_VERSION,
        rid=rid,
        record_digest="sha256:test",
        source_digest=None,
        meta=_META,
        raw=_RAW,
        canon=Canon(
            **{
                **dict.fromkeys(
                    [
                        "doi",
                        "doi_url",
                        "pmid",
                        "pmcid",
                        "title_raw",
                        "title_norm_basic",
                        "abstract_raw",
                        "abstract_norm_basic",
                        "authors_raw",
                        "authors_parsed",
                        "first_author_sig",
                        "author_sig_strict",
                        "author_sig_loose",
                        "year_raw",
                        "year_norm",
                        "year_source",
                        "journal_full",
                        "journal_abbrev",
                        "journal_norm",
                        "volume",
                        "issue",
                        "pages_raw",
                        "pages_norm_long",
                        "page_first",
                        "page_last",
                        "article_number",
                        "language",
                        "publication_type",
                    ]
                ),
                "doi_norm": doi_norm,
                "pmid_norm": pmid_norm,
            }
        ),
        keys=Keys.empty(),
        flags=Flags(
            doi_present=doi_norm is not None,
            pmid_present=pmid_norm is not None,
            title_missing=True,
            title_truncated=False,
            authors_missing=True,
            authors_incomplete=False,
            year_missing=True,
            pages_unreliable=False,
            is_erratum_notice=False,
            is_retraction_notice=False,
            is_corrected_republished=False,
            has_linked_citation=False,
        ),
        provenance={},
    )


def _read_pairs(path: Path) -> list[dict]:
    """Read JSONL pairs from file."""
    with path.open("r") as fh:
        return [json.loads(line) for line in fh]


# ============================================================================
# Core pair generation
# ============================================================================


@pytest.mark.unit
def test_shared_doi_produces_one_pair(tmp_path: Path) -> None:
    """Two records sharing a DOI produce exactly one pair."""
    records = [
        _record("a", doi_norm="10.1234/shared"),
        _record("b", doi_norm="10.1234/shared"),
        _record("c", doi_norm="10.1234/other"),
    ]
    out = tmp_path / "c.jsonl"
    stats = generate_candidates([DOIExactBlocker()], records, out)

    assert stats["global"]["pairs_total_unique"] == 1
    pairs = _read_pairs(out)
    assert len(pairs) == 1
    assert pairs[0]["pair_id"] == "a|b"


@pytest.mark.unit
def test_multi_source_pair(tmp_path: Path) -> None:
    """A pair found by both DOI and PMID has two sources."""
    records = [
        _record("a", doi_norm="10.1/x", pmid_norm="99"),
        _record("b", doi_norm="10.1/x", pmid_norm="99"),
    ]
    out = tmp_path / "c.jsonl"
    stats = generate_candidates([DOIExactBlocker(), PMIDExactBlocker()], records, out)

    assert stats["global"]["pairs_total_unique"] == 1
    assert stats["global"]["pairs_with_multiple_sources"] == 1

    pair = _read_pairs(out)[0]
    blocker_names = {s["blocker"] for s in pair["sources"]}
    assert blocker_names == {"doi_exact", "pmid_exact"}


@pytest.mark.unit
def test_unique_records_produce_no_pairs(tmp_path: Path) -> None:
    """All-different identifiers → zero pairs."""
    records = [_record(f"r{i}", doi_norm=f"10.1/{i}") for i in range(5)]
    out = tmp_path / "c.jsonl"
    stats = generate_candidates([DOIExactBlocker()], records, out)

    assert stats["global"]["pairs_total_unique"] == 0
    assert _read_pairs(out) == []


@pytest.mark.unit
def test_block_combinatorics(tmp_path: Path) -> None:
    """N records in one block → N*(N-1)/2 pairs."""
    n = 5
    records = [_record(f"r{i:02d}", doi_norm="10.1/same") for i in range(n)]
    out = tmp_path / "c.jsonl"
    stats = generate_candidates([DOIExactBlocker()], records, out)

    expected = n * (n - 1) // 2
    assert stats["global"]["pairs_total_unique"] == expected
    assert len(_read_pairs(out)) == expected


@pytest.mark.unit
def test_pair_ids_are_lexicographic(tmp_path: Path) -> None:
    """rid_a < rid_b in every emitted pair, regardless of input order."""
    records = [
        _record("zebra", doi_norm="10.1/x"),
        _record("alpha", doi_norm="10.1/x"),
    ]
    out = tmp_path / "c.jsonl"
    generate_candidates([DOIExactBlocker()], records, out)

    pair = _read_pairs(out)[0]
    assert pair["rid_a"] == "alpha"
    assert pair["rid_b"] == "zebra"


@pytest.mark.unit
def test_deterministic_output(tmp_path: Path) -> None:
    """Shuffled input produces byte-identical output."""
    recs = [_record(c, doi_norm="10.1/x") for c in ("c", "a", "b")]
    out1 = tmp_path / "c1.jsonl"
    generate_candidates([DOIExactBlocker()], recs, out1)

    recs_shuffled = [recs[1], recs[2], recs[0]]
    out2 = tmp_path / "c2.jsonl"
    generate_candidates([DOIExactBlocker()], recs_shuffled, out2)

    assert out1.read_bytes() == out2.read_bytes()


@pytest.mark.unit
def test_jsonl_pair_structure(tmp_path: Path) -> None:
    """Each JSONL line contains the required fields."""
    records = [
        _record("a", doi_norm="10.1/x"),
        _record("b", doi_norm="10.1/x"),
    ]
    out = tmp_path / "c.jsonl"
    generate_candidates([DOIExactBlocker()], records, out)

    pair = _read_pairs(out)[0]
    assert set(pair.keys()) >= {"pair_id", "rid_a", "rid_b", "sources"}
    src = pair["sources"][0]
    assert set(src.keys()) >= {"blocker", "block_key", "match_key"}


@pytest.mark.unit
def test_stats_structure(tmp_path: Path) -> None:
    """Stats dict has expected top-level keys and per-blocker counters."""
    records = [
        _record("a", doi_norm="10.1/x"),
        _record("b", doi_norm="10.1/x"),
    ]
    out = tmp_path / "c.jsonl"
    stats = generate_candidates([DOIExactBlocker()], records, out)

    assert "blockers" in stats
    assert "global" in stats

    doi_stats = stats["blockers"]["doi_exact"]
    assert doi_stats["records_seen"] == 2
    assert doi_stats["records_keyed"] == 2
    assert doi_stats["unique_keys"] == 1
    assert doi_stats["blocks_gt1"] == 1
    assert doi_stats["pairs_raw"] == 1
    assert doi_stats["pairs_unique"] == 1


# ============================================================================
# Audit logging
# ============================================================================


@pytest.mark.unit
def test_audit_events_emitted(tmp_path: Path) -> None:
    """Generator emits stage_started, artifact_written, stage_finished."""
    records = [
        _record("a", doi_norm="10.1/x"),
        _record("b", doi_norm="10.1/x"),
    ]
    log_path = tmp_path / "events.jsonl"
    logger = AuditLogger(run_id="test", log_path=log_path)

    out = tmp_path / "c.jsonl"
    generate_candidates([DOIExactBlocker()], records, out, logger=logger)

    events = [json.loads(line) for line in log_path.read_text().splitlines()]
    event_types = {e["event"] for e in events}

    assert {"stage_started", "artifact_written", "stage_finished"} <= event_types

    artifact = next(e for e in events if e["event"] == "artifact_written")
    assert artifact["data"]["record_count"] == 1
    assert "sha256" in artifact["data"]
