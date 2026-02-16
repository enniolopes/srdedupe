"""Tests for blocker plug-ins and factory."""

from __future__ import annotations

import pytest

from srdedupe.candidates import (
    BLOCKER_REGISTRY,
    BibRareTitleTokensBlocker,
    BibYearPM1FirstAuthorBlocker,
    BibYearPM1TitlePrefixBlocker,
    BlockerConfig,
    DOIExactBlocker,
    MinHashLSHTitleBlocker,
    PMIDExactBlocker,
    SimHashTitleBlocker,
    create_blocker,
    create_blockers,
)
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
    rid: str,
    *,
    doi_norm: str | None = None,
    pmid_norm: str | None = None,
    title_norm_basic: str | None = None,
    title_shingles: list[str] | None = None,
    title_key_strict: str | None = None,
    year_norm: int | None = None,
    first_author_sig: str | None = None,
    title_missing: bool = False,
) -> CanonicalRecord:
    """Build a CanonicalRecord with only the fields we care about."""
    canon = Canon(
        **{
            **dict.fromkeys(
                [
                    "doi",
                    "doi_url",
                    "pmid",
                    "pmcid",
                    "title_raw",
                    "abstract_raw",
                    "abstract_norm_basic",
                    "authors_raw",
                    "authors_parsed",
                    "author_sig_strict",
                    "author_sig_loose",
                    "year_raw",
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
            "title_norm_basic": title_norm_basic,
            "first_author_sig": first_author_sig,
            "year_norm": year_norm,
        },
    )
    keys = Keys(
        title_key_strict=title_key_strict,
        title_year_key=None,
        title_first_author_key=None,
        title_journal_key=None,
        title_key_fuzzy=None,
        title_shingles=title_shingles,
        title_minhash=None,
        title_simhash=None,
    )
    flags = Flags(
        doi_present=doi_norm is not None,
        pmid_present=pmid_norm is not None,
        title_missing=title_missing,
        title_truncated=False,
        authors_missing=first_author_sig is None,
        authors_incomplete=False,
        year_missing=year_norm is None,
        pages_unreliable=False,
        is_erratum_notice=False,
        is_retraction_notice=False,
        is_corrected_republished=False,
        has_linked_citation=False,
    )
    return CanonicalRecord(
        schema_version=SCHEMA_VERSION,
        rid=rid,
        record_digest="sha256:test",
        source_digest=None,
        meta=_META,
        raw=_RAW,
        canon=canon,
        keys=keys,
        flags=flags,
        provenance={},
    )


# ============================================================================
# Exact blockers
# ============================================================================


@pytest.mark.unit
def test_doi_blocker_yields_doi_and_nothing_when_absent() -> None:
    """DOI blocker yields the normalised DOI, or nothing."""
    b = DOIExactBlocker()
    assert list(b.block_keys(_record("r1", doi_norm="10.1234/x"))) == ["10.1234/x"]
    assert list(b.block_keys(_record("r2"))) == []
    assert list(b.block_keys(_record("r3", doi_norm=""))) == []


@pytest.mark.unit
def test_pmid_blocker_yields_pmid_and_nothing_when_absent() -> None:
    """PMID blocker yields the normalised PMID, or nothing."""
    b = PMIDExactBlocker()
    assert list(b.block_keys(_record("r1", pmid_norm="123"))) == ["123"]
    assert list(b.block_keys(_record("r2"))) == []
    assert list(b.block_keys(_record("r3", pmid_norm=""))) == []


# ============================================================================
# MinHash LSH
# ============================================================================


@pytest.mark.unit
def test_minhash_produces_deterministic_banded_keys() -> None:
    """MinHash emits one key per band, deterministically."""
    b = MinHashLSHTitleBlocker(num_perm=128, bands=16)
    rec = _record("r1", title_shingles=["machine", "learning", "algorithms", "data"])
    keys = list(b.block_keys(rec))

    assert len(keys) == 16
    assert all(k.startswith("mh:b") for k in keys)
    assert keys == list(b.block_keys(rec))  # deterministic


@pytest.mark.unit
def test_minhash_identical_titles_share_all_bands() -> None:
    """Records with identical titles must share every band key."""
    b = MinHashLSHTitleBlocker(num_perm=128, bands=16)
    tokens = ["machine", "learning", "algorithms", "data"]
    keys_a = set(b.block_keys(_record("a", title_shingles=tokens)))
    keys_b = set(b.block_keys(_record("b", title_shingles=tokens)))
    assert keys_a == keys_b


@pytest.mark.unit
def test_minhash_skips_missing_or_short_titles() -> None:
    """MinHash produces nothing for missing or too-short titles."""
    b = MinHashLSHTitleBlocker(num_perm=128, bands=16, min_tokens=3)
    assert list(b.block_keys(_record("r1", title_missing=True))) == []
    assert list(b.block_keys(_record("r2", title_shingles=["ab"]))) == []


# ============================================================================
# SimHash
# ============================================================================


@pytest.mark.unit
def test_simhash_produces_deterministic_chunked_keys() -> None:
    """SimHash emits one key per chunk, deterministically."""
    b = SimHashTitleBlocker(bits=64, chunks=4)
    tokens = ["machine", "learning", "algorithms", "data", "analysis"]
    rec = _record("r1", title_shingles=tokens)
    keys = list(b.block_keys(rec))

    assert len(keys) == 4
    assert all(k.startswith("sh:c") for k in keys)
    assert keys == list(b.block_keys(rec))  # deterministic


@pytest.mark.unit
def test_simhash_skips_missing_or_short_titles() -> None:
    """SimHash produces nothing for missing or too-short titles."""
    b = SimHashTitleBlocker(bits=64, chunks=4, min_tokens=5)
    assert list(b.block_keys(_record("r1", title_missing=True))) == []
    assert list(b.block_keys(_record("r2", title_shingles=["a", "b"]))) == []


# ============================================================================
# Bibliographic: year ± 1
# ============================================================================


@pytest.mark.unit
def test_year_author_expands_year_window() -> None:
    """Year-author blocker emits year-1, year, year+1 keys."""
    b = BibYearPM1FirstAuthorBlocker()
    keys = list(b.block_keys(_record("r1", year_norm=2020, first_author_sig="smith")))
    assert set(keys) == {"y2019:smith", "y2020:smith", "y2021:smith"}


@pytest.mark.unit
def test_year_author_skips_incomplete_data() -> None:
    """Year-author blocker requires both year and author."""
    b = BibYearPM1FirstAuthorBlocker()
    assert list(b.block_keys(_record("r1", year_norm=2020))) == []
    assert list(b.block_keys(_record("r2", first_author_sig="smith"))) == []


@pytest.mark.unit
def test_year_author_records_differ_by_one_year_share_keys() -> None:
    """Records differing by 1 year share at least one key."""
    b = BibYearPM1FirstAuthorBlocker()
    keys_a = set(b.block_keys(_record("a", year_norm=2020, first_author_sig="smith")))
    keys_b = set(b.block_keys(_record("b", year_norm=2021, first_author_sig="smith")))
    assert keys_a & keys_b


@pytest.mark.unit
def test_year_title_prefix_keys() -> None:
    """Year-title-prefix blocker emits 3 keys with truncated prefix."""
    b = BibYearPM1TitlePrefixBlocker(prefix_len=10)
    keys = list(
        b.block_keys(
            _record(
                "r1",
                year_norm=2020,
                title_key_strict="machinelearningalgorithms",
            )
        )
    )
    assert set(keys) == {"y2019:tpmachinelea", "y2020:tpmachinelea", "y2021:tpmachinelea"}


@pytest.mark.unit
def test_year_title_prefix_skips_incomplete_data() -> None:
    """Year-title-prefix blocker requires both year and title key."""
    b = BibYearPM1TitlePrefixBlocker()
    assert list(b.block_keys(_record("r1", year_norm=2020))) == []
    assert list(b.block_keys(_record("r2", title_key_strict="something"))) == []


# ============================================================================
# Rare title tokens (stateful)
# ============================================================================

_RARE_CORPUS = [
    _record("r1", title_shingles=["machine", "learning", "algorithms"]),
    _record("r2", title_shingles=["machine", "learning", "models"]),
    _record("r3", title_shingles=["deep", "learning", "networks"]),
]


@pytest.mark.unit
def test_rare_tokens_computes_df_correctly() -> None:
    """Initialize counts document frequency per token."""
    b = BibRareTitleTokensBlocker(k=3, df_max_ratio=0.5)
    b.initialize(_RARE_CORPUS)

    assert b.total_docs == 3
    assert b.token_df is not None
    assert b.token_df["learning"] == 3
    assert b.token_df["machine"] == 2
    assert b.token_df["algorithms"] == 1


@pytest.mark.unit
def test_rare_tokens_selects_rarest() -> None:
    """Block keys are the rarest tokens below the DF threshold."""
    b = BibRareTitleTokensBlocker(k=2, df_max_ratio=0.4)
    b.initialize(_RARE_CORPUS)

    # Record r1: "learning"=3 (100%), "machine"=2 (67%), "algorithms"=1 (33%)
    # max_df = int(3 * 0.4) = 1 → only tokens with DF ≤ 1
    keys = list(b.block_keys(_RARE_CORPUS[0]))
    assert keys == ["rt:algorithms"]


@pytest.mark.unit
def test_rare_tokens_shared_rare_connects_records() -> None:
    """Records sharing a rare token produce a common key."""
    corpus = [
        _record("a", title_shingles=["quantum", "computing", "science"]),
        _record("b", title_shingles=["quantum", "mechanics", "theory"]),
        _record("c", title_shingles=["machine", "learning", "models"]),
        _record("d", title_shingles=["deep", "learning", "networks"]),
    ]
    b = BibRareTitleTokensBlocker(k=3, df_max_ratio=0.5)
    b.initialize(corpus)

    keys_a = set(b.block_keys(corpus[0]))
    keys_b = set(b.block_keys(corpus[1]))
    assert "rt:quantum" in keys_a & keys_b


@pytest.mark.unit
def test_rare_tokens_raises_without_initialize() -> None:
    """Calling block_keys before initialize raises RuntimeError."""
    b = BibRareTitleTokensBlocker()
    with pytest.raises(RuntimeError, match="initialize"):
        list(b.block_keys(_record("r1", title_shingles=["token"])))


# ============================================================================
# Factory
# ============================================================================


@pytest.mark.unit
def test_factory_creates_all_registered_types() -> None:
    """Every key in BLOCKER_REGISTRY produces a valid blocker."""
    for type_name in BLOCKER_REGISTRY:
        blocker = create_blocker(BlockerConfig(type=type_name))
        assert hasattr(blocker, "name")
        assert hasattr(blocker, "block_keys")


@pytest.mark.unit
def test_factory_rejects_unknown_type() -> None:
    """Unknown blocker type raises ValueError."""
    with pytest.raises(ValueError, match="Unknown blocker type"):
        create_blocker(BlockerConfig(type="nonexistent"))


@pytest.mark.unit
def test_factory_passes_params() -> None:
    """Constructor params flow through the factory."""
    b = create_blocker(BlockerConfig(type="simhash", params={"bits": 32, "chunks": 2}))
    assert isinstance(b, SimHashTitleBlocker)
    assert b.bits == 32
    assert b.chunks == 2


@pytest.mark.unit
def test_create_blockers_filters_disabled() -> None:
    """create_blockers skips disabled configs."""
    configs = [
        BlockerConfig(type="doi", enabled=True),
        BlockerConfig(type="pmid", enabled=False),
    ]
    result = create_blockers(configs)
    assert len(result) == 1
    assert isinstance(result[0], DOIExactBlocker)
