"""Tests for canonical merge functionality."""

from collections.abc import Callable

import pytest

from srdedupe.merge.field_merge import merge_canon_fields
from srdedupe.merge.models import compute_merged_id
from srdedupe.merge.ris_writer import format_canon_as_ris
from srdedupe.merge.survivor import select_survivor
from srdedupe.models import AuthorParsed, Canon, CanonicalRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AUTHORS_1 = [AuthorParsed(family="Smith", given="J", initials="J", suffix=None, raw="Smith J")]
_AUTHORS_3 = [
    AuthorParsed(family="Smith", given="J", initials="J", suffix=None, raw="Smith J"),
    AuthorParsed(family="Doe", given="A", initials="A", suffix=None, raw="Doe A"),
    AuthorParsed(family="Brown", given="B", initials="B", suffix=None, raw="Brown B"),
]


def _merge(
    make_record: Callable[..., CanonicalRecord],
    records_kwargs: list[dict],
) -> tuple[Canon, dict]:
    """Build records, select survivor, merge, return canon + provenance dict."""
    records = [make_record(**kw) for kw in records_kwargs]
    survivor_rid = select_survivor(records)
    canon, prov = merge_canon_fields(records, survivor_rid)
    return canon, {k: v.rule for k, v in prov.fields.items()}


# ---------------------------------------------------------------------------
# compute_merged_id
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_merged_id_is_deterministic_and_order_independent() -> None:
    """Merged ID is stable regardless of RID ordering."""
    id_a = compute_merged_id(["rid_002", "rid_001", "rid_003"])
    id_b = compute_merged_id(["rid_001", "rid_003", "rid_002"])

    assert id_a == id_b
    assert id_a.startswith("m:")
    assert len(id_a) == 14  # "m:" + 12 hex chars


# ---------------------------------------------------------------------------
# select_survivor — parametrized ranking
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_empty_records_raises() -> None:
    """Empty input raises ValueError."""
    with pytest.raises(ValueError, match="Cannot select survivor from empty"):
        select_survivor([])


@pytest.mark.unit
@pytest.mark.parametrize(
    ("winner_kw", "loser_kw", "reason"),
    [
        pytest.param(
            {"doi_norm": "10.1234/a", "title_raw": "T"},
            {"title_raw": "T"},
            "DOI presence wins",
            id="doi_wins",
        ),
        pytest.param(
            {"pmid_norm": "123", "title_raw": "T"},
            {"title_raw": "T"},
            "PMID presence wins",
            id="pmid_wins",
        ),
        pytest.param(
            {"title_raw": "T", "abstract_raw": "Abstract text"},
            {"title_raw": "T"},
            "abstract presence wins",
            id="abstract_wins",
        ),
        pytest.param(
            {"title_raw": "T", "authors_parsed": _AUTHORS_3},
            {"title_raw": "T", "authors_parsed": _AUTHORS_1},
            "more authors wins",
            id="author_count_wins",
        ),
        pytest.param(
            {"title_raw": "T", "year_norm": 2020, "journal_full": "J", "volume": "1", "issue": "2"},
            {"title_raw": "T"},
            "higher metadata completeness wins",
            id="completeness_wins",
        ),
    ],
)
def test_survivor_ranking(
    make_record: Callable[..., CanonicalRecord],
    winner_kw: dict,
    loser_kw: dict,
    reason: str,
) -> None:
    """Survivor selection follows expected priority order."""
    winner = make_record("rid_winner", **winner_kw)
    loser = make_record("rid_loser", **loser_kw)

    # Must win regardless of input order
    assert select_survivor([loser, winner]) == "rid_winner"
    assert select_survivor([winner, loser]) == "rid_winner"


@pytest.mark.unit
def test_survivor_tie_breaks_by_smallest_rid(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Equal records tie-break to lexicographically smallest RID."""
    a = make_record("rid_zzz", title_raw="T")
    b = make_record("rid_aaa", title_raw="T")

    assert select_survivor([a, b]) == "rid_aaa"


# ---------------------------------------------------------------------------
# merge_canon_fields — field-level rules
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "short_val", "long_val"),
    [
        pytest.param("title_raw", "Short", "Much Longer Title With Words", id="title"),
        pytest.param("abstract_raw", "Brief", "A much longer abstract text here", id="abstract"),
        pytest.param("journal_full", "J Med", "Journal of Medicine", id="journal"),
    ],
)
def test_merge_prefers_longest_text(
    make_record: Callable[..., CanonicalRecord],
    field: str,
    short_val: str,
    long_val: str,
) -> None:
    """Text fields merge by choosing longest non-null value."""
    canon, _ = _merge(
        make_record,
        [
            {"rid": "rid_001", "doi_norm": "10.1/x", field: short_val},
            {"rid": "rid_002", "doi_norm": "10.1/x", field: long_val},
        ],
    )

    assert getattr(canon, field) == long_val


@pytest.mark.unit
def test_merge_prefers_more_authors(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Authors merge by choosing record with most parsed authors."""
    canon, _ = _merge(
        make_record,
        [
            {"rid": "rid_001", "doi_norm": "10.1/x", "authors_parsed": _AUTHORS_1},
            {"rid": "rid_002", "doi_norm": "10.1/x", "authors_parsed": _AUTHORS_3},
        ],
    )

    assert canon.authors_parsed is not None
    assert len(canon.authors_parsed) == 3


@pytest.mark.unit
def test_merge_year_uses_mode(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Year merges by statistical mode across cluster."""
    canon, _ = _merge(
        make_record,
        [
            {"rid": "rid_001", "doi_norm": "10.1/x", "year_norm": 2020},
            {"rid": "rid_002", "doi_norm": "10.1/x", "year_norm": 2020},
            {"rid": "rid_003", "doi_norm": "10.1/x", "year_norm": 2021},
        ],
    )

    assert canon.year_norm == 2020


@pytest.mark.unit
def test_merge_prefers_reliable_pages(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Pagination merges by preferring reliable over unreliable pages."""
    canon, _ = _merge(
        make_record,
        [
            {
                "rid": "rid_001",
                "doi_norm": "10.1/x",
                "page_first": "100",
                "page_last": "200",
                "pages_unreliable": True,
            },
            {
                "rid": "rid_002",
                "doi_norm": "10.1/x",
                "page_first": "101",
                "page_last": "105",
                "pages_unreliable": False,
            },
        ],
    )

    assert canon.page_first == "101"
    assert canon.page_last == "105"


@pytest.mark.unit
def test_merge_strong_id_unique_passes(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Identical DOI/PMID across cluster merges without error."""
    canon, _ = _merge(
        make_record,
        [
            {"rid": "rid_001", "doi_norm": "10.1/x", "pmid_norm": "123"},
            {"rid": "rid_002", "doi_norm": "10.1/x", "pmid_norm": "123"},
        ],
    )

    assert canon.doi_norm == "10.1/x"
    assert canon.pmid_norm == "123"


@pytest.mark.unit
def test_merge_strong_id_conflict_raises(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Conflicting DOIs in same cluster raises RuntimeError."""
    records = [
        make_record("rid_001", doi_norm="10.1/a"),
        make_record("rid_002", doi_norm="10.1/b"),
    ]
    with pytest.raises(RuntimeError, match="Multiple distinct doi_norm"):
        merge_canon_fields(records, "rid_001")


@pytest.mark.unit
def test_merge_multi_value_union(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Multi-value fields produce sorted distinct union."""
    canon, _ = _merge(
        make_record,
        [
            {"rid": "rid_001", "doi_norm": "10.1/x", "language": "eng"},
            {"rid": "rid_002", "doi_norm": "10.1/x", "language": "fre"},
        ],
    )

    assert canon.language == ["eng", "fre"]


# ---------------------------------------------------------------------------
# Determinism — the critical property
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_merge_is_deterministic_regardless_of_input_order(
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Merge produces identical output for any permutation of inputs."""
    kw1 = {"rid": "rid_001", "doi_norm": "10.1/x", "title_raw": "Short", "year_norm": 2020}
    kw2 = {
        "rid": "rid_002",
        "doi_norm": "10.1/x",
        "title_raw": "Much Longer Title",
        "year_norm": 2020,
    }

    records_ab = [make_record(**kw1), make_record(**kw2)]
    records_ba = [make_record(**kw2), make_record(**kw1)]

    survivor_ab = select_survivor(records_ab)
    survivor_ba = select_survivor(records_ba)
    assert survivor_ab == survivor_ba

    canon_ab, _ = merge_canon_fields(records_ab, survivor_ab)
    canon_ba, _ = merge_canon_fields(records_ba, survivor_ba)

    assert canon_ab.title_raw == canon_ba.title_raw
    assert canon_ab.doi_norm == canon_ba.doi_norm
    assert canon_ab.year_norm == canon_ba.year_norm


# ---------------------------------------------------------------------------
# RIS output — format contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ris_format_ty_first_er_last() -> None:
    """RIS output always starts with TY and ends with ER."""
    canon = Canon(
        doi=None,
        doi_norm=None,
        doi_url=None,
        pmid=None,
        pmid_norm=None,
        pmcid=None,
        title_raw="Test",
        title_norm_basic=None,
        abstract_raw=None,
        abstract_norm_basic=None,
        authors_raw=None,
        authors_parsed=None,
        first_author_sig=None,
        author_sig_strict=None,
        author_sig_loose=None,
        year_raw=None,
        year_norm=None,
        year_source=None,
        journal_full=None,
        journal_abbrev=None,
        journal_norm=None,
        volume=None,
        issue=None,
        pages_raw=None,
        pages_norm_long=None,
        page_first=None,
        page_last=None,
        article_number=None,
        language=None,
        publication_type=None,
    )
    ris = format_canon_as_ris(canon)
    lines = ris.split("\r\n")

    assert lines[0] == "TY  - JOUR"
    assert lines[-1] == "ER  -"


@pytest.mark.unit
def test_ris_includes_all_populated_fields() -> None:
    """RIS output includes expected tags for a fully populated Canon."""
    authors = [
        AuthorParsed(family="Smith", given="John", initials="J", suffix=None, raw="Smith J"),
        AuthorParsed(family="Doe", given="Alice", initials="A", suffix=None, raw="Doe A"),
    ]
    canon = Canon(
        doi="10.1/x",
        doi_norm="10.1/x",
        doi_url="https://doi.org/10.1/x",
        pmid=None,
        pmid_norm=None,
        pmcid=None,
        title_raw="Test Article",
        title_norm_basic=None,
        abstract_raw="Abstract text.",
        abstract_norm_basic=None,
        authors_raw=None,
        authors_parsed=authors,
        first_author_sig=None,
        author_sig_strict=None,
        author_sig_loose=None,
        year_raw="2020",
        year_norm=2020,
        year_source=None,
        journal_full="Test Journal",
        journal_abbrev=None,
        journal_norm=None,
        volume="10",
        issue="5",
        pages_raw=None,
        pages_norm_long=None,
        page_first="100",
        page_last="105",
        article_number=None,
        language="eng",
        publication_type=None,
    )
    ris = format_canon_as_ris(canon)

    expected_tags = [
        "TI  -",
        "AB  -",
        "AU  -",
        "PY  -",
        "T2  -",
        "VL  -",
        "IS  -",
        "SP  -",
        "EP  -",
        "DO  -",
    ]
    for tag in expected_tags:
        assert tag in ris
