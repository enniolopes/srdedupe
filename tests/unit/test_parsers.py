"""Unit tests for all bibliographic format parsers.

Uses a contract-based approach: each parser declares sample content,
and generic parametrized tests verify universal parser guarantees.
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from srdedupe.parse.base import ParseResult
from srdedupe.parse.bibtex import parse_bibtex
from srdedupe.parse.endnote import parse_endnote
from srdedupe.parse.pubmed import parse_pubmed
from srdedupe.parse.ris import parse_ris
from srdedupe.parse.wos import parse_wos


def _parse(
    content: str,
    parser_fn: Callable[..., ParseResult],
    filename: str = "test.txt",
) -> ParseResult:
    """Parse inline content with given parser."""
    file_bytes = content.encode("utf-8")
    lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return parser_fn(Path(filename), lines, file_bytes)


# ---------------------------------------------------------------------------
# Parser contracts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParserContract:
    """Minimal test samples for a parser."""

    parser_fn: Callable[..., ParseResult]
    format_name: str
    single_record: str
    two_records: str
    malformed: str


RIS = ParserContract(
    parser_fn=parse_ris,
    format_name="ris",
    single_record=(
        "TY  - JOUR\n"
        "AU  - Smith, John\n"
        "TI  - Machine Learning\n"
        "AB  - Line one\n"
        "      Line two\n"
        "ER  - \n"
    ),
    two_records=("TY  - JOUR\nTI  - First\nER  - \n\nTY  - BOOK\nTI  - Second\nER  - \n"),
    malformed="TY  - JOUR\nTI  - Unclosed\n",
)

PUBMED = ParserContract(
    parser_fn=parse_pubmed,
    format_name="pubmed",
    single_record=("PMID- 12345\nTI  - Long title\n      continues here\nAU  - Smith J\n"),
    two_records=("PMID- 11111\nTI  - First\n\nPMID - 22222\nTI  - Second\n"),
    malformed="",
)

BIBTEX = ParserContract(
    parser_fn=parse_bibtex,
    format_name="bibtex",
    single_record=(
        "@article{Test2024,\n"
        "  AUTHOR = {Smith, {John A.}},\n"
        "  Title = {Outer {Inner {Deep}} Text},\n"
        "  abstract = {Line one\n"
        "Line two},\n"
        "  YeAr = {2024}\n"
        "}\n"
    ),
    two_records=(
        '@string{IEEE = "IEEE Press"}\n'
        '@preamble{"Stuff"}\n'
        "@comment{Ignored}\n"
        "@article{A,\n  title={First}\n}\n\n"
        "@book{B,\n  title={Second}\n}\n"
    ),
    malformed="@article{Bad,\n  title={Unclosed",
)

WOS = ParserContract(
    parser_fn=parse_wos,
    format_name="wos",
    single_record=("PT J\nAU Smith, JA\n   Doe, JB\nTI Test Article\nER\n"),
    two_records=("PT J\nTI First\nER\n\nPT S\nTI Second\nER\nEF\nPT J\nTI Ghost\nER\n"),
    malformed="",
)

ENDNOTE = ParserContract(
    parser_fn=parse_endnote,
    format_name="endnote_tagged",
    single_record=("%0 Journal Article\n%A Smith, John\n%T Long title\n   continues here\n"),
    two_records="%0 Article\n%T First\n\n%0 Book\n%T Second\n",
    malformed="",
)

ALL_CONTRACTS = [RIS, PUBMED, BIBTEX, WOS, ENDNOTE]


# ---------------------------------------------------------------------------
# Generic contract tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("c", ALL_CONTRACTS, ids=lambda c: c.format_name)
def test_empty_file(c: ParserContract) -> None:
    """Every parser returns 0 records and 0 errors on empty input."""
    records, _, errors = _parse("", c.parser_fn)
    assert len(records) == 0
    assert len(errors) == 0


@pytest.mark.unit
@pytest.mark.parametrize("c", ALL_CONTRACTS, ids=lambda c: c.format_name)
def test_single_record(c: ParserContract) -> None:
    """Every parser produces a valid record with correct schema fields."""
    records, _, errors = _parse(c.single_record, c.parser_fn)

    assert len(errors) == 0
    assert len(records) == 1

    rec = records[0]
    assert rec.schema_version == "1.0.0"
    assert rec.meta.source_format == c.format_name
    assert rec.meta.source_record_index == 0
    assert rec.rid is not None
    assert rec.record_digest.startswith("sha256:")
    assert rec.source_digest.startswith("sha256:")
    assert len(rec.raw.tags) > 0
    assert len(rec.raw.record_lines) > 0


@pytest.mark.unit
@pytest.mark.parametrize("c", ALL_CONTRACTS, ids=lambda c: c.format_name)
def test_continuation_lines(c: ParserContract) -> None:
    """Every parser preserves multi-line field values."""
    records, _, errors = _parse(c.single_record, c.parser_fn)

    assert len(errors) == 0
    assert len(records) == 1

    multi = [t for t in records[0].raw.tags if len(t.value_lines) > 1]
    assert len(multi) > 0, f"No multi-line tags for {c.format_name}"


@pytest.mark.unit
@pytest.mark.parametrize("c", ALL_CONTRACTS, ids=lambda c: c.format_name)
def test_multi_record(c: ParserContract) -> None:
    """Every parser handles multiple records with correct indexing."""
    records, _, errors = _parse(c.two_records, c.parser_fn)

    assert len(errors) == 0
    assert len(records) == 2
    assert records[0].meta.source_record_index == 0
    assert records[1].meta.source_record_index == 1
    assert records[0].rid != records[1].rid


@pytest.mark.unit
@pytest.mark.parametrize("c", ALL_CONTRACTS, ids=lambda c: c.format_name)
def test_deterministic(c: ParserContract) -> None:
    """Every parser produces identical RIDs on repeated calls."""
    records1, _, _ = _parse(c.single_record, c.parser_fn)
    records2, _, _ = _parse(c.single_record, c.parser_fn)

    assert len(records1) == 1
    assert records1[0].rid == records2[0].rid
    assert records1[0].record_digest == records2[0].record_digest


@pytest.mark.unit
@pytest.mark.parametrize(
    "c",
    [c for c in ALL_CONTRACTS if c.malformed],
    ids=lambda c: c.format_name,
)
def test_malformed_input(c: ParserContract) -> None:
    """Parsers with malformed samples produce warnings or errors, never crash."""
    records, warnings, errors = _parse(c.malformed, c.parser_fn)
    assert len(errors) > 0 or len(warnings) > 0
