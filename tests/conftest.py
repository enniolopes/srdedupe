"""Pytest configuration and fixtures for test suite."""

import sys
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

# Add src directory to path for imports
SRC_PATH = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))

from srdedupe.models import (  # noqa: E402
    SCHEMA_VERSION,
    AuthorParsed,
    Canon,
    CanonicalRecord,
    Flags,
    Keys,
    Meta,
    Raw,
    RawTag,
)

_MINIMAL_META = Meta(
    source_file="test.ris",
    source_format="ris",
    source_db=None,
    source_record_index=0,
    ingested_at="2024-01-01T00:00:00Z",
)

_MINIMAL_RAW = Raw(
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


@pytest.fixture
def make_record() -> Callable[..., CanonicalRecord]:
    """Factory for test records with minimal boilerplate.

    Only domain-relevant fields are configurable; structural
    fields (Meta, Raw) use frozen module-level defaults.
    """

    def _factory(
        rid: str = "rid_001",
        *,
        doi_norm: str | None = None,
        pmid_norm: str | None = None,
        title_raw: str | None = None,
        title_norm: str | None = None,
        title_key_strict: str | None = None,
        title_shingles: list[str] | None = None,
        title_truncated: bool = False,
        abstract_raw: str | None = None,
        authors_parsed: list[AuthorParsed] | None = None,
        first_author_sig: str | None = None,
        author_sig_strict: list[str] | None = None,
        year_norm: int | None = None,
        journal_full: str | None = None,
        journal_norm: str | None = None,
        volume: str | None = None,
        issue: str | None = None,
        pages_norm_long: str | None = None,
        page_first: str | None = None,
        page_last: str | None = None,
        pages_unreliable: bool = False,
        language: str | None = None,
        publication_type: list[str] | None = None,
        is_erratum_notice: bool = False,
        is_retraction_notice: bool = False,
        is_corrected_republished: bool = False,
        has_linked_citation: bool = False,
    ) -> CanonicalRecord:
        effective_title = title_raw or title_norm
        canon = replace(
            Canon.empty(),
            doi_norm=doi_norm,
            pmid_norm=pmid_norm,
            title_raw=effective_title,
            title_norm_basic=title_norm or effective_title,
            abstract_raw=abstract_raw,
            authors_parsed=authors_parsed,
            first_author_sig=first_author_sig,
            author_sig_strict=author_sig_strict,
            year_raw=str(year_norm) if year_norm else None,
            year_norm=year_norm,
            journal_full=journal_full or journal_norm,
            journal_norm=journal_norm,
            volume=volume,
            issue=issue,
            pages_raw=pages_norm_long,
            pages_norm_long=pages_norm_long,
            page_first=page_first,
            page_last=page_last,
            language=language,
            publication_type=publication_type,
        )
        keys = replace(
            Keys.empty(),
            title_key_strict=title_key_strict,
            title_shingles=title_shingles,
        )
        flags = replace(
            Flags.pre_normalization(),
            doi_present=doi_norm is not None,
            pmid_present=pmid_norm is not None,
            title_missing=effective_title is None,
            title_truncated=title_truncated,
            authors_missing=first_author_sig is None and authors_parsed is None,
            year_missing=year_norm is None,
            pages_unreliable=pages_unreliable,
            is_erratum_notice=is_erratum_notice,
            is_retraction_notice=is_retraction_notice,
            is_corrected_republished=is_corrected_republished,
            has_linked_citation=has_linked_citation,
        )
        return CanonicalRecord(
            schema_version=SCHEMA_VERSION,
            rid=rid,
            record_digest="sha256:test",
            source_digest=None,
            meta=_MINIMAL_META,
            raw=_MINIMAL_RAW,
            canon=canon,
            keys=keys,
            flags=flags,
            provenance={},
        )

    return _factory
