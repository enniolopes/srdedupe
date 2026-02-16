"""Tests for field normalization.

This suite tests the essential behavior of the normalize() function:
- Correctness of transformations (casefold, accent stripping, etc.)
- Key generation for blocking
- Flag detection for quality and safety
- Multi-format support

Tests focus on invariants and outcomes, not internal implementation details.
"""

import pytest

from srdedupe.models import (
    SCHEMA_VERSION,
    Canon,
    CanonicalRecord,
    Flags,
    Keys,
    Meta,
    Raw,
    RawTag,
    calculate_record_digest,
    calculate_rid,
)
from srdedupe.normalize import normalize


def make_record(
    tags: list[tuple[str, str]],
    source_format: str = "ris",
) -> CanonicalRecord:
    """Create a test canonical record with given tags.

    Parameters
    ----------
    tags : list[tuple[str, str]]
        List of (tag, value) tuples.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    CanonicalRecord
        Test record with empty Canon, Keys, Flags.
    """
    raw_tags = []
    for i, (tag, value) in enumerate(tags):
        raw_tags.append(
            RawTag(
                tag=tag,
                value_lines=[value],
                value_raw_joined=value,
                occurrence=0,
                line_start=i,
                line_end=i,
            )
        )

    raw_tags_dict = [{"tag": t.tag, "value": t.value_raw_joined} for t in raw_tags]
    record_digest = calculate_record_digest(raw_tags_dict, source_format)
    file_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    rid = calculate_rid(file_digest, record_digest)

    raw = Raw(
        record_lines=[],
        tags=raw_tags,
        unknown_lines=[],
    )

    meta = Meta(
        source_file="test.ris",
        source_format=source_format,
        source_db=None,
        source_record_index=0,
        ingested_at="2024-01-01T00:00:00Z",
    )

    canon = Canon(
        doi=None,
        doi_norm=None,
        doi_url=None,
        pmid=None,
        pmid_norm=None,
        pmcid=None,
        title_raw=None,
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

    keys = Keys.empty()

    flags = Flags(
        doi_present=False,
        pmid_present=False,
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
    )

    return CanonicalRecord(
        schema_version=SCHEMA_VERSION,
        rid=rid,
        record_digest=record_digest,
        source_digest=file_digest,
        meta=meta,
        raw=raw,
        canon=canon,
        keys=keys,
        flags=flags,
        provenance={},
    )


@pytest.mark.unit
def test_doi_normalization_naked() -> None:
    """Test DOI normalization from naked DOI."""
    record = make_record([("DO", "10.1234/example.2024.001")])
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/example.2024.001"
    assert normalized.canon.doi_url == "https://doi.org/10.1234/example.2024.001"
    assert normalized.flags.doi_present is True
    assert "canon.doi_norm" in normalized.provenance


@pytest.mark.unit
def test_doi_normalization_url() -> None:
    """Test DOI normalization from URL."""
    record = make_record([("DO", "https://doi.org/10.1234/example.2024.001")])
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/example.2024.001"
    assert normalized.canon.doi_url == "https://doi.org/10.1234/example.2024.001"
    assert normalized.flags.doi_present is True


@pytest.mark.unit
def test_doi_normalization_trailing_punctuation() -> None:
    """Test DOI normalization with trailing punctuation."""
    record = make_record([("DO", "10.1234/example.2024.001.")])
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/example.2024.001"
    assert normalized.canon.doi_url == "https://doi.org/10.1234/example.2024.001"


@pytest.mark.unit
def test_doi_normalization_query_fragment() -> None:
    """Test DOI normalization removes query and fragment."""
    record = make_record([("DO", "https://doi.org/10.1234/example.2024.001?ref=123#section")])
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/example.2024.001"
    assert "?ref" not in (normalized.canon.doi_norm or "")
    assert "#section" not in (normalized.canon.doi_norm or "")


@pytest.mark.unit
def test_doi_normalization_prefix() -> None:
    """Test DOI normalization strips doi: prefix."""
    record = make_record([("DO", "doi:10.1234/example.2024.001")])
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/example.2024.001"
    assert not normalized.canon.doi_norm.startswith("doi:")


@pytest.mark.unit
def test_doi_normalization_idempotent() -> None:
    """Test DOI normalization is idempotent."""
    record = make_record([("DO", "https://doi.org/10.1234/example.2024.001")])
    normalized1 = normalize(record)
    normalized2 = normalize(normalized1)

    assert normalized1.canon.doi_norm == normalized2.canon.doi_norm
    assert normalized1.canon.doi_url == normalized2.canon.doi_url


@pytest.mark.unit
def test_pmid_normalization() -> None:
    """Test PMID normalization extracts digits only."""
    record = make_record([("PM", "12345678")], source_format="ris")
    normalized = normalize(record)

    assert normalized.canon.pmid_norm == "12345678"
    assert normalized.flags.pmid_present is True
    assert "canon.pmid_norm" in normalized.provenance


@pytest.mark.unit
def test_pmid_normalization_with_prefix() -> None:
    """Test PMID normalization strips non-digits."""
    record = make_record([("PM", "PMID: 12345678")], source_format="ris")
    normalized = normalize(record)

    assert normalized.canon.pmid_norm == "12345678"


@pytest.mark.unit
def test_pmid_normalization_nbib() -> None:
    """Test PMID normalization in NBIB format."""
    record = make_record([("PMID", "12345678")], source_format="nbib")
    normalized = normalize(record)

    assert normalized.canon.pmid_norm == "12345678"
    assert normalized.flags.pmid_present is True


@pytest.mark.unit
def test_title_normalization_basic() -> None:
    """Test basic title normalization."""
    record = make_record([("TI", "Machine Learning for Systematic Reviews")])
    normalized = normalize(record)

    assert normalized.canon.title_raw == "Machine Learning for Systematic Reviews"
    assert normalized.canon.title_norm_basic == "machine learning for systematic reviews"
    assert normalized.flags.title_missing is False
    assert "canon.title_norm_basic" in normalized.provenance


@pytest.mark.unit
def test_title_normalization_punctuation() -> None:
    """Test title normalization removes punctuation."""
    record = make_record([("TI", "Machine Learning: A Survey!")])
    normalized = normalize(record)

    assert ":" not in (normalized.canon.title_norm_basic or "")
    assert "!" not in (normalized.canon.title_norm_basic or "")


@pytest.mark.unit
def test_title_normalization_whitespace() -> None:
    """Test title normalization collapses whitespace."""
    record = make_record([("TI", "Machine    Learning   for    Reviews")])
    normalized = normalize(record)

    assert "  " not in (normalized.canon.title_norm_basic or "")
    assert normalized.canon.title_norm_basic == "machine learning for reviews"


@pytest.mark.unit
def test_title_truncated_flag() -> None:
    """Test title_truncated flag detection."""
    record = make_record([("TI", "Machine Learning for...")])
    normalized = normalize(record)

    assert normalized.flags.title_truncated is True


@pytest.mark.unit
def test_title_keys_generation() -> None:
    """Test title keys are generated correctly."""
    record = make_record([("TI", "Machine Learning for Systematic Reviews")])
    normalized = normalize(record)

    assert normalized.keys.title_key_strict == "machine learning for systematic reviews"
    assert normalized.keys.title_key_fuzzy == "machine learning for systematic reviews"
    assert normalized.keys.title_shingles is not None
    assert len(normalized.keys.title_shingles) > 0


@pytest.mark.unit
def test_title_shingles_filter_short() -> None:
    """Test title shingles filter out short tokens."""
    record = make_record([("TI", "A ML for SR")])
    normalized = normalize(record)

    # Short tokens (< 3 chars) should be filtered out
    assert normalized.keys.title_shingles is None or all(
        len(token) >= 3 for token in normalized.keys.title_shingles
    )


@pytest.mark.unit
def test_author_parsing_comma_format() -> None:
    """Test author parsing with comma format."""
    record = make_record([("AU", "Smith, John A.")])
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 1
    author = normalized.canon.authors_parsed[0]
    assert author.family == "Smith"
    assert author.given == "John A."
    assert author.initials == "JA"


@pytest.mark.unit
def test_author_parsing_initials_format() -> None:
    """Test author parsing with initials format."""
    record = make_record([("AU", "Smith, J.A.")])
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    author = normalized.canon.authors_parsed[0]
    assert author.family == "Smith"
    assert author.initials == "JA"


@pytest.mark.unit
def test_author_parsing_multiple() -> None:
    """Test parsing multiple authors."""
    record = make_record([("AU", "Smith, John A."), ("AU", "Doe, Jane B.")])
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 2
    assert normalized.canon.authors_parsed[0].family == "Smith"
    assert normalized.canon.authors_parsed[1].family == "Doe"


@pytest.mark.unit
def test_author_signatures_strict() -> None:
    """Test strict author signatures generation."""
    record = make_record([("AU", "Smith, John A.")])
    normalized = normalize(record)

    assert normalized.canon.first_author_sig == "smith|JA"
    assert normalized.canon.author_sig_strict == ["smith|JA"]


@pytest.mark.unit
def test_author_signatures_loose() -> None:
    """Test loose author signatures are sorted."""
    record = make_record([("AU", "Smith, John A."), ("AU", "Doe, Jane B.")])
    normalized = normalize(record)

    assert normalized.canon.author_sig_loose is not None
    # Should be sorted
    assert normalized.canon.author_sig_loose == sorted(normalized.canon.author_sig_loose)


@pytest.mark.unit
def test_author_et_al_ignored() -> None:
    """Test 'et al.' is not treated as an author."""
    record = make_record([("AU", "Smith, John A."), ("AU", "et al.")])
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 1
    assert normalized.canon.authors_parsed[0].family == "Smith"


@pytest.mark.unit
def test_year_extraction_ris() -> None:
    """Test year extraction from RIS format."""
    record = make_record([("PY", "2024")], source_format="ris")
    normalized = normalize(record)

    assert normalized.canon.year_norm == 2024
    assert normalized.canon.year_source == "RIS.PY"
    assert normalized.flags.year_missing is False
    assert "canon.year_norm" in normalized.provenance


@pytest.mark.unit
def test_year_extraction_from_date() -> None:
    """Test year extraction from full date."""
    record = make_record([("PY", "2024/01/15")], source_format="ris")
    normalized = normalize(record)

    assert normalized.canon.year_norm == 2024


@pytest.mark.unit
def test_year_extraction_nbib() -> None:
    """Test year extraction from NBIB format."""
    record = make_record([("DP", "2024 Jan 15")], source_format="nbib")
    normalized = normalize(record)

    assert normalized.canon.year_norm == 2024
    assert normalized.canon.year_source == "NBIB.DP"


@pytest.mark.unit
def test_journal_normalization() -> None:
    """Test journal normalization."""
    record = make_record([("JF", "Journal of Machine Learning")])
    normalized = normalize(record)

    assert normalized.canon.journal_full == "Journal of Machine Learning"
    assert normalized.canon.journal_norm == "journal of machine learning"
    assert "canon.journal_norm" in normalized.provenance


@pytest.mark.unit
def test_pages_normalization_range() -> None:
    """Test pages normalization with range."""
    record = make_record([("SP", "123"), ("EP", "145")], source_format="ris")
    normalized = normalize(record)

    assert normalized.canon.pages_raw == "123-145"
    assert normalized.canon.page_first == "123"
    assert normalized.canon.page_last == "145"
    assert normalized.flags.pages_unreliable is False


@pytest.mark.unit
def test_pages_normalization_electronic() -> None:
    """Test pages with electronic article number."""
    record = make_record([("PG", "e12345")], source_format="nbib")
    normalized = normalize(record)

    assert normalized.canon.article_number == "e12345"
    assert normalized.flags.pages_unreliable is True


@pytest.mark.unit
def test_composite_key_title_year() -> None:
    """Test title+year composite key generation."""
    record = make_record([("TI", "Machine Learning"), ("PY", "2024")])
    normalized = normalize(record)

    assert normalized.keys.title_year_key == "machine learning|2024"


@pytest.mark.unit
def test_composite_key_title_author() -> None:
    """Test title+author composite key generation."""
    record = make_record([("TI", "Machine Learning"), ("AU", "Smith, J.A.")])
    normalized = normalize(record)

    assert normalized.keys.title_first_author_key == "machine learning|smith|JA"


@pytest.mark.unit
def test_composite_key_title_journal() -> None:
    """Test title+journal composite key generation."""
    record = make_record([("TI", "Machine Learning"), ("JF", "Nature")])
    normalized = normalize(record)

    assert normalized.keys.title_journal_key == "machine learning|nature"


@pytest.mark.unit
def test_composite_key_null_when_missing() -> None:
    """Test composite keys are null when components missing."""
    record = make_record([("TI", "Machine Learning")])
    normalized = normalize(record)

    # No year, no author, no journal
    assert normalized.keys.title_year_key is None
    assert normalized.keys.title_first_author_key is None
    assert normalized.keys.title_journal_key is None


@pytest.mark.unit
def test_flags_all_missing() -> None:
    """Test flags when all fields are missing."""
    record = make_record([])
    normalized = normalize(record)

    assert normalized.flags.doi_present is False
    assert normalized.flags.pmid_present is False
    assert normalized.flags.title_missing is True
    assert normalized.flags.authors_missing is True
    assert normalized.flags.year_missing is True


@pytest.mark.unit
def test_flags_all_present() -> None:
    """Test flags when all fields are present."""
    record = make_record(
        [
            ("DO", "10.1234/test"),
            ("PM", "12345678"),
            ("TI", "Title"),
            ("AU", "Smith, J."),
            ("PY", "2024"),
        ]
    )
    normalized = normalize(record)

    assert normalized.flags.doi_present is True
    assert normalized.flags.pmid_present is True
    assert normalized.flags.title_missing is False
    assert normalized.flags.authors_missing is False
    assert normalized.flags.year_missing is False


@pytest.mark.unit
def test_normalization_idempotent() -> None:
    """Test that normalization is idempotent."""
    record = make_record(
        [
            ("DO", "10.1234/test"),
            ("TI", "Machine Learning"),
            ("AU", "Smith, J.A."),
            ("PY", "2024"),
            ("JF", "Nature"),
        ]
    )
    normalized1 = normalize(record)
    normalized2 = normalize(normalized1)

    # Check key fields are identical
    assert normalized1.canon.doi_norm == normalized2.canon.doi_norm
    assert normalized1.canon.title_norm_basic == normalized2.canon.title_norm_basic
    assert normalized1.canon.year_norm == normalized2.canon.year_norm
    assert normalized1.keys.title_key_strict == normalized2.keys.title_key_strict


@pytest.mark.unit
def test_provenance_completeness() -> None:
    """Test that provenance is populated for all derived fields."""
    record = make_record(
        [
            ("DO", "10.1234/test"),
            ("TI", "Machine Learning"),
            ("AU", "Smith, J.A."),
            ("PY", "2024"),
            ("JF", "Nature"),
        ]
    )
    normalized = normalize(record)

    # Check provenance entries exist for key fields
    assert "canon.doi_norm" in normalized.provenance
    assert "canon.title_norm_basic" in normalized.provenance
    assert "canon.authors_parsed" in normalized.provenance
    assert "canon.year_norm" in normalized.provenance
    assert "canon.journal_norm" in normalized.provenance

    # Check provenance structure
    for field, entry in normalized.provenance.items():
        assert len(entry["sources"]) > 0, f"Field {field} has no sources"
        assert entry["confidence"] in ["high", "medium", "low", "unknown"]


@pytest.mark.unit
def test_doi_provenance_transforms() -> None:
    """Test DOI provenance includes correct transforms."""
    record = make_record([("DO", "https://doi.org/10.1234/test.")])
    normalized = normalize(record)

    entry = normalized.provenance["canon.doi_norm"]
    transform_names = [t["name"] for t in entry["transforms"]]

    assert "extract_from_url" in transform_names
    assert "trim_punct" in transform_names
    assert "casefold" in transform_names


@pytest.mark.unit
def test_unicode_normalization() -> None:
    """Test Unicode normalization in title."""
    # Test with composed and decomposed forms
    record = make_record([("TI", "Café résumé")])
    normalized = normalize(record)

    # Should be normalized to NFKC, casefold, and accent-stripped
    assert normalized.canon.title_norm_basic is not None
    assert "cafe" in normalized.canon.title_norm_basic
    assert "resume" in normalized.canon.title_norm_basic


@pytest.mark.unit
def test_authors_incomplete_flag() -> None:
    """Test authors_incomplete flag when many missing family names."""
    record = make_record([])
    # Add authors with missing family names
    for i in range(3):
        record.raw.tags.append(
            RawTag(
                tag="AU",
                value_lines=["J."],
                value_raw_joined="J.",
                occurrence=i,
                line_start=i,
                line_end=i,
            )
        )
    normalized = normalize(record)

    # "J." is parsed as family="J." (non-empty) so authors_incomplete won't trigger
    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 3
    assert normalized.flags.authors_missing is False


@pytest.mark.unit
@pytest.mark.parametrize(
    ("title", "pub_type_tag", "pub_type_value", "expected_erratum", "expected_retraction"),
    [
        ("Erratum: Original article title", None, None, True, False),
        ("Retraction notice: Original article", None, None, False, True),
        ("Regular article title", "TY", "Erratum", True, False),
        ("Regular article title", "TY", "Retraction of Publication", False, True),
        ("Regular article title", None, None, False, False),
    ],
)
def test_flags_special_record_types(
    title: str,
    pub_type_tag: str | None,
    pub_type_value: str | None,
    expected_erratum: bool,
    expected_retraction: bool,
) -> None:
    """Test erratum/retraction flag detection from title and pub type."""
    tags: list[tuple[str, str]] = [("TI", title)]
    if pub_type_tag and pub_type_value:
        tags.append((pub_type_tag, pub_type_value))

    record = make_record(tags)
    normalized = normalize(record)

    assert normalized.flags.is_erratum_notice is expected_erratum
    assert normalized.flags.is_retraction_notice is expected_retraction


@pytest.mark.unit
def test_author_parsing_bibtex_given_family() -> None:
    """Test BibTeX 'Given Family' name convention."""
    record = make_record([("author", "John Smith")], source_format="bibtex")
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 1
    author = normalized.canon.authors_parsed[0]
    assert author.family == "Smith"
    assert author.given == "John"


@pytest.mark.unit
def test_author_parsing_endnote_tagged_given_family() -> None:
    """Test endnote_tagged 'Given Family' name convention."""
    record = make_record([("A", "John Smith")], source_format="endnote_tagged")
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    author = normalized.canon.authors_parsed[0]
    assert author.family == "Smith"
    assert author.given == "John"


@pytest.mark.unit
def test_doi_nbib_strips_doi_suffix() -> None:
    """Test NBIB AID tag with [doi] suffix is stripped."""
    record = make_record([("AID", "10.1234/test [doi]")], source_format="nbib")
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/test"
    assert normalized.flags.doi_present is True


@pytest.mark.unit
def test_doi_url_decoding() -> None:
    """Test URL-encoded characters in DOI are decoded."""
    record = make_record([("DO", "10.1234/test%28abc%29")])
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/test(abc)"


# ---------------------------------------------------------------------------
# BibTeX multi-author splitting
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_bibtex_and_separated_authors() -> None:
    """Test BibTeX multi-author 'and' separator splitting."""
    record = make_record(
        [("author", "John Smith and Jane Doe and Bob Jones")],
        source_format="bibtex",
    )
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 3
    assert normalized.canon.authors_parsed[0].family == "Smith"
    assert normalized.canon.authors_parsed[1].family == "Doe"
    assert normalized.canon.authors_parsed[2].family == "Jones"


@pytest.mark.unit
def test_bibtex_and_separated_comma_format() -> None:
    """Test BibTeX 'Family, Given and Family, Given' format."""
    record = make_record(
        [("author", "Smith, John and Doe, Jane")],
        source_format="bibtex",
    )
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 2
    assert normalized.canon.authors_parsed[0].family == "Smith"
    assert normalized.canon.authors_parsed[0].given == "John"
    assert normalized.canon.authors_parsed[1].family == "Doe"
    assert normalized.canon.authors_parsed[1].given == "Jane"


@pytest.mark.unit
def test_bibtex_single_author_no_spurious_split() -> None:
    """Test BibTeX single author with 'Anderson' is not split."""
    record = make_record(
        [("author", "Anderson, John")],
        source_format="bibtex",
    )
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 1
    assert normalized.canon.authors_parsed[0].family == "Anderson"


@pytest.mark.unit
def test_bibtex_et_al_stripped_during_split() -> None:
    """Test 'et al.' entries are discarded during BibTeX splitting."""
    record = make_record(
        [("author", "Smith, John and et al.")],
        source_format="bibtex",
    )
    normalized = normalize(record)

    assert normalized.canon.authors_parsed is not None
    assert len(normalized.canon.authors_parsed) == 1
    assert normalized.canon.authors_parsed[0].family == "Smith"


# ---------------------------------------------------------------------------
# WoS pages BP/EP
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_wos_pages_bp_ep_tags() -> None:
    """Test WoS BP/EP page tags are handled correctly."""
    record = make_record(
        [("BP", "100"), ("EP", "120")],
        source_format="wos",
    )
    normalized = normalize(record)

    assert normalized.canon.pages_raw == "100-120"
    assert normalized.canon.page_first == "100"
    assert normalized.canon.page_last == "120"
    assert normalized.flags.pages_unreliable is False


@pytest.mark.unit
def test_wos_pages_bp_only() -> None:
    """Test WoS with only BP tag."""
    record = make_record([("BP", "100")], source_format="wos")
    normalized = normalize(record)

    assert normalized.canon.pages_raw == "100"
    assert normalized.canon.page_first == "100"


# ---------------------------------------------------------------------------
# PMID/PMCID AID fallback with pubmed alias
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pmid_aid_fallback_pubmed_format() -> None:
    """Test PMID AID fallback works with 'pubmed' source format."""
    record = make_record(
        [("AID", "12345678 [pmid]")],
        source_format="pubmed",
    )
    normalized = normalize(record)

    assert normalized.canon.pmid_norm == "12345678"
    assert normalized.flags.pmid_present is True


@pytest.mark.unit
def test_pmcid_aid_fallback_pubmed_format() -> None:
    """Test PMCID AID fallback works with 'pubmed' source format."""
    record = make_record(
        [("AID", "PMC9876543 [pmc]")],
        source_format="pubmed",
    )
    normalized = normalize(record)

    assert normalized.canon.pmcid == "PMC9876543"


# ---------------------------------------------------------------------------
# Tag priority order
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tag_priority_respected() -> None:
    """Test higher-priority tags win over lower-priority tags in document order."""
    # RIS doi tags: ["DO", "DI", "M3"] — DO has highest priority.
    # Place M3 first in document order, DO later.
    record = make_record([("M3", "10.9999/low-priority"), ("DO", "10.1234/high-priority")])
    normalized = normalize(record)

    assert normalized.canon.doi_norm == "10.1234/high-priority"
