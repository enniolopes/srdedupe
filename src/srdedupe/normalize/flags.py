"""Quality and safety flags generation.

This module generates flags that indicate data quality issues and
safety concerns for decision making in the deduplication pipeline.
"""

from srdedupe.models.records import AuthorParsed, Flags

from ._helpers import (
    CORRECTED_REPUBLISHED_PUB_TYPES,
    ERRATUM_PUB_TYPES,
    ERRATUM_TITLE_RE,
    RETRACTION_PUB_TYPES,
    RETRACTION_TITLE_RE,
)


def generate_flags(
    doi_norm: str | None,
    pmid_norm: str | None,
    title_raw: str | None,
    authors_parsed: list[AuthorParsed] | None,
    year_norm: int | None,
    pages_unreliable: bool,
    publication_type: list[str] | None,
) -> Flags:
    """Generate quality and safety flags.

    Parameters
    ----------
    doi_norm : str | None
        Normalized DOI.
    pmid_norm : str | None
        Normalized PMID.
    title_raw : str | None
        Raw title.
    authors_parsed : list[AuthorParsed] | None
        Parsed authors.
    year_norm : int | None
        Normalized year.
    pages_unreliable : bool
        Whether pages are unreliable.
    publication_type : list[str] | None
        Publication type(s) from source record.

    Returns
    -------
    Flags
        Generated flags object.
    """
    # Identifier presence
    doi_present = doi_norm is not None
    pmid_present = pmid_norm is not None

    # Title flags
    title_missing = title_raw is None or not title_raw.strip()
    title_truncated = _is_title_truncated(title_raw) if title_raw else False

    # Author flags
    authors_missing = authors_parsed is None or len(authors_parsed) == 0
    authors_incomplete = _are_authors_incomplete(authors_parsed) if authors_parsed else False

    # Year flag
    year_missing = year_norm is None

    is_erratum, is_retraction, is_corrected = _detect_special_record_type(
        title_raw, publication_type
    )

    return Flags(
        doi_present=doi_present,
        pmid_present=pmid_present,
        title_missing=title_missing,
        title_truncated=title_truncated,
        authors_missing=authors_missing,
        authors_incomplete=authors_incomplete,
        year_missing=year_missing,
        pages_unreliable=pages_unreliable,
        is_erratum_notice=is_erratum,
        is_retraction_notice=is_retraction,
        is_corrected_republished=is_corrected,
        has_linked_citation=False,
    )


def _is_title_truncated(title: str) -> bool:
    """Check if title appears truncated."""
    return (
        "..." in title
        or title.endswith("…")
        or title.endswith("[...]")
        or "[truncated]" in title.lower()
    )


def _are_authors_incomplete(authors: list[AuthorParsed]) -> bool:
    """Check if author list appears incomplete."""
    if not authors:
        return False
    missing_family = sum(1 for a in authors if not a.family)
    return missing_family > len(authors) // 2


def _detect_special_record_type(
    title_raw: str | None,
    publication_type: list[str] | None,
) -> tuple[bool, bool, bool]:
    """Detect erratum, retraction, and corrected-republished records.

    Uses both publication type metadata and title pattern matching.

    Parameters
    ----------
    title_raw : str | None
        Raw title text.
    publication_type : list[str] | None
        Publication type(s) from source record.

    Returns
    -------
    tuple[bool, bool, bool]
        (is_erratum, is_retraction, is_corrected_republished)
    """
    is_erratum = False
    is_retraction = False
    is_corrected = False

    if publication_type:
        pub_types_lower = {pt.casefold() for pt in publication_type}

        if pub_types_lower & ERRATUM_PUB_TYPES:
            is_erratum = True
        if pub_types_lower & RETRACTION_PUB_TYPES:
            is_retraction = True
        if pub_types_lower & CORRECTED_REPUBLISHED_PUB_TYPES:
            is_corrected = True

    if title_raw:
        if not is_erratum and ERRATUM_TITLE_RE.search(title_raw):
            is_erratum = True
        if not is_retraction and RETRACTION_TITLE_RE.search(title_raw):
            is_retraction = True

    return is_erratum, is_retraction, is_corrected
