"""Deterministic normalization and key generation for canonical records.

This module orchestrates field normalization, key generation, and flag
computation for bibliographic records. All functions are pure, deterministic,
and locale-independent.
"""

from typing import Any

from srdedupe.models.records import Canon, CanonicalRecord

from ._fields import (
    extract_other_fields,
    extract_year,
    normalize_authors,
    normalize_doi,
    normalize_journal,
    normalize_pages,
    normalize_pmid_pmcid,
    normalize_title,
)
from .flags import generate_flags
from .keys import generate_keys


def normalize(record: CanonicalRecord) -> CanonicalRecord:
    """Apply normalization to a canonical record.

    This is the main entry point for normalization. It populates
    all canon.*, keys.*, flags.*, and provenance.* fields deterministically.

    Parameters
    ----------
    record : CanonicalRecord
        Input record with parsed raw data.

    Returns
    -------
    CanonicalRecord
        Record with populated normalized fields, keys, flags, and provenance.

    Notes
    -----
    This function is idempotent: running it twice on the same record produces
    identical output.
    """
    raw_tags = record.raw.tags
    source_format = record.meta.source_format

    # Initialize provenance dictionary
    provenance: dict[str, Any] = {}

    # Normalize DOI
    doi_result, doi_prov = normalize_doi(raw_tags, source_format)
    provenance.update(doi_prov)

    # Normalize PMID/PMCID
    pmid_result, pmid_prov = normalize_pmid_pmcid(raw_tags, source_format)
    provenance.update(pmid_prov)

    # Normalize title
    title_result, title_prov = normalize_title(raw_tags, source_format)
    provenance.update(title_prov)

    # Normalize authors
    authors_result, authors_prov = normalize_authors(raw_tags, source_format)
    provenance.update(authors_prov)

    # Extract year
    year_result, year_prov = extract_year(raw_tags, source_format)
    provenance.update(year_prov)

    # Normalize journal
    journal_result, journal_prov = normalize_journal(raw_tags, source_format)
    provenance.update(journal_prov)

    # Normalize pages
    pages_result, pages_prov = normalize_pages(raw_tags, source_format)
    provenance.update(pages_prov)

    # Extract other fields
    other_fields = extract_other_fields(raw_tags, source_format)

    # Generate keys
    keys = generate_keys(
        title_result.norm_basic,
        year_result.norm,
        authors_result.first_sig,
        journal_result.norm,
    )

    # Generate flags
    flags = generate_flags(
        doi_norm=doi_result.norm,
        pmid_norm=pmid_result.pmid_norm,
        title_raw=title_result.raw,
        authors_parsed=authors_result.parsed,
        year_norm=year_result.norm,
        pages_unreliable=pages_result.unreliable,
        publication_type=other_fields.publication_type,
    )

    # Build updated Canon
    canon = Canon(
        doi=doi_result.raw,
        doi_norm=doi_result.norm,
        doi_url=doi_result.url,
        pmid=pmid_result.pmid_raw,
        pmid_norm=pmid_result.pmid_norm,
        pmcid=pmid_result.pmcid,
        title_raw=title_result.raw,
        title_norm_basic=title_result.norm_basic,
        abstract_raw=other_fields.abstract_raw,
        abstract_norm_basic=other_fields.abstract_norm,
        authors_raw=authors_result.raw,
        authors_parsed=authors_result.parsed,
        first_author_sig=authors_result.first_sig,
        author_sig_strict=authors_result.sig_strict,
        author_sig_loose=authors_result.sig_loose,
        year_raw=year_result.raw,
        year_norm=year_result.norm,
        year_source=year_result.source,
        journal_full=journal_result.full,
        journal_abbrev=journal_result.abbrev,
        journal_norm=journal_result.norm,
        volume=other_fields.volume,
        issue=other_fields.issue,
        pages_raw=pages_result.raw,
        pages_norm_long=pages_result.norm_long,
        page_first=pages_result.first,
        page_last=pages_result.last,
        article_number=pages_result.article_number,
        language=other_fields.language,
        publication_type=other_fields.publication_type,
    )

    return CanonicalRecord(
        schema_version=record.schema_version,
        rid=record.rid,
        record_digest=record.record_digest,
        source_digest=record.source_digest,
        meta=record.meta,
        raw=record.raw,
        canon=canon,
        keys=keys,
        flags=flags,
        provenance=provenance,
    )
