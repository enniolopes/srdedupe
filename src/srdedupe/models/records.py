"""Canonical record data models for srdedupe.

This module defines the internal schema for bibliographic reference records.
All downstream modules consume records in this canonical format.
"""

from dataclasses import asdict, dataclass, fields
from typing import Any

# Schema version constant
SCHEMA_VERSION = "1.0.0"


@dataclass(frozen=True)
class RawTag:
    """Raw tag preserving original extraction.

    Attributes
    ----------
    tag : str
        Tag name (e.g., 'TI', 'AU', 'DO').
    value_lines : list[str]
        Raw value fragments (e.g., continuation lines).
    value_raw_joined : str
        Joined representation using deterministic join rule.
    occurrence : int
        0-based occurrence count for repeated tags.
    line_start : int
        0-based line number where tag starts in file.
    line_end : int
        0-based line number where tag ends in file.
    """

    tag: str
    value_lines: list[str]
    value_raw_joined: str
    occurrence: int
    line_start: int
    line_end: int


@dataclass(frozen=True)
class Raw:
    """Lossless capture of original record data.

    Attributes
    ----------
    record_lines : list[str]
        Exact original lines for this record (no newline chars), in order.
    tags : list[RawTag]
        List of raw tag objects preserving order and repeats.
    unknown_lines : list[str]
        Lines that cannot be attributed to a tag safely.
    """

    record_lines: list[str]
    tags: list[RawTag]
    unknown_lines: list[str]


@dataclass(frozen=True)
class Meta:
    """Run-independent metadata about record origin.

    Attributes
    ----------
    source_file : str
        Original filename (basename only).
    source_format : str
        Format of source file ('ris', 'nbib', or 'unknown').
    source_db : str | None
        Source database if known (e.g., 'pubmed', 'embase', 'wos').
    source_record_index : int
        0-based position in source file.
    ingested_at : str
        ISO8601 timestamp (UTC) of ingestion.
    source_file_mtime : str | None
        ISO8601 timestamp of source file modification time.
    source_file_size_bytes : int | None
        Size of source file in bytes.
    parser_version : str | None
        Version or SHA of parser module.
    """

    source_file: str
    source_format: str
    source_db: str | None
    source_record_index: int
    ingested_at: str
    source_file_mtime: str | None = None
    source_file_size_bytes: int | None = None
    parser_version: str | None = None


@dataclass(frozen=True)
class AuthorParsed:
    """Structured author representation.

    Attributes
    ----------
    family : str | None
        Family/last name.
    given : str | None
        Given/first name(s).
    initials : str | None
        Initials.
    suffix : str | None
        Suffix (e.g., 'Jr.', 'III').
    raw : str
        Original author string as extracted.
    """

    family: str | None
    given: str | None
    initials: str | None
    suffix: str | None
    raw: str


@dataclass(frozen=True)
class Canon:
    """Canonical normalized fields.

    All fields exist but may be None if missing or not yet computed.

    Attributes
    ----------
    doi : str | None
        Raw extracted DOI.
    doi_norm : str | None
        Normalized DOI identifier.
    doi_url : str | None
        Canonical display URL.
    pmid : str | None
        PubMed ID (raw).
    pmid_norm : str | None
        Normalized PMID.
    pmcid : str | None
        PubMed Central ID.
    title_raw : str | None
        Original title.
    title_norm_basic : str | None
        Normalized title.
    abstract_raw : str | None
        Original abstract.
    abstract_norm_basic : str | None
        Normalized abstract.
    authors_raw : list[str] | None
        Authors as captured.
    authors_parsed : list[AuthorParsed] | None
        Structured author objects.
    first_author_sig : str | None
        First author signature.
    author_sig_strict : list[str] | None
        Strict author signatures.
    author_sig_loose : list[str] | None
        Loose author signatures.
    year_raw : str | None
        Raw year string.
    year_norm : int | None
        Normalized year.
    year_source : str | None
        Source field hint.
    journal_full : str | None
        Full journal name.
    journal_abbrev : str | None
        Abbreviated journal name.
    journal_norm : str | None
        Normalized journal name.
    volume : str | None
        Volume.
    issue : str | None
        Issue number.
    pages_raw : str | None
        Raw page string.
    pages_norm_long : str | None
        Normalized long-form pagination.
    page_first : str | None
        First page.
    page_last : str | None
        Last page.
    article_number : str | None
        Article number.
    language : str | None
        Publication language.
    publication_type : list[str] | None
        Publication type(s).
    """

    doi: str | None
    doi_norm: str | None
    doi_url: str | None
    pmid: str | None
    pmid_norm: str | None
    pmcid: str | None
    title_raw: str | None
    title_norm_basic: str | None
    abstract_raw: str | None
    abstract_norm_basic: str | None
    authors_raw: list[str] | None
    authors_parsed: list[AuthorParsed] | None
    first_author_sig: str | None
    author_sig_strict: list[str] | None
    author_sig_loose: list[str] | None
    year_raw: str | None
    year_norm: int | None
    year_source: str | None
    journal_full: str | None
    journal_abbrev: str | None
    journal_norm: str | None
    volume: str | None
    issue: str | None
    pages_raw: str | None
    pages_norm_long: str | None
    page_first: str | None
    page_last: str | None
    article_number: str | None
    language: str | None
    publication_type: list[str] | None

    @classmethod
    def empty(cls) -> "Canon":
        """Create Canon with all fields as None.

        Returns
        -------
        Canon
            Canon instance with all fields set to None.
        """
        return cls(**{f.name: None for f in fields(cls)})


@dataclass(frozen=True)
class Keys:
    """Derived keys for matching.

    Attributes
    ----------
    title_key_strict : str | None
        Strict title key.
    title_year_key : str | None
        Title + year composite key.
    title_first_author_key : str | None
        Title + first author composite key.
    title_journal_key : str | None
        Title + journal composite key.
    title_key_fuzzy : str | None
        Fuzzy title key.
    title_shingles : list[str] | None
        Title shingles/tokens.
    title_minhash : str | None
        MinHash signature.
    title_simhash : str | None
        SimHash signature.
    """

    title_key_strict: str | None
    title_year_key: str | None
    title_first_author_key: str | None
    title_journal_key: str | None
    title_key_fuzzy: str | None
    title_shingles: list[str] | None
    title_minhash: str | None
    title_simhash: str | None

    @classmethod
    def empty(cls) -> "Keys":
        """Create Keys with all fields as None.

        Returns
        -------
        Keys
            Keys instance with all fields set to None.
        """
        return cls(**{f.name: None for f in fields(cls)})


@dataclass(frozen=True)
class Flags:
    """Quality and safety flags for decision gating.

    Attributes
    ----------
    doi_present : bool
        True if DOI is present.
    pmid_present : bool
        True if PMID is present.
    title_missing : bool
        True if title is missing.
    title_truncated : bool
        True if title appears truncated.
    authors_missing : bool
        True if no authors found.
    authors_incomplete : bool
        True if author list appears incomplete.
    year_missing : bool
        True if year is missing.
    pages_unreliable : bool
        True if pagination unreliable.
    is_erratum_notice : bool
        True if record is erratum notice.
    is_retraction_notice : bool
        True if record is retraction notice.
    is_corrected_republished : bool
        True if record is correction/republication.
    has_linked_citation : bool
        True if record has linked citations.
    """

    doi_present: bool
    pmid_present: bool
    title_missing: bool
    title_truncated: bool
    authors_missing: bool
    authors_incomplete: bool
    year_missing: bool
    pages_unreliable: bool
    is_erratum_notice: bool
    is_retraction_notice: bool
    is_corrected_republished: bool
    has_linked_citation: bool

    @classmethod
    def pre_normalization(cls) -> "Flags":
        """Create Flags for pre-normalization state.

        Returns
        -------
        Flags
            Flags with missing data indicators set.
        """
        return cls(
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


@dataclass(frozen=True)
class CanonicalRecord:
    """Complete canonical record with all required sections.

    Attributes
    ----------
    schema_version : str
        Schema version (semver).
    rid : str
        Record identifier (UUIDv5).
    record_digest : str
        Content fingerprint (sha256).
    source_digest : str | None
        Source file digest (sha256).
    meta : Meta
        Record metadata.
    raw : Raw
        Raw extracted data.
    canon : Canon
        Canonical normalized fields.
    keys : Keys
        Derived matching keys.
    flags : Flags
        Quality and safety flags.
    provenance : dict[str, Any]
        Field provenance tracking as plain dicts.
    """

    schema_version: str
    rid: str
    record_digest: str
    source_digest: str | None
    meta: Meta
    raw: Raw
    canon: Canon
    keys: Keys
    flags: Flags
    provenance: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert record to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation compatible with JSON schema.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CanonicalRecord":
        """Reconstruct a CanonicalRecord from a dictionary.

        Parameters
        ----------
        data : dict[str, Any]
            Dictionary (e.g. from JSON) with record fields.

        Returns
        -------
        CanonicalRecord
            Reconstructed record.
        """
        raw_data = data.get("raw", {})
        raw = Raw(
            record_lines=raw_data.get("record_lines", []),
            tags=[
                RawTag(
                    tag=t["tag"],
                    value_lines=t.get("value_lines", []),
                    value_raw_joined=t.get("value_raw_joined", ""),
                    occurrence=t.get("occurrence", 0),
                    line_start=t.get("line_start", 0),
                    line_end=t.get("line_end", 0),
                )
                for t in raw_data.get("tags", [])
            ],
            unknown_lines=raw_data.get("unknown_lines", []),
        )

        meta_data = data.get("meta", {})
        meta = Meta(
            source_file=meta_data.get("source_file", ""),
            source_format=meta_data.get("source_format", "unknown"),
            source_db=meta_data.get("source_db"),
            source_record_index=meta_data.get("source_record_index", 0),
            ingested_at=meta_data.get("ingested_at", ""),
            source_file_mtime=meta_data.get("source_file_mtime"),
            source_file_size_bytes=meta_data.get("source_file_size_bytes"),
            parser_version=meta_data.get("parser_version"),
        )

        canon_data = data.get("canon", {})
        authors_parsed = None
        if canon_data.get("authors_parsed"):
            authors_parsed = [
                AuthorParsed(
                    family=a.get("family"),
                    given=a.get("given"),
                    initials=a.get("initials"),
                    suffix=a.get("suffix"),
                    raw=a.get("raw", ""),
                )
                for a in canon_data["authors_parsed"]
            ]

        canon = Canon(
            doi=canon_data.get("doi"),
            doi_norm=canon_data.get("doi_norm"),
            doi_url=canon_data.get("doi_url"),
            pmid=canon_data.get("pmid"),
            pmid_norm=canon_data.get("pmid_norm"),
            pmcid=canon_data.get("pmcid"),
            title_raw=canon_data.get("title_raw"),
            title_norm_basic=canon_data.get("title_norm_basic"),
            abstract_raw=canon_data.get("abstract_raw"),
            abstract_norm_basic=canon_data.get("abstract_norm_basic"),
            authors_raw=canon_data.get("authors_raw"),
            authors_parsed=authors_parsed,
            first_author_sig=canon_data.get("first_author_sig"),
            author_sig_strict=canon_data.get("author_sig_strict"),
            author_sig_loose=canon_data.get("author_sig_loose"),
            year_raw=canon_data.get("year_raw"),
            year_norm=canon_data.get("year_norm"),
            year_source=canon_data.get("year_source"),
            journal_full=canon_data.get("journal_full"),
            journal_abbrev=canon_data.get("journal_abbrev"),
            journal_norm=canon_data.get("journal_norm"),
            volume=canon_data.get("volume"),
            issue=canon_data.get("issue"),
            pages_raw=canon_data.get("pages_raw"),
            pages_norm_long=canon_data.get("pages_norm_long"),
            page_first=canon_data.get("page_first"),
            page_last=canon_data.get("page_last"),
            article_number=canon_data.get("article_number"),
            language=canon_data.get("language"),
            publication_type=canon_data.get("publication_type"),
        )

        keys_data = data.get("keys", {})
        record_keys = Keys(
            title_key_strict=keys_data.get("title_key_strict"),
            title_year_key=keys_data.get("title_year_key"),
            title_first_author_key=keys_data.get("title_first_author_key"),
            title_journal_key=keys_data.get("title_journal_key"),
            title_key_fuzzy=keys_data.get("title_key_fuzzy"),
            title_shingles=keys_data.get("title_shingles"),
            title_minhash=keys_data.get("title_minhash"),
            title_simhash=keys_data.get("title_simhash"),
        )

        flags_data = data.get("flags", {})
        record_flags = Flags(
            doi_present=flags_data.get("doi_present", False),
            pmid_present=flags_data.get("pmid_present", False),
            title_missing=flags_data.get("title_missing", False),
            title_truncated=flags_data.get("title_truncated", False),
            authors_missing=flags_data.get("authors_missing", False),
            authors_incomplete=flags_data.get("authors_incomplete", False),
            year_missing=flags_data.get("year_missing", False),
            pages_unreliable=flags_data.get("pages_unreliable", False),
            is_erratum_notice=flags_data.get("is_erratum_notice", False),
            is_retraction_notice=flags_data.get("is_retraction_notice", False),
            is_corrected_republished=flags_data.get("is_corrected_republished", False),
            has_linked_citation=flags_data.get("has_linked_citation", False),
        )

        return cls(
            schema_version=data.get("schema_version", "1.0.0"),
            rid=data.get("rid", ""),
            record_digest=data.get("record_digest", ""),
            source_digest=data.get("source_digest"),
            meta=meta,
            raw=raw,
            canon=canon,
            keys=record_keys,
            flags=record_flags,
            provenance=data.get("provenance", {}),
        )
