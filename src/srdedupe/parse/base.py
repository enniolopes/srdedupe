"""Base types and utilities for bibliographic parsers."""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

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
from srdedupe.utils import calculate_file_digest, get_file_mtime, get_iso_timestamp

SUPPORTED_EXTENSIONS: dict[str, str] = {
    ".ris": "ris",
    ".nbib": "pubmed",
    ".txt": "pubmed",
    ".bib": "bibtex",
    ".ciw": "wos",
    ".enw": "endnote_tagged",
}


@dataclass(frozen=True)
class FileContext:
    """Immutable context about the source file being parsed.

    Attributes
    ----------
    file_path : Path
        Path to the source file.
    file_digest : str
        SHA-256 digest of file bytes.
    file_mtime : str
        ISO8601 modification timestamp.
    file_size : int
        Size of file in bytes.
    """

    file_path: Path
    file_digest: str
    file_mtime: str
    file_size: int


class ParseResult(NamedTuple):
    """Result of parsing a bibliographic file.

    Supports tuple unpacking: ``records, warnings, errors = parse_ris(...)``.

    Attributes
    ----------
    records : list[CanonicalRecord]
        Parsed canonical records.
    warnings : list[str]
        Warning messages.
    errors : list[str]
        Error messages.
    """

    records: list[CanonicalRecord]
    warnings: list[str]
    errors: list[str]


def create_file_context(file_path: Path, file_bytes: bytes) -> FileContext:
    """Create file context from path and raw bytes.

    Parameters
    ----------
    file_path : Path
        Path to the source file.
    file_bytes : bytes
        Complete file content as bytes.

    Returns
    -------
    FileContext
        Immutable context with file metadata.
    """
    try:
        mtime = get_file_mtime(file_path)
    except OSError:
        mtime = get_iso_timestamp()

    return FileContext(
        file_path=file_path,
        file_digest=calculate_file_digest(file_bytes),
        file_mtime=mtime,
        file_size=len(file_bytes),
    )


def build_raw_tags(
    tags: list[tuple[str, list[str], int, int]],
    value_join: str = "\n",
) -> list[RawTag]:
    """Convert raw tag tuples to RawTag objects with occurrence tracking.

    Parameters
    ----------
    tags : list[tuple[str, list[str], int, int]]
        List of (tag, value_lines, line_start, line_end) tuples.
    value_join : str, optional
        String used to join value lines, by default "\\n".

    Returns
    -------
    list[RawTag]
        List of RawTag objects with correct occurrence counts.
    """
    raw_tags: list[RawTag] = []
    tag_counts: dict[str, int] = {}

    for tag, value_lines, line_start, line_end in tags:
        occurrence = tag_counts.get(tag, 0)
        tag_counts[tag] = occurrence + 1

        raw_tags.append(
            RawTag(
                tag=tag,
                value_lines=value_lines,
                value_raw_joined=value_join.join(value_lines),
                occurrence=occurrence,
                line_start=line_start,
                line_end=line_end,
            )
        )

    return raw_tags


def build_canonical_record(
    raw_tags: list[RawTag],
    record_lines: list[str],
    source_format: str,
    file_context: FileContext,
    record_index: int,
    parser_version: str,
) -> CanonicalRecord | None:
    """Build a canonical record from parsed tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Parsed raw tags.
    record_lines : list[str]
        Original lines of the record.
    source_format : str
        Format identifier (e.g., 'ris', 'pubmed').
    file_context : FileContext
        File metadata context.
    record_index : int
        0-based index of record in file.
    parser_version : str
        Version of the parser.

    Returns
    -------
    CanonicalRecord | None
        Canonical record or None if no tags.
    """
    if not raw_tags:
        return None

    raw_tags_dict = [{"tag": t.tag, "value": t.value_raw_joined} for t in raw_tags]
    record_digest = calculate_record_digest(raw_tags_dict, source_format)
    rid = calculate_rid(file_context.file_digest, record_digest)

    return CanonicalRecord(
        schema_version=SCHEMA_VERSION,
        rid=rid,
        record_digest=record_digest,
        source_digest=file_context.file_digest,
        meta=Meta(
            source_file=file_context.file_path.name,
            source_format=source_format,
            source_db=None,
            source_record_index=record_index,
            ingested_at=get_iso_timestamp(),
            source_file_mtime=file_context.file_mtime,
            source_file_size_bytes=file_context.file_size,
            parser_version=parser_version,
        ),
        raw=Raw(
            record_lines=record_lines,
            tags=raw_tags,
            unknown_lines=[],
        ),
        canon=Canon.empty(),
        keys=Keys.empty(),
        flags=Flags.pre_normalization(),
        provenance={},
    )


def detect_encoding(file_bytes: bytes) -> str:
    """Detect encoding of file bytes using deterministic strategy.

    Parameters
    ----------
    file_bytes : bytes
        Complete file content as bytes.

    Returns
    -------
    str
        Detected encoding (utf-8, latin-1, etc.).
    """
    if file_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    try:
        file_bytes.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    return "latin-1"


def normalize_line_endings(content: str) -> str:
    """Normalize line endings to LF.

    Parameters
    ----------
    content : str
        Text content with potentially mixed line endings.

    Returns
    -------
    str
        Text with normalized line endings (\\n only).
    """
    content = content.replace("\r\n", "\n")
    return content.replace("\r", "\n")


def sniff_format(lines: list[str]) -> str:
    """Sniff format by inspecting file content.

    Uses a 100-line sample window to accommodate records with many
    author/affiliation fields.  RIS detection requires only the ``TY``
    tag because the mandatory ``ER`` closer may fall beyond the sample
    when the first record is large.

    Parameters
    ----------
    lines : list[str]
        Lines of the file (at least 100 recommended).

    Returns
    -------
    str
        Format identifier (ris|pubmed|bibtex|wos|endnote_tagged|unknown).
    """
    sample_text = "\n".join(lines[:100])

    if re.search(r"^@\w+\s*\{", sample_text, re.MULTILINE):
        return "bibtex"

    if re.search(r"^PT [JS]\b", sample_text, re.MULTILINE):
        return "wos"

    if re.search(r"^TY  - ", sample_text, re.MULTILINE):
        return "ris"

    if re.search(r"^PMID-? ", sample_text, re.MULTILINE):
        return "pubmed"

    if re.search(r"^%[A-Z0-9] ", sample_text, re.MULTILINE):
        return "endnote_tagged"

    return "unknown"
