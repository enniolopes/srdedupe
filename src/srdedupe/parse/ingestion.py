"""Multi-file ingestion orchestrator."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from srdedupe.models import CanonicalRecord
from srdedupe.parse.base import (
    SUPPORTED_EXTENSIONS,
    ParseResult,
    detect_encoding,
    normalize_line_endings,
    sniff_format,
)
from srdedupe.parse.bibtex import parse_bibtex
from srdedupe.parse.endnote import parse_endnote
from srdedupe.parse.pubmed import parse_pubmed
from srdedupe.parse.ris import parse_ris
from srdedupe.parse.wos import parse_wos
from srdedupe.utils import get_file_mtime, get_iso_timestamp

INGESTION_VERSION = "1.0.0"

ParserFn = Callable[[Path, list[str], bytes], ParseResult]


@dataclass(frozen=True)
class FileIngestionResult:
    """Immutable result of ingesting a single file.

    Attributes
    ----------
    filename : str
        Name of the file (basename).
    filepath : str
        Full path to the file.
    file_size : int
        Size of file in bytes.
    file_mtime : str
        ISO8601 timestamp of file modification time.
    format_detected : str
        Format detected (ris|pubmed|bibtex|wos|endnote_tagged|unknown).
    source_ext : str
        File extension (.ris|.nbib|.txt|.bib|.ciw|.enw).
    encoding_used : str
        Encoding used to decode file.
    records_parsed : int
        Number of records successfully parsed.
    tags_parsed : int
        Total number of tag lines parsed.
    warnings : tuple[str, ...]
        Warning messages.
    errors : tuple[str, ...]
        Error messages.
    file_digest : str
        SHA-256 digest of file bytes.
    """

    filename: str
    filepath: str
    file_size: int
    file_mtime: str
    format_detected: str
    source_ext: str
    encoding_used: str
    records_parsed: int
    tags_parsed: int
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    file_digest: str = ""


@dataclass(frozen=True)
class IngestionReport:
    """Immutable report for multi-file ingestion run.

    Attributes
    ----------
    tool_version : str
        Version of parser tool.
    run_timestamp : str
        ISO8601 timestamp (UTC) of ingestion run.
    total_files : int
        Total files processed.
    total_records : int
        Total records parsed across all files.
    total_errors : int
        Total errors across all files.
    total_warnings : int
        Total warnings across all files.
    file_results : tuple[FileIngestionResult, ...]
        Per-file ingestion results.
    """

    tool_version: str
    run_timestamp: str
    total_files: int
    total_records: int
    total_errors: int
    total_warnings: int
    file_results: tuple[FileIngestionResult, ...]


_PARSER_MAP: dict[str, ParserFn] = {
    "ris": parse_ris,
    "pubmed": parse_pubmed,
    "bibtex": parse_bibtex,
    "wos": parse_wos,
    "endnote_tagged": parse_endnote,
}


def get_parser_for_format(format_name: str) -> ParserFn | None:
    """Get parser function for format.

    Parameters
    ----------
    format_name : str
        Format name (ris|pubmed|bibtex|wos|endnote_tagged).

    Returns
    -------
    ParserFn | None
        Parser function or None if format not supported.
    """
    return _PARSER_MAP.get(format_name)


def ingest_file(file_path: Path) -> tuple[list[CanonicalRecord], FileIngestionResult]:
    """Ingest a single file.

    Parameters
    ----------
    file_path : Path
        Path to file to ingest.

    Returns
    -------
    tuple[list[CanonicalRecord], FileIngestionResult]
        - List of parsed canonical records
        - File ingestion result with metadata and stats
    """
    try:
        file_bytes = file_path.read_bytes()
    except OSError as e:
        result = FileIngestionResult(
            filename=file_path.name,
            filepath=str(file_path),
            file_size=0,
            file_mtime="",
            format_detected="unknown",
            source_ext=file_path.suffix,
            encoding_used="",
            records_parsed=0,
            tags_parsed=0,
            errors=(f"Failed to read file: {e}",),
        )
        return [], result

    file_mtime = get_file_mtime(file_path)
    file_size = len(file_bytes)
    encoding = detect_encoding(file_bytes)
    extension = file_path.suffix.lower()

    try:
        content = file_bytes.decode(encoding)
    except UnicodeDecodeError as e:
        result = FileIngestionResult(
            filename=file_path.name,
            filepath=str(file_path),
            file_size=file_size,
            file_mtime=file_mtime,
            format_detected="unknown",
            source_ext=extension,
            encoding_used=encoding,
            records_parsed=0,
            tags_parsed=0,
            errors=(f"Failed to decode with {encoding}: {e}",),
        )
        return [], result

    content = normalize_line_endings(content)
    lines = content.split("\n")

    format_detected = sniff_format(lines[:50])
    parser = get_parser_for_format(format_detected)

    if parser is None:
        result = FileIngestionResult(
            filename=file_path.name,
            filepath=str(file_path),
            file_size=file_size,
            file_mtime=file_mtime,
            format_detected=format_detected,
            source_ext=extension,
            encoding_used=encoding,
            records_parsed=0,
            tags_parsed=0,
            errors=(f"No parser available for format: {format_detected}",),
        )
        return [], result

    try:
        records, warnings, errors = parser(file_path, lines, file_bytes)
    except Exception as e:
        result = FileIngestionResult(
            filename=file_path.name,
            filepath=str(file_path),
            file_size=file_size,
            file_mtime=file_mtime,
            format_detected=format_detected,
            source_ext=extension,
            encoding_used=encoding,
            records_parsed=0,
            tags_parsed=0,
            errors=(f"Parser exception: {e}",),
        )
        return [], result

    total_tags = sum(len(rec.raw.tags) for rec in records)

    # Reuse digest already computed by parser via create_file_context
    file_digest = records[0].source_digest if records else ""

    result = FileIngestionResult(
        filename=file_path.name,
        filepath=str(file_path),
        file_size=file_size,
        file_mtime=file_mtime,
        format_detected=format_detected,
        source_ext=extension,
        encoding_used=encoding,
        records_parsed=len(records),
        tags_parsed=total_tags,
        warnings=tuple(warnings),
        errors=tuple(errors),
        file_digest=file_digest or "",
    )

    return records, result


def ingest_folder(
    folder_path: Path, recursive: bool = False, glob_pattern: str = "*"
) -> tuple[list[CanonicalRecord], IngestionReport]:
    """Ingest all supported files in a folder.

    Parameters
    ----------
    folder_path : Path
        Path to folder containing bibliographic files.
    recursive : bool, optional
        Whether to search recursively in subdirectories, by default False.
    glob_pattern : str, optional
        Glob pattern to filter files, by default "*".

    Returns
    -------
    tuple[list[CanonicalRecord], IngestionReport]
        - List of all parsed canonical records
        - Ingestion report with per-file stats and summary
    """
    all_records: list[CanonicalRecord] = []
    file_results: list[FileIngestionResult] = []

    # Find files
    if recursive:
        files = list(folder_path.rglob(glob_pattern))
    else:
        files = list(folder_path.glob(glob_pattern))

    # Filter to supported extensions
    supported_files = [f for f in files if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS]

    # Ingest each file
    for file_path in supported_files:
        records, result = ingest_file(file_path)
        all_records.extend(records)
        file_results.append(result)

    # Build summary report
    total_errors = sum(len(r.errors) for r in file_results)
    total_warnings = sum(len(r.warnings) for r in file_results)
    total_records = sum(r.records_parsed for r in file_results)

    report = IngestionReport(
        tool_version=INGESTION_VERSION,
        run_timestamp=get_iso_timestamp(),
        total_files=len(file_results),
        total_records=total_records,
        total_errors=total_errors,
        total_warnings=total_warnings,
        file_results=tuple(file_results),
    )

    return all_records, report
