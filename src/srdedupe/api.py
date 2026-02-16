"""Public API for parsing bibliographic files.

This module provides the main public API for srdedupe, enabling:
- Parsing files and folders into CanonicalRecord objects
- Exporting records to JSONL format
- Running the deduplication pipeline
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from srdedupe.models import CanonicalRecord
from srdedupe.parse.ingestion import ingest_file, ingest_folder

if TYPE_CHECKING:
    from srdedupe.engine.config import PipelineResult

__all__ = [
    "parse_file",
    "parse_folder",
    "write_jsonl",
    "dedupe",
    "ParseError",
]


class ParseError(Exception):
    """Raised when parsing fails."""

    def __init__(
        self,
        message: str,
        file: str | None = None,
    ) -> None:
        """Initialize parse error.

        Parameters
        ----------
        message : str
            Error message.
        file : str | None, optional
            File where error occurred.
        """
        super().__init__(message)
        self.file = file


def parse_file(
    path: str | Path,
    *,
    strict: bool = True,
) -> list[CanonicalRecord]:
    """Parse a single bibliographic file.

    Format is auto-detected from file content.

    Parameters
    ----------
    path : str | Path
        Path to file to parse.
    strict : bool, optional
        If True, raise exception on parse errors. If False, return
        whatever records could be parsed, by default True.

    Returns
    -------
    list[CanonicalRecord]
        Parsed canonical records.

    Raises
    ------
    ParseError
        If parsing fails and strict=True.
    FileNotFoundError
        If file does not exist.

    Examples
    --------
    Parse a RIS file:

        >>> from srdedupe import parse_file
        >>> records = parse_file("references.ris")
        >>> for record in records:
        ...     print(record.canon.title_raw)
    """
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    records, result = ingest_file(file_path)

    if result.errors and strict:
        error_msg = "; ".join(result.errors)
        raise ParseError(
            f"Failed to parse {file_path.name}: {error_msg}",
            file=str(file_path),
        )

    return records


def parse_folder(
    path: str | Path,
    *,
    pattern: str | None = None,
    recursive: bool = False,
    strict: bool = False,
) -> list[CanonicalRecord]:
    """Parse all supported files in a folder.

    Scans a folder for bibliographic files and returns all canonical
    records found, preserving source file metadata.

    Parameters
    ----------
    path : str | Path
        Path to folder containing files.
    pattern : str | None, optional
        Glob pattern to filter files (e.g., '*.ris').
        If None, all supported extensions are included.
    recursive : bool, optional
        Whether to search recursively in subdirectories, by default False.
    strict : bool, optional
        If True, raise exception on any parse errors. If False, log warnings
        and continue, by default False.

    Returns
    -------
    list[CanonicalRecord]
        Parsed canonical records from all files.

    Raises
    ------
    ParseError
        If parsing fails and strict=True.
    FileNotFoundError
        If folder does not exist.

    Examples
    --------
    Parse all files in a folder:

        >>> from srdedupe import parse_folder
        >>> records = parse_folder("data/")
        >>> print(f"Found {len(records)} records")

    Parse recursively with filtering:

        >>> records = parse_folder("data/", pattern="*.ris", recursive=True)
    """
    folder_path = Path(path)

    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {path}")

    if not folder_path.is_dir():
        raise ValueError(f"Path is not a directory: {path}")

    glob_pattern = pattern if pattern else "*"

    records, report = ingest_folder(folder_path, recursive=recursive, glob_pattern=glob_pattern)

    if report.total_errors > 0 and strict:
        error_files = [
            f"{r.filename}: {', '.join(r.errors)}" for r in report.file_results if r.errors
        ]
        raise ParseError(
            f"Failed to parse {len(error_files)} file(s): {'; '.join(error_files[:3])}"
        )

    return records


def write_jsonl(
    records: list[CanonicalRecord],
    path: str | Path,
    *,
    sort_keys: bool = True,
) -> None:
    """Write records to JSONL file (one JSON object per line).

    Output is deterministic with consistent field ordering and UTF-8 encoding.

    Parameters
    ----------
    records : list[CanonicalRecord]
        Records to write.
    path : str | Path
        Output file path.
    sort_keys : bool, optional
        Whether to sort dictionary keys for deterministic output,
        by default True.

    Examples
    --------
    Export parsed records to JSONL:

        >>> from srdedupe import parse_folder, write_jsonl
        >>> records = parse_folder("data/")
        >>> write_jsonl(records, "output.jsonl")
    """
    file_path = Path(path)

    with file_path.open("w", encoding="utf-8", newline="\n") as f:
        for record in records:
            json_str = json.dumps(
                record.to_dict(),
                ensure_ascii=False,
                sort_keys=sort_keys,
            )
            f.write(json_str + "\n")


def dedupe(
    input_path: str | Path,
    *,
    output_dir: str | Path = "out",
    fpr_alpha: float = 0.01,
    t_low: float = 0.3,
    t_high: float | None = None,
) -> PipelineResult:
    """Deduplicate bibliographic records from file or folder.

    Simplified interface to the full deduplication pipeline.
    Parses input files, identifies duplicates using probabilistic matching,
    and generates deduplicated output files.

    Parameters
    ----------
    input_path : str | Path
        Path to input file or folder containing bibliographic files.
    output_dir : str | Path, optional
        Directory for output files, by default "out".
    fpr_alpha : float, optional
        Maximum acceptable false positive rate (0.0 to 1.0), by default 0.01 (1%).
        Lower values are more conservative (fewer false positives, more review).
    t_low : float, optional
        Lower threshold for AUTO_KEEP decision (0.0 to 1.0), by default 0.3.
    t_high : float | None, optional
        Upper threshold for AUTO_DUP. If None, uses default of 0.95.

    Returns
    -------
    PipelineResult
        Pipeline result with success status, statistics, and output file paths.
        Access ``result.output_files`` for a dict mapping artifact names to paths.

    Raises
    ------
    FileNotFoundError
        If input path does not exist.
    ParseError
        If deduplication fails.

    Examples
    --------
    Deduplicate a single RIS file:

        >>> from srdedupe import dedupe
        >>> result = dedupe("references.ris")
        >>> print(result.total_records, result.total_duplicates_auto)

    Deduplicate a folder with stricter FPR:

        >>> result = dedupe("data/", output_dir="results", fpr_alpha=0.005)
        >>> print(result.output_files)

    Notes
    -----
    The pipeline performs 6 stages:
    1. Parse & Normalize — Convert to canonical format
    2. Candidate Generation — Find potential duplicates
    3. Probabilistic Scoring — Calculate similarity scores
    4. Three-Way Decision — Classify as AUTO_DUP/REVIEW/AUTO_KEEP
    5. Global Clustering — Group duplicates transitively
    6. Canonical Merge — Merge duplicate records

    All outputs are deterministic and fully auditable.
    """
    from srdedupe.engine import PipelineConfig, run_pipeline

    input_path_obj = Path(input_path)
    output_dir_obj = Path(output_dir)

    if not input_path_obj.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")

    config = PipelineConfig(
        fpr_alpha=fpr_alpha,
        t_low=t_low,
        t_high=t_high,
        output_dir=output_dir_obj,
    )

    result = run_pipeline(input_path=input_path_obj, config=config)

    if not result.success:
        raise ParseError(f"Deduplication failed: {result.error_message}")

    return result
