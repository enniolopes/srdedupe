"""PubMed/MEDLINE format parser.

PubMed fields begin with 2-4 char tags, continuation lines are indented.
Record boundaries: blank line or new PMID field.
Reference: https://www.nlm.nih.gov/bsd/mms/medlineelements.html
"""

import re
from pathlib import Path

from srdedupe.models import CanonicalRecord
from srdedupe.parse.base import (
    FileContext,
    ParseResult,
    build_canonical_record,
    build_raw_tags,
    create_file_context,
)

PARSER_NAME = "pubmed_parser"
PARSER_VERSION = "1.0.0"

TAG_PATTERN = re.compile(r"^([A-Z]{2,4})\s*-\s+(.*)$")
CONTINUATION_PATTERN = re.compile(r"^      ")


def parse_pubmed(
    file_path: Path,
    lines: list[str],
    file_bytes: bytes,
) -> ParseResult:
    """Parse PubMed/MEDLINE file and return canonical records.

    Parameters
    ----------
    file_path : Path
        Path to the PubMed/MEDLINE file.
    lines : list[str]
        File content as decoded lines.
    file_bytes : bytes
        Raw file bytes for digest calculation.

    Returns
    -------
    ParseResult
        Records, warnings, and errors.
    """
    warnings: list[str] = []
    errors: list[str] = []
    records: list[CanonicalRecord] = []

    ctx = create_file_context(file_path, file_bytes)

    record_index = 0
    current_record_lines: list[str] = []
    current_tags: list[tuple[str, list[str], int, int]] = []
    current_tag: str | None = None
    current_value_lines: list[str] = []
    current_tag_start: int = 0

    for line_num, line in enumerate(lines):
        match = TAG_PATTERN.match(line)

        if match:
            if current_tag is not None:
                current_tags.append(
                    (current_tag, current_value_lines, current_tag_start, line_num - 1)
                )

            tag, value = match.groups()

            if tag == "PMID" and current_record_lines:
                rec = _build_record(current_record_lines, current_tags, ctx, record_index)
                if rec:
                    records.append(rec)
                    record_index += 1
                current_record_lines = []
                current_tags = []

            current_record_lines.append(line)
            current_tag = tag
            current_value_lines = [value]
            current_tag_start = line_num

        elif line.strip() == "":
            if current_record_lines:
                if current_tag is not None:
                    current_tags.append(
                        (current_tag, current_value_lines, current_tag_start, line_num - 1)
                    )
                    current_tag = None
                    current_value_lines = []

                rec = _build_record(current_record_lines, current_tags, ctx, record_index)
                if rec:
                    records.append(rec)
                    record_index += 1

                current_record_lines = []
                current_tags = []

        else:
            if CONTINUATION_PATTERN.match(line) and current_tag is not None:
                current_record_lines.append(line)
                current_value_lines.append(line)
            elif line and current_record_lines:
                current_record_lines.append(line)
                warnings.append(f"Line {line_num}: Unrecognized line in record: {line[:50]}")

    if current_record_lines:
        if current_tag is not None:
            current_tags.append(
                (current_tag, current_value_lines, current_tag_start, len(lines) - 1)
            )
        rec = _build_record(current_record_lines, current_tags, ctx, record_index)
        if rec:
            records.append(rec)

    return ParseResult(records, warnings, errors)


def _build_record(
    record_lines: list[str],
    tags: list[tuple[str, list[str], int, int]],
    ctx: FileContext,
    record_index: int,
) -> CanonicalRecord | None:
    raw_tags = build_raw_tags(tags)
    return build_canonical_record(
        raw_tags, record_lines, "pubmed", ctx, record_index, PARSER_VERSION
    )
