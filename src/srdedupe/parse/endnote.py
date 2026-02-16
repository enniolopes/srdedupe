"""EndNote Tagged / Refer format parser.

Fields begin with % followed by a single character and a space.
References separated by blank lines. Continuation: non-tag non-blank lines.
Reference: https://refdb.sourceforge.net/manual-0.9.6/sect1-refdb-format.html
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

PARSER_NAME = "endnote_parser"
PARSER_VERSION = "1.0.0"

TAG_PATTERN = re.compile(r"^%([A-Z0-9]) (.*)$")


def parse_endnote(
    file_path: Path,
    lines: list[str],
    file_bytes: bytes,
) -> ParseResult:
    """Parse EndNote Tagged file and return canonical records.

    Parameters
    ----------
    file_path : Path
        Path to the EndNote file.
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
    blank_line_count = 0

    for line_num, line in enumerate(lines):
        match = TAG_PATTERN.match(line)

        if match:
            blank_line_count = 0

            if current_tag is not None:
                current_tags.append(
                    (current_tag, current_value_lines, current_tag_start, line_num - 1)
                )

            tag, value = match.groups()
            current_tag = tag
            current_value_lines = [value]
            current_tag_start = line_num
            current_record_lines.append(line)

        elif line.strip() == "":
            blank_line_count += 1

            if blank_line_count == 1 and current_record_lines:
                if current_tag is not None:
                    current_tags.append(
                        (current_tag, current_value_lines, current_tag_start, line_num - 1)
                    )

                rec = _build_record(current_record_lines, current_tags, ctx, record_index)
                if rec:
                    records.append(rec)
                    record_index += 1

                current_record_lines = []
                current_tags = []
                current_tag = None
                current_value_lines = []

        else:
            blank_line_count = 0
            if current_tag is not None:
                current_value_lines.append(line)
                current_record_lines.append(line)
            else:
                if current_record_lines:
                    warnings.append(f"Line {line_num}: Line without tag context: {line[:50]}")
                current_record_lines.append(line)

    # Handle last record (no trailing blank line)
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
        raw_tags, record_lines, "endnote_tagged", ctx, record_index, PARSER_VERSION
    )
