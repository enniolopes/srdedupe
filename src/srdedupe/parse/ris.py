"""RIS format parser.

RIS specification: Two-letter tags, "TY  - " starts record, "ER  - " ends it.
Reference: https://refdb.sourceforge.net/manual-0.9.6/sect1-ris-format.html
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

PARSER_NAME = "ris_parser"
PARSER_VERSION = "1.0.0"

TAG_PATTERN = re.compile(r"^([A-Z0-9]{2})  - ?(.*)$")


def parse_ris(
    file_path: Path,
    lines: list[str],
    file_bytes: bytes,
) -> ParseResult:
    """Parse RIS file and return canonical records.

    Parameters
    ----------
    file_path : Path
        Path to the RIS file.
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
    in_record = False
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
                current_tag = None
                current_value_lines = []

            tag, value = match.groups()

            if tag == "TY":
                if in_record:
                    warnings.append(
                        f"Line {line_num}: Found TY without closing ER for previous record"
                    )
                    if current_record_lines:
                        rec = _build_record(current_record_lines, current_tags, ctx, record_index)
                        if rec:
                            records.append(rec)
                            record_index += 1
                in_record = True
                current_record_lines = [line]
                current_tags = []
                current_tag = tag
                current_value_lines = [value]
                current_tag_start = line_num

            elif tag == "ER":
                if not in_record:
                    warnings.append(f"Line {line_num}: Found ER without opening TY")
                else:
                    current_record_lines.append(line)
                    current_tags.append((tag, [], line_num, line_num))

                    rec = _build_record(current_record_lines, current_tags, ctx, record_index)
                    if rec:
                        records.append(rec)
                        record_index += 1

                    in_record = False
                    current_record_lines = []
                    current_tags = []
                    current_tag = None
                    current_value_lines = []

            else:
                if in_record:
                    current_record_lines.append(line)
                    current_tag = tag
                    current_value_lines = [value]
                    current_tag_start = line_num

        else:
            if in_record:
                current_record_lines.append(line)
                if line and line[0].isspace() and current_tag is not None:
                    current_value_lines.append(line)
                elif line.strip():
                    warnings.append(f"Line {line_num}: Unrecognized line in record: {line[:50]}")

    if in_record and current_record_lines:
        warnings.append("End of file reached without closing ER tag")
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
    return build_canonical_record(raw_tags, record_lines, "ris", ctx, record_index, PARSER_VERSION)
