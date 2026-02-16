"""Web of Science / ISI field-tagged format parser.

WoS format: Two-character tags, PT starts record, ER ends record.
FN/VR are header tags, EF marks end of file.
Reference: ISI Export Format specification.
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

PARSER_NAME = "wos_parser"
PARSER_VERSION = "1.0.0"

TAG_PATTERN = re.compile(r"^([A-Z0-9]{2})(?: (.*))?$")


def parse_wos(
    file_path: Path,
    lines: list[str],
    file_bytes: bytes,
) -> ParseResult:
    """Parse WoS file and return canonical records.

    Parameters
    ----------
    file_path : Path
        Path to the WoS file.
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
    in_header = True
    in_record = False
    current_record_lines: list[str] = []
    current_tags: list[tuple[str, list[str], int, int]] = []
    current_tag: str | None = None
    current_value_lines: list[str] = []
    current_tag_start: int = 0

    for line_num, line in enumerate(lines):
        # EF = end of file
        if line.strip() == "EF":
            if in_record and current_record_lines:
                if current_tag is not None:
                    current_tags.append(
                        (current_tag, current_value_lines, current_tag_start, line_num - 1)
                    )
                rec = _build_record(current_record_lines, current_tags, ctx, record_index)
                if rec:
                    records.append(rec)
            break

        match = TAG_PATTERN.match(line)

        if match:
            if current_tag is not None:
                current_tags.append(
                    (current_tag, current_value_lines, current_tag_start, line_num - 1)
                )
                current_tag = None
                current_value_lines = []

            tag, value = match.groups()
            value = value or ""

            # Header tags
            if tag in ("FN", "VR"):
                if in_record:
                    warnings.append(f"Line {line_num}: Found header tag {tag} inside record")
                in_header = True
                continue

            # PT = record start
            if tag == "PT":
                in_header = False
                if in_record:
                    warnings.append(
                        f"Line {line_num}: Found PT without closing ER for previous record"
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

            # ER = record end
            elif tag == "ER":
                if not in_record:
                    warnings.append(f"Line {line_num}: Found ER without opening PT")
                else:
                    current_record_lines.append(line)
                    if value.strip():
                        current_tags.append((tag, [value], line_num, line_num))
                    else:
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
                elif not in_header:
                    warnings.append(f"Line {line_num}: Found tag {tag} outside of record")

        else:
            if in_record:
                current_record_lines.append(line)
                if line and line[0].isspace() and current_tag is not None:
                    current_value_lines.append(line.strip())
                elif line.strip():
                    warnings.append(f"Line {line_num}: Unrecognized line in record: {line[:50]}")
            elif not in_header and line.strip():
                warnings.append(f"Line {line_num}: Line outside of record: {line[:50]}")

    # Handle incomplete record (no EF found)
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
    raw_tags = build_raw_tags(tags, value_join=" ")
    return build_canonical_record(raw_tags, record_lines, "wos", ctx, record_index, PARSER_VERSION)
