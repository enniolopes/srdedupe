"""BibTeX format parser.

Entries: @<entrytype>{citekey, field = {value}, ...}
Special entries (@STRING, @PREAMBLE, @COMMENT) are skipped.
Reference: http://www.bibtex.org/Format/
"""

import re
from pathlib import Path

from srdedupe.models import CanonicalRecord, RawTag
from srdedupe.parse.base import (
    FileContext,
    ParseResult,
    build_canonical_record,
    create_file_context,
)

PARSER_NAME = "bibtex_parser"
PARSER_VERSION = "1.0.0"

ENTRY_START_PATTERN = re.compile(r"^@(\w+)\s*\{\s*([^,]*)\s*,?\s*$", re.IGNORECASE)


def parse_bibtex(
    file_path: Path,
    lines: list[str],
    file_bytes: bytes,
) -> ParseResult:
    """Parse BibTeX file and return canonical records.

    Parameters
    ----------
    file_path : Path
        Path to the BibTeX file.
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
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if not line.startswith("@"):
            i += 1
            continue

        match = ENTRY_START_PATTERN.match(line)
        if not match:
            warnings.append(f"Line {i}: Malformed entry start: {line[:50]}")
            i += 1
            continue

        entry_type, citekey = match.groups()
        entry_type = entry_type.lower()
        citekey = citekey.strip()

        # Skip special entries
        if entry_type in ("string", "preamble", "comment"):
            closing_line = _find_closing_brace(lines, i)
            warnings.append(
                f"Line {i}: Skipping @{entry_type.upper()} entry (lines {i}-{closing_line})"
            )
            i = closing_line + 1
            continue

        # Parse regular entry
        entry_start = i
        closing_line = _find_closing_brace(lines, i)

        if closing_line == -1:
            errors.append(f"Line {i}: Unclosed entry @{entry_type}{{{citekey}}}")
            i += 1
            continue

        entry_lines = lines[entry_start : closing_line + 1]
        fields_data = _parse_fields(entry_lines[1 : closing_line - entry_start])

        rec = _build_record(
            entry_type,
            citekey,
            fields_data,
            entry_lines,
            entry_start,
            ctx,
            record_index,
        )
        if rec:
            records.append(rec)
            record_index += 1

        i = closing_line + 1

    return ParseResult(records, warnings, errors)


def _find_closing_brace(lines: list[str], start_line: int) -> int:
    brace_depth = 0
    in_quotes = False
    escape_next = False

    for i in range(start_line, len(lines)):
        for char in lines[i]:
            if escape_next:
                escape_next = False
                continue

            if char == "\\":
                escape_next = True
                continue

            if char == '"':
                in_quotes = not in_quotes
            elif not in_quotes:
                if char == "{":
                    brace_depth += 1
                elif char == "}":
                    brace_depth -= 1
                    if brace_depth == 0:
                        return i

    return -1


def _parse_fields(
    field_lines: list[str],
) -> list[tuple[str, str, list[str], int, int]]:
    fields: list[tuple[str, str, list[str], int, int]] = []
    content = "\n".join(field_lines)

    i = 0
    while i < len(content):
        # Skip whitespace
        while i < len(content) and content[i].isspace():
            i += 1
        if i >= len(content):
            break

        # Match field name pattern: word =
        field_match = re.match(r"(\w+)\s*=\s*", content[i:], re.IGNORECASE)
        if not field_match:
            i += 1
            continue

        field_name = field_match.group(1).lower()
        field_start = i
        i += field_match.end()

        value_lines_start = content[:field_start].count("\n")

        # Skip whitespace before value
        while i < len(content) and content[i] in " \t":
            i += 1
        if i >= len(content):
            break

        # Parse value based on delimiter
        if content[i] == "{":
            value, i = _parse_braced_value(content, i)
        elif content[i] == '"':
            value, i = _parse_quoted_value(content, i)
        else:
            value, i = _parse_bare_value(content, i)

        # Skip trailing comma
        while i < len(content) and content[i] in " \t\n":
            i += 1
        if i < len(content) and content[i] == ",":
            i += 1

        value_end = i
        value_lines_end = content[:value_end].count("\n")
        field_content = content[field_start:value_end]
        value_lines_list = field_content.split("\n")

        fields.append(
            (field_name, value.strip(), value_lines_list, value_lines_start, value_lines_end)
        )

    return fields


def _parse_braced_value(content: str, start: int) -> tuple[str, int]:
    brace_depth = 0
    value_chars: list[str] = []
    i = start

    while i < len(content):
        char = content[i]
        if char == "{":
            brace_depth += 1
            if brace_depth > 1:
                value_chars.append(char)
        elif char == "}":
            brace_depth -= 1
            if brace_depth == 0:
                return "".join(value_chars), i + 1
            value_chars.append(char)
        else:
            value_chars.append(char)
        i += 1

    return "".join(value_chars), i


def _parse_quoted_value(content: str, start: int) -> tuple[str, int]:
    i = start + 1  # skip opening quote
    value_chars: list[str] = []
    escape_next = False

    while i < len(content):
        char = content[i]
        if escape_next:
            value_chars.append(char)
            escape_next = False
        elif char == "\\":
            escape_next = True
        elif char == '"':
            return "".join(value_chars), i + 1
        else:
            value_chars.append(char)
        i += 1

    return "".join(value_chars), i


def _parse_bare_value(content: str, start: int) -> tuple[str, int]:
    value_chars: list[str] = []
    i = start

    while i < len(content) and content[i] not in ",\n}":
        if content[i] == "#":
            break
        value_chars.append(content[i])
        i += 1

    return "".join(value_chars).strip(), i


def _build_record(
    entry_type: str,
    citekey: str,
    fields_data: list[tuple[str, str, list[str], int, int]],
    entry_lines: list[str],
    entry_start_line: int,
    ctx: FileContext,
    record_index: int,
) -> CanonicalRecord | None:
    raw_tags: list[RawTag] = []
    tag_counts: dict[str, int] = {}

    # Synthetic tags for entry type and citekey
    raw_tags.append(
        RawTag(
            tag="__bibtex_entrytype",
            value_lines=[entry_type],
            value_raw_joined=entry_type,
            occurrence=0,
            line_start=entry_start_line,
            line_end=entry_start_line,
        )
    )
    raw_tags.append(
        RawTag(
            tag="__bibtex_citekey",
            value_lines=[citekey],
            value_raw_joined=citekey,
            occurrence=0,
            line_start=entry_start_line,
            line_end=entry_start_line,
        )
    )

    for field_name, value, value_lines, rel_start, rel_end in fields_data:
        occurrence = tag_counts.get(field_name, 0)
        tag_counts[field_name] = occurrence + 1
        abs_start = entry_start_line + rel_start + 1
        abs_end = entry_start_line + rel_end + 1

        raw_tags.append(
            RawTag(
                tag=field_name,
                value_lines=value_lines,
                value_raw_joined=value,
                occurrence=occurrence,
                line_start=abs_start,
                line_end=abs_end,
            )
        )

    return build_canonical_record(
        raw_tags, entry_lines, "bibtex", ctx, record_index, PARSER_VERSION
    )
