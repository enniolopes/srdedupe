"""RIS format writer for merged and canonical records."""

from pathlib import Path

from srdedupe.merge.models import MergedRecord
from srdedupe.models import Canon, CanonicalRecord


def format_canon_as_ris(canon: Canon) -> str:
    """Format Canon fields as a single RIS record.

    Parameters
    ----------
    canon : Canon
        Canonical fields to format.

    Returns
    -------
    str
        RIS-formatted record string.
    """
    lines = ["TY  - JOUR"]

    if canon.title_raw:
        lines.append(f"TI  - {canon.title_raw}")

    if canon.abstract_raw:
        lines.append(f"AB  - {canon.abstract_raw}")

    if canon.authors_parsed:
        for author in canon.authors_parsed:
            if author.family:
                name = f"{author.family}, {author.given}" if author.given else author.family
                lines.append(f"AU  - {name}")
            elif author.raw:
                lines.append(f"AU  - {author.raw}")

    if canon.year_norm:
        lines.append(f"PY  - {canon.year_norm}")

    if canon.journal_full:
        lines.append(f"T2  - {canon.journal_full}")

    if canon.volume:
        lines.append(f"VL  - {canon.volume}")

    if canon.issue:
        lines.append(f"IS  - {canon.issue}")

    if canon.page_first:
        lines.append(f"SP  - {canon.page_first}")
    if canon.page_last:
        lines.append(f"EP  - {canon.page_last}")

    if canon.doi_norm:
        lines.append(f"DO  - {canon.doi_norm}")

    if canon.doi_url:
        lines.append(f"UR  - {canon.doi_url}")

    if canon.language:
        langs = canon.language if isinstance(canon.language, list) else [canon.language]
        for lang in langs:
            lines.append(f"LA  - {lang}")

    lines.append("ER  -")
    return "\r\n".join(lines)


def format_ris_record(merged_record: MergedRecord) -> str:
    """Format merged record as RIS.

    Parameters
    ----------
    merged_record : MergedRecord
        Merged record to format.

    Returns
    -------
    str
        RIS-formatted record string.
    """
    return format_canon_as_ris(merged_record.canon)


def write_ris_file(
    merged_records: list[MergedRecord], output_path: Path, line_ending: str = "\r\n"
) -> None:
    """Write merged records to RIS file.

    Parameters
    ----------
    merged_records : list[MergedRecord]
        Merged records to write.
    output_path : Path
        Output file path.
    line_ending : str, optional
        Line ending to use, by default "\\r\\n".
    """
    _write_ris(
        [r.canon for r in merged_records],
        output_path,
        line_ending,
    )


def write_ris_from_records(
    records: list[CanonicalRecord], output_path: Path, line_ending: str = "\r\n"
) -> None:
    """Write canonical records to RIS file.

    Used for REVIEW records that are not merged.

    Parameters
    ----------
    records : list[CanonicalRecord]
        Canonical records.
    output_path : Path
        Output file path.
    line_ending : str, optional
        Line ending to use, by default "\\r\\n".
    """
    _write_ris(
        [r.canon for r in records],
        output_path,
        line_ending,
    )


def _write_ris(canons: list[Canon], output_path: Path, line_ending: str) -> None:
    """Write a list of Canon objects to an RIS file.

    Parameters
    ----------
    canons : list[Canon]
        Canon instances to write.
    output_path : Path
        Output file path.
    line_ending : str
        Line ending to use between records.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        for i, canon in enumerate(canons):
            f.write(format_canon_as_ris(canon))
            if i < len(canons) - 1:
                f.write(line_ending)
                f.write(line_ending)
