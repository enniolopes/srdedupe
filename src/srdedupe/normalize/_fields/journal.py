"""Journal normalization."""

import unicodedata
from typing import Any

from srdedupe.models.records import RawTag

from .._helpers import find_tag_value, strip_accents
from .._provenance import add_transform, build_provenance_entry
from .._result_types import JournalResult
from ..tag_mappings import get_tags


def normalize_journal(
    raw_tags: list[RawTag], source_format: str
) -> tuple[JournalResult, dict[str, Any]]:
    """Normalize journal from raw tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    tuple[JournalResult, dict[str, Any]]
        (result, provenance_dict)
    """
    full_tags = get_tags(source_format, "journal_full")
    journal_full, full_idx = find_tag_value(raw_tags, full_tags)

    abbrev_tags = get_tags(source_format, "journal_abbrev")
    journal_abbrev, abbrev_idx = find_tag_value(raw_tags, abbrev_tags)

    journal_to_normalize = journal_full or journal_abbrev
    journal_idx = full_idx if journal_full else abbrev_idx

    if not journal_to_normalize or journal_idx is None:
        return JournalResult(journal_full, journal_abbrev, None), {}

    journal_norm = _normalize_journal_string(journal_to_normalize)

    prov = build_provenance_entry(
        "canon.journal_norm",
        raw_tags,
        [journal_idx],
        source_format,
        [
            add_transform(
                "normalize_journal",
                "NFKC, casefold, strip accents, collapse whitespace",
            )
        ],
        "high",
    )

    return JournalResult(journal_full, journal_abbrev, journal_norm), prov


def _normalize_journal_string(journal: str) -> str:
    if not journal:
        return ""
    journal = unicodedata.normalize("NFKC", journal)
    journal = journal.casefold()
    journal = strip_accents(journal)
    journal = " ".join(journal.split())
    journal = journal.strip(". ")
    return journal
