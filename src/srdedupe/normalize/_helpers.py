"""Helper functions and compiled regex patterns for normalization.

This module provides reusable utilities to eliminate boilerplate
and improve performance through pre-compiled regex patterns.
"""

import re
import unicodedata
from collections.abc import Callable

from srdedupe.models.records import RawTag

# Pre-compiled regex patterns
DOI_SUFFIX_RE = re.compile(r"\s*\[doi\]\s*$", re.IGNORECASE)
DOI_URL_RE = re.compile(r"(?:doi\.org|dx\.doi\.org)/([^\s?#]+)")
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
ELOCATOR_RE = re.compile(r"^e\d+", re.IGNORECASE)
PAGE_RANGE_RE = re.compile(r"(\d+)\s*[-–—]\s*(\d+)")
DASH_NORMALIZE_RE = re.compile(r"\s*[–—]\s*")
PUNCT_RE = re.compile(r'[.,:;!?\'"()\[\]{}]+')
SUFFIX_RE = re.compile(r"\s+(Jr\.?|Sr\.?|II|III|IV|V)$", re.IGNORECASE)
INITIALS_RE = re.compile(r"^[A-Z]\.?(\s*[A-Z]\.?)*$")
PMID_AID_RE = re.compile(r"(\d+)\s*\[pmid\]", re.IGNORECASE)
PMCID_AID_RE = re.compile(r"(PMC\d+)\s*\[pmc\]", re.IGNORECASE)

# Patterns for special record type detection
ERRATUM_TITLE_RE = re.compile(
    r"\b(erratum|corrigendum|correction|errata|addendum)\b",
    re.IGNORECASE,
)
RETRACTION_TITLE_RE = re.compile(
    r"\b(retraction|retracted|withdrawal)\b",
    re.IGNORECASE,
)

# Known publication types indicating special records
ERRATUM_PUB_TYPES = frozenset(
    {
        "erratum",
        "published erratum",
        "correction",
        "corrigendum",
        "addendum",
    }
)
RETRACTION_PUB_TYPES = frozenset(
    {
        "retraction of publication",
        "retraction",
        "retracted publication",
        "withdrawal",
    }
)
CORRECTED_REPUBLISHED_PUB_TYPES = frozenset(
    {
        "corrected and republished article",
        "corrected and republished",
    }
)


# ---------------------------------------------------------------------------
# Text normalization functions
# ---------------------------------------------------------------------------


def strip_accents(text: str) -> str:
    """Remove diacritical marks for cross-locale matching.

    Parameters
    ----------
    text : str
        Input text with potential diacritics.

    Returns
    -------
    str
        Text with diacritical marks removed.
    """
    nfd = unicodedata.normalize("NFD", text)
    stripped = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return unicodedata.normalize("NFC", stripped)


def normalize_text_for_matching(text: str) -> str:
    """Full text normalization for dedup matching.

    Applies NFKC, casefold, accent stripping, punctuation removal,
    and whitespace collapsing. Used for titles and fields where
    maximum recall is needed.

    Parameters
    ----------
    text : str
        Raw text to normalize.

    Returns
    -------
    str
        Normalized text ready for matching.
    """
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.casefold()
    text = strip_accents(text)
    text = PUNCT_RE.sub(" ", text)
    text = " ".join(text.split())
    return text.strip()


def normalize_text_light(text: str) -> str:
    """Light text normalization for abstracts.

    Applies NFKC, casefold, and whitespace collapsing only.
    Preserves more structure than full normalization.

    Parameters
    ----------
    text : str
        Raw text to normalize.

    Returns
    -------
    str
        Lightly normalized text.
    """
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.casefold()
    text = " ".join(text.split())
    return text.strip()


# ---------------------------------------------------------------------------
# Tag lookup functions
# ---------------------------------------------------------------------------


def find_tag_value(
    raw_tags: list[RawTag],
    tag_names: list[str],
    *,
    predicate: Callable[[str], bool] | None = None,
) -> tuple[str | None, int | None]:
    """Find first matching tag value respecting tag-name priority.

    Iterates tag_names in priority order, returning the first valid
    match found in raw_tags for the highest-priority tag name.

    Parameters
    ----------
    raw_tags : list[RawTag]
        List of raw tags from record.
    tag_names : list[str]
        Tag names to search for (in priority order).
    predicate : Callable[[str], bool] | None, optional
        Optional filter function for value validation.

    Returns
    -------
    tuple[str | None, int | None]
        (value, index) if found, (None, None) otherwise.
    """
    for tag_name in tag_names:
        for i, tag in enumerate(raw_tags):
            if tag.tag == tag_name:
                value = tag.value_raw_joined.strip()
                if value and (predicate is None or predicate(value)):
                    return value, i
    return None, None


def find_all_tag_values(
    raw_tags: list[RawTag],
    tag_names: list[str],
    *,
    predicate: Callable[[str], bool] | None = None,
) -> list[tuple[str, int]]:
    """Find all matching tag values and their indices.

    Parameters
    ----------
    raw_tags : list[RawTag]
        List of raw tags from record.
    tag_names : list[str]
        Tag names to search for.
    predicate : Callable[[str], bool] | None, optional
        Optional filter function for value validation.

    Returns
    -------
    list[tuple[str, int]]
        List of (value, index) pairs for all matches.
    """
    matches = []
    for i, tag in enumerate(raw_tags):
        if tag.tag in tag_names:
            value = tag.value_raw_joined.strip()
            if value and (predicate is None or predicate(value)):
                matches.append((value, i))
    return matches
