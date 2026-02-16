"""Field comparators for pairwise scoring.

This module provides pure, deterministic functions for comparing bibliographic
record fields. Each comparator maps field values to agreement levels and
similarity scores.

All functions are locale-independent and reproducible.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from srdedupe.models import CanonicalRecord


# Type alias for comparator result
CompareResult = tuple[str, float | None, list[str]]


@dataclass(frozen=True, slots=True)
class FieldConfig:
    """Configuration for a field comparator.

    Attributes
    ----------
    name : str
        Field name (e.g., 'doi', 'title').
    extractor : Callable[[CanonicalRecord, CanonicalRecord], dict[str, Any]]
        Function to extract comparison inputs from record pair.
    comparator : Callable[..., CompareResult]
        Comparison function.
    """

    name: str
    extractor: Callable[["CanonicalRecord", "CanonicalRecord"], dict[str, Any]]
    comparator: Callable[..., CompareResult]

    def compare(self, record_a: "CanonicalRecord", record_b: "CanonicalRecord") -> CompareResult:
        """Extract fields and run comparison.

        Parameters
        ----------
        record_a : CanonicalRecord
            First record.
        record_b : CanonicalRecord
            Second record.

        Returns
        -------
        CompareResult
            (level, similarity, warnings).
        """
        params = self.extractor(record_a, record_b)
        return self.comparator(**params)


def compare_doi(doi_a: str | None, doi_b: str | None) -> CompareResult:
    """Compare DOI fields between two records.

    Parameters
    ----------
    doi_a : str | None
        Normalized DOI from first record.
    doi_b : str | None
        Normalized DOI from second record.

    Returns
    -------
    tuple[str, float | None, list[str]]
        (level, similarity, warnings)
        - level: 'exact', 'both_present_mismatch', or 'missing'
        - similarity: None (DOI comparison is binary)
        - warnings: List of warning codes

    Notes
    -----
    DOI comparison is the strongest signal:
    - 'exact': Both present and identical → strong positive weight
    - 'both_present_mismatch': Both present but different → strong negative weight
    - 'missing': One or both missing → near-zero weight
    """
    warnings: list[str] = []

    # Both missing or one missing
    if not doi_a or not doi_b:
        return ("missing", None, warnings)

    # Both present and equal
    if doi_a == doi_b:
        return ("exact", None, warnings)

    # Both present but different - strong negative signal
    warnings.append("both_present_id_conflicts")
    return ("both_present_mismatch", None, warnings)


def compare_pmid(pmid_a: str | None, pmid_b: str | None) -> CompareResult:
    """Compare PMID fields between two records.

    Parameters
    ----------
    pmid_a : str | None
        Normalized PMID from first record.
    pmid_b : str | None
        Normalized PMID from second record.

    Returns
    -------
    tuple[str, float | None, list[str]]
        (level, similarity, warnings)
        - level: 'exact', 'both_present_mismatch', or 'missing'
        - similarity: None (PMID comparison is binary)
        - warnings: List of warning codes

    Notes
    -----
    PMID comparison follows same logic as DOI.
    """
    warnings: list[str] = []

    if not pmid_a or not pmid_b:
        return ("missing", None, warnings)

    if pmid_a == pmid_b:
        return ("exact", None, warnings)

    warnings.append("both_present_id_conflicts")
    return ("both_present_mismatch", None, warnings)


def jaccard_similarity(set_a: set[str], set_b: set[str]) -> float:
    """Calculate Jaccard similarity between two sets.

    Parameters
    ----------
    set_a : set[str]
        First set.
    set_b : set[str]
        Second set.

    Returns
    -------
    float
        Jaccard similarity (0.0-1.0).

    Notes
    -----
    Jaccard = |A ∩ B| / |A ∪ B|

    **Edge case**: When both sets are empty, returns 1.0 (perfect match)
    rather than 0.0 or undefined. This is a design choice for bibliographic
    matching where "both missing" is treated as agreement rather than
    disagreement.
    """
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0

    intersection = len(set_a & set_b)
    union = len(set_a | set_b)

    if union == 0:
        return 0.0

    return intersection / union


def compare_title(
    title_a: str | None,
    title_b: str | None,
    shingles_a: list[str] | None,
    shingles_b: list[str] | None,
    truncated_a: bool,
    truncated_b: bool,
) -> CompareResult:
    """Compare title fields between two records.

    Parameters
    ----------
    title_a : str | None
        Normalized title from first record.
    title_b : str | None
        Normalized title from second record.
    shingles_a : list[str] | None
        Title shingles from first record (preferred for similarity).
    shingles_b : list[str] | None
        Title shingles from second record.
    truncated_a : bool
        Whether first title is truncated.
    truncated_b : bool
        Whether second title is truncated.

    Returns
    -------
    tuple[str, float | None, list[str]]
        (level, similarity, warnings)
        - level: 'exact', 'high', 'medium', 'low', or 'missing'
        - similarity: Jaccard similarity on shingles (0.0-1.0)
        - warnings: List of warning codes (e.g., 'title_truncated')

    Notes
    -----
    Thresholds:
    - exact: String equality
    - high: similarity >= 0.92
    - medium: similarity >= 0.85
    - low: similarity >= 0.75
    - missing: One/both missing or similarity < 0.75

    If truncated, add warning and optionally cap level at 'medium'.
    """
    warnings: list[str] = []

    # Add truncation warning
    if truncated_a or truncated_b:
        warnings.append("title_truncated")

    # Missing check
    if not title_a or not title_b:
        return ("missing", None, warnings)

    # Exact string match
    if title_a == title_b:
        return ("exact", 1.0, warnings)

    # Calculate similarity using shingles if available, otherwise fallback
    sim: float
    if shingles_a and shingles_b:
        sim = jaccard_similarity(set(shingles_a), set(shingles_b))
    else:
        # Fallback to token-based similarity using space-split
        tokens_a = set(title_a.lower().split())
        tokens_b = set(title_b.lower().split())
        sim = jaccard_similarity(tokens_a, tokens_b)

    # Determine level based on similarity
    if sim >= 0.92:
        level = "high"
    elif sim >= 0.85:
        level = "medium"
    elif sim >= 0.75:
        level = "low"
    else:
        return ("missing", sim, warnings)

    # Cap level at 'medium' if truncated (configurable safety)
    if (truncated_a or truncated_b) and level == "high":
        level = "medium"

    return (level, sim, warnings)


def compare_authors(
    first_author_sig_a: str | None,
    first_author_sig_b: str | None,
    author_sig_strict_a: list[str] | None,
    author_sig_strict_b: list[str] | None,
) -> CompareResult:
    """Compare author fields between two records.

    Parameters
    ----------
    first_author_sig_a : str | None
        First author signature from first record.
    first_author_sig_b : str | None
        First author signature from second record.
    author_sig_strict_a : list[str] | None
        Strict author signatures from first record.
    author_sig_strict_b : list[str] | None
        Strict author signatures from second record.

    Returns
    -------
    tuple[str, float | None, list[str]]
        (level, similarity, warnings)
        - level: 'strong', 'weak', 'mismatch', or 'missing'
        - similarity: Jaccard on author signatures
        - warnings: List of warning codes

    Notes
    -----
    Levels:
    - strong: First author matches + significant overlap (>= 0.5 Jaccard)
    - weak: Partial overlap (< 0.5 Jaccard) or only first author matches
    - mismatch: First authors present but different
    - missing: Missing author information
    """
    warnings: list[str] = []

    # Missing check
    if not first_author_sig_a or not first_author_sig_b:
        return ("missing", None, warnings)

    # First author comparison
    first_author_match = first_author_sig_a == first_author_sig_b

    # Calculate overall overlap
    sim: float | None = None
    if author_sig_strict_a and author_sig_strict_b:
        sim = jaccard_similarity(set(author_sig_strict_a), set(author_sig_strict_b))

    # Determine level
    if first_author_match and sim is not None and sim >= 0.5:
        return ("strong", sim, warnings)
    elif first_author_match or (sim is not None and sim >= 0.3):
        return ("weak", sim, warnings)
    else:
        return ("mismatch", sim, warnings)


def compare_year(year_a: int | None, year_b: int | None) -> CompareResult:
    """Compare year fields between two records.

    Parameters
    ----------
    year_a : int | None
        Normalized year from first record.
    year_b : int | None
        Normalized year from second record.

    Returns
    -------
    tuple[str, float | None, list[str]]
        (level, similarity, warnings)
        - level: 'exact', 'pm1', 'pm2', 'far', or 'missing'
        - similarity: None (year comparison is discrete)
        - warnings: List of warning codes

    Notes
    -----
    Levels:
    - exact: Years match exactly
    - pm1: Years differ by 1
    - pm2: Years differ by 2
    - far: Years differ by more than 2
    - missing: One or both years missing
    """
    warnings: list[str] = []

    if year_a is None or year_b is None:
        return ("missing", None, warnings)

    delta = abs(year_a - year_b)

    if delta == 0:
        return ("exact", None, warnings)
    elif delta == 1:
        return ("pm1", None, warnings)
    elif delta == 2:
        return ("pm2", None, warnings)
    else:
        return ("far", None, warnings)


def compare_journal(journal_a: str | None, journal_b: str | None) -> CompareResult:
    """Compare journal fields between two records.

    Parameters
    ----------
    journal_a : str | None
        Normalized journal from first record.
    journal_b : str | None
        Normalized journal from second record.

    Returns
    -------
    tuple[str, float | None, list[str]]
        (level, similarity, warnings)
        - level: 'high', 'low', or 'missing'
        - similarity: String equality (0.0 or 1.0)
        - warnings: List of warning codes

    Notes
    -----
    Simple comparison:
    - high: Exact match
    - low: Present but different
    - missing: One or both missing
    """
    warnings: list[str] = []

    if not journal_a or not journal_b:
        return ("missing", None, warnings)

    if journal_a == journal_b:
        return ("high", 1.0, warnings)
    else:
        return ("low", 0.0, warnings)


def compare_pages(
    pages_norm_long_a: str | None,
    pages_norm_long_b: str | None,
    page_first_a: str | None,
    page_first_b: str | None,
    page_last_a: str | None,
    page_last_b: str | None,
    article_number_a: str | None,
    article_number_b: str | None,
    pages_unreliable_a: bool,
    pages_unreliable_b: bool,
) -> CompareResult:
    """Compare pages/locator fields between two records.

    Parameters
    ----------
    pages_norm_long_a : str | None
        Normalized long-form pagination from first record.
    pages_norm_long_b : str | None
        Normalized long-form pagination from second record.
    page_first_a : str | None
        First page from first record.
    page_first_b : str | None
        First page from second record.
    page_last_a : str | None
        Last page from first record.
    page_last_b : str | None
        Last page from second record.
    article_number_a : str | None
        Article number from first record.
    article_number_b : str | None
        Article number from second record.
    pages_unreliable_a : bool
        Whether pagination is unreliable for first record.
    pages_unreliable_b : bool
        Whether pagination is unreliable for second record.

    Returns
    -------
    tuple[str, float | None, list[str]]
        (level, similarity, warnings)
        - level: 'unreliable', 'exact', 'compatible', 'mismatch', or 'missing'
        - similarity: None (pagination comparison is discrete)
        - warnings: List of warning codes (e.g., 'pages_unreliable')

    Notes
    -----
    Levels:
    - unreliable: Either record has pages_unreliable flag → near-zero weight
    - exact: pages_norm_long or article_number match
    - compatible: First pages match (conservative)
    - mismatch: Present but incompatible
    - missing: One or both missing
    """
    warnings: list[str] = []

    # Check unreliable flag first
    if pages_unreliable_a or pages_unreliable_b:
        warnings.append("pages_unreliable")
        return ("unreliable", None, warnings)

    # Check article numbers if available
    if article_number_a and article_number_b:
        if article_number_a == article_number_b:
            return ("exact", None, warnings)
        else:
            return ("mismatch", None, warnings)

    # Check full pagination
    if pages_norm_long_a and pages_norm_long_b:
        if pages_norm_long_a == pages_norm_long_b:
            return ("exact", None, warnings)
        # Both present but different — fall through to page_first check
        # before declaring mismatch, as page ranges can differ in formatting

    # Check first page compatibility
    if page_first_a and page_first_b:
        if page_first_a == page_first_b:
            # First pages match - compatible
            return ("compatible", None, warnings)
        else:
            return ("mismatch", None, warnings)

    # Both have pages_norm_long but differ and no page_first to disambiguate
    if pages_norm_long_a and pages_norm_long_b:
        return ("mismatch", None, warnings)

    # If nothing to compare
    if not pages_norm_long_a and not pages_norm_long_b and not page_first_a and not page_first_b:
        return ("missing", None, warnings)

    # One has pages, other doesn't
    return ("missing", None, warnings)


# ---------------------------------------------------------------------------
# Field extractors - map CanonicalRecord pairs to comparator arguments
# ---------------------------------------------------------------------------


def _extract_doi(a: "CanonicalRecord", b: "CanonicalRecord") -> dict[str, Any]:
    return {"doi_a": a.canon.doi_norm, "doi_b": b.canon.doi_norm}


def _extract_pmid(a: "CanonicalRecord", b: "CanonicalRecord") -> dict[str, Any]:
    return {"pmid_a": a.canon.pmid_norm, "pmid_b": b.canon.pmid_norm}


def _extract_title(a: "CanonicalRecord", b: "CanonicalRecord") -> dict[str, Any]:
    return {
        "title_a": a.canon.title_norm_basic,
        "title_b": b.canon.title_norm_basic,
        "shingles_a": a.keys.title_shingles,
        "shingles_b": b.keys.title_shingles,
        "truncated_a": a.flags.title_truncated,
        "truncated_b": b.flags.title_truncated,
    }


def _extract_authors(a: "CanonicalRecord", b: "CanonicalRecord") -> dict[str, Any]:
    return {
        "first_author_sig_a": a.canon.first_author_sig,
        "first_author_sig_b": b.canon.first_author_sig,
        "author_sig_strict_a": a.canon.author_sig_strict,
        "author_sig_strict_b": b.canon.author_sig_strict,
    }


def _extract_year(a: "CanonicalRecord", b: "CanonicalRecord") -> dict[str, Any]:
    return {"year_a": a.canon.year_norm, "year_b": b.canon.year_norm}


def _extract_journal(a: "CanonicalRecord", b: "CanonicalRecord") -> dict[str, Any]:
    return {"journal_a": a.canon.journal_norm, "journal_b": b.canon.journal_norm}


def _extract_pages(a: "CanonicalRecord", b: "CanonicalRecord") -> dict[str, Any]:
    return {
        "pages_norm_long_a": a.canon.pages_norm_long,
        "pages_norm_long_b": b.canon.pages_norm_long,
        "page_first_a": a.canon.page_first,
        "page_first_b": b.canon.page_first,
        "page_last_a": a.canon.page_last,
        "page_last_b": b.canon.page_last,
        "article_number_a": a.canon.article_number,
        "article_number_b": b.canon.article_number,
        "pages_unreliable_a": a.flags.pages_unreliable,
        "pages_unreliable_b": b.flags.pages_unreliable,
    }


# ---------------------------------------------------------------------------
# Field registry - ordered list for deterministic iteration
# ---------------------------------------------------------------------------


FIELD_CONFIGS: tuple[FieldConfig, ...] = (
    FieldConfig(name="doi", extractor=_extract_doi, comparator=compare_doi),
    FieldConfig(name="pmid", extractor=_extract_pmid, comparator=compare_pmid),
    FieldConfig(name="title", extractor=_extract_title, comparator=compare_title),
    FieldConfig(name="authors", extractor=_extract_authors, comparator=compare_authors),
    FieldConfig(name="year", extractor=_extract_year, comparator=compare_year),
    FieldConfig(name="journal", extractor=_extract_journal, comparator=compare_journal),
    FieldConfig(name="pages", extractor=_extract_pages, comparator=compare_pages),
)
