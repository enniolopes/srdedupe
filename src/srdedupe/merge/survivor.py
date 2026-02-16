"""Survivor selection for canonical merge."""

from srdedupe.models import CanonicalRecord


def compute_metadata_completeness_score(record: CanonicalRecord) -> int:
    """Compute metadata completeness score.

    Count non-null values among: year_norm, journal_norm, volume, issue,
    pages_norm_long, language, publication_type, article_number.

    Parameters
    ----------
    record : CanonicalRecord
        Record to score.

    Returns
    -------
    int
        Completeness score (0-8).
    """
    canon = record.canon
    fields = [
        canon.year_norm,
        canon.journal_norm,
        canon.volume,
        canon.issue,
        canon.pages_norm_long,
        canon.language,
        canon.publication_type,
        canon.article_number,
    ]
    return sum(1 for f in fields if f is not None)


def select_survivor(records: list[CanonicalRecord]) -> str:
    """Select survivor RID from cluster records.

    Selection is based on lexicographic tuple ranking:
    1. has_doi_norm (true > false)
    2. has_pmid_norm (true > false)
    3. title_present (true > false)
    4. abstract_present (true > false)
    5. authors_count (higher > lower)
    6. metadata_completeness_score (higher > lower)
    7. tie-breaker: smallest rid lexicographically

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.

    Returns
    -------
    str
        Survivor record ID.

    Raises
    ------
    ValueError
        If records list is empty.
    """
    if not records:
        raise ValueError("Cannot select survivor from empty records list")

    def ranking_key(record: CanonicalRecord) -> tuple[bool, bool, bool, bool, int, int, str]:
        """Compute ranking key for survivor selection."""
        has_doi = record.canon.doi_norm is not None
        has_pmid = record.canon.pmid_norm is not None
        has_title = record.canon.title_raw is not None
        has_abstract = record.canon.abstract_raw is not None
        author_count = len(record.canon.authors_parsed) if record.canon.authors_parsed else 0
        completeness = compute_metadata_completeness_score(record)

        # Return tuple for lexicographic comparison
        # Negate bools/ints for descending order, use rid as is for ascending tie-break
        return (
            not has_doi,  # False (has DOI) comes first
            not has_pmid,
            not has_title,
            not has_abstract,
            -author_count,  # Higher count comes first
            -completeness,
            record.rid,  # Lexicographic tie-breaker (ascending)
        )

    # Sort by ranking key and return first
    sorted_records = sorted(records, key=ranking_key)
    return sorted_records[0].rid
