"""Centralized tag mappings for different bibliographic formats.

This module defines which tags correspond to which fields across different
source formats (RIS, NBIB, etc.). Adding a new format requires only adding
a new entry to TAG_MAPPINGS.
"""

# Tag mappings: format -> field -> list of tag names (priority order)
TAG_MAPPINGS: dict[str, dict[str, list[str]]] = {
    "ris": {
        "doi": ["DO", "DI", "M3"],
        "doi_url": ["UR", "L1", "L2", "L3", "L4"],
        "pmid": ["PM"],
        "pmcid": ["PMC"],
        "title": ["TI", "T1"],
        "author": ["AU", "A1"],
        "year": ["PY", "Y1", "DA"],
        "journal_full": ["JF", "JO", "T2"],
        "journal_abbrev": ["JA", "J1", "J2"],
        "volume": ["VL"],
        "issue": ["IS"],
        "pages_start": ["SP"],
        "pages_end": ["EP"],
        "abstract": ["AB", "N2"],
        "language": ["LA"],
        "publication_type": ["TY"],
    },
    "nbib": {
        "doi": ["AID", "LID"],
        "doi_url": ["UR"],
        "pmid": ["PMID"],
        "pmid_aid": ["AID", "LID"],  # PMID can also be in AID with [pmid] suffix
        "pmcid": ["PMC"],
        "pmcid_aid": ["AID", "LID"],  # PMCID can also be in AID with [pmc] suffix
        "title": ["TI"],
        "author": ["AU", "FAU"],
        "year": ["DP", "DEP", "DA"],
        "journal_full": ["JT"],
        "journal_abbrev": ["TA"],
        "volume": ["VI"],
        "issue": ["IP"],
        "pages": ["PG"],
        "abstract": ["AB"],
        "language": ["LA"],
        "publication_type": ["PT"],
    },
    "wos": {
        "doi": ["DI", "D2"],
        "doi_url": [],
        "pmid": ["PM"],
        "pmcid": [],
        "title": ["TI"],
        "author": ["AU", "AF"],
        "year": ["PY"],
        "journal_full": ["SO"],
        "journal_abbrev": ["J9", "JI"],
        "volume": ["VL"],
        "issue": ["IS"],
        "pages_start": ["BP"],
        "pages_end": ["EP"],
        "abstract": ["AB"],
        "language": ["LA"],
        "publication_type": ["DT", "PT"],
    },
    "bibtex": {
        "doi": ["doi"],
        "doi_url": ["url"],
        "pmid": [],
        "pmcid": [],
        "title": ["title"],
        "author": ["author"],
        "year": ["year"],
        "journal_full": ["journal"],
        "journal_abbrev": ["journaltitle", "shortjournal"],
        "volume": ["volume"],
        "issue": ["number"],
        "pages": ["pages"],
        "abstract": ["abstract"],
        "language": ["language"],
        "publication_type": ["__bibtex_entrytype"],
    },
    "endnote_tagged": {
        "doi": ["R"],
        "doi_url": ["U"],
        "pmid": ["M"],
        "pmcid": [],
        "title": ["T"],
        "author": ["A"],
        "year": ["D"],
        "journal_full": ["J", "B"],
        "journal_abbrev": [],
        "volume": ["V"],
        "issue": ["N"],
        "pages": ["P"],
        "abstract": ["X"],
        "language": ["G"],
        "publication_type": ["0"],
    },
}

# "pubmed" is the source_format used by the PubMed parser; identical to "nbib"
TAG_MAPPINGS["pubmed"] = TAG_MAPPINGS["nbib"]


def get_tags(source_format: str, field: str) -> list[str]:
    """Get tag names for a field in a given format.

    Parameters
    ----------
    source_format : str
        Source format ('ris', 'nbib', or 'unknown').
    field : str
        Field name (e.g., 'doi', 'title', 'author').

    Returns
    -------
    list[str]
        List of tag names for the field, in priority order.
        Returns empty list if format or field not found.
    """
    if source_format not in TAG_MAPPINGS:
        raise ValueError(
            f"Unsupported format for normalization: {source_format!r}. "
            f"Supported formats: {sorted(TAG_MAPPINGS)}"
        )
    return TAG_MAPPINGS[source_format].get(field, [])
