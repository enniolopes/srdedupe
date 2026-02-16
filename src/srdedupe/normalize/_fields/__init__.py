"""Field normalization functions.

Individual field normalizers that extract and normalize bibliographic
metadata from raw tags. Each function is pure, deterministic, and
returns a typed result structure with provenance information.
"""

from .authors import normalize_authors
from .doi import normalize_doi
from .journal import normalize_journal
from .other import extract_other_fields
from .pages import normalize_pages
from .pmid import normalize_pmid_pmcid
from .title import normalize_title
from .year import extract_year

__all__ = [
    "extract_other_fields",
    "extract_year",
    "normalize_authors",
    "normalize_doi",
    "normalize_journal",
    "normalize_pages",
    "normalize_pmid_pmcid",
    "normalize_title",
]
