"""Result dataclasses for normalization functions.

These dataclasses replace large tuples with named, typed structures
for better readability and maintainability.
"""

from dataclasses import dataclass

from srdedupe.models.records import AuthorParsed


@dataclass(frozen=True)
class DoiResult:
    """Result of DOI normalization.

    Attributes
    ----------
    raw : str | None
        Raw extracted DOI.
    norm : str | None
        Normalized DOI.
    url : str | None
        Canonical DOI URL.
    """

    raw: str | None
    norm: str | None
    url: str | None


@dataclass(frozen=True)
class PmidResult:
    """Result of PMID/PMCID normalization.

    Attributes
    ----------
    pmid_raw : str | None
        Raw PMID string.
    pmid_norm : str | None
        Normalized PMID (digits only).
    pmcid : str | None
        PMCID with PMC prefix.
    """

    pmid_raw: str | None
    pmid_norm: str | None
    pmcid: str | None


@dataclass(frozen=True)
class TitleResult:
    """Result of title normalization.

    Attributes
    ----------
    raw : str | None
        Raw title.
    norm_basic : str | None
        Normalized title.
    """

    raw: str | None
    norm_basic: str | None


@dataclass(frozen=True)
class AuthorsResult:
    """Result of author normalization.

    Attributes
    ----------
    raw : list[str] | None
        Raw author strings.
    parsed : list[AuthorParsed] | None
        Parsed author objects.
    first_sig : str | None
        First author signature (family|initials).
    sig_strict : list[str] | None
        Strict signatures (ordered).
    sig_loose : list[str] | None
        Loose signatures (sorted, first initial only).
    """

    raw: list[str] | None
    parsed: list[AuthorParsed] | None
    first_sig: str | None
    sig_strict: list[str] | None
    sig_loose: list[str] | None


@dataclass(frozen=True)
class YearResult:
    """Result of year extraction.

    Attributes
    ----------
    raw : str | None
        Raw year/date string.
    norm : int | None
        Normalized 4-digit year.
    source : str | None
        Source tag hint (e.g., 'RIS.PY').
    """

    raw: str | None
    norm: int | None
    source: str | None


@dataclass(frozen=True)
class JournalResult:
    """Result of journal normalization.

    Attributes
    ----------
    full : str | None
        Full journal name.
    abbrev : str | None
        Abbreviated journal name.
    norm : str | None
        Normalized journal name.
    """

    full: str | None
    abbrev: str | None
    norm: str | None


@dataclass(frozen=True)
class PagesResult:
    """Result of pages normalization.

    Attributes
    ----------
    raw : str | None
        Raw pages string.
    norm_long : str | None
        Normalized long-form pagination.
    first : str | None
        First page number.
    last : str | None
        Last page number.
    article_number : str | None
        Electronic article number (e.g., 'e12345').
    unreliable : bool
        Whether pagination is unreliable.
    """

    raw: str | None
    norm_long: str | None
    first: str | None
    last: str | None
    article_number: str | None
    unreliable: bool


@dataclass(frozen=True)
class OtherFieldsResult:
    """Result of other field extraction.

    Attributes
    ----------
    volume : str | None
        Volume number.
    issue : str | None
        Issue number.
    abstract_raw : str | None
        Raw abstract.
    abstract_norm : str | None
        Normalized abstract.
    language : str | None
        Publication language.
    publication_type : list[str] | None
        Publication type(s).
    """

    volume: str | None
    issue: str | None
    abstract_raw: str | None
    abstract_norm: str | None
    language: str | None
    publication_type: list[str] | None
