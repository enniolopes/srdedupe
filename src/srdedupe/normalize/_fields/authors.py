"""Author normalization."""

from typing import Any

from srdedupe.models.records import AuthorParsed, RawTag

from .._helpers import INITIALS_RE, SUFFIX_RE, find_all_tag_values, strip_accents
from .._provenance import add_transform, build_provenance_entry
from .._result_types import AuthorsResult
from ..tag_mappings import get_tags

# Formats where no-comma names follow "Given Family" convention
_GIVEN_FAMILY_FORMATS = frozenset({"bibtex", "wos", "endnote_tagged"})

# Formats that concatenate multiple authors in a single tag value
_AND_SEPARATED_FORMATS = frozenset({"bibtex"})


def normalize_authors(
    raw_tags: list[RawTag], source_format: str
) -> tuple[AuthorsResult, dict[str, Any]]:
    """Parse and normalize authors from raw tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    tuple[AuthorsResult, dict[str, Any]]
        (result, provenance_dict)
    """
    author_tags = get_tags(source_format, "author")
    author_matches = find_all_tag_values(
        raw_tags, author_tags, predicate=lambda v: v.casefold() != "et al."
    )

    if not author_matches:
        return AuthorsResult(None, None, None, None, None), {}

    # BibTeX stores multiple authors in a single field separated by " and "
    if source_format in _AND_SEPARATED_FORMATS:
        author_matches = _split_and_separated_authors(author_matches)

    authors_raw = [val for val, _ in author_matches]
    author_indices = [idx for _, idx in author_matches]

    authors_parsed = [_parse_author(author, source_format) for author in authors_raw]

    # Generate normalized signatures for matching
    first_author_sig = None
    author_sig_strict: list[str] = []
    author_sig_loose: list[str] = []

    for i, author in enumerate(authors_parsed):
        if author.family and author.initials:
            family_norm = strip_accents(author.family.casefold())
            sig_strict = f"{family_norm}|{author.initials}"
            author_sig_strict.append(sig_strict)

            if i == 0:
                first_author_sig = sig_strict

            first_initial = author.initials[0] if author.initials else ""
            sig_loose = f"{family_norm}|{first_initial}"
            author_sig_loose.append(sig_loose)

    author_sig_loose_sorted = sorted(set(author_sig_loose)) if author_sig_loose else None
    author_sig_strict_final = author_sig_strict if author_sig_strict else None

    prov = build_provenance_entry(
        "canon.authors_parsed",
        raw_tags,
        author_indices,
        source_format,
        [add_transform("parse_author_names", "Parse author names into structured format")],
        "high",
    )

    if first_author_sig:
        prov.update(
            build_provenance_entry(
                "canon.first_author_sig",
                raw_tags,
                author_indices[:1],
                source_format,
                [
                    add_transform(
                        "generate_author_signature",
                        "Generate casefold+accent-stripped family|initials signature",
                    )
                ],
                "high",
            )
        )

    if author_sig_strict_final:
        prov.update(
            build_provenance_entry(
                "canon.author_sig_strict",
                raw_tags,
                author_indices,
                source_format,
                [
                    add_transform(
                        "generate_author_signature",
                        "Generate ordered casefold+accent-stripped family|initials signatures",
                    )
                ],
                "high",
            )
        )

    if author_sig_loose_sorted:
        prov.update(
            build_provenance_entry(
                "canon.author_sig_loose",
                raw_tags,
                author_indices,
                source_format,
                [
                    add_transform(
                        "generate_loose_signature",
                        "Generate sorted casefold+accent-stripped family|first_initial signatures",
                    )
                ],
                "medium",
            )
        )

    return (
        AuthorsResult(
            authors_raw,
            authors_parsed,
            first_author_sig,
            author_sig_strict_final,
            author_sig_loose_sorted,
        ),
        prov,
    )


def _split_and_separated_authors(
    author_matches: list[tuple[str, int]],
) -> list[tuple[str, int]]:
    """Split BibTeX-style 'Author One and Author Two' into individual entries.

    Parameters
    ----------
    author_matches : list[tuple[str, int]]
        Original (value, index) pairs from tag lookup.

    Returns
    -------
    list[tuple[str, int]]
        Expanded list with one entry per author, sharing the parent tag index.
    """
    expanded: list[tuple[str, int]] = []
    for value, idx in author_matches:
        parts = value.split(" and ")
        for part in parts:
            stripped = part.strip()
            if stripped and stripped.casefold() != "et al.":
                expanded.append((stripped, idx))
    return expanded


def _parse_author(author_str: str, source_format: str) -> AuthorParsed:
    """Parse author string into structured components.

    The no-comma format is disambiguated via source_format:
    - RIS/NBIB: "Family Given" (first word = family)
    - BibTeX/WoS/EndNote: "Given Family" (last word = family)
    """
    family = None
    given = None
    initials = None
    suffix = None

    author_str = author_str.strip()

    if "," in author_str:
        parts = author_str.split(",", 1)
        family = parts[0].strip()
        rest = parts[1].strip() if len(parts) > 1 else ""

        suffix_match = SUFFIX_RE.search(family)
        if suffix_match:
            suffix = suffix_match.group(1)
            family = family[: suffix_match.start()].strip()

        if INITIALS_RE.match(rest):
            initials = "".join(c for c in rest if c.isalpha()).upper()
        else:
            given = rest
            if given:
                initials = "".join(word[0].upper() for word in given.split() if word)
    else:
        parts = author_str.split()
        if len(parts) == 1:
            family = parts[0]
        elif source_format in _GIVEN_FAMILY_FORMATS:
            # "Given Family" convention
            family = parts[-1]
            given = " ".join(parts[:-1])
            initials = "".join(word[0].upper() for word in parts[:-1] if word)
        else:
            # "Family Given" convention (RIS, NBIB default)
            family = parts[0]
            given = " ".join(parts[1:])
            initials = "".join(word[0].upper() for word in parts[1:] if word)

    return AuthorParsed(
        family=family,
        given=given,
        initials=initials,
        suffix=suffix,
        raw=author_str,
    )
