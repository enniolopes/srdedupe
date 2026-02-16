"""DOI normalization."""

from typing import Any
from urllib.parse import unquote, urlparse

from srdedupe.models.records import RawTag

from .._helpers import DOI_SUFFIX_RE, DOI_URL_RE, find_tag_value
from .._provenance import add_transform, build_provenance_entry
from .._result_types import DoiResult
from ..tag_mappings import get_tags


def normalize_doi(raw_tags: list[RawTag], source_format: str) -> tuple[DoiResult, dict[str, Any]]:
    """Normalize DOI from raw tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    tuple[DoiResult, dict[str, Any]]
        (result, provenance_dict)
    """
    doi_tags = get_tags(source_format, "doi")
    doi_raw, doi_idx = find_tag_value(
        raw_tags,
        doi_tags,
        predicate=lambda v: _is_doi_candidate(v, source_format),
    )

    # Fallback: search in URLs
    if not doi_raw:
        url_tags = get_tags(source_format, "doi_url")
        for idx, tag in enumerate(raw_tags):
            if tag.tag in url_tags:
                match = DOI_URL_RE.search(tag.value_raw_joined)
                if match:
                    doi_raw = match.group(1)
                    doi_idx = idx
                    break

    if not doi_raw or doi_idx is None:
        return DoiResult(None, None, None), {}

    doi_norm = _normalize_doi_string(doi_raw)
    if not doi_norm:
        return DoiResult(doi_raw, None, None), {}

    doi_url = f"https://doi.org/{doi_norm}"

    transforms = _get_doi_transforms(doi_raw, doi_norm)
    confidence = "high" if raw_tags[doi_idx].tag in doi_tags[:2] else "medium"

    prov: dict[str, Any] = {}
    prov.update(
        build_provenance_entry(
            "canon.doi_norm",
            raw_tags,
            [doi_idx],
            source_format,
            transforms,
            confidence,
        )
    )
    prov.update(
        build_provenance_entry(
            "canon.doi_url",
            raw_tags,
            [doi_idx],
            source_format,
            transforms + [add_transform("generate_canonical_url", "Generate https://doi.org/ URL")],
            confidence,
        )
    )

    return DoiResult(doi_raw, doi_norm, doi_url), prov


def _is_doi_candidate(value: str, source_format: str) -> bool:
    if source_format in ("nbib", "pubmed"):
        return "[doi]" in value.lower() or value.startswith("10.")
    return True


def _normalize_doi_string(doi: str) -> str | None:
    if not doi:
        return None

    doi = doi.strip()

    # Strip NBIB [doi] suffix (e.g., "10.1234/test [doi]")
    doi = DOI_SUFFIX_RE.sub("", doi).strip()

    # Extract from URL
    if doi.startswith(("http://", "https://")):
        try:
            parsed = urlparse(doi)
            doi = parsed.path.lstrip("/")
        except Exception:
            return None

    # Remove prefixes
    for prefix in ["doi:", "DOI:", "doi.org/", "dx.doi.org/"]:
        if doi.casefold().startswith(prefix.casefold()):
            doi = doi[len(prefix) :]
            break

    # URL-decode encoded characters (%2F → /, %28 → (, etc.)
    doi = unquote(doi)

    # Strip trailing citation-artifact punctuation only;
    # parentheses/brackets are valid DOI characters (e.g., 10.1002/(sici)1234)
    doi = doi.rstrip(".,;").strip()

    # Casefold for case-insensitive matching
    doi = doi.casefold()

    # Validate format (must start with 10.)
    if not doi.startswith("10."):
        return None

    return doi


def _get_doi_transforms(doi_raw: str, doi_norm: str) -> list[dict[str, str]]:
    transforms: list[dict[str, str]] = []

    if DOI_SUFFIX_RE.search(doi_raw):
        transforms.append(add_transform("strip_doi_suffix", "Remove [doi] suffix from AID tag"))

    if doi_raw.startswith(("http://", "https://")):
        transforms.append(add_transform("extract_from_url", "Extract DOI from URL"))

    for prefix in ["doi:", "DOI:"]:
        if doi_raw.casefold().startswith(prefix.casefold()):
            transforms.append(add_transform("strip_prefix", f"Remove '{prefix}' prefix"))
            break

    if "%" in doi_raw:
        transforms.append(add_transform("url_decode", "Decode URL-encoded characters"))

    if doi_raw.rstrip(".,;") != doi_raw:
        transforms.append(add_transform("trim_punct", "Remove trailing punctuation"))

    transforms.append(add_transform("casefold", "Apply Unicode case folding"))

    return transforms
