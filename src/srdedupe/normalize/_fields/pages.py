"""Pages normalization."""

from typing import Any

from srdedupe.models.records import RawTag

from .._helpers import DASH_NORMALIZE_RE, ELOCATOR_RE, PAGE_RANGE_RE, find_tag_value
from .._provenance import add_transform, build_provenance_entry
from .._result_types import PagesResult
from ..tag_mappings import get_tags


def normalize_pages(
    raw_tags: list[RawTag], source_format: str
) -> tuple[PagesResult, dict[str, Any]]:
    """Normalize pages from raw tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    tuple[PagesResult, dict[str, Any]]
        (result, provenance_dict)
    """
    pages_raw = None
    page_first = None
    page_last = None
    article_number = None
    pages_unreliable = False
    page_indices: list[int] = []

    # Formats with separate start/end page tags (RIS SP/EP, WoS BP/EP)
    sp_tags = get_tags(source_format, "pages_start")
    ep_tags = get_tags(source_format, "pages_end")

    if sp_tags or ep_tags:
        sp_val, sp_idx = find_tag_value(raw_tags, sp_tags) if sp_tags else (None, None)
        ep_val, ep_idx = find_tag_value(raw_tags, ep_tags) if ep_tags else (None, None)

        if sp_val and sp_idx is not None:
            page_first = sp_val
            page_indices.append(sp_idx)
        if ep_val and ep_idx is not None:
            page_last = ep_val
            page_indices.append(ep_idx)

        if page_first and page_last:
            pages_raw = f"{page_first}-{page_last}"
        elif page_first:
            pages_raw = page_first
        elif page_last:
            pages_raw = page_last

    # Fallback or primary: formats with a combined pages tag
    if not pages_raw:
        pg_tags = get_tags(source_format, "pages")
        pages_raw, pg_idx = find_tag_value(raw_tags, pg_tags)

        if pages_raw and pg_idx is not None:
            page_indices.append(pg_idx)
            page_match = PAGE_RANGE_RE.match(pages_raw)
            if page_match:
                page_first = page_match.group(1)
                page_last = page_match.group(2)
            elif pages_raw.isdigit():
                page_first = pages_raw
            else:
                pages_unreliable = True

    # Check for electronic locators
    if pages_raw and ELOCATOR_RE.match(pages_raw):
        article_number = pages_raw
        pages_unreliable = True
        page_first = None
        page_last = None

    # Normalize pages
    pages_norm_long = None
    if pages_raw and not pages_unreliable:
        pages_norm_long = DASH_NORMALIZE_RE.sub("-", pages_raw)
        pages_norm_long = pages_norm_long.replace(" ", "")

    # Build provenance
    prov: dict[str, Any] = {}
    if pages_raw and page_indices:
        prov = build_provenance_entry(
            "canon.pages_norm_long",
            raw_tags,
            page_indices,
            source_format,
            [add_transform("normalize_pages", "Normalize separators, remove spaces")],
            "medium" if pages_unreliable else "high",
        )

    return (
        PagesResult(
            pages_raw,
            pages_norm_long,
            page_first,
            page_last,
            article_number,
            pages_unreliable,
        ),
        prov,
    )
