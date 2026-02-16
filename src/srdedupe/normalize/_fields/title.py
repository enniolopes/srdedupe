"""Title normalization."""

from typing import Any

from srdedupe.models.records import RawTag

from .._helpers import find_tag_value, normalize_text_for_matching
from .._provenance import add_transform, build_provenance_entry
from .._result_types import TitleResult
from ..tag_mappings import get_tags


def normalize_title(
    raw_tags: list[RawTag], source_format: str
) -> tuple[TitleResult, dict[str, Any]]:
    """Normalize title from raw tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    tuple[TitleResult, dict[str, Any]]
        (result, provenance_dict)
    """
    title_tags = get_tags(source_format, "title")
    title_raw, title_idx = find_tag_value(raw_tags, title_tags)

    if not title_raw or title_idx is None:
        return TitleResult(None, None), {}

    title_norm = normalize_text_for_matching(title_raw)

    prov = build_provenance_entry(
        "canon.title_norm_basic",
        raw_tags,
        [title_idx],
        source_format,
        [
            add_transform(
                "normalize_title",
                "NFKC, casefold, strip accents, collapse whitespace, remove cosmetic punct",
            )
        ],
        "high",
    )

    return TitleResult(title_raw, title_norm), prov
