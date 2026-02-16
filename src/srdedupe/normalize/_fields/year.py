"""Year extraction."""

from typing import Any

from srdedupe.models.records import RawTag

from .._helpers import YEAR_RE
from .._provenance import add_transform, build_provenance_entry
from .._result_types import YearResult
from ..tag_mappings import get_tags


def extract_year(raw_tags: list[RawTag], source_format: str) -> tuple[YearResult, dict[str, Any]]:
    """Extract year from raw tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    tuple[YearResult, dict[str, Any]]
        (result, provenance_dict)
    """
    year_tags = get_tags(source_format, "year")

    year_raw = None
    year_norm = None
    year_source = None
    year_idx = None

    for idx, tag in enumerate(raw_tags):
        if tag.tag in year_tags:
            value = tag.value_raw_joined.strip()
            if value:
                match = YEAR_RE.search(value)
                if match:
                    year_raw = value
                    year_norm = int(match.group(0))
                    year_source = f"{source_format.upper()}.{tag.tag}"
                    year_idx = idx
                    break

    if not year_norm or year_idx is None:
        return YearResult(None, None, None), {}

    prov = build_provenance_entry(
        "canon.year_norm",
        raw_tags,
        [year_idx],
        source_format,
        [add_transform("extract_year", "Extract first 4-digit year from date field")],
        "high",
    )

    return YearResult(year_raw, year_norm, year_source), prov
