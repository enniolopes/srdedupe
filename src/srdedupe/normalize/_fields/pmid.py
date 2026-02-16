"""PMID/PMCID normalization."""

from typing import Any

from srdedupe.models.records import RawTag

from .._helpers import PMCID_AID_RE, PMID_AID_RE, find_tag_value
from .._provenance import add_transform, build_provenance_entry
from .._result_types import PmidResult
from ..tag_mappings import get_tags

# Formats that use AID/LID tags as fallback for PMID/PMCID
_NBIB_LIKE_FORMATS = frozenset({"nbib", "pubmed"})


def normalize_pmid_pmcid(
    raw_tags: list[RawTag], source_format: str
) -> tuple[PmidResult, dict[str, Any]]:
    """Normalize PMID and PMCID from raw tags.

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    tuple[PmidResult, dict[str, Any]]
        (result, provenance_dict)
    """
    pmid_raw = None
    pmid_norm = None
    pmcid = None
    prov: dict[str, Any] = {}

    # Extract PMID
    pmid_tags = get_tags(source_format, "pmid")
    pmid_raw, pmid_idx = find_tag_value(raw_tags, pmid_tags)

    if pmid_raw and pmid_idx is not None:
        pmid_norm = "".join(c for c in pmid_raw if c.isdigit())
        if pmid_norm:
            prov.update(
                build_provenance_entry(
                    "canon.pmid_norm",
                    raw_tags,
                    [pmid_idx],
                    source_format,
                    [add_transform("extract_digits", "Extract digits only from PMID")],
                    "high",
                )
            )
        else:
            pmid_norm = None

    # Fallback: check AID/LID for PMID in NBIB-like formats
    if not pmid_norm and source_format in _NBIB_LIKE_FORMATS:
        aid_tags = get_tags(source_format, "pmid_aid")
        for idx, tag in enumerate(raw_tags):
            if tag.tag in aid_tags:
                match = PMID_AID_RE.search(tag.value_raw_joined)
                if match:
                    pmid_norm = match.group(1)
                    prov.update(
                        build_provenance_entry(
                            "canon.pmid_norm",
                            raw_tags,
                            [idx],
                            source_format,
                            [add_transform("extract_from_aid", "Extract PMID from AID [pmid] tag")],
                            "high",
                        )
                    )
                    break

    # Extract PMCID
    pmcid_tags = get_tags(source_format, "pmcid")
    pmcid_raw, _ = find_tag_value(raw_tags, pmcid_tags)

    if pmcid_raw:
        pmcid = pmcid_raw if pmcid_raw.startswith("PMC") else f"PMC{pmcid_raw}"

    # Fallback: check AID/LID for PMCID in NBIB-like formats
    if not pmcid and source_format in _NBIB_LIKE_FORMATS:
        aid_tags = get_tags(source_format, "pmcid_aid")
        for tag in raw_tags:
            if tag.tag in aid_tags:
                match = PMCID_AID_RE.search(tag.value_raw_joined)
                if match:
                    pmcid = match.group(1).upper()
                    break

    return PmidResult(pmid_raw, pmid_norm, pmcid), prov
