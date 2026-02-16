"""Other fields extraction (volume, issue, abstract, language, publication type)."""

from srdedupe.models.records import RawTag

from .._helpers import find_all_tag_values, find_tag_value, normalize_text_light
from .._result_types import OtherFieldsResult
from ..tag_mappings import get_tags


def extract_other_fields(raw_tags: list[RawTag], source_format: str) -> OtherFieldsResult:
    """Extract other fields (volume, issue, abstract, language, publication_type).

    Parameters
    ----------
    raw_tags : list[RawTag]
        Raw tags from record.
    source_format : str
        Source format ('ris', 'nbib', etc.).

    Returns
    -------
    OtherFieldsResult
        Extracted fields.
    """
    volume, _ = find_tag_value(raw_tags, get_tags(source_format, "volume"))
    issue, _ = find_tag_value(raw_tags, get_tags(source_format, "issue"))

    abstract_raw, _ = find_tag_value(raw_tags, get_tags(source_format, "abstract"))
    abstract_norm = normalize_text_light(abstract_raw) if abstract_raw else None

    language, _ = find_tag_value(raw_tags, get_tags(source_format, "language"))

    pub_type_tags = get_tags(source_format, "publication_type")
    pub_types = [val for val, _ in find_all_tag_values(raw_tags, pub_type_tags)]
    publication_type = pub_types if pub_types else None

    return OtherFieldsResult(
        volume=volume,
        issue=issue,
        abstract_raw=abstract_raw,
        abstract_norm=abstract_norm,
        language=language,
        publication_type=publication_type,
    )
