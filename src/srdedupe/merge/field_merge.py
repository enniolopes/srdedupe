"""Field-level merge rules for canonical records."""

from collections import Counter
from typing import Any

from srdedupe.merge.models import MergeProvenance, MergeProvenanceField
from srdedupe.models import Canon, CanonicalRecord


def merge_canon_fields(
    records: list[CanonicalRecord],
    survivor_rid: str,
) -> tuple[Canon, MergeProvenance]:
    """Merge canonical fields from cluster records.

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.
    survivor_rid : str
        Survivor record ID.

    Returns
    -------
    tuple[Canon, MergeProvenance]
        Merged canon and provenance.

    Raises
    ------
    RuntimeError
        If multiple distinct strong IDs exist in cluster.
    """
    provenance = MergeProvenance()
    survivor = next((r for r in records if r.rid == survivor_rid), records[0])

    # --- Strong identifiers (must be unique across cluster) ---
    doi_norm, doi_prov = _merge_strong_id(records, "doi_norm")
    pmid_norm, pmid_prov = _merge_strong_id(records, "pmid_norm")
    pmcid, pmcid_prov = _merge_strong_id(records, "pmcid")
    provenance.fields["doi_norm"] = doi_prov
    provenance.fields["pmid_norm"] = pmid_prov
    provenance.fields["pmcid"] = pmcid_prov

    # --- Text fields (prefer longest, carry normalized form from source) ---
    title_record, title_prov = _pick_longest_text_record(records, "title_raw", survivor_rid)
    abstract_record, abstract_prov = _pick_longest_text_record(
        records, "abstract_raw", survivor_rid
    )
    provenance.fields["title_raw"] = title_prov
    provenance.fields["abstract_raw"] = abstract_prov

    title_raw = title_record.canon.title_raw if title_record else None
    title_norm_basic = title_record.canon.title_norm_basic if title_record else None
    abstract_raw = abstract_record.canon.abstract_raw if abstract_record else None
    abstract_norm_basic = abstract_record.canon.abstract_norm_basic if abstract_record else None

    # --- Authors (prefer record with most parsed authors) ---
    author_record, author_prov = _pick_best_author_record(records, survivor_rid)
    provenance.fields["authors"] = author_prov

    authors_raw = author_record.canon.authors_raw if author_record else None
    authors_parsed = author_record.canon.authors_parsed if author_record else None
    first_author_sig = author_record.canon.first_author_sig if author_record else None
    author_sig_strict = author_record.canon.author_sig_strict if author_record else None
    author_sig_loose = author_record.canon.author_sig_loose if author_record else None

    # --- Year (mode across cluster) ---
    year_norm, year_source, year_prov = _merge_year(records, survivor_rid)
    provenance.fields["year_norm"] = year_prov

    # --- Journal (prefer longest, carry normalized form) ---
    journal_record, journal_prov = _pick_longest_text_record(records, "journal_full", survivor_rid)
    provenance.fields["journal_full"] = journal_prov

    journal_full = journal_record.canon.journal_full if journal_record else None
    journal_norm = journal_record.canon.journal_norm if journal_record else None

    # --- Pagination (prefer reliable pages) ---
    page_record, pages_prov = _pick_best_pagination_record(records, survivor_rid)
    provenance.fields["pages"] = pages_prov

    # --- Multi-value fields (union distinct) ---
    language, lang_prov = _merge_multi_value(records, "language", survivor_rid)
    pub_type, pub_type_prov = _merge_multi_value(records, "publication_type", survivor_rid)
    provenance.fields["language"] = lang_prov
    provenance.fields["publication_type"] = pub_type_prov

    merged_canon = Canon(
        doi=doi_norm,
        doi_norm=doi_norm,
        doi_url=f"https://doi.org/{doi_norm}" if doi_norm else None,
        pmid=pmid_norm,
        pmid_norm=pmid_norm,
        pmcid=pmcid,
        title_raw=title_raw,
        title_norm_basic=title_norm_basic,
        abstract_raw=abstract_raw,
        abstract_norm_basic=abstract_norm_basic,
        authors_raw=authors_raw,
        authors_parsed=authors_parsed,
        first_author_sig=first_author_sig,
        author_sig_strict=author_sig_strict,
        author_sig_loose=author_sig_loose,
        year_raw=str(year_norm) if year_norm else None,
        year_norm=year_norm,
        year_source=year_source,
        journal_full=journal_full,
        journal_abbrev=survivor.canon.journal_abbrev,
        journal_norm=journal_norm,
        volume=page_record.canon.volume if page_record else None,
        issue=page_record.canon.issue if page_record else None,
        pages_raw=page_record.canon.pages_raw if page_record else None,
        pages_norm_long=page_record.canon.pages_norm_long if page_record else None,
        page_first=page_record.canon.page_first if page_record else None,
        page_last=page_record.canon.page_last if page_record else None,
        article_number=page_record.canon.article_number if page_record else None,
        language=language,
        publication_type=pub_type,
    )

    return merged_canon, provenance


def _merge_strong_id(
    records: list[CanonicalRecord], field_name: str
) -> tuple[str | None, MergeProvenanceField]:
    """Merge strong identifier field.

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.
    field_name : str
        Field name to merge.

    Returns
    -------
    tuple[str | None, MergeProvenanceField]
        Merged value and provenance.

    Raises
    ------
    RuntimeError
        If multiple distinct non-null values exist.
    """
    values = [
        (getattr(r.canon, field_name), r.rid) for r in records if getattr(r.canon, field_name)
    ]

    if not values:
        return None, MergeProvenanceField(from_rid="", rule="no_value_in_cluster")

    unique_values = {v for v, _ in values}
    if len(unique_values) > 1:
        raise RuntimeError(
            f"Multiple distinct {field_name} values in AUTO cluster: {unique_values}"
        )

    value, rid = values[0]
    return value, MergeProvenanceField(from_rid=rid, rule="unique_id_in_cluster")


def _pick_longest_text_record(
    records: list[CanonicalRecord],
    field_name: str,
    survivor_rid: str,
) -> tuple[CanonicalRecord | None, MergeProvenanceField]:
    """Pick the record with the longest non-null text value for a field.

    Returns the full record so callers can also access its normalized companion
    (e.g. title_norm_basic alongside title_raw).

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.
    field_name : str
        Field name to compare.
    survivor_rid : str
        Survivor record ID for tie-breaking.

    Returns
    -------
    tuple[CanonicalRecord | None, MergeProvenanceField]
        Chosen record and provenance.
    """
    candidates = [
        (r, len(getattr(r.canon, field_name))) for r in records if getattr(r.canon, field_name)
    ]

    if not candidates:
        return None, MergeProvenanceField(from_rid=survivor_rid, rule="no_value_in_cluster")

    candidates.sort(key=lambda x: (-x[1], x[0].rid != survivor_rid, x[0].rid))
    chosen = candidates[0][0]

    return chosen, MergeProvenanceField(from_rid=chosen.rid, rule="prefer_longest_non_null")


def _pick_best_author_record(
    records: list[CanonicalRecord], survivor_rid: str
) -> tuple[CanonicalRecord | None, MergeProvenanceField]:
    """Pick the record with the most parsed authors.

    Returns the full record so author signatures are carried over.

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.
    survivor_rid : str
        Survivor record ID for tie-breaking.

    Returns
    -------
    tuple[CanonicalRecord | None, MergeProvenanceField]
        Chosen record and provenance.
    """
    candidates = [(r, len(r.canon.authors_parsed)) for r in records if r.canon.authors_parsed]

    if not candidates:
        return None, MergeProvenanceField(from_rid=survivor_rid, rule="no_authors_in_cluster")

    candidates.sort(key=lambda x: (-x[1], x[0].rid != survivor_rid, x[0].rid))
    chosen = candidates[0][0]

    return chosen, MergeProvenanceField(from_rid=chosen.rid, rule="max_parsed_authors")


def _merge_year(
    records: list[CanonicalRecord], survivor_rid: str
) -> tuple[int | None, str | None, MergeProvenanceField]:
    """Merge year by choosing the mode (most common value).

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.
    survivor_rid : str
        Survivor record ID.

    Returns
    -------
    tuple[int | None, str | None, MergeProvenanceField]
        Merged year_norm, year_source, and provenance.
    """
    years = [(r.canon.year_norm, r.rid) for r in records if r.canon.year_norm]

    if not years:
        return None, None, MergeProvenanceField(from_rid=survivor_rid, rule="no_year_in_cluster")

    year_counts = Counter(y for y, _ in years)
    mode_year, mode_count = year_counts.most_common(1)[0]

    ties = [y for y, c in year_counts.items() if c == mode_count]
    if len(ties) > 1:
        survivor_year = next((y for y, rid in years if rid == survivor_rid), None)
        chosen_year = survivor_year if survivor_year in ties else min(ties)
    else:
        chosen_year = mode_year

    chosen_rid = next(rid for y, rid in years if y == chosen_year)

    return (
        chosen_year,
        "merge:mode",
        MergeProvenanceField(from_rid=chosen_rid, rule="year_mode"),
    )


def _pick_best_pagination_record(
    records: list[CanonicalRecord], survivor_rid: str
) -> tuple[CanonicalRecord, MergeProvenanceField]:
    """Pick the record with the best pagination data.

    Prefers reliable pages with both page_first and page_last.

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.
    survivor_rid : str
        Survivor record ID.

    Returns
    -------
    tuple[CanonicalRecord, MergeProvenanceField]
        Chosen record and provenance.
    """
    candidates = [
        (
            r,
            not r.flags.pages_unreliable,
            r.canon.page_first is not None and r.canon.page_last is not None,
        )
        for r in records
    ]

    candidates.sort(key=lambda x: (not x[1], not x[2], x[0].rid != survivor_rid, x[0].rid))
    chosen = candidates[0][0]

    return chosen, MergeProvenanceField(from_rid=chosen.rid, rule="prefer_reliable_pages")


def _merge_multi_value(
    records: list[CanonicalRecord], field_name: str, survivor_rid: str
) -> tuple[Any, MergeProvenanceField]:
    """Merge multi-value field by computing a distinct sorted union.

    Parameters
    ----------
    records : list[CanonicalRecord]
        Cluster records.
    field_name : str
        Field name to merge.
    survivor_rid : str
        Survivor record ID.

    Returns
    -------
    tuple[Any, MergeProvenanceField]
        Merged value and provenance.
    """
    seen: set[str] = set()
    values: list[str] = []
    rids: list[str] = []

    for record in records:
        value = getattr(record.canon, field_name)
        if not value:
            continue
        items = value if isinstance(value, list) else [value]
        for item in items:
            if item not in seen:
                seen.add(item)
                values.append(item)
                rids.append(record.rid)

    if not values:
        return None, MergeProvenanceField(from_rid=survivor_rid, rule="no_value_in_cluster")

    values = sorted(values) if all(isinstance(v, str) for v in values) else values
    result = values if len(values) > 1 else values[0]
    from_rid = rids if len(rids) > 1 else rids[0]
    rule = "union_distinct_sorted" if len(values) > 1 else "single_value"

    return result, MergeProvenanceField(from_rid=from_rid, rule=rule)
