"""Safety gates for forcing REVIEW decisions.

This module implements safety checks that force REVIEW even when
p_match >= t_high, preventing automatic removal of potentially
valid records.
"""

from srdedupe.decision.models import ReasonCode
from srdedupe.models import CanonicalRecord


def _has_strong_id_match(a: CanonicalRecord, b: CanonicalRecord) -> bool:
    """Check if records share at least one matching strong ID (DOI or PMID)."""
    if a.canon.doi_norm and b.canon.doi_norm and a.canon.doi_norm == b.canon.doi_norm:
        return True
    if a.canon.pmid_norm and b.canon.pmid_norm and a.canon.pmid_norm == b.canon.pmid_norm:
        return True
    return False


def check_safety_gates(
    record_a: CanonicalRecord,
    record_b: CanonicalRecord,
    warnings: list[str],
) -> list[ReasonCode]:
    """Check safety gates and return reasons for forced REVIEW.

    Parameters
    ----------
    record_a : CanonicalRecord
        First record.
    record_b : CanonicalRecord
        Second record.
    warnings : list[str]
        Warnings from scoring stage.

    Returns
    -------
    list[ReasonCode]
        List of reason codes for forced REVIEW (empty if no gates triggered).
    """
    forced_reasons: list[ReasonCode] = []

    # Conflicting strong identifiers
    if (
        record_a.canon.doi_norm
        and record_b.canon.doi_norm
        and record_a.canon.doi_norm != record_b.canon.doi_norm
    ):
        forced_reasons.append(ReasonCode.FORCED_REVIEW_CONFLICTING_DOI)

    if (
        record_a.canon.pmid_norm
        and record_b.canon.pmid_norm
        and record_a.canon.pmid_norm != record_b.canon.pmid_norm
    ):
        forced_reasons.append(ReasonCode.FORCED_REVIEW_CONFLICTING_PMID)

    # Data quality warnings — only when no strong ID match confirms the pair
    if not _has_strong_id_match(record_a, record_b):
        if "title_truncated" in warnings:
            forced_reasons.append(ReasonCode.FORCED_REVIEW_TITLE_TRUNCATED)
        if "pages_unreliable" in warnings:
            forced_reasons.append(ReasonCode.FORCED_REVIEW_PAGES_UNRELIABLE)

    # Special record types
    if record_a.flags.is_erratum_notice or record_b.flags.is_erratum_notice:
        forced_reasons.append(ReasonCode.FORCED_REVIEW_ERRATUM_NOTICE)

    if record_a.flags.is_retraction_notice or record_b.flags.is_retraction_notice:
        forced_reasons.append(ReasonCode.FORCED_REVIEW_RETRACTION_NOTICE)

    if record_a.flags.is_corrected_republished or record_b.flags.is_corrected_republished:
        forced_reasons.append(ReasonCode.FORCED_REVIEW_CORRECTED_REPUBLISHED)

    if record_a.flags.has_linked_citation or record_b.flags.has_linked_citation:
        forced_reasons.append(ReasonCode.FORCED_REVIEW_LINKED_CITATION)

    return forced_reasons
