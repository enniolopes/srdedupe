"""Data models for canonical merge."""

import hashlib
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from srdedupe.models import Canon


@dataclass
class MergePolicy:
    """Merge policy version information.

    Attributes
    ----------
    name : str
        Policy name (e.g., "merge_v1").
    version : str
        Policy version (semver format).
    """

    name: str
    version: str


@dataclass
class MergeProvenanceField:
    """Provenance for a single merged field.

    Attributes
    ----------
    from_rid : str | list[str]
        Record ID(s) that supplied the value.
    rule : str
        Rule used for selection.
    candidates : list[str] | None
        Competing values (optional, capped).
    """

    from_rid: str | list[str]
    rule: str
    candidates: list[str] | None = None


@dataclass
class MergeProvenance:
    """Complete merge provenance tracking.

    Attributes
    ----------
    fields : dict[str, MergeProvenanceField]
        Field-level provenance.
    """

    fields: dict[str, MergeProvenanceField] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            field_name: {
                "from_rid": prov.from_rid,
                "rule": prov.rule,
                "candidates": prov.candidates,
            }
            for field_name, prov in self.fields.items()
        }


@dataclass
class MergedRecord:
    """Merged canonical record from cluster.

    Attributes
    ----------
    merged_id : str
        Unique merged record identifier.
    cluster_id : str | None
        Original cluster ID (None for singletons).
    status : str
        Cluster status ("AUTO" or "SINGLETON").
    survivor_rid : str
        Record ID chosen as base.
    member_rids : list[str]
        All member record IDs (sorted).
    canon : Canon
        Merged canonical fields.
    merge_provenance : MergeProvenance
        Field-level provenance.
    merge_policy : MergePolicy
        Merge policy version.
    """

    merged_id: str
    cluster_id: str | None
    status: str
    survivor_rid: str
    member_rids: list[str]
    canon: Canon
    merge_provenance: MergeProvenance
    merge_policy: MergePolicy

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            "merged_id": self.merged_id,
            "cluster_id": self.cluster_id,
            "status": self.status,
            "survivor_rid": self.survivor_rid,
            "member_rids": self.member_rids,
            "canon": asdict(self.canon),
            "merge_provenance": self.merge_provenance.to_dict(),
            "merge_policy": asdict(self.merge_policy),
        }


@dataclass
class MergeSummary:
    """Summary statistics for merge operation.

    Attributes
    ----------
    clusters_auto_in : int
        Number of AUTO clusters input.
    clusters_review_in : int
        Number of REVIEW clusters input.
    auto_clusters_merged : int
        Number of AUTO clusters merged.
    records_in_total : int
        Total records input.
    records_out_deduped_auto : int
        Records in deduped_auto.ris.
    records_out_review_pending : int
        Records in review_pending.ris.
    max_cluster_size_merged : int
        Maximum cluster size merged.
    merge_warnings_total : int
        Total merge warnings.
    fail_fast_conflict_count : int
        Fail-fast conflicts (should be 0).
    """

    clusters_auto_in: int = 0
    clusters_review_in: int = 0
    auto_clusters_merged: int = 0
    records_in_total: int = 0
    records_out_deduped_auto: int = 0
    records_out_review_pending: int = 0
    max_cluster_size_merged: int = 0
    merge_warnings_total: int = 0
    fail_fast_conflict_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return asdict(self)


def compute_merged_id(rids: Sequence[str]) -> str:
    """Compute deterministic merged ID from sorted RIDs.

    Parameters
    ----------
    rids : Sequence[str]
        Record IDs to merge.

    Returns
    -------
    str
        Merged ID in format "m:{sha256_prefix}".
    """
    sorted_rids = sorted(rids)
    content = "\n".join(sorted_rids)
    hash_digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return f"m:{hash_digest[:12]}"
