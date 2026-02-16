"""Data models for clustering and consistency checks."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class ClusterStatus(StrEnum):
    """Cluster status after consistency checks.

    Attributes
    ----------
    AUTO : str
        Automatically mergeable cluster.
    REVIEW : str
        Requires manual review due to conflicts.
    """

    AUTO = "AUTO"
    REVIEW = "REVIEW"


class ConflictType(StrEnum):
    """Types of consistency conflicts.

    Attributes
    ----------
    DOI_CONFLICT : str
        Multiple distinct DOIs present.
    PMID_CONFLICT : str
        Multiple distinct PMIDs present.
    LINKED_CITATION_RISK : str
        Special record types present (erratum, retraction, etc.).
    INTERNAL_AUTO_KEEP_CONTRADICTION : str
        AUTO_KEEP edge exists within cluster.
    YEAR_FAR : str
        Year spread too large.
    TITLE_KEY_DIVERGENT : str
        Multiple distinct title keys.
    BRIDGED_BY_WEAK_EDGES : str
        Cluster connected only by weak edges.
    """

    DOI_CONFLICT = "doi_conflict"
    PMID_CONFLICT = "pmid_conflict"
    LINKED_CITATION_RISK = "linked_citation_risk"
    INTERNAL_AUTO_KEEP_CONTRADICTION = "internal_auto_keep_contradiction"
    YEAR_FAR = "year_far"
    TITLE_KEY_DIVERGENT = "title_key_divergent"
    BRIDGED_BY_WEAK_EDGES = "bridged_by_weak_edges"


STRONG_REASON_CODES: frozenset[str] = frozenset({"doi_exact", "pmid_exact"})


@dataclass(frozen=True)
class Edge:
    """A pairwise decision edge.

    Attributes
    ----------
    pair_id : str
        Unique pair identifier.
    rid_a : str
        First record ID.
    rid_b : str
        Second record ID.
    decision : str
        Decision label (AUTO_DUP, AUTO_KEEP, REVIEW).
    p_match : float
        Match probability.
    reasons : tuple[str, ...]
        Reason codes for the decision.
    """

    pair_id: str
    rid_a: str
    rid_b: str
    decision: str
    p_match: float
    reasons: tuple[str, ...]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Edge":
        """Create Edge from raw decision dict.

        Parameters
        ----------
        data : dict[str, Any]
            Raw decision dictionary from JSONL.

        Returns
        -------
        Edge
            Typed edge.
        """
        raw_reasons = data.get("reasons", [])
        reason_codes = tuple(
            r.get("code", "") if isinstance(r, dict) else str(r) for r in raw_reasons
        )
        return Edge(
            pair_id=data["pair_id"],
            rid_a=data["rid_a"],
            rid_b=data["rid_b"],
            decision=data["decision"],
            p_match=data.get("p_match", 0.0),
            reasons=reason_codes,
        )

    def is_strong(self, threshold: float, use_reason_codes: bool = True) -> bool:
        """Check if this edge is considered strong.

        Parameters
        ----------
        threshold : float
            Minimum p_match for strong classification.
        use_reason_codes : bool
            Whether to check reason codes, by default True.

        Returns
        -------
        bool
            True if edge is strong.
        """
        if use_reason_codes and STRONG_REASON_CODES.intersection(self.reasons):
            return True
        return self.p_match >= threshold

    def involves(self, rid_set: frozenset[str]) -> bool:
        """Check if both endpoints are in the given set.

        Parameters
        ----------
        rid_set : frozenset[str]
            Set of record IDs.

        Returns
        -------
        bool
            True if both rid_a and rid_b are in the set.
        """
        return self.rid_a in rid_set and self.rid_b in rid_set


@dataclass(frozen=True)
class ClusterSupport:
    """Support metadata for cluster.

    Attributes
    ----------
    edges_auto_dup : int
        Number of AUTO-DUP edges in cluster.
    strong_edge_count : int
        Number of strong edges (high confidence).
    sources : dict[str, int]
        Edge source counts (e.g., doi_exact, pmid_exact).
    """

    edges_auto_dup: int
    strong_edge_count: int
    sources: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            "edges_auto_dup": self.edges_auto_dup,
            "strong_edge_count": self.strong_edge_count,
            "sources": self.sources,
        }


@dataclass(frozen=True)
class ClusterConsistency:
    """Consistency check results for cluster.

    Attributes
    ----------
    hard_conflicts : tuple[str, ...]
        Hard conflicts that block AUTO status.
    soft_conflicts : tuple[str, ...]
        Soft conflicts that suggest caution.
    notes : tuple[str, ...]
        Additional notes about cluster.
    """

    hard_conflicts: tuple[str, ...] = ()
    soft_conflicts: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            "hard_conflicts": list(self.hard_conflicts),
            "soft_conflicts": list(self.soft_conflicts),
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class Cluster:
    """Duplicate cluster with metadata.

    Attributes
    ----------
    cluster_id : str
        Deterministic cluster identifier.
    status : ClusterStatus
        Cluster status (AUTO or REVIEW).
    rids : tuple[str, ...]
        Record IDs in cluster (sorted).
    support : ClusterSupport
        Edge support metadata.
    consistency : ClusterConsistency
        Consistency check results.
    """

    cluster_id: str
    status: ClusterStatus
    rids: tuple[str, ...]
    support: ClusterSupport
    consistency: ClusterConsistency

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return {
            "cluster_id": self.cluster_id,
            "status": self.status.value,
            "rids": list(self.rids),
            "support": self.support.to_dict(),
            "consistency": self.consistency.to_dict(),
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Cluster":
        """Deserialize cluster from dictionary.

        Parameters
        ----------
        data : dict[str, Any]
            Dictionary representation (e.g., from JSONL).

        Returns
        -------
        Cluster
            Deserialized cluster.
        """
        return Cluster(
            cluster_id=data["cluster_id"],
            status=ClusterStatus(data["status"]),
            rids=tuple(data["rids"]),
            support=ClusterSupport(**data["support"]),
            consistency=ClusterConsistency(
                hard_conflicts=tuple(data["consistency"].get("hard_conflicts", ())),
                soft_conflicts=tuple(data["consistency"].get("soft_conflicts", ())),
                notes=tuple(data["consistency"].get("notes", ())),
            ),
        )


@dataclass(frozen=True)
class ClusteringConfig:
    """Configuration for clustering and consistency checks.

    Attributes
    ----------
    strong_edge_use_reason_codes : bool
        Use reason codes to identify strong edges, by default True.
    strong_edge_t_strong : float
        Threshold for strong edges by p_match, by default 0.999.
    soft_conflicts_year_max_spread : int
        Maximum year spread before soft conflict, by default 2.
    soft_conflicts_title_divergence_tolerance : int
        Title key divergence tolerance, by default 0.
    oversized_cluster_max_size_auto : int
        Maximum cluster size for AUTO status, by default 25.
    split_policy_enable_id_split : bool
        Enable deterministic ID-based splitting, by default True.
    """

    strong_edge_use_reason_codes: bool = True
    strong_edge_t_strong: float = 0.999
    soft_conflicts_year_max_spread: int = 2
    soft_conflicts_title_divergence_tolerance: int = 0
    oversized_cluster_max_size_auto: int = 25
    split_policy_enable_id_split: bool = True


def compute_cluster_id(rids: Sequence[str]) -> str:
    """Compute deterministic cluster ID from sorted RIDs.

    Parameters
    ----------
    rids : Sequence[str]
        Record IDs in cluster.

    Returns
    -------
    str
        Cluster ID in format "c:{sha256_prefix}".
    """
    sorted_rids = sorted(rids)
    content = "\n".join(sorted_rids)
    hash_digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return f"c:{hash_digest[:12]}"
