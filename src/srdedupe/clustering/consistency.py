"""Consistency checks for clusters to prevent transitive closure errors."""

from collections import defaultdict

from srdedupe.clustering.models import (
    ClusterConsistency,
    ClusteringConfig,
    ConflictType,
    Edge,
)
from srdedupe.models import CanonicalRecord


def check_cluster_consistency(
    rids: tuple[str, ...],
    records_map: dict[str, CanonicalRecord],
    cluster_edges: list[Edge],
    auto_keep_index: dict[str, set[str]],
    config: ClusteringConfig,
) -> ClusterConsistency:
    """Check cluster for hard and soft consistency conflicts.

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in cluster.
    records_map : dict[str, CanonicalRecord]
        Mapping from rid to canonical record.
    cluster_edges : list[Edge]
        AUTO-DUP edges in this cluster.
    auto_keep_index : dict[str, set[str]]
        Pre-built index: rid -> set of rids it has AUTO_KEEP with.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    ClusterConsistency
        Immutable consistency check results.
    """
    hard = _collect_hard_conflicts(rids, records_map, auto_keep_index)
    soft = _collect_soft_conflicts(rids, records_map, cluster_edges, config)
    notes = _collect_notes(rids, config)

    return ClusterConsistency(
        hard_conflicts=tuple(hard),
        soft_conflicts=tuple(soft),
        notes=tuple(notes),
    )


def _collect_hard_conflicts(
    rids: tuple[str, ...],
    records_map: dict[str, CanonicalRecord],
    auto_keep_index: dict[str, set[str]],
) -> list[str]:
    """Collect hard conflicts that block AUTO status.

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in cluster.
    records_map : dict[str, CanonicalRecord]
        Mapping from rid to canonical record.
    auto_keep_index : dict[str, set[str]]
        Pre-built AUTO_KEEP adjacency index.

    Returns
    -------
    list[str]
        List of hard conflict type values.
    """
    conflicts: list[str] = []
    doi_values: set[str] = set()
    pmid_values: set[str] = set()
    has_special_record = False

    for rid in rids:
        record = records_map.get(rid)
        if not record:
            continue

        if record.canon.doi_norm:
            doi_values.add(record.canon.doi_norm)

        if record.canon.pmid_norm:
            pmid_values.add(record.canon.pmid_norm)

        if (
            record.flags.is_erratum_notice
            or record.flags.is_retraction_notice
            or record.flags.is_corrected_republished
            or record.flags.has_linked_citation
        ):
            has_special_record = True

    if len(doi_values) >= 2:
        conflicts.append(ConflictType.DOI_CONFLICT.value)

    if len(pmid_values) >= 2:
        conflicts.append(ConflictType.PMID_CONFLICT.value)

    if has_special_record:
        conflicts.append(ConflictType.LINKED_CITATION_RISK.value)

    if _has_internal_auto_keep(rids, auto_keep_index):
        conflicts.append(ConflictType.INTERNAL_AUTO_KEEP_CONTRADICTION.value)

    return conflicts


def _has_internal_auto_keep(
    rids: tuple[str, ...],
    auto_keep_index: dict[str, set[str]],
) -> bool:
    """Check if any pair within the cluster has an AUTO_KEEP decision.

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in cluster.
    auto_keep_index : dict[str, set[str]]
        rid -> set of rids it has AUTO_KEEP relationship with.

    Returns
    -------
    bool
        True if contradiction exists.
    """
    rid_set = frozenset(rids)
    for rid in rids:
        partners = auto_keep_index.get(rid, set())
        if partners & rid_set:
            return True
    return False


def _collect_soft_conflicts(
    rids: tuple[str, ...],
    records_map: dict[str, CanonicalRecord],
    cluster_edges: list[Edge],
    config: ClusteringConfig,
) -> list[str]:
    """Collect soft conflicts that suggest caution.

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in cluster.
    records_map : dict[str, CanonicalRecord]
        Mapping from rid to canonical record.
    cluster_edges : list[Edge]
        AUTO-DUP edges in cluster.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    list[str]
        List of soft conflict type values.
    """
    conflicts: list[str] = []
    years: list[int] = []
    title_keys: set[str] = set()

    for rid in rids:
        record = records_map.get(rid)
        if not record:
            continue

        if record.canon.year_norm:
            years.append(record.canon.year_norm)

        if record.keys.title_key_strict:
            title_keys.add(record.keys.title_key_strict)

    if len(years) >= 2:
        year_spread = max(years) - min(years)
        if year_spread > config.soft_conflicts_year_max_spread:
            conflicts.append(ConflictType.YEAR_FAR.value)

    if len(title_keys) > config.soft_conflicts_title_divergence_tolerance + 1:
        conflicts.append(ConflictType.TITLE_KEY_DIVERGENT.value)

    if _is_bridged_by_weak_edges(rids, cluster_edges, config):
        conflicts.append(ConflictType.BRIDGED_BY_WEAK_EDGES.value)

    return conflicts


def _is_bridged_by_weak_edges(
    rids: tuple[str, ...],
    cluster_edges: list[Edge],
    config: ClusteringConfig,
) -> bool:
    """Check if cluster is bridged by weak edges.

    A cluster is considered weakly bridged if it has no strong edges at all,
    or if any degree-1 node is connected only by a weak edge.

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in cluster.
    cluster_edges : list[Edge]
        AUTO-DUP edges in cluster.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    bool
        True if cluster has weak bridge pattern.
    """
    if len(rids) < 3:
        return False

    has_any_strong = any(
        e.is_strong(config.strong_edge_t_strong, config.strong_edge_use_reason_codes)
        for e in cluster_edges
    )
    if not has_any_strong:
        return True

    degree_map: dict[str, int] = defaultdict(int)
    for edge in cluster_edges:
        degree_map[edge.rid_a] += 1
        degree_map[edge.rid_b] += 1

    for rid in rids:
        if degree_map[rid] != 1:
            continue
        incident = next(
            (e for e in cluster_edges if e.rid_a == rid or e.rid_b == rid),
            None,
        )
        if incident and not incident.is_strong(
            config.strong_edge_t_strong, config.strong_edge_use_reason_codes
        ):
            return True

    return False


def _collect_notes(
    rids: tuple[str, ...],
    config: ClusteringConfig,
) -> list[str]:
    """Collect informational notes about the cluster.

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in cluster.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    list[str]
        List of notes.
    """
    notes: list[str] = []
    if len(rids) > config.oversized_cluster_max_size_auto:
        notes.append(
            f"oversized_cluster_size_{len(rids)}_exceeds_{config.oversized_cluster_max_size_auto}"
        )
    return notes


def split_cluster_by_id(
    rids: tuple[str, ...],
    records_map: dict[str, CanonicalRecord],
    conflict_type: ConflictType,
) -> list[tuple[str, ...]]:
    """Split cluster by identifier (DOI or PMID).

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in cluster.
    records_map : dict[str, CanonicalRecord]
        Mapping from rid to canonical record.
    conflict_type : ConflictType
        Type of conflict (DOI_CONFLICT or PMID_CONFLICT).

    Returns
    -------
    list[tuple[str, ...]]
        List of subclusters as sorted tuples.
    """
    id_groups: dict[str, list[str]] = defaultdict(list)
    no_id_group: list[str] = []

    for rid in rids:
        record = records_map.get(rid)
        if not record:
            no_id_group.append(rid)
            continue

        if conflict_type == ConflictType.DOI_CONFLICT:
            id_value = record.canon.doi_norm
        elif conflict_type == ConflictType.PMID_CONFLICT:
            id_value = record.canon.pmid_norm
        else:
            no_id_group.append(rid)
            continue

        if id_value:
            id_groups[id_value].append(rid)
        else:
            no_id_group.append(rid)

    subclusters = [tuple(sorted(group)) for group in id_groups.values()]

    if no_id_group:
        subclusters.append(tuple(sorted(no_id_group)))

    return subclusters
