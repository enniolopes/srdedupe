"""Build clusters from pairwise AUTO-DUP decisions."""

import json
from collections import defaultdict
from pathlib import Path

from srdedupe.clustering.consistency import (
    check_cluster_consistency,
    split_cluster_by_id,
)
from srdedupe.clustering.models import (
    Cluster,
    ClusterConsistency,
    ClusteringConfig,
    ClusterStatus,
    ClusterSupport,
    ConflictType,
    Edge,
    compute_cluster_id,
)
from srdedupe.clustering.union_find import UnionFind
from srdedupe.models import CanonicalRecord


def build_clusters(
    pair_decisions_path: Path,
    records: list[CanonicalRecord],
    config: ClusteringConfig,
) -> list[Cluster]:
    """Build clusters from pairwise AUTO-DUP decisions.

    Parameters
    ----------
    pair_decisions_path : Path
        Path to pair_decisions.jsonl file.
    records : list[CanonicalRecord]
        List of canonical records.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    list[Cluster]
        List of clusters with metadata, sorted by cluster_id.
    """
    records_map = {record.rid: record for record in records}

    auto_dup_edges, auto_keep_index = _load_decisions(pair_decisions_path)

    component_edges = _compute_component_edges(auto_dup_edges)

    clusters: list[Cluster] = []
    for rids, edges in component_edges:
        cluster_list = _process_component(
            rids,
            records_map,
            edges,
            auto_keep_index,
            config,
        )
        clusters.extend(cluster_list)

    clusters.sort(key=lambda c: c.cluster_id)
    return clusters


def _load_decisions(
    pair_decisions_path: Path,
) -> tuple[list[Edge], dict[str, set[str]]]:
    """Load pair decisions from JSONL file.

    Parses all decisions once, returning typed AUTO_DUP edges and a
    pre-built AUTO_KEEP adjacency index for O(1) contradiction lookups.

    Parameters
    ----------
    pair_decisions_path : Path
        Path to pair_decisions.jsonl.

    Returns
    -------
    tuple[list[Edge], dict[str, set[str]]]
        AUTO-DUP edges and AUTO_KEEP adjacency index (rid -> partner rids).
    """
    auto_dup_edges: list[Edge] = []
    auto_keep_index: dict[str, set[str]] = defaultdict(set)

    with pair_decisions_path.open("r") as f:
        for line in f:
            data = json.loads(line)
            decision = data["decision"]

            if decision == "AUTO_DUP":
                auto_dup_edges.append(Edge.from_dict(data))
            elif decision == "AUTO_KEEP":
                rid_a = data["rid_a"]
                rid_b = data["rid_b"]
                auto_keep_index[rid_a].add(rid_b)
                auto_keep_index[rid_b].add(rid_a)

    return auto_dup_edges, dict(auto_keep_index)


def _compute_component_edges(
    edges: list[Edge],
) -> list[tuple[tuple[str, ...], list[Edge]]]:
    """Compute connected components with their edges in a single pass.

    Parameters
    ----------
    edges : list[Edge]
        List of AUTO-DUP edges.

    Returns
    -------
    list[tuple[tuple[str, ...], list[Edge]]]
        Each entry is (sorted rids tuple, edges for that component).
    """
    uf = UnionFind()
    for edge in edges:
        uf.union(edge.rid_a, edge.rid_b)

    # Group edges by their component root in one pass over edges
    root_edges: dict[str, list[Edge]] = defaultdict(list)
    for edge in edges:
        root = uf.find(edge.rid_a)
        root_edges[root].append(edge)

    result: list[tuple[tuple[str, ...], list[Edge]]] = []
    for component_rids in uf.get_components():
        sorted_rids = tuple(sorted(component_rids))
        root = uf.find(sorted_rids[0])
        result.append((sorted_rids, root_edges.get(root, [])))

    return result


def _process_component(
    rids: tuple[str, ...],
    records_map: dict[str, CanonicalRecord],
    cluster_edges: list[Edge],
    auto_keep_index: dict[str, set[str]],
    config: ClusteringConfig,
) -> list[Cluster]:
    """Process a single component to create cluster(s).

    Parameters
    ----------
    rids : tuple[str, ...]
        Sorted record IDs in component.
    records_map : dict[str, CanonicalRecord]
        Mapping from rid to record.
    cluster_edges : list[Edge]
        AUTO-DUP edges in component.
    auto_keep_index : dict[str, set[str]]
        AUTO_KEEP adjacency index.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    list[Cluster]
        One or more clusters (if split).
    """
    consistency = check_cluster_consistency(
        rids,
        records_map,
        cluster_edges,
        auto_keep_index,
        config,
    )

    should_split = config.split_policy_enable_id_split and (
        ConflictType.DOI_CONFLICT.value in consistency.hard_conflicts
        or ConflictType.PMID_CONFLICT.value in consistency.hard_conflicts
    )

    if should_split:
        return _split_and_create_clusters(
            rids,
            records_map,
            cluster_edges,
            auto_keep_index,
            config,
            consistency,
        )

    return [_create_cluster(rids, cluster_edges, consistency, config)]


def _split_and_create_clusters(
    rids: tuple[str, ...],
    records_map: dict[str, CanonicalRecord],
    cluster_edges: list[Edge],
    auto_keep_index: dict[str, set[str]],
    config: ClusteringConfig,
    original_consistency: ClusterConsistency,
) -> list[Cluster]:
    """Split cluster by ID conflicts and create subclusters.

    Parameters
    ----------
    rids : tuple[str, ...]
        Record IDs in original cluster.
    records_map : dict[str, CanonicalRecord]
        Mapping from rid to record.
    cluster_edges : list[Edge]
        AUTO-DUP edges.
    auto_keep_index : dict[str, set[str]]
        AUTO_KEEP adjacency index.
    config : ClusteringConfig
        Clustering configuration.
    original_consistency : ClusterConsistency
        Original consistency check.

    Returns
    -------
    list[Cluster]
        List of subclusters.
    """
    conflict_type = (
        ConflictType.DOI_CONFLICT
        if ConflictType.DOI_CONFLICT.value in original_consistency.hard_conflicts
        else ConflictType.PMID_CONFLICT
    )

    subclusters_rids = split_cluster_by_id(rids, records_map, conflict_type)

    clusters: list[Cluster] = []
    for sub_rids in subclusters_rids:
        rid_set = frozenset(sub_rids)
        sub_edges = [e for e in cluster_edges if e.involves(rid_set)]

        consistency = check_cluster_consistency(
            sub_rids,
            records_map,
            sub_edges,
            auto_keep_index,
            config,
        )
        clusters.append(_create_cluster(sub_rids, sub_edges, consistency, config))

    return clusters


def _create_cluster(
    rids: tuple[str, ...],
    cluster_edges: list[Edge],
    consistency: ClusterConsistency,
    config: ClusteringConfig,
) -> Cluster:
    """Create a cluster object.

    Parameters
    ----------
    rids : tuple[str, ...]
        Sorted record IDs in cluster.
    cluster_edges : list[Edge]
        AUTO-DUP edges in cluster.
    consistency : ClusterConsistency
        Consistency check results.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    Cluster
        Cluster object.
    """
    cluster_id = compute_cluster_id(rids)
    support = _compute_support(cluster_edges, config)
    status = (
        ClusterStatus.REVIEW
        if consistency.hard_conflicts or consistency.soft_conflicts
        else ClusterStatus.AUTO
    )

    return Cluster(
        cluster_id=cluster_id,
        status=status,
        rids=rids,
        support=support,
        consistency=consistency,
    )


def _compute_support(
    cluster_edges: list[Edge],
    config: ClusteringConfig,
) -> ClusterSupport:
    """Compute support metadata for cluster.

    Parameters
    ----------
    cluster_edges : list[Edge]
        AUTO-DUP edges in cluster.
    config : ClusteringConfig
        Clustering configuration.

    Returns
    -------
    ClusterSupport
        Support metadata.
    """
    strong_count = 0
    source_counts: dict[str, int] = defaultdict(int)

    for edge in cluster_edges:
        if edge.is_strong(config.strong_edge_t_strong, config.strong_edge_use_reason_codes):
            strong_count += 1

        for code in edge.reasons:
            if code:
                source_counts[code] += 1

    return ClusterSupport(
        edges_auto_dup=len(cluster_edges),
        strong_edge_count=strong_count,
        sources=dict(source_counts),
    )
