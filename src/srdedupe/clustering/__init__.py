"""Clustering and global consistency checks for duplicate detection.

This module transforms pairwise AUTO-DUP decisions into duplicate clusters
using Union-Find (DSU), with global consistency checks to prevent transitive
closure errors.
"""

from srdedupe.clustering.cluster_builder import build_clusters
from srdedupe.clustering.models import (
    Cluster,
    ClusterConsistency,
    ClusteringConfig,
    ClusterStatus,
    ClusterSupport,
    ConflictType,
    Edge,
)

__all__ = [
    "Cluster",
    "ClusterConsistency",
    "ClusteringConfig",
    "ClusterStatus",
    "ClusterSupport",
    "ConflictType",
    "Edge",
    "build_clusters",
]
