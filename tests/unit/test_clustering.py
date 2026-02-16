"""Tests for clustering and global consistency checks."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from srdedupe.clustering import (
    ClusteringConfig,
    ClusterStatus,
    build_clusters,
)
from srdedupe.clustering.models import compute_cluster_id
from srdedupe.clustering.union_find import UnionFind
from srdedupe.models import CanonicalRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decision(
    rid_a: str,
    rid_b: str,
    *,
    decision: str = "AUTO_DUP",
    p_match: float = 0.999,
    reason: str = "doi_exact",
) -> dict[str, Any]:
    """Build a single pair-decision dict."""
    return {
        "pair_id": f"{rid_a}|{rid_b}",
        "rid_a": rid_a,
        "rid_b": rid_b,
        "p_match": p_match,
        "decision": decision,
        "reasons": [{"code": reason}],
    }


def _write_decisions(tmp_path: Path, decisions: list[dict[str, Any]]) -> Path:
    """Write decision dicts to a JSONL file and return the path."""
    path = tmp_path / "pair_decisions.jsonl"
    with path.open("w") as f:
        for dec in decisions:
            f.write(json.dumps(dec) + "\n")
    return path


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_union_find_basic() -> None:
    """Test basic Union-Find operations."""
    uf = UnionFind()

    uf.union("a", "b")
    uf.union("b", "c")

    assert uf.find("a") == uf.find("b") == uf.find("c")

    uf.union("d", "e")
    assert uf.find("d") == uf.find("e")
    assert uf.find("d") != uf.find("a")


@pytest.mark.unit
def test_union_find_components() -> None:
    """Test getting connected components."""
    uf = UnionFind()

    uf.union("a", "b")
    uf.union("b", "c")
    uf.union("d", "e")

    component_sets = [set(comp) for comp in uf.get_components()]

    assert len(component_sets) == 2
    assert {"a", "b", "c"} in component_sets
    assert {"d", "e"} in component_sets


# ---------------------------------------------------------------------------
# Cluster ID
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_compute_cluster_id_deterministic() -> None:
    """Test cluster ID is deterministic regardless of input order."""
    id1 = compute_cluster_id(["rid_c", "rid_a", "rid_b"])
    id2 = compute_cluster_id(["rid_b", "rid_a", "rid_c"])

    assert id1 == id2
    assert id1.startswith("c:")
    assert len(id1) == 14


# ---------------------------------------------------------------------------
# build_clusters — happy path
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_simple_cluster_doi_exact(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test simple AUTO cluster from a single DOI-exact edge."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="doi_exact"),
        ],
    )
    records = [
        make_record("rid_a", doi_norm="10.1234/abc"),
        make_record("rid_b", doi_norm="10.1234/abc"),
    ]

    clusters = build_clusters(path, records, ClusteringConfig())

    assert len(clusters) == 1
    cluster = clusters[0]
    assert cluster.status == ClusterStatus.AUTO
    assert set(cluster.rids) == {"rid_a", "rid_b"}
    assert cluster.support.strong_edge_count == 1
    assert len(cluster.consistency.hard_conflicts) == 0


@pytest.mark.unit
def test_empty_input_no_auto_dup(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test that only AUTO_KEEP edges produce zero clusters."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", decision="AUTO_KEEP", p_match=0.10, reason="p_below_t_low"),
        ],
    )
    records = [make_record("rid_a"), make_record("rid_b")]

    clusters = build_clusters(path, records, ClusteringConfig())

    assert len(clusters) == 0


@pytest.mark.unit
def test_determinism_shuffle_input(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test output is identical regardless of decision file order."""
    decisions = [
        _decision("rid_a", "rid_b", reason="doi_exact"),
        _decision("rid_c", "rid_d", reason="pmid_exact"),
    ]

    path1 = _write_decisions(tmp_path, decisions)
    path2 = tmp_path / "decisions_rev.jsonl"
    with path2.open("w") as f:
        for dec in reversed(decisions):
            f.write(json.dumps(dec) + "\n")

    records = [
        make_record("rid_a", doi_norm="10.1234/abc"),
        make_record("rid_b", doi_norm="10.1234/abc"),
        make_record("rid_c", pmid_norm="12345678"),
        make_record("rid_d", pmid_norm="12345678"),
    ]
    config = ClusteringConfig()

    c1 = sorted(build_clusters(path1, records, config), key=lambda c: c.cluster_id)
    c2 = sorted(build_clusters(path2, records, config), key=lambda c: c.cluster_id)

    assert len(c1) == len(c2)
    for a, b in zip(c1, c2, strict=True):
        assert a.cluster_id == b.cluster_id
        assert a.rids == b.rids
        assert a.status == b.status


# ---------------------------------------------------------------------------
# Hard conflicts
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_hard_doi_conflict(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test that distinct DOIs within a cluster trigger DOI hard conflict."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="title_exact"),
        ],
    )
    records = [
        make_record("rid_a", doi_norm="10.1234/abc"),
        make_record("rid_b", doi_norm="10.5678/xyz"),
    ]

    clusters = build_clusters(path, records, ClusteringConfig(split_policy_enable_id_split=False))

    assert len(clusters) == 1
    assert clusters[0].status == ClusterStatus.REVIEW
    assert "doi_conflict" in clusters[0].consistency.hard_conflicts


@pytest.mark.unit
def test_hard_pmid_conflict(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test that distinct PMIDs within a cluster trigger PMID hard conflict."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="title_exact"),
        ],
    )
    records = [
        make_record("rid_a", pmid_norm="11111111"),
        make_record("rid_b", pmid_norm="22222222"),
    ]

    clusters = build_clusters(path, records, ClusteringConfig(split_policy_enable_id_split=False))

    assert len(clusters) == 1
    assert clusters[0].status == ClusterStatus.REVIEW
    assert "pmid_conflict" in clusters[0].consistency.hard_conflicts


@pytest.mark.unit
def test_internal_auto_keep_contradiction(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test AUTO_KEEP between cluster members triggers hard conflict."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="title_exact"),
            _decision("rid_b", "rid_c", reason="title_exact"),
            _decision("rid_a", "rid_c", decision="AUTO_KEEP", p_match=0.10, reason="p_below_t_low"),
        ],
    )
    records = [make_record("rid_a"), make_record("rid_b"), make_record("rid_c")]

    clusters = build_clusters(path, records, ClusteringConfig())

    assert len(clusters) == 1
    assert clusters[0].status == ClusterStatus.REVIEW
    assert "internal_auto_keep_contradiction" in clusters[0].consistency.hard_conflicts


@pytest.mark.unit
def test_special_record_type_flags(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test special record type flags (erratum, retraction) trigger REVIEW."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="doi_exact"),
        ],
    )
    records = [
        make_record("rid_a", doi_norm="10.1234/abc"),
        make_record("rid_b", doi_norm="10.1234/abc", is_erratum_notice=True),
    ]

    clusters = build_clusters(path, records, ClusteringConfig())

    assert len(clusters) == 1
    assert clusters[0].status == ClusterStatus.REVIEW
    assert "linked_citation_risk" in clusters[0].consistency.hard_conflicts


# ---------------------------------------------------------------------------
# Soft conflicts
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_transitive_closure_weak_bridge(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test chain of weak edges triggers soft conflict."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", p_match=0.70, reason="title_similar"),
            _decision("rid_b", "rid_c", p_match=0.70, reason="title_similar"),
        ],
    )
    records = [make_record("rid_a"), make_record("rid_b"), make_record("rid_c")]

    clusters = build_clusters(path, records, ClusteringConfig(strong_edge_t_strong=0.999))

    assert len(clusters) == 1
    assert clusters[0].status == ClusterStatus.REVIEW
    assert "bridged_by_weak_edges" in clusters[0].consistency.soft_conflicts


@pytest.mark.unit
def test_year_spread_soft_conflict(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test large year spread triggers soft conflict."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="title_exact"),
        ],
    )
    records = [
        make_record("rid_a", year_norm=2020),
        make_record("rid_b", year_norm=2025),
    ]

    clusters = build_clusters(
        path,
        records,
        ClusteringConfig(soft_conflicts_year_max_spread=2),
    )

    assert len(clusters) == 1
    assert clusters[0].status == ClusterStatus.REVIEW
    assert "year_far" in clusters[0].consistency.soft_conflicts


@pytest.mark.unit
def test_title_key_divergent_soft_conflict(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test distinct title keys trigger soft conflict."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="author_match"),
        ],
    )
    records = [
        make_record("rid_a", title_key_strict="title_one"),
        make_record("rid_b", title_key_strict="title_two"),
    ]

    clusters = build_clusters(
        path,
        records,
        ClusteringConfig(soft_conflicts_title_divergence_tolerance=0),
    )

    assert len(clusters) == 1
    assert clusters[0].status == ClusterStatus.REVIEW
    assert "title_key_divergent" in clusters[0].consistency.soft_conflicts


# ---------------------------------------------------------------------------
# Split policy
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doi_conflict_with_split(
    tmp_path: Path,
    make_record: Callable[..., CanonicalRecord],
) -> None:
    """Test DOI conflict with splitting creates separate subclusters."""
    path = _write_decisions(
        tmp_path,
        [
            _decision("rid_a", "rid_b", reason="title_exact"),
            _decision("rid_a", "rid_c", reason="doi_exact"),
            _decision("rid_b", "rid_d", reason="doi_exact"),
        ],
    )
    records = [
        make_record("rid_a", doi_norm="10.1234/abc"),
        make_record("rid_b", doi_norm="10.5678/xyz"),
        make_record("rid_c", doi_norm="10.1234/abc"),
        make_record("rid_d", doi_norm="10.5678/xyz"),
    ]

    clusters = build_clusters(
        path,
        records,
        ClusteringConfig(split_policy_enable_id_split=True),
    )

    assert len(clusters) == 2
    cluster_rids = [set(c.rids) for c in clusters]
    assert {"rid_a", "rid_c"} in cluster_rids
    assert {"rid_b", "rid_d"} in cluster_rids
