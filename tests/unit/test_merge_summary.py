"""Tests for MergeSummary model and process_canonical_merge summary output."""

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from srdedupe.clustering.models import (
    Cluster,
    ClusterConsistency,
    ClusterStatus,
    ClusterSupport,
)
from srdedupe.merge.models import MergeSummary
from srdedupe.merge.processor import process_canonical_merge
from srdedupe.models import CanonicalRecord

# ---------------------------------------------------------------------------
# MergeSummary model
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_merge_summary_defaults() -> None:
    """All fields default to zero/empty."""
    summary = MergeSummary()

    assert summary.records_in_total == 0
    assert summary.clusters_auto_in == 0
    assert summary.clusters_review_in == 0
    assert summary.auto_clusters_merged == 0
    assert summary.max_cluster_size_merged == 0
    assert summary.records_not_found == 0
    assert summary.singletons_count == 0
    assert summary.records_out_deduped_auto == 0
    assert summary.records_out_review_pending == 0
    assert summary.records_out_unique_total == 0
    assert summary.dedup_rate == 0.0
    assert summary.timestamp == ""


@pytest.mark.unit
def test_merge_summary_to_dict_contains_all_fields() -> None:
    """to_dict includes every field for JSON serialization."""
    summary = MergeSummary(
        records_in_total=100,
        clusters_auto_in=10,
        singletons_count=50,
        dedup_rate=0.4,
        timestamp="2026-01-01T00:00:00+00:00",
    )

    d = summary.to_dict()

    expected_keys = {
        "records_in_total",
        "clusters_auto_in",
        "clusters_review_in",
        "auto_clusters_merged",
        "max_cluster_size_merged",
        "records_not_found",
        "singletons_count",
        "records_out_deduped_auto",
        "records_out_review_pending",
        "records_out_unique_total",
        "dedup_rate",
        "timestamp",
        "execution_time_seconds",
    }
    assert set(d.keys()) == expected_keys


@pytest.mark.unit
def test_merge_summary_no_dead_fields() -> None:
    """Verify removed fields (fail_fast_conflict_count, merge_warnings_total) are gone."""
    d = MergeSummary().to_dict()

    assert "fail_fast_conflict_count" not in d
    assert "merge_warnings_total" not in d


@pytest.mark.unit
def test_merge_summary_to_dict_is_json_serializable() -> None:
    """to_dict output can be serialized to JSON without errors."""
    summary = MergeSummary(
        records_in_total=100,
        dedup_rate=0.35,
        timestamp="2026-02-18T12:00:00+00:00",
    )

    serialized = json.dumps(summary.to_dict(), sort_keys=True)
    roundtrip = json.loads(serialized)

    assert roundtrip["records_in_total"] == 100
    assert roundtrip["dedup_rate"] == 0.35


# ---------------------------------------------------------------------------
# process_canonical_merge — summary population
# ---------------------------------------------------------------------------


def _write_clusters(path: Path, clusters: list[Cluster]) -> None:
    """Write clusters to JSONL file."""
    with path.open("w") as f:
        for cluster in clusters:
            json.dump(cluster.to_dict(), f, sort_keys=True)
            f.write("\n")


def _make_cluster(
    cluster_id: str,
    rids: list[str],
    status: ClusterStatus = ClusterStatus.AUTO,
) -> Cluster:
    """Create a minimal cluster for testing."""
    return Cluster(
        cluster_id=cluster_id,
        status=status,
        rids=rids,
        support=ClusterSupport(edges_auto_dup=1, strong_edge_count=1, sources={}),
        consistency=ClusterConsistency(),
    )


@pytest.mark.unit
def test_process_merge_records_in_total(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """records_in_total reflects len(records_map)."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")
    r3 = make_record("r:003", title_raw="Singleton Title")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2, r3]}

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert summary.records_in_total == 3


@pytest.mark.unit
def test_process_merge_singletons_count(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """singletons_count counts records not in any cluster."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")
    r3 = make_record("r:003", title_raw="Singleton One")
    r4 = make_record("r:004", title_raw="Singleton Two")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2, r3, r4]}

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert summary.singletons_count == 2


@pytest.mark.unit
def test_process_merge_singletons_ris_written(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """singletons.ris is written when singletons exist."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")
    r3 = make_record("r:003", title_raw="Singleton")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2, r3]}

    process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert (output_dir / "singletons.ris").exists()


@pytest.mark.unit
def test_process_merge_no_singletons_ris_when_none(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """singletons.ris is NOT written when all records are in clusters."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2]}

    process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert not (output_dir / "singletons.ris").exists()


@pytest.mark.unit
def test_process_merge_unique_total(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """records_out_unique_total = singletons + auto_clusters_merged."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")
    r3 = make_record("r:003", title_raw="Singleton")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2, r3]}

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert (
        summary.records_out_unique_total == summary.singletons_count + summary.auto_clusters_merged
    )
    assert summary.records_out_unique_total == 2  # 1 singleton + 1 merged cluster


@pytest.mark.unit
def test_process_merge_dedup_rate(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """dedup_rate = 1.0 - unique_total / records_in_total."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")
    r3 = make_record("r:003", title_raw="Singleton")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2, r3]}

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    # 3 records → 2 unique (1 merged + 1 singleton) → rate = 1 - 2/3 ≈ 0.3333
    expected = round(1.0 - 2 / 3, 4)
    assert summary.dedup_rate == expected


@pytest.mark.unit
def test_process_merge_timestamp_populated(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """timestamp is a non-empty ISO-8601 string."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2]}

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert summary.timestamp != ""
    assert "T" in summary.timestamp  # ISO-8601 contains T separator


@pytest.mark.unit
def test_process_merge_records_not_found(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """records_not_found counts RIDs in clusters missing from records_map."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    # r:002 is in the cluster but NOT in records_map

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r1.rid: r1}  # only r:001, missing r:002

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert summary.records_not_found == 1


@pytest.mark.unit
def test_process_merge_summary_json_written(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """merge_summary.json is written with all expected keys."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    r2 = make_record("r:002", doi_norm="10.1/a", title_raw="Title A Longer")

    clusters = [_make_cluster("c:001", ["r:001", "r:002"])]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r.rid: r for r in [r1, r2]}

    process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    summary_path = tmp_path / "reports" / "merge_summary.json"
    assert summary_path.exists()

    with summary_path.open("r") as f:
        data = json.load(f)

    assert data["records_in_total"] == 2
    assert data["singletons_count"] == 0
    assert data["auto_clusters_merged"] == 1
    assert data["records_out_unique_total"] == 1
    assert "timestamp" in data
    assert "dedup_rate" in data
    # Removed fields should not appear
    assert "fail_fast_conflict_count" not in data
    assert "merge_warnings_total" not in data


@pytest.mark.unit
def test_process_merge_review_records_not_found(
    make_record: Callable[..., CanonicalRecord],
    tmp_path: Path,
) -> None:
    """records_not_found tracks missing RIDs in REVIEW clusters too."""
    r1 = make_record("r:001", doi_norm="10.1/a", title_raw="Title A")
    # r:002 and r:003 are in review cluster but not in records_map

    clusters = [
        _make_cluster("c:review", ["r:002", "r:003"], status=ClusterStatus.REVIEW),
    ]
    clusters_path = tmp_path / "clusters.jsonl"
    _write_clusters(clusters_path, clusters)

    output_dir = tmp_path / "artifacts"
    records_map = {r1.rid: r1}

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=tmp_path,
        output_dir=output_dir,
        records_map=records_map,
    )

    assert summary.records_not_found == 2
