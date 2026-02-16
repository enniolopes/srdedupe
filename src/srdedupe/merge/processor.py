"""Merge workflow: clusters to deduplicated outputs."""

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from srdedupe.clustering.models import Cluster, ClusterStatus
from srdedupe.merge.field_merge import merge_canon_fields
from srdedupe.merge.models import (
    MergedRecord,
    MergePolicy,
    MergeSummary,
    compute_merged_id,
)
from srdedupe.merge.ris_writer import write_ris_file, write_ris_from_records
from srdedupe.merge.survivor import select_survivor
from srdedupe.models import CanonicalRecord


def load_clusters(clusters_path: Path) -> list[Cluster]:
    """Load clusters from JSONL file.

    Parameters
    ----------
    clusters_path : Path
        Path to clusters.jsonl file.

    Returns
    -------
    list[Cluster]
        List of clusters.
    """
    clusters = []
    with clusters_path.open("r") as f:
        for line in f:
            if line.strip():
                clusters.append(Cluster.from_dict(json.loads(line)))
    return clusters


def load_records_batch(rids: Sequence[str], records_dir: Path) -> dict[str, CanonicalRecord]:
    """Load multiple canonical records efficiently.

    Parameters
    ----------
    rids : Sequence[str]
        Record IDs to load.
    records_dir : Path
        Directory containing canonical records.

    Returns
    -------
    dict[str, CanonicalRecord]
        Mapping from RID to CanonicalRecord.
    """
    records: dict[str, CanonicalRecord] = {}
    rid_set = set(rids)
    records_path = records_dir / "canonical_records.jsonl"

    if records_path.exists():
        with records_path.open("r") as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    rid = data.get("rid")
                    if rid in rid_set:
                        records[rid] = CanonicalRecord.from_dict(data)
                        if len(records) == len(rid_set):
                            break

    return records


def process_canonical_merge(
    clusters_path: Path,
    records_dir: Path,
    output_dir: Path,
    *,
    records_map: dict[str, CanonicalRecord] | None = None,
) -> MergeSummary:
    """Process canonical merge and generate outputs.

    Parameters
    ----------
    clusters_path : Path
        Path to clusters.jsonl file.
    records_dir : Path
        Directory containing canonical records.
    output_dir : Path
        Output directory for artifacts.
    records_map : dict[str, CanonicalRecord] | None, optional
        Pre-loaded records indexed by RID. When provided, avoids
        re-reading records from disk. By default None (reads from disk).

    Returns
    -------
    MergeSummary
        Merge summary statistics.
    """
    clusters = load_clusters(clusters_path)
    clusters.sort(key=lambda c: c.cluster_id)

    auto_clusters = [c for c in clusters if c.status == ClusterStatus.AUTO]
    review_clusters = [c for c in clusters if c.status == ClusterStatus.REVIEW]

    summary = MergeSummary(
        clusters_auto_in=len(auto_clusters),
        clusters_review_in=len(review_clusters),
    )

    merge_policy = MergePolicy(name="merge_v1", version="1.0.0")

    merged_records: list[MergedRecord] = []
    enriched_clusters: list[dict[str, Any]] = []

    for cluster in auto_clusters:
        if records_map is not None:
            cluster_records_dict = {
                rid: records_map[rid] for rid in cluster.rids if rid in records_map
            }
        else:
            cluster_records_dict = load_records_batch(cluster.rids, records_dir)
        cluster_records = [cluster_records_dict[rid] for rid in sorted(cluster.rids)]

        if not cluster_records:
            continue

        survivor_rid = select_survivor(cluster_records)
        merged_canon, merge_provenance = merge_canon_fields(cluster_records, survivor_rid)

        merged_id = compute_merged_id(cluster.rids)
        merged_record = MergedRecord(
            merged_id=merged_id,
            cluster_id=cluster.cluster_id,
            status="AUTO",
            survivor_rid=survivor_rid,
            member_rids=sorted(cluster.rids),
            canon=merged_canon,
            merge_provenance=merge_provenance,
            merge_policy=merge_policy,
        )

        merged_records.append(merged_record)

        enriched_cluster = {
            **cluster.to_dict(),
            "survivor_rid": survivor_rid,
            "merged_id": merged_id,
        }
        enriched_clusters.append(enriched_cluster)

        summary.auto_clusters_merged += 1
        summary.max_cluster_size_merged = max(summary.max_cluster_size_merged, len(cluster.rids))

    for cluster in review_clusters:
        enriched_clusters.append(cluster.to_dict())

    enriched_clusters.sort(key=lambda c: c["cluster_id"])
    merged_records.sort(key=lambda r: r.merged_id)

    summary.records_out_deduped_auto = len(merged_records)

    output_dir.mkdir(parents=True, exist_ok=True)
    merged_records_path = output_dir / "merged_records.jsonl"
    with merged_records_path.open("w") as f:
        for record in merged_records:
            json.dump(record.to_dict(), f, sort_keys=True)
            f.write("\n")

    deduped_ris_path = output_dir / "deduped_auto.ris"
    write_ris_file(merged_records, deduped_ris_path)

    # Collect review records as CanonicalRecord objects (not dicts)
    review_records: list[CanonicalRecord] = []
    for rev_cluster in review_clusters:
        if records_map is not None:
            cluster_records_dict = {
                rid: records_map[rid] for rid in rev_cluster.rids if rid in records_map
            }
        else:
            cluster_records_dict = load_records_batch(rev_cluster.rids, records_dir)
        for rid in sorted(rev_cluster.rids):
            if rid in cluster_records_dict:
                review_records.append(cluster_records_dict[rid])

    summary.records_out_review_pending = len(review_records)

    if review_records:
        review_ris_path = output_dir / "review_pending.ris"
        write_ris_from_records(review_records, review_ris_path)

    clusters_enriched_path = output_dir / "clusters_enriched.jsonl"
    with clusters_enriched_path.open("w") as f:
        for cluster_dict in enriched_clusters:
            json.dump(cluster_dict, f, sort_keys=True)
            f.write("\n")

    summary_path = output_dir.parent / "reports" / "merge_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w") as f:
        json.dump(summary.to_dict(), f, indent=2, sort_keys=True)

    return summary
