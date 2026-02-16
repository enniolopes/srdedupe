"""Candidate pair generation orchestrator.

Coordinates multiple blocker plug-ins to produce a single, deduplicated
stream of candidate pairs written as deterministic JSONL.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from collections.abc import Iterable
from itertools import combinations
from pathlib import Path
from typing import Any

from srdedupe.audit.logger import AuditLogger
from srdedupe.candidates.blockers import Blocker, BlockerStats
from srdedupe.candidates.models import CandidatePair, CandidateSource
from srdedupe.models.records import CanonicalRecord
from srdedupe.utils import calculate_file_sha256

DEFAULT_MAX_BLOCK_SIZE = 1000
STAGE_NAME = "candidate_generation"


def generate_candidates(
    blockers: list[Blocker],
    records: Iterable[CanonicalRecord],
    output_path: Path,
    *,
    logger: AuditLogger | None = None,
    max_block_size: int = DEFAULT_MAX_BLOCK_SIZE,
) -> dict[str, Any]:
    """Generate candidate pairs from *records* using *blockers*.

    Parameters
    ----------
    blockers : list[Blocker]
        Blocker plug-ins to apply (order-independent).
    records : Iterable[CanonicalRecord]
        Input records (materialised once).
    output_path : Path
        Destination for ``candidates.jsonl``.
    logger : AuditLogger | None, optional
        Audit logger for observability events.
    max_block_size : int, optional
        Log a warning when a block exceeds this size.

    Returns
    -------
    dict[str, Any]
        ``{"blockers": {name: stats}, "global": {…}}``.
    """
    start = time.perf_counter()

    if logger:
        logger.stage_started(STAGE_NAME)

    # Sort blockers for deterministic output
    sorted_blockers = sorted(blockers, key=lambda b: b.name)
    records_list = list(records)

    # Initialize stateful blockers
    for blocker in sorted_blockers:
        if hasattr(blocker, "initialize"):
            blocker.initialize(records_list)

    # Run all blockers and aggregate pairs
    stats: dict[str, BlockerStats] = {}
    pair_sources: dict[str, list[CandidateSource]] = defaultdict(list)

    for blocker in sorted_blockers:
        blocker_stats, blocker_pairs = _run_blocker(blocker, records_list, max_block_size, logger)
        stats[blocker.name] = blocker_stats

        for pair_id, source in blocker_pairs.items():
            pair_sources[pair_id].append(source)

    # Write deterministic JSONL
    _write_jsonl(pair_sources, output_path)

    # Global stats
    global_stats = {
        "pairs_total_unique": len(pair_sources),
        "pairs_with_multiple_sources": sum(1 for srcs in pair_sources.values() if len(srcs) > 1),
    }

    # Audit: artifact written
    if logger:
        artifact_size = output_path.stat().st_size
        logger.artifact_written(
            path=str(output_path.name),
            sha256=calculate_file_sha256(output_path),
            stage=STAGE_NAME,
            bytes_written=artifact_size,
            record_count=global_stats["pairs_total_unique"],
        )

    duration = time.perf_counter() - start

    # Audit: stage finished
    if logger:
        flat: dict[str, int] = {}
        for bname, bstats in stats.items():
            for key, value in bstats.to_dict().items():
                flat[f"{bname}_{key}"] = value
        flat.update(global_stats)
        logger.stage_finished(
            stage=STAGE_NAME,
            duration_seconds=duration,
            counters=flat,
        )

    return {
        "blockers": {name: s.to_dict() for name, s in stats.items()},
        "global": global_stats,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _run_blocker(
    blocker: Blocker,
    records: list[CanonicalRecord],
    max_block_size: int,
    logger: AuditLogger | None,
) -> tuple[BlockerStats, dict[str, CandidateSource]]:
    """Index records by blocker keys, then emit pairs from each block."""
    stats = BlockerStats()

    # Phase 1 — build inverted index: key → [rid, …]
    index: dict[str, list[str]] = defaultdict(list)

    for record in records:
        stats.records_seen += 1
        keys = list(blocker.block_keys(record))
        if not keys:
            continue
        stats.records_keyed += 1
        for key in keys:
            index[key].append(record.rid)

    stats.unique_keys = len(index)

    # Phase 2 — emit candidate pairs from blocks with ≥ 2 records
    unique_pairs: dict[str, CandidateSource] = {}

    for block_key in sorted(index):
        rids = sorted(set(index[block_key]))
        block_size = len(rids)

        if block_size < 2:
            continue

        stats.blocks_gt1 += 1
        stats.max_block = max(stats.max_block, block_size)

        if block_size > max_block_size and logger:
            logger.event(
                "oversized_block",
                data={
                    "blocker": blocker.name,
                    "block_key": block_key[:100],
                    "block_size": block_size,
                    "max_block_size": max_block_size,
                },
                level="WARN",
                stage=STAGE_NAME,
            )

        source = CandidateSource(
            blocker=blocker.name,
            block_key=block_key,
            match_key=blocker.match_key,
        )

        for rid_a, rid_b in combinations(rids, 2):
            pair_id = f"{rid_a}|{rid_b}"  # rids already sorted
            stats.pairs_raw += 1
            if pair_id not in unique_pairs:
                unique_pairs[pair_id] = source

    stats.pairs_unique = len(unique_pairs)
    return stats, unique_pairs


def _write_jsonl(
    pair_sources: dict[str, list[CandidateSource]],
    output_path: Path,
) -> None:
    """Write candidate pairs as deterministic JSONL."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as fh:
        for pair_id in sorted(pair_sources):
            rid_a, rid_b = pair_id.split("|")
            pair = CandidatePair(
                pair_id=pair_id,
                rid_a=rid_a,
                rid_b=rid_b,
                sources=pair_sources[pair_id],
            )
            json.dump(pair.to_dict(), fh, ensure_ascii=False, separators=(",", ":"))
            fh.write("\n")
