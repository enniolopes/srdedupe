"""End-to-end deduplication pipeline runner.

This module chains all 6 stages of the srdedupe architecture into a single
deterministic, auditable pipeline.

Architecture Flow:
    Stage 1: Parsing & Normalization
    Stage 2: Candidate Generation
    Stage 3: Probabilistic Scoring
    Stage 4: Three-Way Decision
    Stage 5: Global Clustering
    Stage 6: Canonical Merge

Note: Pairs with REVIEW status are identified and preserved in outputs
but no automated review workflow is implemented.
"""

import json
import time
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any

from srdedupe.audit.logger import AuditLogger
from srdedupe.candidates.blockers import Blocker
from srdedupe.candidates.factory import BlockerConfig, create_blockers
from srdedupe.candidates.generator import generate_candidates
from srdedupe.clustering.cluster_builder import build_clusters
from srdedupe.clustering.models import ClusteringConfig
from srdedupe.decision.models import ConfusionMatrix, NPCalibration, Thresholds
from srdedupe.decision.policy import make_pair_decisions
from srdedupe.engine.config import PipelineConfig, PipelineResult
from srdedupe.merge.models import MergeSummary
from srdedupe.merge.processor import process_canonical_merge
from srdedupe.models import CanonicalRecord
from srdedupe.normalize import normalize
from srdedupe.parse.ingestion import IngestionReport, ingest_file, ingest_folder
from srdedupe.scoring.fs_model import FSModel
from srdedupe.scoring.score_pairs import score_all_pairs

# Fallback path to default FSModel - repo root is 3 levels up from engine/
_FALLBACK_FSMODEL_PATH = Path(__file__).parent.parent.parent.parent / "models" / "fs_v1.json"


# ---------------------------------------------------------------------------
# Stage directory helpers
# ---------------------------------------------------------------------------

_STAGE_DIRS = ("stage1", "stage2", "stage3", "stage4", "stage5", "artifacts", "reports")


def _ensure_output_dirs(output_dir: Path) -> dict[str, Path]:
    """Create and return all stage output directories."""
    dirs: dict[str, Path] = {}
    for name in _STAGE_DIRS:
        d = output_dir / name
        d.mkdir(parents=True, exist_ok=True)
        dirs[name] = d
    return dirs


def _write_ingestion_report(
    report: IngestionReport | None,
    output_dir: Path,
) -> None:
    """Persist ingestion report with per-file statistics.

    Parameters
    ----------
    report : IngestionReport | None
        Ingestion report from folder parsing (None for single-file).
    output_dir : Path
        Base pipeline output directory.
    """
    if report is None:
        return

    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "ingestion_report.json"

    report_dict = asdict(report)
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=2, ensure_ascii=False, sort_keys=True)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_fs_model(fs_model_path: Path | None = None) -> FSModel:
    """Load the Fellegi-Sunter model from path or fallback default.

    Parameters
    ----------
    fs_model_path : Path | None, optional
        Explicit model path. If None, uses bundled default.

    Returns
    -------
    FSModel
        Loaded Fellegi-Sunter model.

    Raises
    ------
    FileNotFoundError
        If the model file cannot be found.
    """
    path = fs_model_path if fs_model_path is not None else _FALLBACK_FSMODEL_PATH
    if not path.exists():
        raise FileNotFoundError(f"FS model not found: {path}. Set fs_model_path in PipelineConfig.")
    with path.open("r") as f:
        model_config = json.load(f)
    return FSModel(model_config)


def _create_thresholds(
    config: PipelineConfig,
) -> tuple[Thresholds, NPCalibration]:
    """Build thresholds and NP calibration from config."""
    if config.t_high is not None:
        t_high = config.t_high
        t_high_np = config.t_high
    else:
        # Default threshold when no calibration data is available
        t_high = 0.95
        t_high_np = 0.95

    thresholds = Thresholds(
        t_high=t_high,
        t_low=config.t_low,
        t_high_np=t_high_np,
        t_high_conformal=None,
    )

    # NP calibration metadata (zeroed until real calibration data is provided)
    np_calibration = NPCalibration(
        alpha=config.fpr_alpha,
        calibration_set="default",
        method="neyman_pearson",
        calibration_size=0,
        estimated_fpr=config.fpr_alpha,
        confusion_matrix=ConfusionMatrix(tp=0, fp=0, tn=0, fn=0),
    )

    return thresholds, np_calibration


# ---------------------------------------------------------------------------
# Individual stage functions
# ---------------------------------------------------------------------------


def _stage1_parse_and_normalize(
    input_path: Path,
    output_dir: Path,
    logger: AuditLogger | None,
) -> tuple[list[CanonicalRecord], IngestionReport | None]:
    """Stage 1: Parse input files and normalize fields.

    Returns
    -------
    tuple[list[CanonicalRecord], IngestionReport | None]
        Normalized records and the ingestion report (None for single-file).
    """
    if logger:
        logger.event("stage1_parse_started", stage="stage1_parse")

    report: IngestionReport | None = None

    if input_path.is_file():
        records_iter, _ = ingest_file(input_path)
        records = list(records_iter)
    else:
        records_iter, report = ingest_folder(input_path, recursive=True)
        records = list(records_iter)

    normalized_records = [normalize(rec) for rec in records]

    output_path = output_dir / "canonical_records.jsonl"
    with output_path.open("w", encoding="utf-8") as f:
        for record in normalized_records:
            json.dump(record.to_dict(), f, ensure_ascii=False, sort_keys=True)
            f.write("\n")

    if logger:
        logger.event(
            "stage1_parse_complete",
            stage="stage1_parse",
            data={"records_count": len(normalized_records)},
        )

    return normalized_records, report


def _stage2_generate_candidates(
    records: list[CanonicalRecord],
    config: PipelineConfig,
    output_dir: Path,
    logger: AuditLogger | None,
) -> dict[str, Any]:
    """Stage 2: Generate candidate pairs via blocking."""
    if logger:
        logger.event("stage2_candidates_started", stage="stage2_candidates")

    configs = [BlockerConfig(type=name) for name in (config.candidate_blockers or [])]
    blockers: list[Blocker] = create_blockers(configs)

    output_path = output_dir / "candidate_pairs.jsonl"
    stats = generate_candidates(
        blockers=blockers,
        records=records,
        output_path=output_path,
        logger=logger,
    )

    if logger:
        logger.event(
            "stage2_candidates_complete",
            stage="stage2_candidates",
            data={"candidate_pairs": stats.get("pairs_total_unique", 0)},
        )

    return stats


def _stage3_score_pairs(
    records: list[CanonicalRecord],
    candidates_dir: Path,
    output_dir: Path,
    logger: AuditLogger | None,
    fs_model_path: Path | None = None,
) -> dict[str, Any]:
    """Stage 3: Score all candidate pairs."""
    if logger:
        logger.event("stage3_scoring_started", stage="stage3_scoring")

    candidates_path = candidates_dir / "candidate_pairs.jsonl"
    output_path = output_dir / "scored_pairs.jsonl"
    model = _load_fs_model(fs_model_path)

    stats = score_all_pairs(
        candidates_path=candidates_path,
        records=records,
        output_path=output_path,
        model=model,
        logger=logger,
    )

    if logger:
        logger.event(
            "stage3_scoring_complete",
            stage="stage3_scoring",
            data={"pairs_scored": stats.get("pairs_scored", 0)},
        )

    return stats


def _stage4_make_decisions(
    records: list[CanonicalRecord],
    config: PipelineConfig,
    scored_dir: Path,
    output_dir: Path,
    logger: AuditLogger | None,
) -> dict[str, int]:
    """Stage 4: Make three-way decisions (AUTO_DUP/REVIEW/AUTO_KEEP)."""
    if logger:
        logger.event("stage4_decision_started", stage="stage4_decision")

    scored_pairs_path = scored_dir / "scored_pairs.jsonl"
    output_path = output_dir / "pair_decisions.jsonl"

    thresholds, np_calibration = _create_thresholds(config)

    summary = make_pair_decisions(
        pair_scores_path=scored_pairs_path,
        records=records,
        thresholds=thresholds,
        np_calibration=np_calibration,
        output_path=output_path,
        logger=logger,
        conformal_calibration=None,
    )

    stats = {
        "auto_dup": summary.auto_dup,
        "review": summary.review,
        "auto_keep": summary.auto_keep,
    }

    if logger:
        logger.event(
            "stage4_decision_complete",
            stage="stage4_decision",
            data=stats,
        )

    return stats


def _stage5_build_clusters(
    records: list[CanonicalRecord],
    decisions_dir: Path,
    output_dir: Path,
    logger: AuditLogger | None,
) -> int:
    """Stage 5: Build global clusters with consistency checks."""
    if logger:
        logger.event("stage5_clustering_started", stage="stage5_clustering")

    decisions_path = decisions_dir / "pair_decisions.jsonl"
    output_path = output_dir / "clusters.jsonl"

    clusters = build_clusters(
        pair_decisions_path=decisions_path,
        records=records,
        config=ClusteringConfig(),
    )

    with output_path.open("w", encoding="utf-8") as f:
        for cluster in clusters:
            json.dump(cluster.to_dict(), f, ensure_ascii=False, sort_keys=True)
            f.write("\n")

    if logger:
        logger.event(
            "stage5_clustering_complete",
            stage="stage5_clustering",
            data={"clusters_count": len(clusters)},
        )

    return len(clusters)


def _stage6_canonical_merge(
    clusters_dir: Path,
    records_dir: Path,
    artifacts_dir: Path,
    records: list[CanonicalRecord],
    logger: AuditLogger | None,
) -> MergeSummary:
    """Stage 6: Merge clusters and generate deduplicated outputs."""
    if logger:
        logger.event("stage6_merge_started", stage="stage6_merge")

    clusters_path = clusters_dir / "clusters.jsonl"
    records_map = {r.rid: r for r in records}

    summary = process_canonical_merge(
        clusters_path=clusters_path,
        records_dir=records_dir,
        output_dir=artifacts_dir,
        records_map=records_map,
    )

    if logger:
        logger.event(
            "stage6_merge_complete",
            stage="stage6_merge",
            data={
                "merged_clusters": summary.auto_clusters_merged,
                "singletons_count": summary.singletons_count,
                "records_out_review_pending": summary.records_out_review_pending,
                "records_out_unique_total": summary.records_out_unique_total,
                "dedup_rate": summary.dedup_rate,
            },
        )

    return summary


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _run_stages(
    input_path: Path,
    config: PipelineConfig,
    logger: AuditLogger | None,
) -> PipelineResult:
    """Execute all 6 pipeline stages sequentially.

    Accumulates partial results so that diagnostic information
    is preserved even when a late stage fails.
    """
    start_time = time.perf_counter() if config.track_execution_time else None

    if not input_path.exists():
        return PipelineResult(
            success=False,
            total_records=0,
            total_candidates=0,
            total_duplicates_auto=0,
            total_review_records=0,
            total_unique_records=0,
            dedup_rate=0.0,
            output_files={},
            error_message=f"Input path does not exist: {input_path}",
        )

    dirs = _ensure_output_dirs(config.output_dir)

    total_records = 0
    total_candidates = 0
    total_duplicates_auto = 0
    total_review_records = 0
    total_unique_records = 0
    dedup_rate = 0.0
    output_files: dict[str, str] = {}

    try:
        records, ingestion_report = _stage1_parse_and_normalize(input_path, dirs["stage1"], logger)
        total_records = len(records)

        # Persist ingestion report with per-file statistics
        _write_ingestion_report(ingestion_report, config.output_dir)

        if total_records == 0:
            return PipelineResult(
                success=False,
                total_records=0,
                total_candidates=0,
                total_duplicates_auto=0,
                total_review_records=0,
                total_unique_records=0,
                dedup_rate=0.0,
                output_files={},
                error_message="No records found in input",
            )

        candidate_stats = _stage2_generate_candidates(records, config, dirs["stage2"], logger)
        total_candidates = candidate_stats.get("pairs_total_unique", 0)

        _stage3_score_pairs(records, dirs["stage2"], dirs["stage3"], logger, config.fs_model_path)

        _stage4_make_decisions(records, config, dirs["stage3"], dirs["stage4"], logger)

        _stage5_build_clusters(records, dirs["stage4"], dirs["stage5"], logger)

        merge_summary = _stage6_canonical_merge(
            dirs["stage5"], dirs["stage1"], dirs["artifacts"], records, logger
        )

        # Capture total pipeline execution time
        if start_time is not None:
            merge_summary.execution_time_seconds = time.perf_counter() - start_time
            summary_path = dirs["reports"] / "merge_summary.json"
            with summary_path.open("w") as f:
                json.dump(merge_summary.to_dict(), f, indent=2, sort_keys=True)

        # Record/cluster-level metrics from the final merge stage
        total_duplicates_auto = merge_summary.auto_clusters_merged
        total_review_records = merge_summary.records_out_review_pending
        total_unique_records = merge_summary.records_out_unique_total
        dedup_rate = merge_summary.dedup_rate

        output_files = {
            "canonical_records": str(dirs["stage1"] / "canonical_records.jsonl"),
            "candidate_pairs": str(dirs["stage2"] / "candidate_pairs.jsonl"),
            "scored_pairs": str(dirs["stage3"] / "scored_pairs.jsonl"),
            "decisions": str(dirs["stage4"] / "pair_decisions.jsonl"),
            "clusters": str(dirs["stage5"] / "clusters.jsonl"),
            "deduplicated_ris": str(dirs["artifacts"] / "deduped_auto.ris"),
            "merged_records": str(dirs["artifacts"] / "merged_records.jsonl"),
            "clusters_enriched": str(dirs["artifacts"] / "clusters_enriched.jsonl"),
        }

        ingestion_report_path = dirs["reports"] / "ingestion_report.json"
        if ingestion_report_path.exists():
            output_files["ingestion_report"] = str(ingestion_report_path)

        singletons_ris = dirs["artifacts"] / "singletons.ris"
        if singletons_ris.exists():
            output_files["singletons_ris"] = str(singletons_ris)

        review_ris = dirs["artifacts"] / "review_pending.ris"
        if review_ris.exists():
            output_files["review_pending_ris"] = str(review_ris)

        return PipelineResult(
            success=True,
            total_records=total_records,
            total_candidates=total_candidates,
            total_duplicates_auto=total_duplicates_auto,
            total_review_records=total_review_records,
            total_unique_records=total_unique_records,
            dedup_rate=dedup_rate,
            output_files=output_files,
        )

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        if logger:
            logger.event(
                "pipeline_error",
                stage="pipeline",
                data={"error": error_msg, "traceback": traceback.format_exc()},
                level="ERROR",
            )
        return PipelineResult(
            success=False,
            total_records=total_records,
            total_candidates=total_candidates,
            total_duplicates_auto=total_duplicates_auto,
            total_review_records=total_review_records,
            total_unique_records=total_unique_records,
            dedup_rate=dedup_rate,
            output_files=output_files,
            error_message=error_msg,
        )


def run_pipeline(
    input_path: Path | str,
    config: PipelineConfig | None = None,
    logger: AuditLogger | None = None,
) -> PipelineResult:
    """Run the complete deduplication pipeline.

    This is the main entry point for running the full 6-stage pipeline
    with the frozen architecture from Issue #16.

    Parameters
    ----------
    input_path : Path | str
        Path to input file or folder.
    config : PipelineConfig | None, optional
        Pipeline configuration. If None, uses defaults.
    logger : AuditLogger | None, optional
        Audit logger for tracking. If None, no logging.

    Returns
    -------
    PipelineResult
        Pipeline execution results.

    Examples
    --------
    Run with defaults:

        >>> from srdedupe.engine import run_pipeline
        >>> result = run_pipeline("data/references.ris")
        >>> if result.success:
        ...     print(f"Deduplicated {result.total_records} records")

    Run with custom config:

        >>> from pathlib import Path
        >>> from srdedupe.engine import PipelineConfig
        >>> config = PipelineConfig(fpr_alpha=0.005, t_low=0.4)
        >>> result = run_pipeline(Path("data/"), config=config)
    """
    input_path = Path(input_path)

    if config is None:
        config = PipelineConfig()

    return _run_stages(input_path, config, logger)
