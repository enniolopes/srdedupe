"""Pipeline configuration and result dataclasses."""

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class PipelineConfig:
    """Configuration for the complete deduplication pipeline.

    Attributes
    ----------
    fpr_alpha : float
        Maximum acceptable false positive rate (default: 0.01 = 1%).
    t_low : float
        Lower threshold for AUTO_KEEP decision (default: 0.3).
    t_high : float | None
        Upper threshold for AUTO_DUP. If None, computed via Neyman-Pearson.
    candidate_blockers : list[str]
        Blocker names to use. Available: 'doi', 'pmid', 'year_title'.
    fs_model_path : Path | None
        Path to Fellegi-Sunter model JSON. If None, uses bundled default.
    output_dir : Path
        Base directory for all outputs.
    track_execution_time : bool
        Record total pipeline wall-clock time in merge_summary.json.
    """

    fpr_alpha: float = 0.01
    t_low: float = 0.3
    t_high: float | None = None
    candidate_blockers: list[str] | None = None
    fs_model_path: Path | None = None
    output_dir: Path = Path("out")
    track_execution_time: bool = False

    def __post_init__(self) -> None:
        """Set defaults and validate."""
        if self.candidate_blockers is None:
            self.candidate_blockers = ["doi", "pmid", "year_title"]

        if not 0.0 <= self.fpr_alpha <= 1.0:
            raise ValueError(f"fpr_alpha must be in [0, 1], got {self.fpr_alpha}")

        if not 0.0 <= self.t_low <= 1.0:
            raise ValueError(f"t_low must be in [0, 1], got {self.t_low}")

        if self.t_high is not None and not 0.0 <= self.t_high <= 1.0:
            raise ValueError(f"t_high must be in [0, 1], got {self.t_high}")

        if self.t_high is not None and self.t_high <= self.t_low:
            raise ValueError(f"t_high ({self.t_high}) must be greater than t_low ({self.t_low})")

        if self.fs_model_path is not None:
            self.fs_model_path = Path(self.fs_model_path)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["output_dir"] = str(self.output_dir)
        data["fs_model_path"] = str(self.fs_model_path) if self.fs_model_path is not None else None
        return data


@dataclass
class PipelineResult:
    """Results from pipeline execution.

    All metrics are **record/cluster-level**, derived from the final
    merge stage (stage 6).  Pair-level counts from the decision stage
    are internal and not exposed here.

    Attributes
    ----------
    success : bool
        Whether pipeline completed successfully.
    total_records : int
        Total records ingested.
    total_candidates : int
        Candidate pairs generated.
    total_duplicates_auto : int
        Duplicate clusters auto-merged (each cluster had 2+ records
        that were collapsed into one representative record).
    total_review_records : int
        Individual records in clusters flagged for human review.
    total_unique_records : int
        Unique records after automated deduplication
        (singletons + auto-merged clusters, excludes review records).
    dedup_rate : float
        Fraction of input records removed by deduplication (0.0–1.0).
    output_files : dict[str, str]
        Map of artifact type to file path.
    error_message : str | None
        Error message if failed.
    """

    success: bool
    total_records: int
    total_candidates: int
    total_duplicates_auto: int
    total_review_records: int
    total_unique_records: int
    dedup_rate: float
    output_files: dict[str, str]
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
