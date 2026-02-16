"""Unit tests for the deduplication pipeline orchestrator."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from srdedupe.clustering.models import (
    Cluster,
    ClusterConsistency,
    ClusterStatus,
    ClusterSupport,
)
from srdedupe.decision.models import DecisionSummary, Thresholds
from srdedupe.engine import PipelineConfig, PipelineResult, run_pipeline
from srdedupe.engine.runner import (
    _create_thresholds,
    _ensure_output_dirs,
    _load_fs_model,
)
from srdedupe.merge.models import MergeSummary
from srdedupe.models import CanonicalRecord

# ---------------------------------------------------------------------------
# PipelineConfig
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pipeline_config_defaults() -> None:
    """Test PipelineConfig default values."""
    config = PipelineConfig()

    assert config.fpr_alpha == 0.01
    assert config.t_low == 0.3
    assert config.t_high is None
    assert config.fs_model_path is None
    assert config.candidate_blockers == ["doi", "pmid", "year_title"]
    assert config.output_dir == Path("out")


@pytest.mark.unit
def test_pipeline_config_custom_values() -> None:
    """Test PipelineConfig with custom values."""
    config = PipelineConfig(
        fpr_alpha=0.005,
        t_low=0.4,
        t_high=0.95,
        candidate_blockers=["doi"],
        fs_model_path=Path("custom_model.json"),
        output_dir=Path("custom_out"),
    )

    assert config.fpr_alpha == 0.005
    assert config.t_low == 0.4
    assert config.t_high == 0.95
    assert config.candidate_blockers == ["doi"]
    assert config.fs_model_path == Path("custom_model.json")
    assert config.output_dir == Path("custom_out")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"fpr_alpha": -0.1}, "fpr_alpha must be in"),
        ({"fpr_alpha": 1.5}, "fpr_alpha must be in"),
        ({"t_low": -0.1}, "t_low must be in"),
        ({"t_low": 1.5}, "t_low must be in"),
        ({"t_high": -0.1}, "t_high must be in"),
        ({"t_high": 1.5}, "t_high must be in"),
        ({"t_low": 0.5, "t_high": 0.5}, "t_high .* must be greater than t_low"),
        ({"t_low": 0.8, "t_high": 0.3}, "t_high .* must be greater than t_low"),
    ],
    ids=[
        "fpr_alpha_negative",
        "fpr_alpha_above_1",
        "t_low_negative",
        "t_low_above_1",
        "t_high_negative",
        "t_high_above_1",
        "t_high_equals_t_low",
        "t_high_below_t_low",
    ],
)
def test_pipeline_config_validation(kwargs: dict, match: str) -> None:
    """Test PipelineConfig rejects invalid parameter combinations."""
    with pytest.raises(ValueError, match=match):
        PipelineConfig(**kwargs)


@pytest.mark.unit
def test_pipeline_config_to_dict() -> None:
    """Test PipelineConfig to_dict serializes all fields correctly."""
    config = PipelineConfig(
        fpr_alpha=0.01,
        t_low=0.3,
        fs_model_path=Path("/tmp/m.json"),
        output_dir=Path("out"),
    )

    result = config.to_dict()

    assert result["fpr_alpha"] == 0.01
    assert result["t_low"] == 0.3
    assert result["output_dir"] == "out"
    assert isinstance(result["output_dir"], str)
    assert result["fs_model_path"] == "/tmp/m.json"

    # None path serializes to None
    config_none = PipelineConfig()
    assert config_none.to_dict()["fs_model_path"] is None


# ---------------------------------------------------------------------------
# PipelineResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pipeline_result_to_dict() -> None:
    """Test PipelineResult to_dict round-trips all fields."""
    result = PipelineResult(
        success=True,
        total_records=100,
        total_candidates=50,
        total_duplicates_auto=10,
        total_review_pairs=5,
        output_files={"canonical_records": "out/stage1/canonical_records.jsonl"},
        error_message=None,
    )

    d = result.to_dict()

    assert d["success"] is True
    assert d["total_records"] == 100
    assert d["total_candidates"] == 50
    assert d["total_duplicates_auto"] == 10
    assert d["total_review_pairs"] == 5
    assert "canonical_records" in d["output_files"]
    assert d["error_message"] is None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ensure_output_dirs(tmp_path: Path) -> None:
    """Test all stage directories are created with correct keys."""
    dirs = _ensure_output_dirs(tmp_path / "out")

    expected = {"stage1", "stage2", "stage3", "stage4", "stage5", "artifacts"}
    assert set(dirs.keys()) == expected

    for name in expected:
        assert (tmp_path / "out" / name).is_dir()
        assert dirs[name] == tmp_path / "out" / name


@pytest.mark.unit
def test_load_fs_model_default() -> None:
    """Test _load_fs_model loads bundled model when no path given."""
    model = _load_fs_model()

    assert model.name == "fs_v1"
    assert model.version == "1.0.0"
    assert 0.0 < model.lambda_prior < 1.0


@pytest.mark.unit
def test_load_fs_model_explicit_path(tmp_path: Path) -> None:
    """Test _load_fs_model with explicit model path."""
    model_data = {
        "model": {"name": "test_model", "version": "0.1.0", "description": "Test"},
        "lambda_prior": 0.01,
        "round_decimals": 6,
        "fields": {},
    }
    model_path = tmp_path / "test_model.json"
    model_path.write_text(json.dumps(model_data))

    model = _load_fs_model(model_path)
    assert model.name == "test_model"


@pytest.mark.unit
def test_load_fs_model_missing_path_raises() -> None:
    """Test _load_fs_model raises FileNotFoundError for missing path."""
    with pytest.raises(FileNotFoundError, match="FS model not found"):
        _load_fs_model(Path("/nonexistent/model.json"))


@pytest.mark.unit
def test_create_thresholds_explicit() -> None:
    """Test threshold creation with explicit t_high."""
    config = PipelineConfig(fpr_alpha=0.01, t_low=0.3, t_high=0.95)

    thresholds, np_cal = _create_thresholds(config)

    assert thresholds.t_high == 0.95
    assert thresholds.t_low == 0.3
    assert thresholds.t_high_np == 0.95
    assert np_cal.alpha == 0.01
    assert np_cal.method == "neyman_pearson"


@pytest.mark.unit
def test_create_thresholds_auto() -> None:
    """Test threshold creation with automatic t_high defaults to 0.95."""
    config = PipelineConfig(fpr_alpha=0.01, t_low=0.3, t_high=None)

    thresholds, np_cal = _create_thresholds(config)

    assert thresholds.t_high == 0.95
    assert thresholds.t_low == 0.3
    assert np_cal.alpha == 0.01


# ---------------------------------------------------------------------------
# Individual stage functions
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_stage1_parse_and_normalize(tmp_path: Path) -> None:
    """Test stage 1 parsing and normalization with real RIS."""
    from srdedupe.engine.runner import _stage1_parse_and_normalize

    output_dir = tmp_path / "out" / "stage1"
    output_dir.mkdir(parents=True)

    input_file = tmp_path / "input.ris"
    input_file.write_text(
        "TY  - JOUR\nTI  - Test Article\nPY  - 2023\nER  -\n",
        encoding="utf-8",
    )

    records = _stage1_parse_and_normalize(input_file, output_dir, logger=None)

    assert len(records) > 0
    assert isinstance(records[0], CanonicalRecord)
    assert (output_dir / "canonical_records.jsonl").exists()


@pytest.mark.unit
@patch("srdedupe.engine.runner.score_all_pairs")
def test_stage3_score_pairs(mock_score: Mock, tmp_path: Path) -> None:
    """Test stage 3 delegates to score_all_pairs and returns stats."""
    from srdedupe.engine.runner import _stage3_score_pairs

    candidates_dir = tmp_path / "stage2"
    candidates_dir.mkdir(parents=True)
    (candidates_dir / "candidate_pairs.jsonl").write_text("")

    output_dir = tmp_path / "stage3"
    output_dir.mkdir(parents=True)

    mock_score.return_value = {"pairs_scored": 42}

    stats = _stage3_score_pairs(
        records=[], candidates_dir=candidates_dir, output_dir=output_dir, logger=None
    )

    assert stats["pairs_scored"] == 42
    mock_score.assert_called_once()


@pytest.mark.unit
@patch("srdedupe.engine.runner.make_pair_decisions")
def test_stage4_make_decisions(mock_decisions: Mock, tmp_path: Path) -> None:
    """Test stage 4 extracts decision counts from summary."""
    from srdedupe.engine.runner import _stage4_make_decisions

    scored_dir = tmp_path / "stage3"
    scored_dir.mkdir(parents=True)
    (scored_dir / "scored_pairs.jsonl").write_text("")

    output_dir = tmp_path / "stage4"
    output_dir.mkdir(parents=True)

    mock_decisions.return_value = DecisionSummary(
        pairs_in=100,
        auto_dup=30,
        review=20,
        auto_keep=50,
        forced_review_conflicting_ids=5,
        forced_review_special_records=3,
        forced_review_data_quality=0,
        estimated_fpr_at_t_high=0.01,
        alpha=0.01,
        thresholds=Thresholds(t_high=0.95, t_low=0.3),
    )

    config = PipelineConfig()
    stats = _stage4_make_decisions(
        records=[], config=config, scored_dir=scored_dir, output_dir=output_dir, logger=None
    )

    assert stats == {"auto_dup": 30, "review": 20, "auto_keep": 50}


@pytest.mark.unit
@patch("srdedupe.engine.runner.build_clusters")
def test_stage5_build_clusters(mock_build: Mock, tmp_path: Path) -> None:
    """Test stage 5 writes clusters to JSONL and returns count."""
    from srdedupe.engine.runner import _stage5_build_clusters

    decisions_dir = tmp_path / "stage4"
    decisions_dir.mkdir(parents=True)
    (decisions_dir / "pair_decisions.jsonl").write_text("")

    output_dir = tmp_path / "stage5"
    output_dir.mkdir(parents=True)

    mock_build.return_value = [
        Cluster(
            cluster_id="c:test",
            status=ClusterStatus.AUTO,
            rids=["r:1", "r:2"],
            support=ClusterSupport(edges_auto_dup=1, strong_edge_count=1, sources={}),
            consistency=ClusterConsistency(),
        )
    ]

    num_clusters = _stage5_build_clusters(
        records=[], decisions_dir=decisions_dir, output_dir=output_dir, logger=None
    )

    assert num_clusters == 1
    assert (output_dir / "clusters.jsonl").exists()


@pytest.mark.unit
@patch("srdedupe.engine.runner.process_canonical_merge")
def test_stage6_canonical_merge(mock_merge: Mock, tmp_path: Path) -> None:
    """Test stage 6 computes correct output stats from merge summary."""
    from srdedupe.engine.runner import _stage6_canonical_merge

    clusters_dir = tmp_path / "stage5"
    clusters_dir.mkdir(parents=True)
    (clusters_dir / "clusters.jsonl").write_text("")

    records_dir = tmp_path / "stage1"
    records_dir.mkdir(parents=True)

    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir(parents=True)

    mock_summary = MergeSummary(clusters_auto_in=10, clusters_review_in=5)
    mock_summary.auto_clusters_merged = 10
    mock_summary.records_out_deduped_auto = 90
    mock_summary.records_out_review_pending = 10
    mock_merge.return_value = mock_summary

    stats = _stage6_canonical_merge(
        clusters_dir=clusters_dir,
        records_dir=records_dir,
        artifacts_dir=artifacts_dir,
        records=[],
        logger=None,
    )

    assert stats == {"merged_clusters": 10, "total_records_output": 100}


# ---------------------------------------------------------------------------
# run_pipeline (public API)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_pipeline_no_input(tmp_path: Path) -> None:
    """Test run_pipeline with non-existent input."""
    result = run_pipeline(
        input_path=tmp_path / "nonexistent.ris",
        config=PipelineConfig(output_dir=tmp_path / "out"),
    )

    assert result.success is False
    assert "does not exist" in result.error_message


@pytest.mark.unit
def test_run_pipeline_empty_input(tmp_path: Path) -> None:
    """Test run_pipeline with empty input file."""
    input_file = tmp_path / "empty.ris"
    input_file.write_text("", encoding="utf-8")

    result = run_pipeline(
        input_path=input_file,
        config=PipelineConfig(output_dir=tmp_path / "out"),
    )

    assert result.success is False
    assert result.total_records == 0


@pytest.mark.unit
def test_run_pipeline_accepts_string_path(tmp_path: Path) -> None:
    """Test run_pipeline accepts str and converts to Path."""
    result = run_pipeline(
        input_path=str(tmp_path / "nonexistent.ris"),
        config=PipelineConfig(output_dir=tmp_path / "out"),
    )

    assert result.success is False
    assert "does not exist" in result.error_message


@pytest.mark.unit
def test_run_pipeline_error_includes_exception_type(tmp_path: Path) -> None:
    """Test error message includes exception type for diagnostics."""
    input_file = tmp_path / "bad.ris"
    input_file.write_text("TY  - JOUR\nTI  - Test\nER  -\n", encoding="utf-8")

    result = run_pipeline(
        input_path=input_file,
        config=PipelineConfig(
            output_dir=tmp_path / "out",
            fs_model_path=Path("/nonexistent/model.json"),
        ),
    )

    assert result.success is False
    assert "FileNotFoundError" in result.error_message


@pytest.mark.unit
def test_run_pipeline_preserves_partial_results_on_failure(tmp_path: Path) -> None:
    """Test that pipeline preserves counts from completed stages on failure."""
    input_file = tmp_path / "input.ris"
    input_file.write_text(
        "TY  - JOUR\nTI  - Test Article\nPY  - 2023\nER  -\n",
        encoding="utf-8",
    )

    # Stage 1 will succeed (parsing), stage 3 will fail (bad model path)
    result = run_pipeline(
        input_path=input_file,
        config=PipelineConfig(
            output_dir=tmp_path / "out",
            fs_model_path=Path("/nonexistent/model.json"),
        ),
    )

    assert result.success is False
    assert result.total_records > 0  # Stage 1 result preserved


@pytest.mark.unit
@patch("srdedupe.engine.runner._run_stages")
def test_run_pipeline_passes_logger(mock_run: Mock, tmp_path: Path) -> None:
    """Test run_pipeline forwards logger to _run_stages."""
    from srdedupe.audit.logger import AuditLogger

    mock_run.return_value = PipelineResult(
        success=True,
        total_records=1,
        total_candidates=0,
        total_duplicates_auto=0,
        total_review_pairs=0,
        output_files={},
    )

    config = PipelineConfig(output_dir=tmp_path / "out")
    logger = AuditLogger(run_id="test123", log_path=tmp_path / "logs" / "events.jsonl")
    input_file = tmp_path / "test.ris"
    input_file.write_text("TY  - JOUR\nER  -\n", encoding="utf-8")

    run_pipeline(input_path=input_file, config=config, logger=logger)

    call_args, _ = mock_run.call_args
    assert call_args[2] is logger
