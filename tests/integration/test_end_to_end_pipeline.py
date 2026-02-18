"""Integration tests for end-to-end deduplication pipeline.

This module tests the complete 6-stage pipeline with real data flows.
"""

import json
import tempfile
from pathlib import Path

import pytest

from srdedupe.engine import PipelineConfig, run_pipeline


@pytest.mark.integration
def test_end_to_end_pipeline_with_no_duplicates() -> None:
    """Test full pipeline with non-duplicate records."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Use synthetic sample file with 2 non-duplicate records
        input_file = Path("tests/fixtures/synthetic/sample.ris")
        output_dir = tmpdir_path / "out"

        # Configure pipeline
        config = PipelineConfig(
            fpr_alpha=0.01,
            t_low=0.3,
            t_high=None,  # Auto-compute
            candidate_blockers=["doi", "pmid", "year_title"],
            output_dir=output_dir,
        )

        # Run pipeline
        result = run_pipeline(input_path=input_file, config=config)

        # Verify success
        assert result.success
        assert result.error_message is None

        # Verify record counts
        assert result.total_records == 2
        assert result.total_candidates >= 0  # May be 0 or more
        assert result.total_duplicates_auto == 0  # No duplicates expected
        assert result.total_review_records >= 0
        assert result.total_unique_records >= 0
        assert 0.0 <= result.dedup_rate <= 1.0

        # Verify output files exist
        assert "canonical_records" in result.output_files
        assert "deduplicated_ris" in result.output_files

        canonical_path = Path(result.output_files["canonical_records"])
        assert canonical_path.exists()

        deduped_ris_path = Path(result.output_files["deduplicated_ris"])
        assert deduped_ris_path.exists()

        # Verify canonical records JSONL format
        with canonical_path.open() as f:
            lines = f.readlines()
            assert len(lines) == 2
            for line in lines:
                record_dict = json.loads(line)
                assert "rid" in record_dict
                assert "canon" in record_dict
                assert "schema_version" in record_dict


@pytest.mark.integration
def test_end_to_end_pipeline_with_mini_dataset() -> None:
    """Test full pipeline with mini real dataset."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Use real mini dataset
        input_file = Path("tests/fixtures/real/mini_generic.ris")
        if not input_file.exists():
            pytest.skip("mini_generic.ris not found")

        output_dir = tmpdir_path / "out"

        # Configure pipeline with permissive settings
        config = PipelineConfig(
            fpr_alpha=0.05,  # More permissive for testing
            t_low=0.2,
            t_high=0.8,
            candidate_blockers=["doi", "pmid", "year_title"],
            output_dir=output_dir,
        )

        # Run pipeline
        result = run_pipeline(input_path=input_file, config=config)

        # Verify success
        assert result.success
        assert result.total_records > 0

        # Verify all stage outputs exist
        for name, path in result.output_files.items():
            path_obj = Path(path)
            assert path_obj.exists(), f"Output file missing: {name} at {path}"


@pytest.mark.integration
def test_end_to_end_pipeline_determinism() -> None:
    """Test that pipeline produces identical results on repeated runs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        input_file = Path("tests/fixtures/synthetic/sample.ris")

        # Run 1
        output_dir_1 = tmpdir_path / "run1"
        config_1 = PipelineConfig(
            fpr_alpha=0.01,
            t_low=0.3,
            t_high=0.95,  # Fixed threshold for determinism
            candidate_blockers=["doi", "pmid"],
            output_dir=output_dir_1,
        )
        result_1 = run_pipeline(input_path=input_file, config=config_1)

        # Run 2 with identical config
        output_dir_2 = tmpdir_path / "run2"
        config_2 = PipelineConfig(
            fpr_alpha=0.01,
            t_low=0.3,
            t_high=0.95,
            candidate_blockers=["doi", "pmid"],
            output_dir=output_dir_2,
        )
        result_2 = run_pipeline(input_path=input_file, config=config_2)

        # Verify both succeeded
        assert result_1.success
        assert result_2.success

        # Verify identical statistics
        assert result_1.total_records == result_2.total_records
        assert result_1.total_candidates == result_2.total_candidates
        assert result_1.total_duplicates_auto == result_2.total_duplicates_auto
        assert result_1.total_review_records == result_2.total_review_records
        assert result_1.total_unique_records == result_2.total_unique_records
        assert result_1.dedup_rate == result_2.dedup_rate

        # Verify canonical records are identical (content-wise)
        canonical_1 = Path(result_1.output_files["canonical_records"])
        canonical_2 = Path(result_2.output_files["canonical_records"])

        with canonical_1.open() as f1, canonical_2.open() as f2:
            lines_1 = [json.loads(line) for line in f1]
            lines_2 = [json.loads(line) for line in f2]

            assert len(lines_1) == len(lines_2)

            # Sort by RID for comparison (order might vary slightly)
            lines_1_sorted = sorted(lines_1, key=lambda x: x["rid"])
            lines_2_sorted = sorted(lines_2, key=lambda x: x["rid"])

            for rec1, rec2 in zip(lines_1_sorted, lines_2_sorted, strict=False):
                # Compare key fields for determinism
                assert rec1["rid"] == rec2["rid"]
                assert rec1["canon"] == rec2["canon"]
                assert rec1["keys"] == rec2["keys"]
                assert rec1["flags"] == rec2["flags"]


@pytest.mark.integration
def test_end_to_end_pipeline_error_handling() -> None:
    """Test pipeline error handling with invalid inputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Non-existent input
        result = run_pipeline(
            input_path=Path("does_not_exist.ris"),
            config=PipelineConfig(output_dir=tmpdir_path),
        )

        assert not result.success
        assert result.error_message is not None
        assert "does not exist" in result.error_message.lower()


@pytest.mark.integration
@pytest.mark.slow
def test_end_to_end_pipeline_with_folder_input() -> None:
    """Test pipeline with folder containing multiple files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Use fixtures folder with multiple files
        input_folder = Path("tests/fixtures/synthetic")
        if not input_folder.exists():
            pytest.skip("Synthetic fixtures folder not found")

        output_dir = tmpdir_path / "out"

        config = PipelineConfig(
            fpr_alpha=0.01,
            t_low=0.3,
            candidate_blockers=["doi", "pmid"],
            output_dir=output_dir,
        )

        # Run pipeline
        result = run_pipeline(input_path=input_folder, config=config)

        # Verify success
        assert result.success
        assert result.total_records >= 2  # At least 2 records from sample.ris

        # Verify outputs
        canonical_path = Path(result.output_files["canonical_records"])
        assert canonical_path.exists()

        with canonical_path.open() as f:
            records = [json.loads(line) for line in f]
            assert len(records) >= 2
