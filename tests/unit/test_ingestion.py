"""Integration tests for multi-file ingestion orchestration."""

from pathlib import Path

import pytest

from srdedupe.parse.ingestion import ingest_file, ingest_folder


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to synthetic fixtures directory."""
    return Path(__file__).parent.parent / "fixtures" / "synthetic"


@pytest.mark.integration
def test_ingest_folder_finds_all_formats(fixtures_dir: Path) -> None:
    """Test folder ingestion discovers all supported formats."""
    records, report = ingest_folder(fixtures_dir, recursive=False)

    # Should find 6 files: .ris, .nbib, .txt, .bib, .ciw, .enw
    assert report.total_files == 6
    assert report.total_records > 0
    assert len(records) == report.total_records

    # All 5 formats detected
    formats_found = {r.format_detected for r in report.file_results}
    expected = {"ris", "pubmed", "bibtex", "wos", "endnote_tagged"}
    assert expected.issubset(formats_found)


@pytest.mark.integration
def test_ingest_folder_report_structure(fixtures_dir: Path) -> None:
    """Test IngestionReport structure is frozen and complete."""
    records, report = ingest_folder(fixtures_dir)

    # Report fields
    assert report.tool_version == "1.0.0"
    assert report.run_timestamp is not None
    assert report.total_files >= 0
    assert report.total_records >= 0
    assert len(report.file_results) == report.total_files

    # FileIngestionResult structure (frozen, tuples)
    for result in report.file_results:
        assert result.filename is not None
        assert result.filepath is not None
        assert result.file_size > 0
        assert result.format_detected in ["ris", "pubmed", "bibtex", "wos", "endnote_tagged"]
        assert result.encoding_used in ["utf-8", "utf-8-sig", "latin-1"]
        assert isinstance(result.warnings, tuple)
        assert isinstance(result.errors, tuple)
        if result.records_parsed > 0:
            assert result.file_digest.startswith("sha256:")


@pytest.mark.integration
def test_ingest_folder_deterministic(fixtures_dir: Path) -> None:
    """Test folder ingestion produces identical RIDs across runs."""
    records1, _ = ingest_folder(fixtures_dir)
    records2, _ = ingest_folder(fixtures_dir)

    assert len(records1) == len(records2)
    rids1 = [r.rid for r in records1]
    rids2 = [r.rid for r in records2]
    assert rids1 == rids2


@pytest.mark.integration
def test_ingest_folder_empty(tmp_path: Path) -> None:
    """Test empty folder returns zero results."""
    records, report = ingest_folder(tmp_path)

    assert report.total_files == 0
    assert report.total_records == 0
    assert len(records) == 0
    assert len(report.file_results) == 0


@pytest.mark.unit
def test_ingest_file_happy_path(fixtures_dir: Path) -> None:
    """Test single file ingestion."""
    ris_path = fixtures_dir / "sample.ris"
    records, result = ingest_file(ris_path)

    assert result.filename == "sample.ris"
    assert result.format_detected == "ris"
    assert result.records_parsed == 2
    assert len(records) == 2
    assert len(result.errors) == 0

    for rec in records:
        assert rec.meta.source_format == "ris"
        assert rec.rid is not None
        assert rec.record_digest.startswith("sha256:")


@pytest.mark.unit
def test_ingest_file_nonexistent() -> None:
    """Test nonexistent file returns empty with error."""
    fake_path = Path("/tmp/nonexistent_xyz.ris")
    records, result = ingest_file(fake_path)

    assert len(records) == 0
    assert len(result.errors) == 1
    assert "Failed to read file" in result.errors[0]
    assert result.records_parsed == 0
