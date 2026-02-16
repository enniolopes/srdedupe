"""Smoke tests for real-world fixtures."""

from pathlib import Path

import pytest

from srdedupe.models.identifiers import validate_rid_format
from srdedupe.parse.ingestion import ingest_file


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to real-world fixtures."""
    return Path(__file__).parent.parent / "fixtures" / "real"


# Expected counts for each fixture
FIXTURE_EXPECTATIONS = {
    "mini_generic.ris": {"format": "ris", "min_records": 3},
    "mini_pubmed.nbib": {"format": "pubmed", "min_records": 1},
    "mini_scopus.bib": {"format": "bibtex", "min_records": 50},
    "mini_wos.ciw": {"format": "wos", "min_records": 1},
    "mini_endnote.enw": {"format": "endnote_tagged", "min_records": 2},
    "mini_scholar.enw": {"format": "endnote_tagged", "min_records": 1},
}


@pytest.mark.unit
@pytest.mark.parametrize("fixture_name,expectations", FIXTURE_EXPECTATIONS.items())
def test_real_fixture_parses_without_errors(
    fixtures_dir: Path, fixture_name: str, expectations: dict
) -> None:
    """Test real-world fixture parses successfully."""
    file_path = fixtures_dir / fixture_name
    records, result = ingest_file(file_path)

    # No parsing errors
    assert len(result.errors) == 0, f"{fixture_name}: {result.errors}"

    # Correct format detected
    assert result.format_detected == expectations["format"]

    # Minimum record count
    assert result.records_parsed >= expectations["min_records"]
    assert len(records) >= expectations["min_records"]

    # Each record is valid
    for rec in records:
        assert rec.schema_version == "1.0.0"
        assert validate_rid_format(rec.rid)
        assert rec.record_digest.startswith("sha256:")
        assert rec.source_digest.startswith("sha256:")
        assert rec.meta.source_format == expectations["format"]
        assert len(rec.raw.record_lines) > 0
        assert len(rec.raw.tags) > 0


@pytest.mark.unit
@pytest.mark.parametrize("fixture_name", FIXTURE_EXPECTATIONS.keys())
def test_real_fixture_deterministic(fixtures_dir: Path, fixture_name: str) -> None:
    """Test real-world fixture produces identical RIDs."""
    file_path = fixtures_dir / fixture_name
    records1, _ = ingest_file(file_path)
    records2, _ = ingest_file(file_path)

    assert len(records1) == len(records2)
    for r1, r2 in zip(records1, records2, strict=False):
        assert r1.rid == r2.rid, f"{fixture_name}: RID not deterministic"
        assert r1.record_digest == r2.record_digest


@pytest.mark.unit
@pytest.mark.parametrize("fixture_name", FIXTURE_EXPECTATIONS.keys())
def test_real_fixture_lossless(fixtures_dir: Path, fixture_name: str) -> None:
    """Test real-world fixture preserves all data."""
    file_path = fixtures_dir / fixture_name
    records, _ = ingest_file(file_path)

    for rec in records:
        # Lossless raw structure
        assert len(rec.raw.record_lines) > 0, f"{fixture_name}: Empty record_lines"
        assert len(rec.raw.tags) > 0, f"{fixture_name}: Empty tags"
        assert isinstance(rec.raw.unknown_lines, list)

        # All tags have content
        for tag in rec.raw.tags:
            assert tag.value_lines is not None
            assert tag.value_raw_joined is not None
            assert tag.line_start >= 0
            assert tag.line_end >= tag.line_start

        # Provenance scaffolding exists
        assert hasattr(rec, "canon")
        assert hasattr(rec, "keys")
        assert hasattr(rec, "flags")
        assert isinstance(rec.provenance, dict)
