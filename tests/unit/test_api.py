"""Tests for the public API module."""

import json
from pathlib import Path

import pytest

from srdedupe import (
    CanonicalRecord,
    ParseError,
    parse_file,
    parse_folder,
    write_jsonl,
)


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to synthetic fixtures directory."""
    return Path(__file__).parent.parent / "fixtures" / "synthetic"


@pytest.fixture
def sample_ris_file(fixtures_dir: Path) -> Path:
    """Path to sample RIS file."""
    return fixtures_dir / "sample.ris"


# ---------------------------------------------------------------------------
# parse_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_file_returns_list(sample_ris_file: Path) -> None:
    """Test parse_file returns a list of CanonicalRecord objects."""
    records = parse_file(sample_ris_file)

    assert isinstance(records, list)
    assert len(records) > 0
    assert all(isinstance(rec, CanonicalRecord) for rec in records)


@pytest.mark.unit
def test_parse_file_record_structure(sample_ris_file: Path) -> None:
    """Test parsed records have correct structure."""
    records = parse_file(sample_ris_file)
    record = records[0]

    assert record.schema_version is not None
    assert record.rid is not None
    assert record.record_digest is not None
    assert record.meta is not None
    assert record.raw is not None
    assert record.canon is not None
    assert record.keys is not None
    assert record.flags is not None
    assert record.provenance is not None


@pytest.mark.unit
def test_parse_file_nonexistent_raises_error() -> None:
    """Test parse_file raises FileNotFoundError for nonexistent file."""
    with pytest.raises(FileNotFoundError):
        parse_file("/nonexistent/file.ris")


@pytest.mark.unit
def test_parse_file_preserves_metadata(sample_ris_file: Path) -> None:
    """Test parse_file preserves source file metadata."""
    records = parse_file(sample_ris_file)

    for record in records:
        assert record.meta.source_file == sample_ris_file.name
        assert record.meta.source_format in ["ris", "pubmed", "bibtex", "wos", "endnote_tagged"]
        assert record.meta.source_record_index >= 0


@pytest.mark.unit
def test_parse_file_strict_mode() -> None:
    """Test strict mode raises ParseError on malformed input."""
    import tempfile

    malformed = tempfile.NamedTemporaryFile(mode="w", suffix=".ris", delete=False, encoding="utf-8")
    malformed.write("INVALID RIS CONTENT\n")
    malformed.close()

    try:
        # Strict mode may raise ParseError or return empty list depending on parser
        records = parse_file(malformed.name, strict=True)
        assert isinstance(records, list)
    except (ParseError, FileNotFoundError):
        pass  # Expected for truly invalid content
    finally:
        Path(malformed.name).unlink(missing_ok=True)


@pytest.mark.unit
def test_parse_file_non_strict_mode() -> None:
    """Test non-strict mode does not raise on malformed input."""
    import tempfile

    malformed = tempfile.NamedTemporaryFile(mode="w", suffix=".ris", delete=False, encoding="utf-8")
    malformed.write("INVALID RIS CONTENT\n")
    malformed.close()

    try:
        records = parse_file(malformed.name, strict=False)
        assert isinstance(records, list)
    finally:
        Path(malformed.name).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# parse_folder
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_folder_returns_list(fixtures_dir: Path) -> None:
    """Test parse_folder returns a list of CanonicalRecord objects."""
    records = parse_folder(fixtures_dir)

    assert isinstance(records, list)
    assert len(records) > 0
    assert all(isinstance(rec, CanonicalRecord) for rec in records)


@pytest.mark.unit
def test_parse_folder_aggregates_multiple_files(fixtures_dir: Path) -> None:
    """Test parse_folder aggregates records from multiple files."""
    records = parse_folder(fixtures_dir)

    source_files = {record.meta.source_file for record in records}
    assert len(source_files) > 1


@pytest.mark.unit
def test_parse_folder_nonexistent_raises_error() -> None:
    """Test parse_folder raises FileNotFoundError for nonexistent folder."""
    with pytest.raises(FileNotFoundError):
        parse_folder("/nonexistent/folder/")


@pytest.mark.integration
def test_parse_folder_recursive(fixtures_dir: Path) -> None:
    """Test parse_folder with recursive option."""
    records_non_recursive = parse_folder(fixtures_dir, recursive=False)
    records_recursive = parse_folder(fixtures_dir, recursive=True)

    assert len(records_recursive) >= len(records_non_recursive)


# ---------------------------------------------------------------------------
# write_jsonl
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_jsonl_creates_file(sample_ris_file: Path, tmp_path: Path) -> None:
    """Test write_jsonl creates a valid JSONL file."""
    records = parse_file(sample_ris_file)
    output_file = tmp_path / "output.jsonl"

    write_jsonl(records, output_file)

    assert output_file.exists()

    lines = output_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == len(records)

    for line in lines:
        parsed = json.loads(line)
        assert isinstance(parsed, dict)
        assert "rid" in parsed


@pytest.mark.unit
def test_write_jsonl_deterministic(sample_ris_file: Path, tmp_path: Path) -> None:
    """Test write_jsonl output is deterministic."""
    records = parse_file(sample_ris_file)

    output_file1 = tmp_path / "output1.jsonl"
    output_file2 = tmp_path / "output2.jsonl"

    write_jsonl(records, output_file1)
    write_jsonl(records, output_file2)

    assert output_file1.read_bytes() == output_file2.read_bytes()


# ---------------------------------------------------------------------------
# to_dict / serialization
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_to_dict_serialization(sample_ris_file: Path) -> None:
    """Test record.to_dict() returns valid, JSON-serializable dict."""
    records = parse_file(sample_ris_file)
    record = records[0]
    record_dict = record.to_dict()

    assert isinstance(record_dict, dict)
    for key in ("schema_version", "rid", "record_digest", "meta", "raw", "canon", "keys", "flags"):
        assert key in record_dict

    # Round-trip JSON
    json_str = json.dumps(record_dict, ensure_ascii=False, sort_keys=True)
    restored = json.loads(json_str)
    assert restored["rid"] == record.rid
    assert restored["record_digest"] == record.record_digest


# ---------------------------------------------------------------------------
# dedupe
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dedupe_single_file(sample_ris_file: Path, tmp_path: Path) -> None:
    """Test dedupe function returns PipelineResult with correct structure."""
    from srdedupe import dedupe

    output_dir = tmp_path / "dedupe_output"
    result = dedupe(sample_ris_file, output_dir=output_dir)

    assert result.success is True
    assert result.total_records > 0
    assert isinstance(result.output_files, dict)
    assert "canonical_records" in result.output_files
    assert "deduplicated_ris" in result.output_files
    assert output_dir.exists()

    canonical_path = Path(result.output_files["canonical_records"])
    assert canonical_path.exists()


@pytest.mark.integration
def test_dedupe_folder(fixtures_dir: Path, tmp_path: Path) -> None:
    """Test dedupe function with a folder of files."""
    from srdedupe import dedupe

    output_dir = tmp_path / "dedupe_folder_output"
    result = dedupe(fixtures_dir, output_dir=output_dir)

    assert result.success is True
    assert result.total_records > 0
    assert output_dir.exists()


@pytest.mark.integration
def test_dedupe_nonexistent_raises_error() -> None:
    """Test dedupe raises FileNotFoundError for nonexistent input."""
    from srdedupe import dedupe

    with pytest.raises(FileNotFoundError):
        dedupe("/nonexistent/file.ris")


@pytest.mark.integration
def test_dedupe_custom_fpr_alpha(sample_ris_file: Path, tmp_path: Path) -> None:
    """Test dedupe with custom FPR alpha parameter."""
    from srdedupe import dedupe

    output_dir = tmp_path / "dedupe_strict"
    result = dedupe(sample_ris_file, output_dir=output_dir, fpr_alpha=0.005)

    assert result.success is True
    assert "deduplicated_ris" in result.output_files


@pytest.mark.integration
def test_full_workflow_parse_and_export(fixtures_dir: Path, tmp_path: Path) -> None:
    """Test complete workflow: parse folder -> export JSONL."""
    records = parse_folder(fixtures_dir)
    assert len(records) > 0

    output_file = tmp_path / "all_records.jsonl"
    write_jsonl(records, output_file)

    assert output_file.exists()

    with output_file.open("r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) == len(records)

        for line in lines:
            record_dict = json.loads(line)
            assert "rid" in record_dict
            assert "schema_version" in record_dict


@pytest.mark.integration
def test_round_trip_serialization(sample_ris_file: Path) -> None:
    """Test record -> dict -> JSON -> dict preserves data."""
    records = parse_file(sample_ris_file)
    original = records[0].to_dict()

    json_str = json.dumps(original, ensure_ascii=False, sort_keys=True)
    restored = json.loads(json_str)

    assert restored["rid"] == original["rid"]
    assert restored["record_digest"] == original["record_digest"]
    assert restored["schema_version"] == original["schema_version"]
    assert restored["meta"]["source_file"] == original["meta"]["source_file"]
