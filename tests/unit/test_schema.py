"""Unit tests for canonical record schema validation and identifiers.

Tests validate:
- Schema shape and required fields
- Determinism constraints for RID/fingerprints
- Required provenance structure
- Dataclass construction
"""

import json
from pathlib import Path

import pytest

from srdedupe.models import (
    SCHEMA_VERSION,
    Canon,
    CanonicalRecord,
    Flags,
    Keys,
    Meta,
    Raw,
    RawTag,
    calculate_record_digest,
    calculate_rid,
    calculate_source_digest,
    validate_digest_format,
    validate_rid_format,
)


@pytest.fixture
def sample_raw_tags() -> list[dict[str, str | int]]:
    """Provide sample raw tags for testing."""
    return [
        {"tag": "TI", "value": "Test Title", "occurrence": 0, "line": 1},
        {"tag": "AU", "value": "Smith, J.", "occurrence": 0, "line": 2},
        {"tag": "PY", "value": "2024", "occurrence": 0, "line": 3},
    ]


@pytest.fixture
def sample_source_bytes() -> bytes:
    """Provide sample source file bytes."""
    return b"TY  - JOUR\nTI  - Test Title\nAU  - Smith, J.\nPY  - 2024\nER  -\n"


@pytest.mark.unit
def test_schema_version_constant() -> None:
    """Test schema version constant is defined and valid."""
    assert SCHEMA_VERSION == "1.0.0"
    assert isinstance(SCHEMA_VERSION, str)

    parts = SCHEMA_VERSION.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


@pytest.mark.unit
def test_calculate_record_digest_deterministic(sample_raw_tags: list[dict]) -> None:
    """Test record_digest is deterministic for same input."""
    digest1 = calculate_record_digest(sample_raw_tags, "ris")
    digest2 = calculate_record_digest(sample_raw_tags, "ris")

    assert digest1 == digest2
    assert digest1.startswith("sha256:")
    assert len(digest1) == 71


@pytest.mark.unit
def test_calculate_record_digest_different_content() -> None:
    """Test record_digest changes with different content."""
    tags1 = [{"tag": "TI", "value": "Title A", "occurrence": 1, "line": 1}]
    tags2 = [{"tag": "TI", "value": "Title B", "occurrence": 1, "line": 1}]

    digest1 = calculate_record_digest(tags1, "ris")
    digest2 = calculate_record_digest(tags2, "ris")

    assert digest1 != digest2


@pytest.mark.unit
def test_calculate_record_digest_ignores_line_numbers(sample_raw_tags: list[dict]) -> None:
    """Test record_digest ignores line numbers (platform independence)."""
    tags_modified = [
        {"tag": t["tag"], "value": t["value"], "occurrence": t["occurrence"], "line": 999}
        for t in sample_raw_tags
    ]

    digest1 = calculate_record_digest(sample_raw_tags, "ris")
    digest2 = calculate_record_digest(tags_modified, "ris")

    assert digest1 == digest2


@pytest.mark.unit
def test_calculate_record_digest_includes_source_format(sample_raw_tags: list[dict]) -> None:
    """Test record_digest includes source_format to prevent collisions."""
    digest_ris = calculate_record_digest(sample_raw_tags, "ris")
    digest_nbib = calculate_record_digest(sample_raw_tags, "nbib")

    assert digest_ris != digest_nbib


@pytest.mark.unit
def test_calculate_source_digest(sample_source_bytes: bytes) -> None:
    """Test source_digest calculation."""
    digest = calculate_source_digest(sample_source_bytes)

    assert digest.startswith("sha256:")
    assert len(digest) == 71
    assert validate_digest_format(digest)


@pytest.mark.unit
def test_calculate_source_digest_deterministic(sample_source_bytes: bytes) -> None:
    """Test source_digest is deterministic."""
    digest1 = calculate_source_digest(sample_source_bytes)
    digest2 = calculate_source_digest(sample_source_bytes)

    assert digest1 == digest2


@pytest.mark.unit
def test_calculate_rid_deterministic() -> None:
    """Test RID is deterministic for same inputs."""
    source_digest = "sha256:abc123" + "0" * 58
    record_digest = "sha256:def456" + "0" * 58

    rid1 = calculate_rid(source_digest, record_digest)
    rid2 = calculate_rid(source_digest, record_digest)

    assert rid1 == rid2
    assert validate_rid_format(rid1)


@pytest.mark.unit
def test_calculate_rid_different_source_digest() -> None:
    """Test RID changes with different source_digest."""
    record_digest = "sha256:def456" + "0" * 58
    source_digest1 = "sha256:abc111" + "0" * 58
    source_digest2 = "sha256:abc222" + "0" * 58

    rid1 = calculate_rid(source_digest1, record_digest)
    rid2 = calculate_rid(source_digest2, record_digest)

    assert rid1 != rid2


@pytest.mark.unit
def test_calculate_rid_different_record_digest() -> None:
    """Test RID changes with different record_digest."""
    source_digest = "sha256:abc123" + "0" * 58
    record_digest1 = "sha256:def111" + "0" * 58
    record_digest2 = "sha256:def222" + "0" * 58

    rid1 = calculate_rid(source_digest, record_digest1)
    rid2 = calculate_rid(source_digest, record_digest2)

    assert rid1 != rid2


@pytest.mark.unit
def test_validate_digest_format_valid() -> None:
    """Test digest format validation for valid digests."""
    valid_digest = "sha256:" + "a" * 64
    assert validate_digest_format(valid_digest) is True


@pytest.mark.unit
def test_validate_digest_format_invalid() -> None:
    """Test digest format validation for invalid digests."""
    assert validate_digest_format("invalid") is False
    assert validate_digest_format("sha256:short") is False
    assert validate_digest_format("md5:" + "a" * 32) is False
    assert validate_digest_format("sha256:" + "g" * 64) is False


@pytest.mark.unit
def test_validate_rid_format_valid() -> None:
    """Test RID format validation for valid UUIDv5."""
    valid_rid = "550e8400-e29b-51d4-a716-446655440000"
    assert validate_rid_format(valid_rid) is True


@pytest.mark.unit
def test_validate_rid_format_invalid() -> None:
    """Test RID format validation for invalid UUIDs."""
    assert validate_rid_format("invalid") is False
    assert validate_rid_format("550e8400-e29b-41d4-a716-446655440000") is False


@pytest.mark.unit
def test_raw_tag_structure() -> None:
    """Test RawTag dataclass structure."""
    tag = RawTag(
        tag="TI",
        value_lines=["Test"],
        value_raw_joined="Test",
        occurrence=0,
        line_start=10,
        line_end=10,
    )

    assert tag.tag == "TI"
    assert tag.value_lines == ["Test"]
    assert tag.value_raw_joined == "Test"
    assert tag.occurrence == 0
    assert tag.line_start == 10
    assert tag.line_end == 10


@pytest.mark.unit
def test_meta_required_fields() -> None:
    """Test Meta dataclass has all required fields."""
    meta = Meta(
        source_file="test.ris",
        source_format="ris",
        source_db="pubmed",
        source_record_index=0,
        ingested_at="2024-01-30T12:00:00Z",
    )

    assert meta.source_file == "test.ris"
    assert meta.source_format == "ris"
    assert meta.source_db == "pubmed"
    assert meta.source_record_index == 0
    assert meta.ingested_at == "2024-01-30T12:00:00Z"


@pytest.mark.unit
def test_meta_optional_fields() -> None:
    """Test Meta dataclass optional fields default to None."""
    meta = Meta(
        source_file="test.ris",
        source_format="ris",
        source_db=None,
        source_record_index=0,
        ingested_at="2024-01-30T12:00:00Z",
    )

    assert meta.source_file_mtime is None
    assert meta.source_file_size_bytes is None
    assert meta.parser_version is None


@pytest.mark.unit
def test_canon_all_fields_nullable() -> None:
    """Test Canon dataclass allows all fields to be None."""
    canon = Canon.empty()

    assert all(getattr(canon, field) is None for field in canon.__dataclass_fields__)


@pytest.mark.unit
def test_keys_flat_structure() -> None:
    """Test Keys dataclass has flat structure."""
    keys = Keys(
        title_key_strict="test",
        title_year_key="test|2024",
        title_first_author_key="test|smith",
        title_journal_key="test|nature",
        title_key_fuzzy="testfuzzy",
        title_shingles=["test", "shingle"],
        title_minhash=None,
        title_simhash=None,
    )

    assert keys.title_key_strict == "test"
    assert keys.title_year_key == "test|2024"
    assert keys.title_key_fuzzy == "testfuzzy"
    assert keys.title_shingles == ["test", "shingle"]


@pytest.mark.unit
def test_keys_empty() -> None:
    """Test Keys.empty() returns all None."""
    keys = Keys.empty()

    assert all(getattr(keys, field) is None for field in keys.__dataclass_fields__)


@pytest.mark.unit
def test_flags_all_boolean() -> None:
    """Test Flags dataclass requires all boolean values."""
    flags = Flags(
        doi_present=False,
        pmid_present=False,
        title_missing=True,
        title_truncated=False,
        authors_missing=False,
        authors_incomplete=False,
        year_missing=False,
        pages_unreliable=False,
        is_erratum_notice=False,
        is_retraction_notice=False,
        is_corrected_republished=False,
        has_linked_citation=False,
    )

    assert isinstance(flags.doi_present, bool)
    assert isinstance(flags.title_missing, bool)
    assert flags.title_missing is True


@pytest.mark.unit
def test_provenance_is_plain_dict() -> None:
    """Test provenance is a plain dict (no dataclass wrappers)."""
    provenance: dict = {
        "canon.doi_norm": {
            "sources": [{"path": "raw.tags[0]", "tag": "DO", "value_snippet": "10.1234/test"}],
            "transforms": [{"name": "lowercase", "version": "1"}],
            "confidence": "high",
        }
    }

    assert isinstance(provenance, dict)
    assert "canon.doi_norm" in provenance
    assert provenance["canon.doi_norm"]["confidence"] == "high"


@pytest.mark.unit
def test_canonical_record_structure() -> None:
    """Test CanonicalRecord has all required top-level fields."""
    raw = Raw(record_lines=[], tags=[], unknown_lines=[])
    meta = Meta(
        source_file="test.ris",
        source_format="ris",
        source_db=None,
        source_record_index=0,
        ingested_at="2024-01-30T12:00:00Z",
    )
    canon = Canon.empty()
    keys = Keys.empty()
    flags = Flags.pre_normalization()

    record = CanonicalRecord(
        schema_version="1.0.0",
        rid="550e8400-e29b-51d4-a716-446655440000",
        record_digest="sha256:" + "a" * 64,
        source_digest="sha256:" + "b" * 64,
        meta=meta,
        raw=raw,
        canon=canon,
        keys=keys,
        flags=flags,
        provenance={},
    )

    assert record.schema_version == "1.0.0"
    assert record.rid == "550e8400-e29b-51d4-a716-446655440000"
    assert record.record_digest.startswith("sha256:")
    assert record.meta is not None
    assert record.raw is not None
    assert record.canon is not None
    assert record.keys is not None
    assert record.flags is not None
    assert record.provenance is not None


@pytest.mark.unit
def test_canonical_record_to_dict() -> None:
    """Test CanonicalRecord.to_dict() produces JSON-serializable output."""
    raw = Raw(record_lines=["TY  - JOUR"], tags=[], unknown_lines=[])
    meta = Meta(
        source_file="test.ris",
        source_format="ris",
        source_db=None,
        source_record_index=0,
        ingested_at="2024-01-30T12:00:00Z",
    )
    canon = Canon.empty()
    keys = Keys.empty()
    flags = Flags.pre_normalization()

    record = CanonicalRecord(
        schema_version="1.0.0",
        rid="550e8400-e29b-51d4-a716-446655440000",
        record_digest="sha256:" + "a" * 64,
        source_digest=None,
        meta=meta,
        raw=raw,
        canon=canon,
        keys=keys,
        flags=flags,
        provenance={},
    )

    data = record.to_dict()

    assert isinstance(data, dict)
    assert data["schema_version"] == "1.0.0"
    assert data["rid"] == "550e8400-e29b-51d4-a716-446655440000"

    json_str = json.dumps(data)
    assert len(json_str) > 0


@pytest.mark.unit
def test_golden_example_loads() -> None:
    """Test that the golden example JSONL loads correctly."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "golden_example.jsonl"
    assert fixture_path.exists(), f"Golden example not found at {fixture_path}"

    with open(fixture_path) as f:
        data = json.load(f)

    assert data["schema_version"] == "1.0.0"
    assert "rid" in data
    assert "record_digest" in data
    assert "meta" in data
    assert "raw" in data
    assert "canon" in data
    assert "keys" in data
    assert "flags" in data
    assert "provenance" in data

    # Validate meta structure
    assert data["meta"]["source_format"] in ["ris", "nbib", "unknown"]
    assert isinstance(data["meta"]["source_record_index"], int)

    # Validate raw structure
    assert "tags" in data["raw"]
    assert isinstance(data["raw"]["tags"], list)

    # Validate keys are flat
    assert "title_key_strict" in data["keys"]
    assert "title_shingles" in data["keys"]

    # Validate flags are boolean
    for flag_name, flag_value in data["flags"].items():
        assert isinstance(flag_value, bool), f"Flag {flag_name} must be boolean"

    # Validate provenance is plain dict
    for _field_name, prov_entry in data["provenance"].items():
        assert "sources" in prov_entry
        assert "transforms" in prov_entry
        assert "confidence" in prov_entry
