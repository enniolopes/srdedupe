"""Tests for schema validation of manifests and events."""

import json
from pathlib import Path

import jsonschema
import pytest

_SCHEMAS_DIR = Path(__file__).parent.parent.parent / "schemas"
_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "audit"


@pytest.fixture(scope="module")
def manifest_schema() -> dict:
    """Load run manifest JSON schema."""
    with (_SCHEMAS_DIR / "run_manifest.schema.json").open() as f:
        return json.load(f)


@pytest.fixture(scope="module")
def event_schema() -> dict:
    """Load log event JSON schema."""
    with (_SCHEMAS_DIR / "log_event.schema.json").open() as f:
        return json.load(f)


@pytest.mark.unit
def test_example_fixtures_validate(manifest_schema: dict, event_schema: dict) -> None:
    """Test example manifest and events fixtures pass schema validation."""
    with (_FIXTURES_DIR / "example_run.json").open() as f:
        manifest = json.load(f)
    jsonschema.validate(instance=manifest, schema=manifest_schema)

    with (_FIXTURES_DIR / "example_events.jsonl").open() as f:
        for line in f:
            jsonschema.validate(instance=json.loads(line), schema=event_schema)


@pytest.mark.unit
def test_generated_manifest_validates(tmp_path: Path, manifest_schema: dict) -> None:
    """Test programmatically generated manifest validates against schema."""
    from srdedupe.audit import RunContext

    output_dir = tmp_path / "output"
    run = RunContext.start(output_dir=output_dir, parameters={"stage1": {"normalization": "v1"}})
    run.start_stage("stage1_normalize")
    run.finish_stage("stage1_normalize", counters={"records_in": 10})
    run.finish(status="success")

    with (output_dir / "run.json").open() as f:
        jsonschema.validate(instance=json.load(f), schema=manifest_schema)


@pytest.mark.unit
def test_generated_events_validate(tmp_path: Path, event_schema: dict) -> None:
    """Test programmatically generated events validate against schema."""
    from srdedupe.audit import RunContext

    output_dir = tmp_path / "output"
    run = RunContext.start(output_dir=output_dir, parameters={})
    run.start_stage("stage1_normalize")
    run.audit_logger.record_flagged(rid="test-rid", flag_name="f", reason_code="rc")
    run.finish_stage("stage1_normalize", counters={})
    run.finish(status="success")

    with (output_dir / "events.jsonl").open() as f:
        for line in f:
            if line.strip():
                jsonschema.validate(instance=json.loads(line), schema=event_schema)


@pytest.mark.unit
def test_invalid_data_rejected_by_schema(
    manifest_schema: dict,
    event_schema: dict,
) -> None:
    """Test schemas reject invalid status, level, and missing fields."""
    # Invalid manifest status
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={
                "manifest_version": "1.0.0",
                "run_id": "x",
                "created_at": "2026-01-01T00:00:00Z",
                "status": "bogus",
                "transform_version": "v1",
                "command": {"argv": ["x"]},
                "environment": {
                    "python_version": "3.12",
                    "platform": "Linux",
                    "package_version": "0.1.0",
                },
                "inputs": {"root": "", "files": [], "total_records_extracted": 0},
                "parameters": {},
                "stages": [],
                "outputs": {"artifacts": []},
            },
            schema=manifest_schema,
        )

    # Missing required event fields
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"ts": "x", "run_id": "x"}, schema=event_schema)
