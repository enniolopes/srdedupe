"""Tests for manifest writer module."""

import json
from pathlib import Path

import pytest

from srdedupe.audit.manifest import ManifestWriter
from srdedupe.audit.models import (
    CommandInfo,
    EnvironmentInfo,
    ErrorInfo,
    FileInfo,
    InputsInfo,
    StageInfo,
)

_COMMAND = CommandInfo(argv=["srdedupe", "run"], cwd="workdir")
_ENVIRONMENT = EnvironmentInfo(
    python_version="3.12.3",
    platform="Linux-6.8.0-x86_64",
    package_version="0.18.0",
    dependencies={"jsonschema": "4.26.0"},
)


@pytest.fixture
def writer(tmp_path: Path) -> ManifestWriter:
    """Create a ManifestWriter with minimal config."""
    return ManifestWriter(
        run_id="test_run_123",
        output_dir=tmp_path,
        command=_COMMAND,
        environment=_ENVIRONMENT,
        transform_version="git:abc123",
        parameters={"stage1": {"normalization": "v1"}},
    )


@pytest.mark.unit
def test_manifest_init_state(writer: ManifestWriter, tmp_path: Path) -> None:
    """Test manifest starts with correct initial state."""
    assert writer.manifest.run_id == "test_run_123"
    assert writer.manifest.status == "partial"
    assert writer.manifest.transform_version == "git:abc123"
    assert writer.manifest_path == tmp_path / "run.json"
    assert writer.manifest.stages == []
    assert writer.manifest.errors == []


@pytest.mark.unit
def test_manifest_stage_lifecycle(writer: ManifestWriter) -> None:
    """Test add → update counters → finish for a stage."""
    stage = StageInfo(name="s1", started_at="2026-02-03T12:00:00Z", counters={"in": 100})
    writer.add_stage(stage)

    writer.update_stage_counters("s1", {"out": 95, "flagged": 5})
    writer.finish_stage("s1", finished_at="2026-02-03T12:05:00Z", duration_seconds=300.0)

    result = writer.manifest.stages[0]
    assert result.counters == {"in": 100, "out": 95, "flagged": 5}
    assert result.finished_at == "2026-02-03T12:05:00Z"
    assert result.duration_seconds == 300.0


@pytest.mark.unit
def test_manifest_stage_not_found_raises(writer: ManifestWriter) -> None:
    """Test operations on nonexistent stage raise ValueError."""
    with pytest.raises(ValueError, match="Stage not found"):
        writer.update_stage_counters("ghost", {"n": 1})

    with pytest.raises(ValueError, match="Stage not found"):
        writer.finish_stage("ghost")


@pytest.mark.unit
def test_manifest_compute_output_artifacts(writer: ManifestWriter, tmp_path: Path) -> None:
    """Test compute_output_artifacts hashes events.jsonl."""
    events_path = tmp_path / "events.jsonl"
    events_path.write_text('{"event":"test"}\n')

    writer.compute_output_artifacts()

    artifacts = writer.manifest.outputs.artifacts
    assert len(artifacts) == 1
    assert artifacts[0].path == "events.jsonl"
    assert artifacts[0].sha256.startswith("sha256:")
    assert artifacts[0].bytes == events_path.stat().st_size


@pytest.mark.unit
def test_manifest_compute_artifacts_skips_missing(writer: ManifestWriter) -> None:
    """Test compute_output_artifacts does nothing when events.jsonl absent."""
    writer.compute_output_artifacts()

    assert writer.manifest.outputs.artifacts == []


@pytest.mark.unit
def test_manifest_atomic_write(writer: ManifestWriter, tmp_path: Path) -> None:
    """Test finish writes run.json atomically (no temp file left)."""
    writer.finish(status="success", finished_at="2026-02-03T12:10:00Z", duration_seconds=600.0)

    assert writer.manifest_path.exists()
    assert not (tmp_path / "run.tmp").exists()

    with writer.manifest_path.open() as f:
        data = json.load(f)

    assert data["status"] == "success"
    assert data["run_id"] == "test_run_123"
    assert data["duration_seconds"] == 600.0


@pytest.mark.unit
def test_manifest_complete_workflow(writer: ManifestWriter, tmp_path: Path) -> None:
    """Test full workflow: inputs → stage → error → finish → valid JSON."""
    writer.set_inputs(
        InputsInfo(
            root="data",
            files=[
                FileInfo(
                    name="test.ris",
                    format="ris",
                    bytes=1024,
                    sha256="sha256:" + "a" * 64,
                    records_extracted=100,
                )
            ],
            total_records_extracted=100,
        )
    )

    writer.add_stage(StageInfo(name="s1", started_at="2026-02-03T12:00:00Z"))
    writer.update_stage_counters("s1", {"records_out": 100})
    writer.finish_stage("s1", finished_at="2026-02-03T12:05:00Z", duration_seconds=300.0)

    writer.add_error(
        ErrorInfo(
            timestamp="2026-02-03T12:04:00Z",
            exception_class="ValueError",
            message="bad record",
            stage="s1",
        )
    )

    writer.finish(status="success", finished_at="2026-02-03T12:10:00Z", duration_seconds=600.0)

    with writer.manifest_path.open() as f:
        data = json.load(f)

    assert data["status"] == "success"
    assert data["inputs"]["total_records_extracted"] == 100
    assert data["stages"][0]["counters"]["records_out"] == 100
    assert len(data["errors"]) == 1


@pytest.mark.unit
def test_manifest_to_dict(writer: ManifestWriter) -> None:
    """Test to_dict returns a serializable dictionary."""
    d = writer.to_dict()

    assert isinstance(d, dict)
    assert d["run_id"] == "test_run_123"
    # Roundtrip through JSON should not raise
    json.loads(json.dumps(d))
