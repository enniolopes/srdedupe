"""Tests for run context module."""

import json
from pathlib import Path

import pytest

from srdedupe.audit.context import RunContext


def _read_manifest(output_dir: Path) -> dict:
    """Read and parse run.json."""
    with (output_dir / "run.json").open() as f:
        return json.load(f)


def _read_events(output_dir: Path) -> list[dict]:
    """Read and parse events.jsonl."""
    with (output_dir / "events.jsonl").open() as f:
        return [json.loads(line) for line in f if line.strip()]


@pytest.mark.unit
def test_context_start_creates_structure(tmp_path: Path) -> None:
    """Test start() creates dirs, events file, and sets run_id."""
    output_dir = tmp_path / "output"
    run = RunContext.start(output_dir=output_dir, parameters={"k": 1})

    assert run.run_id is not None
    assert "__" in run.run_id
    assert output_dir.exists()
    assert (output_dir / "artifacts").is_dir()
    assert (output_dir / "reports").is_dir()
    assert (output_dir / "events.jsonl").exists()

    run.finish(status="success")


@pytest.mark.unit
def test_context_stage_lifecycle(tmp_path: Path) -> None:
    """Test start_stage → finish_stage records timing and counters."""
    output_dir = tmp_path / "output"
    run = RunContext.start(output_dir=output_dir, parameters={})

    run.start_stage("s1", expected_records=50)
    run.finish_stage("s1", counters={"records_in": 50, "records_out": 48})

    stage = run.manifest_writer.manifest.stages[0]
    assert stage.name == "s1"
    assert stage.finished_at is not None
    assert stage.duration_seconds is not None
    assert stage.duration_seconds >= 0
    assert stage.counters["records_out"] == 48

    run.finish(status="success")


@pytest.mark.unit
def test_context_finish_stage_not_started_raises(tmp_path: Path) -> None:
    """Test finishing a stage that was never started raises ValueError."""
    output_dir = tmp_path / "output"
    run = RunContext.start(output_dir=output_dir, parameters={})

    with pytest.raises(ValueError, match="Stage not started"):
        run.finish_stage("ghost")

    run.finish(status="failed")


@pytest.mark.unit
def test_context_error_recording(tmp_path: Path) -> None:
    """Test record_error adds error to manifest and events."""
    output_dir = tmp_path / "output"
    run = RunContext.start(output_dir=output_dir, parameters={})

    exc = ValueError("bad input")
    run.record_error(exc, stage="s1", include_traceback=True)

    error = run.manifest_writer.manifest.errors[0]
    assert error.exception_class == "ValueError"
    assert error.message == "bad input"
    assert error.stage == "s1"
    assert error.traceback is not None

    run.finish(status="failed")


@pytest.mark.unit
def test_context_manager_success(tmp_path: Path) -> None:
    """Test context manager writes success manifest on clean exit."""
    output_dir = tmp_path / "output"

    with RunContext.start(output_dir=output_dir, parameters={}) as run:
        run.start_stage("s1")
        run.finish_stage("s1", counters={"n": 10})

    data = _read_manifest(output_dir)
    assert data["status"] == "success"
    assert data["duration_seconds"] > 0


@pytest.mark.unit
def test_context_manager_exception(tmp_path: Path) -> None:
    """Test context manager writes failed manifest on exception."""
    output_dir = tmp_path / "output"

    with pytest.raises(RuntimeError):
        with RunContext.start(output_dir=output_dir, parameters={}):
            raise RuntimeError("boom")

    data = _read_manifest(output_dir)
    assert data["status"] == "failed"
    assert len(data["errors"]) == 1
    assert data["errors"][0]["exception_class"] == "RuntimeError"


@pytest.mark.unit
def test_context_full_workflow_produces_valid_outputs(tmp_path: Path) -> None:
    """Test full workflow: events.jsonl has all lifecycle events, run.json is complete."""
    output_dir = tmp_path / "output"

    run = RunContext.start(
        output_dir=output_dir,
        parameters={"stage1": {"normalization": "v1"}},
        command_argv=["srdedupe", "run"],
    )
    run.start_stage("stage1_normalize", expected_records=100)
    run.finish_stage("stage1_normalize", counters={"records_in": 100, "doi_present": 60})
    run.finish(status="success", records_processed=100)

    # Validate events.jsonl
    events = _read_events(output_dir)
    event_types = [e["event"] for e in events]
    assert event_types == ["run_started", "stage_started", "stage_finished", "run_finished"]
    assert all(e["run_id"] == run.run_id for e in events)

    # Validate run.json
    data = _read_manifest(output_dir)
    assert data["status"] == "success"
    assert data["manifest_version"] == "1.0.0"
    assert data["parameters"]["stage1"]["normalization"] == "v1"
    assert data["command"]["argv"] == ["srdedupe", "run"]
    assert data["environment"]["python_version"] is not None
    assert len(data["stages"]) == 1
    assert data["stages"][0]["counters"]["doi_present"] == 60
    assert data["duration_seconds"] > 0

    # events.jsonl is hashed in outputs
    artifact_paths = [a["path"] for a in data["outputs"]["artifacts"]]
    assert "events.jsonl" in artifact_paths
