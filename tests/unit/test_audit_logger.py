"""Tests for audit logger module."""

import json
from pathlib import Path

import pytest

from srdedupe.audit.logger import AuditLogger


@pytest.fixture
def logger(tmp_path: Path) -> AuditLogger:
    """Create a logger that auto-closes after test."""
    lg = AuditLogger(run_id="test_run", log_path=tmp_path / "events.jsonl")
    yield lg
    lg.close()


def _read_events(path: Path) -> list[dict]:
    """Read all JSONL events from file."""
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


@pytest.mark.unit
def test_logger_init_creates_file(logger: AuditLogger) -> None:
    """Test logger creates the log file and sets initial state."""
    assert logger.log_path.exists()
    assert logger.current_stage is None
    assert logger.run_id == "test_run"


@pytest.mark.unit
def test_logger_event_writes_valid_jsonl(logger: AuditLogger) -> None:
    """Test event() writes a valid JSONL line with correct envelope."""
    logger.event("test_event", data={"key": "value"}, level="INFO", rid="r1")

    events = _read_events(logger.log_path)

    assert len(events) == 1
    evt = events[0]
    assert evt["run_id"] == "test_run"
    assert evt["event"] == "test_event"
    assert evt["level"] == "INFO"
    assert evt["data"] == {"key": "value"}
    assert evt["rid"] == "r1"
    assert evt["ts"].endswith("Z")


@pytest.mark.unit
def test_logger_stage_context_inheritance(logger: AuditLogger) -> None:
    """Test stage set via set_stage propagates to events."""
    logger.set_stage("stage1")
    logger.event("ev1")
    logger.event("ev2", stage="override")
    logger.set_stage(None)
    logger.event("ev3")

    events = _read_events(logger.log_path)

    assert events[0]["stage"] == "stage1"
    assert events[1]["stage"] == "override"
    assert events[2]["stage"] is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("method", "kwargs", "expected_event", "expected_level"),
    [
        ("run_started", {"command": ["srdedupe"], "parameters": {"k": 1}}, "run_started", "INFO"),
        ("run_finished", {"status": "success", "duration_seconds": 1.5}, "run_finished", "INFO"),
        ("stage_started", {"stage": "s1", "expected_records": 10}, "stage_started", "INFO"),
        (
            "stage_finished",
            {"stage": "s1", "duration_seconds": 2.0, "counters": {"n": 5}},
            "stage_finished",
            "INFO",
        ),
        (
            "record_flagged",
            {"rid": "r1", "flag_name": "f", "reason_code": "rc"},
            "record_flagged",
            "INFO",
        ),
        (
            "artifact_written",
            {"path": "a.jsonl", "sha256": "sha256:abc"},
            "artifact_written",
            "INFO",
        ),
        ("error", {"exception_class": "ValueError", "message": "bad"}, "error", "ERROR"),
    ],
)
def test_logger_convenience_methods(
    logger: AuditLogger,
    method: str,
    kwargs: dict,
    expected_event: str,
    expected_level: str,
) -> None:
    """Test all convenience methods produce correct event type and level."""
    getattr(logger, method)(**kwargs)

    events = _read_events(logger.log_path)

    assert len(events) == 1
    assert events[0]["event"] == expected_event
    assert events[0]["level"] == expected_level


@pytest.mark.unit
def test_logger_multiple_events_appended(logger: AuditLogger) -> None:
    """Test multiple events are appended as separate lines."""
    for i in range(3):
        logger.event(f"ev_{i}")

    events = _read_events(logger.log_path)
    assert [e["event"] for e in events] == ["ev_0", "ev_1", "ev_2"]


@pytest.mark.unit
def test_logger_close_and_context_manager(tmp_path: Path) -> None:
    """Test close() flushes and context manager auto-closes."""
    log_path = tmp_path / "events.jsonl"

    with AuditLogger(run_id="r1", log_path=log_path) as lg:
        lg.event("inside")

    # After __exit__, file should be closed and content readable
    events = _read_events(log_path)
    assert len(events) == 1

    # Second logger can append to same file
    with AuditLogger(run_id="r2", log_path=log_path) as lg2:
        lg2.event("second")

    events = _read_events(log_path)
    assert len(events) == 2
    assert events[0]["run_id"] == "r1"
    assert events[1]["run_id"] == "r2"


@pytest.mark.unit
def test_logger_creates_parent_directories(tmp_path: Path) -> None:
    """Test logger creates nested parent directories."""
    nested = tmp_path / "a" / "b" / "events.jsonl"
    lg = AuditLogger(run_id="test", log_path=nested)
    lg.event("test")
    lg.close()

    assert nested.exists()
    assert len(_read_events(nested)) == 1
