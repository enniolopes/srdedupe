"""Structured audit logger for JSONL event logging.

Provides append-only structured event logging to JSONL files with
a persistent file handle for efficient I/O.
"""

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from srdedupe.audit.models import LogEvent
from srdedupe.utils import get_iso_timestamp

__all__ = ["AuditLogger"]


class AuditLogger:
    """JSONL audit logger with persistent file handle.

    Writes structured log events to a JSONL file (one JSON object per line).
    Events are append-only and flushed after each write for durability.

    Attributes
    ----------
    run_id : str
        Unique run identifier.
    log_path : Path
        Path to JSONL log file.
    current_stage : str | None
        Current stage name for context.
    """

    def __init__(self, run_id: str, log_path: Path) -> None:
        """Initialize audit logger and open file handle.

        Parameters
        ----------
        run_id : str
            Unique run identifier.
        log_path : Path
            Path to JSONL log file.
        """
        self.run_id = run_id
        self.log_path = log_path
        self.current_stage: str | None = None

        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.log_path.open("a", encoding="utf-8")

    def __enter__(self) -> "AuditLogger":
        """Enter context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager and close file."""
        self.close()

    def close(self) -> None:
        """Flush and close the log file handle."""
        if not self._file.closed:
            self._file.flush()
            self._file.close()

    def set_stage(self, stage: str | None) -> None:
        """Set current stage context.

        Parameters
        ----------
        stage : str | None
            Stage name or None to clear.
        """
        self.current_stage = stage

    def event(
        self,
        event_type: str,
        data: dict[str, Any] | None = None,
        level: str = "INFO",
        stage: str | None = None,
        rid: str | None = None,
    ) -> None:
        """Write structured event to log.

        Parameters
        ----------
        event_type : str
            Event type identifier (e.g., "stage_started").
        data : dict[str, Any] | None, optional
            Event-specific data payload.
        level : str, optional
            Log level ("DEBUG", "INFO", "WARN", "ERROR").
        stage : str | None, optional
            Stage identifier, uses current_stage if not provided.
        rid : str | None, optional
            Record identifier if event is record-specific.
        """
        if data is None:
            data = {}

        if stage is None:
            stage = self.current_stage

        log_event = LogEvent(
            ts=get_iso_timestamp(),
            run_id=self.run_id,
            level=level,
            event=event_type,
            data=data,
            stage=stage,
            rid=rid,
        )

        self._write_event(log_event)

    def _write_event(self, event: LogEvent) -> None:
        """Write event to JSONL file and flush.

        Parameters
        ----------
        event : LogEvent
            Event to write.
        """
        event_dict = asdict(event)
        json.dump(event_dict, self._file, ensure_ascii=False, separators=(",", ":"))
        self._file.write("\n")
        self._file.flush()

    def run_started(self, command: list[str], parameters: dict[str, Any]) -> None:
        """Log run_started event.

        Parameters
        ----------
        command : list[str]
            Command-line arguments.
        parameters : dict[str, Any]
            Configuration parameters.
        """
        self.event(
            "run_started",
            data={"command": command, "parameters": parameters},
        )

    def run_finished(
        self,
        status: str,
        duration_seconds: float,
        records_processed: int | None = None,
    ) -> None:
        """Log run_finished event.

        Parameters
        ----------
        status : str
            Run status ("success", "failed", "partial").
        duration_seconds : float
            Total execution time in seconds.
        records_processed : int | None, optional
            Total records processed.
        """
        data: dict[str, Any] = {
            "status": status,
            "duration_seconds": duration_seconds,
        }
        if records_processed is not None:
            data["records_processed"] = records_processed

        self.event("run_finished", data=data)

    def stage_started(self, stage: str, expected_records: int | None = None) -> None:
        """Log stage_started event.

        Parameters
        ----------
        stage : str
            Stage identifier.
        expected_records : int | None, optional
            Expected number of records.
        """
        self.set_stage(stage)

        data: dict[str, Any] = {}
        if expected_records is not None:
            data["expected_records"] = expected_records

        self.event("stage_started", data=data, stage=stage)

    def stage_finished(
        self,
        stage: str,
        duration_seconds: float,
        counters: dict[str, int] | None = None,
    ) -> None:
        """Log stage_finished event.

        Parameters
        ----------
        stage : str
            Stage identifier.
        duration_seconds : float
            Stage execution time in seconds.
        counters : dict[str, int] | None, optional
            Stage-specific counters.
        """
        data: dict[str, Any] = {"duration_seconds": duration_seconds}
        if counters:
            data["counters"] = counters

        self.event("stage_finished", data=data, stage=stage)

    def record_flagged(
        self,
        rid: str,
        flag_name: str,
        reason_code: str,
        stage: str | None = None,
    ) -> None:
        """Log record_flagged event.

        Parameters
        ----------
        rid : str
            Record identifier.
        flag_name : str
            Flag that was set.
        reason_code : str
            Reason code from centralized list.
        stage : str | None, optional
            Stage identifier.
        """
        self.event(
            "record_flagged",
            data={"flag_name": flag_name, "reason_code": reason_code},
            stage=stage,
            rid=rid,
        )

    def artifact_written(
        self,
        path: str,
        sha256: str,
        stage: str | None = None,
        bytes_written: int | None = None,
        record_count: int | None = None,
    ) -> None:
        """Log artifact_written event.

        Parameters
        ----------
        path : str
            Relative path to artifact.
        sha256 : str
            SHA256 hash of artifact.
        stage : str | None, optional
            Stage that produced artifact.
        bytes_written : int | None, optional
            File size in bytes.
        record_count : int | None, optional
            Number of records in artifact.
        """
        data: dict[str, Any] = {"path": path, "sha256": sha256}
        if bytes_written is not None:
            data["bytes"] = bytes_written
        if record_count is not None:
            data["record_count"] = record_count

        self.event("artifact_written", data=data, stage=stage)

    def error(
        self,
        exception_class: str,
        message: str,
        stage: str | None = None,
        rid: str | None = None,
        traceback: str | None = None,
    ) -> None:
        """Log error event.

        Parameters
        ----------
        exception_class : str
            Exception class name.
        message : str
            Error message.
        stage : str | None, optional
            Stage where error occurred.
        rid : str | None, optional
            Record identifier if error is record-specific.
        traceback : str | None, optional
            Stack trace (only in debug mode).
        """
        data: dict[str, Any] = {
            "exception_class": exception_class,
            "message": message,
        }
        if traceback is not None:
            data["traceback"] = traceback
        if rid is not None:
            data["rid"] = rid

        self.event("error", data=data, stage=stage, level="ERROR")
