"""Run context manager for audit logging and manifest tracking."""

import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from srdedupe.audit.helpers import (
    generate_run_id,
    get_dependency_versions,
    get_git_sha,
    get_package_version,
    get_platform_info,
    get_python_version,
)
from srdedupe.audit.logger import AuditLogger
from srdedupe.audit.manifest import ManifestWriter
from srdedupe.audit.models import (
    CommandInfo,
    EnvironmentInfo,
    ErrorInfo,
    StageInfo,
)
from srdedupe.utils import get_iso_timestamp

__all__ = ["RunContext"]


class RunContext:
    """Context manager for pipeline run lifecycle.

    Manages audit logging and manifest writing for a complete pipeline run.
    Tracks stage timing internally to avoid coupling with ManifestWriter
    internals.

    Attributes
    ----------
    run_id : str
        Unique run identifier.
    output_dir : Path
        Output directory for all artifacts.
    audit_logger : AuditLogger
        Structured event logger.
    manifest_writer : ManifestWriter
        Manifest builder and writer.
    start_time : datetime
        Run start timestamp.
    """

    def __init__(
        self,
        run_id: str,
        output_dir: Path,
        audit_logger: AuditLogger,
        manifest_writer: ManifestWriter,
    ) -> None:
        """Initialize run context.

        Parameters
        ----------
        run_id : str
            Unique run identifier.
        output_dir : Path
            Output directory.
        audit_logger : AuditLogger
            Event logger.
        manifest_writer : ManifestWriter
            Manifest writer.
        """
        self.run_id = run_id
        self.output_dir = output_dir
        self.audit_logger = audit_logger
        self.manifest_writer = manifest_writer
        self.start_time = datetime.now(UTC)
        self._stage_start_times: dict[str, datetime] = {}

    @classmethod
    def start(
        cls,
        output_dir: Path,
        parameters: dict[str, Any],
        command_argv: list[str] | None = None,
    ) -> "RunContext":
        """Start a new run context.

        Creates output directory structure, initializes logger and manifest.

        Parameters
        ----------
        output_dir : Path
            Output directory for run artifacts.
        parameters : dict[str, Any]
            Configuration parameters for run.
        command_argv : list[str] | None, optional
            Command-line arguments, uses sys.argv if None.

        Returns
        -------
        RunContext
            Initialized run context.
        """
        run_id = generate_run_id()

        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "artifacts").mkdir(exist_ok=True)
        (output_dir / "reports").mkdir(exist_ok=True)

        command = CommandInfo(
            argv=command_argv or sys.argv,
            cwd=Path.cwd().name or None,
        )

        environment = EnvironmentInfo(
            python_version=get_python_version(),
            platform=get_platform_info(),
            package_version=get_package_version(),
            dependencies=get_dependency_versions(["jsonschema", "click"]),
        )

        git_sha = get_git_sha()
        transform_version = f"git:{git_sha}" if git_sha else get_package_version()

        audit_logger = AuditLogger(
            run_id=run_id,
            log_path=output_dir / "events.jsonl",
        )

        manifest_writer = ManifestWriter(
            run_id=run_id,
            output_dir=output_dir,
            command=command,
            environment=environment,
            transform_version=transform_version,
            parameters=parameters,
        )

        audit_logger.run_started(command=command.argv, parameters=parameters)

        return cls(
            run_id=run_id,
            output_dir=output_dir,
            audit_logger=audit_logger,
            manifest_writer=manifest_writer,
        )

    def start_stage(self, stage_name: str, expected_records: int | None = None) -> None:
        """Start a pipeline stage.

        Parameters
        ----------
        stage_name : str
            Stage identifier.
        expected_records : int | None, optional
            Expected number of records to process.
        """
        self._stage_start_times[stage_name] = datetime.now(UTC)

        stage = StageInfo(
            name=stage_name,
            started_at=get_iso_timestamp(),
        )

        self.manifest_writer.add_stage(stage)

        self.audit_logger.stage_started(
            stage=stage_name,
            expected_records=expected_records,
        )

    def finish_stage(
        self,
        stage_name: str,
        counters: dict[str, int] | None = None,
    ) -> None:
        """Finish a pipeline stage.

        Parameters
        ----------
        stage_name : str
            Stage identifier.
        counters : dict[str, int] | None, optional
            Final counters for stage.

        Raises
        ------
        ValueError
            If stage was not started.
        """
        start_time = self._stage_start_times.pop(stage_name, None)
        if start_time is None:
            raise ValueError(f"Stage not started: {stage_name}")

        finished_at = get_iso_timestamp()
        duration = (datetime.now(UTC) - start_time).total_seconds()

        self.manifest_writer.finish_stage(
            stage_name=stage_name,
            finished_at=finished_at,
            duration_seconds=duration,
        )

        if counters:
            self.manifest_writer.update_stage_counters(
                stage_name=stage_name,
                counters=counters,
            )

        self.audit_logger.stage_finished(
            stage=stage_name,
            duration_seconds=duration,
            counters=counters,
        )

    def record_error(
        self,
        exception: Exception,
        stage: str | None = None,
        rid: str | None = None,
        include_traceback: bool = False,
    ) -> None:
        """Record an error in logs and manifest.

        Parameters
        ----------
        exception : Exception
            Exception that occurred.
        stage : str | None, optional
            Stage where error occurred.
        rid : str | None, optional
            Record identifier if error is record-specific.
        include_traceback : bool, optional
            Whether to include stack trace, by default False.
        """
        exception_class = type(exception).__name__
        message = str(exception)

        tb = None
        if include_traceback:
            if sys.exc_info()[0] is not None:
                tb = traceback.format_exc()
            else:
                tb = "".join(
                    traceback.format_exception(
                        type(exception),
                        exception,
                        exception.__traceback__,
                    )
                )

        error_info = ErrorInfo(
            timestamp=get_iso_timestamp(),
            exception_class=exception_class,
            message=message,
            stage=stage,
            traceback=tb,
            rid=rid,
        )

        self.manifest_writer.add_error(error_info)

        self.audit_logger.error(
            exception_class=exception_class,
            message=message,
            stage=stage,
            rid=rid,
            traceback=tb,
        )

    def finish(
        self,
        status: str = "success",
        records_processed: int | None = None,
    ) -> None:
        """Finish the run and write final manifest.

        Closes the audit logger before computing artifact hashes to ensure
        the events.jsonl file is complete on disk.

        Parameters
        ----------
        status : str, optional
            Final run status, by default "success".
        records_processed : int | None, optional
            Total records processed.
        """
        duration = (datetime.now(UTC) - self.start_time).total_seconds()

        self.audit_logger.run_finished(
            status=status,
            duration_seconds=duration,
            records_processed=records_processed,
        )

        self.audit_logger.close()

        self.manifest_writer.compute_output_artifacts()

        self.manifest_writer.finish(
            status=status,
            finished_at=get_iso_timestamp(),
            duration_seconds=duration,
        )

    def __enter__(self) -> "RunContext":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, recording errors if present."""
        if exc_type is not None:
            self.record_error(exc_val, include_traceback=True)
            self.finish(status="failed")
        else:
            self.finish(status="success")
