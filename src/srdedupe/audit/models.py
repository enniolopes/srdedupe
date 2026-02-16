"""Data models for audit logging and run manifests.

This module defines dataclasses for structured audit logging and execution
manifests in srdedupe.
"""

from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "CommandInfo",
    "EnvironmentInfo",
    "FileInfo",
    "InputsInfo",
    "ArtifactInfo",
    "StageInfo",
    "ErrorInfo",
    "OutputsInfo",
    "ManifestData",
    "LogEvent",
]


@dataclass
class CommandInfo:
    """Command-line information.

    Attributes
    ----------
    argv : list[str]
        Complete command-line arguments.
    cwd : str | None
        Working directory basename (for privacy).
    """

    argv: list[str]
    cwd: str | None = None


@dataclass
class EnvironmentInfo:
    """Execution environment information.

    Attributes
    ----------
    python_version : str
        Python version (e.g., "3.12.3").
    platform : str
        OS and architecture (e.g., "Linux-6.8.0-x86_64").
    package_version : str
        srdedupe package version.
    dependencies : dict[str, str]
        Key dependency versions.
    """

    python_version: str
    platform: str
    package_version: str
    dependencies: dict[str, str] = field(default_factory=dict)


@dataclass
class FileInfo:
    """Input file metadata.

    Attributes
    ----------
    name : str
        Filename.
    format : str
        Detected format (e.g., "ris", "nbib").
    bytes : int
        File size in bytes.
    mtime : str | None
        ISO8601 modification time.
    sha256 : str
        SHA256 digest with "sha256:" prefix.
    records_extracted : int
        Number of records extracted.
    """

    name: str
    format: str
    bytes: int
    sha256: str
    records_extracted: int
    mtime: str | None = None


@dataclass
class InputsInfo:
    """Input files inventory.

    Attributes
    ----------
    root : str
        Input directory basename.
    files : list[FileInfo]
        List of input file metadata.
    total_records_extracted : int
        Sum of records across all files.
    """

    root: str
    files: list[FileInfo]
    total_records_extracted: int


@dataclass
class ArtifactInfo:
    """Output artifact metadata.

    Attributes
    ----------
    path : str
        Relative path from output directory.
    sha256 : str
        SHA256 digest with "sha256:" prefix.
    bytes : int | None
        File size in bytes.
    record_count : int | None
        Number of records in artifact.
    """

    path: str
    sha256: str
    bytes: int | None = None
    record_count: int | None = None


@dataclass
class StageInfo:
    """Stage execution information.

    Attributes
    ----------
    name : str
        Stage identifier.
    started_at : str
        ISO8601 start time.
    finished_at : str | None
        ISO8601 end time.
    duration_seconds : float | None
        Stage execution duration.
    counters : dict[str, int]
        Stage-specific metrics.
    artifacts : list[ArtifactInfo]
        Artifacts produced by stage.
    """

    name: str
    started_at: str
    counters: dict[str, int] = field(default_factory=dict)
    finished_at: str | None = None
    duration_seconds: float | None = None
    artifacts: list[ArtifactInfo] = field(default_factory=list)


@dataclass
class ErrorInfo:
    """Error record.

    Attributes
    ----------
    timestamp : str
        ISO8601 when error occurred.
    exception_class : str
        Exception class name.
    message : str
        Error message.
    stage : str | None
        Stage where error occurred.
    traceback : str | None
        Stack trace (if debug mode).
    rid : str | None
        Record identifier if error is record-specific.
    """

    timestamp: str
    exception_class: str
    message: str
    stage: str | None = None
    traceback: str | None = None
    rid: str | None = None


@dataclass
class OutputsInfo:
    """Output artifacts inventory.

    Attributes
    ----------
    artifacts : list[ArtifactInfo]
        List of output artifacts.
    """

    artifacts: list[ArtifactInfo] = field(default_factory=list)


@dataclass
class ManifestData:
    """Complete run manifest.

    Attributes
    ----------
    manifest_version : str
        Schema version (semver).
    run_id : str
        Unique run identifier.
    created_at : str
        ISO8601 UTC timestamp when run started.
    status : str
        Run status ("success", "failed", "partial").
    transform_version : str
        Git SHA or package version.
    command : CommandInfo
        Command-line information.
    environment : EnvironmentInfo
        Execution environment.
    inputs : InputsInfo
        Input files inventory.
    parameters : dict[str, Any]
        Configuration snapshot.
    stages : list[StageInfo]
        Stage execution records.
    outputs : OutputsInfo
        Output artifacts inventory.
    finished_at : str | None
        ISO8601 UTC timestamp when run finished.
    duration_seconds : float | None
        Total execution time.
    errors : list[ErrorInfo]
        Error records.
    """

    manifest_version: str
    run_id: str
    created_at: str
    status: str
    transform_version: str
    command: CommandInfo
    environment: EnvironmentInfo
    inputs: InputsInfo
    parameters: dict[str, Any]
    stages: list[StageInfo]
    outputs: OutputsInfo
    finished_at: str | None = None
    duration_seconds: float | None = None
    errors: list[ErrorInfo] = field(default_factory=list)


@dataclass
class LogEvent:
    """Structured log event.

    Attributes
    ----------
    ts : str
        ISO8601 timestamp with microseconds (UTC).
    run_id : str
        Unique run identifier.
    level : str
        Log level ("DEBUG", "INFO", "WARN", "ERROR").
    event : str
        Event type identifier.
    data : dict[str, Any]
        Event-specific data payload.
    stage : str | None
        Current stage identifier.
    rid : str | None
        Record identifier if event is record-specific.
    """

    ts: str
    run_id: str
    level: str
    event: str
    data: dict[str, Any]
    stage: str | None = None
    rid: str | None = None
