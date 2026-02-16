"""Manifest writer for run execution metadata.

Provides atomic manifest writing with O(1) stage lookup.
"""

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from srdedupe.audit.models import (
    ArtifactInfo,
    CommandInfo,
    EnvironmentInfo,
    ErrorInfo,
    InputsInfo,
    ManifestData,
    OutputsInfo,
    StageInfo,
)
from srdedupe.utils import calculate_file_sha256, get_iso_timestamp

__all__ = ["ManifestWriter"]

MANIFEST_VERSION = "1.0.0"


class ManifestWriter:
    """Atomic manifest writer with indexed stage lookup.

    Builds a complete run manifest and writes it atomically to avoid
    partial/corrupted files. Stage lookups use a dict index for O(1) access.

    Attributes
    ----------
    manifest : ManifestData
        Current manifest data being built.
    output_dir : Path
        Output directory for manifest files.
    """

    def __init__(
        self,
        run_id: str,
        output_dir: Path,
        command: CommandInfo,
        environment: EnvironmentInfo,
        transform_version: str,
        parameters: dict[str, Any],
    ) -> None:
        """Initialize manifest writer.

        Parameters
        ----------
        run_id : str
            Unique run identifier.
        output_dir : Path
            Output directory for manifest.
        command : CommandInfo
            Command-line information.
        environment : EnvironmentInfo
            Execution environment.
        transform_version : str
            Git SHA or package version.
        parameters : dict[str, Any]
            Configuration parameters.
        """
        self.output_dir = output_dir
        self.manifest_path = output_dir / "run.json"

        self.manifest = ManifestData(
            manifest_version=MANIFEST_VERSION,
            run_id=run_id,
            created_at=get_iso_timestamp(),
            status="partial",
            transform_version=transform_version,
            command=command,
            environment=environment,
            inputs=InputsInfo(root="", files=[], total_records_extracted=0),
            parameters=parameters,
            stages=[],
            outputs=OutputsInfo(artifacts=[]),
        )

        self._stage_index: dict[str, StageInfo] = {}

    def _get_stage(self, stage_name: str) -> StageInfo:
        """Look up stage by name (O(1)).

        Parameters
        ----------
        stage_name : str
            Stage identifier.

        Returns
        -------
        StageInfo
            Stage execution record.

        Raises
        ------
        ValueError
            If stage not found.
        """
        stage = self._stage_index.get(stage_name)
        if stage is None:
            raise ValueError(f"Stage not found: {stage_name}")
        return stage

    def set_inputs(self, inputs: InputsInfo) -> None:
        """Set input files information.

        Parameters
        ----------
        inputs : InputsInfo
            Input files inventory.
        """
        self.manifest.inputs = inputs

    def add_stage(self, stage: StageInfo) -> None:
        """Add stage execution information.

        Parameters
        ----------
        stage : StageInfo
            Stage execution record.
        """
        self.manifest.stages.append(stage)
        self._stage_index[stage.name] = stage

    def update_stage_counters(self, stage_name: str, counters: dict[str, int]) -> None:
        """Update counters for an existing stage.

        Parameters
        ----------
        stage_name : str
            Name of stage to update.
        counters : dict[str, int]
            Counter values to merge.

        Raises
        ------
        ValueError
            If stage not found.
        """
        self._get_stage(stage_name).counters.update(counters)

    def finish_stage(
        self,
        stage_name: str,
        finished_at: str | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        """Mark stage as finished.

        Parameters
        ----------
        stage_name : str
            Name of stage to finish.
        finished_at : str | None, optional
            ISO8601 timestamp, uses current time if None.
        duration_seconds : float | None, optional
            Stage duration in seconds.

        Raises
        ------
        ValueError
            If stage not found.
        """
        stage = self._get_stage(stage_name)
        stage.finished_at = finished_at or get_iso_timestamp()
        stage.duration_seconds = duration_seconds

    def add_output_artifact(self, artifact: ArtifactInfo) -> None:
        """Add output artifact to manifest.

        Parameters
        ----------
        artifact : ArtifactInfo
            Artifact metadata.
        """
        self.manifest.outputs.artifacts.append(artifact)

    def add_error(self, error: ErrorInfo) -> None:
        """Add error record to manifest.

        Parameters
        ----------
        error : ErrorInfo
            Error information.
        """
        self.manifest.errors.append(error)

    def finish(
        self,
        status: str,
        finished_at: str | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        """Finalize manifest and write atomically.

        Parameters
        ----------
        status : str
            Final run status ("success", "failed", "partial").
        finished_at : str | None, optional
            ISO8601 timestamp, uses current time if None.
        duration_seconds : float | None, optional
            Total run duration in seconds.
        """
        self.manifest.status = status
        self.manifest.finished_at = finished_at or get_iso_timestamp()
        self.manifest.duration_seconds = duration_seconds

        self._write_manifest_atomic(self.manifest_path)

    def compute_output_artifacts(self) -> None:
        """Hash events.jsonl and register as output artifact."""
        events_path = self.output_dir / "events.jsonl"

        if events_path.exists():
            self.add_output_artifact(
                ArtifactInfo(
                    path="events.jsonl",
                    sha256=calculate_file_sha256(events_path),
                    bytes=events_path.stat().st_size,
                )
            )

    def _write_manifest_atomic(self, path: Path) -> None:
        """Write manifest atomically: write to temp, fsync, rename.

        Parameters
        ----------
        path : Path
            Final manifest path.
        """
        temp_path = path.with_suffix(".tmp")
        manifest_dict = asdict(self.manifest)

        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(manifest_dict, f, indent=2, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

        temp_path.replace(path)

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest to dictionary.

        Returns
        -------
        dict[str, Any]
            Manifest as dictionary.
        """
        return asdict(self.manifest)
