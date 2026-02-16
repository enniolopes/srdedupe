"""Pipeline orchestration engine.

This package provides the main entry point for running the complete
deduplication pipeline, including configuration and result types.
"""

from srdedupe.engine.config import PipelineConfig, PipelineResult
from srdedupe.engine.runner import run_pipeline

__all__ = [
    "PipelineConfig",
    "PipelineResult",
    "run_pipeline",
]
