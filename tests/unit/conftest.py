"""Shared fixtures for scoring unit tests."""

from pathlib import Path

import pytest

from srdedupe.scoring import FSModel, load_model

_MODELS_DIR = Path(__file__).parent.parent.parent / "models"


@pytest.fixture(scope="session")
def fs_model() -> FSModel:
    """Load the production FS model once for all unit tests."""
    return load_model(_MODELS_DIR / "fs_v1.json")
