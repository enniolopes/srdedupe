"""Safe, reproducible deduplication for bibliographic references.

This package provides:
- Data models (srdedupe.models) — canonical types
- Parsing (srdedupe.parse) — bibliographic file ingestion
- Normalization (srdedupe.normalize) — field normalization
- Candidates (srdedupe.candidates) — blocking and candidate generation
- Scoring (srdedupe.scoring) — pairwise probabilistic scoring
- Decision (srdedupe.decision) — three-way classification
- Clustering (srdedupe.clustering) — global transitive clustering
- Merge (srdedupe.merge) — canonical record merging
- Engine (srdedupe.engine) — pipeline orchestration
- Audit (srdedupe.audit) — logging and traceability
- CLI (srdedupe.cli) — command-line interface
- Public API (srdedupe.api) — high-level convenience functions
"""

__version__ = "0.18.0"
__author__ = "Ennio Politi Lopes <enniolopes@gmail.com>"
__license__ = "MIT"

from srdedupe.api import (
    ParseError,
    dedupe,
    parse_file,
    parse_folder,
    write_jsonl,
)
from srdedupe.models import CanonicalRecord
from srdedupe.normalize import normalize

__all__ = [
    "__version__",
    "__author__",
    "__license__",
    "CanonicalRecord",
    "parse_file",
    "parse_folder",
    "dedupe",
    "write_jsonl",
    "normalize",
    "ParseError",
]
