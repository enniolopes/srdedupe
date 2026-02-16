"""Candidate pair generation via blocking strategies."""

from srdedupe.candidates.blockers import (
    BibRareTitleTokensBlocker,
    BibYearPM1FirstAuthorBlocker,
    BibYearPM1TitlePrefixBlocker,
    Blocker,
    BlockerStats,
    DOIExactBlocker,
    MinHashLSHTitleBlocker,
    PMIDExactBlocker,
    SimHashTitleBlocker,
    StatefulBlocker,
)
from srdedupe.candidates.factory import (
    BLOCKER_REGISTRY,
    BlockerConfig,
    create_blocker,
    create_blockers,
)
from srdedupe.candidates.generator import generate_candidates

__all__ = [
    # Protocol
    "Blocker",
    "BlockerStats",
    "StatefulBlocker",
    # Exact blockers
    "DOIExactBlocker",
    "PMIDExactBlocker",
    # Lexical blockers
    "MinHashLSHTitleBlocker",
    "SimHashTitleBlocker",
    # Bibliographic blockers
    "BibYearPM1FirstAuthorBlocker",
    "BibYearPM1TitlePrefixBlocker",
    "BibRareTitleTokensBlocker",
    # Factory
    "BLOCKER_REGISTRY",
    "BlockerConfig",
    "create_blocker",
    "create_blockers",
    # Generator
    "generate_candidates",
]
