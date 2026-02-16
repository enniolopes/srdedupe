"""Data models for candidate pair representation.

This module defines the schema for candidate pairs produced by the
blocking/indexing stage.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class CandidateSource:
    """Provenance information for a candidate pair.

    Attributes
    ----------
    blocker : str
        Name of the blocker that generated this pair.
    block_key : str
        The exact value used for blocking (e.g., DOI string).
    match_key : str
        Field name used for matching (e.g., 'doi_norm', 'pmid_norm').
    """

    blocker: str
    block_key: str
    match_key: str


@dataclass
class CandidatePair:
    """A candidate duplicate pair with provenance.

    Attributes
    ----------
    pair_id : str
        Deterministic pair identifier (format: "rid_a|rid_b").
    rid_a : str
        First record ID (lexicographically smaller).
    rid_b : str
        Second record ID (lexicographically larger).
    sources : list[CandidateSource]
        List of blockers that generated this pair.

    Notes
    -----
    rid_a and rid_b are always sorted lexicographically to ensure
    deterministic pair_id generation.
    """

    pair_id: str
    rid_a: str
    rid_b: str
    sources: list[CandidateSource]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict
            Dictionary representation with sources as list of dicts.
        """
        return {
            "pair_id": self.pair_id,
            "rid_a": self.rid_a,
            "rid_b": self.rid_b,
            "sources": [
                {
                    "blocker": s.blocker,
                    "block_key": s.block_key,
                    "match_key": s.match_key,
                }
                for s in self.sources
            ],
        }
