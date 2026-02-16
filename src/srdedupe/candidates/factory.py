"""Registry-based factory for blocker instantiation.

New blocker types are added by extending ``BLOCKER_REGISTRY`` — no
``match``/``case`` cascade to maintain.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from srdedupe.candidates.blockers import (
    BibRareTitleTokensBlocker,
    BibYearPM1FirstAuthorBlocker,
    BibYearPM1TitlePrefixBlocker,
    Blocker,
    DOIExactBlocker,
    MinHashLSHTitleBlocker,
    PMIDExactBlocker,
    SimHashTitleBlocker,
)

# type → callable that returns a Blocker
BLOCKER_REGISTRY: dict[str, type] = {
    "doi": DOIExactBlocker,
    "pmid": PMIDExactBlocker,
    "minhash": MinHashLSHTitleBlocker,
    "simhash": SimHashTitleBlocker,
    "year_author": BibYearPM1FirstAuthorBlocker,
    "year_title": BibYearPM1TitlePrefixBlocker,
    "rare_tokens": BibRareTitleTokensBlocker,
}


@dataclass(frozen=True)
class BlockerConfig:
    """Declarative configuration for a single blocker.

    Attributes
    ----------
    type : str
        Key in ``BLOCKER_REGISTRY``.
    enabled : bool
        Disabled configs are silently skipped by ``create_blockers``.
    params : dict[str, Any]
        Keyword arguments forwarded to the blocker constructor.
    """

    type: str
    enabled: bool = True
    params: dict[str, Any] = field(default_factory=dict)


def create_blocker(config: BlockerConfig) -> Blocker:
    """Instantiate a single blocker from *config*.

    Parameters
    ----------
    config : BlockerConfig
        Blocker specification.

    Returns
    -------
    Blocker
        Ready-to-use blocker instance.

    Raises
    ------
    ValueError
        If ``config.type`` is not in the registry.
    """
    cls = BLOCKER_REGISTRY.get(config.type)
    if cls is None:
        valid = ", ".join(sorted(BLOCKER_REGISTRY))
        raise ValueError(f"Unknown blocker type: {config.type!r}. Valid types: {valid}")
    return cls(**config.params)  # type: ignore[no-any-return]


def create_blockers(configs: list[BlockerConfig]) -> list[Blocker]:
    """Instantiate all *enabled* blockers from a config list.

    Parameters
    ----------
    configs : list[BlockerConfig]
        Blocker specifications (disabled entries are filtered out).

    Returns
    -------
    list[Blocker]
        Instantiated blockers.
    """
    return [create_blocker(cfg) for cfg in configs if cfg.enabled]
