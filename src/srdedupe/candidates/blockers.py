"""Blocker plug-ins for candidate generation.

Each blocker maps records to block keys. Records sharing a key become
candidate pairs. The design prioritises *recall* — catching all potential
duplicates — while deferring precision to the scoring stage.

Architecture
------------
* ``Blocker`` — structural protocol (two attributes + one method).
* Pure functions for hashing / tokenisation (no hidden state).
* Stateful blockers (e.g. ``BibRareTitleTokensBlocker``) expose an
  explicit ``initialize()`` hook called by the generator before keying.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Protocol, runtime_checkable

from srdedupe.models.records import CanonicalRecord

try:
    from datasketch import MinHash
except ImportError:  # pragma: no cover
    MinHash = None


# ============================================================================
# Constants
# ============================================================================

MINHASH_NUM_PERM = 128
MINHASH_BANDS = 16
MINHASH_MIN_TOKENS = 3
MINHASH_SEED = 42

SIMHASH_BITS = 64
SIMHASH_CHUNKS = 4
SIMHASH_MIN_TOKENS = 5

TITLE_PREFIX_LEN = 32

RARE_TOKEN_K = 3
RARE_TOKEN_DF_MAX_RATIO = 0.01

MIN_TOKEN_LEN = 3

YEAR_OFFSETS = (-1, 0, 1)


# ============================================================================
# Statistics
# ============================================================================


@dataclass
class BlockerStats:
    """Counters collected while running a single blocker.

    Attributes
    ----------
    records_seen : int
        Total records processed.
    records_keyed : int
        Records that produced at least one blocking key.
    unique_keys : int
        Distinct blocking keys generated.
    blocks_gt1 : int
        Blocks containing two or more records.
    pairs_raw : int
        Total candidate pairs before cross-blocker dedup.
    pairs_unique : int
        Unique pairs emitted by this blocker.
    max_block : int
        Largest block size encountered.
    """

    records_seen: int = 0
    records_keyed: int = 0
    unique_keys: int = 0
    blocks_gt1: int = 0
    pairs_raw: int = 0
    pairs_unique: int = 0
    max_block: int = 0

    def to_dict(self) -> dict[str, int]:
        """Serialise to a plain dict."""
        return asdict(self)


# ============================================================================
# Protocol
# ============================================================================


@runtime_checkable
class Blocker(Protocol):
    """Structural protocol every blocker must satisfy.

    Attributes
    ----------
    name : str
        Stable identifier used in audit logs and pair provenance.
    match_key : str
        Semantic label for the field(s) this blocker relies on.
    """

    name: str
    match_key: str

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield zero or more blocking keys for *record*.

        Returns an empty iterable when the record lacks the data
        this blocker needs (replaces the old ``enabled`` + ``blocking_key``
        two-method pattern with a single, simpler contract).
        """
        ...


class StatefulBlocker(Protocol):
    """Extension for blockers requiring a corpus-level pre-pass.

    Blockers implementing this protocol will have ``initialize``
    called once **before** any ``block_keys`` call.
    """

    def initialize(self, records: list[CanonicalRecord]) -> None:
        """Pre-compute corpus-level statistics.

        Parameters
        ----------
        records : list[CanonicalRecord]
            The full record set that will subsequently be keyed.
        """
        ...


# ============================================================================
# Pure helpers
# ============================================================================


def _deterministic_hash(value: str, seed: int = 0) -> int:
    """SHA-256 based hash immune to Python's ``PYTHONHASHSEED``."""
    digest = hashlib.sha256(f"{seed}:{value}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _compute_simhash(tokens: list[str], bits: int = SIMHASH_BITS) -> int:
    """Standard SimHash using deterministic token hashes."""
    vector = [0] * bits
    for token in tokens:
        h = _deterministic_hash(token)
        for i in range(bits):
            vector[i] += 1 if (h >> i) & 1 else -1

    fingerprint = 0
    for i in range(bits):
        if vector[i] > 0:
            fingerprint |= 1 << i
    return fingerprint


def _title_tokens(record: CanonicalRecord, min_len: int = MIN_TOKEN_LEN) -> list[str]:
    """Extract title tokens, preferring pre-computed shingles."""
    shingles = record.keys.title_shingles
    if shingles is not None and len(shingles) > 0:
        return shingles

    text = record.canon.title_norm_basic
    if not text:
        return []
    return [t for t in text.split() if len(t) >= min_len]


# ============================================================================
# Exact-match blockers
# ============================================================================


class DOIExactBlocker:
    """Block by normalised DOI."""

    name: str = "doi_exact"
    match_key: str = "doi_norm"

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield the normalised DOI if present."""
        doi = record.canon.doi_norm
        if doi:
            yield doi


class PMIDExactBlocker:
    """Block by normalised PMID."""

    name: str = "pmid_exact"
    match_key: str = "pmid_norm"

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield the normalised PMID if present."""
        pmid = record.canon.pmid_norm
        if pmid:
            yield pmid


# ============================================================================
# Lexical / fuzzy blockers
# ============================================================================


class MinHashLSHTitleBlocker:
    """LSH banding over MinHash signatures of title tokens.

    Attributes
    ----------
    num_perm : int
        Number of MinHash permutations.
    bands : int
        Number of LSH bands.
    min_tokens : int
        Minimum token count to produce keys.
    """

    name: str = "minhash_lsh_title_v1"
    match_key: str = "title_shingles"

    def __init__(
        self,
        num_perm: int = MINHASH_NUM_PERM,
        bands: int = MINHASH_BANDS,
        min_tokens: int = MINHASH_MIN_TOKENS,
    ) -> None:
        if MinHash is None:
            raise ImportError(
                "datasketch is required for MinHashLSHTitleBlocker. "
                "Install with: pip install datasketch>=1.6.0"
            )
        self.num_perm = num_perm
        self.bands = bands
        self.rows_per_band = num_perm // bands
        self.min_tokens = min_tokens

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield one band-hash key per LSH band."""
        if record.flags.title_missing:
            return

        tokens = _title_tokens(record)
        if len(tokens) < self.min_tokens:
            return

        mh = MinHash(num_perm=self.num_perm, seed=MINHASH_SEED)
        for token in tokens:
            mh.update(token.encode("utf-8"))

        hv = mh.hashvalues
        for band in range(self.bands):
            start = band * self.rows_per_band
            band_bytes = ",".join(map(str, hv[start : start + self.rows_per_band]))
            band_hash = hashlib.sha256(band_bytes.encode("utf-8")).hexdigest()[:16]
            yield f"mh:b{band}:{band_hash}"


class SimHashTitleBlocker:
    """Block by SimHash fingerprint chunks.

    Attributes
    ----------
    bits : int
        Fingerprint width.
    chunks : int
        Number of chunks (each tolerates ≤ chunk_bits bit-flips).
    min_tokens : int
        Minimum token count to produce keys.
    """

    name: str = "simhash_title_v1"
    match_key: str = "title_norm_basic"

    def __init__(
        self,
        bits: int = SIMHASH_BITS,
        chunks: int = SIMHASH_CHUNKS,
        min_tokens: int = SIMHASH_MIN_TOKENS,
    ) -> None:
        self.bits = bits
        self.chunks = chunks
        self.chunk_bits = bits // chunks
        self.min_tokens = min_tokens

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield one key per fingerprint chunk."""
        if record.flags.title_missing:
            return

        tokens = _title_tokens(record)
        if len(tokens) < self.min_tokens:
            return

        fp = _compute_simhash(tokens, self.bits)
        mask = (1 << self.chunk_bits) - 1
        hex_width = self.chunk_bits // 4

        for chunk in range(self.chunks):
            value = (fp >> (chunk * self.chunk_bits)) & mask
            yield f"sh:c{chunk}:{value:0{hex_width}x}"


# ============================================================================
# Bibliographic blockers
# ============================================================================


class BibYearPM1FirstAuthorBlocker:
    """Block by (year ± 1, first-author signature)."""

    name: str = "bib_year_pm1_first_author_v1"
    match_key: str = "year_first_author"

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield three keys covering year window."""
        year = record.canon.year_norm
        author = record.canon.first_author_sig
        if year is None or not author:
            return
        for offset in YEAR_OFFSETS:
            yield f"y{year + offset}:{author}"


class BibYearPM1TitlePrefixBlocker:
    """Block by (year ± 1, title-key prefix)."""

    name: str = "bib_year_pm1_title_prefix_v1"
    match_key: str = "year_title_prefix"

    def __init__(self, prefix_len: int = TITLE_PREFIX_LEN) -> None:
        self.prefix_len = prefix_len

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield three keys covering year window."""
        year = record.canon.year_norm
        title = record.keys.title_key_strict
        if year is None or not title:
            return
        prefix = title[: self.prefix_len]
        for offset in YEAR_OFFSETS:
            yield f"y{year + offset}:tp{prefix}"


class BibRareTitleTokensBlocker:
    """Block by corpus-rare title tokens (requires ``initialize``).

    Attributes
    ----------
    k : int
        Maximum rare tokens emitted per record.
    df_max_ratio : float
        Tokens appearing in more than this fraction of documents
        are considered common and excluded.
    """

    name: str = "bib_rare_title_tokens_v1"
    match_key: str = "rare_tokens"

    def __init__(
        self,
        k: int = RARE_TOKEN_K,
        df_max_ratio: float = RARE_TOKEN_DF_MAX_RATIO,
    ) -> None:
        self.k = k
        self.df_max_ratio = df_max_ratio
        self._token_df: Counter[str] | None = None
        self._total_docs: int = 0

    @property
    def token_df(self) -> Counter[str] | None:
        """Expose token DF for inspection / testing."""
        return self._token_df

    @property
    def total_docs(self) -> int:
        """Expose total document count for inspection / testing."""
        return self._total_docs

    def initialize(self, records: list[CanonicalRecord]) -> None:
        """Compute document frequencies across the corpus."""
        df: Counter[str] = Counter()
        total = 0
        for record in records:
            if record.flags.title_missing:
                continue
            tokens = _title_tokens(record)
            if tokens:
                for t in set(tokens):
                    df[t] += 1
                total += 1
        self._token_df = df
        self._total_docs = total

    def block_keys(self, record: CanonicalRecord) -> Iterable[str]:
        """Yield keys for the *k* rarest tokens in the record.

        Raises
        ------
        RuntimeError
            If ``initialize()`` has not been called.
        """
        if self._token_df is None:
            raise RuntimeError("initialize() must be called before block_keys()")

        if record.flags.title_missing:
            return

        tokens = _title_tokens(record)
        if not tokens:
            return

        max_df = int(self._total_docs * self.df_max_ratio)
        rare = [
            (t, self._token_df.get(t, 0)) for t in tokens if 0 < self._token_df.get(t, 0) <= max_df
        ]
        rare.sort(key=lambda x: x[1])

        for token in sorted(t for t, _ in rare[: self.k]):
            yield f"rt:{token}"
