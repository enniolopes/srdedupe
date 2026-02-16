"""Matching keys generation for deduplication.

This module generates various keys used for candidate pair generation
and blocking in the deduplication pipeline.
"""

from srdedupe.models.records import Keys

MIN_SHINGLE_TOKEN_LEN = 3


def generate_keys(
    title_norm_basic: str | None,
    year_norm: int | None,
    first_author_sig: str | None,
    journal_norm: str | None,
) -> Keys:
    """Generate matching keys from normalized fields.

    Parameters
    ----------
    title_norm_basic : str | None
        Normalized title.
    year_norm : int | None
        Normalized year.
    first_author_sig : str | None
        First author signature.
    journal_norm : str | None
        Normalized journal.

    Returns
    -------
    Keys
        Generated keys object.
    """
    # Strict title key
    title_key_strict = title_norm_basic if title_norm_basic else None

    # Fuzzy title key (hyphens as spaces)
    title_key_fuzzy = None
    if title_norm_basic:
        fuzzy = title_norm_basic.replace("-", " ")
        fuzzy = " ".join(fuzzy.split())
        title_key_fuzzy = fuzzy

    title_shingles = _generate_shingles(title_norm_basic) if title_norm_basic else None

    # Composite keys
    title_year_key = None
    if title_key_strict and year_norm:
        title_year_key = f"{title_key_strict}|{year_norm}"

    title_first_author_key = None
    if title_key_strict and first_author_sig:
        title_first_author_key = f"{title_key_strict}|{first_author_sig}"

    title_journal_key = None
    if title_key_strict and journal_norm:
        title_journal_key = f"{title_key_strict}|{journal_norm}"

    return Keys(
        title_key_strict=title_key_strict,
        title_year_key=title_year_key,
        title_first_author_key=title_first_author_key,
        title_journal_key=title_journal_key,
        title_key_fuzzy=title_key_fuzzy,
        title_shingles=title_shingles,
        title_minhash=None,
        title_simhash=None,
    )


def _generate_shingles(text: str) -> list[str] | None:
    """Generate word unigram and bigram shingles for MinHash/SimHash blocking.

    Combines individual tokens (>=3 chars) with word bigrams for
    better overlap estimation in LSH-based blocking.

    Parameters
    ----------
    text : str
        Normalized title text.

    Returns
    -------
    list[str] | None
        Combined unigram + bigram shingles, or None if empty.
    """
    tokens = [t for t in text.split() if len(t) >= MIN_SHINGLE_TOKEN_LEN]
    if not tokens:
        return None

    bigrams = [f"{tokens[i]} {tokens[i + 1]}" for i in range(len(tokens) - 1)]
    shingles = tokens + bigrams
    return shingles if shingles else None
