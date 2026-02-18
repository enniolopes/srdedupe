"""Unit tests for format detection and encoding utilities."""

import pytest

from srdedupe.parse.base import detect_encoding, normalize_line_endings, sniff_format

# === Format Sniffing Tests ===


@pytest.mark.unit
def test_sniff_format_bibtex() -> None:
    """Test BibTeX format detection (highest priority)."""
    lines = ["@article{Test2024,", "  author = {Smith, J},", "}"]
    assert sniff_format(lines) == "bibtex"


@pytest.mark.unit
def test_sniff_format_wos() -> None:
    """Test WoS format detection via PT marker."""
    lines = ["PT J", "AU Smith, J", "TI Test Article", "ER"]
    assert sniff_format(lines) == "wos"


@pytest.mark.unit
def test_sniff_format_ris() -> None:
    """Test RIS format detection via TY tag."""
    lines = ["TY  - JOUR", "AU  - Smith, J", "TI  - Test", "ER  - "]
    assert sniff_format(lines) == "ris"


@pytest.mark.unit
def test_sniff_format_ris_without_er_in_sample() -> None:
    """Test RIS detection when first record is large and ER falls beyond sample."""
    lines = ["TY  - JOUR", "AU  - Smith, J"]
    lines.extend([f"AD  - Affiliation {i}" for i in range(120)])
    assert sniff_format(lines) == "ris"


@pytest.mark.unit
def test_sniff_format_pubmed() -> None:
    """Test PubMed format detection via PMID."""
    lines = ["PMID- 12345678", "TI  - Test Article", "AB  - Abstract"]
    assert sniff_format(lines) == "pubmed"


@pytest.mark.unit
def test_sniff_format_endnote() -> None:
    """Test EndNote format detection via %X tags."""
    lines = ["%0 Journal Article", "%A Smith, J", "%T Test", "%D 2024"]
    assert sniff_format(lines) == "endnote_tagged"


@pytest.mark.unit
def test_sniff_format_unknown() -> None:
    """Test unknown format fallback."""
    lines = ["Random text", "No format markers here"]
    assert sniff_format(lines) == "unknown"


@pytest.mark.unit
def test_sniff_format_empty() -> None:
    """Test empty file returns unknown."""
    assert sniff_format([]) == "unknown"


@pytest.mark.unit
def test_sniff_format_priority_order() -> None:
    """Test format detection follows priority: BibTeX > WoS > RIS > PubMed > EndNote."""
    # If multiple markers exist, most specific wins
    bibtex_lines = ["@article{Test,", "PT J", "TY  - JOUR"]
    assert sniff_format(bibtex_lines) == "bibtex"

    wos_lines = ["PT J", "TY  - JOUR", "PMID- 123"]
    assert sniff_format(wos_lines) == "wos"

    ris_lines = ["TY  - JOUR", "ER  - ", "PMID- 123"]
    assert sniff_format(ris_lines) == "ris"


# === Encoding Detection Tests ===


@pytest.mark.unit
def test_detect_encoding_utf8() -> None:
    """Test UTF-8 encoding detection."""
    content = "Hello World"
    assert detect_encoding(content.encode("utf-8")) == "utf-8"


@pytest.mark.unit
def test_detect_encoding_utf8_bom() -> None:
    """Test UTF-8 with BOM detection."""
    content = b"\xef\xbb\xbfHello"
    assert detect_encoding(content) == "utf-8-sig"


@pytest.mark.unit
def test_detect_encoding_latin1() -> None:
    """Test Latin-1 fallback for invalid UTF-8."""
    # Byte sequence invalid in UTF-8
    content = b"\xe9\xe0\xe7"
    assert detect_encoding(content) == "latin-1"


@pytest.mark.unit
def test_detect_encoding_non_ascii_utf8() -> None:
    """Test UTF-8 detection with non-ASCII characters."""
    content = "Café Müller Étude"
    assert detect_encoding(content.encode("utf-8")) == "utf-8"


# === Line Ending Normalization Tests ===


@pytest.mark.unit
def test_normalize_line_endings_crlf() -> None:
    """Test CRLF to LF conversion."""
    content = "line1\r\nline2\r\nline3"
    assert normalize_line_endings(content) == "line1\nline2\nline3"


@pytest.mark.unit
def test_normalize_line_endings_cr() -> None:
    """Test CR to LF conversion."""
    content = "line1\rline2\rline3"
    assert normalize_line_endings(content) == "line1\nline2\nline3"


@pytest.mark.unit
def test_normalize_line_endings_mixed() -> None:
    """Test mixed line endings normalization."""
    content = "line1\r\nline2\rline3\nline4"
    assert normalize_line_endings(content) == "line1\nline2\nline3\nline4"


@pytest.mark.unit
def test_normalize_line_endings_lf_only() -> None:
    """Test LF-only content unchanged."""
    content = "line1\nline2\nline3"
    assert normalize_line_endings(content) == "line1\nline2\nline3"
