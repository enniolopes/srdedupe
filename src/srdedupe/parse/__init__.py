"""Multi-format bibliographic file parsing.

Supported formats:
- RIS (.ris) - Research Information Systems format
- PubMed/MEDLINE (.nbib, .txt) - NLM tagged format
- BibTeX (.bib) - BibTeX format
- Web of Science (.ciw) - ISI field-tagged format
- EndNote Tagged (.enw) - EndNote/Refer format

Main entry points:
- ingest_folder: Scan a folder and parse all supported files
- ingest_file: Parse a single file
"""

from srdedupe.parse.ingestion import ingest_file, ingest_folder

__all__ = [
    "ingest_file",
    "ingest_folder",
]
