# srdedupe — Safe Bibliographic Deduplication

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![CI](https://github.com/enniolopes/srdedupe/actions/workflows/ci.yml/badge.svg)](https://github.com/enniolopes/srdedupe/actions)
[![Codecov](https://codecov.io/gh/enniolopes/srdedupe/branch/main/graph/badge.svg)](https://codecov.io/gh/enniolopes/srdedupe)

Safe, reproducible deduplication for systematic reviews and bibliographic databases.

Parses and deduplicates bibliographic reference files (RIS, NBIB, BibTeX, WoS, EndNote) with FPR-controlled decision making, full audit trails, and deterministic outputs.

## Installation

```bash
pip install srdedupe
```

## Quick Start

### Parse and export

```python
from srdedupe import parse_file, parse_folder, write_jsonl

# Single file (format auto-detected)
records = parse_file("references.ris")

# Entire folder
records = parse_folder("data/", recursive=True)

# Export to JSONL
write_jsonl(records, "output.jsonl")
```

### Deduplicate

```python
from srdedupe import dedupe

result = dedupe("references.ris", output_dir="out", fpr_alpha=0.01)

print(f"Records: {result.total_records}")
print(f"Auto-merged: {result.total_duplicates_auto}")
print(f"Review required: {result.total_review_pairs}")
print(f"Output: {result.output_files['deduplicated_ris']}")
```

### CLI

```bash
# Parse to JSONL
srdedupe parse references.ris -o output.jsonl
srdedupe parse data/ -o records.jsonl --recursive

# Full deduplication pipeline
srdedupe deduplicate references.ris
srdedupe deduplicate data/ -o results --fpr-alpha 0.005 --verbose
```

## How It Works

A 6-stage pipeline controlled by false positive rate (FPR):

1. **Parse & Normalize** — Multi-format ingestion, field normalization
2. **Candidate Generation** — High-recall blocking (DOI, PMID, year+title, LSH)
3. **Probabilistic Scoring** — Fellegi-Sunter model with field-level comparisons
4. **Three-Way Decision** — AUTO_DUP / REVIEW / AUTO_KEEP with Neyman-Pearson FPR control
5. **Global Clustering** — Connected components with anti-transitivity checks
6. **Canonical Merge** — Deterministic survivor selection and field merging

Pairs classified as REVIEW are preserved in output artifacts for manual inspection.

## API Reference

`parse_file(path, *, strict=True) -> list[CanonicalRecord]`

- Parse a single bibliographic file. Format is auto-detected from file content.

`parse_folder(path, *, pattern=None, recursive=False, strict=False) -> list[CanonicalRecord]`

- Parse all supported files in a folder. Optional glob `pattern` (e.g. `"*.ris"`).

`write_jsonl(records, path, *, sort_keys=True) -> None`

- Write records to JSONL file with deterministic field ordering.

`dedupe(input_path, *, output_dir="out", fpr_alpha=0.01, t_low=0.3, t_high=None) -> PipelineResult`

Run the full deduplication pipeline. Returns a `PipelineResult` with:
- `success`, `total_records`, `total_candidates`, `total_duplicates_auto`, `total_review_pairs`
- `output_files` — dict mapping artifact names to file paths
- `error_message` — error details if `success` is False

### Advanced: `PipelineConfig` + `run_pipeline`

For full control (custom blockers, FS model path, audit logger):

```python
from pathlib import Path
from srdedupe.engine import PipelineConfig, run_pipeline

config = PipelineConfig(
    fpr_alpha=0.01,
    t_low=0.3,
    t_high=None,
    candidate_blockers=["doi", "pmid", "year_title"],
    output_dir=Path("out"),
)

result = run_pipeline(input_path=Path("references.ris"), config=config)
```

## Supported Formats

| Format | Extensions |
|--------|-----------|
| RIS | `.ris` |
| PubMed/NBIB | `.nbib`, `.txt` |
| BibTeX | `.bib` |
| Web of Science | `.ciw` |
| EndNote Tagged | `.enw` |

## Pipeline Output Structure

```
out/
├── stage1/canonical_records.jsonl
├── stage2/candidate_pairs.jsonl
├── stage3/scored_pairs.jsonl
├── stage4/pair_decisions.jsonl
├── stage5/clusters.jsonl
└── artifacts/
    ├── deduped_auto.ris
    ├── merged_records.jsonl
    └── clusters_enriched.jsonl
```

## Development

```bash
make dev           # Install dependencies + pre-commit hooks
make test-fast     # Quick validation while coding
make check         # Lint + type check + format (before committing)
make test          # Full test suite (417 tests, ≥80% coverage)
```

## Documentation

- [CONTRIBUTING.md](CONTRIBUTING.md) — Code style, testing, contribution guidelines

## License

MIT — see [LICENSE](LICENSE).

## Citation

```bibtex
@software{srdedupe2026,
  author = {Lopes, Ennio Politi},
  title = {srdedupe: Safe Bibliographic Deduplication},
  year = {2026},
  url = {https://github.com/enniolopes/srdedupe}
}
```
