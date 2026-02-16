# Test Fixtures

This directory contains test data for various pipeline stages.

## Directory Structure

```
fixtures/
├── synthetic/          # Minimal examples for each file format
├── real/              # Real-world data samples
├── stage1/            # Stage 1 normalization test cases
├── candidates/        # Candidate generation fixtures
├── decision/          # Decision module calibration data
├── audit/             # Audit logging examples
├── scoring/           # Pairwise scoring test cases
└── clustering/        # Clustering algorithm fixtures
```

## Usage

Most fixtures are defined inline in test files for maintainability. This directory contains:

- **File format examples** (`synthetic/`, `real/`) - Sample RIS, NBIB, BibTeX, WoS, EndNote files
- **Golden outputs** (`stage1/`, `scoring/`) - Expected outputs for determinism tests
- **Reference data** (`decision/`, `audit/`) - Schema validation and calibration examples

See individual test files in `tests/unit/` and `tests/integration/` for how fixtures are used.
