# Contributing

Thank you for contributing! Please follow these guidelines:

## Code of Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Reporting Issues

- Check [existing issues](https://github.com/enniolopes/srdedupe/issues) first
- Include clear title, reproduction steps, version info, and relevant error messages

## Pull Requests

1. Fork and clone the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Install dependencies: `poetry install && pre-commit install`
4. Make changes with type hints and tests
5. Run checks before committing:
   ```bash
   make check  # Linting, type checking, formatting
   make test   # Run test suite
   ```
6. Commit with [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` — new feature (minor version bump)
   - `fix:` — bug fix (patch version bump)
   - `docs:` — documentation changes (no version bump)
   - `feat!:` — breaking changes (major version bump)

   Versions follow [PEP 440](https://www.python.org/dev/peps/pep-0440/).

## Release Process

Releases use [git-cliff](https://git-cliff.org/) for changelog generation and **Trusted Publishing (OIDC)** for secure PyPI publishing.

### Publishing a Release

1. **Run the release target** (bumps version, generates changelog, tags, and pushes):
   ```bash
   make release VERSION=0.19.0
   ```

2. **Automated workflows** will:
   - Create a GitHub Release with rich notes (`.github/workflows/release.yml`)
   - Build and publish to TestPyPI, then PyPI after approval (`.github/workflows/publish.yml`)

### Trusted Publishing Setup

The workflow uses **OpenID Connect (OIDC)** instead of API tokens for enhanced security:
- No long-lived secrets stored in GitHub
- Ephemeral tokens valid for ~15 minutes
- Cryptographic proof of workflow identity

For setup instructions, see [`.github/TRUSTED_PUBLISHING_SETUP.md`](.github/TRUSTED_PUBLISHING_SETUP.md).

## Testing Guidelines

Write tests for all new features using pytest markers to organize tests:

### Unit Tests
- **Location**: `tests/unit/`
- **Scope**: Single function/class in isolation
- **Speed**: < 100ms per test
- **Mock**: External dependencies

```python
@pytest.mark.unit
def test_process_valid_input() -> None:
    """Test process function with valid input."""
    result = process([1, 2, 3])
    assert result == 6
```

### Integration Tests
- **Location**: `tests/integration/`
- **Scope**: Multiple components working together
- **Speed**: < 1s per test
- **Files**: Use real test data in `tests/fixtures/`

```python
@pytest.mark.integration
def test_end_to_end_workflow() -> None:
    """Test complete workflow from input to output."""
    data = load_fixture("input.json")
    result = process_workflow(data)
    assert result.status == "success"
```

### Slow Tests
```python
@pytest.mark.slow
def test_large_dataset() -> None:
    """Test with large dataset (>1s runtime)."""
    pass
```

**Run tests selectively:**
```bash
make test-fast           # Skip slow tests
make test-unit           # Only unit tests
make test-integration    # Only integration tests
```

**Coverage**: Minimum 80% enforced. Check with:
```bash
make test                # Shows coverage report
```

## Code Style & Type Hints

All code must pass strict type checking. Pre-commit hooks enforce this automatically:

### Type Hints (Mandatory)
```python
from typing import Optional

# Correct: clear types
def calculate(items: list[str], threshold: float = 0.85) -> Optional[str]:
    """Calculate something from items."""
    return None

# Incorrect: missing types
def calculate(items, threshold=0.85):
    return None
```

### Docstrings
Follow NumPy style:
```python
def process(data: list[int]) -> dict[str, int]:
    """Process data and return results.

    Parameters
    ----------
    data : list[int]
        Input data to process.

    Returns
    -------
    dict[str, int]
        Results mapping names to values.
    """
    return {}
```

### Code Formatting & Linting

Pre-commit hooks automatically enforce:
- **Format**: `ruff format` (consistent code style)
- **Lint**: `ruff check` (code quality issues)
- **Type**: `mypy --strict` (type safety)

Manually run checks with:
```bash
make format    # Auto-format code
make lint      # Check for issues
make type      # Type check
make check     # All checks
```

## Project Structure

See [docs/architecture.md](docs/architecture.md) for complete architecture overview including:
- Module responsibilities and boundaries
- Data flow through the pipeline
- Design principles and conventions
- Import rules and dependency graph

## Development Workflow

### Initial Setup
```bash
poetry install        # Install dependencies
pre-commit install    # Set up pre-commit hooks
```

### Daily Development
```bash
make format             # Format code
make lint               # Check quality
make type               # Type check
make test-fast          # Quick tests
make test               # Full test suite
```

### Before Committing

Pre-commit hooks run automatically on every commit:
```bash
git add .
git commit -m "feat: add feature"  # Hooks run here
```

To run hooks manually:
```bash
pre-commit run --all-files
```

## Questions?

Open a [GitHub Discussion](https://github.com/enniolopes/srdedupe/discussions)

---

## Packaging QA

Before releasing a new version, run the packaging QA checks to ensure the package builds correctly, installs cleanly, and the CLI works as expected.

### Running Packaging QA

```bash
make qa-packaging
```

This command executes a comprehensive suite of checks:

1. **Build Artifacts**: Creates wheel (`.whl`) and source distribution (`.tar.gz`) using `python -m build`
2. **Metadata Validation**: Runs `twine check --strict` to validate package metadata and README rendering for PyPI
3. **Wheel Install Test**: Installs from wheel in a clean virtual environment and runs smoke tests
4. **Sdist Install Test**: Installs from source distribution in a clean virtual environment (builds wheel during install)
5. **CLI Functional Test**: Runs a real CLI command with test fixtures to ensure end-to-end functionality

### What's Validated

Each test verifies:
- Package imports correctly (`import srdedupe`)
- Version metadata is accessible
- CLI commands execute successfully (`--help`, `--version`, `parse`)
- Output files are created as expected

### Prerequisites

The QA script requires:
- `build` package for building distributions
- `twine` package for metadata validation

These are included in the `dev` dependencies:
```bash
poetry install  # Installs all dev dependencies
```

### Manual Execution

You can also run the script directly:
```bash
python3 scripts/qa_packaging.py
```

Exit code is `0` if all checks pass, `1` if any check fails.

### Troubleshooting

**Build fails**: Ensure `pyproject.toml` is correctly configured and all source files are included

**Twine check fails**: Check README.md formatting and ensure it's valid Markdown for PyPI rendering

**Install fails**: Verify dependencies in `pyproject.toml` are correct and the package structure follows best practices

**CLI tests fail**: Ensure fixtures exist in `tests/fixtures/` and the CLI entry point is properly configured
