# Contributing

Thank you for your interest in contributing!

## Code of Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## How to Contribute

- Check [existing issues](https://github.com/enniolopes/srdedupe/issues) before opening a new one.
- Use feature/bug branches: `feature/your-feature` or `fix/your-bug`.
- All code must include type hints, docstrings, and tests.
- Use [Conventional Commits](https://www.conventionalcommits.org/) for commit messages.

## Development Setup

```bash
poetry install        # Install dependencies
pre-commit install    # Enable pre-commit hooks
```

## PR Checklist

- [ ] Code uses type hints and docstrings
- [ ] Tests cover new features (pytest, correct markers)
- [ ] All checks pass:
  ```bash
  make check      # Lint, type, format
  make test       # Full test suite
  ```
- [ ] Commit with [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat: ...` — new feature (minor version bump)
   - `fix: ...` — bug fix (patch version bump)
   - `docs: ...` — documentation changes (no version bump)
   - `feat!: ...` — breaking changes (major version bump)


## Testing

- Unit tests: `tests/unit/`
- Integration tests: `tests/integration/`
- Use markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`
- Minimum coverage: 80%

## Releases

- Releases are triggered by pushing a `vX.Y.Z` tag to main. Changelog is generated automatically in GitHub Releases.
- PyPI publishing is fully automated via GitHub Actions (Trusted Publishing).

---

**Summary:**
- Clean, typed, tested code
- Conventional commits
- PRs must pass all checks
- Releases and changelog are automatic
