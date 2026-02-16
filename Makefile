.PHONY: help install dev test test-fast test-unit test-integration lint format format-check type check clean run qa-packaging changelog release

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies (production)
	poetry install --only main

dev: ## Install all dependencies (dev + prod)
	@if [ "$$(poetry config virtualenvs.create)" != "true" ]; then \
		echo "Configuring Poetry for local development..."; \
		poetry config virtualenvs.create true; \
		poetry config virtualenvs.in-project true; \
	fi
	poetry install
	poetry run pre-commit install

test: ## Run all tests with coverage
	poetry run pytest -v --cov-report=term-missing

test-fast: ## Run tests without slow ones (alias: pytest -m "not slow")
	poetry run pytest -v -m "not slow"

test-unit: ## Run only unit tests (alias: pytest -m unit)
	poetry run pytest -v -m unit

test-integration: ## Run only integration tests (alias: pytest -m integration)
	poetry run pytest -v -m integration

lint: ## Run ruff linter
	poetry run ruff check src/srdedupe/ tests/

format: ## Format code with ruff
	poetry run ruff format src/srdedupe/ tests/
	poetry run ruff check --fix src/srdedupe/ tests/

format-check: ## Check code formatting
	poetry run ruff format --check src/srdedupe/ tests/

type: ## Run mypy type checker
	poetry run mypy src/srdedupe/

check: format-check lint type ## Run all checks (format + lint + type)

clean: ## Clean build artifacts and caches
	rm -rf build/ dist/ .eggs/ *.egg-info/ .pytest_cache/ .coverage htmlcov/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

run: ## Run CLI (example: make run ARGS="--help")
	poetry run srdedupe $(ARGS)

qa-packaging: ## Run packaging QA checks (build, twine, install tests)
	python3 scripts/qa_packaging.py

changelog: ## Regenerate CHANGELOG.md from git history (requires git-cliff)
	@command -v git-cliff >/dev/null 2>&1 || { echo "git-cliff not found. Install: https://git-cliff.org/docs/installation"; exit 1; }
	git-cliff -o CHANGELOG.md
	@echo "✓ CHANGELOG.md updated"

release: ## Create a release (usage: make release VERSION=x.y.z)
	@if [ -z "$(VERSION)" ]; then echo "Usage: make release VERSION=x.y.z"; exit 1; fi
	@command -v git-cliff >/dev/null 2>&1 || { echo "git-cliff not found. Install: https://git-cliff.org/docs/installation"; exit 1; }
	@echo "Releasing v$(VERSION)..."
	sed -i 's/^version = ".*"/version = "$(VERSION)"/' pyproject.toml
	git-cliff --tag v$(VERSION) -o CHANGELOG.md
	git add pyproject.toml CHANGELOG.md
	git commit -m "chore(release): $(VERSION)"
	git tag -a v$(VERSION) -m "Release v$(VERSION)"
	git push origin main --follow-tags
	@echo "✓ Released v$(VERSION) — CI will publish to PyPI"
