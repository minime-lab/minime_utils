PYTHON_VERSION ?= 3.12

.PHONY: help ensure-uv init first-run sync test check

help:
	@echo "Available targets:"
	@echo "  make first-run   - install Python and sync deps"
	@echo "  make run         - run app locally with reload"
	@echo "  make test        - run tests"
	@echo "  make check       - format check + lint + tests"

ensure-uv:
	uv --version

init: ensure-uv
	uv python install $(PYTHON_VERSION)
	uv sync

first-run: init
	@echo "Project bootstrap complete"

sync: ensure-uv
	uv sync

test: ensure-uv
	uv run pytest -q

check: ensure-uv
	uv run ruff format --check .
	uv run ruff check .
	uv run pytest -q


