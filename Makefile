PYTHON_VERSION ?= 3.12

.PHONY: help ensure-uv init first-run sync test check ruff

help:
	@echo "Available targets:"
	@echo "  make first-run   - install Python and sync deps"
	@echo "  make run         - run app locally with reload"
	@echo "  make test        - run tests"
	@echo "  make check       - format check + lint + tests"
	@echo "  make ruff         - run the pre-commit checks"

ensure-uv:
	uv --version

init: ensure-uv
	uv run pre-commit install
	uv python install $(PYTHON_VERSION)
	uv sync

first-run: init
	@echo "Project bootstrap complete"

sync: ensure-uv
	uv sync

test: ensure-uv
	uv run pytest -q

ruff: ensure-uv
	uv run pre-commit run --all-files

check: ensure-uv
	$(MAKE) ruff
	uv run pytest -q


