TAG_PREFIX ?= v
VERSION ?=

# The immutable release tag and the floating major tag consumers pin (`@v1`).
TAG      = $(TAG_PREFIX)$(VERSION)
MAJOR    = $(firstword $(subst ., ,$(VERSION)))
MAJOR_TAG = v$(MAJOR)

.PHONY: help sync test lint format check release

help:
	@echo "Available targets:"
	@echo "  make sync                   - install dependencies with uv"
	@echo "  make test                   - run pytest"
	@echo "  make check                  - ruff format check + lint + tests"
	@echo "  make release VERSION=1.2.6  - bump, tag, roll the floating vN tag, push"

sync:
	uv sync --group dev

test:
	uv run pytest -q

lint:
	uv run ruff check .

format:
	uv run ruff format .

check:
	uv run ruff format --check .
	uv run ruff check .
	uv run pytest -q

release: test
	@test -n "$(VERSION)" || { echo "error: VERSION is required, e.g. make release VERSION=1.2.6"; exit 1; }
	@echo "$(VERSION)" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' || { echo "error: VERSION must be X.Y.Z, got '$(VERSION)'"; exit 1; }
	@test "$$(git rev-parse --abbrev-ref HEAD)" = main || { echo "error: releases happen from main, you are on '$$(git rev-parse --abbrev-ref HEAD)'"; exit 1; }
	@test -z "$$(git status --porcelain --untracked-files=no)" || { echo "error: working tree has uncommitted changes:"; git status --short --untracked-files=no; exit 1; }
	@git fetch --quiet origin --tags
	@test "$$(git rev-list --count HEAD..origin/main)" = 0 || { echo "error: local main is behind origin/main - pull first"; exit 1; }
	@test -z "$$(git tag --list '$(TAG)')" || { echo "error: tag '$(TAG)' already exists"; exit 1; }
	@echo "Releasing $(VERSION)  (tag: $(TAG), rolling $(MAJOR_TAG))"
	sed -i.bak -E 's/^version = "[0-9]+\.[0-9]+\.[0-9]+"$$/version = "$(VERSION)"/' pyproject.toml && rm -f pyproject.toml.bak
	@grep -q '^version = "$(VERSION)"$$' pyproject.toml || { echo "error: failed to set version in pyproject.toml"; exit 1; }
	uv lock
	git add pyproject.toml uv.lock
	git commit -m "Bump version to $(VERSION)"
	git tag -a $(TAG) -m "$(VERSION)"
	git tag -f -a $(MAJOR_TAG) -m "latest $(MAJOR).x"
	git push origin main
	git push origin $(TAG)
	git push --force origin $(MAJOR_TAG)
	@echo ""
	@echo "Released $(VERSION). Consumers pinned at @$(MAJOR_TAG) pick it up on their next"
	@echo "  uv lock --upgrade-package <pkg>   (or Docker rebuild)"
