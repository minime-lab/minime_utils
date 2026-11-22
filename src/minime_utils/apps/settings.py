from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from .paths import LocalPathResolver


def resolve_settings_path(
    module_file: str | Path, *, settings_path: str | Path | None = None
) -> Path:
    """Resolve root-level ``settings.toml`` for the calling app or DAG."""
    return (
        Path(settings_path).expanduser().resolve()
        if settings_path is not None
        else LocalPathResolver.from_module(module_file).add_relative_file(
            "settings", "settings.toml"
        )
    )


def load_settings(
    module_file: str | Path, *, settings_path: str | Path | None = None
) -> dict[str, Any]:
    """Load the calling app's root-level ``settings.toml``.

    Pass the caller module's ``__file__`` so source checkouts resolve from that
    app's project root. The current working directory is also searched for
    installed apps whose module files live in site-packages.
    """
    resolved_settings_path = resolve_settings_path(
        module_file, settings_path=settings_path
    )
    if not resolved_settings_path.is_file():
        raise FileNotFoundError(f"settings.toml not found at {resolved_settings_path}")

    with resolved_settings_path.open("rb") as settings_file:
        return tomllib.load(settings_file)
