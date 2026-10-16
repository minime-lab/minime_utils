from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any


def find_project_root(module_file: str | Path) -> Path:
    """Find the project root for a source module or installed application.

    Source modules are resolved from their file path. If the module lives in
    site-packages, the current working directory is searched as the deployment
    project root.
    """
    module_path = Path(module_file).expanduser().resolve()
    starts = (module_path.parent, Path.cwd())

    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / "pyproject.toml").is_file():
                return candidate

    raise FileNotFoundError(
        f"Could not find project root (pyproject.toml) from {module_path} or {Path.cwd()}"
    )


def resolve_settings_path(
    module_file: str | Path, *, settings_path: str | Path | None = None
) -> Path:
    """Resolve root-level ``settings.toml`` for the calling app or DAG."""
    return (
        Path(settings_path).expanduser().resolve()
        if settings_path is not None
        else find_project_root(module_file) / "settings.toml"
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
