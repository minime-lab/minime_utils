from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path


def resolve_app_version(
    package_name: str,
    *,
    module_file: str | Path | None = None,
    pyproject_parents: int = 1,
) -> str:
    """Resolve app version from installed metadata, then pyproject fallback."""
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        if module_file is None:
            return "dev"

        pyproject_path = Path(module_file).resolve().parents[pyproject_parents] / "pyproject.toml"
        try:
            with pyproject_path.open("rb") as f:
                return str(tomllib.load(f).get("project", {}).get("version", "dev"))
        except (OSError, tomllib.TOMLDecodeError):
            return "dev"

