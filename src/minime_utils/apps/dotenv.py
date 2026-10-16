from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv as _load_dotenv

from .settings import find_project_root


def load_dotenv(
    module_file: str | Path,
    *,
    dotenv_path: str | Path | None = None,
    override: bool = False,
    verbose: bool = False,
    required: bool = False,
) -> bool:
    """Load the calling application's root-level ``.env`` file.

    Pass the caller's ``__file__`` so source checkouts resolve from the
    project root and installed applications can resolve from the working
    directory. An explicit ``dotenv_path`` overrides discovery.
    """
    resolved_path = (
        Path(dotenv_path).expanduser().resolve()
        if dotenv_path is not None
        else find_project_root(module_file) / ".env"
    )
    if required and not resolved_path.is_file():
        raise FileNotFoundError(f".env not found at {resolved_path}")

    return _load_dotenv(
        dotenv_path=resolved_path,
        override=override,
        verbose=verbose,
    )
