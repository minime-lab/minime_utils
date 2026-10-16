"""Application-related shared utilities."""

from .settings import load_settings
from .versioning import resolve_app_version

__all__ = ["load_settings", "resolve_app_version"]
