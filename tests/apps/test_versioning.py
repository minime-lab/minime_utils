from __future__ import annotations

import importlib.metadata

from minime_utils.apps.versioning import resolve_app_version


def test_resolve_app_version_uses_installed_metadata(monkeypatch):
    monkeypatch.setattr(importlib.metadata, "version", lambda package: "2.5.1")

    assert resolve_app_version("demo-app") == "2.5.1"


def test_resolve_app_version_falls_back_to_pyproject(tmp_path, monkeypatch):
    pkg_dir = tmp_path / "demo_pkg"
    pkg_dir.mkdir()
    module_file = pkg_dir / "app.py"
    module_file.write_text("# demo", encoding="utf-8")

    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text('[project]\nversion = "0.4.2"\n', encoding="utf-8")

    def _raise_missing(_package: str):
        raise importlib.metadata.PackageNotFoundError

    monkeypatch.setattr(importlib.metadata, "version", _raise_missing)

    assert resolve_app_version("demo-app", module_file=module_file) == "0.4.2"


def test_resolve_app_version_returns_dev_when_unavailable(tmp_path, monkeypatch):
    pkg_dir = tmp_path / "demo_pkg"
    pkg_dir.mkdir()
    module_file = pkg_dir / "app.py"
    module_file.write_text("# demo", encoding="utf-8")

    def _raise_missing(_package: str):
        raise importlib.metadata.PackageNotFoundError

    monkeypatch.setattr(importlib.metadata, "version", _raise_missing)

    assert resolve_app_version("demo-app", module_file=module_file) == "dev"

