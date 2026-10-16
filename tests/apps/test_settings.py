from __future__ import annotations

from pathlib import Path

import pytest

from minime_utils.apps import find_project_root, load_settings, resolve_settings_path


def test_find_project_root_prefers_source_module_project(tmp_path, monkeypatch):
    source_root = tmp_path / "source-project"
    module_file = source_root / "src" / "demo_app" / "main.py"
    module_file.parent.mkdir(parents=True)
    module_file.touch()
    (source_root / "pyproject.toml").touch()

    working_root = tmp_path / "working-project"
    working_root.mkdir()
    (working_root / "pyproject.toml").touch()
    monkeypatch.chdir(working_root)

    assert find_project_root(module_file) == source_root


def test_find_project_root_uses_working_directory_for_installed_module(
    tmp_path, monkeypatch
):
    module_file = tmp_path / "site-packages" / "demo_app" / "main.py"
    module_file.parent.mkdir(parents=True)
    module_file.touch()

    project_root = tmp_path / "runtime-project"
    project_root.mkdir()
    (project_root / "pyproject.toml").touch()
    monkeypatch.chdir(project_root)

    assert find_project_root(module_file) == project_root


def test_settings_apis_use_installed_app_project_root(tmp_path, monkeypatch):
    module_file = tmp_path / "site-packages" / "demo_app" / "main.py"
    module_file.parent.mkdir(parents=True)
    module_file.touch()

    project_root = tmp_path / "runtime-project"
    project_root.mkdir()
    (project_root / "pyproject.toml").touch()
    settings_file = project_root / "settings.toml"
    settings_file.write_text('[app]\nname = "demo"\n', encoding="utf-8")
    monkeypatch.chdir(project_root)

    assert resolve_settings_path(module_file) == settings_file
    assert load_settings(module_file) == {"app": {"name": "demo"}}


def test_find_project_root_raises_when_no_project_root_exists(tmp_path, monkeypatch):
    module_file = tmp_path / "site-packages" / "demo_app" / "main.py"
    module_file.parent.mkdir(parents=True)
    module_file.touch()
    working_directory = tmp_path / "runtime"
    working_directory.mkdir()
    monkeypatch.chdir(working_directory)

    with pytest.raises(FileNotFoundError, match="Could not find project root"):
        find_project_root(module_file)
