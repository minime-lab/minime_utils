from __future__ import annotations

import os

import pytest

from minime_utils.apps import (
    find_project_root,
    load_dotenv,
    load_settings,
    resolve_settings_path,
)


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


def test_load_dotenv_uses_source_module_project_root(tmp_path, monkeypatch):
    project_root = tmp_path / "source-project"
    module_file = project_root / "src" / "demo_app" / "main.py"
    module_file.parent.mkdir(parents=True)
    module_file.touch()
    (project_root / "pyproject.toml").touch()
    (project_root / ".env").write_text(
        "MINIME_TEST_SOURCE=loaded\n", encoding="utf-8"
    )
    monkeypatch.delenv("MINIME_TEST_SOURCE", raising=False)

    assert load_dotenv(module_file)
    assert os.environ["MINIME_TEST_SOURCE"] == "loaded"


def test_load_dotenv_uses_working_directory_for_installed_module(tmp_path, monkeypatch):
    module_file = tmp_path / "site-packages" / "demo_app" / "main.py"
    module_file.parent.mkdir(parents=True)
    module_file.touch()
    project_root = tmp_path / "runtime-project"
    project_root.mkdir()
    (project_root / "pyproject.toml").touch()
    (project_root / ".env").write_text(
        "MINIME_TEST_INSTALLED=loaded\n", encoding="utf-8"
    )
    monkeypatch.chdir(project_root)
    monkeypatch.delenv("MINIME_TEST_INSTALLED", raising=False)

    assert load_dotenv(module_file)
    assert os.environ["MINIME_TEST_INSTALLED"] == "loaded"


def test_load_dotenv_can_require_the_project_file(tmp_path, monkeypatch):
    module_file = tmp_path / "site-packages" / "demo_app" / "main.py"
    module_file.parent.mkdir(parents=True)
    module_file.touch()
    project_root = tmp_path / "runtime-project"
    project_root.mkdir()
    (project_root / "pyproject.toml").touch()
    monkeypatch.chdir(project_root)

    with pytest.raises(FileNotFoundError, match=r"\.env not found"):
        load_dotenv(module_file, required=True)
