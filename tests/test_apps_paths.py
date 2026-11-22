from __future__ import annotations

from pathlib import Path

import pytest

from minime_utils.apps.paths import (
    LocalPathResolver,
    MinioPath,
    MinioPathResolver,
    PathCollection,
    find_project_root,
)
from minime_utils.apps.settings import resolve_settings_path


def test_find_project_root_walks_up_from_module_file(tmp_path: Path) -> None:
    project = tmp_path / "project"
    module = project / "src" / "sample" / "app.py"
    module.parent.mkdir(parents=True)
    module.touch()
    (project / "pyproject.toml").touch()

    assert find_project_root(module) == project


def test_local_resolver_registers_named_paths_and_settings(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").touch()
    module = tmp_path / "src" / "app.py"
    module.parent.mkdir()
    module.touch()
    resolver = LocalPathResolver.from_module(module)

    settings_path = resolver.add_relative_file("settings", "settings.toml")
    logs_path = resolver.add_relative_path("logs", "var/logs")

    assert settings_path == tmp_path / "settings.toml"
    assert resolver.paths.settings == settings_path
    assert resolver.paths["logs"] == tmp_path / "var" / "logs"
    assert resolver.paths.as_dict() == {"settings": settings_path, "logs": logs_path}
    assert resolve_settings_path(module) == settings_path


@pytest.mark.parametrize("relative", ["../outside", "nested/../../outside"])
def test_local_resolver_rejects_paths_outside_root(
    tmp_path: Path, relative: str
) -> None:
    resolver = LocalPathResolver(tmp_path / "project")
    with pytest.raises(ValueError, match="escapes"):
        resolver.add_relative_path("bad", relative)


def test_local_resolver_rejects_absolute_paths(tmp_path: Path) -> None:
    resolver = LocalPathResolver(tmp_path)
    with pytest.raises(ValueError, match="relative"):
        resolver.add_relative_file("bad", tmp_path / "file.txt")


def test_path_collection_rejects_duplicate_or_invalid_names() -> None:
    paths = PathCollection()
    paths.add("logs", Path("logs"))
    with pytest.raises(ValueError, match="already registered"):
        paths.add("logs", Path("other"))
    with pytest.raises(ValueError, match="identifier"):
        paths.add("not-a-name", Path("other"))
    with pytest.raises(AttributeError):
        _ = paths.missing


def test_minio_resolver_builds_app_relative_keys() -> None:
    resolver = MinioPathResolver(" app-data ", "apps/example//")
    settings = resolver.add_relative_file("settings", "settings.toml")
    logs = resolver.add_relative_path("logs", "./logs/")

    assert settings == MinioPath("app-data", "apps/example/settings.toml")
    assert logs == MinioPath("app-data", "apps/example/logs")
    assert str(settings) == "s3://app-data/apps/example/settings.toml"
    assert resolver.paths.logs == logs


@pytest.mark.parametrize("relative", ["", "../other", "/absolute/key"])
def test_minio_resolver_rejects_invalid_relative_keys(relative: str) -> None:
    resolver = MinioPathResolver("bucket", "apps/example")
    with pytest.raises(ValueError):
        resolver.add_relative_file("bad", relative)


def test_minio_resolver_requires_bucket() -> None:
    with pytest.raises(ValueError, match="bucket"):
        MinioPathResolver(" ")
