"""Named local filesystem paths and MinIO object locations for an app."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator


@dataclass(frozen=True)
class MinioPath:
    """A MinIO object location, represented by bucket and object key."""

    bucket: str
    key: str

    def __str__(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class PathCollection:
    """A named collection of paths with convenient attribute access."""

    def __init__(self) -> None:
        self._paths: dict[str, Path | MinioPath] = {}

    def add(self, name: str, path: Path | MinioPath) -> Path | MinioPath:
        if not name.isidentifier() or name.startswith("_"):
            raise ValueError(f"Path name must be a public identifier: {name!r}")
        if name in self._paths:
            raise ValueError(f"Path name already registered: {name!r}")
        self._paths[name] = path
        return path

    def __getattr__(self, name: str) -> Path | MinioPath:
        try:
            return self._paths[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __getitem__(self, name: str) -> Path | MinioPath:
        return self._paths[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._paths)

    def as_dict(self) -> dict[str, Path | MinioPath]:
        return self._paths.copy()


class LocalPathResolver:
    """Resolve named local paths relative to a discovered or explicit root."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.paths = PathCollection()

    @classmethod
    def from_module(cls, module_file: str | Path) -> LocalPathResolver:
        return cls(find_project_root(module_file))

    def add_relative_path(self, name: str, relative_path: str | Path) -> Path:
        path = self._resolve_relative(relative_path)
        return self.paths.add(name, path)  # type: ignore[return-value]

    def add_relative_file(self, name: str, relative_file: str | Path) -> Path:
        path = self._resolve_relative(relative_file)
        return self.paths.add(name, path)  # type: ignore[return-value]

    def _resolve_relative(self, relative: str | Path) -> Path:
        candidate = Path(relative).expanduser()
        if candidate.is_absolute():
            raise ValueError("Expected a path relative to the local root")
        resolved = (self.root / candidate).resolve()
        if not resolved.is_relative_to(self.root):
            raise ValueError("Relative path escapes the local root")
        return resolved


class MinioPathResolver:
    """Resolve named MinIO object keys relative to an app bucket and prefix."""

    def __init__(self, bucket: str, root_prefix: str = "") -> None:
        if not bucket.strip():
            raise ValueError("bucket must not be empty")
        self.bucket = bucket.strip()
        self.root_prefix = _normalize_minio_key(root_prefix)
        self.paths = PathCollection()

    def add_relative_path(
        self, name: str, relative_path: str | PurePosixPath
    ) -> MinioPath:
        return self.add_relative_file(name, relative_path)

    def add_relative_file(
        self, name: str, relative_file: str | PurePosixPath
    ) -> MinioPath:
        relative_key = _normalize_minio_key(str(relative_file))
        if (
            not relative_key
            or relative_key.startswith("/")
            or ".." in PurePosixPath(relative_key).parts
        ):
            raise ValueError("Expected a non-empty key relative to the MinIO root")
        key = "/".join(part for part in (self.root_prefix, relative_key) if part)
        return self.paths.add(name, MinioPath(bucket=self.bucket, key=key))  # type: ignore[return-value]


def find_project_root(module_file: str | Path) -> Path:
    """Find project root by searching for ``pyproject.toml`` from module or cwd."""
    module_path = Path(module_file).expanduser().resolve()
    for start in (module_path.parent, Path.cwd()):
        for candidate in (start, *start.parents):
            if (candidate / "pyproject.toml").is_file():
                return candidate
    raise FileNotFoundError(
        f"Could not find project root (pyproject.toml) from {module_path} or {Path.cwd()}"
    )


def _normalize_minio_key(key: str) -> str:
    normalized = "/".join(
        part for part in key.replace("\\", "/").split("/") if part not in ("", ".")
    )
    return normalized
