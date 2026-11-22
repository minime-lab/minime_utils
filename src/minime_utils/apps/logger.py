"""Queue-backed application logging with local buffering and MinIO upload."""

from __future__ import annotations

import json
import logging
import logging.handlers
import queue
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from minime_utils.minio.writers import write_bytes


class _JsonLineFormatter(logging.Formatter):
    """Format log records as JSON Lines, retaining exception and extra fields."""

    _standard_fields = set(logging.makeLogRecord({}).__dict__) | {
        "message",
        "asctime",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        result: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            result["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            result["stack"] = self.formatStack(record.stack_info)
        result.update(
            (key, value)
            for key, value in record.__dict__.items()
            if key not in self._standard_fields and key not in result
        )
        return json.dumps(result, default=str)


class MinimeLogger:
    """Manage a named logger, local rotating files, and explicit MinIO flushes.

    ``logs_path`` and ``minio_bucket`` are required. ``minio_prefix`` is the
    app-specific directory within the bucket. A flush waits for queued records,
    closes the active file by rotating it, and uploads all closed segments.
    Failed segments remain on disk for a later retry.
    """

    def __init__(
        self,
        *,
        name: str,
        logs_path: str | Path,
        minio_bucket: str,
        minio_prefix: str,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 3,
        level: int | str = logging.INFO,
        queue_size: int = 10_000,
    ) -> None:
        if not minio_bucket.strip():
            raise ValueError("minio_bucket must not be empty")
        if max_bytes <= 0 or backup_count < 1 or queue_size < 1:
            raise ValueError("max_bytes, backup_count, and queue_size must be positive")

        self.name = name
        self.logs_path = Path(logs_path).expanduser().resolve()
        self.logs_path.mkdir(parents=True, exist_ok=True)
        self.minio_bucket = minio_bucket
        self.minio_prefix = minio_prefix.strip("/")
        self._file_path = self.logs_path / f"{name}.jsonl"
        self._queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=queue_size)
        self._handler = logging.handlers.QueueHandler(self._queue)
        self._handler.setLevel(level)
        self._file_handler = logging.handlers.RotatingFileHandler(
            self._file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self._file_handler.setFormatter(_JsonLineFormatter())
        self._listener = logging.handlers.QueueListener(
            self._queue, self._file_handler, respect_handler_level=True
        )
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.addHandler(self._handler)
        self.logger.propagate = False
        self._lock = threading.RLock()
        self._closed = False
        self._listener.start()

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        *,
        name: str,
        minio_bucket: str,
        minio_prefix: str,
        project_root: str | Path | None = None,
    ) -> MinimeLogger:
        """Build a logger from a settings dictionary.

        Missing logging keys default to the values documented in the former
        example settings: ``logs``, 10 MiB, three backups, a 10,000 record
        queue, and INFO. Relative log paths are resolved from ``project_root``
        when supplied, otherwise from the current working directory.
        """
        if not isinstance(config, dict):
            raise TypeError("config must be a dictionary")
        logging_config = config.get("logging", config)
        if not isinstance(logging_config, dict):
            raise ValueError("config['logging'] must be a dictionary")
        logs_path = Path(logging_config.get("logs_path", "logs")).expanduser()
        if not logs_path.is_absolute():
            logs_path = (
                Path(project_root).expanduser() / logs_path
                if project_root
                else Path.cwd() / logs_path
            )
        return cls(
            name=name,
            logs_path=logs_path,
            minio_bucket=minio_bucket,
            minio_prefix=minio_prefix,
            max_bytes=int(logging_config.get("max_bytes", 10 * 1024 * 1024)),
            backup_count=int(logging_config.get("backup_count", 3)),
            queue_size=int(logging_config.get("queue_size", 10_000)),
            level=logging_config.get("level", "INFO"),
        )

    def flush(self) -> list[str]:
        """Upload queued and rotated log files; return uploaded MinIO keys.

        Upload errors are logged to stderr (never through this logger) and the
        corresponding local file is retained for retry.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot flush a closed MinimeLogger")
            self._queue.join()
            self._file_handler.acquire()
            try:
                self._file_handler.doRollover()
                self._file_handler.flush()
            finally:
                self._file_handler.release()

            uploaded: list[str] = []
            for path in sorted(self.logs_path.glob(f"{self.name}.jsonl.*")):
                if not path.is_file():
                    continue
                date_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                object_name = f"logs_{uuid.uuid4().hex[:8]}.jsonl"
                key = "/".join(
                    part
                    for part in (self.minio_prefix, date_stamp, object_name)
                    if part
                )
                try:
                    write_bytes(
                        bucket=self.minio_bucket,
                        key=key,
                        data=path.read_bytes(),
                        content_type="application/x-ndjson",
                    )
                except Exception as exc:
                    logging.getLogger(__name__).error(
                        "Failed to upload log segment %s to MinIO: %s", path, exc
                    )
                    continue
                path.unlink()
                uploaded.append(key)
            return uploaded

    def close(self) -> list[str]:
        """Flush once, detach handlers, and stop the queue listener."""
        with self._lock:
            if self._closed:
                return []
            uploaded = self.flush()
            self._closed = True
            self._listener.stop()
            self.logger.removeHandler(self._handler)
            self._handler.close()
            self._file_handler.close()
            return uploaded

    def __enter__(self) -> MinimeLogger:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
