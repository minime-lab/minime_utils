from __future__ import annotations

import logging
from pathlib import Path

import pytest

from minime_utils.apps.logger import MinimeLogger


@pytest.fixture
def logger_factory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    uploads: list[tuple[str, str, bytes, str]] = []

    def write_bytes(*, bucket: str, key: str, data: bytes, content_type: str) -> None:
        uploads.append((bucket, key, data, content_type))

    monkeypatch.setattr("minime_utils.apps.logger.write_bytes", write_bytes)

    def create(**kwargs) -> MinimeLogger:
        return MinimeLogger(
            name=f"test_logger_{id(kwargs)}",
            logs_path=tmp_path,
            minio_bucket="logs-bucket",
            minio_prefix="apps/example/logs",
            **kwargs,
        )

    return create, uploads


def test_from_config_uses_defaults_and_relative_project_path(tmp_path: Path) -> None:
    logger = MinimeLogger.from_config(
        {},
        name="config-defaults-test",
        minio_bucket="logs-bucket",
        minio_prefix="apps/example/logs",
        project_root=tmp_path,
    )
    try:
        assert logger.logs_path == tmp_path / "logs"
        assert logger._file_handler.maxBytes == 10 * 1024 * 1024
        assert logger._file_handler.backupCount == 3
        assert logger._queue.maxsize == 10_000
        assert logger.logger.level == logging.INFO
    finally:
        logger.close()


def test_from_config_reads_logging_table(tmp_path: Path) -> None:
    logger = MinimeLogger.from_config(
        {
            "logging": {
                "logs_path": "var/app-logs",
                "max_bytes": 512,
                "backup_count": 2,
                "queue_size": 20,
                "level": "DEBUG",
            }
        },
        name="config-values-test",
        minio_bucket="logs-bucket",
        minio_prefix="apps/example/logs",
        project_root=tmp_path,
    )
    try:
        assert logger.logs_path == tmp_path / "var" / "app-logs"
        assert logger._file_handler.maxBytes == 512
        assert logger._file_handler.backupCount == 2
        assert logger._queue.maxsize == 20
        assert logger.logger.level == logging.DEBUG
    finally:
        logger.close()


def test_flush_uploads_local_segment_and_removes_it(logger_factory) -> None:
    create, uploads = logger_factory
    logger = create()
    logger.logger.info("request complete", extra={"request_id": "req-1"})

    keys = logger.flush()

    assert len(keys) == 1
    assert keys[0].startswith("apps/example/logs/")
    assert len(uploads) == 1
    bucket, key, payload, content_type = uploads[0]
    assert bucket == "logs-bucket"
    assert key == keys[0]
    assert b'"message": "request complete"' in payload
    assert b'"request_id": "req-1"' in payload
    assert content_type == "application/x-ndjson"
    assert not list(logger.logs_path.glob(f"{logger.name}.jsonl.*"))
    logger.close()


def test_each_flush_uses_unique_date_and_uuid_object_key(logger_factory) -> None:
    create, uploads = logger_factory
    logger = create()
    try:
        logger.logger.info("first segment")
        first_key = logger.flush()[0]
        logger.logger.info("second segment")
        second_key = logger.flush()[0]

        assert first_key != second_key
        for key in (first_key, second_key):
            date_stamp, filename = key.rsplit("/", 2)[-2:]
            date_parts = date_stamp.split("-")
            assert len(date_parts) == 3
            assert all(part.isdigit() for part in date_parts)
            assert filename.startswith("logs_")
            assert filename.endswith(".jsonl")
            run_uuid = filename.removeprefix("logs_").removesuffix(".jsonl")
            assert len(run_uuid) == 8
        assert len(uploads) == 2
    finally:
        logger.close()


def test_full_file_rotation_and_upload(logger_factory) -> None:
    create, uploads = logger_factory
    logger = create(max_bytes=1, backup_count=3)
    logger.logger.warning("large enough to rotate")
    logger.logger.warning("second record triggers rotation")
    logger._queue.join()

    assert list(logger.logs_path.glob(f"{logger.name}.jsonl.*"))
    logger.flush()

    assert uploads
    assert not list(logger.logs_path.glob(f"{logger.name}.jsonl.*"))
    logger.close()


def test_failed_upload_keeps_local_segment_for_retry(logger_factory) -> None:
    create, uploads = logger_factory
    logger = create()
    logger.logger.info("retry me")
    logger._queue.join()

    def fail_upload(**_kwargs) -> None:
        raise OSError("MinIO unavailable")

    import minime_utils.apps.logger as logger_module

    original = logger_module.write_bytes
    logger_module.write_bytes = fail_upload
    try:
        assert logger.flush() == []
    finally:
        logger_module.write_bytes = original

    assert list(logger.logs_path.glob(f"{logger.name}.jsonl.*"))
    assert logger.flush()
    assert uploads
    logger.close()


def test_invalid_config_rejected() -> None:
    with pytest.raises(TypeError):
        MinimeLogger.from_config([], name="invalid", minio_bucket="b", minio_prefix="p")
    with pytest.raises(ValueError):
        MinimeLogger.from_config(
            {"logging": []}, name="invalid", minio_bucket="b", minio_prefix="p"
        )
