"""Tests for minime_utils.minio.client."""

import pytest
from unittest.mock import MagicMock, patch

from minime_utils.minio.client import build_s3_client, get_minio_credentials
from minime_utils.minio.exceptions import MinIOCredentialError


# ---------------------------------------------------------------------------
# get_minio_credentials
# ---------------------------------------------------------------------------


def test_get_credentials_raises_when_all_missing(monkeypatch):
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)

    with pytest.raises(MinIOCredentialError) as exc_info:
        get_minio_credentials()

    msg = str(exc_info.value)
    assert "MINIO_ENDPOINT" in msg
    assert "MINIO_ACCESS_KEY" in msg
    assert "MINIO_SECRET_KEY" in msg


def test_get_credentials_raises_when_one_missing(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "key")
    monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)

    with pytest.raises(MinIOCredentialError) as exc_info:
        get_minio_credentials()

    assert "MINIO_SECRET_KEY" in str(exc_info.value)


def test_get_credentials_raises_when_empty_string(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "  ")  # whitespace only
    monkeypatch.setenv("MINIO_ACCESS_KEY", "key")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")

    with pytest.raises(MinIOCredentialError) as exc_info:
        get_minio_credentials()

    assert "MINIO_ENDPOINT" in str(exc_info.value)


def test_get_credentials_returns_stripped_values(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "  http://minio:9000  ")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "  mykey  ")
    monkeypatch.setenv("MINIO_SECRET_KEY", "  mysecret  ")

    endpoint, access_key, secret_key = get_minio_credentials()

    assert endpoint == "http://minio:9000"
    assert access_key == "mykey"
    assert secret_key == "mysecret"


# ---------------------------------------------------------------------------
# build_s3_client
# ---------------------------------------------------------------------------


def test_build_s3_client_passes_correct_config(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "key")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")

    captured: dict = {}

    def fake_boto3_client(service_name: str, **kwargs):
        captured["service_name"] = service_name
        captured.update(kwargs)
        return MagicMock()

    with patch("minime_utils.minio.client.boto3.client", fake_boto3_client):
        build_s3_client()

    assert captured["service_name"] == "s3"
    assert captured["endpoint_url"] == "http://minio:9000"
    assert captured["aws_access_key_id"] == "key"
    assert captured["aws_secret_access_key"] == "secret"
    assert captured["region_name"] == "us-east-1"
    # path-style addressing is critical for MinIO
    assert captured["config"].s3 == {"addressing_style": "path"}


def test_build_s3_client_raises_when_credentials_missing(monkeypatch):
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)

    with pytest.raises(MinIOCredentialError):
        build_s3_client()

