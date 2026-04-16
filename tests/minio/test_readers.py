"""Tests for minime_utils.minio.readers."""

import json
import pytest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from minime_utils.minio.readers import read_bytes, read_csv, read_json, read_text
from minime_utils.minio.exceptions import MinIOObjectNotFoundError, MinIOReadError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "test error"}}, "GetObject")


def _mock_client(content: bytes) -> MagicMock:
    client = MagicMock()
    client.get_object.return_value = {"Body": MagicMock(read=lambda: content)}
    return client


# ---------------------------------------------------------------------------
# read_bytes
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_bytes_returns_content(mock_build):
    mock_build.return_value = _mock_client(b"hello world")
    assert read_bytes(bucket="b", key="k") == b"hello world"


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_bytes_raises_not_found_on_no_such_key(mock_build):
    client = MagicMock()
    client.get_object.side_effect = _client_error("NoSuchKey")
    mock_build.return_value = client

    with pytest.raises(MinIOObjectNotFoundError):
        read_bytes(bucket="b", key="missing")


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_bytes_raises_read_error_on_other_client_error(mock_build):
    client = MagicMock()
    client.get_object.side_effect = _client_error("AccessDenied")
    mock_build.return_value = client

    with pytest.raises(MinIOReadError):
        read_bytes(bucket="b", key="k")


# ---------------------------------------------------------------------------
# read_json
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_json_deserializes_content(mock_build):
    payload = {"x": 1, "y": [2, 3]}
    mock_build.return_value = _mock_client(json.dumps(payload).encode())

    assert read_json(bucket="b", key="k") == payload


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_json_raises_read_error_on_invalid_json(mock_build):
    mock_build.return_value = _mock_client(b"this is not json {{")

    with pytest.raises(MinIOReadError):
        read_json(bucket="b", key="k")


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_json_propagates_not_found(mock_build):
    client = MagicMock()
    client.get_object.side_effect = _client_error("NoSuchKey")
    mock_build.return_value = client

    with pytest.raises(MinIOObjectNotFoundError):
        read_json(bucket="b", key="missing")


# ---------------------------------------------------------------------------
# read_text
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_text_decodes_utf8(mock_build):
    mock_build.return_value = _mock_client("héllo wörld".encode("utf-8"))

    assert read_text(bucket="b", key="k") == "héllo wörld"


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_text_raises_read_error_on_bad_encoding(mock_build):
    mock_build.return_value = _mock_client(b"\xff\xfe")  # invalid utf-8

    with pytest.raises(MinIOReadError):
        read_text(bucket="b", key="k", encoding="ascii")


# ---------------------------------------------------------------------------
# read_csv
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_csv_returns_rows(mock_build):
    csv_data = b"name,age\nAlice,30\nBob,25"
    mock_build.return_value = _mock_client(csv_data)

    rows = read_csv(bucket="b", key="k")

    assert rows == [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_csv_empty_file_returns_empty_list(mock_build):
    mock_build.return_value = _mock_client(b"")

    rows = read_csv(bucket="b", key="k")

    assert rows == []


@patch("minime_utils.minio.readers.build_s3_client")
def test_read_csv_propagates_not_found(mock_build):
    client = MagicMock()
    client.get_object.side_effect = _client_error("NoSuchKey")
    mock_build.return_value = client

    with pytest.raises(MinIOObjectNotFoundError):
        read_csv(bucket="b", key="missing")

