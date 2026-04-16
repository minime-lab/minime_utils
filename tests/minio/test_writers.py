"""Tests for minime_utils.minio.writers."""

import json
import pytest
from unittest.mock import MagicMock, patch

import pandas as pd
from botocore.exceptions import ClientError

from minime_utils.minio.writers import (
    write_bytes,
    write_csv,
    write_dataframe,
    write_json,
    write_text,
)
from minime_utils.minio.exceptions import MinIOWriteError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "test error"}}, "PutObject")


def _put_kwargs(mock_client: MagicMock) -> dict:
    return mock_client.put_object.call_args.kwargs


# ---------------------------------------------------------------------------
# write_bytes
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_bytes_calls_put_object(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    write_bytes(bucket="b", key="k", data=b"hello")

    client.put_object.assert_called_once()
    kwargs = _put_kwargs(client)
    assert kwargs["Bucket"] == "b"
    assert kwargs["Key"] == "k"
    assert kwargs["Body"] == b"hello"


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_bytes_sets_content_type(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    write_bytes(bucket="b", key="k", data=b"x", content_type="image/png")

    assert _put_kwargs(client)["ContentType"] == "image/png"


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_bytes_raises_write_error_on_client_error(mock_build):
    client = MagicMock()
    client.put_object.side_effect = _client_error("AccessDenied")
    mock_build.return_value = client

    with pytest.raises(MinIOWriteError):
        write_bytes(bucket="b", key="k", data=b"x")


# ---------------------------------------------------------------------------
# write_json
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_json_serializes_to_utf8(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    write_json(bucket="b", key="k", data={"foo": "bar"})

    body = _put_kwargs(client)["Body"]
    assert json.loads(body.decode()) == {"foo": "bar"}


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_json_sets_content_type(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    write_json(bucket="b", key="k", data={})

    assert _put_kwargs(client)["ContentType"] == "application/json"


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_json_raises_write_error_on_unserializable(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    with pytest.raises(MinIOWriteError):
        write_json(bucket="b", key="k", data=object())  # not JSON-serializable


# ---------------------------------------------------------------------------
# write_text
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_text_encodes_utf8(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    write_text(bucket="b", key="k", data="héllo")

    body = _put_kwargs(client)["Body"]
    assert body == "héllo".encode("utf-8")


# ---------------------------------------------------------------------------
# write_csv
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_csv_contains_header_and_rows(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    rows = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
    write_csv(bucket="b", key="k", rows=rows)

    body = _put_kwargs(client)["Body"]
    text = body.decode("utf-8")
    assert "name" in text
    assert "Alice" in text
    assert "Bob" in text


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_csv_empty_rows_writes_empty(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    write_csv(bucket="b", key="k", rows=[])

    body = _put_kwargs(client)["Body"]
    assert body == b""


# ---------------------------------------------------------------------------
# write_dataframe
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_dataframe_writes_csv_content(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [10, 20]})
    write_dataframe(bucket="b", key="k", dataframe=df)

    body = _put_kwargs(client)["Body"]
    text = body.decode("utf-8")
    assert "name" in text
    assert "Alice" in text
    assert "score" in text


@patch("minime_utils.minio.writers.build_s3_client")
def test_write_dataframe_excludes_index_by_default(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    df = pd.DataFrame({"val": [1, 2]})
    write_dataframe(bucket="b", key="k", dataframe=df)

    body = _put_kwargs(client)["Body"].decode("utf-8")
    # default pandas index would be 0,1 — should not appear as a column
    assert body.startswith("val")

