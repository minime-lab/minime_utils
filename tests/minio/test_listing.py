"""Tests for minime_utils.minio.listing."""

import pytest
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError

from minime_utils.minio.listing import (
    _normalize_prefix,
    delete_object,
    delete_prefix,
    list_buckets,
    list_directories,
    list_immediate_children,
    list_objects,
    object_exists,
)
from minime_utils.minio.exceptions import MinIOReadError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _client_error(code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}}, "ListObjectsV2"
    )


def _list_response(keys: list[str], truncated: bool = False) -> dict:
    return {
        "Contents": [{"Key": k} for k in keys],
        "IsTruncated": truncated,
    }


def _prefix_response(prefixes: list[str], truncated: bool = False) -> dict:
    return {
        "CommonPrefixes": [{"Prefix": p} for p in prefixes],
        "Contents": [],
        "IsTruncated": truncated,
    }


# ---------------------------------------------------------------------------
# _normalize_prefix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("logs", "logs/"),
        ("logs/", "logs/"),
        ("/logs/", "logs/"),
        ("a/b/c", "a/b/c/"),
        ("", ""),
        ("   ", ""),
    ],
)
def test_normalize_prefix(raw, expected):
    assert _normalize_prefix(raw) == expected


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_objects_returns_sorted_keys(mock_build):
    client = MagicMock()
    client.list_objects_v2.return_value = _list_response(["b.txt", "a.txt", "c.txt"])
    mock_build.return_value = client

    result = list_objects(bucket="b", prefix="logs")

    assert result == ["a.txt", "b.txt", "c.txt"]


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_objects_handles_pagination(mock_build):
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {**_list_response(["a.txt"]), "IsTruncated": True, "NextContinuationToken": "tok1"},
        _list_response(["b.txt"]),
    ]
    mock_build.return_value = client

    result = list_objects(bucket="b", prefix="p")

    assert result == ["a.txt", "b.txt"]
    assert client.list_objects_v2.call_count == 2


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_objects_empty_bucket_returns_empty(mock_build):
    client = MagicMock()
    client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}
    mock_build.return_value = client

    assert list_objects(bucket="b") == []


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_objects_raises_read_error_on_client_error(mock_build):
    client = MagicMock()
    client.list_objects_v2.side_effect = _client_error("AccessDenied")
    mock_build.return_value = client

    with pytest.raises(MinIOReadError):
        list_objects(bucket="b")


# ---------------------------------------------------------------------------
# list_directories
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_directories_returns_stripped_prefixes(mock_build):
    client = MagicMock()
    client.list_objects_v2.return_value = _prefix_response(
        ["logs/2025/", "logs/2026/"]
    )
    mock_build.return_value = client

    result = list_directories(bucket="b", prefix="logs")

    assert result == ["logs/2025", "logs/2026"]


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_directories_empty_returns_empty(mock_build):
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "CommonPrefixes": [],
        "IsTruncated": False,
    }
    mock_build.return_value = client

    assert list_directories(bucket="b") == []


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_immediate_children_returns_directories_and_files(mock_build):
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        _prefix_response(["logs/2025/", "logs/2026/"]),
        _list_response(["logs/a.txt", "logs/z.txt"]),
    ]
    mock_build.return_value = client

    directories, files = list_immediate_children(bucket="b", prefix="logs")

    assert directories == ["logs/2025", "logs/2026"]
    assert files == ["logs/a.txt", "logs/z.txt"]
    assert client.list_objects_v2.call_count == 2


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_buckets_returns_sorted_names(mock_build):
    client = MagicMock()
    client.list_buckets.return_value = {"Buckets": [{"Name": "zeta"}, {"Name": "alpha"}]}
    mock_build.return_value = client

    assert list_buckets() == ["alpha", "zeta"]


@patch("minime_utils.minio.listing.build_s3_client")
def test_list_buckets_raises_on_client_error(mock_build):
    client = MagicMock()
    client.list_buckets.side_effect = _client_error("AccessDenied")
    mock_build.return_value = client

    with pytest.raises(MinIOReadError):
        list_buckets()


# ---------------------------------------------------------------------------
# object_exists
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.listing.build_s3_client")
def test_object_exists_returns_true_when_found(mock_build):
    client = MagicMock()
    client.head_object.return_value = {}
    mock_build.return_value = client

    assert object_exists(bucket="b", key="k") is True


@patch("minime_utils.minio.listing.build_s3_client")
def test_object_exists_returns_false_when_not_found(mock_build):
    client = MagicMock()
    client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )
    mock_build.return_value = client

    assert object_exists(bucket="b", key="missing") is False


@patch("minime_utils.minio.listing.build_s3_client")
def test_object_exists_raises_on_other_error(mock_build):
    client = MagicMock()
    client.head_object.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
    )
    mock_build.return_value = client

    with pytest.raises(MinIOReadError):
        object_exists(bucket="b", key="k")


# ---------------------------------------------------------------------------
# delete_object
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.listing.build_s3_client")
def test_delete_object_calls_delete(mock_build):
    client = MagicMock()
    mock_build.return_value = client

    delete_object(bucket="b", key="k")

    client.delete_object.assert_called_once_with(Bucket="b", Key="k")


@patch("minime_utils.minio.listing.build_s3_client")
def test_delete_object_raises_on_error(mock_build):
    client = MagicMock()
    client.delete_object.side_effect = _client_error("AccessDenied")
    mock_build.return_value = client

    with pytest.raises(MinIOReadError):
        delete_object(bucket="b", key="k")


# ---------------------------------------------------------------------------
# delete_prefix
# ---------------------------------------------------------------------------


@patch("minime_utils.minio.listing.build_s3_client")
def test_delete_prefix_returns_count(mock_build):
    client = MagicMock()
    client.list_objects_v2.return_value = _list_response(["a.txt", "b.txt", "c.txt"])
    mock_build.return_value = client

    count = delete_prefix(bucket="b", prefix="logs")

    assert count == 3
    client.delete_objects.assert_called_once()


@patch("minime_utils.minio.listing.build_s3_client")
def test_delete_prefix_empty_prefix_returns_zero(mock_build):
    client = MagicMock()
    client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}
    mock_build.return_value = client

    assert delete_prefix(bucket="b", prefix="empty") == 0
    client.delete_objects.assert_not_called()

