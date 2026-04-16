"""Tests for minime_utils.minio.exceptions."""

import pytest

from minime_utils.minio.exceptions import (
    MinIOBucketNotFoundError,
    MinIOConnectionError,
    MinIOCredentialError,
    MinIOError,
    MinIOObjectNotFoundError,
    MinIOReadError,
    MinIOWriteError,
)


def test_all_exceptions_inherit_from_minio_error():
    for exc_class in [
        MinIOCredentialError,
        MinIOConnectionError,
        MinIOObjectNotFoundError,
        MinIOBucketNotFoundError,
        MinIOReadError,
        MinIOWriteError,
    ]:
        assert issubclass(exc_class, MinIOError), f"{exc_class} must inherit from MinIOError"


def test_all_exceptions_inherit_from_base_exception():
    assert issubclass(MinIOError, Exception)


def test_exceptions_can_be_raised_and_caught():
    for exc_class in [
        MinIOCredentialError,
        MinIOConnectionError,
        MinIOObjectNotFoundError,
        MinIOBucketNotFoundError,
        MinIOReadError,
        MinIOWriteError,
    ]:
        with pytest.raises(MinIOError):
            raise exc_class("test message")


def test_exception_message_is_preserved():
    exc = MinIOReadError("something went wrong")
    assert str(exc) == "something went wrong"

