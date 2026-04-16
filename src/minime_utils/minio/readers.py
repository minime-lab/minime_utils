"""Read operations for MinIO objects."""

from __future__ import annotations

import csv
import io
import json
import logging
from typing import Any

from botocore.exceptions import ClientError

from .client import build_s3_client
from .exceptions import MinIOObjectNotFoundError, MinIOReadError

logger = logging.getLogger(__name__)


def read_bytes(*, bucket: str, key: str) -> bytes:
    """
    Read raw bytes from a MinIO object.

    Args:
        bucket: Bucket name.
        key: Object key/path.

    Returns:
        Raw bytes content.

    Raises:
        MinIOObjectNotFoundError: If the object does not exist.
        MinIOReadError: If the read operation fails.
    """
    logger.info("Reading bytes from MinIO: bucket=%s key=%s", bucket, key)
    client = build_s3_client()

    try:
        response = client.get_object(Bucket=bucket, Key=key)
        content: bytes = response["Body"].read()
        logger.debug("Read %d bytes from %s/%s", len(content), bucket, key)
        return content
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "NoSuchKey":
            raise MinIOObjectNotFoundError(f"Object not found: {bucket}/{key}") from e
        raise MinIOReadError(
            f"MinIO read error: code={code} bucket={bucket} key={key}"
        ) from e


def read_json(*, bucket: str, key: str) -> Any:
    """
    Read and deserialize a JSON object from MinIO.

    Args:
        bucket: Bucket name.
        key: Object key/path.

    Returns:
        Deserialized JSON value.

    Raises:
        MinIOObjectNotFoundError: If the object does not exist.
        MinIOReadError: If the read or JSON parsing fails.
    """
    logger.info("Reading JSON from MinIO: bucket=%s key=%s", bucket, key)

    try:
        content = read_bytes(bucket=bucket, key=key)
        return json.loads(content.decode("utf-8"))
    except (MinIOObjectNotFoundError, MinIOReadError):
        raise
    except json.JSONDecodeError as e:
        raise MinIOReadError(f"Invalid JSON in {bucket}/{key}: {e}") from e


def read_text(*, bucket: str, key: str, encoding: str = "utf-8") -> str:
    """
    Read and decode text from a MinIO object.

    Args:
        bucket: Bucket name.
        key: Object key/path.
        encoding: Text encoding (default: utf-8).

    Returns:
        Decoded text content.

    Raises:
        MinIOObjectNotFoundError: If the object does not exist.
        MinIOReadError: If the read or decode fails.
    """
    logger.info("Reading text from MinIO: bucket=%s key=%s", bucket, key)

    try:
        content = read_bytes(bucket=bucket, key=key)
        return content.decode(encoding)
    except (MinIOObjectNotFoundError, MinIOReadError):
        raise
    except UnicodeDecodeError as e:
        raise MinIOReadError(
            f"Failed to decode {bucket}/{key} with encoding={encoding}: {e}"
        ) from e


def read_csv(
    *, bucket: str, key: str, encoding: str = "utf-8", **csv_kwargs: Any
) -> list[dict[str, Any]]:
    """
    Read and parse CSV from a MinIO object.

    Args:
        bucket: Bucket name.
        key: Object key/path.
        encoding: Text encoding (default: utf-8).
        **csv_kwargs: Extra arguments forwarded to csv.DictReader.

    Returns:
        List of row dictionaries.

    Raises:
        MinIOObjectNotFoundError: If the object does not exist.
        MinIOReadError: If the read or CSV parsing fails.
    """
    logger.info("Reading CSV from MinIO: bucket=%s key=%s", bucket, key)

    try:
        text = read_text(bucket=bucket, key=key, encoding=encoding)
        rows = list(csv.DictReader(io.StringIO(text), **csv_kwargs))
        logger.debug("Parsed %d rows from CSV %s/%s", len(rows), bucket, key)
        return rows
    except (MinIOObjectNotFoundError, MinIOReadError):
        raise
    except Exception as e:
        raise MinIOReadError(f"Failed to parse CSV from {bucket}/{key}: {e}") from e

