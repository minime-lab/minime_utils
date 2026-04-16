"""Write operations for MinIO objects."""

from __future__ import annotations

import csv
import io
import json
import logging
from typing import Any

import pandas as pd
from botocore.exceptions import ClientError

from .client import build_s3_client
from .exceptions import MinIOWriteError

logger = logging.getLogger(__name__)


def write_bytes(
    *,
    bucket: str,
    key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    """
    Write raw bytes to a MinIO object.

    Args:
        bucket: Bucket name.
        key: Object key/path.
        data: Raw bytes to write.
        content_type: MIME type of the object (default: application/octet-stream).

    Raises:
        MinIOWriteError: If the write operation fails.
    """
    logger.info(
        "Writing bytes to MinIO: bucket=%s key=%s size=%d", bucket, key, len(data)
    )
    client = build_s3_client()

    try:
        client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        logger.debug("Wrote %d bytes to %s/%s", len(data), bucket, key)
    except ClientError as e:
        err = e.response.get("Error", {})
        raise MinIOWriteError(
            f"MinIO write error: code={err.get('Code')} bucket={bucket} key={key}"
        ) from e


def write_json(
    *,
    bucket: str,
    key: str,
    data: Any,
    indent: int = 2,
    content_type: str = "application/json",
) -> None:
    """
    Serialize and write JSON to a MinIO object.

    Args:
        bucket: Bucket name.
        key: Object key/path.
        data: Value to serialize as JSON.
        indent: JSON indentation level (default: 2).
        content_type: MIME type of the object.

    Raises:
        MinIOWriteError: If serialization or write fails.
    """
    logger.info("Writing JSON to MinIO: bucket=%s key=%s", bucket, key)

    try:
        encoded = json.dumps(data, indent=indent).encode("utf-8")
    except (TypeError, ValueError) as e:
        raise MinIOWriteError(
            f"Failed to serialize JSON for {bucket}/{key}: {e}"
        ) from e

    write_bytes(bucket=bucket, key=key, data=encoded, content_type=content_type)


def write_text(
    *,
    bucket: str,
    key: str,
    data: str,
    encoding: str = "utf-8",
    content_type: str = "text/plain; charset=utf-8",
) -> None:
    """
    Encode and write text to a MinIO object.

    Args:
        bucket: Bucket name.
        key: Object key/path.
        data: Text string to write.
        encoding: Text encoding (default: utf-8).
        content_type: MIME type of the object.

    Raises:
        MinIOWriteError: If encoding or write fails.
    """
    logger.info("Writing text to MinIO: bucket=%s key=%s", bucket, key)

    try:
        encoded = data.encode(encoding)
    except UnicodeEncodeError as e:
        raise MinIOWriteError(
            f"Failed to encode text for {bucket}/{key} with encoding={encoding}: {e}"
        ) from e

    write_bytes(bucket=bucket, key=key, data=encoded, content_type=content_type)


def write_csv(
    *,
    bucket: str,
    key: str,
    rows: list[dict[str, Any]],
    encoding: str = "utf-8",
    content_type: str = "text/csv; charset=utf-8",
    **csv_kwargs: Any,
) -> None:
    """
    Serialize a list of dicts as CSV and write to MinIO.

    All rows must share the same keys; column order is taken from the first row.

    Args:
        bucket: Bucket name.
        key: Object key/path.
        rows: List of row dictionaries.
        encoding: Text encoding (default: utf-8).
        content_type: MIME type of the object.
        **csv_kwargs: Extra arguments forwarded to csv.DictWriter.

    Raises:
        MinIOWriteError: If CSV serialization or write fails.
    """
    logger.info(
        "Writing CSV to MinIO: bucket=%s key=%s rows=%d", bucket, key, len(rows)
    )

    if not rows:
        logger.warning("Writing empty CSV to %s/%s", bucket, key)
        write_text(
            bucket=bucket, key=key, data="", encoding=encoding, content_type=content_type
        )
        return

    try:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), **csv_kwargs)
        writer.writeheader()
        writer.writerows(rows)
        write_text(
            bucket=bucket,
            key=key,
            data=buf.getvalue(),
            encoding=encoding,
            content_type=content_type,
        )
    except MinIOWriteError:
        raise
    except Exception as e:
        raise MinIOWriteError(
            f"Failed to serialize CSV for {bucket}/{key}: {e}"
        ) from e


def write_dataframe(
    *,
    bucket: str,
    key: str,
    dataframe: pd.DataFrame,
    include_index: bool = False,
    encoding: str = "utf-8",
    content_type: str = "text/csv; charset=utf-8",
) -> None:
    """
    Serialize a pandas DataFrame as CSV and write to MinIO.

    Args:
        bucket: Bucket name.
        key: Object key/path.
        dataframe: DataFrame to write.
        include_index: Whether to include the DataFrame index (default: False).
        encoding: Text encoding (default: utf-8).
        content_type: MIME type of the object.

    Raises:
        MinIOWriteError: If serialization or write fails.
    """
    logger.info(
        "Writing DataFrame to MinIO: bucket=%s key=%s rows=%d",
        bucket,
        key,
        len(dataframe),
    )

    try:
        csv_text = dataframe.to_csv(index=include_index)
        write_text(
            bucket=bucket,
            key=key,
            data=csv_text,
            encoding=encoding,
            content_type=content_type,
        )
    except MinIOWriteError:
        raise
    except Exception as e:
        raise MinIOWriteError(
            f"Failed to serialize DataFrame for {bucket}/{key}: {e}"
        ) from e

