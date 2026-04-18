"""Listing, navigation, and management operations for MinIO objects."""

from __future__ import annotations

import logging
from typing import Any

from botocore.exceptions import ClientError

from .client import build_s3_client
from .exceptions import MinIOReadError

logger = logging.getLogger(__name__)


def _normalize_prefix(prefix: str) -> str:
    """Normalize a key prefix to always end with '/' if non-empty."""
    cleaned = prefix.strip().strip("/")
    return f"{cleaned}/" if cleaned else ""


def list_objects(
    *, bucket: str, prefix: str = "", recursive: bool = True
) -> list[str]:
    """
    List all object keys in a bucket under a given prefix.

    Args:
        bucket: Bucket name.
        prefix: Key prefix to filter by (default: "" = all objects).
        recursive: If False, only lists objects at the immediate level (default: True).

    Returns:
        Sorted list of object keys.

    Raises:
        MinIOReadError: If the list operation fails.
    """
    logger.info(
        "Listing objects: bucket=%s prefix=%s recursive=%s", bucket, prefix, recursive
    )
    client = build_s3_client()
    normalized = _normalize_prefix(prefix) if prefix else ""
    keys: list[str] = []
    continuation_token: str | None = None

    try:
        while True:
            params: dict[str, Any] = {"Bucket": bucket}
            if normalized:
                params["Prefix"] = normalized
            if not recursive:
                params["Delimiter"] = "/"
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = client.list_objects_v2(**params)
            keys.extend(obj["Key"] for obj in response.get("Contents", []))

            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

        logger.debug("Found %d objects in %s/%s", len(keys), bucket, normalized)
        return sorted(keys)
    except ClientError as e:
        err = e.response.get("Error", {})
        raise MinIOReadError(
            f"MinIO list error: code={err.get('Code')} bucket={bucket} prefix={prefix}"
        ) from e


def list_directories(*, bucket: str, prefix: str = "") -> list[str]:
    """
    List immediate subdirectory prefixes under a parent prefix.

    Uses the S3 delimiter API so only the direct children are returned,
    not the full recursive tree.

    Args:
        bucket: Bucket name.
        prefix: Parent prefix (default: "" = root).

    Returns:
        Sorted list of subdirectory prefixes (trailing slash stripped).

    Raises:
        MinIOReadError: If the list operation fails.
    """
    logger.info("Listing directories: bucket=%s prefix=%s", bucket, prefix)
    client = build_s3_client()
    normalized = _normalize_prefix(prefix) if prefix else ""
    directories: list[str] = []
    continuation_token: str | None = None

    try:
        while True:
            params: dict[str, Any] = {"Bucket": bucket, "Delimiter": "/"}
            if normalized:
                params["Prefix"] = normalized
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = client.list_objects_v2(**params)
            directories.extend(
                cp["Prefix"].rstrip("/")
                for cp in response.get("CommonPrefixes", [])
                if cp.get("Prefix")
            )

            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

        logger.debug(
            "Found %d directories in %s/%s", len(directories), bucket, normalized
        )
        return sorted(directories)
    except ClientError as e:
        err = e.response.get("Error", {})
        raise MinIOReadError(
            f"MinIO list error: code={err.get('Code')} bucket={bucket} prefix={prefix}"
        ) from e


def list_immediate_children(
    *, bucket: str, prefix: str = ""
) -> tuple[list[str], list[str]]:
    """Return immediate (directories, files) under bucket/prefix.

    Directories are returned without trailing slash.
    Files are object keys directly under the prefix.
    """
    normalized = _normalize_prefix(prefix) if prefix else ""
    directories = list_directories(bucket=bucket, prefix=normalized)
    files = [
        key
        for key in list_objects(bucket=bucket, prefix=normalized, recursive=False)
        if not key.endswith("/")
    ]
    return sorted(directories), sorted(files)


def list_buckets() -> list[str]:
    """List available bucket names in MinIO."""
    client = build_s3_client()
    try:
        response = client.list_buckets()
        buckets = [
            item["Name"] for item in response.get("Buckets", []) if item.get("Name")
        ]
        return sorted(buckets)
    except ClientError as e:
        err = e.response.get("Error", {})
        raise MinIOReadError(f"MinIO list buckets error: code={err.get('Code')}") from e


def object_exists(*, bucket: str, key: str) -> bool:
    """
    Check whether a specific object exists in MinIO.

    Args:
        bucket: Bucket name.
        key: Object key/path.

    Returns:
        True if the object exists, False otherwise.

    Raises:
        MinIOReadError: If the check fails for a reason other than not-found.
    """
    client = build_s3_client()

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return False
        err = e.response.get("Error", {})
        raise MinIOReadError(
            f"MinIO head error: code={err.get('Code')} bucket={bucket} key={key}"
        ) from e


def delete_object(*, bucket: str, key: str) -> None:
    """
    Delete a single object from MinIO.

    Args:
        bucket: Bucket name.
        key: Object key/path.

    Raises:
        MinIOReadError: If the delete operation fails.
    """
    logger.info("Deleting object: bucket=%s key=%s", bucket, key)
    client = build_s3_client()

    try:
        client.delete_object(Bucket=bucket, Key=key)
        logger.debug("Deleted %s/%s", bucket, key)
    except ClientError as e:
        err = e.response.get("Error", {})
        raise MinIOReadError(
            f"MinIO delete error: code={err.get('Code')} bucket={bucket} key={key}"
        ) from e


def delete_prefix(*, bucket: str, prefix: str) -> int:
    """
    Delete all objects under a prefix in MinIO.

    Uses batched deletes (up to 1000 objects per request, as per S3 API limits).

    Args:
        bucket: Bucket name.
        prefix: Prefix to clear (e.g. "logs/2025/").

    Returns:
        Number of objects deleted.

    Raises:
        MinIOReadError: If listing or deletion fails.
    """
    logger.info(
        "Deleting all objects under prefix: bucket=%s prefix=%s", bucket, prefix
    )
    client = build_s3_client()
    keys = list_objects(bucket=bucket, prefix=prefix, recursive=True)

    if not keys:
        logger.debug("No objects to delete under %s/%s", bucket, prefix)
        return 0

    deleted = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        try:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )
        except ClientError as e:
            err = e.response.get("Error", {})
            raise MinIOReadError(
                f"MinIO batch delete error: code={err.get('Code')} bucket={bucket}"
            ) from e
        deleted += len(chunk)

    logger.debug("Deleted %d objects from %s/%s", deleted, bucket, prefix)
    return deleted

