"""MinIO client initialization and credential management."""

from __future__ import annotations

import logging
import os
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError

from .exceptions import MinIOConnectionError, MinIOCredentialError

logger = logging.getLogger(__name__)


def get_minio_credentials() -> tuple[str, str, str]:
    """
    Retrieve and validate MinIO credentials from environment variables.

    Returns:
        Tuple of (endpoint, access_key, secret_key).

    Raises:
        MinIOCredentialError: If any required environment variable is missing or empty.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT", "").strip()
    access_key = os.environ.get("MINIO_ACCESS_KEY", "").strip()
    secret_key = os.environ.get("MINIO_SECRET_KEY", "").strip()

    missing = [
        name
        for name, val in [
            ("MINIO_ENDPOINT", endpoint),
            ("MINIO_ACCESS_KEY", access_key),
            ("MINIO_SECRET_KEY", secret_key),
        ]
        if not val
    ]

    if missing:
        raise MinIOCredentialError(
            f"Missing required MinIO environment variables: {', '.join(missing)}"
        )

    return endpoint, access_key, secret_key


def build_s3_client() -> Any:
    """
    Build a boto3 S3 client configured for MinIO.

    Uses S3v4 signature and path-style addressing, both required for MinIO.
    Credentials are read from MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY.

    Returns:
        Configured boto3 S3 client.

    Raises:
        MinIOCredentialError: If credentials are missing.
        MinIOConnectionError: If the client cannot be instantiated.
    """
    endpoint, access_key, secret_key = get_minio_credentials()

    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},  # required for MinIO
            ),
        )
        logger.debug("MinIO S3 client created: endpoint=%s", endpoint)
        return client
    except BotoCoreError as e:
        raise MinIOConnectionError(f"Failed to create MinIO S3 client: {e}") from e

