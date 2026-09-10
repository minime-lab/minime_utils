"""Rotating, once-a-day MinIO prefix backups.

Typical usage::

    from minime_utils.minio import MinioBackup, build_s3_client

    client = build_s3_client()
    result = MinioBackup(client, "mydata", "configs/app/").run()
    print(result)  # BackupResult(action='created', date='20260910', ...)

Backups are stored under ``<backup_bucket>/<source_bucket>/<source_key>/backup_<yyyymmdd>/``
preserving the relative path of every source object. At most ``max_backups``
(default 7) dated folders are kept; the oldest is pruned after each successful
run. Re-running within the same calendar day is a no-op (``action='skipped_today'``).

Note:
    No cross-process locking. Two ``MinioBackup.run()`` calls started within the
    same day on different processes can both create today's dated folder.
    Acceptable for a once-a-day DAG op; not safe for sub-minute parallelism.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_BACKUP_FOLDER_RE = re.compile(r"^backup_(\d{8})$")


@dataclass(frozen=True)
class BackupResult:
    """Outcome of a single ``MinioBackup.run()`` call."""

    action: str  # "created" | "skipped_today"
    date: str  # yyyymmdd of the day's folder, or "" if skipped
    backup_prefix: str  # backups/<src-bucket>/<src-key>/backup_<date>/
    objects_copied: int = 0
    pruned_folders: list[str] = field(default_factory=list)


class MinioBackup:
    """Rotate daily backups of a MinIO/S3 prefix into a destination bucket.

    Args:
        s3_client: A configured boto3 S3 client (use ``build_s3_client()``).
        source_bucket: Bucket that holds the data to back up.
        source_key: Key prefix to back up. ``""`` means the whole bucket.
            Treated as a folder regardless of trailing slash.
        max_backups: Maximum number of dated backup folders to retain
            (default 7). Must be >= 1.
        backup_bucket: Destination bucket name (default ``"backups"``).
            Created if it does not exist.
        clock: Callable returning the current ``date``. Inject for tests.

    Raises:
        ValueError: If ``max_backups < 1``.
    """

    def __init__(
        self,
        s3_client: Any,
        source_bucket: str,
        source_key: str,
        *,
        max_backups: int = 7,
        backup_bucket: str = "backups",
        clock: Callable[[], date] = date.today,
    ) -> None:
        if max_backups < 1:
            raise ValueError(f"max_backups must be >= 1, got {max_backups}")
        if not source_bucket:
            raise ValueError("source_bucket must be a non-empty string")

        self._client = s3_client
        self._source_bucket = source_bucket
        self._source_key = self._normalize_key(source_key)
        self._max_backups = max_backups
        self._backup_bucket = backup_bucket
        self._clock = clock

    # ------------------------------------------------------------------ run

    def run(self) -> BackupResult:
        """Execute one backup attempt and return what happened."""
        today_str = self._clock().strftime("%Y%m%d")
        parent_prefix = self._destination_parent_prefix()
        today_prefix = f"{parent_prefix}backup_{today_str}/"

        # 1) ensure destination bucket exists BEFORE listing in it —
        # otherwise the skip check below raises NoSuchBucket on a fresh setup.
        self._ensure_bucket(self._backup_bucket)

        # 2) skip if today's folder already exists (once-a-day rule)
        if self._folder_exists(self._backup_bucket, today_prefix):
            logger.info(
                "Backup already exists for today: %s/%s — skipping",
                self._backup_bucket,
                today_prefix,
            )
            return BackupResult(
                action="skipped_today",
                date=today_str,
                backup_prefix=today_prefix,
            )

        # 3) copy source prefix → today's folder
        copied = self._copy_prefix(self._source_bucket, self._source_key, today_prefix)

        # 4) prune oldest dated folders beyond max_backups
        pruned = self._prune_oldest(self._backup_bucket, parent_prefix, today_str)

        logger.info(
            "Backup created: %s/%s (%d objects, pruned %d)",
            self._backup_bucket,
            today_prefix,
            copied,
            len(pruned),
        )
        return BackupResult(
            action="created",
            date=today_str,
            backup_prefix=today_prefix,
            objects_copied=copied,
            pruned_folders=pruned,
        )

    # ----------------------------------------------------------- destination

    def _destination_parent_prefix(self) -> str:
        """``backups/<source_bucket>/<source_key>/`` (trailing slash)."""
        cleaned_key = self._source_key.strip("/")
        if cleaned_key:
            return f"{self._source_bucket}/{cleaned_key}/"
        return f"{self._source_bucket}/"

    # ----------------------------------------------------------- copy logic

    def _copy_prefix(
        self, source_bucket: str, source_key: str, dest_prefix: str
    ) -> int:
        """Server-side copy every object under ``source_key`` into ``dest_prefix``.

        Returns the number of objects copied. An empty source prefix is allowed
        and returns 0 without touching the destination bucket.
        """
        if source_key and not source_key.endswith("/"):
            normalized = source_key + "/"
        else:
            normalized = source_key

        paginator = self._client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=source_bucket, Prefix=normalized)
        copied = 0
        for page in pages:
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                rel = src_key[len(normalized) :] if normalized else src_key
                dest_key = f"{dest_prefix}{rel}"
                copy_source = {"Bucket": source_bucket, "Key": src_key}
                self._client.copy_object(
                    Bucket=self._backup_bucket,
                    CopySource=copy_source,
                    Key=dest_key,
                )
                copied += 1
        return copied

    # -------------------------------------------------------------- pruning

    def _prune_oldest(
        self, bucket: str, parent_prefix: str, today_str: str
    ) -> list[str]:
        """Delete dated ``backup_<yyyymmdd>`` folders beyond ``max_backups``.

        The just-created ``backup_<today_str>`` folder is always considered
        the newest. Returns the names (without trailing slash) of deleted
        folders, oldest first.
        """
        dated = self._list_dated_folders(bucket, parent_prefix)
        # today is the newest; the rest are sorted oldest first by name.
        # _BACKUP_FOLDER_RE guarantees the suffix is yyyymmdd → lex sort = chrono sort.
        excess = len(dated) - self._max_backups
        if excess <= 0:
            return []

        # Sort chronologically (oldest first), drop today's folder from the
        # delete candidates, then delete the first `excess` entries.
        to_delete = [d for d in sorted(dated) if d != f"backup_{today_str}"][:excess]

        for folder in to_delete:
            self._delete_folder(bucket, f"{parent_prefix}{folder}/")
        return to_delete

    def _list_dated_folders(self, bucket: str, parent_prefix: str) -> list[str]:
        """Return immediate child names matching ``backup_<yyyymmdd>``."""
        paginator = self._client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=parent_prefix, Delimiter="/")
        folders: list[str] = []
        for page in pages:
            for cp in page.get("CommonPrefixes", []):
                name = cp["Prefix"][len(parent_prefix) :].rstrip("/")
                if _BACKUP_FOLDER_RE.match(name):
                    folders.append(name)
        return folders

    def _delete_folder(self, bucket: str, prefix: str) -> int:
        """Batch-delete every object under ``prefix``."""
        paginator = self._client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        keys: list[str] = []
        for page in pages:
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        if not keys:
            return 0
        # S3 batch delete caps at 1000 per request.
        for i in range(0, len(keys), 1000):
            chunk = keys[i : i + 1000]
            self._client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )
        return len(keys)

    # ----------------------------------------------------------- bucket ops

    def _ensure_bucket(self, bucket: str) -> None:
        try:
            self._client.head_bucket(Bucket=bucket)
            return
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") not in {
                "404",
                "NoSuchBucket",
                "NotFound",
            }:
                raise
        try:
            self._client.create_bucket(Bucket=bucket)
            logger.info("Created backup bucket: %s", bucket)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in {
                "BucketAlreadyExists",
                "BucketAlreadyOwnedByYou",
            }:
                # Another process raced us to create it — fine.
                return
            raise

    def _folder_exists(self, bucket: str, prefix: str) -> bool:
        """True iff at least one object (or sub-prefix) exists under ``prefix``."""
        response = self._client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1
        )
        return bool(response.get("Contents")) or bool(response.get("CommonPrefixes"))

    # --------------------------------------------------------------- utils

    @staticmethod
    def _normalize_key(key: str) -> str:
        """Trim whitespace and trailing slashes; preserve empty string as-is."""
        return key.strip().rstrip("/") if key else ""
