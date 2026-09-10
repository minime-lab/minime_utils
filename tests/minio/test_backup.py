"""Tests for minime_utils.minio.backup."""

from datetime import date
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from minime_utils.minio.backup import BackupResult, MinioBackup


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, op: str = "HeadObject") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "test"}}, op)


def _make_paginator(pages: list[dict]) -> MagicMock:
    """Return a MagicMock get_paginator whose .paginate(...) yields ``pages``."""
    paginator = MagicMock()
    paginator.paginate.return_value = iter(pages)
    return paginator


def _page(
    *, contents: list[str] | None = None, prefixes: list[str] | None = None
) -> dict:
    page: dict = {"Contents": [], "CommonPrefixes": []}
    if contents:
        page["Contents"] = [{"Key": k} for k in contents]
    if prefixes:
        page["CommonPrefixes"] = [{"Prefix": p} for p in prefixes]
    return page


def _fixed_clock(year: int, month: int, day: int):
    def clock() -> date:
        return date(year, month, day)

    return clock


def _make_client(*, head_bucket_codes: list[str] | None = None) -> MagicMock:
    """Build a client mock with sensible defaults."""
    client = MagicMock()
    # head_bucket: by default returns success (no error)
    if head_bucket_codes:
        client.head_bucket.side_effect = [
            _client_error(code, "HeadBucket") for code in head_bucket_codes
        ]
    return client


# ---------------------------------------------------------------------------
# construction
# ---------------------------------------------------------------------------


def test_init_rejects_max_backups_below_one():
    client = MagicMock()
    with pytest.raises(ValueError, match="max_backups"):
        MinioBackup(client, "src", "", max_backups=0)


def test_init_rejects_empty_source_bucket():
    client = MagicMock()
    with pytest.raises(ValueError, match="source_bucket"):
        MinioBackup(client, "", "key")


def test_init_strips_trailing_slash_on_source_key():
    client = MagicMock()
    b = MinioBackup(client, "src", "configs/app/")
    assert b._source_key == "configs/app"


def test_init_keeps_empty_source_key_for_whole_bucket():
    client = MagicMock()
    b = MinioBackup(client, "src", "")
    assert b._source_key == ""


# ---------------------------------------------------------------------------
# destination prefix
# ---------------------------------------------------------------------------


def test_destination_prefix_for_prefixed_source():
    client = MagicMock()
    b = MinioBackup(client, "mydata", "configs/app")
    assert b._destination_parent_prefix() == "mydata/configs/app/"


def test_destination_prefix_for_whole_bucket():
    client = MagicMock()
    b = MinioBackup(client, "mydata", "")
    assert b._destination_parent_prefix() == "mydata/"


# ---------------------------------------------------------------------------
# run() — once-a-day rule
# ---------------------------------------------------------------------------


def test_run_skips_when_today_folder_already_exists():
    client = MagicMock()
    # _folder_exists does a list_objects_v2 with Delimiter; here it returns
    # a non-empty CommonPrefixes → folder exists → skip.
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [{"Prefix": "mydata/cfg/backup_20260910/"}],
    }

    result = MinioBackup(
        client,
        "mydata",
        "cfg",
        clock=_fixed_clock(2026, 9, 10),
    ).run()

    assert isinstance(result, BackupResult)
    assert result.action == "skipped_today"
    assert result.date == "20260910"
    assert result.backup_prefix == "mydata/cfg/backup_20260910/"
    assert result.objects_copied == 0
    # No copy_object, no create_bucket, no delete_objects should have been called.
    client.copy_object.assert_not_called()
    client.create_bucket.assert_not_called()


def test_run_second_call_same_day_is_noop():
    client = MagicMock()
    # First call: _folder_exists(bucket=backups, prefix=src/k/backup_20260910/) → empty
    # Second call: _folder_exists → folder present (CommonPrefixes non-empty)
    client.list_objects_v2.side_effect = [
        {"Contents": [], "CommonPrefixes": []},
        {
            "Contents": [],
            "CommonPrefixes": [{"Prefix": "src/k/backup_20260910/"}],
        },
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),  # copy
        _make_paginator([_page(prefixes=[])]),  # list_dated
    ]

    clock = _fixed_clock(2026, 9, 10)
    b = MinioBackup(client, "src", "k", clock=clock)

    first = b.run()
    second = b.run()

    assert first.action == "created"
    assert first.objects_copied == 0
    assert second.action == "skipped_today"
    assert second.date == "20260910"


# ---------------------------------------------------------------------------
# run() — full happy path
# ---------------------------------------------------------------------------


def test_run_copies_objects_and_prunes_oldest():
    client = MagicMock()

    # Source bucket "src" with two objects under prefix "data/".
    src_objects = ["data/a.json", "data/sub/b.json"]

    # Existing dated folders (oldest first by yyyymmdd sort) — 8 of them,
    # we will keep the 7 most recent.
    existing_folders = [
        "src/data/backup_20260903/",
        "src/data/backup_20260904/",
        "src/data/backup_20260905/",
        "src/data/backup_20260906/",
        "src/data/backup_20260907/",
        "src/data/backup_20260908/",
        "src/data/backup_20260909/",
        "src/data/backup_20260910/",  # idempotency: _folder_exists on backups/
    ]

    # _folder_exists on backups/ for today's prefix → empty.
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }

    copy_paginator = _make_paginator([_page(contents=src_objects)])
    dated_paginator = _make_paginator([_page(prefixes=existing_folders)])
    # _delete_folder lists under "src/data/backup_20260903/" and finds nothing.
    empty_paginator = _make_paginator([_page(contents=[])])

    client.get_paginator.side_effect = [
        copy_paginator,  # _copy_prefix
        dated_paginator,  # _list_dated_folders
        empty_paginator,  # _delete_folder
    ]

    clock = _fixed_clock(2026, 9, 11)  # one day after the newest
    b = MinioBackup(client, "src", "data", max_backups=7, clock=clock)
    result = b.run()

    assert result.action == "created"
    assert result.date == "20260911"
    assert result.backup_prefix == "src/data/backup_20260911/"
    assert result.objects_copied == 2

    # Two copy_object calls with the right CopySource and Key.
    copy_calls = client.copy_object.call_args_list
    assert len(copy_calls) == 2
    keys_written = sorted(call.kwargs["Key"] for call in copy_calls)
    assert keys_written == [
        "src/data/backup_20260911/a.json",
        "src/data/backup_20260911/sub/b.json",
    ]
    copy_sources = sorted(
        (call.kwargs["CopySource"]["Bucket"], call.kwargs["CopySource"]["Key"])
        for call in copy_calls
    )
    assert copy_sources == [
        ("src", "data/a.json"),
        ("src", "data/sub/b.json"),
    ]

    # Oldest folder pruned.
    assert result.pruned_folders == ["backup_20260903"]


def test_run_creates_backup_bucket_if_missing():
    client = MagicMock()
    # head_bucket: first call raises 404, then create_bucket is called.
    client.head_bucket.side_effect = _client_error("404", "HeadBucket")
    client.list_objects_v2.side_effect = [
        # _folder_exists check → not present
        {"Contents": [], "CommonPrefixes": []},
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),  # copy
        _make_paginator([_page(prefixes=[])]),  # list_dated
    ]

    MinioBackup(client, "src", "k", clock=_fixed_clock(2026, 9, 10)).run()

    client.head_bucket.assert_called_once_with(Bucket="backups")
    client.create_bucket.assert_called_once_with(Bucket="backups")


def test_run_does_not_recreate_existing_bucket():
    client = MagicMock()
    # head_bucket succeeds (no exception) → no create_bucket.
    client.list_objects_v2.side_effect = [
        {"Contents": [], "CommonPrefixes": []},
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=[])]),
    ]

    MinioBackup(client, "src", "k", clock=_fixed_clock(2026, 9, 10)).run()

    client.create_bucket.assert_not_called()


def test_run_tolerates_bucket_already_exists_race():
    client = MagicMock()
    client.head_bucket.side_effect = _client_error("404", "HeadBucket")
    client.create_bucket.side_effect = _client_error(
        "BucketAlreadyOwnedByYou", "CreateBucket"
    )
    client.list_objects_v2.side_effect = [
        {"Contents": [], "CommonPrefixes": []},
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=[])]),
    ]

    # Should not raise.
    MinioBackup(client, "src", "k", clock=_fixed_clock(2026, 9, 10)).run()


def test_run_creates_bucket_before_folder_check():
    """Regression: ensure_bucket must run BEFORE _folder_exists,
    otherwise a fresh ``backups`` bucket raises NoSuchBucket from the
    list-objects check before we ever get a chance to create it.
    """
    client = MagicMock()
    # head_bucket says the bucket is missing → create_bucket should be called.
    client.head_bucket.side_effect = _client_error("404", "HeadBucket")
    # After creation, _folder_exists returns "no folder for today".
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),  # copy
        _make_paginator([_page(prefixes=[])]),  # list_dated
    ]

    result = MinioBackup(client, "src", "k", clock=_fixed_clock(2026, 9, 10)).run()

    # Verify the ordering: head_bucket + create_bucket happen before the
    # first list_objects_v2 (which is _folder_exists).
    assert result.action == "created"
    client.head_bucket.assert_called_once_with(Bucket="backups")
    client.create_bucket.assert_called_once_with(Bucket="backups")
    # The very first list call must be the folder-exists check, not a copy.
    first_list_kwargs = client.list_objects_v2.call_args_list[0].kwargs
    assert first_list_kwargs["Bucket"] == "backups"
    assert first_list_kwargs["Delimiter"] == "/"
    assert first_list_kwargs["MaxKeys"] == 1


# ---------------------------------------------------------------------------
# run() — pruning edge cases
# ---------------------------------------------------------------------------


def test_run_with_max_backups_three_prunes_to_three():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"Contents": [], "CommonPrefixes": []},
    ]
    existing = [
        "src/k/backup_20260901/",
        "src/k/backup_20260902/",
        "src/k/backup_20260903/",
        "src/k/backup_20260904/",
        "src/k/backup_20260905/",
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),  # copy
        _make_paginator([_page(prefixes=existing)]),  # list_dated
        _make_paginator([_page(contents=[])]),  # delete_folder (first pruned)
        _make_paginator([_page(contents=[])]),  # delete_folder (second pruned)
    ]

    result = MinioBackup(
        client, "src", "k", max_backups=3, clock=_fixed_clock(2026, 9, 10)
    ).run()

    assert result.action == "created"
    assert result.pruned_folders == ["backup_20260901", "backup_20260902"]


def test_run_with_fewer_existing_than_max_does_not_prune():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"Contents": [], "CommonPrefixes": []},
    ]
    existing = ["src/k/backup_20260909/", "src/k/backup_20260910/"]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=existing)]),
    ]

    result = MinioBackup(
        client, "src", "k", max_backups=7, clock=_fixed_clock(2026, 9, 11)
    ).run()

    assert result.action == "created"
    assert result.pruned_folders == []
    client.delete_objects.assert_not_called()


# ---------------------------------------------------------------------------
# run() — empty source prefix
# ---------------------------------------------------------------------------


def test_run_with_empty_source_prefix_still_creates_today_folder():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"Contents": [], "CommonPrefixes": []},
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),  # copy (empty)
        _make_paginator([_page(prefixes=[])]),  # list_dated
    ]

    result = MinioBackup(client, "src", "", clock=_fixed_clock(2026, 9, 10)).run()

    assert result.action == "created"
    assert result.objects_copied == 0
    assert result.backup_prefix == "src/backup_20260910/"


# ---------------------------------------------------------------------------
# _BACKUP_FOLDER_RE guards against accidental prefix collisions
# ---------------------------------------------------------------------------


def test_list_dated_folders_ignores_non_backup_subdirs():
    """Non-``backup_`` subfolders must not be returned as dated folders."""
    client = MagicMock()
    folders = [
        "src/k/backup_20260909/",
        "src/k/random_user_dir/",  # MUST be ignored
    ]
    client.get_paginator.return_value = _make_paginator([_page(prefixes=folders)])

    b = MinioBackup(client, "src", "k")
    dated = b._list_dated_folders("backups", "src/k/")
    assert dated == ["backup_20260909"]
