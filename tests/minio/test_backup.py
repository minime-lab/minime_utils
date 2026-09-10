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


# ---------------------------------------------------------------------------
# data-safety guards — pruning must NEVER delete anything outside
# backups/<src-bucket>/<src-key>/backup_<yyyymmdd>/<...>
# ---------------------------------------------------------------------------


def test_prune_never_touches_source_bucket():
    """The source bucket must not be listed or deleted by pruning."""
    client = MagicMock()
    folders = [f"src/data/backup_2026090{i}/" for i in range(2, 10)]
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),  # copy
        _make_paginator([_page(prefixes=folders)]),  # list_dated
        # delete_folder: one object per deleted folder, all under backups/
        _make_paginator([_page(contents=["backups/src/data/backup_20260902/x"])]),
    ]

    MinioBackup(
        client, "src", "data", max_backups=7, clock=_fixed_clock(2026, 9, 10)
    ).run()

    # Source bucket must never be the target of a delete_objects call.
    assert client.get_paginator.call_count == 3
    assert client.delete_objects.call_count == 1
    for call in client.delete_objects.call_args_list:
        assert call.kwargs["Bucket"] == "backups"
        for obj in call.kwargs["Delete"]["Objects"]:
            assert obj["Key"].startswith("backups/")


def test_prune_never_deletes_non_dated_folder():
    """Even with stray non-dated folders, pruning must skip them."""
    client = MagicMock()
    # 8 dated folders (so max_backups=7 → exactly 1 prune happens) plus
    # 3 non-dated folders that must NEVER be touched.
    dated = [f"src/k/backup_2026090{i}/" for i in range(2, 10)]  # 8 dated
    non_dated = [
        "src/k/backup_NOTADATE/",
        "src/k/backup_2026/",
        "src/k/my_precious_data/",
    ]
    all_listed = dated + non_dated

    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }
    # Only the single oldest DATED folder (backup_20260902) has content.
    # If pruning wrongly targeted the non-dated folders, this assertion
    # would fail (no delete_objects call → call_count==0).
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=all_listed)]),
        _make_paginator([_page(contents=["backups/src/k/backup_20260902/only_file"])]),
    ]

    result = MinioBackup(
        client, "src", "k", max_backups=7, clock=_fixed_clock(2026, 9, 10)
    ).run()

    # Only the oldest DATED folder was deleted; the 3 non-dated must be
    # left alone and never appear in the delete payload.
    assert result.pruned_folders == ["backup_20260902"]
    assert client.delete_objects.call_count == 1
    deleted_keys = [
        obj["Key"]
        for call in client.delete_objects.call_args_list
        for obj in call.kwargs["Delete"]["Objects"]
    ]
    assert deleted_keys == ["backups/src/k/backup_20260902/only_file"]
    # And none of the non-dated prefixes appears as a directory anywhere
    # in any deleted key. (Use '/' boundaries to avoid the regex pattern
    # matching inside a legitimate dated folder name like backup_20260902
    # which contains the substring "backup_2026".)
    for nd in non_dated:
        nd_path = nd.strip("/")  # e.g. "src/k/backup_NOTADATE"
        for k in deleted_keys:
            assert f"/{nd_path}/" not in k and not k.startswith(f"{nd_path}/"), (
                f"non-dated folder {nd} touched by delete: {k}"
            )


def test_prune_never_deletes_today_folder():
    """The folder we just created (today) must NEVER appear in pruned."""
    client = MagicMock()
    today = "src/k/backup_20260910/"
    folders = [
        "src/k/backup_20260901/",
        "src/k/backup_20260905/",
        today,
        "src/k/backup_20260907/",
        "src/k/backup_20260908/",
        "src/k/backup_20260909/",
    ]
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }
    # 3 deletes (excess = 6 - 3 = 3): oldest non-today folders.
    delete_paginators = [
        _make_paginator([_page(contents=[f"backups/src/k/backup_2026090{i}/x"])])
        for i in (1, 5, 7)
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=folders)]),
        *delete_paginators,
    ]

    result = MinioBackup(
        client, "src", "k", max_backups=3, clock=_fixed_clock(2026, 9, 10)
    ).run()

    assert result.action == "created"
    assert "backup_20260910" not in result.pruned_folders
    assert client.delete_objects.call_count == 3


def test_prune_count_never_exceeds_excess():
    """delete_objects must be called exactly `excess` times, never more."""
    client = MagicMock()
    folders = [f"src/k/backup_202609{i:02d}/" for i in range(1, 16)]  # 15
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }
    delete_paginators = [
        _make_paginator([_page(contents=[f"backups/src/k/backup_2026090{i}/x"])])
        for i in range(1, 9)  # 01..08
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=folders)]),
        *delete_paginators,
    ]

    result = MinioBackup(
        client, "src", "k", max_backups=7, clock=_fixed_clock(2026, 9, 10)
    ).run()

    # Today is backup_20260910 → preserved. 8 oldest (01..08) deleted.
    assert result.pruned_folders == [f"backup_2026090{i}" for i in range(1, 9)]
    assert client.delete_objects.call_count == 8


def test_prune_delete_prefix_is_always_under_parent():
    """Every delete_objects call must target backups/<src>/<key>/ only."""
    client = MagicMock()
    folders = [
        "src/data/backup_20260908/",
        "src/data/backup_20260909/",
        "src/data/backup_20260910/",
    ]
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=folders)]),
        # The oldest (backup_20260908) has a file inside.
        _make_paginator(
            [_page(contents=["backups/src/data/backup_20260908/old.json"])]
        ),
    ]

    MinioBackup(
        client, "src", "data", max_backups=2, clock=_fixed_clock(2026, 9, 11)
    ).run()

    delete_calls = client.delete_objects.call_args_list
    assert len(delete_calls) == 1
    bucket = delete_calls[0].kwargs["Bucket"]
    keys = [obj["Key"] for obj in delete_calls[0].kwargs["Delete"]["Objects"]]
    assert bucket == "backups"
    for k in keys:
        assert k.startswith("backups/src/data/"), (
            f"deleting outside backups/<src>/<key>/: {k}"
        )


def test_run_with_max_backups_one_keeps_only_today():
    """max_backups=1 must leave exactly one folder (today) afterwards."""
    client = MagicMock()
    folders = [
        "src/k/backup_20260901/",
        "src/k/backup_20260905/",
        "src/k/backup_20260908/",
        "src/k/backup_20260909/",
        "src/k/backup_20260910/",  # today
    ]
    client.list_objects_v2.return_value = {
        "Contents": [],
        "CommonPrefixes": [],
    }
    delete_paginators = [
        _make_paginator([_page(contents=[f"backups/src/k/backup_202609{i:02d}/x"])])
        for i in (1, 5, 8, 9)
    ]
    client.get_paginator.side_effect = [
        _make_paginator([_page(contents=[])]),
        _make_paginator([_page(prefixes=folders)]),
        *delete_paginators,
    ]

    result = MinioBackup(
        client, "src", "k", max_backups=1, clock=_fixed_clock(2026, 9, 10)
    ).run()

    assert result.action == "created"
    assert result.pruned_folders == [
        "backup_20260901",
        "backup_20260905",
        "backup_20260908",
        "backup_20260909",
    ]
    assert client.delete_objects.call_count == 4
