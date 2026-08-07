# minime_utils

Shared utilities package for the minime homeserver ecosystem.

Current scope focuses on a reusable MinIO/S3-compatible toolkit used by apps and DAGs.

## What the package can do right now

### MinIO client and credential handling

- Build a boto3 client configured for MinIO path-style addressing.
- Validate and read credentials from:
  - `MINIO_ENDPOINT`
  - `MINIO_ACCESS_KEY`
  - `MINIO_SECRET_KEY`

### Read helpers

- `read_bytes(...)` for raw object bytes
- `read_text(...)` for decoded text payloads
- `read_json(...)` for JSON payloads
- `read_csv(...)` for list-of-dicts CSV parsing
- `read_dataframe(...)` for pandas DataFrame loading from CSV objects

### Write helpers

- `write_bytes(...)`
- `write_text(...)`
- `write_json(...)`
- `write_csv(...)`
- `write_dataframe(...)`

### Listing and object management

- `list_objects(...)` with pagination support
- `list_directories(...)` for top-level prefixes under a path
- `object_exists(...)`
- `delete_object(...)`
- `delete_prefix(...)` (batched delete)

### Typed exceptions

- `MinIOError`
- `MinIOCredentialError`
- `MinIOConnectionError`
- `MinIOObjectNotFoundError`
- `MinIOBucketNotFoundError`
- `MinIOReadError`
- `MinIOWriteError`

## Quick usage

### Example 1: Read JSON from MinIO

```python
from minime_utils.minio import MinIOObjectNotFoundError, read_json

try:
	payload = read_json(bucket="configs", key="app/settings.json")
	print(payload)
except MinIOObjectNotFoundError:
	print("settings.json is missing")
```

### Example 2: Load and write pandas DataFrames

```python
from minime_utils.minio import read_dataframe, write_dataframe

df = read_dataframe(bucket="datasets", key="transactions/latest.csv")
df["amount_abs"] = df["amount"].abs()

write_dataframe(
	bucket="datasets",
	key="transactions/enriched/latest.csv",
	dataframe=df,
	include_index=False,
)
```

## Development

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)

### Setup

```bash
uv sync --group dev
```

### Test

```bash
uv run pytest -q
```

### Lint

```bash
uv run ruff check .
```

## Releasing

This package is distributed as a **git tag** — there is no PyPI or artifact
registry. Consumers depend on it as a VCS dependency:

```toml
dependencies = [
    "minime-utils @ git+https://github.com/minime-lab/minime_utils.git@v1",
]
```

The `v1` tag is a **floating major tag**: it always points at the newest `1.x`
release. Pinning `@v1` means consumers get every backwards-compatible update but
never an unannounced `2.0.0`.

### Cut a new version

From a clean `main`:

```bash
make release VERSION=1.0.5
```

That single command:

1. runs the tests (the release aborts if they fail)
2. bumps `version` in `pyproject.toml` and refreshes `uv.lock`
3. commits the bump
4. tags the release `v1.0.5` (immutable) and moves `v1` onto the same commit
5. pushes `main`, the new tag, and the force-updated `v1`

`VERSION` is required and must be `X.Y.Z` — pick it yourself following semver:

| Change | Bump | Example |
| --- | --- | --- |
| Bug fix, internal refactor | patch | `1.0.4` → `1.0.5` |
| New backwards-compatible feature | minor | `1.0.4` → `1.1.0` |
| Breaking change | major | `1.0.4` → `2.0.0` |

A major bump rolls **`v2`** instead and leaves `v1` frozen, so existing
consumers keep resolving to the last `1.x` until you migrate them deliberately.

The release refuses to run unless you are on `main`, the tree is clean (ignoring
untracked files), `main` is up to date with `origin`, and the tag does not
already exist.

### Consuming a new release

Tags resolve at **lock/build time**, not at runtime — a released version does not
reach an app until it re-resolves:

```bash
cd ../<consumer_repo>
uv lock --upgrade-package minime-utils
```

Commit the resulting `uv.lock`. Dockerised services pick it up on their next
image build.
