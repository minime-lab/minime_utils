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
