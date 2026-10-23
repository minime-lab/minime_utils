# minime_utils

Shared utilities package for the minime homeserver ecosystem.

Current scope focuses on a reusable MinIO/S3-compatible toolkit used by apps and DAGs.

## App configuration

Apps and DAGs can load their repository-root `settings.toml` through the shared
loader. `find_project_root` uses the same discovery rule for other project-root
files such as `.env`. Pass the calling module's `__file__`; discovery searches
upward from that module for `pyproject.toml`. If the module is installed in
`site-packages`, it searches upward from the current working directory instead.

```python
from minime_utils.apps import load_dotenv, load_settings

settings = load_settings(__file__)
load_dotenv(__file__)
```

`load_dotenv` finds `.env` at the same project root used by `load_settings`.
For installed applications, it searches from the working directory. Set
`required=True` to raise `FileNotFoundError` when the file is missing.

## Email alerts

`EmailClient` sends plain-text email through Gmail SMTP over implicit TLS on
port 465. Configure `GMAIL_ADDRESS` and `GMAIL_APP_PASSWORD`; the latter should
be a Google app password, not the account's regular password. App passwords
require 2-Step Verification on the Google account.

```python
from minime_utils.email import EmailClient

email_client = EmailClient.from_env()
email_client.send_email("Alert title", "Alert details", "you@example.com")
```

SMTP and authentication errors propagate to the caller.

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
