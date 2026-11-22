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

### Local and MinIO paths

Use `LocalPathResolver` to discover the project root and register commonly used
local paths. `PathCollection` exposes registered names as attributes or mapping
keys. The resolver only constructs paths; it does not create files or
directories.

```python
from minime_utils.apps import LocalPathResolver

local = LocalPathResolver.from_module(__file__)
settings_path = local.add_relative_file("settings", "settings.toml")
logs_dir = local.add_relative_path("logs", "logs")

print(local.paths.settings)
print(local.paths["logs"])
```

`find_project_root` is available from both `minime_utils.apps.paths` and
`minime_utils.apps.settings` for compatibility. Both names refer to the same
function.

Use `MinioPathResolver` to construct object keys under an app-specific bucket
and prefix. MinIO paths are represented by `MinioPath(bucket, key)`; they are
not local filesystem paths and do not create MinIO objects.

```python
from minime_utils.apps import MinioPathResolver

remote = MinioPathResolver("minime-lab", "apps/my_app")
remote.add_relative_path("logs", "logs")
print(remote.paths.logs)  # MinioPath(bucket="minime-lab", key="apps/my_app/logs")
```

### Queue-backed MinimeLogger

`MinimeLogger` writes JSON Lines records through a bounded logging queue to a
local rotating file. `flush()` waits for queued records, closes the active
segment, and uploads closed segments using the MinIO writer. Successful uploads
are removed locally; failed uploads are retained for retry. The MinIO bucket
and prefix are passed at setup. MinIO credentials come from
`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, and `MINIO_SECRET_KEY`.

The logger accepts a settings dictionary, either the full `load_settings()`
result containing a `[logging]` section or the logging mapping itself. Missing
values default to `logs`, 10 MiB per file, three backups, a 10,000-record queue,
and `INFO` level. Relative log paths resolve under `project_root`, or the
current working directory if no root is supplied.

```toml
[logging]
logs_path = "logs"
max_bytes = 10485760
backup_count = 3
queue_size = 10000
level = "INFO"
minio_bucket = "minime-lab"
minio_prefix = "apps/my_app/logs"
```

```python
from minime_utils.apps import LocalPathResolver, MinimeLogger, load_settings

local = LocalPathResolver.from_module(__file__)
settings = load_settings(__file__)
logging_config = settings["logging"]
logger = MinimeLogger.from_config(
    settings,
    name="my_app",
    minio_bucket=logging_config["minio_bucket"],
    minio_prefix=logging_config["minio_prefix"],
    project_root=local.root,
)

logger.logger.info("App started")
# Call after a request or DAG task when its records should be uploaded.
logger.flush()
# Close once at process shutdown to flush and stop the queue listener.
logger.close()
```

For a web app, call `flush()` after each request if logs should be uploaded
promptly, and call `close()` during application shutdown. If MinIO is
unavailable, flush leaves failed segments on local disk; retry with a later
flush. Keep a logger instance scoped to the process when a shared app logger is
desired; a flush uploads all queued records for that logger, not only records
from the current request.

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

## DAG job failure notifications

`minime_utils.dag_utils.notify_failure` sends a job failure email without an
Airflow dependency. Call it in the operator job's existing error handler:

```python
from minime_utils.dag_utils import notify_failure

try:
    run_job()
except Exception:
    notify_failure("validate_creds", "my_dag.main", run_id, 1)
    return 1
```

The email includes the DAG/job name, run ID, exit code, and active exception
traceback. For failures without an exception, supply `error_message` explicitly.
Configure `GMAIL_ADDRESS`, `GMAIL_APP_PASSWORD`, and `DAG_FAILURE_EMAIL` in the
job container environment. Email delivery failures are logged and suppressed so
callers retain the original job failure. Retry policy remains with the caller.

Release this library before migrating consumers. After publishing the desired
version/tag, run `uv lock --upgrade-package minime-utils` in each consumer
repository and rebuild its app or task image. Old frozen lockfiles will not
contain newly added modules.
