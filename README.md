# minime_utils

Shared utilities library for minime homeserver, containing utilities for Minio access, logging, settings management, and other common functionality.

## Features

- MinIO client utilities
- Logging configuration
- Settings management
- And more...

## Development

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)

### Setup

```bash
uv sync --group dev
```

### Testing

```bash
uv run pytest
```

### Code Quality

```bash
uv run ruff check .
uv run ruff format .
```
