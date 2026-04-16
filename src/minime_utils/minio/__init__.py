"""MinIO utilities subpackage for minime_utils.

Typical usage:

    from minime_utils.minio import read_json, write_json, list_objects
    from minime_utils.minio import MinIOObjectNotFoundError

For more explicit imports:

    from minime_utils.minio.client import build_s3_client
    from minime_utils.minio.readers import read_bytes
    from minime_utils.minio.writers import write_bytes
    from minime_utils.minio.listing import list_objects, list_directories
"""

from .client import build_s3_client, get_minio_credentials
from .exceptions import (
    MinIOBucketNotFoundError,
    MinIOConnectionError,
    MinIOCredentialError,
    MinIOError,
    MinIOObjectNotFoundError,
    MinIOReadError,
    MinIOWriteError,
)
from .listing import (
    delete_object,
    delete_prefix,
    list_directories,
    list_objects,
    object_exists,
)
from .readers import read_bytes, read_csv, read_dataframe, read_json, read_text
from .writers import write_bytes, write_csv, write_dataframe, write_json, write_text

__all__ = [
    # client
    "build_s3_client",
    "get_minio_credentials",
    # exceptions
    "MinIOError",
    "MinIOCredentialError",
    "MinIOConnectionError",
    "MinIOObjectNotFoundError",
    "MinIOBucketNotFoundError",
    "MinIOReadError",
    "MinIOWriteError",
    # readers
    "read_bytes",
    "read_json",
    "read_text",
    "read_csv",
    "read_dataframe",
    # writers
    "write_bytes",
    "write_json",
    "write_text",
    "write_csv",
    "write_dataframe",
    # listing
    "list_objects",
    "list_directories",
    "object_exists",
    "delete_object",
    "delete_prefix",
]

