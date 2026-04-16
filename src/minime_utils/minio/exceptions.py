"""MinIO-related exceptions."""


class MinIOError(Exception):
    """Base exception for all MinIO operations."""


class MinIOCredentialError(MinIOError):
    """Raised when MinIO credentials are missing or invalid."""


class MinIOConnectionError(MinIOError):
    """Raised when unable to connect to MinIO."""


class MinIOObjectNotFoundError(MinIOError):
    """Raised when a requested object does not exist in MinIO."""


class MinIOBucketNotFoundError(MinIOError):
    """Raised when a requested bucket does not exist in MinIO."""


class MinIOReadError(MinIOError):
    """Raised when a read operation fails."""


class MinIOWriteError(MinIOError):
    """Raised when a write operation fails."""

