"""Typed exceptions for blob storage operations.

Every exception here subclasses ``ValueError``, which is what the blob APIs raised
before these types existed. Existing ``except ValueError`` handlers keep working
unchanged; new code can catch the specific type it cares about.

``BlobNotFoundError`` additionally subclasses ``FileNotFoundError`` so that code
treating blob storage as a filesystem-like interface can use the builtin.
"""

from typing import Optional

__all__ = [
    "BlobError",
    "BlobNotFoundError",
    "BlobPreconditionFailedError",
    "BlobTooLargeError",
]


class BlobError(ValueError):
    """Base class for blob storage errors.

    Subclasses ``ValueError`` for backwards compatibility with callers written
    against the untyped API.
    """


class BlobNotFoundError(FileNotFoundError, BlobError):
    """A blob object does not exist at the expected location.

    Subclasses both ``FileNotFoundError`` and ``ValueError``.
    """

    def __init__(self, message: str, *, s3_key: Optional[str] = None, bucket: Optional[str] = None):
        super().__init__(message)
        self.s3_key = s3_key
        self.bucket = bucket


class BlobPreconditionFailedError(BlobError):
    """The stored object is not the object the caller expected.

    Raised when an ``if_match`` / ``source_etag`` guard does not match the object
    currently stored -- i.e., the object was replaced between the time its ETag was
    captured and the time it was read or copied. Corresponds to an S3 412 response.
    """

    def __init__(
        self,
        message: str,
        *,
        s3_key: Optional[str] = None,
        bucket: Optional[str] = None,
        expected_etag: Optional[str] = None,
    ):
        super().__init__(message)
        self.s3_key = s3_key
        self.bucket = bucket
        self.expected_etag = expected_etag


class BlobTooLargeError(BlobError):
    """A blob is larger than the caller is willing to download.

    Raised before the object body is read into memory, so the payload is never
    allocated. ``size_bytes`` is the stored (post-compression) object size, the same
    unit reported by ``head_blob()["size_bytes"]`` and enforced by ``max_size_bytes``
    on write.
    """

    def __init__(
        self,
        message: str,
        *,
        s3_key: Optional[str] = None,
        size_bytes: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ):
        super().__init__(message)
        self.s3_key = s3_key
        self.size_bytes = size_bytes
        self.max_bytes = max_bytes
