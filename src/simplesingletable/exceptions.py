"""Typed exceptions for simplesingletable.

Every exception here subclasses ``ValueError``, which is what these APIs raised
before the types existed. Existing ``except ValueError`` handlers keep working
unchanged; new code can catch the specific type it cares about.

Two families live here:

* **Conflicts** (:class:`ConflictError` and below) -- a write was rejected because
  the stored state was not what the caller expected. These are retryable-by-reread,
  and are the ones an HTTP layer wants to map to 409 rather than 400.
* **Blob errors** (:class:`BlobError` and below) -- S3-backed field storage.

The transaction types (:class:`TransactionError` and below) also live here so that
there is one home for the hierarchy; ``simplesingletable.transactions`` re-exports
them, so ``from simplesingletable.transactions import VersionConflictError``
continues to work.
"""

__all__ = [
    "BlobError",
    "BlobNotFoundError",
    "BlobPreconditionFailedError",
    "BlobTooLargeError",
    "ConflictError",
    "ResourceExistsError",
    "ResourceNotFoundError",
    "TransactionConditionFailedError",
    "TransactionError",
    "VersionConflictError",
]


class ConflictError(ValueError):
    """A write was rejected because stored state was not what the caller expected.

    This is the type to catch when the question is "did I lose a race?" -- it covers
    both the transactional and non-transactional write paths, so a caller can map it
    to HTTP 409 without knowing which path the repository used underneath.

    Subclasses ``ValueError`` because the non-transactional path raised a bare
    ``ValueError`` before these types existed.
    """

    def __init__(
        self,
        message: str,
        *,
        resource_type: str | None = None,
        resource_id: str | None = None,
    ):
        super().__init__(message)
        self.resource_type = resource_type
        self.resource_id = resource_id


class ResourceExistsError(ConflictError):
    """A conditional create found the resource already present.

    Raised by writes that carry an ``attribute_not_exists`` condition -- creating a
    versioned resource, or saving with ``expected_version=0``.
    """


class TransactionError(Exception):
    """Raised when a transaction fails.

    Attributes:
        cancellation_reasons: The raw DynamoDB ``CancellationReasons`` payload, when the
            failure originated from a ``TransactionCanceledException``. Empty otherwise.
        operation_indexes: Indexes (into ``TransactionContext.operations``) of the
            specific operations whose conditions/conflicts caused the cancellation.
    """

    def __init__(
        self,
        message: str,
        *,
        cancellation_reasons: list[dict] | None = None,
        operation_indexes: list[int] | None = None,
    ):
        super().__init__(message)
        self.cancellation_reasons = cancellation_reasons or []
        self.operation_indexes = operation_indexes or []


class TransactionConditionFailedError(TransactionError, ConflictError):
    """Raised when a transaction is cancelled because one or more conditions did not hold.

    This is the canonical exception for both version-token collisions (implicit
    conditions set by the library) and user-supplied ``condition=`` checks. The two
    cases can be distinguished by inspecting the ``condition`` field of each operation
    referenced by ``operation_indexes``.

    Also a :class:`ConflictError` (and therefore a ``ValueError``) so that one
    ``except ConflictError`` covers transactional and non-transactional writes alike.
    """

    def __init__(
        self,
        message: str,
        *,
        cancellation_reasons: list[dict] | None = None,
        operation_indexes: list[int] | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
    ):
        TransactionError.__init__(
            self,
            message,
            cancellation_reasons=cancellation_reasons,
            operation_indexes=operation_indexes,
        )
        self.resource_type = resource_type
        self.resource_id = resource_id


class VersionConflictError(TransactionConditionFailedError):
    """A write lost an optimistic-concurrency race.

    Raised whenever a condition check fails, on either write path: inside a
    transaction (any condition, implicit or user-supplied), and on the
    non-transactional versioned update when the pre-image is no longer the latest
    version.

    ``expected_version`` and ``actual_version`` are populated where the raising site
    knows them, and are ``None`` otherwise (a transaction cancellation reports which
    condition failed, not what the stored value was).
    """

    def __init__(
        self,
        message: str,
        *,
        cancellation_reasons: list[dict] | None = None,
        operation_indexes: list[int] | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        expected_version: int | None = None,
        actual_version: int | None = None,
    ):
        super().__init__(
            message,
            cancellation_reasons=cancellation_reasons,
            operation_indexes=operation_indexes,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        self.expected_version = expected_version
        self.actual_version = actual_version


class ResourceNotFoundError(Exception):
    """Raised when a resource is not found."""


class BlobError(ValueError):
    """Base class for blob storage errors.

    Subclasses ``ValueError`` for backwards compatibility with callers written
    against the untyped API.
    """


class BlobNotFoundError(FileNotFoundError, BlobError):
    """A blob object does not exist at the expected location.

    Subclasses both ``FileNotFoundError`` and ``ValueError``.
    """

    def __init__(self, message: str, *, s3_key: str | None = None, bucket: str | None = None):
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
        s3_key: str | None = None,
        bucket: str | None = None,
        expected_etag: str | None = None,
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
        s3_key: str | None = None,
        size_bytes: int | None = None,
        max_bytes: int | None = None,
    ):
        super().__init__(message)
        self.s3_key = s3_key
        self.size_bytes = size_bytes
        self.max_bytes = max_bytes
