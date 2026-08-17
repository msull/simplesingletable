# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [18.1.0] 2026-08-17

### Added

* **Conditional blob reads** (#9). `head_blob` now returns the object's `etag`, and `get_blob` accepts `if_match=`, so a caller can guarantee the bytes it processes are the bytes it validated — the identity gap that made the blob API unusable for presigned uploads, where the object can be replaced between validation and consumption. Quoted and unquoted ETags are both accepted (they are normalized); an ETag is an opaque identity token, never a checksum, since multipart uploads do not produce MD5s. A conditional read always goes to S3: the cache can attest what an object *was*, not what it is now, so it is bypassed whenever `if_match` is given. `put_blob`, `copy_blob_object` and `head_blob` all record the observed ETag on the returned `BlobPlaceholder` (new optional `etag` key).

* **`DynamoDbMemory.head_blob(resource, field_name)` and `DynamoDbMemory.read_blob(resource, field_name, ...)`** (#9). Blob metadata and single-field reads previously had no memory-level entry point at all — callers had to reach into `memory.s3_blob_storage` and rebuild the resource-type/id/version triple by hand. `read_blob` returns the value without mutating the resource and carries the `if_match=` / `max_bytes=` guards. Mirrored on `LocalStorageMemory`.

* **Typed blob exceptions** (#9): `BlobNotFoundError`, `BlobPreconditionFailedError`, and `BlobTooLargeError` in the new `simplesingletable.exceptions` module, exported from the package root. "Never uploaded", "changed underneath me", and "too big to pull into memory" are now distinguishable without matching on message text — each carries the relevant context (`s3_key`, `bucket`, `expected_etag`, `size_bytes`/`max_bytes`). All subclass `BlobError(ValueError)`, the type these paths raised before, so existing `except ValueError` handlers are unaffected; `BlobNotFoundError` additionally subclasses `FileNotFoundError`.

* **Read-side size enforcement** (#9). `get_blob(max_bytes=)` refuses an oversized object based on `ContentLength`, before the body is read, so the payload is never allocated in a memory-limited process. `memory.read_blob` defaults `max_bytes` to the field's configured `max_size_bytes`, which until now was only enforced on write. A cached entry that looks oversized is not served, so the limit is always decided by S3's authoritative size rather than the cache's approximate accounting.

* **`source_etag=` on `copy_blob` and `register_external_blob`** (#9), guarding the server-side copy via `CopySourceIfMatch`.

### Fixed

* **`register_external_blob` and `copy_blob` no longer race their own validation** (#9). Both HEAD the source to validate it and then issue a separate `copy_object`, with nothing tying the two together — a replacement written in that window was copied silently, and with `delete_source=True` the original was then deleted. The copy is now guarded on the ETag observed by that same HEAD, so a source replaced mid-operation raises `BlobPreconditionFailedError` instead of quietly substituting the new bytes. Callers who can supply an ETag captured earlier (at upload validation time, in another process) should pass `source_etag=` to extend the guarantee back that far.

* **`register_external_blob` raises `BlobNotFoundError`** for a missing source object instead of a bare `ValueError` (`BlobNotFoundError` subclasses `ValueError`, so this is not a breaking change).

## [18.0.0] 2026-07-23

### Changed

* **BREAKING: `TransactionContext.put()` applies optimistic locking by default** (#8). The put is now guarded on the resource's `updated_at` as captured at queue time, ANDed with any user-supplied `condition=`. If another writer modified the item after the resource was read, the commit raises `TransactionConditionFailedError` instead of silently overwriting the concurrent write — a full-state put built from a stale read is a full-state lost update. The guard is never auto-retried (a resend of the same stale state cannot succeed; re-read and re-apply to make progress). Pass `optimistic=False` to restore the previous last-writer-wins behavior. Callers whose put resources always hold a fresh read are unaffected.

* **Transaction retry policy now covers transient failures** (#8). When `auto_retry=True` (still the default), a `TransactionCanceledException` whose cancellation reasons are all transient (`TransactionConflict`, throttling, capacity) and top-level transient error codes (`TransactionInProgressException`, `ThrottlingException`, `ProvisionedThroughputExceededException`, `RequestLimitExceeded`, `InternalServerError`) are retried up to `max_retries` times with full-jitter exponential backoff. Previously all of these raised `TransactionError` immediately, even though they are the one class of failure where resending identical items can succeed.

* **`TransactionContext.increment(amount=...)`** accepts `int | float | Decimal` (previously typed `int`); float amounts are normalized to `Decimal` like every other transactional expression value (#8).

### Added

* **`condition_names` parameter on `TransactionContext.create` / `put` / `update` / `delete`** (#8) supplies `#alias -> attribute-name` mappings (`ExpressionAttributeNames`) for condition expressions, making it possible to write conditions against DynamoDB reserved words (`status`, `name`, `total`, ...). Previously `Put`/`Delete` items never set `ExpressionAttributeNames` at all, and `Update` only aliased fields that also appeared in `updates=`, so such conditions were impossible to express. On `update`, user-supplied aliases seed the placeholder allocator so they can never collide with auto-generated `SET` placeholders.

### Fixed

* **Transactional expression values are normalized through `clean_data`** (#8). `txn.update(updates=...)`, `condition_values` on every operation, `txn.increment`, and `txn.append` previously passed values straight to boto3's `TypeSerializer`, so a plain `float` raised `TypeError: Float types are not supported` and a `datetime` raised `Unsupported type` at build time. All expression values now get the same `float`→`Decimal` / `date`/`datetime`→ISO-format conversion (recursively, through nested dicts and lists) as `to_dynamodb_item()` on the non-transactional write path. Empty sets — which `clean_data` silently drops on item writes — raise a clear client-side `ValueError` instead of leaving a dangling expression placeholder.

* **Implicit-condition retries no longer rebuild from stale cached state** (#8). When a versioned update failed its version-token check and was retried, the rebuild reused the caller-supplied `current=` pre-image or the snapshot-isolation read cache, re-derived the same stale version number, and was guaranteed to fail again — burning every retry before raising `VersionConflictError`. The failed operations now have `op.current` cleared and their read-cache entries dropped before the rebuild, so the retry re-reads fresh state and can actually make progress.

* **`txn.put` no longer mutates the caller's resource on a failed commit** (#8). The builder previously bumped `updated_at` on the caller's object during every build attempt, so a failed transaction left the object holding a timestamp that was never written (which would in turn poison the new optimistic guard on the next attempt). The builder now works on a copy; `commit()` syncs the written `updated_at` back onto the caller's object only after success, so a successfully-put object remains valid for a follow-up optimistic put.

* **`TRANSACTION_USAGE.md` refreshed** (#8): documents `txn.put` as the overwrite path (with optimistic locking and opt-out), `recompute_gsis`/`clear_fields`, `condition_names` for reserved words, and the actual retry semantics; removed a stale `_version_token` implementation note that described a mechanism that does not exist.

## [17.0.2] 2026-05-19

### Fixed

* **Pydantic 2.11+ deprecation warning** when accessing `model_fields` on resource instances. All internal lookups now use `type(instance).model_fields` instead of `instance.model_fields`, which is the canonical class-level access and silences `PydanticDeprecatedSince211`. No behavior change; affects `dynamodb_memory.py`, `local_storage_memory.py`, `models.py`, and `extras/habit_tracker.py`.

## [17.0.1] 2026-05-18

### Fixed

* **`clean_data` now serializes `datetime.date` values** alongside `datetime.datetime` (#5). Previously, Pydantic fields typed as plain `date` on uncompressed resources reached boto3 as raw `date` instances and raised `TypeError: Unsupported type "<class 'datetime.date'>"` on save. The check now uses `isinstance(value, date)`, which covers both `date` and `datetime` (since `datetime` subclasses `date`). The same conversion is applied inside `_clean_list` so date values nested in lists round-trip correctly as well.

## [17.0.0] 2026-05-13

### Added

* **`ResourceConfig(omit_none_attributes=True)`** opt-in to drop `None`-valued fields from DynamoDB items before marshalling (#1). Without this flag, boto3 marshalls `None` as `{"NULL": True}`, which makes `attribute_not_exists(field)` return False after the very first PUT — breaking the standard "claim this slot" conditional-update pattern. Off by default for backward compatibility; recommended for any resource that uses `Optional` fields as slot markers.

    ```python
    class Asset(DynamoDbResource):
        resource_config: ClassVar[ResourceConfig] = ResourceConfig(
            omit_none_attributes=True,
        )
        asset_tag: str
        assigned_user_id: Optional[str] = None
    ```

* **`PaginatedList.pagination_key`** property aliases `next_pagination_key` so callers can use the same attribute name as the query's input parameter (#1).

* **Transaction overhaul** brings transactional CRUD to parity with the non-transactional path and fills gaps that previously required workarounds (#2):
    * **`TransactionContext.put(resource, condition=..., condition_values=...)`** writes the full state of a non-versioned resource via `to_dynamodb_item()`. Naturally recomputes every GSI key and (with `omit_none_attributes=True`) drops nulled-out attributes.
    * **`TransactionContext.update(..., recompute_gsis=True)`** reads the current state, applies the update in memory, re-runs `get_gsi_config`, and folds the resulting GSI key SETs/REMOVEs into the same update expression. Fixes the long-standing footgun where updating a field that participated in a GSI key left the GSI key attribute stale.
    * **`TransactionContext.update(..., clear_fields=[...])`** emits a `REMOVE` clause for the given fields in the same update expression — at parity with the non-transactional `update_existing(clear_fields=...)` parameter. Combines with `recompute_gsis=True` to drop a resource out of a sparse GSI cleanly.
    * **`TransactionContext.update(..., current=...)`** lets callers supply a pre-loaded resource instance to skip the internal `get_existing` read used by versioned updates and by `recompute_gsis=True`. Also used as the pre-image for audit field-change tracking.
    * **`commit()` post-commit hooks** now emit audit logs and increment `MemoryStats` counters for every operation that should produce them (CREATE / UPDATE / PUT / DELETE on audit-enabled resources). Previously, every transactional mutation silently bypassed the audit feed and counter system. Transaction-wide attribution is provided via the new `memory.transaction(changed_by=..., audit_metadata=...)` kwargs; a `transaction_id` (ULID) is auto-attached to every audit row so they can be grouped post-hoc.
    * **`DynamoDbMemory.emit_audit_log(...)` and `emit_audit_logs(entries: list[AuditEntry])`** are now public entry points for the audit-write path used by CRUD methods. Useful for callers that need to emit audit rows for state managed outside the standard CRUD path; the batch variant uses a single `BatchWriteItem` per chunk.

### Changed

* **`paginated_dynamodb_query`** docstring now documents that `ascending` maps directly to DynamoDB's `ScanIndexForward` parameter (#1).

* **Transaction exception hierarchy** is normalized so a single logical event — "a conditional check inside the transaction did not hold" — surfaces as a single exception class (#2):
    * **`TransactionConditionFailedError`** (subclass of `TransactionError`) is the new canonical exception for any condition-check failure inside a transaction. Carries `cancellation_reasons` (the raw DynamoDB payload) and `operation_indexes` (the index, into `TransactionContext.operations`, of the operation(s) that triggered the failure).
    * **`VersionConflictError`** is preserved as a subclass of `TransactionConditionFailedError` so existing `except VersionConflictError` blocks continue to behave as before.
    * **`TransactionError.__init__`** now accepts the same `cancellation_reasons` / `operation_indexes` kwargs so this metadata is available on the parent class too.
    * Raw `botocore.exceptions.ClientError` is never re-raised from `TransactionContext.commit()` for a recognized DynamoDB transaction-cancellation case.

* **Transaction retry policy is now condition-aware** (#2). When `auto_retry=True` (still the default), the transaction is only retried if every failed item came from an operation with no user-supplied `condition=` — typically a versioned-update version-token collision. Any user-supplied condition failure raises immediately, because that encodes semantic intent (e.g., "this slot must be empty") that retrying cannot resolve and only adds latency. The previous behavior was to retry every `ConditionalCheckFailed` reason up to `max_retries` times regardless of source.

* **AuditLog gains a sparse GSI on `changed_by`** (#3) for the "what has user X changed across the entire system?" access pattern. Backed by the table's existing `gsi3pk`/`gsi3sk`. `AuditLogQuerier.get_logs_by_changer(changed_by)` (without a resource_type filter) now uses this index directly instead of falling back to `gsitype` + filter expression. `AuditLog.INDEX_BY_RESOURCE`, `INDEX_BY_TYPE`, `INDEX_BY_CHANGER`, and `INDEX_BY_UPDATED_AT` class constants are now the source of truth for index names; `AuditLogQuerier` no longer carries hardcoded `"gsi1"` / `"gsi2"` / `"gsitype"` literals.

* **`DynamoDbMemory.audit_view`** is a new property that returns a cached secondary `DynamoDbMemory` view targeting the audit table (when `audit_table_name` is configured) — or `self` otherwise (#3). `AuditLogQuerier` now delegates to this cached view, so multiple queriers against the same memory share a single secondary instance instead of each lazily building its own.

* **`AuditConfig` docstring** now documents the interactions between `enabled` / `track_field_changes` / `include_snapshot` / `old_resource` with a "what you get" matrix (#3). README has a new "Internal Library Resources" section documenting the `_INTERNAL` namespace convention and the `AuditLog` row shape.

## [16.5.0] 2026-02-02

### Added

* **Server-Side S3 Blob Copy Operations**: Added `copy_blob()` and `register_external_blob()` methods to both `DynamoDbMemory` and `LocalStorageMemory` for efficient blob manipulation without downloading data to the client:
    - **`copy_blob()`**: Server-side S3 copy of a blob field between resources with zero Lambda/client memory usage
        - Supports copying between any combination of versioned and non-versioned resources
        - Supports copying between different resource types
        - Supports copying to a different field on the same resource
        - Optional `delete_source=True` for move semantics
        - Self-copy guard prevents copying a blob to the same resource+field
        - Compression mismatch detection with warning (copies as-is since server-side copy cannot re-compress)
        - Automatic DynamoDB metadata updates (`_blob_fields`, `_blob_versions`) on target resource
        - Automatic cache invalidation on the target key
        - In-memory resource state updated after copy
    - **`register_external_blob()`**: Register an arbitrary S3 object as a blob field on a resource
        - Copies from any S3 key/bucket to the resource's canonical blob location
        - `source_bucket` parameter enables cross-bucket copies (defaults to managed bucket)
        - `compressed` flag declares the compression state of the external object (stored in S3 metadata)
        - Optional `delete_source=True` to clean up the external object after registration
        - Full validation of field configuration and source object existence
    - **Low-Level Storage Methods**:
        - `S3BlobStorage.head_blob()`: `head_object()` wrapper returning size, compression state, content type, and metadata
        - `S3BlobStorage.copy_blob_object()`: Server-side `copy_object()` with proper metadata replacement and cache invalidation
        - `S3BlobStorage._cache_invalidate()`: Extracted single-key cache removal (refactored from `delete_blob`)
        - `LocalBlobStorage.head_blob()`: Filesystem equivalent reading `.meta` companion file
        - `LocalBlobStorage.copy_blob_object()`: Filesystem equivalent using `shutil.copy2`
    - **DynamoDB Metadata Update Strategy**: Private helper `_update_blob_metadata_on_dynamodb()` reads current blob metadata from DynamoDB before merging, ensuring existing blob fields are preserved. Uses `transact_write_safe` for versioned resources (updates both v0 and vN items atomically).
    - **Comprehensive Test Coverage**: 26 new tests in `tests/test_blob_copy.py` covering unit tests, integration tests, metadata verification, and cache invalidation

    Example usage:
    ```python
# Copy a blob between resources (server-side, zero memory)
memory.copy_blob(source_doc, "content", target_doc, "content")

# Move a blob (copy + delete source)
memory.copy_blob(source_doc, "content", target_doc, "content", delete_source=True)

# Register an external S3 object as a blob field
memory.register_external_blob(
    resource=my_doc,
    field_name="data",
    source_s3_key="uploads/user-file.json",
    content_type="application/json",
    delete_source=True,  # clean up upload after registration
)

# Cross-bucket registration
memory.register_external_blob(
    resource=my_doc,
    field_name="data",
    source_s3_key="incoming/report.gz",
    source_bucket="external-uploads-bucket",
    compressed=True,
)
    ```

## [16.4.0] 2026-02-02

### Added

* **Batch Read Support**: Added `batch_get_existing()` method to both `DynamoDbMemory` and `LocalStorageMemory` for efficient multi-ID lookups:
    - Returns `dict[str, T]` mapping resource_id to resource (missing IDs absent from result)
    - Automatic deduplication of input IDs
    - Auto-chunking into batches of 100 (DynamoDB `batch_get_item` limit)
    - Automatic retry of `UnprocessedKeys` with backoff
    - Works with both versioned and non-versioned resources (fetches current version for versioned)
    - `LocalStorageMemory` implementation provides API parity via simple loop

* **Repository-Level TTL Caching**: Added opt-in caching to all repository classes via new `cache_ttl_seconds` parameter:
    - **`ResourceRepository`**: Cache integrated with `get()`, `create()`, `update()`, `delete()`, and new `batch_get()`
    - **`VersionedResourceRepository`**: Passes `cache_ttl_seconds` through to parent; `restore_version()` benefits automatically via `update()`
    - **`ReadOnlyResourceRepository`**: Cache integrated with `get()` and new `batch_get()`
    - **`ReadOnlyVersionedResourceRepository`**: Passes `cache_ttl_seconds` through to parent
    - Cache disabled by default (no overhead when not configured)
    - Automatic invalidation on writes (create populates, update refreshes, delete removes)
    - `batch_get()` checks cache first, fetches only missing IDs from DynamoDB, then populates cache with results
    - `clear_cache()` method for manual invalidation

* **TTLCache Utility Class**: New `TTLCache` class in `simplesingletable.extras.cache` (exported from `simplesingletable.extras`):
    - Stdlib-only, no external dependencies
    - Uses `time.monotonic()` for TTL (immune to system clock changes)
    - Lazy eviction on access (no background threads)
    - Defensive copying on both `put()` and `get()` to prevent callers from mutating cached state
    - Accepts optional `copy_fn` parameter (repositories use Pydantic `model_copy(deep=True)` by default)
    - Methods: `get()`, `get_many()`, `put()`, `put_many()`, `invalidate()`, `clear()`

* **Internal Refactor**: Extracted `_build_blob_placeholders()` helper in `DynamoDbMemory`, shared by `get_existing()` and `batch_get_existing()`

    Example usage:
    ```python
    from simplesingletable.extras import ResourceRepository, TTLCache

    # Repository with 5-minute cache
    repo = ResourceRepository(
        ddb=memory,
        model_class=User,
        create_schema_class=CreateUserSchema,
        update_schema_class=UpdateUserSchema,
        cache_ttl_seconds=300,
    )

    # Batch get - uses cache for hits, fetches only missing from DDB
    users = repo.batch_get(["id1", "id2", "id3"])

    # Single get - served from cache if available
    user = repo.get("id1")

    # Writes automatically update cache
    repo.update("id1", {"name": "New Name"})

    # Manual cache clear
    repo.clear_cache()

    # Direct memory-level batch get (no cache)
    results = memory.batch_get_existing(["id1", "id2"], User)
    ```

## [16.3.0] 2025-10-28

### Added

* **Separate Audit Table Support**: Added ability to write audit logs to a separate DynamoDB table with optional separate connection parameters:
    - **New DynamoDbMemory Parameters**:
        - `audit_table_name`: Optional separate table name for audit logs (defaults to main table)
        - `audit_endpoint_url`: Optional endpoint URL for separate audit table
        - `audit_connection_params`: Optional connection parameters (region, credentials) for separate audit table
    - **Use Cases**:
        - Compliance requirements: Isolate audit logs in separate storage for regulatory compliance
        - Cross-account storage: Write audit logs to different AWS account for security
        - Different region: Store audit logs in different region for disaster recovery
        - Performance isolation: Prevent audit writes from affecting main table performance
    - **Backward Compatible**: If audit table parameters are not specified, audit logs continue to write to main table (existing behavior)
    - **AuditLogQuerier Updates**: All query methods automatically use the configured audit table
    - **Comprehensive Testing**: 13 new tests covering separate table writes, queries, backward compatibility, and property access
    - **Example Usage**: Added `get_memory_with_separate_audit_table()` example to `examples/audit_example.py`

    Example usage:
    ```python
    # Option 1: Same table (existing behavior, still works)
    memory = DynamoDbMemory(
        table_name="my-app-table",
        logger=logger
    )

    # Option 2: Separate table, same credentials
    memory = DynamoDbMemory(
        table_name="my-app-table",
        audit_table_name="my-audit-table",
        logger=logger
    )

    # Option 3: Separate table, separate credentials (cross-account)
    memory = DynamoDbMemory(
        table_name="my-app-table",
        connection_params={"region_name": "us-east-1"},
        audit_table_name="audit-table-in-different-account",
        audit_connection_params={
            "region_name": "us-west-2",
            "aws_access_key_id": "AUDIT_KEY",
            "aws_secret_access_key": "AUDIT_SECRET"
        },
        logger=logger
    )
    ```

## [16.2.0] 2025-10-27

### Added

* **Local File Storage Implementation**: Added `LocalStorageMemory` and `LocalBlobStorage` classes for offline demos and local testing without AWS dependencies:
    - **LocalStorageMemory**: Mostly complete drop-in replacement for `DynamoDbMemory` that stores data in local JSON files
        - Stores resources as JSON files in `{storage_dir}/resources/` directory (one file per resource type)
        - Full support for all CRUD operations (create, read, update, delete)
        - Complete versioned resource support with full version history
        - GSI query support (gsitype, gsi1, gsi2, gsi3, gsi4) with filtering and pagination
        - Filter expressions and filter functions
        - Audit logging integration
        - Statistics tracking via `MemoryStats`
        - Counter operations and set manipulation
        - Thread-safe file locking using `fcntl` (Unix) for concurrent access
        - Automatic encoding/decoding of binary data (bytes) and sets for JSON compatibility
        - Zero dependencies on AWS services - works completely offline
    - **LocalBlobStorage**: Local file-based blob storage that mirrors `S3BlobStorage` interface
        - Stores blobs as files in `{storage_dir}/blobs/{ResourceType}/{resource_id}/` directory structure
        - Support for compression, content types, and size limits
        - Metadata storage for each blob (version, compression, content type)
        - Handles complex Pydantic types via TypeAdapter serialization
        - List blob versions and automatic cleanup on resource deletion
    - **Binary Data Encoding**: Automatic base64 encoding for bytes and list conversion for sets to enable JSON serialization
    - **Same API as DynamoDbMemory**: Identical interface makes switching between local and DynamoDB storage seamless
    - **Use Cases**:
        - Offline demos and presentations without Docker
        - Local development and testing
        - CI/CD environments without AWS credentials
        - Learning and exploring simplesingletable features
        - Prototyping before deploying to AWS
    - **Streamlit Demo with Local Storage**: Added `app_local.py` variant of the Streamlit demo that uses local storage
        - No Docker containers needed
        - Same scenarios as the DynamoDB version (CRUD, versioning, audit logging, blob storage)
        - Storage viewer with formatted tables and raw JSON view
        - File structure browser
        - Quick reset and folder opening
        - Documentation in `README_LOCAL.md`
    - **Example Scripts**: Added `examples/local_storage_example.py` demonstrating local storage usage
    - **Comprehensive Testing**: 24 tests covering all features including versioned resources, blob storage, GSI queries, pagination, stats tracking, and set serialization

    Example usage:
    ```python
    from simplesingletable import LocalStorageMemory, DynamoDbVersionedResource
    from logzero import logger

    # Create local storage
    storage = LocalStorageMemory(
        logger=logger,
        storage_dir="./my_local_data",
        track_stats=True,
        use_blob_storage=True,
    )

    # Use exactly like DynamoDbMemory!
    resource = storage.create_new(MyResource, {"field": "value"})
    updated = storage.update_existing(resource, {"field": "new_value"})
    results = storage.list_type_by_updated_at(MyResource)

    # All data stored in JSON files:
    # ./my_local_data/resources/MyResource.json
    # ./my_local_data/blobs/MyResource/{resource_id}/field_name.blob
    ```

## [16.1.0] 2025-10-21

### Added

* **Repository Audit Logging Support**: Extended `ResourceRepository` and `VersionedResourceRepository` to expose audit logging parameters:
    - Added `changed_by` and `audit_metadata` parameters to `create()`, `update()`, and `delete()` methods in `ResourceRepository`
    - Added `changed_by` and `audit_metadata` parameters to `restore_version()` method in `VersionedResourceRepository`
    - Parameters are forwarded to underlying `DynamoDbMemory` methods to integrate with the audit logging system (v14.0.0+)
    - All parameters are optional and backward compatible - existing repository code continues to work unchanged
    - Enables repository pattern users to leverage audit trail capabilities without dropping down to the memory layer

## [16.0.0] 2025-10-21

### Changed

* **Audit Logging: Optional `changed_by` Parameter**: Modified audit logging to make the `changed_by` parameter optional by default:
    - Added new `changed_by_required` configuration option to `AuditConfig` (defaults to `None`/`False`)
    - When `changed_by_required=True`, the system enforces that `changed_by` must be provided during resource creation/updates
    - When `changed_by_required=False` or unset (default), `changed_by` is optional and audit logs can be created without change attribution
    - Replaced the `changed_by_field` validation logic: previously, if `changed_by_field` was set but the field value was `None`, an error was raised; now, this validation only occurs when `changed_by_required=True`
    - **Breaking Change**: Minimal impact - only affects users who were relying on the previous behavior where `changed_by_field` presence enforced `changed_by` requirement
    - Provides more flexibility for audit logging scenarios where change attribution is not always available or necessary

## [15.0.0] 2025-10-21

### Changed

* **BREAKING: AuditLog GSI Type Partitioning**: Modified `AuditLog` to use a custom `gsitype` value (`_INTERNAL#AuditLog`) instead of the class name (`AuditLog`). This separates internal audit logs from user-defined resources in the `gsitype` index, preventing audit logs from appearing in queries for user resources.
    - **Breaking Change**: Existing `AuditLog` entries created in v14.0.0 will no longer be returned by `gsitype` queries after this update
    - **Impact**: Only affects users who adopted the audit logging feature from v14.0.0 (released 2025-10-16)
    - **Migration**: No action required for most users; audit logs remain accessible via dedicated audit query methods (`AuditLogQuerier`)
    - Implemented via new `db_get_gsitypepk()` override in `AuditLog` class that returns `get_unique_key_prefix()`

* **Introduced `db_get_gsitypepk()` Method**: Added new classmethod `db_get_gsitypepk()` to `BaseDynamoDbResource` that allows resources to customize their `gsitype` partition key value:
    - Default implementation returns `cls.__name__` for backward compatibility
    - Can be overridden by subclasses to use custom prefixes or grouping strategies
    - Used by `AuditLog` to implement internal resource partitioning
    - All `gsitype` assignments now use `db_get_gsitypepk()` instead of direct `__class__.__name__` references

## [14.0.0]  2025-10-16

### Added

* **Comprehensive Audit Logging System**: Added full audit trail capabilities for tracking resource changes with field-level granularity:
    - **AuditLog Resource Model**: New versioned resource type that captures CREATE, UPDATE, DELETE, and RESTORE operations with automatic ULID-based chronological ordering
    - **Opt-In Per-Resource Configuration**: Resources enable audit logging via `ResourceConfig.audit_config` with granular control:
        - `enabled`: Toggle audit logging on/off
        - `track_field_changes`: Capture old vs new values for each modified field
        - `include_snapshot`: Store complete resource state at time of change
        - `exclude_fields`: Forbid sensitive fields from audit tracking
        - `changed_by_field`: Auto-extract change attribution from resource field
    - **Automatic Change Attribution**: Support for explicit `changed_by` parameter or automatic extraction from resource fields (e.g., `user_id`, `modified_by`)
    - **Field-Level Change Tracking**: Audit logs capture granular field changes showing `{"old": value, "new": value}` for each modified field
    - **Blob Field Support**: Audit logs store blob field metadata (size, compression, content type) instead of actual content, preventing audit logs from becoming bloated
    - **Smart Field Filtering**: Automatically excludes base resource fields (`resource_id`, `created_at`, `updated_at`, `version`) from change tracking to focus on business data
    - **Custom Metadata**: Attach arbitrary context to audit events via `audit_metadata` parameter (e.g., `{"reason": "user request", "ticket": "JIRA-123"}`)
    - **AuditLogQuerier Helper Class**: Powerful query interface in `simplesingletable.extras.audit` for analyzing audit history:
        - `get_logs_for_resource()`: All changes to a specific resource with optional date range filtering
        - `get_logs_for_resource_type()`: All changes across resource type with ULID-based date range queries
        - `get_logs_by_operation()`: Filter by operation type (CREATE/UPDATE/DELETE/RESTORE)
        - `get_logs_by_changer()`: Track all changes by specific user/system
        - `get_field_history()`: Complete change history for individual fields showing progression over time
        - `get_recent_changes()`: Most recent audit activity across all or specific resource types
    - **Optimized GSI Structure**: Three specialized indices for different access patterns:
        - `gsi1` (gsi1pk: `{resource_type}#{resource_id}`, sort: pk/ULID): Resource-specific audit trail
        - `gsi2` (gsi2pk: `{resource_type}`, sort: pk/ULID): Type-level change tracking
        - `gsitype` (gsitype: "AuditLog", sort: created_at): Recent changes across all resources
    - **Pagination Support**: All query methods return `PaginatedList` with continuation tokens for handling large audit histories
    - **Recursion Prevention**: AuditLog resources don't audit themselves to prevent infinite loops
    - **Compression Disabled**: AuditLog uses `compress_data=False` to enable efficient DynamoDB filter expressions on operation and changed_by fields
    - **Full Test Coverage**: 58 comprehensive tests covering CREATE/UPDATE/DELETE operations, nested Pydantic models, blob fields, versioned resources, pagination, date ranges, and edge cases
    - **Zero Performance Impact When Disabled**: Resources without audit configuration have no overhead
    - **Seamless Integration**: Works transparently with both `DynamoDbResource` and `DynamoDbVersionedResource`

    Example usage:
    ```python
    from simplesingletable import DynamoDbResource, AuditConfig, AuditLogQuerier
    from simplesingletable.models import ResourceConfig

    class User(DynamoDbResource):
        name: str
        email: str
        status: str
        password_hash: str

        resource_config = ResourceConfig(
            audit_config=AuditConfig(
                enabled=True,
                track_field_changes=True,
                include_snapshot=True,
                exclude_fields={"password_hash"},  # Don't audit sensitive fields
            )
        )

    # CREATE with audit
    user = memory.create_new(
        User,
        {"name": "Alice", "email": "alice@example.com", "status": "active", "password_hash": "ABCDEFG"},
        changed_by="admin@example.com"
    )

    # UPDATE with audit
    memory.update_existing(
        user,
        {"status": "inactive"},
        changed_by="system@example.com",
        audit_metadata={"reason": "Account deactivated", "ticket": "SUPPORT-123"}
    )

    # Query audit trail
    querier = AuditLogQuerier(memory)

    # Get all changes to this user
    logs = querier.get_logs_for_resource("User", user.resource_id)
    for log in logs:
        print(f"{log.operation} by {log.changed_by} at {log.created_at}")
        if log.changed_fields:
            for field, change in log.changed_fields.items():
                print(f"  {field}: {change['old']} → {change['new']}")

    # Track field history
    email_history = querier.get_field_history("User", user.resource_id, "email")

    # Find all changes by specific user
    admin_changes = querier.get_logs_by_changer("admin@example.com")
    ```

## [13.2.0]  2025-10-08

### Fixed

* **Nested Pydantic Models in Compressed Resources**: Fixed Pydantic serialization warnings when resources with
  `compress_data=True` contained nested Pydantic models (e.g., `address: Address` where `Address` is a Pydantic
  BaseModel) as regular (non-blob) fields:
    - **Root Cause**: The compression path was calling `model_dump()` → `model_copy()` → `compress_model_content()`,
      which converted nested Pydantic instances to dicts before calling `model_dump_json()`, triggering warnings about
      unexpected input types
    - **Solution**: Changed compression path to call `model_dump_json()` directly on the original instance, bypassing
      the round-trip that loses type information
    - **Impact**: Eliminates serialization warnings for nested models with `set` fields, `datetime` fields, or any other
      Pydantic-managed types
    - **Applies To**: Both `DynamoDbResource` and `DynamoDbVersionedResource` with `compress_data=True`
    - **Backward Compatible**: No changes to serialization format, only to the serialization process
    - Test coverage added in `test_nested_pydantic_models.py` covering compressed/uncompressed and
      versioned/non-versioned resources

## [13.1.0] 2025-10-08

### Fixed

* **Blob Field Serialization with Empty Sets**: Fixed critical serialization bug where Pydantic models containing `set`
  fields with empty sets (`set()`) were being incorrectly serialized as string literals `"set()"` instead of JSON
  arrays, causing TypeAdapter validation failures during blob loading. The fix implements proper type-aware
  serialization using Pydantic's `TypeAdapter`:
    - **Root Cause**: `json.dumps(value, default=str)` in `blob_storage.py` was converting Python `set()` objects to
      their string representation rather than JSON-compatible lists
    - **Solution**: Added `field_annotation` parameter to `S3BlobStorage.put_blob()` and uses `TypeAdapter.dump_json()`
      to serialize with full type information
    - **Flexibility**: Handles any complex type annotation: `list[BaseModel]`, `dict[str, BaseModel]`,
      `dict[str, list[dict[str, BaseModel]]]`, `Optional[...]`, etc.
    - **Serialization Flow**:
        - Extracts blob field values as Pydantic instances before `model_dump()` to preserve types
        - Passes field annotations from `resource.model_fields[field_name].annotation` to storage layer
        - Uses `TypeAdapter.dump_json()` for perfect symmetry with existing `TypeAdapter.validate_python()`
          deserialization
    - **Auto-Detection Fallback**: When annotations unavailable, automatically detects Pydantic models and handles them
      appropriately
    - **Backward Compatibility**:
        - Old data without sets continues to work
        - Old data with empty sets was already broken and requires re-saving
        - New data works perfectly with all complex types including sets
    - **Performance**: Eliminates Pydantic serialization warnings by preserving model instances throughout serialization
      pipeline
    - **None Handling**: Properly distinguishes between `None` (no blob stored) and empty collections like `[]` or
      `set()`
    - **Version Preservation**: Correctly maintains blob version references when updating resources without modifying
      blob fields
    - Comprehensive test coverage added in `test_blob_empty_set_issue.py` with cache clearing to verify actual S3
      round-trip behavior

## [13.0.0] 2025-10-06

### Fixed

* **Blob Field Type Reconstruction**: Fixed blob fields containing `list[BaseModel]` to properly reconstruct Pydantic
  model instances when loaded from S3. Previously, Pydantic models in lists were deserialized as dictionaries and not
  reconstructed, causing attribute access errors. Now uses Pydantic's `TypeAdapter` to validate and reconstruct proper
  types for all blob field data during loading.
    - Affects any blob field containing Pydantic models (e.g., `list[MyModel]`, `Optional[MyModel]`, etc.)
    - Comprehensive test coverage added for both compressed and uncompressed blob fields with Pydantic models

## [12.8.0] 2025-10-02

### Added

* **Adaptive Filter Efficiency Tracking and Learned Query Multiplier**: Dramatically reduces DynamoDB API calls when
  using filter expressions by learning filter selectivity and adaptively adjusting query batch sizes:
    - **Filter Efficiency Tracking**: Automatically tracks the effectiveness of DynamoDB filter expressions
    - **Learned Multiplier**: After the first query, the system calculates actual filter efficiency and dynamically
      adjusts the query multiplier for subsequent paginated calls
    - **Intelligent Batch Sizing**: Uses observed efficiency to fetch appropriate amounts of data (e.g., 20%
      efficiency → multiplier of 5x)
    - **Minimum Batch Size**: Enforces a floor of 50 items per query to prevent tiny API calls late in recursion
    - **New PaginatedList Fields**:
        - `filter_efficiency`: Float (0.0-1.0) showing percentage of scanned items that matched the filter
        - `total_items_scanned`: Total DynamoDB items examined across all API calls
    - **Performance Improvement**: Reduces API calls by 60-75% for heavily filtered queries
    - **Example**: With 20% filter match rate requesting 100 items:
        - **Before**: 15 API calls with diminishing returns
        ```
        [I] Beginning paginated dynamodb query
        [D] query_limit=15
        [D] Getting more data! Want 5 more result(s)
        [I] Beginning paginated dynamodb query
        [D] query_limit=15
        [D] Getting more data! Want 4 more result(s)
        ... (13 more API calls)
        [I] Completed dynamodb query; items_returned=5 api_calls_required=15
        ```
        - **After**: 2 API calls with learned efficiency
        ```
        [I] Beginning paginated dynamodb query
        [D] First call with default filter_limit_multiplier=3, query_limit=15
        [D] Filter efficiency: this_call=4.00%, running_avg=4.00%, scanned=50, matched=2
        [D] Getting more data! Want 3 more result(s)
        [I] Beginning paginated dynamodb query
        [D] Using learned multiplier: efficiency=4.00%, multiplier=25, query_limit=75
        [D] Filter efficiency: this_call=6.00%, running_avg=4.60%, scanned=50, matched=3
        [I] Completed dynamodb query; items_returned=5 total_scanned=100 api_calls_required=2 filter_efficiency=0.05
        ```
    - Fully backward compatible - existing code benefits automatically without changes
    - Configurable initial `filter_limit_multiplier` still supported for fine-tuning first query
    - Works with both `filter_expression` (DynamoDB-level) and `filter_fn` (Python-level) filtering

## [12.5.1] 2025-09-25

### Changed

* Minor logging tweak

## [12.5.0] 2025-09-17

### Added

* **Blob Storage Caching**: Added comprehensive caching layer to S3 blob storage for improved performance with
  frequently accessed blobs:
    - LRU (Least Recently Used) eviction policy using OrderedDict for efficient memory management
    - Configurable cache size limits (total size and per-item limits)
    - Optional TTL (Time To Live) support for automatic cache expiration
    - Thread-safe implementation with proper locking mechanisms
    - Comprehensive cache statistics tracking (hits, misses, evictions, hit rate)
    - Cache management methods: `clear_cache()`, `warm_cache()`, `get_cache_stats()`, `get_cache_info()`
    - Automatic cache population on `put_blob()` and cache checking on `get_blob()`
    - Automatic cache invalidation when blobs are deleted
    - Configurable via S3BlobStorage constructor parameters:
        - `cache_enabled` (default: True)
        - `cache_max_size_bytes` (default: 100MB)
        - `cache_max_items` (default: 1000)
        - `cache_ttl_seconds` (default: 15 minutes)
        - `cache_max_item_size_bytes` (default: 1MB)
    - Fully backward compatible - no API changes required
    - Significant performance improvements for frequently accessed blobs by eliminating redundant S3 API calls

## [12.4.0] 2025-09-16

### Added

* **Transparent Float Support**: Added automatic float-to-Decimal conversion for DynamoDB compatibility:
    - Float fields in Pydantic models now work seamlessly with DynamoDB's Decimal requirement
    - Automatic conversion of float values to Decimal during serialization
    - Preserves float precision through string-based Decimal conversion
    - Different behavior for compressed vs. uncompressed resources:
        - Compressed resources: All float values preserved through JSON serialization
        - Uncompressed resources: Top-level float fields work, lists of floats work, dict float values return as Decimal
    - Full backward compatibility - existing code continues to work without changes
    - Note: Floats in generic dicts return as Decimal from DynamoDB (use `List[float]` for automatic conversion)

## [12.3.0] 2025-09-16

### Added

* **Tuple-Based GSI Configuration**: Enhanced GSI configuration to support defining both partition and sort keys with a
  single method:
    - New tuple format: `("gsi3pk", "gsi3sk"): method_returning_tuple` in `get_gsi_config()`
    - Methods can return `tuple[str, str] | None` to set both pk and sk values atomically
    - Useful for correlated index values that should always be set together
    - Maintains full backward compatibility with existing single-field GSI configurations
    - Example:
      ```python
      @classmethod
      def get_gsi_config(cls) -> dict:
          return {
              "gsi3": {("gsi3pk", "gsi3sk"): cls._get_gsi3_values}
          }

      def _get_gsi3_values(self) -> tuple[str, str] | None:
          if self.active:
              return (f"user#{self.username}", self.last_activity.isoformat())
          return None
      ```

## [12.2.0] 2025-09-10

### Added

* **TTL (Time To Live) Support**: Added support for automatic TTL management on DynamoDB resources:
    - New `ResourceConfig` options: `ttl_field` and `ttl_attribute_name` to configure TTL behavior
    - Both fields must be set together for TTL to be enabled
    - Supports two TTL value types:
        - `datetime`: Absolute expiration timestamp
        - `int`: Seconds from `created_at` time (relative expiration)
    - TTL applies to both versioned and non-versioned resources
    - For versioned resources, TTL is set on all items (v0 and version history)
    - TTL attributes are automatically excluded when reconstructing resources from DynamoDB
    - Example: `ResourceConfig(ttl_field="expires_at", ttl_attribute_name="ttl")`

## [12.1.0]

### Added

* **Read-Only Repository Classes**: Introduced `ReadOnlyResourceRepository` and `ReadOnlyVersionedResourceRepository`
  classes for safe, read-only access to resources:
    - `ReadOnlyResourceRepository` in `simplesingletable.extras.readonly_repository` provides read-only access to
      standard resources
    - `ReadOnlyVersionedResourceRepository` in `simplesingletable.extras.readonly_versioned_repository` provides
      read-only access to versioned resources with version querying capabilities
    - Both classes expose only safe read operations (`get()`, `read()`, `list()`) and hide all mutation methods
    - `ReadOnlyVersionedResourceRepository` additionally provides `list_versions()` and `get_version()` methods for
      version inspection
    - Useful for services and components that should only have read access to data, ensuring data integrity at the
      repository level

## [12.0.1] 2025-08-22

### Fixed

* **Pagination with Blob Fields**: Fixed a TypeError that occurred when building LastEvaluatedKey during paginated
  queries on resources with blob fields. When `to_dynamodb_item()` returns a tuple `(db_item, blob_data)` for resources
  with blob storage configured, the pagination logic now correctly extracts just the db_item portion before building the
  LastEvaluatedKey.

## [12.0.0] 2025-08-20

### Changed

* **Potential breaking change** - to_dynamodb_item no longer passed `exclude_none=True` when serializing to the DynamoDb
  Item

## [11.3.0] 2025-08-15

### Changed

* **Code Refactoring - Eliminated Duplication**: Extracted ~150 lines of duplicated code between `DynamoDbResource` and
  `DynamoDbVersionedResource` into their base class `BaseDynamoDbResource`. The refactoring introduces several protected
  helper methods:
    - `_extract_blob_fields()` - Handles blob field extraction from model data
    - `_apply_gsi_configuration()` - Applies dynamic GSI configuration and legacy GSI methods
    - `_add_blob_metadata()` - Manages blob metadata in DynamoDB items
    - `_build_resource_from_data()` - Constructs resources from DynamoDB data with blob handling
    - `_get_excluded_dynamodb_keys()` - Provides consistent key filtering for DynamoDB-specific attributes

## [11.2.1] 2025-08-15

### Fixed

* **ResourceConfig Inheritance for Versioned Resources**: Fixed the `compress_data` resource configuration to properly
  respect subclass settings. The `__pydantic_init_subclass__` method was moved from the base class to the specific
  resource classes (`DynamoDbResource` and `DynamoDbVersionedResource`) to ensure that subclasses correctly inherit and
  merge their parent's default configurations. This fix ensures that:
    - Non-versioned resources default to `compress_data=False`
    - Versioned resources default to `compress_data=True`
    - Subclasses can override these defaults and their settings will be properly respected
    - The `to_dynamodb_item()` and `from_dynamodb_item()` methods now correctly check the `compress_data` setting before
      compressing/decompressing data

## [11.2.0] 2025-08-15

### Fixed

* **Blob Field Preservation for Versioned Resources**: Fixed the critical issue where blob field metadata was lost when
  updating versioned resources without modifying the blob fields. The fix introduces blob version references to track
  which S3 version each blob field points to:
    - Added `_blob_versions` mapping to track S3 blob version references for each field
    - Modified `to_dynamodb_item()` to always include `_blob_fields` metadata when blob fields are configured,
      regardless of whether data exists
    - Updated `create_new()` and `update_existing()` to properly set and preserve blob version references
    - Enhanced `load_blob_fields()` to use the correct S3 version when loading blobs based on version references
    - Fixed blob placeholder creation to only create placeholders for fields with actual blob data (not cleared fields)
    - Now safe to use `load_blobs=True` even when no blobs exist - no errors will occur
    - Maintains full backward compatibility - existing resources without `_blob_versions` continue to work correctly

  This ensures that blob fields remain accessible across all versions without duplicating unchanged data in S3.

### Added

* **Real S3 Integration Tests**: Added comprehensive integration test suite using MinIO for testing blob storage with
  actual S3 operations:
    - Added MinIO service to `docker-compose.yml` for local S3-compatible storage
    - Added `test_blob_storage_integration.py` with full integration tests covering all blob storage scenarios
    - Tests verify actual S3 operations including blob creation, retrieval, versioning, and deletion
    - Provides confidence that blob storage works correctly with real S3-compatible services

## [11.1.1] 2025-08-14

### Fixed

* **Blob Storage Bugfixes**: Fixed critical issues with the S3 blob storage feature introduced in v11.1.0:
    - Fixed version comparison when updating versioned resources with blob fields. Changed from object equality check to
      version number comparison to avoid false mismatches when blob placeholders differ.
    - Fixed `_blob_placeholders` initialization using Pydantic's `PrivateAttr` instead of `__init__` for proper private
      attribute handling and to prevent serialization issues.
    - Fixed blob field placeholder handling in paginated queries (`list_type_by_updated_at`, etc.) to correctly set
      placeholders when loading items from query results.
    - Fixed version number parsing in paginated queries - now correctly handles Decimal values from DynamoDB instead of
      assuming string format with 'v' prefix.

  **Known Limitation**: When updating a versioned resource without modifying its blob fields, the blob field metadata is
  not preserved in the new version. This means blob fields become regular `None` values after such updates. To preserve
  blob references, you must re-supply the blob data in the update. This will be addressed in a future release.

## [11.1.0] 2025-08-14

### Added

* **S3 Blob Storage Support**: Added comprehensive support for storing large fields in S3 instead of DynamoDB, enabling
  efficient storage of large data while maintaining fast query performance.
    - New `BlobFieldConfig` type for configuring blob field behavior (compression, content type, size limits)
    - Extended `ResourceConfig` with `blob_fields` configuration option
    - Created `S3BlobStorage` module for handling all S3 operations
    - Lazy loading of blob fields with `load_blobs` parameter and `load_blob_fields()` method
    - Full support for both versioned and non-versioned resources
    - Automatic compression with configurable gzip option
    - Size limit enforcement per field
    - Automatic cleanup of S3 blobs when resources are deleted
    - Complete backward compatibility - existing code works without changes

  Example usage:
  ```python
  class MyResource(DynamoDbResource):
      title: str
      large_data: Optional[dict] = None  # Stored in S3
      
      resource_config = ResourceConfig(
          blob_fields={
              "large_data": BlobFieldConfig(
                  compress=True,
                  content_type="application/json",
                  max_size_bytes=10 * 1024 * 1024  # 10MB limit
              )
          }
      )
  
  # Initialize with S3
  memory = DynamoDbMemory(
      logger=logger,
      table_name="my-table",
      s3_bucket="my-bucket",
      s3_key_prefix="blobs"  # optional
  )
  
  # Create - large_data automatically goes to S3
  resource = memory.create_new(MyResource, {
      "title": "Test",
      "large_data": {"huge": "dataset"}
  })
  
  # Read without blobs (fast)
  doc = memory.get_existing(id, MyResource)
  
  # Load blobs when needed
  doc.load_blob_fields(memory)
  ```

## [11.0.1] 2025-08-08

### Fixed

* **GSI Callable Handling**: Fixed issue where GSI callable functions returning `None` would incorrectly add fields with
  `None` values to DynamoDB items, causing validation errors. Now, when a GSI callable returns `None`, the corresponding
  field is properly excluded from the DynamoDB item.

## [11.0.0] 2025-08-08

### Changed

* **BREAKING: GSI Configuration**: Major refactoring of the GSI configuration system.
    - **GSI Configuration Breaking Change**: The GSI configuration format has changed from nested dictionaries with
      `"pk"` and `"sk"` keys to a flat dictionary structure where keys are the actual DynamoDB attribute names:
      ```python
      # Old format (still works via legacy methods)
      gsi_config = {
          "gsi1": {"pk": lambda self: f"owner#{self.owner}", "sk": lambda self: self.created_at.isoformat()}
      }
      
      # New format (required for classvar/classmethod approach)
      gsi_config = {
          "gsi1": {
              "gsi1pk": lambda self: f"owner#{self.owner}",
              "gsi1sk": lambda self: self.created_at.isoformat()
          }
      }
      ```
    - Simplified dynamic GSI field iteration to support arbitrary key names and both callables and static values
    - Updated GSI field exclusion logic in `from_dynamodb_item()` to dynamically handle any configured GSI fields

  **Note**: This is a breaking change for the GSI configuration feature introduced in v8.0.0 and v10.1.0, but since this
  feature
  was very recently added and has limited adoption, the impact should be minimal.

## [10.1.0] - 2025-08-08

### Added

* **GSI Configuration via Classmethod Override**: Added ability to override GSI configuration using a classmethod
  `get_gsi_config()` in addition to the existing classvar approach. This provides more flexibility for dynamic
  GSI configuration scenarios:
  ```python
  class MyResource(DynamoDbResource):
      @classmethod
      def get_gsi_config(cls) -> dict:
          # Dynamic GSI configuration logic here
          return {
              "gsi1": {"pk": lambda self: f"owner#{self.owner}", "sk": None},
          }
  ```
    - The classmethod takes precedence over the classvar when both are defined
    - Maintains full backward compatibility with existing classvar and legacy method approaches
    - Useful for cases where GSI configuration needs to be computed dynamically or based on environment

## [10.0.0] - 2025-08-08

### Changed

* **Refactored Version Limit Configuration**: Moved `max_versions` configuration from `model_config` to
  `resource_config`
  for better separation of concerns and consistency with other resource-level settings. This change:
    - Aligns version limiting with other resource configuration options like `compress_data`
    - Provides a cleaner API by separating Pydantic model configuration from resource-specific settings
    - Maintains backward compatibility through automatic config merging in subclasses

## [9.1.0] - 2025-08-01

### Added

* **Versioned Repository with Version Management API**: New `VersionedResourceRepository` class in
  `simplesingletable.extras.versioned_repository` extends the repository pattern to provide comprehensive version
  management capabilities for `DynamoDbVersionedResource` models.

## [9.0.0] - 2025-07-29

### Added

* **Explicit Field Clearing in Updates**: Added `clear_fields` parameter to update methods, enabling explicit clearing
  of optional fields to `None`. This solves the common REST API design problem where there's no way to distinguish
  between "don't change this field" vs "clear this field to null":
  ```python
  # Clear an optional field to None
  updated = repo.update(
      existing_resource,
      {"name": "New Name"},
      clear_fields={"expires_at", "description"}
  )
  ```
    - Supported in both `DynamoDbMemory.update_existing()` and `ResourceRepository.update()`
    - Works with both versioned and non-versioned resources
    - Maintains backward compatibility - existing code continues to work unchanged

### Fixed

* **🚨 CRITICAL: Version Limit Enforcement Bug with Double-Digit Versions**: Fixed a critical bug in the
  `max_versions` functionality for `DynamoDbVersionedResource` where version numbers ≥10 were incorrectly
  deleted due to lexicographical sorting of version strings. Previously, when versions exceeded 9:
    - Version "v10" would sort before "v2" lexicographically
    - This caused the wrong versions to be deleted when enforcing `max_versions` limits
    - Resources would fail to update once reaching version 10

  **Impact**: This bug affected any versioned resources with `max_versions` configured that reached 10+ versions.
  The fix changes the sorting logic in `enforce_version_limit()` to sort by actual version numbers instead of
  version string keys, ensuring the most recent versions are always preserved correctly.

  **Migration**: No migration required - the fix is backward compatible and automatically resolves the issue.

## [8.2.0] - 2025-07-15

### Added

* **Repository Pattern Interface**: New `ResourceRepository` class in `simplesingletable.extras.repository` provides a
  simplified CRUD interface on top of `DynamoDbMemory`. Features include:
    - Type-safe operations with Pydantic schema validation for create/update operations
    - Support for both versioned and non-versioned resources
    - Flexible ID generation with optional override functions
    - Default object creation with customizable factory functions
    - Traditional repository methods: `create()`, `get()`, `read()`, `update()`, `delete()`, `list()`, `get_or_create()`
    - Comprehensive logging for debugging and monitoring

## [8.1.1] - 2025-07-15

### Added

* **Versioned Resource Deletion**: Added support for deleting versioned resources with new `delete_existing()` method
  that handles both specific version deletion and automatic v0 cleanup, plus `delete_all_versions()` method for bulk
  deletion.

## [8.0.0] - 2025-07-15

### Added

* **Dynamic GSI Configuration**: Introduced declarative index configuration system using `gsi_config` class variable on
  resource models. This replaces hardcoded GSI logic and makes it easy to define custom indices:
  ```python
  gsi_config = {
      'gsi1': {
          'pk': lambda self: f"category#{self.category}",
          'sk': lambda self: self.created_at.isoformat(),
      }
  }
  ```
* **Version Limit Enforcement**: Added `max_versions` configuration for `DynamoDbVersionedResource` to automatically
  clean up old versions:
  ```python
  model_config = ConfigDict(extra="forbid", max_versions=5)
  ```
* **Improved Transaction Error Handling**: New `transact_write_safe()` function provides detailed error messages when
  DynamoDB transactions fail, making debugging much easier.
* **Dynamic Pagination Helper**: Added `build_lek_data()` function that dynamically constructs LastEvaluatedKey based on
  index configuration, eliminating 30+ lines of hardcoded logic.

### Changed

* Refactored `paginated_dynamodb_query` to use the new dynamic pagination helper, removing hardcoded index handling.
* Updated `to_dynamodb_item()` methods to support both new dynamic GSI configuration and legacy GSI methods for backward
  compatibility.
* All DynamoDB transactions now use the safer error handling wrapper.

### Fixed

* GSI field exclusion in `from_dynamodb_item()` now dynamically handles configured indices instead of using a hardcoded
  list.

## [7.0.0]

### Changed

* Enhanced `paginated_dynamodb_query` and `list_type_by_updated_at` to properly support boto3's ConditionBase for filter
  expressions. This allows using `Attr` conditions (e.g., `Attr('status').eq('active')`) which automatically handle
  expression attribute names and values, making filtering safer and more convenient.

## [6.0.0] 2025-06-04

### Fixed

* Bugfix for pagination calls on non-versioned resources.

## [5.3.0] 2025-01-31

### Added

* Added a V2 Habit tracker with better data storage.

## [5.2.0] 2025-01-22

### Added

* Added new "extra" `habit_tracker`.

## [5.1.0] 2024-12-10

### Added

* Added support for deleting Form columns.

## [5.0.0] 2024-10-18

### Added

* Added MANIFEST.in to exclude demo streamlit app.

## [4.1.0] 2024-06-12

### Added

* FormDataMapping now has a `get_item_by_key` function with an `ignore_hidden_columns` parameter, useful is one needs to
  do something to all data in a Form regardless of column visibility, such as a metadata update.

## [4.0.2] 2024-04-23

### Fixed

* FormEntry : Bugfix for gsi2 key calculation.

## [4.0.1] 2024-04-23

### Fixed

* FormDataManager now passes its logger value into the created FormDataMapping object when calling `get_mapping`.

## [4.0.0] 2024-04-16

### Changed

* FormEntry : Modify gsi2 usage to allow row id lookup across groups

## [3.4.0] 2024-04-15

### Added

* FormEntry : Utilize gsi2 to track all data for a group / row combination, allowing efficient retrieval for a single
  row.

## [3.3.1] 2024-03-18

### Fixed

* Added `exhaust_pagination` to the core imports in `__init__.py`.

## [3.3.0] 2024-03-18

### Added

* Implemented hide_columns_by_group for controlling column visibility in `form_data` extra.

## [3.2.1] 2024-03-18

### Fixed

* Added `PaginatedList` to the core imports in `__init__.py`.

## [3.2.0] 2024-03-05

### Added

* Added a new utility function to create the standard dynamodb table, given a dynamodb resource object.

### Changed

* Re-organized the core DynamoDbMemory code into a module (out of `__init__.py`) to enhance logging.

## [3.1.0] 2024-03-01

### Added

* Introduce new `form_data` extra for managing data with spreadsheet-like access patterns.

## [3.0.0] 2024-02-23

### Removed

* Removed deprecated aliases for resources and memory classes. Now, use the standard names: DynamoDbMemory,
  DynamoDbResource, and DynamoDbVersionedResource for all references.

## [2.3.2] 2024-02-12

### Fixed

* Fixed bug with computing database item size again.

## [2.3.1] 2024-02-12

### Fixed

* Fixed bug with computing database item size.

## [2.3.0] 2024-02-12

### Added

* Added a new "extras" sub-package; added new Singleton resource models for storing / retrieving things that should only
  be in the database once, like application configuration.
* Added a "use_case_examples" top-level folder with examples of various features and access patterns; currently includes
  a single script for the Singleton objects.

## [2.2.0] 2023-12-12

### Added

* All resource types now support overriding the default gsitype sk value (`updated_at.isoformat()`) via the
  method `db_get_gsitypesk` to enable alternative access patterns on the `gsitype` index.

## [2.1.0] 2023-12-12

### Added

* Added `delete_existing` method for non-versioned resources.

## [2.0.0] 2023-12-08

### Changed

* Breaking change; switched to the full class name by default for the custom resource identifier, rather than just the
  extracted capital letters.

## [1.6.0] 2023-11-15

### Added

* Add `resource_config` ClassVar to begin exposing configuration of resources.

### Changed

* Renamed models and memory class for consistent casing convention; left backwards compatible names in place as well.

## [1.5.1] 2023-11-06

### Fixed

* Pinned `pydantic` to a minimum working version.

## [1.5.0] 2023-11-06

### Added

* Implemented optional automated stats tracking by object data_class

### Changed

* Atomic counter increments on non-versioned resources now modifies the `updated_at` attribute and can be utilized with
  dictionary-based counters.

## [1.4.0] 2023-11-06

### Added

* Added support for a non-versioned resource, including methods for atomic counters and set manipulation.
* Added example streamlit_app.py as the start of some documentation.

### Changed

* Refactored codebase a bit, started `models.py`

## [1.3.0] 2023-10-26

### Added

* Added Change Log

### Changed

* Now uses a consistent read after updating a versioned item.

### Deprecated

### Removed

### Fixed

### Security

## [1.2.0] - 2023-10-26

### Added

* Added docstring for paginated query fn

### Fixed

* Pagination key is now properly returned when max_api_calls is reached during a query.

## [1.1.0] - 2023-10-16

### Changed

* Eliminated `pydantic<2` restriction from requirements.

## [1.0.0] - 2023-10-16

### Added

* Initial Release of library
