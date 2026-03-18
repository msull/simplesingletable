# simplesingletable — Complete Usage Guide

A Python library providing a Pydantic-based abstraction layer for AWS DynamoDB, implementing single-table design patterns. Optimized for small-to-medium scale applications with automatic resource versioning, ULID-based ID generation, and comprehensive secondary access pattern support.

**Package:** `simplesingletable`
**Python:** 3.10+
**Dependencies:** boto3, pydantic v2, ulid-py, humanize

---

## Table of Contents

1. [DynamoDbMemory — Main Interface](#1-dynamodbmemory--main-interface)
2. [Base Resource Classes](#2-base-resource-classes)
3. [ResourceConfig](#3-resourceconfig)
4. [GSI Configuration](#4-gsi-configuration)
5. [Repository Pattern (v2)](#5-repository-pattern-v2)
6. [Singleton Pattern](#6-singleton-pattern)
7. [Querying & Pagination](#7-querying--pagination)
8. [Filtering](#8-filtering)
9. [Blob Storage](#9-blob-storage)
10. [Audit Logging](#10-audit-logging)
11. [TTL Support](#11-ttl-support)
12. [Atomic Operations](#12-atomic-operations)
13. [Batch Operations](#13-batch-operations)
14. [Transactions](#14-transactions)
15. [LocalStorageMemory](#15-localstoragememory)
16. [Table Creation & Setup](#16-table-creation--setup)
17. [Common Production Patterns](#17-common-production-patterns)

---

## 1. DynamoDbMemory — Main Interface

`DynamoDbMemory` is the primary interface for all CRUD operations. It is a `@dataclass`.

```python
from simplesingletable import DynamoDbMemory

memory = DynamoDbMemory(
    logger=logger,                    # Required: logging.Logger instance
    table_name="my-table",            # Required: DynamoDB table name
    endpoint_url=None,                # Optional: for DynamoDB Local (e.g., "http://localhost:8000")
    connection_params=None,           # Optional: dict passed to boto3 (aws_access_key_id, etc.)
    track_stats=True,                 # Optional: track operation statistics
    s3_bucket=None,                   # Optional: S3 bucket for blob storage
    s3_key_prefix=None,               # Optional: prefix for S3 blob keys
    audit_table_name=None,            # Optional: separate table for audit logs
    audit_endpoint_url=None,          # Optional: endpoint for audit table
    audit_connection_params=None,     # Optional: connection params for audit table
)
```

### FastAPI Dependency Injection (Common Pattern)

```python
_memory = None

def get_dynamo_db_memory(
    logger: logging.Logger = Depends(lambda: logger),
    dynamodb_table_name: str = Depends(lambda: os.environ["DYNAMODB_TABLE_NAME"]),
    s3_bucket_name: str = Depends(lambda: os.environ["S3_BUCKET_NAME"]),
) -> DynamoDbMemory:
    global _memory
    if _memory is None:
        _memory = DynamoDbMemory(
            logger=logger,
            table_name=dynamodb_table_name,
            s3_bucket=s3_bucket_name,
            s3_key_prefix="appBlobStore",
        )
    return _memory
```

### Core CRUD Methods

```python
# CREATE — returns the created resource
resource = memory.create_new(
    MyResource,                        # Resource class
    {"field1": "value", ...},          # dict or Pydantic BaseModel
    override_id=None,                  # Optional: custom resource_id instead of auto-ULID
    changed_by=None,                   # Optional: for audit logging
    audit_metadata=None,               # Optional: dict of extra audit context
)

# READ — returns None if not found
resource = memory.get_existing(
    "resource_id_here",
    MyResource,
    version=0,                         # 0 = latest (versioned resources only)
    consistent_read=False,
    load_blobs=False,                  # If True, loads blob fields from S3
)

# READ — raises ValueError if not found
resource = memory.read_existing("resource_id_here", MyResource)

# UPDATE — returns updated resource
updated = memory.update_existing(
    existing_resource,                 # The resource object to update
    {"field1": "new_value"},           # dict or Pydantic BaseModel
    clear_fields={"optional_field"},   # Optional: explicitly set fields to None
    changed_by=None,                   # Optional: for audit logging
    audit_metadata=None,
)

# DELETE
memory.delete_existing(
    existing_resource,
    changed_by=None,
    audit_metadata=None,
)
```

### Version-Specific Methods

```python
# Get all versions (newest first)
versions = memory.get_all_versions("resource_id", MyVersionedResource)

# Delete all versions
memory.delete_all_versions("resource_id", MyVersionedResource)

# Restore a previous version (creates new version with old content)
restored = memory.restore_version(
    "resource_id",
    MyVersionedResource,
    version=2,                         # Version number to restore
    changed_by=None,
    audit_metadata=None,
)
```

---

## 2. Base Resource Classes

### Imports

```python
# Core classes
from simplesingletable import DynamoDbResource, DynamoDbVersionedResource

# For repository pattern (recommended aliases)
from simplesingletable.extras.repository import ResourceRepository
from simplesingletable.extras.versioned_repository import VersionedResourceRepository

# Convenient aliases used in some projects
from simplesingletable.models import DynamoDbResource as Resource
from simplesingletable.models import DynamoDbVersionedResource as VersionedResource
```

### DynamoDbResource (Non-Versioned)

For resources that don't need version history. Default: `compress_data=False`.

```python
class DynamoDbResource(BaseDynamoDbResource, ABC):
    resource_id: str           # Auto-generated ULID if not provided
    created_at: datetime       # Auto-set on creation
    updated_at: datetime       # Auto-set on creation and update

    # ClassVars (override in subclass)
    gsi_config: ClassVar[dict] = {}
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)

    # Config
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
```

**Key methods:**
- `create_new(data, override_id=None)` — class method, creates instance (not persisted)
- `update_existing(data, clear_fields=None)` — creates updated copy
- `dynamodb_lookup_keys_from_id(existing_id)` — returns `{"pk": ..., "sk": ...}`
- `get_unique_key_prefix()` — returns class name by default (override for custom)
- `get_gsi_config()` — returns GSI configuration dict
- `resource_id_as_ulid()` — parse resource_id as ULID
- `created_ago()` / `updated_ago()` — human-readable time strings
- `has_unloaded_blobs()` — True if blob fields not yet loaded
- `get_unloaded_blob_fields()` — list of unloaded blob field names
- `load_blob_fields(memory, fields=None)` — load blob fields from S3

### DynamoDbVersionedResource (Versioned)

For resources with full version history. Default: `compress_data=True`.

```python
class DynamoDbVersionedResource(BaseDynamoDbResource, ABC):
    resource_id: str           # Auto-generated ULID if not provided
    version: int               # Auto-managed, starts at 1
    created_at: datetime       # Auto-set on creation
    updated_at: datetime       # Auto-set on creation and update

    # ClassVars (override in subclass)
    gsi_config: ClassVar[dict] = {}
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=True, max_versions=None)

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
```

**Additional methods:**
- `enforce_version_limit(memory, resource_id)` — delete old versions beyond max_versions

### Version Storage in DynamoDB

- `sk="v0"` — pointer to current version (has GSI attributes for queries)
- `sk="v1"`, `sk="v2"`, etc. — actual version data
- Updates create a new version item and update the v0 pointer

### Defining a Resource

```python
from simplesingletable import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig, AuditConfig
from pydantic import BaseModel, Field
from typing import ClassVar, Optional
from datetime import datetime

# Non-versioned
class UserResource(DynamoDbResource):
    username: str
    email: str
    given_name: str = ""
    family_name: str = ""
    is_active: bool = True
    member_of_groups: set[str] = Field(default_factory=set)

# Versioned
class DocumentResource(DynamoDbVersionedResource):
    title: str
    content: str
    author: str
    tags: list[str] = Field(default_factory=list)

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        max_versions=10,
    )
```

### Create and Update Schemas (for Repository Pattern)

```python
from pydantic import BaseModel

class CreateUser(BaseModel):
    username: str
    email: str
    given_name: str = ""
    family_name: str = ""

class UpdateUser(BaseModel):
    email: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
```

---

## 3. ResourceConfig

`ResourceConfig` is a `TypedDict` controlling resource behavior. Defined as a `ClassVar` on resource classes.

```python
class ResourceConfig(TypedDict, total=False):
    compress_data: bool | None          # Gzip-compress stored data (default: False for Resource, True for Versioned)
    max_versions: int | None            # Max version history to keep (versioned only, None = unlimited)
    blob_fields: dict[str, BlobFieldConfig] | None  # Fields to store in S3 instead of DynamoDB
    ttl_field: str | None               # Field name containing TTL value
    ttl_attribute_name: str | None      # DynamoDB TTL attribute name (e.g., "ttl")
    audit_config: AuditConfig | None    # Audit logging configuration
```

### BlobFieldConfig

```python
class BlobFieldConfig(TypedDict, total=False):
    compress: bool                      # Compress blob before storing in S3
    content_type: str | None            # MIME type (e.g., "image/png", "application/json")
    max_size_bytes: int | None          # Optional size limit
```

### AuditConfig

```python
class AuditConfig(TypedDict, total=False):
    enabled: bool                       # Enable audit logging
    track_field_changes: bool           # Track old/new values per field
    exclude_fields: set[str] | None     # Fields to exclude from audit
    include_snapshot: bool              # Include full resource snapshot
    changed_by_required: bool | None    # Require changed_by parameter
    changed_by_field: str | None        # Field name for user/service identifier
```

### Full ResourceConfig Example

```python
class SpedNote(DynamoDbResource):
    student_id: str
    note_text: str
    service_date: datetime
    signed_by: Optional[str] = None
    archived: bool = False

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            changed_by_required=True,
        ),
    )
```

---

## 4. GSI Configuration

> **IMPORTANT: GSI names and sort keys are fully configurable per-project.**
> The DynamoDB table's GSI names and key schemas are defined in the project's infrastructure code (e.g., CDK, CloudFormation, Terraform), NOT by simplesingletable itself. The library adapts to whatever GSIs exist on the table.
>
> **Before writing GSI code, always check the actual table schema for the repo you're working in.** Look at:
> 1. Other model files in the repo to see what GSI names they use
> 2. The infrastructure/CDK/CloudFormation files for the table definition
> 3. Ask the user if unsure
>
> Common GSI naming conventions vary by project:
> - **Legacy**: `gsi1`, `gsi2`, `gsi3` — these have a built-in assumption that the sort key is `pk` (the table's partition key). This means you cannot define a custom sort key for these indices.
> - **Modern**: `gsi-1`, `gsi-2`, `gsi-3` (or any custom name) — these allow fully custom sort keys (e.g., `gsi-1sk`). Use this naming to get full control over both partition and sort keys.

### Default Table (from create_standard_dynamodb_table)

The utility function `create_standard_dynamodb_table` creates these indices:
- **Primary**: `pk` (HASH) + `sk` (RANGE)
- **gsitype**: `gsitype` (HASH) + `gsitypesk` (RANGE) — auto-populated for listing by type
- **gsi1**: `gsi1pk` (HASH) + `pk` (RANGE) — **sort key is `pk`, not configurable**
- **gsi2**: `gsi2pk` (HASH) + `pk` (RANGE) — **sort key is `pk`, not configurable**
- **gsi3**: `gsi3pk` (HASH) + `gsi3sk` (RANGE) — custom sort key

However, many production projects define their own table with different GSI names and schemas.

### How GSI Key Names Map to Index Names

The convention is: `{index_name}pk` for the partition key, `{index_name}sk` for the sort key. Examples:
- Index `"gsi1"` → attributes `gsi1pk` (partition), sort key is `pk` (legacy hardcoded)
- Index `"gsi-1"` → attributes `gsi-1pk` (partition), `gsi-1sk` (sort key, fully custom)
- Index `"gsi3"` → attributes `gsi3pk` (partition), `gsi3sk` (sort key)

### Static GSI Configuration (ClassVar)

```python
class TaskResource(DynamoDbResource):
    title: str
    category: str
    priority: int
    assigned_to: str
    completed: bool = False

    # Keys in the dict match the GSI index names on the DynamoDB table
    gsi_config: ClassVar[dict] = {
        "gsi-1": {
            "gsi-1pk": lambda self: f"category#{self.category}",
            "gsi-1sk": lambda self: self.title,  # Custom sort key (only works with non-legacy GSI names)
        },
        "gsi-2": {
            "gsi-2pk": lambda self: f"assignee#{self.assigned_to}",
        },
        "gsi3": {
            "gsi3pk": lambda self: f"priority#{self.priority}",
            "gsi3sk": lambda self: self.title,
        },
    }
```

### Dynamic GSI Configuration (classmethod override)

```python
class ApiKey(DynamoDbResource):
    key_hash: str
    username: str
    is_active: bool = True

    @classmethod
    def get_gsi_config(cls):
        return {
            "gsi-1": {"gsi-1pk": lambda self: f"USER#{self.username}"},
            "gsi-2": {"gsi-2pk": lambda self: f"KEYHASH#{self.key_hash}" if self.is_active else None},
        }
```

### Conditional / Sparse GSIs

Return `None` to exclude items from a GSI. This is the recommended pattern for filtering by state:

```python
class TrainingLink(DynamoDbVersionedResource):
    title: str
    url: str
    active: bool = True

    @classmethod
    def get_gsi_config(cls):
        def _gsi1pk_value(self):
            if self.active:
                return cls.get_unique_key_prefix()  # Only index active items
            return None  # Exclude inactive from GSI
        return {"gsi1": {"gsi1pk": _gsi1pk_value}}
```

### Query Helper Pattern (Recommended)

Define classmethods that return kwargs for `paginated_dynamodb_query`:

```python
class ConversationResource(DynamoDbResource):
    username: str
    title: str

    @classmethod
    def get_gsi_config(cls):
        return {
            "gsi3": {
                "gsi3pk": lambda self: f"{cls.get_unique_key_prefix()}#{self.username}",
                "gsi3sk": lambda self: self.updated_at.isoformat(),
            }
        }

    @classmethod
    def query_by_user_kwargs(cls, username: str):
        return {
            "index_name": "gsi3",
            "key_condition": Key("gsi3pk").eq(f"{cls.get_unique_key_prefix()}#{username}"),
        }
```

Usage:
```python
from boto3.dynamodb.conditions import Key

results = memory.paginated_dynamodb_query(
    **ConversationResource.query_by_user_kwargs("john"),
    resource_class=ConversationResource,
    results_limit=20,
)
```

### Date Range Queries via GSI Sort Key

```python
class SpedNote(DynamoDbResource):
    student_id: str
    service_date: datetime

    @classmethod
    def get_gsi_config(cls):
        return {
            "gsi3": {
                "gsi3pk": lambda self: f"{cls.get_unique_key_prefix()}#{self.student_id}",
                "gsi3sk": lambda self: f"{self.service_date.timestamp():.6f}",
            }
        }

    @classmethod
    def query_by_student_kwargs(cls, student_id, start_date=None, end_date=None):
        pk = f"{cls.get_unique_key_prefix()}#{student_id}"
        key_condition = Key("gsi3pk").eq(pk)
        if start_date and end_date:
            key_condition &= Key("gsi3sk").between(
                f"{start_date.timestamp():.6f}",
                f"{end_date.timestamp():.6f}",
            )
        elif start_date:
            key_condition &= Key("gsi3sk").gte(f"{start_date.timestamp():.6f}")
        elif end_date:
            key_condition &= Key("gsi3sk").lte(f"{end_date.timestamp():.6f}")
        return {"index_name": "gsi3", "key_condition": key_condition, "resource_class": cls}
```

### Legacy GSI Methods (Older Pattern)

Some older code uses method overrides instead of `gsi_config`/`get_gsi_config()`:

```python
class MyResource(DynamoDbVersionedResource):
    parent_id: str

    def db_get_gsi1pk(self) -> str | None:
        return f"parent_id#{self.parent_id}"

    def db_get_gsi2pk(self) -> str | None:
        return f"status#{self.status}" if self.is_active else None

    def db_get_gsi3pk_and_sk(self) -> tuple[str, str] | None:
        return (f"category#{self.category}", self.name)
```

---

## 5. Repository Pattern (v2)

The repository pattern wraps `DynamoDbMemory` with typed CRUD operations and optional caching.

### ResourceRepository (Non-Versioned or Versioned)

```python
from simplesingletable.extras.repository import ResourceRepository

class UserRepository(ResourceRepository):
    def __init__(self, dynamodb_memory: DynamoDbMemory, logger):
        super().__init__(
            ddb=dynamodb_memory,           # Required: DynamoDbMemory instance
            model_class=UserResource,      # Required: the resource class
            create_schema_class=CreateUser, # Required: Pydantic model for creation
            update_schema_class=UpdateUser, # Required: Pydantic model for updates
            logger=logger,                 # Optional: logger
            default_create_obj_fn=None,    # Optional: Callable[[str], CreateSchema] for get_or_create
            override_id_fn=None,           # Optional: Callable[[CreateSchema], str] for custom IDs
            cache_ttl_seconds=None,        # Optional: enable TTLCache for get/batch_get
        )
```

**Methods:**

```python
repo.create(obj_in, override_id=None, changed_by=None, audit_metadata=None) -> T
repo.get(id) -> Optional[T]               # Returns None if not found
repo.read(id) -> T                         # Raises ValueError if not found
repo.update(id_or_obj, obj_in, clear_fields=None, changed_by=None, audit_metadata=None) -> T
repo.delete(id, changed_by=None, audit_metadata=None) -> None
repo.get_or_create(id) -> T               # Requires default_create_obj_fn
repo.batch_get(ids: list[str]) -> dict[str, T]  # Uses cache if enabled
repo.list(limit=None) -> list[T]           # List all of this type
repo.clear_cache() -> None                 # Clear TTLCache
```

### VersionedResourceRepository

Extends `ResourceRepository` with version management:

```python
from simplesingletable.extras.versioned_repository import VersionedResourceRepository, VersionInfo

class DocumentRepository(VersionedResourceRepository):
    def __init__(self, dynamodb_memory, logger):
        super().__init__(
            ddb=dynamodb_memory,
            model_class=DocumentResource,
            create_schema_class=CreateDocument,
            update_schema_class=UpdateDocument,
            logger=logger,
        )
```

**Additional methods:**

```python
repo.list_versions(item_id) -> list[VersionInfo]   # All versions with metadata
repo.get_version(item_id, version: int) -> Optional[T]  # Get specific version
repo.restore_version(item_id, version, changed_by=None, audit_metadata=None) -> T
```

**VersionInfo:**
```python
class VersionInfo(BaseModel):
    version_id: str       # "v1", "v2", etc.
    version_number: int
    created_at: datetime
    updated_at: datetime
    is_latest: bool = False
```

### ReadOnlyResourceRepository

Safe read-only access without create/update/delete methods:

```python
from simplesingletable.extras.readonly_repository import ReadOnlyResourceRepository
from simplesingletable.extras.readonly_versioned_repository import ReadOnlyVersionedResourceRepository

class ReadOnlyUserRepo(ReadOnlyResourceRepository):
    def __init__(self, dynamodb_memory, logger=None):
        super().__init__(
            ddb=dynamodb_memory,
            model_class=UserResource,
            logger=logger,
            cache_ttl_seconds=300,  # Optional cache
        )
```

**Methods:**
```python
repo.get(id) -> Optional[T]
repo.read(id) -> T
repo.batch_get(ids) -> dict[str, T]
repo.list(limit=None) -> list[T]
repo.clear_cache() -> None
```

**ReadOnlyVersionedResourceRepository** adds:
```python
repo.list_versions(item_id) -> list[VersionInfo]
repo.get_version(item_id, version) -> Optional[T]
```

### Custom ID and Default Creation Patterns

```python
class UserCheckLogRepository(ResourceRepository):
    def __init__(self, dynamodb_memory, logger):
        def _override_id(obj_in):
            return obj_in.username  # Use username as resource_id

        def _create_default(username):
            return CreateUserCheckLog(username=username)

        super().__init__(
            model_class=UserCheckLog,
            ddb=dynamodb_memory,
            logger=logger,
            create_schema_class=CreateUserCheckLog,
            update_schema_class=UpdateUserCheckLog,
            override_id_fn=_override_id,
            default_create_obj_fn=_create_default,
        )

# Now get_or_create works with custom IDs:
log = repo.get_or_create("john_doe")
```

### Adding Custom Query Methods to Repositories

```python
class ApiKeyRepository(ResourceRepository):
    def __init__(self, dynamodb_memory, logger):
        super().__init__(
            model_class=ApiKey,
            ddb=dynamodb_memory,
            logger=logger,
            create_schema_class=ApiKeyCreateSchema,
            update_schema_class=ApiKeyUpdateSchema,
        )

    def get_by_key_hash(self, key_hash: str) -> Optional[ApiKey]:
        results = list(self.ddb.paginated_dynamodb_query(
            index_name="gsi2",
            key_condition=Key("gsi2pk").eq(f"KEYHASH#{key_hash}"),
            resource_class=ApiKey,
        ))
        return results[0] if results else None

    def get_by_username(self, username: str) -> list[ApiKey]:
        return list(self.ddb.paginated_dynamodb_query(
            index_name="gsi1",
            key_condition=Key("gsi1pk").eq(f"USER#{username}"),
            resource_class=ApiKey,
        ))
```

---

## 6. Singleton Pattern

Singletons use the class name as resource_id and share a `SINGLETON` key prefix.

```python
from simplesingletable.extras.singleton import SingletonResource, SingletonVersionedResource
```

### SingletonResource (Non-Versioned)

```python
class AppConfig(SingletonResource):
    feature_flags: dict[str, bool] = Field(default_factory=dict)
    max_retries: int = 3

# Get or create singleton
config = AppConfig.ensure_exists(memory, consistent_read=True)

# Update singleton
config.feature_flags["new_ui"] = True
config = config.saved_updated_singleton(memory)
```

### SingletonVersionedResource (Versioned)

```python
class ActiveCanvasStudents(SingletonVersionedResource):
    student_ids: list[str] = Field(default_factory=list)
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=True)

    def replace_list(self, memory, new_student_ids):
        return memory.update_existing(self, {"student_ids": new_student_ids})

# Usage
students = ActiveCanvasStudents.ensure_exists(memory)
students = students.replace_list(memory, ["student1", "student2"])
```

**Key behavior:**
- `ensure_exists(memory)` — returns existing singleton or creates a new one
- `saved_updated_singleton(memory)` — updates the singleton in-place
- Versioned singletons check that you're updating from the latest version
- All singletons use `pk=SINGLETON#{ClassName}`, so they share the SINGLETON namespace

---

## 7. Querying & Pagination

### paginated_dynamodb_query — Full Signature

```python
def paginated_dynamodb_query(
    self,
    *,
    key_condition: ConditionBase,
    # The DynamoDB key condition (e.g., Key("gsi1pk").eq("value"))

    resource_class: Type[AnyDbResource] = None,
    # The resource class for deserialization. Provide this OR resource_class_fn.

    resource_class_fn: Callable[[dict], Type[AnyDbResource]] = None,
    # Dynamic class selection based on raw DynamoDB item data.
    # Use when a single query may return multiple resource types.

    index_name: Optional[str] = None,
    # GSI name: "gsitype", "gsi1", "gsi2", "gsi3", or None for primary index.

    filter_expression: Optional[ConditionBase] = None,
    # DynamoDB server-side filter (e.g., Attr("status").eq("active")).
    # Consumes RCUs for scanned items even if they don't match.

    filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
    # Python-level post-retrieval filter. Receives deserialized resource,
    # returns True to include. Applied after DynamoDB filter_expression.

    results_limit: Optional[int] = None,
    # Max results to return. Default: 250 (Constants.SYSTEM_DEFAULT_LIMIT).

    max_api_calls: int = 10,
    # Max DynamoDB API calls before stopping. Default: 10.
    # Prevents runaway queries. Increase for large filtered result sets.

    pagination_key: Optional[str] = None,
    # Base64-encoded pagination key from a previous query's next_pagination_key.

    ascending: bool = False,
    # Sort order. False = newest first (descending). True = oldest first.

    filter_limit_multiplier: int = 3,
    # When filtering, over-fetch by this multiplier to reduce API calls.
    # Adaptive: adjusts based on observed filter efficiency.
) -> PaginatedList[AnyDbResource]
```

**Behavior:**
- Recursively queries until `results_limit` is met, items are exhausted, or `max_api_calls` is reached
- Adaptive multiplier learning: if first call shows low filter efficiency, subsequent calls fetch more
- Supports both DynamoDB-level filtering (`filter_expression`) and Python-level (`filter_fn`)

### list_type_by_updated_at — Full Signature

Convenience wrapper around `paginated_dynamodb_query` that queries the `gsitype` index:

```python
def list_type_by_updated_at(
    self,
    data_class: Type[AnyDbResource],
    *,
    filter_expression: Optional[ConditionBase] = None,
    filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
    results_limit: Optional[int] = None,
    max_api_calls: int = 10,
    pagination_key: Optional[str] = None,
    ascending: bool = False,             # False = newest first
    filter_limit_multiplier: int = 3,
) -> PaginatedList[AnyDbResource]
```

### PaginatedList

`PaginatedList` extends `list` with pagination metadata:

```python
class PaginatedList(list[T]):
    limit: int                           # The requested results_limit
    current_pagination_key: Optional[str]  # Key used for this page
    next_pagination_key: Optional[str]     # Key for next page (None if last page)
    api_calls_made: int = 0              # DynamoDB API calls consumed
    rcus_consumed_by_query: int = 0      # Read capacity units consumed
    query_time_ms: Optional[float]       # Total query time in milliseconds
    filter_efficiency: Optional[float]   # 0.0-1.0, % of scanned items that matched
    total_items_scanned: int = 0         # Total items examined across all API calls
```

Usage:
```python
page1 = memory.list_type_by_updated_at(MyResource, results_limit=20)
for item in page1:
    print(item.resource_id)

if page1.next_pagination_key:
    page2 = memory.list_type_by_updated_at(
        MyResource,
        results_limit=20,
        pagination_key=page1.next_pagination_key,
    )
```

### exhaust_pagination Helper

Generator that iterates through all pages:

```python
from simplesingletable import exhaust_pagination

def query_fn(pagination_key):
    return memory.paginated_dynamodb_query(
        key_condition=Key("gsi1pk").eq("USER#john"),
        index_name="gsi1",
        resource_class=MyResource,
        results_limit=100,
        pagination_key=pagination_key,
    )

all_items = []
for page in exhaust_pagination(query_fn):
    all_items.extend(page)
```

---

## 8. Filtering

### DynamoDB Filter Expressions (Server-Side)

Uses boto3's `Attr` class. Applied at DynamoDB level (still consumes RCUs for scanned items):

```python
from boto3.dynamodb.conditions import Attr

# Equality
results = memory.list_type_by_updated_at(
    MyResource,
    filter_expression=Attr("status").eq("active"),
)

# Comparison
results = memory.list_type_by_updated_at(
    MyResource,
    filter_expression=Attr("priority").gt(3),
)

# Compound (AND)
results = memory.list_type_by_updated_at(
    MyResource,
    filter_expression=Attr("status").eq("active") & Attr("priority").gt(3),
)

# Compound (OR)
results = memory.list_type_by_updated_at(
    MyResource,
    filter_expression=Attr("status").eq("active") | Attr("status").eq("pending"),
)

# Contains (for lists/sets/strings)
results = memory.list_type_by_updated_at(
    MyResource,
    filter_expression=Attr("tags").contains("urgent"),
)

# Between
results = memory.list_type_by_updated_at(
    MyResource,
    filter_expression=Attr("priority").between(3, 7),
)
```

### Python Filter Functions (Post-Retrieval)

For complex filtering not expressible in DynamoDB filter expressions:

```python
results = memory.list_type_by_updated_at(
    MyResource,
    filter_fn=lambda r: r.name.startswith("Project") and len(r.tags) > 2,
)

# Combine both for efficiency (DynamoDB filter reduces data transfer, Python filter for complex logic)
results = memory.list_type_by_updated_at(
    MyResource,
    filter_expression=Attr("status").eq("active"),
    filter_fn=lambda r: r.score > calculate_threshold(r),
)
```

---

## 9. Blob Storage

Blob fields store large data in S3 instead of DynamoDB (400KB item limit).

### Configuration

```python
class DocumentResource(DynamoDbResource):
    title: str
    author: str
    content: Optional[str] = None           # Stored in S3
    image_data: Optional[bytes] = None      # Binary blob

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={
            "content": BlobFieldConfig(
                compress=True,              # Gzip before storing
                content_type="text/plain",
            ),
            "image_data": BlobFieldConfig(
                compress=False,
                content_type="image/png",
                max_size_bytes=10 * 1024 * 1024,  # 10MB limit
            ),
        },
    )
```

### DynamoDbMemory with S3

```python
memory = DynamoDbMemory(
    logger=logger,
    table_name="my-table",
    s3_bucket="my-bucket",
    s3_key_prefix="blobs",    # S3 keys: blobs/{ResourceType}/{id}/{field_name}
)
```

### Usage

```python
# Create with blob data
doc = memory.create_new(DocumentResource, {
    "title": "My Doc",
    "author": "John",
    "content": "Very large text content...",
    "image_data": b"\x89PNG...",
})

# Read WITHOUT loading blobs (fast, metadata only)
doc = memory.get_existing(doc.resource_id, DocumentResource, load_blobs=False)
doc.has_unloaded_blobs()        # True
doc.get_unloaded_blob_fields()  # ["content", "image_data"]
doc.content                     # None (placeholder)

# Read WITH blobs (loads from S3)
doc = memory.get_existing(doc.resource_id, DocumentResource, load_blobs=True)
doc.content                     # "Very large text content..."

# Load specific blob fields on demand
doc = memory.get_existing(doc.resource_id, DocumentResource)
doc.load_blob_fields(memory, fields=["content"])  # Load only content
doc.load_blob_fields(memory)                       # Load all remaining
```

### Server-Side Blob Copy

```python
placeholder = memory.copy_blob(
    source_resource=doc1,
    source_field="content",
    target_resource=doc2,
    target_field="content",
    delete_source=False,
)
```

### Register External S3 Object as Blob

```python
placeholder = memory.register_external_blob(
    resource=doc,
    field_name="content",
    source_s3_key="uploads/user-file.txt",
    content_type="text/plain",
    source_bucket="upload-bucket",  # Optional, defaults to configured bucket
    delete_source=True,
)
```

---

## 10. Audit Logging

### Enable on Resource

```python
class AuditedUser(DynamoDbVersionedResource):
    name: str
    email: str
    password_hash: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,       # Log old/new values per field
            include_snapshot=True,           # Include full resource state
            exclude_fields={"password_hash"},  # Don't log sensitive fields
            changed_by_required=True,       # Require changed_by parameter
        ),
    )
```

### Audit Operations

```python
# Create with audit
user = memory.create_new(
    AuditedUser,
    {"name": "Alice", "email": "alice@example.com", "password_hash": "..."},
    changed_by="admin@example.com",
    audit_metadata={"reason": "New employee onboarding"},
)

# Update with audit
updated = memory.update_existing(
    user,
    {"email": "alice.new@example.com"},
    changed_by="alice@example.com",
)

# Delete with audit
memory.delete_existing(user, changed_by="admin@example.com")
```

### Query Audit Logs

```python
from simplesingletable import AuditLogQuerier

querier = AuditLogQuerier(memory)

# Logs for a specific resource
logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

# Logs for a resource type
logs = querier.get_logs_for_resource_type("AuditedUser", limit=50)

# Filter by operation
logs = querier.get_logs_by_operation("AuditedUser", "UPDATE")

# Filter by who made the change
logs = querier.get_logs_by_changer("admin@example.com")

# Date range filtering
from datetime import datetime, timezone, timedelta
logs = querier.get_logs_for_resource_type(
    "AuditedUser",
    start_date=datetime.now(timezone.utc) - timedelta(days=7),
    end_date=datetime.now(timezone.utc),
)
```

### AuditLog Resource Fields

```python
class AuditLog(DynamoDbResource):
    audited_resource_type: str              # e.g., "AuditedUser"
    audited_resource_id: str
    operation: str                          # "CREATE", "UPDATE", "DELETE", "RESTORE"
    changed_by: Optional[str] = None
    changed_fields: Optional[dict] = None   # {"email": {"old": "old@x.com", "new": "new@x.com"}}
    resource_snapshot: Optional[dict] = None
    audit_metadata: dict = {}
```

---

## 11. TTL Support

DynamoDB TTL automatically deletes expired items.

### Datetime TTL

```python
class ApiKey(DynamoDbResource):
    key_hash: str
    username: str
    expires_at: Optional[datetime] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        ttl_field="expires_at",           # Field containing expiration datetime
        ttl_attribute_name="ttl",         # DynamoDB TTL attribute
    )

# Create with expiration
key = memory.create_new(ApiKey, {
    "key_hash": "abc123",
    "username": "john",
    "expires_at": datetime.now(timezone.utc) + timedelta(hours=24),
})
```

### Integer TTL (seconds from now)

```python
class TempResource(DynamoDbResource):
    content: str
    ttl_seconds: Optional[int] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        ttl_field="ttl_seconds",
        ttl_attribute_name="ttl",
    )

resource = memory.create_new(TempResource, {"content": "temp", "ttl_seconds": 3600})
```

---

## 12. Atomic Operations

Available only for **non-versioned** resources (`DynamoDbResource`).

### Increment Counter

```python
class PageView(DynamoDbResource):
    url: str
    view_count: int = 0
    counts: dict[str, int] = Field(default_factory=dict)  # For mapped counters

# Simple increment
new_value = memory.increment_counter(page_view, "view_count", incr_by=1)

# Nested dict counter (dot notation)
new_value = memory.increment_counter(page_view, "counts.mobile", incr_by=1)
```

### Set Operations

```python
class UserResource(DynamoDbResource):
    username: str
    member_of_groups: set[str] = Field(default_factory=set)

memory.add_to_set(user, "member_of_groups", "admin")
memory.remove_from_set(user, "member_of_groups", "admin")
```

---

## 13. Batch Operations

### batch_get_existing

Fetch multiple resources by ID in a single call. Auto-chunks for >100 items.

```python
# Returns dict[resource_id, resource] — only found items
results = memory.batch_get_existing(
    ids=["id1", "id2", "id3"],
    data_class=MyResource,
    consistent_read=False,
    load_blobs=False,
)

resource1 = results.get("id1")  # None if not found
```

### Repository batch_get

```python
# Uses cache if cache_ttl_seconds is configured
results = repo.batch_get(["id1", "id2", "id3"])
# Returns dict[str, T] — only found items
```

---

## 14. Transactions

Atomic multi-item operations using DynamoDB transactions.

```python
with memory.transaction(
    isolation_level="read_committed",
    auto_retry=True,
    max_retries=3,
) as txn:
    user = txn.create(user_obj)
    profile = txn.create(profile_obj)
    txn.update(OtherResource, resource_id="id", updates={"field": "value"})
    txn.delete(OtherResource, resource_id="id")
    txn.increment(CounterResource, resource_id="id", field="count", amount=1)
```

---

## 15. LocalStorageMemory

File-based storage that mimics DynamoDbMemory interface. For local development and demos without AWS.

```python
from simplesingletable import LocalStorageMemory

memory = LocalStorageMemory(
    logger=logger,
    storage_dir="/tmp/local_db",      # Directory for JSON files
    track_stats=True,
    use_blob_storage=True,            # Enable local blob storage
)

# Same API as DynamoDbMemory:
resource = memory.create_new(MyResource, {"name": "test"})
resource = memory.get_existing(resource.resource_id, MyResource)
updated = memory.update_existing(resource, {"name": "updated"})
memory.delete_existing(resource)

# Queries work too:
results = memory.list_type_by_updated_at(MyResource)
results = memory.paginated_dynamodb_query(
    key_condition=Key("gsi1pk").eq("value"),
    index_name="gsi1",
    resource_class=MyResource,
)
```

---

## 16. Table Creation & Setup

### create_standard_dynamodb_table

Creates a DynamoDB table with all required indices for simplesingletable:

```python
from simplesingletable.utils import create_standard_dynamodb_table
import boto3

dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
table = create_standard_dynamodb_table(
    table_name="my-app-table",
    dynamodb_resource=dynamodb,
)
```

Creates (legacy naming — see GSI Configuration section for modern alternatives):
- **Primary**: `pk` (S, HASH) + `sk` (S, RANGE)
- **gsitype** GSI: `gsitype` (S, HASH) + `gsitypesk` (S, RANGE)
- **gsi1** GSI: `gsi1pk` (S, HASH) + `pk` (S, RANGE) — sort key forced to `pk`
- **gsi2** GSI: `gsi2pk` (S, HASH) + `pk` (S, RANGE) — sort key forced to `pk`
- **gsi3** GSI: `gsi3pk` (S, HASH) + `gsi3sk` (S, RANGE)
- Billing: PAY_PER_REQUEST

> **Note:** Production projects often define their own table with custom GSI names (e.g., `gsi-1`, `gsi-2`) to get full control over sort keys. Always check the actual infra code for the project you're working in.

### Test Fixture Pattern

```python
@pytest.fixture(scope="session")
def dynamodb_table(dynamodb_endpoint):
    resource = boto3.resource(
        "dynamodb",
        endpoint_url=dynamodb_endpoint,
        aws_access_key_id="unused",
        aws_secret_access_key="unused",
        region_name="us-west-2",
    )
    table = create_standard_dynamodb_table(
        table_name=f"test-{uuid4().hex}",
        dynamodb_resource=resource,
    )
    yield resource.Table(table.table_name)

@pytest.fixture()
def memory(dynamodb_table, dynamodb_endpoint):
    truncate_dynamo_table(dynamodb_table)
    yield DynamoDbMemory(
        logger=logging.getLogger("test"),
        table_name=dynamodb_table.table_name,
        endpoint_url=dynamodb_endpoint,
        connection_params={
            "aws_access_key_id": "unused",
            "aws_secret_access_key": "unused",
            "region_name": "us-west-2",
        },
    )
```

### Utility Functions

```python
from simplesingletable.utils import (
    create_standard_dynamodb_table,
    truncate_dynamo_table,         # Delete all items (scan + batch delete)
    generate_date_sortable_id,     # Generate ULID
    encode_pagination_key,         # Encode DynamoDB LEK to base64
    decode_pagination_key,         # Decode base64 back to LEK
)
```

---

## 17. Common Production Patterns

### Pattern: Service Layer with Repositories

```python
class UserManagementSystem:
    def __init__(self, dynamodb_memory, cognito_pool_id, logger):
        self.user_repo = UserRepository(dynamodb_memory, logger)
        self.group_repo = GroupRepository(dynamodb_memory, logger)

    def create_user(self, create_data):
        existing = self.user_repo.get(create_data.username)
        if existing:
            raise DuplicateUser(f"User {create_data.username} already exists")
        return self.user_repo.create(create_data, override_id=create_data.username)

    def update_user(self, username, update_data):
        user = self.user_repo.read(username)
        return self.user_repo.update(user, update_data)
```

### Pattern: Base Resource Classes

```python
# Non-versioned base
class BaseResource(DynamoDbResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)

# Versioned base
class BaseVersionedResource(DynamoDbVersionedResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        max_versions=5,
    )
```

### Pattern: Enum Status Fields

```python
from enum import StrEnum

class EmailStatus(StrEnum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"

class Email(DynamoDbResource):
    recipient: str
    subject: str
    body_html: str
    status: EmailStatus = EmailStatus.PENDING
    message_id: Optional[str] = None
```

### Pattern: Nested Pydantic Models

```python
class Address(BaseModel):
    street: str
    city: str
    state: str
    zip_code: str

class Order(DynamoDbResource):
    customer_email: str
    shipping_address: Address
    total_amount: float
    status: str = "pending"
```

### Pattern: Paginated API Endpoint

```python
@router.get("/conversations")
def list_conversations(
    username: str,
    limit: int = 20,
    pagination_key: Optional[str] = None,
    memory: DynamoDbMemory = Depends(get_dynamo_db_memory),
):
    results = memory.paginated_dynamodb_query(
        **ConversationResource.query_by_user_kwargs(username),
        resource_class=ConversationResource,
        results_limit=limit,
        pagination_key=pagination_key,
    )
    return {
        "items": [r.model_dump() for r in results],
        "next_pagination_key": results.next_pagination_key,
    }
```

### Pattern: Repository with Blob Loading

```python
class ConversationRepository(ResourceRepository):
    def get_with_blobs(self, resource_id):
        return self.ddb.read_existing(resource_id, self.model_class, load_blobs=True)

    def get_messages(self, conversation_id, load_blobs=True):
        messages = list(self.ddb.paginated_dynamodb_query(
            **ConversationMessage.query_by_conversation_kwargs(conversation_id),
            resource_class=ConversationMessage,
            ascending=True,
        ))
        if load_blobs:
            for msg in messages:
                if msg.has_unloaded_blobs():
                    msg.load_blob_fields(self.ddb)
        return messages
```

### Pattern: Counting via Pagination

```python
def count_messages(self, conversation_id):
    count = 0
    pagination_key = None
    while True:
        result = self.ddb.paginated_dynamodb_query(
            **ConversationMessage.query_by_conversation_kwargs(conversation_id),
            resource_class=ConversationMessage,
            results_limit=100,
            pagination_key=pagination_key,
        )
        count += len(result)
        if not result.next_pagination_key:
            break
        pagination_key = result.next_pagination_key
    return count
```

---

## Quick Reference: Key Imports

```python
# Core
from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable import PaginatedList, exhaust_pagination
from simplesingletable import LocalStorageMemory, AuditLogQuerier
from simplesingletable.models import ResourceConfig, BlobFieldConfig, AuditConfig, AuditLog

# Repository pattern
from simplesingletable.extras.repository import ResourceRepository
from simplesingletable.extras.versioned_repository import VersionedResourceRepository, VersionInfo
from simplesingletable.extras.readonly_repository import ReadOnlyResourceRepository
from simplesingletable.extras.readonly_versioned_repository import ReadOnlyVersionedResourceRepository

# Singleton
from simplesingletable.extras.singleton import SingletonResource, SingletonVersionedResource

# Cache
from simplesingletable.extras.cache import TTLCache

# Utils
from simplesingletable.utils import create_standard_dynamodb_table, truncate_dynamo_table

# Querying (from boto3)
from boto3.dynamodb.conditions import Key, Attr
```
