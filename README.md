# Simple Single Table

**Latest Version:** 17.0.2

## Project Overview

**simplesingletable**  is a Python library providing an abstraction layer for AWS DynamoDB operations, specifically
designed for single-table design patterns. The library uses Pydantic for model definitions and includes "
batteries-included" functionality for common DynamoDB use cases.

### Target Use Cases

- Small to medium scale applications
- Single-table DynamoDB design patterns
- Applications requiring versioned resources with automatic history tracking
- Fast, consistent, and cost-effective storage solutions

## Key Features

1. **Single Table Design**: Store different object types in a single DynamoDB table
2. **Automatic ID Generation**: Uses lexicographically sortable IDs via `ulid-py` for chronological ordering
3. **Resource Versioning**: Automatic versioning with complete history and optimistic concurrency control
4. **Secondary Access Patterns**: Support for GSI-based queries and filtering
5. **Pydantic Integration**: Type-safe models with validation
6. **Compression Support**: Optional gzip compression for large data

## Architecture

### Core Components

#### Models (`src/simplesingletable/models.py`)

- **`BaseDynamoDbResource`**: Abstract base class for all resources
- **`DynamoDbResource`**: Non-versioned resources (simpler, lighter)
- **`DynamoDbVersionedResource`**: Versioned resources with automatic history tracking
- **`PaginatedList`**: Enhanced list for paginated query results

#### Memory Layer (`src/simplesingletable/dynamodb_memory.py`)

- **`DynamoDbMemory`**: Main interface for DynamoDB operations (CRUD, queries, filtering)

#### Utilities (`src/simplesingletable/utils.py`)

- ID generation, pagination helpers, DynamoDB type marshalling

### Resource Types

**Non-Versioned Resources** (`DynamoDbResource`):

- Lighter weight, direct updates
- No version history
- Fields: `resource_id`, `created_at`, `updated_at`

**Versioned Resources** (`DynamoDbVersionedResource`):

- Complete version history
- Optimistic concurrency control
- Version limit enforcement (configurable via `max_versions`)
- Compressed data storage by default
- Fields: `resource_id`, `version`, `created_at`, `updated_at`

### GSI (Global Secondary Index) Configuration

The library supports both static and dynamic GSI configuration:

```python
class MyResource(DynamoDbVersionedResource):
    # Static configuration
    gsi_config: ClassVar[Dict[str, IndexFieldConfig]] = {
        "status": {
            "pk": lambda self: f"status#{self.status}",
            "sk": lambda self: self.resource_id,  # Sort by creation time
        }
    }

    # Or dynamic configuration via get_gsi_config()
    @classmethod
    def get_gsi_config(cls) -> Dict[str, IndexFieldConfig]:
        return {"dynamic_index": {"pk": lambda self: f"type#{self.type}"}}
```

### Extras Module (`src/simplesingletable/extras/`)

Additional patterns and utilities:

- **Repository Pattern**: `ResourceRepository` for higher-level operations
- **Versioned Repository**: `VersionedResourceRepository` with version management
- **Singleton Pattern**: For configuration resources
- **Form Data**: Streamlit integration helpers
- **Habit Tracker**: Example application

## Development Setup

### Dependencies

**Core Dependencies**:

- `boto3` - AWS SDK
- `pydantic>2` - Data validation and serialization
- `ulid-py` - Unique ID generation
- `humanize` - Human-readable formatting

**Development Dependencies**:

- `pytest` + `pytest-cov` + `pytest-docker` - Testing framework
- `black` + `isort` + `ruff` - Code formatting and linting
- `invoke` - Task automation
- `bumpver` - Version management

### Build and Test Commands

The project uses `invoke` for task automation (`tasks.py`):

```bash
# Dependency management
inv compile-requirements          # Compile requirements.txt from pyproject.toml
inv compile-requirements --upgrade # Update dependencies

# Development
inv lint                         # Format and lint code (black, isort, ruff)
inv launch-dynamodb-local       # Start local DynamoDB for testing
inv halt-dynamodb-local         # Stop local DynamoDB

# Testing
pytest                          # Run test suite with coverage
pytest tests/test_specific.py   # Run specific tests

# Release management
inv bumpver --patch|minor|major # Bump version
inv build                       # Build distribution packages
inv fullrelease --patch         # Complete release cycle (lint, test, bump, build, publish)
```

### Testing Architecture

- **Docker-based**: Uses DynamoDB Local via Docker Compose
- **Pytest fixtures**: `conftest.py` provides database setup/teardown
- **Comprehensive coverage**: Tests for CRUD, versioning, GSI, filtering
- **Test files**:
    - `test_simplesingletable.py` - Core functionality
    - `test_versioned_repository.py` - Version management
    - `test_repository.py` - Repository pattern
    - `test_filter_expressions.py` - Query filtering

## Important Patterns and Conventions

### Resource Definition Pattern

```python
from simplesingletable import DynamoDbVersionedResource


class MyResource(DynamoDbVersionedResource):
    name: str
    status: str
    metadata: Optional[dict] = None
    
    # the following are defined on the base class
    # resource_id: str
    # version: int
    # created_at: datetime
    # updated_at: datetime

    # Optional: Configure compression and version limits
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,  # Default for versioned
        max_versions=10  # Keep only 10 versions
    )

    # Optional: GSI configuration for secondary access patterns
    def db_get_gsi1pk(self) -> str | None:
        return f"status#{self.status}"
```

### Optional Fields and Conditional Writes

By default, Pydantic fields set to `None` are written to DynamoDB as `{"NULL": True}`
attributes. Boto3's wire protocol treats those attributes as **present**, which means
`attribute_not_exists(field)` returns False after the very first PUT — even on a
freshly created resource where the field was never set. This is the dominant
foot-gun for the standard "claim this slot" pattern:

```python
class Asset(DynamoDbResource):
    asset_tag: str
    assigned_user_id: Optional[str] = None

memory.create_new(Asset, {"asset_tag": "X"})

# Without omit_none_attributes, this rejects every time:
memory.update_existing(
    asset, {"assigned_user_id": "alice"},
    condition="attribute_not_exists(assigned_user_id)",
)
```

Set `omit_none_attributes=True` on the resource's `ResourceConfig` to drop
`None`-valued fields before marshalling. The natural `attribute_not_exists`
pattern then works as expected:

```python
class Asset(DynamoDbResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        omit_none_attributes=True,
    )

    asset_tag: str
    assigned_user_id: Optional[str] = None
```

The flag is off by default for backward compatibility, but is recommended for any
resource that uses `Optional` fields as slot markers or relies on
`attribute_not_exists` in conditional updates. The flag has no effect on
compressed (versioned) resources, since their fields live inside the gzipped
`data` blob rather than as separate DynamoDB attributes.

### CRUD Operations Pattern

```python
from simplesingletable import DynamoDbMemory

memory = DynamoDbMemory(logger=logger, table_name="my-table")

# Create
resource = memory.create_new(MyResource, {"name": "test", "status": "active"})

# Read
retrieved = memory.read_existing(resource.resource_id, MyResource)

# Update (versioned resources automatically increment version)
updated = memory.update_existing(retrieved, {"status": "inactive"})

# List with filtering
resources = memory.list_resources(
    MyResource,
    filter_fn=lambda r: r.status == "active",
    limit=50
)
```

### Paginated Queries

The library provides powerful paginated query capabilities for efficient data retrieval from DynamoDB, supporting both primary key and GSI-based queries with filtering.

#### Basic Paginated Query

```python
from simplesingletable import exhaust_pagination

# Direct paginated query with GSI
results = memory.paginated_dynamodb_query(
    resource_class=MyResource,
    index_name="gsi1",
    key_condition=Key("gsi1pk").eq("status#active"),
    results_limit=100,  # Items per page
    pagination_key=None  # For first page
)

# Get all results (automatically handles pagination)
all_results = []
for page in exhaust_pagination(
    lambda pk=None: memory.paginated_dynamodb_query(
        resource_class=MyResource,
        index_name="gsi1", 
        key_condition=Key("gsi1pk").eq("status#active"),
        results_limit=100,
        pagination_key=pk
    )
):
    all_results.extend(page)
```

#### Query Patterns with GSI

Define query methods on your resource classes for reusable access patterns:

```python
class MyResource(DynamoDbVersionedResource):
    status: str
    category: str
    
    def db_get_gsi1pk(self) -> str | None:
        # Sparse GSI - only index active items
        if self.status == "active":
            return f"{self.get_unique_key_prefix()}#{self.category}"
        return None
    
    @classmethod
    def query_by_category_kwargs(cls, category: str):
        """Build query kwargs for category-based queries."""
        return {
            "index_name": "gsi1",
            "key_condition": Key("gsi1pk").eq(f"{cls.get_unique_key_prefix()}#{category}")
        }

# Use the query pattern
active_in_category = memory.paginated_dynamodb_query(
    resource_class=MyResource,
    **MyResource.query_by_category_kwargs("important"),
    results_limit=50
)
```

#### Advanced Filtering

**Important Note on Versioned Resources**: Versioned resources use compression by default, which means DynamoDB filter expressions can only access attributes that are part of a GSI (pk, sk, gsi1pk, gsi1sk, etc.). For compressed resources, attribute-based filtering must be done client-side. Non-versioned resources don't have this limitation.

```python
from boto3.dynamodb.conditions import Attr

# For NON-VERSIONED resources - DynamoDB-side filtering works
filter_expression = Attr("status").eq("active") & Attr("priority").gt(5)

# For VERSIONED resources - use client-side filtering or GSI design
def custom_filter(resource: MyVersionedResource) -> bool:
    return resource.status == "active" and resource.priority > 5

# Combined approach (versioned resource example)
results = memory.paginated_dynamodb_query(
    resource_class=MyVersionedResource,
    filter_fn=custom_filter,  # Client-side only for versioned
    results_limit=100
)
```

**Best Practices for Efficient Filtering**:
1. **Design GSIs carefully** - For versioned resources, encode filterable attributes in GSI keys
2. **Consider data volume** - Client-side filtering is acceptable for small result sets (< 1000 items)
3. **Use `list_type_by_updated_at`** - Efficient for time-based queries with limited client-side filtering
4. **Non-versioned for high-volume filtering** - Consider using non-versioned resources when you need extensive DynamoDB-side filtering

#### Pagination Handling

For UI-driven pagination or batch processing:

```python
# First page
page1 = memory.paginated_dynamodb_query(
    resource_class=MyResource,
    results_limit=20
)

# Get pagination key for next page
if page1.has_more:
    next_key = page1.pagination_key
    
    # Fetch next page
    page2 = memory.paginated_dynamodb_query(
        resource_class=MyResource,
        results_limit=20,
        pagination_key=next_key
    )
```

#### Query by Updated Time

Special helper for time-based queries:

```python
# List resources by most recently updated
recent = memory.list_type_by_updated_at(
    MyResource,
    results_limit=50,
    filter_expression=Attr("status").eq("pending")
)
```

### Version Management

```python
# Get all versions of a resource
versions = memory.get_resource_versions(resource_id, MyResource)

# Get specific version
v2 = memory.read_existing_version(resource_id, MyResource, version=2)

# Version limits automatically enforced during updates
# (configure via resource_config['max_versions'])
```

## Internal Library Resources

simplesingletable stores its own internal resources alongside your application
data, namespaced under `_INTERNAL`. This keeps the library's bookkeeping
out of the way of your resource type prefixes (which use the class name) but
still lives in the same table so it benefits from single-table-design queries.

| Resource     | pk                            | Purpose                                                                 |
|--------------|-------------------------------|-------------------------------------------------------------------------|
| `MemoryStats`| `_INTERNAL#MemoryStats`       | Counter of items per resource type; visible via `memory.get_stats()`.   |
| `AuditLog`   | `_INTERNAL#AuditLog#<ULID>`   | One row per CREATE/UPDATE/DELETE/RESTORE on an audit-enabled resource.  |

When troubleshooting via `aws dynamodb scan`, filter on `begins_with(pk, "_INTERNAL#")`
to see what the library is storing. Audit rows can also be configured to live in a
separate table — see `audit_table_name` on `DynamoDbMemory` if your app produces
enough audit volume that you want to isolate it.

### Auditing

To enable audit logging for a resource, set `audit_config` on its `resource_config`:

```python
from simplesingletable import DynamoDbResource
from simplesingletable.models import AuditConfig, ResourceConfig

class User(DynamoDbResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,  # populate `changed_fields` on UPDATEs
            include_snapshot=True,     # populate `resource_snapshot` on every row
        ),
    )
    email: str
    role: Optional[str] = None
```

See the `AuditConfig` docstring for the full "what you get" matrix — in short,
`track_field_changes=True` only emits a diff if the caller supplies an
`old_resource` (the non-transactional path always does; for transactions, use
`txn.read(...)` or pass `current=resource` to `txn.update(...)`).

`AuditLogQuerier` exposes the standard access patterns:

```python
from simplesingletable import AuditLogQuerier

querier = AuditLogQuerier(memory)

# All changes to a specific resource
logs = querier.get_logs_for_resource("User", user_id)

# All changes by a user — backed by a sparse GSI on changed_by, no scan.
edits_by_admin = querier.get_logs_by_changer("admin@example.com")
```

## Code Quality Standards

- **Line length**: 120 characters
- **Python version**: ≥3.10
- **Type hints**: Required for all public APIs
- **Documentation**: Docstrings for classes and complex methods
- **Testing**: High coverage requirements (see `pyproject.toml`)

## Key Files and Their Purposes

- `src/simplesingletable/__init__.py` - Public API exports
- `src/simplesingletable/models.py` - Core resource classes and types
- `src/simplesingletable/dynamodb_memory.py` - Main DynamoDB interface
- `src/simplesingletable/utils.py` - Utility functions
- `src/simplesingletable/extras/` - Additional patterns and examples
- `tests/` - Comprehensive test suite
- `tasks.py` - Development automation scripts
- `pyproject.toml` - Project configuration and dependencies

## Common Gotchas and Considerations

1. **Versioned vs Non-versioned**: Choose based on whether you need history tracking
2. **Compression**: Versioned resources use compression by default; configure via `resource_config`
3. **GSI Limits**: DynamoDB has GSI limits; design access patterns carefully
4. **Version Limits**: Set `max_versions` to prevent unbounded growth
5. **Pagination**: Use `exhaust_pagination()` for complete result sets
6. **Concurrency**: Versioned resources prevent concurrent updates from same version
