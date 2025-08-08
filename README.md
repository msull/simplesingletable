# Simple Single Table

**Latest Version:** 10.0.0

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
    filter_expression=lambda r: r.status == "active",
    limit=50
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
