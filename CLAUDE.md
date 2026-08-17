# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

simplesingletable is a Python library providing a Pydantic-based abstraction layer for AWS DynamoDB, implementing single-table design patterns. It's optimized for small-to-medium scale applications with automatic resource versioning, ULID-based ID generation, and comprehensive secondary access pattern support.

## Key Commands

### Testing
```bash
# Run all tests (requires Docker for DynamoDB Local)
pytest

# Run specific test file
pytest tests/test_core.py

# Run with coverage
pytest --cov=simplesingletable --cov-report=term-missing

# Run tests matching pattern
pytest -k "test_version"
```

### Development Tasks
```bash
# Lint and format code (black, isort, ruff)
inv lint
```

### Releasing

```bash
# Lint, test, stamp CHANGELOG, bump version, build, publish, push (tasks.py: fullrelease)
inv fullrelease --minor    # or --major / --patch
```

**Write CHANGELOG entries under `## [Unreleased]`, never under a hand-written version
header.** `inv fullrelease` calls `stamp_changelog`, which replaces the `## [Unreleased]`
marker with `## [Unreleased]` followed by a dated `## [X.Y.Z]` header for the version it is
about to release. Adding that header by hand produces a duplicate — one empty, one with the
entries — which reads as an empty release. The `## [Unreleased]` marker must be present or
the release aborts.

The version number itself lives in `[tool.bumpver]` in `pyproject.toml` and is propagated by
`bumpver` to `package_version` in `src/simplesingletable/__init__.py` and to the version
line in `README.md`. Do not edit those by hand either.

## Architecture Overview

### Core Resource Types

1. **DynamoDbResource**: Base non-versioned resource
   - Simple CRUD operations
   - No version history
   - Located in `simplesingletable/models.py`

2. **DynamoDbVersionedResource**: Full version management
   - Maintains complete version history
   - Optimistic concurrency control via `version_token`
   - Automatic gzip compression
   - Configurable version limits via `ResourceConfig`
   - Located in `simplesingletable/models.py`

### Single-Table Design Pattern

All resources are stored in a single DynamoDB table with composite keys:
- **Primary Key**: `pk` (partition key) + `sk` (sort key)
- **GSI Support**: Up to 5 GSIs for secondary access patterns
- **Key Format**: `{resource_type}#{id}` for pk, version number for sk

### Memory Layer (`DynamoDbMemory`)

The main interface for CRUD operations:
- Handles both versioned and non-versioned resources
- Manages GSI configurations dynamically
- Provides filtering and query capabilities
- Located in `simplesingletable/memory.py`

### Repository Pattern (Extras)

Extended patterns in `simplesingletable/extras/`:
- **VersionedResourceRepository**: Repository pattern with version management API
- **DynamoDbSingleton**: Singleton resource pattern
- **StreamlitMemory**: Streamlit integration with caching

## Key Design Patterns

### GSI Configuration
Resources can define GSIs either statically or dynamically:
```python
# Static (in model)
class MyResource(DynamoDbResource):
    gsi_config: ClassVar[GSIConfig] = GSIConfig(...)

# Dynamic (via classmethod)
@classmethod
def get_gsi_config(cls) -> GSIConfig:
    return GSIConfig(...)
```

### Version Management
- Versions stored as separate items in DynamoDB
- Current version at `sk="0"`, history at `sk="1"`, `sk="2"`, etc.
- Automatic version increment on updates
- Version limits configurable via `ResourceConfig.max_versions`

### ID Generation
- Uses ULID (Universally Lexicographically Sortable Identifier)
- Provides chronological ordering
- Generated automatically if not provided

## Testing Approach

- **Integration Tests**: Use DynamoDB Local via Docker container
- **Test Structure**: Comprehensive fixtures in `tests/conftest.py`
- **Coverage**: High standards defined in `pyproject.toml`
- **Test Categories**:
  - Core CRUD operations (`test_core.py`)
  - Versioning behavior (`test_versioning.py`)
  - Repository patterns (`test_repository.py`)
  - Filtering and queries (`test_filtering.py`)

## Important Conventions

1. **Type Safety**: Extensive use of Pydantic models and type hints throughout
2. **Error Handling**: Custom exceptions in `simplesingletable/exceptions.py`
3. **Async Support**: All database operations are synchronous (boto3 based)
4. **Compression**: Automatic gzip for versioned resources to reduce storage
5. **Timestamps**: Automatic `created_at` and `updated_at` management

## Environment Setup

- Python 3.10+ required
- Docker needed for running tests (DynamoDB Local)
- AWS credentials configured for production use
- Development dependencies in `requirements-dev.txt`