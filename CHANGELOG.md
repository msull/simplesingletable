# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [13.1.0] 2025-10-08

### Fixed

* **Blob Field Serialization with Empty Sets**: Fixed critical serialization bug where Pydantic models containing `set` fields with empty sets (`set()`) were being incorrectly serialized as string literals `"set()"` instead of JSON arrays, causing TypeAdapter validation failures during blob loading. The fix implements proper type-aware serialization using Pydantic's `TypeAdapter`:
    - **Root Cause**: `json.dumps(value, default=str)` in `blob_storage.py` was converting Python `set()` objects to their string representation rather than JSON-compatible lists
    - **Solution**: Added `field_annotation` parameter to `S3BlobStorage.put_blob()` and uses `TypeAdapter.dump_json()` to serialize with full type information
    - **Flexibility**: Handles any complex type annotation: `list[BaseModel]`, `dict[str, BaseModel]`, `dict[str, list[dict[str, BaseModel]]]`, `Optional[...]`, etc.
    - **Serialization Flow**:
        - Extracts blob field values as Pydantic instances before `model_dump()` to preserve types
        - Passes field annotations from `resource.model_fields[field_name].annotation` to storage layer
        - Uses `TypeAdapter.dump_json()` for perfect symmetry with existing `TypeAdapter.validate_python()` deserialization
    - **Auto-Detection Fallback**: When annotations unavailable, automatically detects Pydantic models and handles them appropriately
    - **Backward Compatibility**:
        - Old data without sets continues to work
        - Old data with empty sets was already broken and requires re-saving
        - New data works perfectly with all complex types including sets
    - **Performance**: Eliminates Pydantic serialization warnings by preserving model instances throughout serialization pipeline
    - **None Handling**: Properly distinguishes between `None` (no blob stored) and empty collections like `[]` or `set()`
    - **Version Preservation**: Correctly maintains blob version references when updating resources without modifying blob fields
    - Comprehensive test coverage added in `test_blob_empty_set_issue.py` with cache clearing to verify actual S3 round-trip behavior

## [13.0.0] 2025-10-06

### Fixed

* **Blob Field Type Reconstruction**: Fixed blob fields containing `list[BaseModel]` to properly reconstruct Pydantic model instances when loaded from S3. Previously, Pydantic models in lists were deserialized as dictionaries and not reconstructed, causing attribute access errors. Now uses Pydantic's `TypeAdapter` to validate and reconstruct proper types for all blob field data during loading.
    - Affects any blob field containing Pydantic models (e.g., `list[MyModel]`, `Optional[MyModel]`, etc.)
    - Comprehensive test coverage added for both compressed and uncompressed blob fields with Pydantic models

## [12.8.0] 2025-10-02

### Added

* **Adaptive Filter Efficiency Tracking and Learned Query Multiplier**: Dramatically reduces DynamoDB API calls when using filter expressions by learning filter selectivity and adaptively adjusting query batch sizes:
    - **Filter Efficiency Tracking**: Automatically tracks the effectiveness of DynamoDB filter expressions
    - **Learned Multiplier**: After the first query, the system calculates actual filter efficiency and dynamically adjusts the query multiplier for subsequent paginated calls
    - **Intelligent Batch Sizing**: Uses observed efficiency to fetch appropriate amounts of data (e.g., 20% efficiency → multiplier of 5x)
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

* **Blob Storage Caching**: Added comprehensive caching layer to S3 blob storage for improved performance with frequently accessed blobs:
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

* **Tuple-Based GSI Configuration**: Enhanced GSI configuration to support defining both partition and sort keys with a single method:
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

* **Read-Only Repository Classes**: Introduced `ReadOnlyResourceRepository` and `ReadOnlyVersionedResourceRepository` classes for safe, read-only access to resources:
    - `ReadOnlyResourceRepository` in `simplesingletable.extras.readonly_repository` provides read-only access to standard resources
    - `ReadOnlyVersionedResourceRepository` in `simplesingletable.extras.readonly_versioned_repository` provides read-only access to versioned resources with version querying capabilities
    - Both classes expose only safe read operations (`get()`, `read()`, `list()`) and hide all mutation methods
    - `ReadOnlyVersionedResourceRepository` additionally provides `list_versions()` and `get_version()` methods for version inspection
    - Useful for services and components that should only have read access to data, ensuring data integrity at the repository level

## [12.0.1] 2025-08-22

### Fixed

* **Pagination with Blob Fields**: Fixed a TypeError that occurred when building LastEvaluatedKey during paginated queries on resources with blob fields. When `to_dynamodb_item()` returns a tuple `(db_item, blob_data)` for resources with blob storage configured, the pagination logic now correctly extracts just the db_item portion before building the LastEvaluatedKey.

## [12.0.0] 2025-08-20

### Changed

* **Potential breaking change** - to_dynamodb_item no longer passed `exclude_none=True` when serializing to the DynamoDb Item

## [11.3.0] 2025-08-15

### Changed

* **Code Refactoring - Eliminated Duplication**: Extracted ~150 lines of duplicated code between `DynamoDbResource` and `DynamoDbVersionedResource` into their base class `BaseDynamoDbResource`. The refactoring introduces several protected helper methods:
    - `_extract_blob_fields()` - Handles blob field extraction from model data
    - `_apply_gsi_configuration()` - Applies dynamic GSI configuration and legacy GSI methods
    - `_add_blob_metadata()` - Manages blob metadata in DynamoDB items
    - `_build_resource_from_data()` - Constructs resources from DynamoDB data with blob handling
    - `_get_excluded_dynamodb_keys()` - Provides consistent key filtering for DynamoDB-specific attributes

## [11.2.1] 2025-08-15

### Fixed

* **ResourceConfig Inheritance for Versioned Resources**: Fixed the `compress_data` resource configuration to properly respect subclass settings. The `__pydantic_init_subclass__` method was moved from the base class to the specific resource classes (`DynamoDbResource` and `DynamoDbVersionedResource`) to ensure that subclasses correctly inherit and merge their parent's default configurations. This fix ensures that:
    - Non-versioned resources default to `compress_data=False` 
    - Versioned resources default to `compress_data=True`
    - Subclasses can override these defaults and their settings will be properly respected
    - The `to_dynamodb_item()` and `from_dynamodb_item()` methods now correctly check the `compress_data` setting before compressing/decompressing data

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
