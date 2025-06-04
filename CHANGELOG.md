# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
