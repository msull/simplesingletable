# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
