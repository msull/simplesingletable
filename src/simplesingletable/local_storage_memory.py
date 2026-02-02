"""Local file-based storage implementation for offline/demo usage."""

import base64
import fcntl
import json
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, ClassVar, Optional, Set, Type, TypeVar, Union

from boto3.dynamodb.conditions import ConditionBase, Key
from pydantic import BaseModel, Field

from .local_blob_storage import LocalBlobStorage
from .models import (
    AuditLog,
    BlobPlaceholder,
    DynamoDbResource,
    DynamoDbVersionedResource,
    PaginatedList,
)
from .utils import decode_pagination_key, encode_pagination_key

AnyDbResource = TypeVar("AnyDbResource", bound=Union[DynamoDbVersionedResource, DynamoDbResource])
VersionedDbResourceOnly = TypeVar("VersionedDbResourceOnly", bound=DynamoDbVersionedResource)
NonversionedDbResourceOnly = TypeVar("NonversionedDbResourceOnly", bound=DynamoDbResource)

_PlainBaseModel = TypeVar("_PlainBaseModel", bound=BaseModel)


class Constants:
    SYSTEM_DEFAULT_LIMIT = 250
    QUERY_DEFAULT_MAX_API_CALLS = 10


# Internal resources for stats tracking
class InternalResourceBase(DynamoDbResource):
    """Base class for internal resources."""

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "_INTERNAL"

    @classmethod
    def ensure_exists(cls, memory: "LocalStorageMemory") -> "InternalResourceBase":
        if not (existing := memory.get_existing(cls.pk, data_class=cls)):
            return memory.create_new(cls, {}, override_id=cls.pk)
        return existing


class MemoryStats(InternalResourceBase):
    """Statistics about resources stored in memory."""

    pk: ClassVar[str] = "MemoryStats"

    counts_by_type: dict[str, int] = Field(default_factory=dict)


def _encode_binary_data(obj: Any) -> Any:
    """Recursively encode binary data and sets for JSON serialization.

    Handles:
    - bytes: encoded as base64
    - sets: converted to list with marker
    - dicts: recursively processed
    - lists: recursively processed
    """
    if isinstance(obj, bytes):
        return {"__type__": "bytes", "data": base64.b64encode(obj).decode("utf-8")}
    elif isinstance(obj, set):
        # Convert set to list with a marker
        # Try to sort for consistent output, but fallback to unsorted if not sortable
        try:
            data = sorted(list(obj))
        except (TypeError, AttributeError):
            # Items not sortable (mixed types or non-comparable objects)
            data = list(obj)
        return {"__type__": "set", "data": data}
    elif isinstance(obj, dict):
        return {k: _encode_binary_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_encode_binary_data(item) for item in obj]
    else:
        return obj


def _decode_binary_data(obj: Any) -> Any:
    """Recursively decode base64 data and sets back to original types.

    Handles:
    - bytes: decoded from base64
    - sets: converted from list with marker
    - dicts: recursively processed
    - lists: recursively processed
    """
    if isinstance(obj, dict):
        if obj.get("__type__") == "bytes":
            return base64.b64decode(obj["data"])
        elif obj.get("__type__") == "set":
            return set(obj["data"])
        else:
            return {k: _decode_binary_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_decode_binary_data(item) for item in obj]
    else:
        return obj


@dataclass
class LocalStorageMemory:
    """Local file-based storage that mimics DynamoDbMemory interface.

    Stores resources as JSON files in a local directory. Intended for offline
    demos and local testing without AWS dependencies.
    """

    logger: Any
    storage_dir: str
    track_stats: bool = True
    use_blob_storage: bool = True
    _local_blob_storage: Optional["LocalBlobStorage"] = field(default=None, init=False)

    def __post_init__(self):
        """Initialize storage directories."""
        self.storage_path = Path(self.storage_dir)
        self.resources_dir = self.storage_path / "resources"
        self.resources_dir.mkdir(parents=True, exist_ok=True)

        if self.use_blob_storage:
            self._local_blob_storage = LocalBlobStorage(storage_dir=self.storage_dir)

    @property
    def s3_blob_storage(self) -> Optional[LocalBlobStorage]:
        """Property to match DynamoDbMemory interface."""
        return self._local_blob_storage

    def _get_resource_file_path(self, resource_class: Type[AnyDbResource]) -> Path:
        """Get the file path for a resource type."""
        prefix = resource_class.get_unique_key_prefix()
        # Replace any path-unsafe characters
        safe_prefix = prefix.replace("#", "_").replace("/", "_")
        return self.resources_dir / f"{safe_prefix}.json"

    @contextmanager
    def _lock_and_load(self, file_path: Path):
        """Context manager for thread-safe file operations.

        Yields:
            dict: The loaded data from the file
        """
        # Ensure file exists
        if not file_path.exists():
            file_path.write_text("{}")

        # Open file and acquire lock
        with open(file_path, "r+") as f:
            # Acquire exclusive lock
            if sys.platform != "win32":
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)

            try:
                # Load current data
                f.seek(0)
                content = f.read()
                if content:
                    data = json.loads(content)
                    # Decode any binary data
                    data = _decode_binary_data(data)
                else:
                    data = {}
                yield data, f
            finally:
                # Release lock
                if sys.platform != "win32":
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _save_data(self, f, data: dict):
        """Save data to an open file handle."""
        f.seek(0)
        f.truncate()
        # Encode binary data before saving
        encoded_data = _encode_binary_data(data)
        json.dump(encoded_data, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())

    def _make_storage_key(self, pk: str, sk: str) -> str:
        """Make a storage key from pk and sk."""
        return f"{pk}#{sk}"

    def get_existing(
        self,
        existing_id: str,
        data_class: Type[AnyDbResource],
        version: int = 0,
        consistent_read=False,
        load_blobs: bool = False,
    ) -> Optional[AnyDbResource]:
        """Get object of the specified type with the provided key.

        The `version` parameter is ignored on non-versioned resources.
        If load_blobs is True, blob fields will be loaded from local storage.
        """
        if issubclass(data_class, DynamoDbResource):
            if version:
                self.logger.warning(
                    f"Version parameter ignored when fetching non-versioned resource; provided {version=}"
                )
            key = data_class.dynamodb_lookup_keys_from_id(existing_id)
        elif issubclass(data_class, DynamoDbVersionedResource):
            key = data_class.dynamodb_lookup_keys_from_id(existing_id, version=version)
        else:
            raise ValueError("Invalid data_class provided")

        file_path = self._get_resource_file_path(data_class)
        if not file_path.exists():
            return None

        with self._lock_and_load(file_path) as (data, _):
            storage_key = self._make_storage_key(key["pk"], key["sk"])
            item = data.get(storage_key)

            if not item:
                return None

            # Build blob placeholders if blob fields are present
            blob_placeholders = {}
            if "_blob_fields" in item and self.s3_blob_storage:
                blob_fields_config = data_class.resource_config.get("blob_fields", {}) or {}
                blob_versions = item.get("_blob_versions", {})

                for field_name in item["_blob_fields"]:
                    if field_name in blob_fields_config:
                        # Only create placeholder if this field has a blob stored
                        if issubclass(data_class, DynamoDbVersionedResource):
                            if field_name not in blob_versions:
                                continue  # No blob stored for this field

                        # Build placeholder for this blob field
                        s3_key = self.s3_blob_storage._build_s3_key(
                            resource_type=data_class.__name__,
                            resource_id=existing_id,
                            field_name=field_name,
                            version=version if issubclass(data_class, DynamoDbVersionedResource) else None,
                        )
                        blob_placeholders[field_name] = BlobPlaceholder(
                            field_name=field_name,
                            s3_key=s3_key,
                            size_bytes=0,
                            content_type=blob_fields_config[field_name].get("content_type"),
                            compressed=blob_fields_config[field_name].get("compress", False),
                        )

            resource = data_class.from_dynamodb_item(item, blob_placeholders)

            # Load blobs if requested
            if load_blobs and resource and resource.has_unloaded_blobs():
                resource.load_blob_fields(self)

            return resource

    def batch_get_existing(
        self,
        ids: list[str],
        data_class: Type[AnyDbResource],
        consistent_read: bool = False,
        load_blobs: bool = False,
    ) -> dict[str, AnyDbResource]:
        """Batch-get multiple resources by ID. Returns only found items.

        Simple loop implementation for API parity with DynamoDbMemory.

        Args:
            ids: List of resource IDs to fetch
            data_class: The resource class to deserialize into
            consistent_read: Whether to use strongly consistent reads (ignored locally)
            load_blobs: If True, blob fields will be loaded from local storage

        Returns:
            Dict mapping resource_id -> resource for found items only.
        """
        if not ids:
            return {}

        results: dict[str, AnyDbResource] = {}
        for rid in dict.fromkeys(ids):  # deduplicate while preserving order
            resource = self.get_existing(rid, data_class, consistent_read=consistent_read, load_blobs=load_blobs)
            if resource is not None:
                results[rid] = resource
        return results

    def read_existing(
        self,
        existing_id: str,
        data_class: Type[AnyDbResource],
        version: int = 0,
        consistent_read=False,
        load_blobs: bool = False,
    ) -> AnyDbResource:
        """Return object of the specified type with the provided key.

        The `version` parameter is ignored on non-versioned resources.

        Raises a ValueError if no object with the provided id was found.
        """
        if not (
            item := self.get_existing(
                existing_id, data_class, version, consistent_read=consistent_read, load_blobs=load_blobs
            )
        ):
            raise ValueError("No item found with the provided key.")
        return item

    def create_new(
        self,
        data_class: Type[AnyDbResource],
        data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
        changed_by: Optional[str] = None,
        audit_metadata: Optional[dict[str, Any]] = None,
    ) -> AnyDbResource:
        """Create a new resource."""
        new_resource = data_class.create_new(data, override_id=override_id)

        if issubclass(data_class, DynamoDbResource):
            resource = self._put_nonversioned_resource(new_resource)
        elif issubclass(data_class, DynamoDbVersionedResource):
            resource = self._create_new_versioned(new_resource)
        else:
            raise ValueError("Invalid data_class provided")

        if self.track_stats:
            stats = MemoryStats.ensure_exists(self)
            self.increment_counter(stats, "counts_by_type." + data_class.__name__)

        # Create audit log after successful creation
        self._create_audit_log(
            operation="CREATE",
            resource=resource,
            changed_by=changed_by,
            audit_metadata=audit_metadata,
        )

        return resource

    def _put_nonversioned_resource(self, resource: NonversionedDbResourceOnly) -> NonversionedDbResourceOnly:
        """Store a non-versioned resource."""
        result = resource.to_dynamodb_item()
        # Handle both return types for backward compatibility
        if isinstance(result, tuple):
            item, blob_fields_data = result
        else:
            item, blob_fields_data = result, {}

        file_path = self._get_resource_file_path(resource.__class__)

        with self._lock_and_load(file_path) as (data, f):
            storage_key = self._make_storage_key(item["pk"], item["sk"])
            data[storage_key] = item
            self._save_data(f, data)

        # Store blob fields if configured
        if blob_fields_data and self.s3_blob_storage:
            blob_fields_config = resource.resource_config.get("blob_fields", {}) or {}
            for field_name, value in blob_fields_data.items():
                if field_name in blob_fields_config and value is not None:
                    # Get field annotation for proper serialization
                    field_annotation = (
                        resource.model_fields[field_name].annotation if field_name in resource.model_fields else None
                    )

                    self.s3_blob_storage.put_blob(
                        resource_type=resource.__class__.__name__,
                        resource_id=resource.resource_id,
                        field_name=field_name,
                        value=value,
                        config=blob_fields_config[field_name],
                        version=None,
                        field_annotation=field_annotation,
                    )

        return resource

    def _create_new_versioned(self, resource: VersionedDbResourceOnly) -> VersionedDbResourceOnly:
        """Create a new versioned resource."""
        # Handle both return types for backward compatibility
        result = resource.to_dynamodb_item()
        if isinstance(result, tuple):
            main_item, blob_fields_data = result
        else:
            main_item, blob_fields_data = result, {}

        v0_result = resource.to_dynamodb_item(v0_object=True)
        if isinstance(v0_result, tuple):
            v0_item, _ = v0_result
        else:
            v0_item = v0_result

        file_path = self._get_resource_file_path(resource.__class__)

        with self._lock_and_load(file_path) as (data, f):
            # Check that neither item exists (simulating DynamoDB condition)
            main_key = self._make_storage_key(main_item["pk"], main_item["sk"])
            v0_key = self._make_storage_key(v0_item["pk"], v0_item["sk"])

            if main_key in data or v0_key in data:
                raise ValueError("Resource already exists")

            # Store both items
            data[main_key] = main_item
            data[v0_key] = v0_item
            self._save_data(f, data)

        # Store blob fields if configured
        if blob_fields_data and self.s3_blob_storage:
            blob_fields_config = resource.resource_config.get("blob_fields", {}) or {}
            for field_name, value in blob_fields_data.items():
                if field_name in blob_fields_config and value is not None:
                    field_annotation = (
                        resource.model_fields[field_name].annotation if field_name in resource.model_fields else None
                    )

                    self.s3_blob_storage.put_blob(
                        resource_type=resource.__class__.__name__,
                        resource_id=resource.resource_id,
                        field_name=field_name,
                        value=value,
                        config=blob_fields_config[field_name],
                        version=resource.version,
                        field_annotation=field_annotation,
                    )

        return self.read_existing(
            existing_id=resource.resource_id,
            data_class=resource.__class__,
            version=resource.version,
            consistent_read=True,
        )

    def update_existing(
        self,
        existing_resource: AnyDbResource,
        update_obj: _PlainBaseModel | dict,
        clear_fields: Optional[Set[str]] = None,
        changed_by: Optional[str] = None,
        audit_metadata: Optional[dict[str, Any]] = None,
    ) -> AnyDbResource:
        """Update an existing resource."""
        data_class = existing_resource.__class__
        updated_resource = existing_resource.update_existing(update_obj, clear_fields=clear_fields)

        if issubclass(data_class, DynamoDbResource):
            result = self._put_nonversioned_resource(updated_resource)
            # Create audit log after successful update
            self._create_audit_log(
                operation="UPDATE",
                resource=result,
                changed_by=changed_by,
                old_resource=existing_resource,
                audit_metadata=audit_metadata,
            )
            return result
        elif issubclass(data_class, DynamoDbVersionedResource):
            latest_resource = self.read_existing(
                existing_id=existing_resource.resource_id,
                data_class=data_class,
            )
            if existing_resource.version != latest_resource.version:
                raise ValueError("Cannot update from non-latest version")

            self._update_existing_versioned(updated_resource, previous_version=latest_resource.version)

            # Enforce version limit if configured
            data_class.enforce_version_limit(self, updated_resource.resource_id)

            result = self.read_existing(
                existing_id=updated_resource.resource_id,
                data_class=data_class,
                version=updated_resource.version,
                consistent_read=True,
            )

            # Create audit log after successful update
            self._create_audit_log(
                operation="UPDATE",
                resource=result,
                changed_by=changed_by,
                old_resource=existing_resource,
                audit_metadata=audit_metadata,
            )
            return result
        else:
            raise ValueError("Invalid data_class provided")

    def _update_existing_versioned(self, resource: VersionedDbResourceOnly, previous_version: int):
        """Update a versioned resource."""
        # Handle both return types
        result = resource.to_dynamodb_item()
        if isinstance(result, tuple):
            main_item, blob_fields_data = result
        else:
            main_item, blob_fields_data = result, {}

        v0_result = resource.to_dynamodb_item(v0_object=True)
        if isinstance(v0_result, tuple):
            v0_item, _ = v0_result
        else:
            v0_item = v0_result

        file_path = self._get_resource_file_path(resource.__class__)

        with self._lock_and_load(file_path) as (data, f):
            main_key = self._make_storage_key(main_item["pk"], main_item["sk"])
            v0_key = self._make_storage_key(v0_item["pk"], v0_item["sk"])

            # Check that main item doesn't exist and v0 item has correct version
            if main_key in data:
                raise ValueError("Version already exists")

            if v0_key not in data:
                raise ValueError("Resource does not exist")

            if data[v0_key].get("version") != previous_version:
                raise ValueError("Version conflict")

            # Store both items
            data[main_key] = main_item
            data[v0_key] = v0_item
            self._save_data(f, data)

        # Store blob fields if configured
        if blob_fields_data and self.s3_blob_storage:
            blob_fields_config = resource.resource_config.get("blob_fields", {}) or {}
            for field_name, value in blob_fields_data.items():
                if field_name in blob_fields_config and value is not None:
                    field_annotation = (
                        resource.model_fields[field_name].annotation if field_name in resource.model_fields else None
                    )

                    self.s3_blob_storage.put_blob(
                        resource_type=resource.__class__.__name__,
                        resource_id=resource.resource_id,
                        field_name=field_name,
                        value=value,
                        config=blob_fields_config[field_name],
                        version=resource.version,
                        field_annotation=field_annotation,
                    )

    def delete_existing(
        self,
        existing_resource: AnyDbResource,
        changed_by: Optional[str] = None,
        audit_metadata: Optional[dict[str, Any]] = None,
    ):
        """Delete an existing resource."""
        # Create audit log before deleting
        self._create_audit_log(
            operation="DELETE",
            resource=existing_resource,
            changed_by=changed_by,
            audit_metadata=audit_metadata,
        )

        if issubclass(existing_resource.__class__, DynamoDbResource):
            self._delete_nonversioned_resource(existing_resource)
        elif issubclass(existing_resource.__class__, DynamoDbVersionedResource):
            self._delete_versioned_resource(existing_resource)
        else:
            raise ValueError("Invalid resource type provided")

    def _delete_nonversioned_resource(self, existing_resource: NonversionedDbResourceOnly):
        """Delete a non-versioned resource."""
        self.logger.info(
            f"Deleting resource:{existing_resource.__class__.__name__} "
            f"with resource_id='{existing_resource.resource_id}"
        )

        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        file_path = self._get_resource_file_path(existing_resource.__class__)

        with self._lock_and_load(file_path) as (data, f):
            storage_key = self._make_storage_key(key["pk"], key["sk"])
            if storage_key in data:
                del data[storage_key]
                self._save_data(f, data)

        # Delete blob fields if configured
        if self.s3_blob_storage:
            blob_fields_config = existing_resource.resource_config.get("blob_fields", {}) or {}
            if blob_fields_config:
                self.s3_blob_storage.delete_all_blobs(
                    resource_type=existing_resource.__class__.__name__, resource_id=existing_resource.resource_id
                )

        if self.track_stats:
            stats = MemoryStats.ensure_exists(self)
            self.increment_counter(stats, "counts_by_type." + existing_resource.__class__.__name__, -1)

    def _delete_versioned_resource(self, existing_resource: VersionedDbResourceOnly):
        """Delete a specific version of a versioned resource."""
        self.logger.info(
            f"Deleting versioned resource:{existing_resource.__class__.__name__} "
            f"with resource_id='{existing_resource.resource_id}' version={existing_resource.version}"
        )

        key = existing_resource.dynamodb_lookup_keys_from_id(
            existing_resource.resource_id, version=existing_resource.version
        )
        file_path = self._get_resource_file_path(existing_resource.__class__)

        with self._lock_and_load(file_path) as (data, f):
            storage_key = self._make_storage_key(key["pk"], key["sk"])
            if storage_key in data:
                del data[storage_key]

                # Also delete v0 if this is the latest version
                v0_key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id, version=0)
                v0_storage_key = self._make_storage_key(v0_key["pk"], v0_key["sk"])
                if v0_storage_key in data and data[v0_storage_key].get("version") == existing_resource.version:
                    del data[v0_storage_key]

                self._save_data(f, data)

        # Delete blob fields for this version if configured
        if self.s3_blob_storage:
            blob_fields_config = existing_resource.resource_config.get("blob_fields", {}) or {}
            for field_name in blob_fields_config:
                self.s3_blob_storage.delete_blob(
                    resource_type=existing_resource.__class__.__name__,
                    resource_id=existing_resource.resource_id,
                    field_name=field_name,
                    version=existing_resource.version,
                )

        if self.track_stats:
            stats = MemoryStats.ensure_exists(self)
            self.increment_counter(stats, "counts_by_type." + existing_resource.__class__.__name__, -1)

    def delete_all_versions(self, resource_id: str, data_class: Type[VersionedDbResourceOnly]):
        """Delete all versions of a versioned resource."""
        if not issubclass(data_class, DynamoDbVersionedResource):
            raise ValueError("delete_all_versions can only be used with versioned resources")

        self.logger.info(f"Deleting all versions of resource:{data_class.__name__} with resource_id='{resource_id}'")

        file_path = self._get_resource_file_path(data_class)
        prefix = f"{data_class.get_unique_key_prefix()}#{resource_id}"

        with self._lock_and_load(file_path) as (data, f):
            # Find all keys for this resource
            keys_to_delete = [k for k in data.keys() if k.startswith(prefix + "#")]

            if not keys_to_delete:
                self.logger.warning(f"No versions found for resource {resource_id}")
                return

            # Delete all matching keys
            for key in keys_to_delete:
                del data[key]

            self._save_data(f, data)

        self.logger.info(f"Deleted {len(keys_to_delete)} versions for resource {resource_id}")

        # Delete all blob fields if configured
        if self.s3_blob_storage:
            blob_fields_config = data_class.resource_config.get("blob_fields", {}) or {}
            if blob_fields_config:
                deleted_blobs = self.s3_blob_storage.delete_all_blobs(
                    resource_type=data_class.__name__, resource_id=resource_id
                )
                if deleted_blobs:
                    self.logger.info(f"Deleted {deleted_blobs} blob fields for resource {resource_id}")

        if self.track_stats:
            stats = MemoryStats.ensure_exists(self)
            self.increment_counter(stats, "counts_by_type." + data_class.__name__, -1)

    def get_all_versions(
        self,
        resource_id: str,
        data_class: Type[VersionedDbResourceOnly],
        load_blobs: bool = False,
    ) -> list[VersionedDbResourceOnly]:
        """Get all versions of a versioned resource, sorted newest first."""
        if not issubclass(data_class, DynamoDbVersionedResource):
            raise ValueError("get_all_versions can only be used with versioned resources")

        self.logger.debug(f"Getting all versions of {data_class.__name__} with resource_id='{resource_id}'")

        file_path = self._get_resource_file_path(data_class)
        if not file_path.exists():
            return []

        prefix = f"{data_class.get_unique_key_prefix()}#{resource_id}#v"
        versions = []

        with self._lock_and_load(file_path) as (data, _):
            for key, item in data.items():
                if key.startswith(prefix) and not key.endswith("#v0"):
                    # Build blob placeholders if needed
                    blob_placeholders = {}
                    if "_blob_fields" in item and self.s3_blob_storage:
                        blob_fields_config = data_class.resource_config.get("blob_fields", {}) or {}
                        blob_versions = item.get("_blob_versions", {})
                        version_num = int(item.get("version", 0))

                        for field_name in item["_blob_fields"]:
                            if field_name in blob_fields_config and field_name in blob_versions:
                                s3_key = self.s3_blob_storage._build_s3_key(
                                    resource_type=data_class.__name__,
                                    resource_id=resource_id,
                                    field_name=field_name,
                                    version=version_num,
                                )
                                blob_placeholders[field_name] = BlobPlaceholder(
                                    field_name=field_name,
                                    s3_key=s3_key,
                                    size_bytes=0,
                                    content_type=blob_fields_config[field_name].get("content_type"),
                                    compressed=blob_fields_config[field_name].get("compress", False),
                                )

                    resource = data_class.from_dynamodb_item(item, blob_placeholders)

                    # Load blobs if requested
                    if load_blobs and resource.has_unloaded_blobs():
                        resource.load_blob_fields(self)

                    versions.append(resource)

        # Sort by version number, newest first
        versions.sort(key=lambda v: v.version, reverse=True)

        self.logger.debug(f"Found {len(versions)} versions for resource {resource_id}")
        return versions

    def restore_version(
        self,
        resource_id: str,
        data_class: Type[VersionedDbResourceOnly],
        version: int,
        changed_by: Optional[str] = None,
        audit_metadata: Optional[dict[str, Any]] = None,
    ) -> VersionedDbResourceOnly:
        """Restore a previous version by creating a new version with the same content."""
        if not issubclass(data_class, DynamoDbVersionedResource):
            raise ValueError("restore_version can only be used with versioned resources")

        if version <= 0:
            raise ValueError(f"Version must be a positive integer, got: {version}")

        self.logger.info(f"Restoring version {version} of {data_class.__name__} with resource_id='{resource_id}'")

        # Get the version to restore (load blobs so we can copy them)
        version_to_restore = self.get_existing(resource_id, data_class, version=version, load_blobs=True)
        if not version_to_restore:
            raise ValueError(f"Version {version} not found for {data_class.__name__} {resource_id}")

        # Get the current latest version
        current = self.get_existing(resource_id, data_class, version=0)
        if not current:
            raise ValueError(f"{data_class.__name__} {resource_id} not found")

        # Create update data from the old version, excluding system fields
        update_data = version_to_restore.model_dump(exclude={"resource_id", "version", "created_at", "updated_at"})

        # Update the current item with the old version's data
        restored_item = self.update_existing(current, update_data, changed_by=changed_by, audit_metadata=audit_metadata)

        self.logger.info(
            f"Restored {data_class.__name__} {resource_id} from version {version} "
            f"as new version {restored_item.version}"
        )

        return restored_item

    def get_stats(self) -> MemoryStats:
        """Get memory statistics."""
        return MemoryStats.ensure_exists(self)

    def increment_counter(
        self, existing_resource: NonversionedDbResourceOnly, field_name: str, incr_by: int = 1
    ) -> int:
        """Increment a counter field on a non-versioned resource."""
        if not issubclass(existing_resource.__class__, DynamoDbResource):
            raise TypeError("increment_counter can only be utilized with non-versioned resources")

        file_path = self._get_resource_file_path(existing_resource.__class__)
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        storage_key = self._make_storage_key(key["pk"], key["sk"])

        with self._lock_and_load(file_path) as (data, f):
            if storage_key not in data:
                raise ValueError("Resource not found")

            item = data[storage_key]

            # Handle nested counters (e.g., "counts_by_type.MyResource")
            if "." in field_name:
                first_part, remainder = field_name.split(".", maxsplit=1)
                if first_part not in item:
                    item[first_part] = {}
                if remainder not in item[first_part]:
                    item[first_part][remainder] = 0
                item[first_part][remainder] += incr_by
                new_value = item[first_part][remainder]
            else:
                if field_name not in item:
                    item[field_name] = 0
                item[field_name] += incr_by
                new_value = item[field_name]

            # Update timestamps
            now = datetime.now(timezone.utc).isoformat()
            item["updated_at"] = now
            item["gsitypesk"] = now

            data[storage_key] = item
            self._save_data(f, data)

        return new_value

    def add_to_set(self, existing_resource: NonversionedDbResourceOnly, field_name: str, val: str):
        """Add a value to a set field."""
        if not issubclass(existing_resource.__class__, DynamoDbResource):
            raise TypeError("add_to_set can only be utilized with non-versioned resources")

        file_path = self._get_resource_file_path(existing_resource.__class__)
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        storage_key = self._make_storage_key(key["pk"], key["sk"])

        with self._lock_and_load(file_path) as (data, f):
            if storage_key not in data:
                raise ValueError("Resource not found")

            item = data[storage_key]
            if field_name not in item:
                item[field_name] = []
            if val not in item[field_name]:
                item[field_name].append(val)

            data[storage_key] = item
            self._save_data(f, data)

    def remove_from_set(self, existing_resource: NonversionedDbResourceOnly, field_name: str, val: str):
        """Remove a value from a set field."""
        if not issubclass(existing_resource.__class__, DynamoDbResource):
            raise TypeError("remove_from_set can only be utilized with non-versioned resources")

        file_path = self._get_resource_file_path(existing_resource.__class__)
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        storage_key = self._make_storage_key(key["pk"], key["sk"])

        with self._lock_and_load(file_path) as (data, f):
            if storage_key not in data:
                raise ValueError("Resource not found")

            item = data[storage_key]
            if field_name in item and val in item[field_name]:
                item[field_name].remove(val)

            data[storage_key] = item
            self._save_data(f, data)

    def list_type_by_updated_at(
        self,
        data_class: Type[AnyDbResource],
        *,
        filter_expression: Optional[ConditionBase] = None,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = None,
        max_api_calls: int = Constants.QUERY_DEFAULT_MAX_API_CALLS,
        pagination_key: Optional[str] = None,
        ascending=False,
        filter_limit_multiplier: int = 3,
    ) -> PaginatedList[AnyDbResource]:
        """List all resources of a type, sorted by updated_at."""
        return self.paginated_dynamodb_query(
            key_condition=Key("gsitype").eq(data_class.db_get_gsitypepk()),
            index_name="gsitype",
            resource_class=data_class,
            filter_expression=filter_expression,
            filter_fn=filter_fn,
            results_limit=results_limit,
            max_api_calls=max_api_calls,
            pagination_key=pagination_key,
            ascending=ascending,
            filter_limit_multiplier=filter_limit_multiplier,
        )

    def paginated_dynamodb_query(
        self,
        *,
        key_condition: ConditionBase,
        resource_class: Type[AnyDbResource] = None,
        resource_class_fn: Callable[[dict], Type[AnyDbResource]] = None,
        index_name: Optional[str] = None,
        filter_expression: Optional[ConditionBase] = None,
        filter_fn: Optional[Callable[[AnyDbResource], bool]] = None,
        results_limit: Optional[int] = None,
        max_api_calls: int = Constants.QUERY_DEFAULT_MAX_API_CALLS,
        pagination_key: Optional[str] = None,
        ascending=False,
        filter_limit_multiplier: int = 3,
        _current_api_calls_on_stack: int = 0,
        _observed_filter_efficiency: Optional[float] = None,
        _total_items_scanned: int = 0,
    ) -> PaginatedList[AnyDbResource]:
        """Execute a paginated query with filtering.

        This is a simplified implementation that loads all matching resources
        into memory and filters them. For local demos, this is acceptable.
        """
        if not (resource_class or resource_class_fn):
            raise ValueError("Must supply either resource_class or resource_class_fn")

        self.logger.info("Beginning paginated local storage query")
        started_at = time.time()

        if results_limit is None or results_limit < 1:
            results_limit = Constants.SYSTEM_DEFAULT_LIMIT

        # Load all items from file
        file_path = self._get_resource_file_path(resource_class)
        if not file_path.exists():
            # Return empty result
            result = PaginatedList([])
            result.limit = results_limit
            result.current_pagination_key = pagination_key
            result.next_pagination_key = None
            result.api_calls_made = 1
            result.rcus_consumed_by_query = 0
            result.query_time_ms = round((time.time() - started_at) * 1000, 3)
            return result

        with self._lock_and_load(file_path) as (data, _):
            # Filter by key condition
            matching_items = []
            for storage_key, item in data.items():
                if self._matches_key_condition(item, key_condition, index_name):
                    matching_items.append(item)

            # Sort items based on index
            if index_name:
                matching_items = self._sort_items(matching_items, index_name, ascending)

            # Load resources and build blob placeholders
            loaded_resources = []
            for item in matching_items:
                if resource_class_fn:
                    data_class = resource_class_fn(item)
                else:
                    data_class = resource_class

                # Build blob placeholders
                blob_placeholders = {}
                if "_blob_fields" in item and self.s3_blob_storage:
                    version = item.get("version")
                    if version is not None:
                        version = int(version)
                    blob_fields_config = data_class.resource_config.get("blob_fields", {}) or {}
                    blob_versions = item.get("_blob_versions", {})

                    for field_name in item["_blob_fields"]:
                        if field_name in blob_fields_config:
                            if version is not None and field_name not in blob_versions:
                                continue

                            s3_key = self.s3_blob_storage._build_s3_key(
                                resource_type=data_class.__name__,
                                resource_id=item["pk"].removeprefix(data_class.get_unique_key_prefix() + "#"),
                                field_name=field_name,
                                version=version,
                            )
                            blob_placeholders[field_name] = BlobPlaceholder(
                                field_name=field_name,
                                s3_key=s3_key,
                                size_bytes=0,
                                content_type=blob_fields_config[field_name].get("content_type"),
                                compressed=blob_fields_config[field_name].get("compress", False),
                            )

                resource = data_class.from_dynamodb_item(item, blob_placeholders=blob_placeholders)
                loaded_resources.append(resource)

        # Apply filter_fn if provided
        if filter_fn:
            response_data = [x for x in loaded_resources if filter_fn(x)]
        else:
            response_data = loaded_resources

        # Handle pagination
        offset = 0
        if pagination_key:
            try:
                decoded_key = decode_pagination_key(pagination_key)
                offset = decoded_key.get("offset", 0)
            except Exception:
                offset = 0

        # Slice for pagination
        paginated_data = response_data[offset : offset + results_limit]

        # Determine if there's more data
        next_pagination_key = None
        if len(response_data) > offset + results_limit:
            next_offset = offset + results_limit
            next_pagination_key = encode_pagination_key({"offset": next_offset})

        # Build result
        result = PaginatedList(paginated_data)
        result.limit = results_limit
        result.current_pagination_key = pagination_key
        result.next_pagination_key = next_pagination_key
        result.api_calls_made = 1
        result.rcus_consumed_by_query = 0
        result.query_time_ms = round((time.time() - started_at) * 1000, 3)
        result.filter_efficiency = len(paginated_data) / len(response_data) if response_data else 1.0
        result.total_items_scanned = len(loaded_resources)

        self.logger.info(
            f"Completed local storage query; query_time_ms={result.query_time_ms} "
            f"items_returned={len(result)} total_scanned={result.total_items_scanned}"
        )

        return result

    def _matches_key_condition(self, item: dict, key_condition: ConditionBase, index_name: Optional[str]) -> bool:
        """Check if an item matches a key condition."""
        # Access the internal structure of boto3 Condition object
        # The Condition object has _values attribute that contains the field name and comparison value

        try:
            # For Key conditions, the structure is:
            # - _values[0] is the attribute name
            # - _values[1] is the comparison value (for eq)
            if hasattr(key_condition, "_values") and len(key_condition._values) >= 2:
                field_name = key_condition._values[0].name
                expected_value = key_condition._values[1]

                # Check if item has the field and it matches
                return item.get(field_name) == expected_value
        except (AttributeError, IndexError) as e:
            self.logger.warning(f"Could not parse key condition: {e}")

        # Fallback: include the item if we can't parse the condition
        return True

    def _sort_items(self, items: list[dict], index_name: str, ascending: bool) -> list[dict]:
        """Sort items based on index."""
        if index_name == "gsitype":
            # Sort by gsitypesk (updated_at)
            items.sort(key=lambda x: x.get("gsitypesk", ""), reverse=not ascending)
        elif index_name in ["gsi1", "gsi2"]:
            # Sort by pk (which includes timestamp for ULID)
            items.sort(key=lambda x: x.get("pk", ""), reverse=not ascending)
        elif index_name == "gsi3":
            # Sort by gsi3sk
            items.sort(key=lambda x: x.get("gsi3sk", ""), reverse=not ascending)

        return items

    # Audit logging methods (reused from DynamoDbMemory)

    def _extract_blob_metadata(
        self,
        field_name: str,
        value: Any,
        resource: AnyDbResource,
        blob_fields_config: dict,
    ) -> Optional[dict[str, Any]]:
        """Extract lightweight metadata for a blob field."""
        if value is None:
            return None

        blob_version = None
        if hasattr(resource, "_blob_versions") and resource._blob_versions:
            blob_version = resource._blob_versions.get(field_name)

        import sys

        size_estimate = sys.getsizeof(str(value))

        return {
            "__blob_ref__": True,
            "size_bytes": size_estimate,
            "version": blob_version,
            "compressed": blob_fields_config[field_name].get("compress", False),
            "content_type": blob_fields_config[field_name].get("content_type"),
        }

    def _build_audit_snapshot(
        self,
        resource: AnyDbResource,
        audit_config: dict,
    ) -> Optional[dict[str, Any]]:
        """Build resource snapshot with blob placeholders."""
        if not audit_config.get("include_snapshot"):
            return None

        snapshot = resource.model_dump()
        blob_fields_config = resource.resource_config.get("blob_fields", {}) or {}

        for field_name in blob_fields_config:
            if field_name in snapshot and snapshot[field_name] is not None:
                blob_meta = self._extract_blob_metadata(
                    field_name,
                    snapshot[field_name],
                    resource,
                    blob_fields_config,
                )

                if blob_meta and self.s3_blob_storage:
                    version = None
                    if isinstance(resource, DynamoDbVersionedResource):
                        version = blob_meta.get("version") or resource.version

                    blob_meta["s3_key"] = self.s3_blob_storage._build_s3_key(
                        resource_type=resource.__class__.__name__,
                        resource_id=resource.resource_id,
                        field_name=field_name,
                        version=version,
                    )

                    snapshot[field_name] = blob_meta

        return snapshot

    def _compute_field_changes(
        self,
        old_resource: AnyDbResource,
        new_resource: AnyDbResource,
        audit_config: dict,
    ) -> Optional[dict[str, dict[str, Any]]]:
        """Compute which fields changed."""
        changed_fields = {}
        exclude_fields = audit_config.get("exclude_fields", set()) or set()
        blob_fields_config = old_resource.resource_config.get("blob_fields", {}) or {}

        old_data = old_resource.model_dump()
        new_data = new_resource.model_dump()

        base_keys = old_resource.get_db_resource_base_keys()
        skip_fields = base_keys | exclude_fields

        for field_name in new_data:
            if field_name in skip_fields:
                continue

            old_val = old_data.get(field_name)
            new_val = new_data.get(field_name)

            if field_name in blob_fields_config:
                old_blob_meta = self._extract_blob_metadata(field_name, old_val, old_resource, blob_fields_config)
                new_blob_meta = self._extract_blob_metadata(field_name, new_val, new_resource, blob_fields_config)

                if old_blob_meta != new_blob_meta:
                    changed_fields[field_name] = {
                        "old": old_blob_meta,
                        "new": new_blob_meta,
                    }
            else:
                if old_val != new_val:
                    changed_fields[field_name] = {
                        "old": old_val,
                        "new": new_val,
                    }

        return changed_fields if changed_fields else None

    def _create_audit_log(
        self,
        operation: str,
        resource: AnyDbResource,
        changed_by: Optional[str],
        old_resource: Optional[AnyDbResource] = None,
        audit_metadata: Optional[dict[str, Any]] = None,
    ):
        """Create an audit log entry for a resource operation."""
        # Don't audit AuditLog itself
        if isinstance(resource, AuditLog):
            return

        audit_config = resource.resource_config.get("audit_config", {}) or {}
        if not audit_config.get("enabled"):
            return

        # Extract changed_by from resource if specified and not provided
        if not changed_by and (field := audit_config.get("changed_by_field")):
            changed_by = getattr(resource, field, None)

        # Validate changed_by if required
        if audit_config.get("changed_by_required") and not changed_by:
            raise ValueError(
                f"Audit logging enabled for {resource.__class__.__name__} with option `changed_by_required` "
                "but 'changed_by' was not provided"
            )

        # Compute field changes for UPDATE operations
        changed_fields = None
        if operation == "UPDATE" and old_resource and audit_config.get("track_field_changes"):
            changed_fields = self._compute_field_changes(old_resource, resource, audit_config)

        # Build snapshot if configured
        snapshot = self._build_audit_snapshot(resource, audit_config)

        # Create the audit log entry
        audit_log_data = {
            "audited_resource_type": resource.__class__.__name__,
            "audited_resource_id": resource.resource_id,
            "operation": operation,
            "changed_by": changed_by,
            "changed_fields": changed_fields,
            "resource_snapshot": snapshot,
            "audit_metadata": audit_metadata or {},
        }

        # Create the audit log
        self.create_new(AuditLog, audit_log_data)

    @property
    def dynamodb_table(self):
        """Property stub for compatibility with enforce_version_limit."""

        # Create a minimal stub that supports the query method
        class TableStub:
            def __init__(self, memory):
                self.memory = memory

            def query(self, **kwargs):
                # This is called by enforce_version_limit
                # Extract resource info from KeyConditionExpression
                kwargs.get("KeyConditionExpression")
                # Parse the expression to get resource type and id
                # For simplicity, we'll just implement this as needed
                # For now, return empty to avoid errors
                return {"Items": []}

        return TableStub(self)
