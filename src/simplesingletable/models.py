import gzip
import json
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    TypedDict,
    TypeVar,
    get_args,
    get_origin,
)

import ulid
from boto3.dynamodb.types import Binary
from humanize import naturalsize, precisedelta
from pydantic import BaseModel, ConfigDict, PrivateAttr, TypeAdapter

from .utils import _now, generate_date_sortable_id

if TYPE_CHECKING:
    from .dynamodb_memory import DynamoDbMemory

_T = TypeVar("_T")


class PaginatedList(list[_T]):
    limit: int
    current_pagination_key: Optional[str] = None
    next_pagination_key: Optional[str] = None
    api_calls_made: int = 0
    rcus_consumed_by_query: int = 0
    query_time_ms: Optional[float] = None
    filter_efficiency: Optional[float] = None  # 0.0-1.0, % of scanned items that matched filter
    total_items_scanned: int = 0  # Total items examined across all API calls

    def as_list(self) -> list[_T]:
        return self


_PlainBaseModel = TypeVar("_PlainBaseModel", bound=BaseModel)


class DynamoDbVersionedItemKeys(TypedDict):
    """The specific attributes on the dynamodb items we store"""

    pk: str
    sk: str
    version: int
    data: dict

    # keys for the gsitype index that is automatically applied sparsely on v0 objects
    # the sk value is the "updated_at" datetime value on the object, meaning the gsitype index
    # sorts by modified time of the objects for any particular type
    gsitype: Optional[str]
    gsitypesk: Optional[str]

    # user-defineable attributes, used sparsely on the v0 object to enable secondary lookups / access patterns
    # gsi1 and gsi2 use the pk as the range key; using the default ID generation system, this means it automatically
    # sorts by the creation time of the resources
    # use this for access patterns like "all resources associated with a parent object"
    # and set the pk value to "parent_id#<actual id value>"
    # or use it for categories, or tracking COMPLETE/INCOMPLETE by setting static string values
    # e.g. if you were managing a "Task" resource, you might want to easily be able to find all complete/incomplete
    # tasks and set `gsi1pk` to "t|COMPLETE" or "t|INCOMPLETE" based on the "completed" attribute of the Task

    # gsi3 has a separate sortkey the user defines, to enable lookups that sort by something other than created_at
    gsi1pk: Optional[str]
    gsi2pk: Optional[str]
    gsi3pk: Optional[str]
    gsi3sk: Optional[str]
    metadata: Optional[dict]  # user supplied metadata for anything that needs to be accessible to dynamodb filter expr


class BlobFieldConfig(TypedDict, total=False):
    """Configuration for a blob field stored in S3."""

    compress: bool
    """Whether to compress the blob data before storing in S3."""

    content_type: str | None
    """Optional content type for the blob (e.g., 'application/json')."""

    max_size_bytes: int | None
    """Optional maximum size limit for the blob in bytes."""


class AuditConfig(TypedDict, total=False):
    """Configuration for audit logging behavior."""

    enabled: bool
    """Enable audit logging for this resource type."""

    track_field_changes: bool
    """Track individual field changes (old vs new values)."""

    exclude_fields: set[str] | None
    """Fields to exclude from audit logging (e.g., sensitive data)."""

    include_snapshot: bool
    """Include full resource snapshot in audit log."""

    changed_by_field: str | None
    """Field name containing user/service identifier for change tracking."""


class ResourceConfig(TypedDict, total=False):
    """A TypedDict for configuring Resource behaviour."""

    compress_data: bool | None
    """Should the resource content be compress (gzip)."""

    max_versions: int | None
    """For versioned resources, the maximum number of versions to keep."""

    blob_fields: Dict[str, BlobFieldConfig] | None
    """Configuration for fields that should be stored as blobs in S3."""

    ttl_field: str | None
    """The field name in the resource that contains the TTL value (datetime, timedelta, or int)."""

    ttl_attribute_name: str | None
    """The DynamoDB attribute name for TTL (defaults to 'ttl')."""

    audit_config: AuditConfig | None
    """Configuration for audit logging behavior."""


class BlobPlaceholder(TypedDict):
    """Metadata for a blob field stored in S3."""

    field_name: str
    s3_key: str
    size_bytes: int
    content_type: Optional[str]
    compressed: bool


class BaseDynamoDbResource(BaseModel, ABC):
    """Exists only to provide a common parent for the resource classes."""

    resource_id: str
    created_at: datetime
    updated_at: datetime

    gsi_config: ClassVar[Dict[str, Dict]] = {}
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=None, max_versions=None, blob_fields=None)

    _blob_placeholders: Dict[str, BlobPlaceholder] = PrivateAttr(default_factory=dict)
    _blob_versions: Dict[str, int] = PrivateAttr(default_factory=dict)

    @classmethod
    def get_gsi_config(cls) -> Dict[str, Dict]:
        """Get the GSI configuration for this resource.

        Override this method to provide dynamic GSI configuration.
        By default, returns the class variable gsi_config.

        Returns:
            Dictionary mapping GSI names to their field configurations.
        """
        return cls.gsi_config

    @abstractmethod
    def get_db_resource_base_keys(self) -> set[str]:
        """Returns a set of the string values corresponding to all of attributes on the Base resource object.

        For example, this will return something like {"resource_id", "created_at", "updated_at"} along with others,
        depending on which resource class is being used (for example a versioned resource will have a "version"
        attribute included.

        This can be useful for filtering out all the base attributes, e.g. when calling pydantic's model_dump.
        """

    # override these in resource classes to enable secondary lookups on the latest version of the resource
    def db_get_gsi1pk(self) -> str | None:
        pass

    def db_get_gsi2pk(self) -> str | None:
        pass

    def db_get_gsi3pk_and_sk(self) -> tuple[str, str] | None:
        pass

    def db_get_filter_metadata(self) -> tuple[str, str] | None:
        pass

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return cls.__name__

    def db_get_gsitypesk(self) -> str:
        return self.updated_at.isoformat()

    def resource_id_as_ulid(self) -> ulid.ULID:
        return ulid.parse(self.resource_id)

    def created_ago(self, now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.created_at), minimum_unit="minutes") + " ago"

    def updated_ago(self, now: Optional[datetime] = None) -> str:
        now = now or _now(tz=self.created_at.tzinfo)
        return precisedelta((now - self.updated_at), minimum_unit="minutes") + " ago"

    def get_db_item_size_in_bytes(self) -> int:
        """Return the size of the database item, in bytes."""
        return sys.getsizeof(json.dumps(self.to_dynamodb_item(), default=str))

    def get_db_item_size(self) -> str:
        return naturalsize(self.get_db_item_size_in_bytes())

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return cls.__name__

    def compress_model_content(self) -> bytes:
        """Helper that can be used in to_dynamodb_item."""
        return gzip.compress(self.model_dump_json().encode())

    @staticmethod
    def decompress_model_content(content: bytes | Binary) -> dict:
        if isinstance(content, Binary):
            content = bytes(content)  # noqa
        entry_data: str = gzip.decompress(content).decode()
        return json.loads(entry_data)

    def _extract_blob_fields(self, model_data: dict) -> tuple[dict, dict]:
        """Extract blob fields from model data.

        Returns:
            tuple of (model_data_without_blobs, blob_fields_data)
        """
        blob_fields_config = self.resource_config.get("blob_fields", {}) or {}
        blob_fields_data = {}

        # Extract blob fields from model data
        for field_name in blob_fields_config:
            if field_name in model_data:
                blob_fields_data[field_name] = model_data.pop(field_name)

        return model_data, blob_fields_data

    def _extract_blob_field_values(self) -> dict:
        """Extract blob field values directly from the model instance.

        This preserves Pydantic model instances instead of converting to dicts,
        which avoids TypeAdapter warnings during serialization.

        Includes None values to ensure they're excluded from model_dump(),
        but caller should skip storing None values in S3.

        Returns:
            dict of blob field values (as actual instances or None, not dicts)
        """
        blob_fields_config = self.resource_config.get("blob_fields", {}) or {}
        blob_fields_data = {}

        for field_name in blob_fields_config:
            # Extract the value (including None) if the field exists
            if hasattr(self, field_name):
                blob_fields_data[field_name] = getattr(self, field_name)

        return blob_fields_data

    def _apply_gsi_configuration(self, dynamodb_data: dict) -> None:
        """Apply dynamic GSI configuration to DynamoDB item."""
        # Apply dynamic GSI configuration
        gsi_config = self.get_gsi_config()
        for fields in gsi_config.values():
            for key, value_or_func in fields.items():
                # Handle tuple keys like ("gsi3pk", "gsi3sk")
                if isinstance(key, tuple):
                    # This is a combined pk/sk definition
                    if value_or_func and callable(value_or_func):
                        result = value_or_func(self)
                        if result:
                            # Result should be a tuple of (pk_value, sk_value)
                            if len(key) == 2 and len(result) == 2:
                                dynamodb_data[key[0]] = result[0]
                                dynamodb_data[key[1]] = result[1]
                else:
                    # Handle regular single-field definitions
                    if value_or_func:
                        if callable(value_or_func):
                            if value := value_or_func(self):
                                dynamodb_data[key] = value
                        else:
                            dynamodb_data[key] = value_or_func

        # Legacy GSI methods for backward compatibility
        if gsi1pk := self.db_get_gsi1pk():
            dynamodb_data["gsi1pk"] = gsi1pk
        if gsi2pk := self.db_get_gsi2pk():
            dynamodb_data["gsi2pk"] = gsi2pk
        if data := self.db_get_gsi3pk_and_sk():
            gsi3pk, gsi3sk = data
            dynamodb_data["gsi3pk"] = gsi3pk
            dynamodb_data["gsi3sk"] = gsi3sk

    def _add_blob_metadata(self, dynamodb_data: dict, blob_fields_config: dict, blob_fields_data: dict):
        """Add blob metadata to DynamoDB item and return appropriate result.

        Returns:
            dict if no blob fields configured (backward compatibility)
            tuple[dict, dict] if blob fields are configured with data
        """
        if blob_fields_config:
            # Include blob fields that have data OR have version references
            blob_fields_with_data = [k for k, v in blob_fields_data.items() if v is not None]
            blob_fields_with_versions = list(self._blob_versions.keys()) if self._blob_versions else []

            # Combine both lists (union of fields with data and fields with version refs)
            all_blob_fields = list(set(blob_fields_with_data + blob_fields_with_versions))
            if all_blob_fields:
                dynamodb_data["_blob_fields"] = all_blob_fields

            # Include blob version references if any exist
            if self._blob_versions:
                dynamodb_data["_blob_versions"] = self._blob_versions

            # Return tuple only if there's actual blob data to store (non-None)
            blob_data_to_store = {k: v for k, v in blob_fields_data.items() if v is not None}
            if blob_data_to_store:
                return dynamodb_data, blob_data_to_store

        # Return just dict for backward compatibility when no blob fields
        return dynamodb_data

    def _calculate_ttl(self) -> Optional[int]:
        """Calculate TTL value based on resource configuration.

        Returns:
            Unix timestamp for TTL or None if no TTL configured
        """
        ttl_field = self.resource_config.get("ttl_field")
        ttl_attribute_name = self.resource_config.get("ttl_attribute_name")

        # Both must be set for TTL to be enabled
        if not ttl_field or not ttl_attribute_name:
            return None

        ttl_value = getattr(self, ttl_field, None)
        if ttl_value is None:
            return None

        # Handle different TTL value types
        if isinstance(ttl_value, datetime):
            # Direct datetime - use as absolute expiration
            return int(ttl_value.timestamp())
        elif isinstance(ttl_value, int):
            # Integer seconds - add to created_at
            expiration = self.created_at + timedelta(seconds=ttl_value)
            return int(expiration.timestamp())
        else:
            raise ValueError(
                f"Unsupported TTL field type: {type(ttl_value).__name__}. Only datetime and int are supported."
            )

    @abstractmethod
    def to_dynamodb_item(self):
        """Convert to DynamoDB item.

        Returns either:
        - dict: DynamoDB item (for backward compatibility)
        - tuple[dict, dict]: (DynamoDB item, blob fields data)
        """
        pass

    @classmethod
    def _build_resource_from_data(cls, data: dict, blob_placeholders: Optional[Dict[str, BlobPlaceholder]] = None):
        """Build resource instance from data dictionary.

        Handles blob fields and placeholders.
        Converts Decimal back to float for List[float] typed fields.
        """
        # Extract metadata before processing
        blob_field_names = data.pop("_blob_fields", [])
        blob_versions = data.pop("_blob_versions", {})

        # Handle blob fields
        blob_fields_config = cls.resource_config.get("blob_fields", {}) or {}

        # Set blob fields to None if they're configured as blobs
        for field_name in blob_field_names:
            if field_name in blob_fields_config:
                data[field_name] = None

        # Convert Decimal to float for List[float] typed fields
        for field_name, field_info in cls.model_fields.items():
            if field_name in data:
                field_type = field_info.annotation
                # Check if field is List[float] or similar
                if get_origin(field_type) in (list, List):
                    args = get_args(field_type)
                    if args and args[0] is float:
                        # Convert any Decimal values in the list to float
                        if isinstance(data[field_name], list):
                            data[field_name] = [
                                float(item) if isinstance(item, Decimal) else item for item in data[field_name]
                            ]

        # Create the resource instance
        resource = cls.model_validate(data)

        # Store blob placeholders if provided
        if blob_placeholders:
            resource._blob_placeholders = blob_placeholders

        # Restore blob version references
        if blob_versions:
            resource._blob_versions = blob_versions

        return resource

    @classmethod
    def _get_excluded_dynamodb_keys(cls) -> set[str]:
        """Get the set of DynamoDB-specific keys to exclude when building from item."""
        excluded_keys = {"pk", "sk", "gsitypesk", "gsitype", "_blob_fields", "_blob_versions", "_version_token"}

        # Add any dynamic GSI fields to exclusion
        gsi_config = cls.get_gsi_config()
        for fields in gsi_config.values():
            for key in fields:
                # Handle tuple keys like ("gsi3pk", "gsi3sk")
                if isinstance(key, tuple):
                    excluded_keys.update(key)
                else:
                    excluded_keys.add(key)

        # Also exclude legacy GSI fields
        excluded_keys.update({"gsi1pk", "gsi2pk", "gsi3pk", "gsi3sk"})

        # Exclude TTL attribute if configured
        ttl_attr = cls.resource_config.get("ttl_attribute_name")
        if ttl_attr:
            excluded_keys.add(ttl_attr)

        return excluded_keys

    def has_unloaded_blobs(self) -> bool:
        """Check if this resource has blob fields that haven't been loaded."""
        return bool(self._blob_placeholders)

    def get_unloaded_blob_fields(self) -> list[str]:
        """Get list of blob field names that haven't been loaded."""
        return list(self._blob_placeholders.keys())

    def load_blob_fields(self, memory: "DynamoDbMemory", fields: Optional[list[str]] = None) -> None:
        """Load blob fields from S3.

        Args:
            memory: DynamoDbMemory instance with S3 configuration
            fields: Optional list of specific fields to load. If None, loads all blob fields.
        """
        if not memory.s3_blob_storage:
            raise ValueError("S3 blob storage not configured in DynamoDbMemory")

        if not self._blob_placeholders:
            return  # No blobs to load

        fields_to_load = fields or list(self._blob_placeholders.keys())

        for field_name in fields_to_load:
            if field_name not in self._blob_placeholders:
                continue

            # Get version for versioned resources - use blob version reference if available
            if isinstance(self, DynamoDbVersionedResource):
                # Use the referenced version for this blob field, or fallback to current version
                version = self._blob_versions.get(field_name, getattr(self, "version", None))
            else:
                version = None

            # Load blob from S3
            blob_data = memory.s3_blob_storage.get_blob(
                resource_type=self.__class__.__name__,
                resource_id=self.resource_id,
                field_name=field_name,
                version=version,
            )

            # Reconstruct proper types from deserialized data
            # This converts dicts back to Pydantic models for fields like list[BaseModel]
            field_info = self.model_fields.get(field_name)
            if field_info and field_info.annotation and blob_data is not None:
                try:
                    type_adapter = TypeAdapter(field_info.annotation)
                    blob_data = type_adapter.validate_python(blob_data)
                except Exception:
                    # If type validation fails, use raw data (backward compatibility)
                    pass

            # Set the field value
            setattr(self, field_name, blob_data)

            # Remove from placeholders
            del self._blob_placeholders[field_name]


class DynamoDbResource(BaseDynamoDbResource, ABC):
    resource_id: str
    created_at: datetime
    updated_at: datetime
    _version_token: Optional[str] = PrivateAttr(default=None)

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        # Merge base resource_config into child if it defines its own
        if "resource_config" in cls.__dict__:
            merged = DynamoDbResource.resource_config.copy()
            merged.update(cls.__dict__["resource_config"])
            cls.resource_config = merged
        else:
            # Inherit from base if not defined
            cls.resource_config = DynamoDbResource.resource_config.copy()

    def get_db_resource_base_keys(self) -> set[str]:
        return {"resource_id", "created_at", "updated_at"}

    def to_dynamodb_item(self):
        """Convert resource to DynamoDB item format.

        Returns:
            dict if no blob fields configured (backward compatibility)
            tuple[dict, dict] if blob fields are configured
        """
        prefix = self.get_unique_key_prefix()
        key = f"{prefix}#{self.resource_id}"

        # Extract blob field values BEFORE model_dump() to preserve Pydantic instances
        blob_fields_data = self._extract_blob_field_values()

        if self.resource_config["compress_data"]:
            # When compressing, use model_dump_json directly with exclude to preserve nested Pydantic models
            # This avoids the model_dump() -> model_copy() round-trip that converts nested models to dicts
            blob_field_names = set(blob_fields_data.keys()) if blob_fields_data else None
            compressed_json = self.model_dump_json(exclude=blob_field_names)
            dynamodb_data = {"data": gzip.compress(compressed_json.encode())}
        else:
            # Get model data (blob fields will be excluded from dump)
            model_data = self.model_dump(exclude=set(blob_fields_data.keys()) if blob_fields_data else None)
            dynamodb_data = clean_data(model_data)

        dynamodb_data.update(
            {
                "pk": key,
                "sk": key,
                "gsitype": self.db_get_gsitypepk(),
                "gsitypesk": self.db_get_gsitypesk(),
            }
        )

        # Add version token if present for optimistic locking
        if self._version_token:
            dynamodb_data["_version_token"] = self._version_token

        # Apply GSI configuration
        self._apply_gsi_configuration(dynamodb_data)

        # Add TTL if configured (both ttl_field and ttl_attribute_name must be set)
        if ttl_value := self._calculate_ttl():
            ttl_attr = self.resource_config.get("ttl_attribute_name")
            dynamodb_data[ttl_attr] = ttl_value

        # Add blob metadata and return
        blob_fields_config = self.resource_config.get("blob_fields", {}) or {}
        return self._add_blob_metadata(dynamodb_data, blob_fields_config, blob_fields_data)

    @classmethod
    def from_dynamodb_item(
        cls: Type["DynamoDbResource"],
        dynamodb_data: DynamoDbVersionedItemKeys | dict,
        blob_placeholders: Optional[Dict[str, BlobPlaceholder]] = None,
    ) -> "DynamoDbResource":
        if cls.resource_config["compress_data"]:
            compressed_data = dynamodb_data["data"]
            data = cls.decompress_model_content(compressed_data)  # noqa
        else:
            # Filter out DynamoDB-specific keys
            excluded_keys = cls._get_excluded_dynamodb_keys()
            data = {k: v for k, v in dynamodb_data.items() if k not in excluded_keys}

        # Add metadata back temporarily for _build_resource_from_data to process
        data["_blob_fields"] = dynamodb_data.get("_blob_fields", [])
        data["_blob_versions"] = dynamodb_data.get("_blob_versions", {})

        resource = cls._build_resource_from_data(data, blob_placeholders)

        # Restore version token if present
        if "_version_token" in dynamodb_data:
            resource._version_token = dynamodb_data["_version_token"]

        return resource

    @classmethod
    def dynamodb_lookup_keys_from_id(cls, existing_id: str) -> dict:
        key = f"{cls.get_unique_key_prefix()}#{existing_id}"
        return {"pk": key, "sk": key}

    @classmethod
    def create_new(
        cls: Type["DynamoDbResource"],
        create_data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> "DynamoDbResource":
        if isinstance(create_data, BaseModel):
            kwargs = create_data.model_dump()
        else:
            kwargs = {**create_data}
        now = _now()
        kwargs.update(
            {"resource_id": override_id or generate_date_sortable_id(now), "created_at": now, "updated_at": now}
        )
        return cls.model_validate(kwargs)

    def update_existing(
        self: "DynamoDbResource", update_data: _PlainBaseModel | dict, clear_fields: Optional[set[str]] = None
    ) -> "DynamoDbResource":
        now = _now()
        if isinstance(update_data, BaseModel):
            update_kwargs = update_data.model_dump(exclude_none=True)
        else:
            update_kwargs = {**update_data}

        # Handle clear_fields
        if clear_fields:
            for field_name in clear_fields:
                update_kwargs[field_name] = None

        kwargs = self.model_dump()
        kwargs.update(update_kwargs)
        kwargs.update({"resource_id": self.resource_id, "created_at": self.created_at, "updated_at": now})
        return self.__class__.model_validate(kwargs)


# for backwards compatibility
DynamodbResource = DynamoDbResource


class DynamoDbVersionedResource(BaseDynamoDbResource, ABC):
    resource_id: str
    version: int
    created_at: datetime
    updated_at: datetime

    def get_db_resource_base_keys(self) -> set[str]:
        return {"resource_id", "version", "created_at", "updated_at"}

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=True, max_versions=None)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        # Merge base resource_config into child if it defines its own
        if "resource_config" in cls.__dict__:
            merged = DynamoDbVersionedResource.resource_config.copy()
            merged.update(cls.__dict__["resource_config"])
            cls.resource_config = merged
        else:
            # Inherit from base if not defined
            cls.resource_config = DynamoDbVersionedResource.resource_config.copy()

    def to_dynamodb_item(self, v0_object: bool = False):
        """Convert resource to DynamoDB item format.

        Returns:
            dict if no blob fields configured (backward compatibility)
            tuple[dict, dict] if blob fields are configured
        """
        prefix = self.get_unique_key_prefix()
        key = f"{prefix}#{self.resource_id}"

        # Extract blob field values BEFORE model_dump() to preserve Pydantic instances
        blob_fields_data = self._extract_blob_field_values()

        if self.resource_config["compress_data"]:
            # When compressing, use model_dump_json directly with exclude to preserve nested Pydantic models
            # This avoids the model_dump() -> model_copy() round-trip that converts nested models to dicts
            blob_field_names = set(blob_fields_data.keys()) if blob_fields_data else None
            compressed_json = self.model_dump_json(exclude=blob_field_names)
            dynamodb_data = {"data": gzip.compress(compressed_json.encode())}
        else:
            # Get model data (blob fields will be excluded from dump)
            model_data = self.model_dump(exclude=set(blob_fields_data.keys()) if blob_fields_data else None)
            dynamodb_data = clean_data(model_data)

        dynamodb_data.update({"pk": key, "version": self.version})

        if v0_object:
            sk = "v0"
        else:
            sk = f"v{self.version}"
        dynamodb_data["sk"] = sk

        if v0_object:
            # all v0 objects get gsitype applied to enable "get all <type> sorted by last updated"
            dynamodb_data["gsitype"] = self.db_get_gsitypepk()
            dynamodb_data["gsitypesk"] = self.db_get_gsitypesk()

            # Apply GSI configuration
            self._apply_gsi_configuration(dynamodb_data)

            # Add filter metadata (versioned-specific)
            if filter_metadata := self.db_get_filter_metadata():
                dynamodb_data["metadata"] = filter_metadata

        # Add TTL if configured (applies to both v0 and version items)
        if ttl_value := self._calculate_ttl():
            ttl_attr = self.resource_config.get("ttl_attribute_name")
            dynamodb_data[ttl_attr] = ttl_value

        # Add blob metadata and return
        blob_fields_config = self.resource_config.get("blob_fields", {}) or {}
        return self._add_blob_metadata(dynamodb_data, blob_fields_config, blob_fields_data)

    @classmethod
    def from_dynamodb_item(
        cls: Type["DynamoDbVersionedResource"],
        dynamodb_data: DynamoDbVersionedItemKeys | dict,
        blob_placeholders: Optional[Dict[str, BlobPlaceholder]] = None,
    ) -> "DynamoDbVersionedResource":
        if cls.resource_config["compress_data"]:
            compressed_data = dynamodb_data["data"]
            data = cls.decompress_model_content(compressed_data)  # noqa
        else:
            # Filter out DynamoDB-specific keys
            excluded_keys = cls._get_excluded_dynamodb_keys()
            data = {k: v for k, v in dynamodb_data.items() if k not in excluded_keys}

        # Add metadata back temporarily for _build_resource_from_data to process
        data["_blob_fields"] = dynamodb_data.get("_blob_fields", [])
        data["_blob_versions"] = dynamodb_data.get("_blob_versions", {})

        return cls._build_resource_from_data(data, blob_placeholders)

    @classmethod
    def dynamodb_lookup_keys_from_id(cls, existing_id: str, version: int = 0) -> dict:
        return {
            "pk": f"{cls.get_unique_key_prefix()}#{existing_id}",
            "sk": f"v{version}",
        }

    @classmethod
    def create_new(
        cls: Type["DynamoDbVersionedResource"],
        create_data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> "DynamoDbVersionedResource":
        if isinstance(create_data, BaseModel):
            kwargs = create_data.model_dump()
        else:
            kwargs = {**create_data}
        now = _now()
        kwargs.update(
            {
                "version": 1,
                "resource_id": override_id or generate_date_sortable_id(now),
                "created_at": now,
                "updated_at": now,
            }
        )
        new_resource = cls.model_validate(kwargs)

        # Set blob version references for any blob fields that have data
        blob_fields_config = cls.resource_config.get("blob_fields", {}) or {}
        if blob_fields_config:
            blob_versions = {}
            for field_name in blob_fields_config:
                if field_name in kwargs and kwargs[field_name] is not None:
                    # This field has data and will be stored as a blob at version 1
                    blob_versions[field_name] = 1

            if blob_versions:
                new_resource._blob_versions = blob_versions

        return new_resource

    def update_existing(
        self: "DynamoDbVersionedResource", update_data: _PlainBaseModel | dict, clear_fields: Optional[set[str]] = None
    ) -> "DynamoDbVersionedResource":
        now = _now()
        if isinstance(update_data, BaseModel):
            update_kwargs = update_data.model_dump(exclude_none=True)
        else:
            update_kwargs = {**update_data}

        # Handle clear_fields
        if clear_fields:
            for field_name in clear_fields:
                update_kwargs[field_name] = None

        kwargs = self.model_dump()
        kwargs.update(update_kwargs)
        kwargs.update(
            {
                "version": self.version + 1,
                "resource_id": self.resource_id,
                "created_at": self.created_at,
                "updated_at": now,
            }
        )

        # Create the new resource instance
        new_resource = self.__class__.model_validate(kwargs)

        # Handle blob version references
        blob_fields_config = self.resource_config.get("blob_fields", {}) or {}
        if blob_fields_config:
            new_blob_versions = {}

            for field_name in blob_fields_config:
                # Check if this field is being updated or cleared
                if field_name in update_kwargs:
                    if update_kwargs[field_name] is not None:
                        # Field is being updated with new data - will get new version
                        new_blob_versions[field_name] = new_resource.version
                    # If None (cleared), don't add to blob_versions
                elif field_name in self._blob_versions:
                    # Field not being updated - preserve existing version reference
                    new_blob_versions[field_name] = self._blob_versions[field_name]
                elif field_name not in self._blob_placeholders and getattr(self, field_name, None) is not None:
                    # Field has data but no version reference (loaded directly) - use current version
                    new_blob_versions[field_name] = self.version

            # Set the blob versions on the new resource
            new_resource._blob_versions = new_blob_versions

        return new_resource

    @classmethod
    def enforce_version_limit(cls, memory: "DynamoDbMemory", resource_id: str):
        """Enforce the max_versions limit by deleting old versions."""
        max_versions = cls.resource_config.get("max_versions", None)
        if not max_versions or max_versions < 1:
            return

        from boto3.dynamodb.conditions import Key

        # Query all versions for this resource
        versions = memory.dynamodb_table.query(
            KeyConditionExpression=Key("pk").eq(f"{cls.get_unique_key_prefix()}#{resource_id}")
            & Key("sk").begins_with("v"),
            ScanIndexForward=True,  # Ascending order (oldest first)
            ProjectionExpression="pk, sk, version",
        )["Items"]

        # Filter out v0 if present
        versions = [v for v in versions if v["sk"] != "v0"]

        if len(versions) <= max_versions:
            return

        # Sort by actual version number (not SK) to handle double-digit versions correctly
        versions.sort(key=lambda x: int(x["version"]))

        # Delete oldest versions, keeping only the most recent max_versions
        to_delete = versions[:-max_versions]
        with memory.dynamodb_table.batch_writer() as batch:
            for item in to_delete:
                batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

        memory.logger.info(f"Deleted {len(to_delete)} old versions for resource {resource_id}")


DynamodbVersionedResource = DynamoDbVersionedResource


class AuditLog(DynamodbResource):
    """Audit log resource for tracking changes to other resources.

    This resource automatically tracks CREATE, UPDATE, DELETE, and RESTORE operations
    on other resources. It can track field-level changes, full resource snapshots,
    and custom audit metadata.

    Note: AuditLog disables compression (compress_data=False) to allow efficient
    DynamoDB filter expressions on fields like 'operation' and 'changed_by'.

    GSI Configuration:
    - gsi1pk: Query all changes to a specific resource (resource_type#resource_id) sorted by creation time
    - gsi2pk: Query all changes by resource type, sorted by creation time
    """

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)

    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "_INTERNAL#AuditLog"

    @classmethod
    def db_get_gsitypepk(cls) -> str:
        return cls.get_unique_key_prefix()

    def db_get_gsitypesk(self) -> str:
        return self.created_at.isoformat()

    audited_resource_type: str
    """The type of resource being audited (e.g., 'User', 'Project')."""

    audited_resource_id: str
    """The ID of the resource being audited."""

    operation: str
    """The operation performed: 'CREATE', 'UPDATE', 'DELETE', or 'RESTORE'."""

    changed_by: Optional[str] = None
    """Identifier of the user or service that made the change."""

    changed_fields: Optional[Dict[str, Dict[str, Any]]] = None
    """Field-level changes for UPDATE operations.

    Format: {"field_name": {"old": old_value, "new": new_value}}
    """

    resource_snapshot: Optional[Dict[str, Any]] = None
    """Full snapshot of the resource after the operation."""

    audit_metadata: Dict[str, Any] = {}
    """Custom audit metadata (e.g., reason for change, request ID, etc.)."""

    @classmethod
    def get_gsi_config(cls) -> Dict[str, Dict]:
        """Configure GSIs for querying audit logs."""
        prefix = cls.get_unique_key_prefix()
        return {
            "gsi1": {"gsi1pk": lambda self: f"{prefix}#{self.audited_resource_type}#{self.audited_resource_id}"},
            "gsi2": {"gsi2pk": lambda self: f"{prefix}#{self.audited_resource_type}"},
        }


# def _now(tz: Any = False):
#     # this function exists only to make it easy to mock the utcnow call in date_id when creating resources in the tests
#
#     # explicitly check for False, so that `None` is a valid option to provide for the tz
#     if tz is False:
#         tz = timezone.utc
#     return datetime.now(tz=tz)


def clean_data(data: dict):
    from decimal import Decimal

    data = {**data}
    del_keys = set()
    for key, value in data.items():
        if isinstance(value, float):
            # convert floats to Decimal for DynamoDB compatibility
            data[key] = Decimal(str(value))
        elif isinstance(value, datetime):
            # convert datetimes to isoformat -- dynamodb has no native datetime
            data[key] = value.isoformat()
        elif isinstance(value, set) and not value:
            # clear out empty sets entirely from the data
            del_keys.add(key)
        elif isinstance(value, dict):
            # run recursively on dicts
            data[key] = clean_data(value)
        elif isinstance(value, list):
            # handle lists that might contain floats
            data[key] = _clean_list(value)

    for key in del_keys:
        data.pop(key)

    return data


def _clean_list(lst: list):
    """Clean list items, converting floats to Decimal."""

    cleaned = []
    for item in lst:
        if isinstance(item, float):
            cleaned.append(Decimal(str(item)))
        elif isinstance(item, dict):
            cleaned.append(clean_data(item))
        elif isinstance(item, list):
            cleaned.append(_clean_list(item))
        else:
            cleaned.append(item)
    return cleaned
