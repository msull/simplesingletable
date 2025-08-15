import decimal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, Set, Type, TypeVar, Union

import boto3
from boto3.dynamodb.conditions import ConditionBase, Key
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

from .blob_storage import S3BlobStorage
from .models import BlobPlaceholder, DynamoDbResource, DynamoDbVersionedResource, PaginatedList
from .utils import decode_pagination_key, encode_pagination_key, marshall

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table


class Constants:
    SYSTEM_DEFAULT_LIMIT = 250
    QUERY_DEFAULT_MAX_API_CALLS = 10


AnyDbResource = TypeVar("AnyDbResource", bound=Union[DynamoDbVersionedResource, DynamoDbResource])
VersionedDbResourceOnly = TypeVar("VersionedDbResourceOnly", bound=DynamoDbVersionedResource)
NonversionedDbResourceOnly = TypeVar("NonversionedDbResourceOnly", bound=DynamoDbResource)

_PlainBaseModel = TypeVar("_PlainBaseModel", bound=BaseModel)


def exhaust_pagination(query: Callable[[Optional[str]], PaginatedList]):
    result = query(None)
    while result.next_pagination_key:
        yield result
        result = query(result.next_pagination_key)
    yield result


def build_lek_data(db_item: dict, index_name: Optional[str], resource_class: Type[AnyDbResource]) -> dict:
    """Build LastEvaluatedKey data dynamically based on index configuration."""
    lek_data = {"pk": db_item["pk"], "sk": db_item["sk"]}

    if not index_name:
        return lek_data

    # Handle built-in gsitype index
    if index_name == "gsitype":
        if "gsitype" in db_item:
            lek_data["gsitype"] = db_item["gsitype"]
        if "gsitypesk" in db_item:
            lek_data["gsitypesk"] = db_item["gsitypesk"]
        return lek_data

    # Handle dynamic GSI configuration
    gsi_config = resource_class.get_gsi_config()
    if index_name in gsi_config:
        # Add pk field for this index
        pk_field = f"{index_name}pk"
        if pk_field in db_item:
            lek_data[pk_field] = db_item[pk_field]

        # Add sk field if it exists for this index
        sk_field = f"{index_name}sk"
        if sk_field in db_item:
            lek_data[sk_field] = db_item[sk_field]

        return lek_data

    # Handle legacy hardcoded indices for backward compatibility
    if index_name in ["gsi1", "gsi2", "gsi3"]:
        pk_field = f"{index_name}pk"
        if pk_field in db_item:
            lek_data[pk_field] = db_item[pk_field]

        if index_name == "gsi3":
            sk_field = f"{index_name}sk"
            if sk_field in db_item:
                lek_data[sk_field] = db_item[sk_field]

        return lek_data

    raise RuntimeError(f"Unsupported index {index_name=}")


def transact_write_safe(client: "DynamoDBClient", transact_items: list):
    """Execute transact_write_items with better error handling."""
    try:
        return client.transact_write_items(TransactItems=transact_items)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "TransactionCanceledException":
            cancellation_reasons = e.response.get("CancellationReasons", [])
            detailed_reasons = []
            for i, reason in enumerate(cancellation_reasons):
                if reason and reason.get("Code"):
                    detailed_reasons.append(f"Item {i}: {reason['Code']} - {reason.get('Message', 'No message')}")
            if detailed_reasons:
                raise ValueError(f"Transaction failed: {'; '.join(detailed_reasons)}") from e
            else:
                raise ValueError(f"Transaction failed: {cancellation_reasons}") from e
        else:
            raise


class InternalResourceBase(DynamoDbResource):
    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "_INTERNAL"

    @classmethod
    def ensure_exists(cls, memory: "DynamoDbMemory") -> "InternalResourceBase":
        if not (existing := memory.get_existing(cls.pk, data_class=cls)):
            return memory.create_new(cls, {}, override_id=cls.pk)
        return existing


class MemoryStats(InternalResourceBase):
    pk: ClassVar[str] = "MemoryStats"

    counts_by_type: dict[str, int] = Field(default_factory=dict)


@dataclass
class DynamoDbMemory:
    logger: Any
    table_name: str
    endpoint_url: Optional[str] = None
    connection_params: Optional[dict] = None
    track_stats: bool = True
    s3_bucket: Optional[str] = None
    s3_key_prefix: Optional[str] = None
    _dynamodb_client: Optional["DynamoDBClient"] = field(default=None, init=False)
    _dynamodb_table: Optional["Table"] = field(default=None, init=False)
    _s3_blob_storage: Optional["S3BlobStorage"] = field(default=None, init=False)

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
        If load_blobs is True, blob fields will be loaded from S3.
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
        response = self.dynamodb_table.get_item(Key=key, ConsistentRead=consistent_read)
        item = response.get("Item")
        if item:
            # Build blob placeholders if blob fields are present
            blob_placeholders = {}
            if "_blob_fields" in item and self.s3_blob_storage:
                blob_fields_config = data_class.resource_config.get("blob_fields", {}) or {}
                blob_versions = item.get("_blob_versions", {})

                for field_name in item["_blob_fields"]:
                    if field_name in blob_fields_config:
                        # Only create placeholder if this field has a blob stored
                        # Check _blob_versions for versioned resources, or just field presence for non-versioned
                        if issubclass(data_class, DynamoDbVersionedResource):
                            # For versioned resources, check if field has a version reference
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
                            size_bytes=0,  # We don't track size in current implementation
                            content_type=blob_fields_config[field_name].get("content_type"),
                            compressed=blob_fields_config[field_name].get("compress", False),
                        )

            resource = data_class.from_dynamodb_item(item, blob_placeholders)

            # Load blobs if requested
            if load_blobs and resource and resource.has_unloaded_blobs():
                resource.load_blob_fields(self)

            return resource

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

    def update_existing(
        self,
        existing_resource: AnyDbResource,
        update_obj: _PlainBaseModel | dict,
        clear_fields: Optional[Set[str]] = None,
    ) -> AnyDbResource:
        data_class = existing_resource.__class__
        updated_resource = existing_resource.update_existing(update_obj, clear_fields=clear_fields)

        if issubclass(data_class, DynamoDbResource):
            return self._put_nonversioned_resource(updated_resource)
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

            return self.read_existing(
                existing_id=updated_resource.resource_id,
                data_class=data_class,
                version=updated_resource.version,
                consistent_read=True,
            )
        else:
            raise ValueError("Invalid data_class provided")

    @property
    def dynamodb_client(self) -> "DynamoDBClient":
        if not self._dynamodb_client:
            kwargs = self.connection_params or {}
            self._dynamodb_client = boto3.client("dynamodb", endpoint_url=self.endpoint_url, **kwargs)
        return self._dynamodb_client

    @property
    def dynamodb_table(self) -> "Table":
        if not self._dynamodb_table:
            kwargs = self.connection_params or {}
            dynamodb = boto3.resource("dynamodb", endpoint_url=self.endpoint_url, **kwargs)
            self._dynamodb_table = dynamodb.Table(self.table_name)
        return self._dynamodb_table

    @property
    def s3_blob_storage(self) -> Optional[S3BlobStorage]:
        if self.s3_bucket and not self._s3_blob_storage:
            self._s3_blob_storage = S3BlobStorage(
                bucket_name=self.s3_bucket,
                key_prefix=self.s3_key_prefix,
                connection_params=self.connection_params,
                endpoint_url=self.endpoint_url,
            )
        return self._s3_blob_storage

    def create_new(
        self,
        data_class: Type[AnyDbResource],
        data: _PlainBaseModel | dict,
        override_id: Optional[str] = None,
    ) -> AnyDbResource:
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
        return resource

    def delete_existing(self, existing_resource: AnyDbResource):
        if issubclass(existing_resource.__class__, DynamoDbResource):
            self._delete_nonversioned_resource(existing_resource)
        elif issubclass(existing_resource.__class__, DynamoDbVersionedResource):
            self._delete_versioned_resource(existing_resource)
        else:
            raise ValueError("Invalid resource type provided")

    def _delete_nonversioned_resource(self, existing_resource: NonversionedDbResourceOnly):
        self.logger.info(
            f"Deleting resource:{existing_resource.__class__.__name__} "
            f"with resource_id='{existing_resource.resource_id}"
        )
        self.dynamodb_table.delete_item(
            Key=existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        )

        # Delete blob fields from S3 if configured
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
        self.dynamodb_table.delete_item(
            Key=existing_resource.dynamodb_lookup_keys_from_id(
                existing_resource.resource_id, version=existing_resource.version
            )
        )

        # Delete blob fields for this version from S3 if configured
        if self.s3_blob_storage:
            blob_fields_config = existing_resource.resource_config.get("blob_fields", {}) or {}
            for field_name in blob_fields_config:
                self.s3_blob_storage.delete_blob(
                    resource_type=existing_resource.__class__.__name__,
                    resource_id=existing_resource.resource_id,
                    field_name=field_name,
                    version=existing_resource.version,
                )

        # Also delete v0 if this is the latest version
        latest_resource = self.get_existing(
            existing_id=existing_resource.resource_id, data_class=existing_resource.__class__, version=0
        )
        if latest_resource and latest_resource.version == existing_resource.version:
            self.logger.info(
                f"Deleting v0 record for resource:{existing_resource.__class__.__name__} "
                f"with resource_id='{existing_resource.resource_id}'"
            )
            self.dynamodb_table.delete_item(
                Key=existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id, version=0)
            )
            if self.track_stats:
                stats = MemoryStats.ensure_exists(self)
                self.increment_counter(stats, "counts_by_type." + existing_resource.__class__.__name__, -1)

    def delete_all_versions(self, resource_id: str, data_class: Type[VersionedDbResourceOnly]):
        """Delete all versions of a versioned resource."""
        if not issubclass(data_class, DynamoDbVersionedResource):
            raise ValueError("delete_all_versions can only be used with versioned resources")

        from boto3.dynamodb.conditions import Key

        self.logger.info(
            f"Deleting all versions of resource:{data_class.__name__} " f"with resource_id='{resource_id}'"
        )

        # Query all versions for this resource
        versions = self.dynamodb_table.query(
            KeyConditionExpression=Key("pk").eq(f"{data_class.get_unique_key_prefix()}#{resource_id}")
            & Key("sk").begins_with("v"),
            ProjectionExpression="pk, sk",
        )["Items"]

        if not versions:
            self.logger.warning(f"No versions found for resource {resource_id}")
            return

        # Delete all versions using batch writer
        with self.dynamodb_table.batch_writer() as batch:
            for item in versions:
                batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

        self.logger.info(f"Deleted {len(versions)} versions for resource {resource_id}")

        # Delete all blob fields from S3 if configured
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

    def get_stats(self) -> MemoryStats:
        return MemoryStats.ensure_exists(self)

    def _put_nonversioned_resource(self, resource: NonversionedDbResourceOnly) -> NonversionedDbResourceOnly:
        result = resource.to_dynamodb_item()
        # Handle both return types for backward compatibility
        if isinstance(result, tuple):
            item, blob_fields_data = result
        else:
            item, blob_fields_data = result, {}
        self.dynamodb_table.put_item(Item=item)

        # Store blob fields in S3 if configured
        if blob_fields_data and self.s3_blob_storage:
            blob_fields_config = resource.resource_config.get("blob_fields", {}) or {}
            for field_name, value in blob_fields_data.items():
                if field_name in blob_fields_config:
                    self.s3_blob_storage.put_blob(
                        resource_type=resource.__class__.__name__,
                        resource_id=resource.resource_id,
                        field_name=field_name,
                        value=value,
                        config=blob_fields_config[field_name],
                        version=None,
                    )

        return resource

    def _create_new_versioned(self, resource: VersionedDbResourceOnly) -> VersionedDbResourceOnly:
        # Handle both return types for backward compatibility
        result = resource.to_dynamodb_item()
        if isinstance(result, tuple):
            main_item, blob_fields_data = result
        else:
            main_item, blob_fields_data = result, {}

        v0_result = resource.to_dynamodb_item(v0_object=True)
        if isinstance(v0_result, tuple):
            v0_item, _ = v0_result  # v0 uses same blob data
        else:
            v0_item = v0_result
        self.logger.debug("transact_write_items begin")
        transact_write_safe(
            self.dynamodb_client,
            [
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(main_item),
                        "ConditionExpression": "attribute_not_exists(pk) and attribute_not_exists(sk)",
                    }
                },
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(v0_item),
                        "ConditionExpression": "attribute_not_exists(pk) and attribute_not_exists(sk)",
                    }
                },
            ],
        )
        self.logger.debug("transact_write_items complete")

        # Store blob fields in S3 if configured
        if blob_fields_data and self.s3_blob_storage:
            blob_fields_config = resource.resource_config.get("blob_fields", {}) or {}
            for field_name, value in blob_fields_data.items():
                if field_name in blob_fields_config:
                    self.s3_blob_storage.put_blob(
                        resource_type=resource.__class__.__name__,
                        resource_id=resource.resource_id,
                        field_name=field_name,
                        value=value,
                        config=blob_fields_config[field_name],
                        version=resource.version,
                    )

        return self.read_existing(
            existing_id=resource.resource_id,
            data_class=resource.__class__,
            version=resource.version,
            consistent_read=True,
        )

    def _update_existing_versioned(self, resource: VersionedDbResourceOnly, previous_version: int):
        # Handle both return types for backward compatibility
        result = resource.to_dynamodb_item()
        if isinstance(result, tuple):
            main_item, blob_fields_data = result
        else:
            main_item, blob_fields_data = result, {}

        v0_result = resource.to_dynamodb_item(v0_object=True)
        if isinstance(v0_result, tuple):
            v0_item, _ = v0_result  # v0 uses same blob data
        else:
            v0_item = v0_result

        transact_write_safe(
            self.dynamodb_client,
            [
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(main_item),
                        "ConditionExpression": "attribute_not_exists(pk) and attribute_not_exists(sk)",
                    }
                },
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": marshall(v0_item),
                        "ConditionExpression": "attribute_exists(pk) and attribute_exists(sk) and #version = :version",
                        "ExpressionAttributeNames": {"#version": "version"},
                        "ExpressionAttributeValues": marshall({":version": previous_version}),
                    }
                },
            ],
        )

        # Store blob fields in S3 if configured
        if blob_fields_data and self.s3_blob_storage:
            blob_fields_config = resource.resource_config.get("blob_fields", {}) or {}
            for field_name, value in blob_fields_data.items():
                if field_name in blob_fields_config:
                    self.s3_blob_storage.put_blob(
                        resource_type=resource.__class__.__name__,
                        resource_id=resource.resource_id,
                        field_name=field_name,
                        value=value,
                        config=blob_fields_config[field_name],
                        version=resource.version,
                    )

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
        return self.paginated_dynamodb_query(
            key_condition=Key("gsitype").eq(data_class.__name__),
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

    def _increment_mapped_counter(
        self, existing_resource, field_name: str, field: FieldInfo, counter_name: str, incr_by: int = 1
    ):
        now = _now(tz=existing_resource.created_at.tzinfo)
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)

        if not field.annotation == dict[str, int]:
            raise TypeError(f"Field {field_name=} dict of ints; {field.annotation=}")

        update_expression = (
            f"SET {field_name}.#attr1 = if_not_exists({field_name}.#attr1, :start) + :incrval, "
            "updated_at = :nowval, "
            "gsitypesk = :nowval"
        )
        expression_values = {
            ":incrval": decimal.Decimal(incr_by),
            ":start": decimal.Decimal(0),
            ":nowval": now.isoformat(),
        }

        response = self.dynamodb_table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ReturnValues="UPDATED_NEW",
            ExpressionAttributeNames={"#attr1": counter_name},
        )
        return int(response["Attributes"][field_name][counter_name])

    def _increment_nonmapped_counter(self, existing_resource, field_name: str, field: FieldInfo, incr_by: int = 1):
        now = _now(tz=existing_resource.created_at.tzinfo)
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)

        if not field.annotation == int:  # noqa
            raise TypeError(f"Field {field_name=} must be an int; {field.annotation=}")

        response = self.dynamodb_table.update_item(
            Key=key,
            UpdateExpression="SET updated_at = :nowval, gsitypesk = :nowval ADD #attr1 :val1",
            ExpressionAttributeNames={"#attr1": field_name},
            ExpressionAttributeValues={":val1": decimal.Decimal(incr_by), ":nowval": now.isoformat()},
            ReturnValues="UPDATED_NEW",
        )
        self.logger.debug(response)
        return int(response["Attributes"][field_name])

    def increment_counter(
        self, existing_resource: NonversionedDbResourceOnly, field_name: str, incr_by: int = 1
    ) -> int:
        if not issubclass(existing_resource.__class__, DynamoDbResource):
            raise TypeError("increment_counter can only be utilized with non-versioned resources")
        if "." in field_name:
            first_part, remainder = field_name.split(".", maxsplit=1)
            field = existing_resource.model_fields.get(first_part)
            if not field:
                raise ValueError(f"Unknown field {first_part=}")
            return self._increment_mapped_counter(existing_resource, first_part, field, remainder, incr_by)
        else:
            field = existing_resource.model_fields.get(field_name)
            if not field:
                raise ValueError(f"Unknown field {field_name=}")
            return self._increment_nonmapped_counter(existing_resource, field_name, field, incr_by)

    def add_to_set(self, existing_resource: NonversionedDbResourceOnly, field_name: str, val: str):
        if not issubclass(existing_resource.__class__, DynamoDbResource):
            raise TypeError("add_to_set can only be utilized with non-versioned resources")
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        field = existing_resource.model_fields.get(field_name)
        if not field:
            raise ValueError(f"Unknown field {field_name=}")
        if not (field.annotation == set[str] or field.annotation == Optional[set[str]]):
            raise TypeError(f"Field {field_name=} must be set[str]")
        self.dynamodb_table.update_item(
            Key=key,
            UpdateExpression="ADD #attr1 :val1",
            ExpressionAttributeNames={"#attr1": field_name},
            ExpressionAttributeValues={":val1": {val}},
            ReturnValues="NONE",
        )

    def remove_from_set(self, existing_resource: NonversionedDbResourceOnly, field_name: str, val: str):
        if not issubclass(existing_resource.__class__, DynamoDbResource):
            raise TypeError("remove_from_set can only be utilized with non-versioned resources")
        key = existing_resource.dynamodb_lookup_keys_from_id(existing_resource.resource_id)
        field = existing_resource.model_fields.get(field_name)
        if not field:
            raise ValueError(f"Unknown field {field_name=}")
        if not (field.annotation == set[str] or field.annotation == Optional[set[str]]):
            raise TypeError(f"Field {field_name=} must be set[str]")
        self.dynamodb_table.update_item(
            Key=key,
            UpdateExpression="DELETE #attr1 :val1",
            ExpressionAttributeNames={"#attr1": field_name},
            ExpressionAttributeValues={":val1": {val}},
            ReturnValues="NONE",
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
    ) -> PaginatedList[AnyDbResource]:
        """
        Execute a paginated query against a DynamoDB table, supporting filters and optional post-retrieval filtering.

        Parameters:
            key_condition (ConditionBase): The condition used for querying the DynamoDB table.
            resource_class (Type[AnyDbResource], optional): The class type used to deserialize the DynamoDB items.
            resource_class_fn (Callable[[dict], Type[AnyDbResource]], optional): A function to determine
                the resource class type dynamically based on the DynamoDB item data.
            index_name (str, optional): The name of the secondary index to query. If not provided,
                the main table is queried.
            filter_expression (ConditionBase, optional): DynamoDB filter expression to limit results returned.
                Can be constructed using boto3's Attr class (e.g., Attr('status').eq('active')).
            filter_fn (Callable[[AnyDbResource], bool], optional): A post-retrieval filter function to apply to results.
            results_limit (int, optional): The maximum number of results to return. Defaults to system default limit.
            max_api_calls (int): The maximum number of API calls to make. Defaults to QUERY_DEFAULT_MAX_API_CALLS.
            pagination_key (str, optional): Key to start pagination from, if continuing from a previous query.
            ascending (bool): If True, return results in ascending order. Default is False (descending).
            filter_limit_multiplier (int): Multiplier for results limit when using a filter. Default is 3.
            _current_api_calls_on_stack (int, internal): Tracks the number of API calls made
                during recursive operations.

        Returns:
            PaginatedList[AnyDbResource]: A paginated list of deserialized DynamoDB items.

        Raises:
            ValueError: If neither `resource_class` nor `resource_class_fn` is provided.
            RuntimeError: If an unsupported index name is encountered.

        Notes:
            - The method supports both filtering at the DynamoDB level (using `filter_expression`) and post-retrieval
              filtering using the provided `filter_fn` function. The provided filter_fn will receive a single loaded
              database item and should return True if the item should be included in the results, False if not.
            - If the initial results don't meet the `results_limit` and there are more items in DynamoDB to query,
              this method will recursively query until it either meets the desired results count, exhausts the items
              in DynamoDB, or reaches the `max_api_calls` limit.

        Example:
            # Filter by status
            active_items = memory.paginated_dynamodb_query(
                key_condition=Key("gsitype").eq("MyResource"),
                index_name="gsitype",
                resource_class=MyResource,
                filter_expression=Attr("status").eq("active")
            )

            # Compound filter with reserved word handling
            filtered_items = memory.paginated_dynamodb_query(
                key_condition=Key("gsitype").eq("MyResource"),
                index_name="gsitype",
                resource_class=MyResource,
                filter_expression=Attr("status").eq("active") & Attr("size").gt(100)
            )
        """
        if not (resource_class or resource_class_fn):
            raise ValueError("Must supply either resource_class or resource_class_fn")
        self.logger.info("Beginning paginated dynamodb query")
        started_at = time.time()

        if results_limit is None or results_limit < 1:
            results_limit = Constants.SYSTEM_DEFAULT_LIMIT

        query_limit = results_limit
        # if we are doing any filtering increase the number of objects evaluated, to try and limit the number of
        # api calls we need to make to hit the requested limit; sometimes this means we will pull too much data
        if filter_expression or filter_fn:
            filter_limit_multiplier = int(filter_limit_multiplier)
            if filter_limit_multiplier < 1:
                filter_limit_multiplier = 1
                self.logger.warning("filter_limit_multiplier below 1 not supported; used 1 instead")
            query_limit = min(results_limit * filter_limit_multiplier, 1000)
            self.logger.debug(f"{query_limit=}")

        # boto api requires some fields to not be present on the call at all if no values are supplied;
        # build up the call via partials

        # start with basic query function, and ensure we are getting the RCUs utilized
        query_fn = partial(self.dynamodb_table.query, ReturnConsumedCapacity="TOTAL")

        # then build up with index, pagination key, and filter expression
        if index_name:
            query_fn = partial(query_fn, IndexName=index_name)

        exclusive_start_key = None
        if pagination_key:
            try:
                exclusive_start_key = decode_pagination_key(pagination_key)
            except:  # noqa: E722
                pagination_key = None
        if exclusive_start_key:
            query_fn = partial(query_fn, ExclusiveStartKey=exclusive_start_key)

        if filter_expression:
            query_fn = partial(query_fn, FilterExpression=filter_expression)

        # execute the query and load the data
        query_result = query_fn(
            KeyConditionExpression=key_condition,
            Limit=query_limit,
            ScanIndexForward=ascending,
        )

        def _load_item(item):
            blob_placeholders = {}

            if resource_class_fn:
                data_class = resource_class_fn(item)
            else:
                data_class = resource_class
            if "_blob_fields" in item and self.s3_blob_storage:
                version = item.get("version")
                if version is not None:
                    version = int(version)
                else:
                    version = None
                blob_fields_config = data_class.resource_config.get("blob_fields", {}) or {}
                blob_versions = item.get("_blob_versions", {})

                for field_name in item["_blob_fields"]:
                    if field_name in blob_fields_config:
                        # Only create placeholder if this field has a blob stored
                        # Check _blob_versions for versioned resources
                        if version is not None:  # Versioned resource
                            if field_name not in blob_versions:
                                continue  # No blob stored for this field

                        # Build placeholder for this blob field
                        s3_key = self.s3_blob_storage._build_s3_key(
                            resource_type=data_class.__name__,
                            resource_id=item["pk"].removeprefix(data_class.get_unique_key_prefix() + "#"),
                            field_name=field_name,
                            version=version,
                        )
                        blob_placeholders[field_name] = BlobPlaceholder(
                            field_name=field_name,
                            s3_key=s3_key,
                            size_bytes=0,  # We don't track size in current implementation
                            content_type=blob_fields_config[field_name].get("content_type"),
                            compressed=blob_fields_config[field_name].get("compress", False),
                        )
            return data_class.from_dynamodb_item(item, blob_placeholders=blob_placeholders)

        loaded_data = (_load_item(x) for x in query_result["Items"])

        # apply any post-retrieval filtration from the supplied function
        if filter_fn:
            response_data = [x for x in loaded_data if filter_fn(x)]
        else:
            response_data = list(loaded_data)

        # figure out the pagination stuff -- do we have enough results, do we have more data to check on the server,
        #   have we hit the limit on our API calls, etc.
        lek_data = query_result.get("LastEvaluatedKey")
        current_count = len(response_data)

        _current_api_calls_on_stack += 1
        this_call_count = _current_api_calls_on_stack
        rcus_consumed_by_query = query_result["ConsumedCapacity"]["CapacityUnits"]

        if _current_api_calls_on_stack >= max_api_calls:
            self.logger.debug(
                "Reached max API calls before finding requested number of "
                "results or exhausting search; stopping early"
            )
        elif current_count < results_limit:
            # don't have enough results yet -- can we get more?

            if lek_data:
                self.logger.debug(f"Getting more data! Want {results_limit - current_count} more result(s)")
                # recursively call self with the updated limit
                extra_data = self.paginated_dynamodb_query(
                    key_condition=key_condition,
                    resource_class=resource_class,
                    index_name=index_name,
                    filter_expression=filter_expression,
                    filter_fn=filter_fn,
                    results_limit=results_limit - current_count,  # only fetch the amount we need
                    pagination_key=encode_pagination_key(lek_data),  # start from where we just left off
                    ascending=ascending,
                    max_api_calls=max_api_calls,
                    filter_limit_multiplier=filter_limit_multiplier,
                    _current_api_calls_on_stack=_current_api_calls_on_stack,
                )
                response_data += extra_data
                # replace our lek_data with the extra_data's pagination key info
                if extra_data.next_pagination_key:
                    lek_data = decode_pagination_key(extra_data.next_pagination_key)
                else:
                    lek_data = None
                _current_api_calls_on_stack = extra_data.api_calls_made
                rcus_consumed_by_query += extra_data.rcus_consumed_by_query
            else:
                self.logger.debug(f"Want {results_limit - current_count} more results, but no data available")
        elif current_count > results_limit:
            # trim and update lek_data based on the last item in our trimmed result
            response_data = response_data[:results_limit]
            if lek_data:
                self.logger.debug("Have too many results, replacing existing pagination key with new computed one")
            else:
                self.logger.debug("Have too many results, adding a pagination key where one did not exist")
            if issubclass(response_data[-1].__class__, DynamoDbVersionedResource):
                db_item = response_data[-1].to_dynamodb_item(v0_object=True)
            else:
                db_item = response_data[-1].to_dynamodb_item()
            # Use dynamic helper to build LastEvaluatedKey
            lek_data = build_lek_data(db_item, index_name, response_data[-1].__class__)

        if lek_data:
            next_pagination_key = encode_pagination_key(lek_data)
        else:
            next_pagination_key = None

        response_data = PaginatedList(response_data)
        response_data.limit = results_limit
        response_data.current_pagination_key = pagination_key
        response_data.next_pagination_key = next_pagination_key
        response_data.api_calls_made = _current_api_calls_on_stack
        response_data.rcus_consumed_by_query = rcus_consumed_by_query
        response_data.query_time_ms = round((time.time() - started_at) * 1000, 3)

        query_took_ms = response_data.query_time_ms

        items_returned = len(response_data)

        if this_call_count > 1:
            self.logger.debug(
                f"Completed dynamodb recursive sub-query; {query_took_ms=} {this_call_count=} {items_returned=}"
            )
        else:
            api_calls_required = _current_api_calls_on_stack
            self.logger.info(
                f"Completed dynamodb query; {query_took_ms=} {items_returned=} {api_calls_required=} "
                f"{rcus_consumed_by_query=}"
            )

        return response_data


def _now(tz: Any = False):
    # this function exists only to make it easy to mock the utcnow call in date_id when creating resources in the tests

    # explicitly check for False, so that `None` is a valid option to provide for the tz
    if tz is False:
        tz = timezone.utc
    return datetime.now(tz=tz)
