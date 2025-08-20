"""Transaction support for DynamoDB single-table operations."""

from __future__ import annotations

import logging
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union

from botocore.exceptions import ClientError
from ulid import ULID

from .models import DynamoDbResource, DynamoDbVersionedResource
from .utils import marshall

if TYPE_CHECKING:
    from .dynamodb_memory import DynamoDbMemory

logger = logging.getLogger(__name__)


class TransactionError(Exception):
    """Raised when a transaction fails."""

    pass


class VersionConflictError(TransactionError):
    """Raised when a transaction fails due to version conflict."""

    pass


class ResourceNotFoundError(Exception):
    """Raised when a resource is not found."""

    pass


class OperationType(Enum):
    """Types of operations that can be performed in a transaction."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    INCREMENT = "increment"
    APPEND = "append"


@dataclass
class TransactionOperation:
    """Represents a single operation within a transaction."""

    operation_type: OperationType
    resource_class: Type[DynamoDbResource]
    resource: Optional[DynamoDbResource] = None
    resource_id: Optional[str] = None
    updates: Optional[Dict[str, Any]] = None
    field_name: Optional[str] = None
    value: Optional[Any] = None
    condition: Optional[str] = None
    condition_values: Optional[Dict[str, Any]] = None

    # For tracking operation results
    result_resource: Optional[DynamoDbResource] = None
    transact_item: Optional[Dict[str, Any]] = None


@dataclass
class TransactionContext:
    """Context for accumulating transaction operations."""

    memory: DynamoDbMemory
    operations: List[TransactionOperation] = field(default_factory=list)
    read_cache: Dict[str, DynamoDbResource] = field(default_factory=dict)
    pending_creates: Dict[str, DynamoDbResource] = field(default_factory=dict)
    isolation_level: str = "read_committed"
    auto_retry: bool = True
    max_retries: int = 3

    # Track resources by type for validation
    resources_by_type: Dict[Type, List[TransactionOperation]] = field(default_factory=lambda: defaultdict(list))

    def create(
        self, resource: DynamoDbResource, condition: Optional[str] = None, **condition_values
    ) -> DynamoDbResource:
        """Queue a create operation."""
        if not resource.resource_id:
            resource.resource_id = str(ULID())

        # Store in pending creates for reference within transaction
        cache_key = f"{resource.__class__.__name__}#{resource.resource_id}"
        self.pending_creates[cache_key] = resource

        op = TransactionOperation(
            operation_type=OperationType.CREATE,
            resource_class=resource.__class__,
            resource=resource,
            condition=condition,
            condition_values=condition_values or None,
        )

        self.operations.append(op)
        self.resources_by_type[resource.__class__].append(op)

        # Return the resource so it can be referenced
        op.result_resource = resource
        return resource

    def update(
        self,
        resource: Union[DynamoDbResource, Type[DynamoDbResource]],
        resource_id: Optional[str] = None,
        updates: Optional[Dict[str, Any]] = None,
        condition: Optional[str] = None,
        condition_values: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> TransactionOperation:
        """Queue an update operation."""
        if isinstance(resource, type):
            # Updating by class and ID
            if not resource_id:
                raise ValueError("resource_id required when updating by class")
            resource_class = resource
            resource_obj = None
        else:
            # Updating an instance
            resource_class = resource.__class__
            resource_obj = resource
            resource_id = resource.resource_id

        # Merge updates dict with kwargs
        all_updates = {}
        if updates:
            all_updates.update(updates)
        all_updates.update(kwargs)

        op = TransactionOperation(
            operation_type=OperationType.UPDATE,
            resource_class=resource_class,
            resource=resource_obj,
            resource_id=resource_id,
            updates=all_updates,
            condition=condition,
            condition_values=condition_values,
        )

        self.operations.append(op)
        self.resources_by_type[resource_class].append(op)
        return op

    def delete(
        self,
        resource: Union[DynamoDbResource, Type[DynamoDbResource]],
        resource_id: Optional[str] = None,
        condition: Optional[str] = None,
        **condition_values,
    ) -> TransactionOperation:
        """Queue a delete operation."""
        if isinstance(resource, type):
            if not resource_id:
                raise ValueError("resource_id required when deleting by class")
            resource_class = resource
            resource_obj = None
        else:
            resource_class = resource.__class__
            resource_obj = resource
            resource_id = resource.resource_id

        op = TransactionOperation(
            operation_type=OperationType.DELETE,
            resource_class=resource_class,
            resource=resource_obj,
            resource_id=resource_id,
            condition=condition,
            condition_values=condition_values or None,
        )

        self.operations.append(op)
        self.resources_by_type[resource_class].append(op)
        return op

    def increment(
        self,
        resource: Union[DynamoDbResource, Type[DynamoDbResource]],
        field_name: str,
        amount: int = 1,
        resource_id: Optional[str] = None,
    ) -> TransactionOperation:
        """Queue an increment operation."""
        if isinstance(resource, type):
            if not resource_id:
                raise ValueError("resource_id required when incrementing by class")
            resource_class = resource
            resource_obj = None
        else:
            resource_class = resource.__class__
            resource_obj = resource
            resource_id = resource.resource_id

        op = TransactionOperation(
            operation_type=OperationType.INCREMENT,
            resource_class=resource_class,
            resource=resource_obj,
            resource_id=resource_id,
            field_name=field_name,
            value=amount,
        )

        self.operations.append(op)
        self.resources_by_type[resource_class].append(op)
        return op

    def append(
        self,
        resource: Union[DynamoDbResource, Type[DynamoDbResource]],
        field_name: str,
        values: List[Any],
        resource_id: Optional[str] = None,
    ) -> TransactionOperation:
        """Queue an append operation for list fields."""
        if isinstance(resource, type):
            if not resource_id:
                raise ValueError("resource_id required when appending by class")
            resource_class = resource
            resource_obj = None
        else:
            resource_class = resource.__class__
            resource_obj = resource
            resource_id = resource.resource_id

        op = TransactionOperation(
            operation_type=OperationType.APPEND,
            resource_class=resource_class,
            resource=resource_obj,
            resource_id=resource_id,
            field_name=field_name,
            value=values,
        )

        self.operations.append(op)
        self.resources_by_type[resource_class].append(op)
        return op

    def read(
        self, resource_class: Type[DynamoDbResource], resource_id: str, force_refresh: bool = False
    ) -> Optional[DynamoDbResource]:
        """Read a resource, using cache if available."""
        cache_key = f"{resource_class.__name__}#{resource_id}"

        # Check pending creates first
        if cache_key in self.pending_creates:
            return self.pending_creates[cache_key]

        # Check read cache if using snapshot isolation
        if self.isolation_level == "snapshot" and not force_refresh:
            if cache_key in self.read_cache:
                return self.read_cache[cache_key]

        # Read from database
        try:
            resource = self.memory.get_existing(resource_id, resource_class)
            if self.isolation_level == "snapshot":
                self.read_cache[cache_key] = resource
            return resource
        except (ValueError, AttributeError):
            return None

    def _build_transaction_items(self) -> List[Dict[str, Any]]:
        """Build DynamoDB transaction items from queued operations."""
        items = []

        for op in self.operations:
            if op.operation_type == OperationType.CREATE:
                items.extend(self._build_create_items(op))
            elif op.operation_type == OperationType.UPDATE:
                items.extend(self._build_update_items(op))
            elif op.operation_type == OperationType.DELETE:
                items.extend(self._build_delete_items(op))
            elif op.operation_type == OperationType.INCREMENT:
                items.extend(self._build_increment_items(op))
            elif op.operation_type == OperationType.APPEND:
                items.extend(self._build_append_items(op))

        # Store transaction items for debugging
        for op, item in zip(self.operations, items):
            op.transact_item = item

        return items

    def _build_create_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for a create operation."""
        resource = op.resource

        # Handle versioned resources (needs 2 items)
        if isinstance(resource, DynamoDbVersionedResource):
            main_item = resource.to_dynamodb_item(v0_object=False)
            v0_item = resource.to_dynamodb_item(v0_object=True)

            return [
                {
                    "Put": {
                        "TableName": self.memory.table_name,
                        "Item": marshall(main_item),
                        "ConditionExpression": "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                    }
                },
                {
                    "Put": {
                        "TableName": self.memory.table_name,
                        "Item": marshall(v0_item),
                        "ConditionExpression": "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                    }
                },
            ]
        else:
            # Non-versioned resource
            item = resource.to_dynamodb_item()
            condition = op.condition or "attribute_not_exists(pk) AND attribute_not_exists(sk)"

            put_item = {
                "Put": {"TableName": self.memory.table_name, "Item": marshall(item), "ConditionExpression": condition}
            }

            if op.condition_values:
                put_item["Put"]["ExpressionAttributeValues"] = marshall(op.condition_values)

            return [put_item]

    def _build_update_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for an update operation."""
        # For versioned resources, we need special handling
        if issubclass(op.resource_class, DynamoDbVersionedResource):
            # This is complex - versioned updates need to read current version
            # and create new version items
            return self._build_versioned_update_items(op)

        # Build update expression
        update_parts = []
        expression_values = {}
        expression_names = {}

        for key, value in op.updates.items():
            safe_key = key.replace(".", "_")
            update_parts.append(f"#{safe_key} = :{safe_key}")
            expression_values[f":{safe_key}"] = value
            expression_names[f"#{safe_key}"] = key

        update_expr = "SET " + ", ".join(update_parts)

        # Add updated_at
        from datetime import datetime, timezone

        update_expr += ", #updated_at = :updated_at"
        expression_names["#updated_at"] = "updated_at"
        expression_values[":updated_at"] = datetime.now(timezone.utc).isoformat()

        pk = f"{op.resource_class.get_unique_key_prefix()}#{op.resource_id}"
        # For non-versioned resources, sk is the same as pk
        sk = pk if not issubclass(op.resource_class, DynamoDbVersionedResource) else "0"

        update_item = {
            "Update": {
                "TableName": self.memory.table_name,
                "Key": marshall({"pk": pk, "sk": sk}),
                "UpdateExpression": update_expr,
                "ExpressionAttributeNames": expression_names,
                "ExpressionAttributeValues": marshall(expression_values),
            }
        }

        # Add condition if specified
        if op.condition:
            update_item["Update"]["ConditionExpression"] = op.condition
            if op.condition_values:
                # Merge condition values into expression attribute values
                for k, v in marshall(op.condition_values).items():
                    update_item["Update"]["ExpressionAttributeValues"][k] = v

        return [update_item]

    def _build_versioned_update_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for updating a versioned resource."""
        # This requires reading the current resource first
        current: DynamoDbVersionedResource = self.memory.get_existing(op.resource_id, op.resource_class)
        if not current:
            raise ResourceNotFoundError(f"Resource {op.resource_id} not found")

        # Apply updates to create new version
        for key, value in op.updates.items():
            setattr(current, key, value)

        # Increment version
        from datetime import datetime, timezone

        current.version += 1
        current.updated_at = datetime.now(timezone.utc)

        # Create items for new version
        main_item = current.to_dynamodb_item(v0_object=False)
        v0_item = current.to_dynamodb_item(v0_object=True)

        return [
            {
                "Put": {
                    "TableName": self.memory.table_name,
                    "Item": marshall(main_item),
                    "ConditionExpression": "attribute_not_exists(pk) AND attribute_not_exists(sk)",
                }
            },
            {
                "Put": {
                    "TableName": self.memory.table_name,
                    "Item": marshall(v0_item),
                    "ConditionExpression": "attribute_exists(pk) AND attribute_exists(sk) AND #version = :version",
                    "ExpressionAttributeNames": {"#version": "version"},
                    "ExpressionAttributeValues": marshall({":version": current.version - 1}),
                }
            },
        ]

    def _build_delete_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for a delete operation."""
        pk = f"{op.resource_class.get_unique_key_prefix()}#{op.resource_id}"

        # For non-versioned resources, sk is the same as pk
        sk = pk if not issubclass(op.resource_class, DynamoDbVersionedResource) else "0"

        delete_item = {"Delete": {"TableName": self.memory.table_name, "Key": marshall({"pk": pk, "sk": sk})}}

        if op.condition:
            delete_item["Delete"]["ConditionExpression"] = op.condition
            if op.condition_values:
                delete_item["Delete"]["ExpressionAttributeValues"] = marshall(op.condition_values)

        return [delete_item]

    def _build_increment_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for an increment operation."""
        pk = f"{op.resource_class.get_unique_key_prefix()}#{op.resource_id}"
        # For non-versioned resources, sk is the same as pk
        sk = pk if not issubclass(op.resource_class, DynamoDbVersionedResource) else "0"

        update_item = {
            "Update": {
                "TableName": self.memory.table_name,
                "Key": marshall({"pk": pk, "sk": sk}),
                "UpdateExpression": f"ADD #{op.field_name} :inc",
                "ExpressionAttributeNames": {f"#{op.field_name}": op.field_name},
                "ExpressionAttributeValues": marshall({":inc": op.value}),
            }
        }

        return [update_item]

    def _build_append_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for an append operation."""
        pk = f"{op.resource_class.get_unique_key_prefix()}#{op.resource_id}"
        # For non-versioned resources, sk is the same as pk
        sk = pk if not issubclass(op.resource_class, DynamoDbVersionedResource) else "0"

        update_item = {
            "Update": {
                "TableName": self.memory.table_name,
                "Key": marshall({"pk": pk, "sk": sk}),
                "UpdateExpression": f"SET #{op.field_name} = list_append(if_not_exists(#{op.field_name}, :empty_list), :val)",
                "ExpressionAttributeNames": {f"#{op.field_name}": op.field_name},
                "ExpressionAttributeValues": marshall({":val": op.value, ":empty_list": []}),
            }
        }

        return [update_item]

    def commit(self):
        """Execute all queued operations as a transaction."""
        if not self.operations:
            return  # Nothing to commit

        items = self._build_transaction_items()

        # Check transaction size limits
        if len(items) > 100:
            raise TransactionError(f"Transaction has {len(items)} items, exceeds DynamoDB limit of 100")

        # Execute transaction with retry logic
        retries = 0
        while retries <= self.max_retries:
            try:
                response = self.memory.dynamodb_client.transact_write_items(TransactItems=items)

                # Clear operations after successful commit
                self.operations.clear()
                self.resources_by_type.clear()
                self.pending_creates.clear()

                return response

            except ClientError as e:
                error_code = e.response["Error"]["Code"]

                if error_code == "TransactionCanceledException":
                    # Check for version conflicts
                    reasons = e.response.get("CancellationReasons", [])
                    for reason in reasons:
                        if reason.get("Code") == "ConditionalCheckFailed":
                            if self.auto_retry and retries < self.max_retries:
                                retries += 1
                                logger.warning(f"Transaction failed, retrying ({retries}/{self.max_retries})")
                                continue
                            raise VersionConflictError("Transaction failed due to version conflict")

                    raise TransactionError(f"Transaction cancelled: {reasons}")

                elif error_code == "ValidationException":
                    raise TransactionError(f"Transaction validation failed: {e}")

                else:
                    raise TransactionError(f"Transaction failed: {e}")

        raise TransactionError(f"Transaction failed after {self.max_retries} retries")

    def rollback(self):
        """Clear all queued operations without executing."""
        self.operations.clear()
        self.resources_by_type.clear()
        self.pending_creates.clear()
        self.read_cache.clear()


class TransactionManager:
    """Manages transactions for DynamoDbMemory."""

    def __init__(self, memory: DynamoDbMemory):
        self.memory = memory

    @contextmanager
    def transaction(self, isolation_level: str = "read_committed", auto_retry: bool = True, max_retries: int = 3):
        """Create a transaction context."""
        context = TransactionContext(
            memory=self.memory, isolation_level=isolation_level, auto_retry=auto_retry, max_retries=max_retries
        )

        try:
            yield context
            # Automatically commit on successful exit
            context.commit()
        except Exception:
            # Rollback on any exception
            context.rollback()
            raise
