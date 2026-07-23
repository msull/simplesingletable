"""Transaction support for DynamoDB single-table operations."""

from __future__ import annotations

import logging
import random
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Type, Union

from botocore.exceptions import ClientError

from .models import DynamoDbResource, DynamoDbVersionedResource, clean_data
from .utils import generate_date_sortable_id, marshall

if TYPE_CHECKING:
    from .dynamodb_memory import DynamoDbMemory

logger = logging.getLogger(__name__)

# Cancellation-reason codes (within TransactionCanceledException) that are transient:
# the write lost a race or hit capacity limits, and an identical resend can succeed.
_TRANSIENT_CANCELLATION_CODES = {
    "TransactionConflict",
    "ThrottlingError",
    "ProvisionedThroughputExceeded",
}

# Top-level ClientError codes that are likewise transient for transact_write_items.
_TRANSIENT_ERROR_CODES = {
    "TransactionInProgressException",
    "ThrottlingException",
    "ProvisionedThroughputExceededException",
    "RequestLimitExceeded",
    "InternalServerError",
}


def _marshall_values(values: Dict[str, Any]) -> Dict[str, Any]:
    """Marshall expression values with the same normalization as item writes.

    Routes values through ``clean_data`` (float→Decimal, date/datetime→isoformat,
    recursively) before boto3's ``TypeSerializer``, matching what
    ``to_dynamodb_item()`` does on the non-transactional write path.
    """
    cleaned = clean_data(values)
    # clean_data silently drops empty sets, which is fine for a full item put but
    # would leave a dangling placeholder in an update/condition expression.
    missing = set(values) - set(cleaned)
    if missing:
        raise ValueError(f"Empty set value(s) not supported in transaction expressions: {sorted(missing)}")
    return marshall(cleaned)


class TransactionError(Exception):
    """Raised when a transaction fails.

    Attributes:
        cancellation_reasons: The raw DynamoDB ``CancellationReasons`` payload, when the
            failure originated from a ``TransactionCanceledException``. Empty otherwise.
        operation_indexes: Indexes (into ``TransactionContext.operations``) of the
            specific operations whose conditions/conflicts caused the cancellation.
    """

    def __init__(
        self,
        message: str,
        *,
        cancellation_reasons: Optional[List[Dict[str, Any]]] = None,
        operation_indexes: Optional[List[int]] = None,
    ):
        super().__init__(message)
        self.cancellation_reasons = cancellation_reasons or []
        self.operation_indexes = operation_indexes or []


class TransactionConditionFailedError(TransactionError):
    """Raised when a transaction is cancelled because one or more conditions did not hold.

    This is the canonical exception for both version-token collisions (implicit
    conditions set by the library) and user-supplied ``condition=`` checks. The two
    cases can be distinguished by inspecting the ``condition`` field of each operation
    referenced by ``operation_indexes``.
    """


class VersionConflictError(TransactionConditionFailedError):
    """Back-compat alias raised for any condition-check failure inside a transaction.

    New code should catch :class:`TransactionConditionFailedError` (or the parent
    :class:`TransactionError`). This subclass exists so that pre-existing
    ``except VersionConflictError`` blocks continue to behave as before.
    """


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
    PUT = "put"


@dataclass
class TransactionOperation:
    """Represents a single operation within a transaction."""

    operation_type: OperationType
    resource_class: Type[DynamoDbResource]
    resource: Optional[DynamoDbResource] = None
    resource_id: Optional[str] = None
    updates: Optional[Dict[str, Any]] = None
    clear_fields: Optional[List[str]] = None
    field_name: Optional[str] = None
    value: Optional[Any] = None
    condition: Optional[str] = None
    condition_values: Optional[Dict[str, Any]] = None
    recompute_gsis: bool = False
    current: Optional[DynamoDbResource] = None

    # Tracking populated by the build phase, used by post-commit hooks for audit/stats.
    result_resource: Optional[DynamoDbResource] = None
    pre_image: Optional[DynamoDbResource] = None
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

    # Transaction-wide audit attribution. Applied to every audit row emitted by this
    # commit() unless the resource's audit_config provides a more specific override.
    changed_by: Optional[str] = None
    audit_metadata: Optional[Dict[str, Any]] = None

    # Track resources by type for validation
    resources_by_type: Dict[Type, List[TransactionOperation]] = field(default_factory=lambda: defaultdict(list))

    def create(
        self, resource: DynamoDbResource, condition: Optional[str] = None, **condition_values
    ) -> DynamoDbResource:
        """Queue a create operation."""
        if not resource.resource_id:
            resource.resource_id = generate_date_sortable_id()

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
        clear_fields: Optional[Union[List[str], Set[str]]] = None,
        recompute_gsis: bool = False,
        current: Optional[DynamoDbResource] = None,
        **kwargs,
    ) -> TransactionOperation:
        """Queue an update operation.

        Args:
            resource: The resource instance or class to update.
            resource_id: Required when ``resource`` is a class.
            updates: Mapping of field-name to new value.
            condition: Optional DynamoDB condition expression string.
            condition_values: Values for the condition expression.
            clear_fields: Field names to REMOVE from the item (sets the underlying
                model field to ``None`` for purposes of GSI recomputation). Emitted
                as a ``REMOVE`` clause in the same update expression.
            recompute_gsis: When True, the builder reads the current state, applies
                ``updates`` and ``clear_fields`` in memory, re-runs the resource's
                ``get_gsi_config``, and folds the resulting GSI key SETs/REMOVEs into
                the update expression. Costs one read per op unless ``current=`` is
                supplied. Required when an update changes a field that participates in
                a GSI key — without this flag, the GSI key attribute on the item is
                left stale and the index points at the old value.
            current: Pre-loaded state of the resource. Skips the internal
                ``get_existing`` read used by versioned updates and by
                ``recompute_gsis=True``. Also used as the pre-image for audit log
                ``changed_fields`` / ``include_snapshot`` if audit is enabled.
        """
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

        clear_fields_list: Optional[List[str]] = None
        if clear_fields:
            clear_fields_list = list(clear_fields)
            overlap = set(clear_fields_list) & set(all_updates.keys())
            if overlap:
                raise ValueError(f"clear_fields and updates cannot reference the same field(s): {sorted(overlap)}")

        op = TransactionOperation(
            operation_type=OperationType.UPDATE,
            resource_class=resource_class,
            resource=resource_obj,
            resource_id=resource_id,
            updates=all_updates,
            clear_fields=clear_fields_list,
            condition=condition,
            condition_values=condition_values,
            recompute_gsis=recompute_gsis,
            current=current,
        )

        self.operations.append(op)
        self.resources_by_type[resource_class].append(op)
        return op

    def put(
        self,
        resource: DynamoDbResource,
        condition: Optional[str] = None,
        condition_values: Optional[Dict[str, Any]] = None,
    ) -> TransactionOperation:
        """Queue a full-state PUT operation.

        Unlike :meth:`update`, which generates a partial ``SET ... REMOVE ...``
        expression, ``put`` writes the entire current state of the resource via
        ``to_dynamodb_item()``. This naturally:

        - Recomputes every GSI key (via ``_apply_gsi_configuration``).
        - Removes attributes that were nulled out (when paired with
          ``ResourceConfig(omit_none_attributes=True)``).
        - Updates the ``updated_at`` timestamp if the caller already bumped it.

        ``put`` does NOT exist for versioned resources — those require explicit
        version-incrementing semantics; use :meth:`update` for those.

        The default condition is ``attribute_exists(pk)`` (i.e., the resource must
        already exist; for fresh inserts use :meth:`create`). Override via
        ``condition``/``condition_values``.
        """
        if isinstance(resource, DynamoDbVersionedResource):
            raise ValueError(
                "txn.put() is only supported for non-versioned resources. "
                "Use txn.update() for versioned resources so version-token semantics are preserved."
            )
        if not isinstance(resource, DynamoDbResource):
            raise TypeError("txn.put() requires a DynamoDbResource instance")
        if not resource.resource_id:
            raise ValueError("Resource must have a resource_id before put()")

        op = TransactionOperation(
            operation_type=OperationType.PUT,
            resource_class=resource.__class__,
            resource=resource,
            resource_id=resource.resource_id,
            condition=condition,
            condition_values=condition_values,
        )

        self.operations.append(op)
        self.resources_by_type[resource.__class__].append(op)
        op.result_resource = resource
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

    def _build_transaction_items(self) -> tuple[List[Dict[str, Any]], List[int]]:
        """Build DynamoDB transaction items from queued operations.

        Returns:
            (items, item_to_op_index) where ``item_to_op_index[i]`` is the index of
            the operation in ``self.operations`` that produced ``items[i]``. Some ops
            produce more than one transaction item (notably versioned CREATE/UPDATE),
            so this mapping is needed to resolve DynamoDB CancellationReasons back to
            the originating operations.
        """
        items: List[Dict[str, Any]] = []
        item_to_op_index: List[int] = []

        for op_index, op in enumerate(self.operations):
            if op.operation_type == OperationType.CREATE:
                op_items = self._build_create_items(op)
            elif op.operation_type == OperationType.UPDATE:
                op_items = self._build_update_items(op)
            elif op.operation_type == OperationType.DELETE:
                op_items = self._build_delete_items(op)
            elif op.operation_type == OperationType.INCREMENT:
                op_items = self._build_increment_items(op)
            elif op.operation_type == OperationType.APPEND:
                op_items = self._build_append_items(op)
            elif op.operation_type == OperationType.PUT:
                op_items = self._build_put_items(op)
            else:
                op_items = []

            # Track which op each transaction item came from.
            items.extend(op_items)
            item_to_op_index.extend([op_index] * len(op_items))

            # Store a reference to the (first) transaction item for debugging.
            if op_items:
                op.transact_item = op_items[0]

        return items, item_to_op_index

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
                put_item["Put"]["ExpressionAttributeValues"] = _marshall_values(op.condition_values)

            return [put_item]

    def _build_update_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for an update operation."""
        # For versioned resources, we need special handling
        if issubclass(op.resource_class, DynamoDbVersionedResource):
            # This is complex - versioned updates need to read current version
            # and create new version items
            return self._build_versioned_update_items(op)

        from datetime import datetime, timezone

        set_parts: List[str] = []
        remove_parts: List[str] = []
        expression_values: Dict[str, Any] = {}
        expression_names: Dict[str, str] = {}

        # Resolve current state (only needed for GSI recompute on non-versioned).
        current = op.current
        if op.recompute_gsis and current is None:
            current = self._load_current_for_op(op)

        # If we have current state, compute the post-update GSI keys.
        gsi_set: Dict[str, Any] = {}
        gsi_remove: Set[str] = set()
        if op.recompute_gsis and current is not None:
            gsi_set, gsi_remove = self._compute_gsi_changes(op, current)
            # Stash so the audit pre-image walk can use the same state.
            op.current = current

        # User-supplied SET updates.
        for key, value in (op.updates or {}).items():
            placeholder = self._allocate_placeholder(key, expression_names)
            set_parts.append(f"{placeholder} = :{placeholder[1:]}")
            expression_values[f":{placeholder[1:]}"] = value

        # GSI key SETs from recompute (may collide with user-supplied keys; user wins).
        for key, value in gsi_set.items():
            if key in (op.updates or {}):
                continue
            placeholder = self._allocate_placeholder(key, expression_names)
            set_parts.append(f"{placeholder} = :{placeholder[1:]}")
            expression_values[f":{placeholder[1:]}"] = value

        # Always bump updated_at.
        expression_names["#updated_at"] = "updated_at"
        expression_values[":updated_at"] = datetime.now(timezone.utc).isoformat()
        set_parts.append("#updated_at = :updated_at")

        # REMOVE clauses for clear_fields + nulled GSI keys.
        clear_fields = list(op.clear_fields or [])
        for key in clear_fields:
            placeholder = self._allocate_placeholder(key, expression_names)
            remove_parts.append(placeholder)
        for key in sorted(gsi_remove):
            if key in (op.updates or {}) or key in gsi_set:
                continue
            placeholder = self._allocate_placeholder(key, expression_names)
            if placeholder not in remove_parts:
                remove_parts.append(placeholder)

        update_expr = "SET " + ", ".join(set_parts)
        if remove_parts:
            update_expr += " REMOVE " + ", ".join(remove_parts)

        pk = f"{op.resource_class.get_unique_key_prefix()}#{op.resource_id}"
        # For non-versioned resources, sk is the same as pk
        sk = pk

        update_item = {
            "Update": {
                "TableName": self.memory.table_name,
                "Key": marshall({"pk": pk, "sk": sk}),
                "UpdateExpression": update_expr,
                "ExpressionAttributeNames": expression_names,
                "ExpressionAttributeValues": _marshall_values(expression_values),
            }
        }

        # Add condition if specified
        if op.condition:
            update_item["Update"]["ConditionExpression"] = op.condition
            if op.condition_values:
                # Merge condition values into expression attribute values
                for k, v in _marshall_values(op.condition_values).items():
                    update_item["Update"]["ExpressionAttributeValues"][k] = v

        return [update_item]

    @staticmethod
    def _allocate_placeholder(key: str, expression_names: Dict[str, str]) -> str:
        """Allocate a unique ``#name`` placeholder for ``key`` in ``expression_names``.

        Re-uses an existing placeholder if one already maps to ``key``.
        """
        for placeholder, name in expression_names.items():
            if name == key:
                return placeholder
        safe_key = key.replace(".", "_")
        placeholder = f"#{safe_key}"
        suffix = 1
        while placeholder in expression_names:
            placeholder = f"#{safe_key}_{suffix}"
            suffix += 1
        expression_names[placeholder] = key
        return placeholder

    def _load_current_for_op(self, op: TransactionOperation) -> Optional[DynamoDbResource]:
        """Return the current resource state for ``op``, using the read cache when possible."""
        cache_key = f"{op.resource_class.__name__}#{op.resource_id}"
        if cache_key in self.read_cache:
            return self.read_cache[cache_key]
        try:
            current = self.memory.get_existing(op.resource_id, op.resource_class)
        except (ValueError, AttributeError):
            return None
        if current is not None:
            self.read_cache[cache_key] = current
        return current

    @staticmethod
    def _compute_gsi_changes(op: TransactionOperation, current: DynamoDbResource) -> tuple[Dict[str, Any], Set[str]]:
        """Apply ``op.updates`` / ``op.clear_fields`` to ``current`` and recompute GSI keys.

        Returns ``(keys_to_set, keys_to_remove)`` where:
        - keys_to_set maps GSI attribute name to its new value.
        - keys_to_remove is a set of GSI attribute names whose post-update value is None
          and therefore should be REMOVEd from the item.
        """
        projected = current.model_copy(deep=True)
        for key, value in (op.updates or {}).items():
            if hasattr(projected, key):
                setattr(projected, key, value)
        for key in op.clear_fields or []:
            if hasattr(projected, key):
                setattr(projected, key, None)

        keys_to_set: Dict[str, Any] = {}
        keys_to_remove: Set[str] = set()

        gsi_config = projected.get_gsi_config()
        for fields in gsi_config.values():
            for key, value_or_func in fields.items():
                # Tuple keys (combined pk/sk)
                if isinstance(key, tuple):
                    if callable(value_or_func):
                        result = value_or_func(projected)
                        if result and len(key) == 2 and len(result) == 2:
                            pk_val, sk_val = result
                            if pk_val is None:
                                keys_to_remove.add(key[0])
                            else:
                                keys_to_set[key[0]] = pk_val
                            if sk_val is None:
                                keys_to_remove.add(key[1])
                            else:
                                keys_to_set[key[1]] = sk_val
                        else:
                            # Lambda returned falsy → REMOVE both keys.
                            keys_to_remove.add(key[0])
                            keys_to_remove.add(key[1])
                    continue

                if callable(value_or_func):
                    value = value_or_func(projected)
                elif value_or_func is not None:
                    value = value_or_func
                else:
                    value = None

                if value is None or value == "" or value is False:
                    keys_to_remove.add(key)
                else:
                    keys_to_set[key] = value

        # Legacy GSI methods (db_get_gsi1pk etc.) also need recomputation.
        legacy_methods = {
            "gsi1pk": projected.db_get_gsi1pk,
            "gsi2pk": projected.db_get_gsi2pk,
        }
        for key, method in legacy_methods.items():
            value = method()
            if value is None:
                if key not in keys_to_set:
                    keys_to_remove.add(key)
            else:
                keys_to_set[key] = value

        gsi3_data = projected.db_get_gsi3pk_and_sk()
        if gsi3_data:
            gsi3pk, gsi3sk = gsi3_data
            if gsi3pk is None:
                keys_to_remove.add("gsi3pk")
            else:
                keys_to_set["gsi3pk"] = gsi3pk
            if gsi3sk is None:
                keys_to_remove.add("gsi3sk")
            else:
                keys_to_set["gsi3sk"] = gsi3sk
        else:
            # Only mark for removal if no GSI config lambda produced them.
            if "gsi3pk" not in keys_to_set:
                keys_to_remove.add("gsi3pk")
            if "gsi3sk" not in keys_to_set:
                keys_to_remove.add("gsi3sk")

        # A key cannot be both SET and REMOVE.
        keys_to_remove -= set(keys_to_set.keys())
        return keys_to_set, keys_to_remove

    def _build_put_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for a full-state PUT."""
        from datetime import datetime, timezone

        resource = op.resource
        # Always bump updated_at so callers don't have to remember.
        resource.updated_at = datetime.now(timezone.utc)

        item = resource.to_dynamodb_item()
        condition = op.condition or "attribute_exists(pk) AND attribute_exists(sk)"

        put_item = {
            "Put": {"TableName": self.memory.table_name, "Item": marshall(item), "ConditionExpression": condition}
        }
        if op.condition_values:
            put_item["Put"]["ExpressionAttributeValues"] = _marshall_values(op.condition_values)

        op.result_resource = resource
        return [put_item]

    def _build_versioned_update_items(self, op: TransactionOperation) -> List[Dict[str, Any]]:
        """Build transaction items for updating a versioned resource."""
        # Reuse caller-supplied pre-image when available to avoid an extra read.
        if op.current is not None:
            current_pre = op.current.model_copy(deep=True)
        else:
            cache_key = f"{op.resource_class.__name__}#{op.resource_id}"
            if cache_key in self.read_cache:
                current_pre = self.read_cache[cache_key].model_copy(deep=True)
            else:
                current_pre = self.memory.get_existing(op.resource_id, op.resource_class)

        if not current_pre:
            raise ResourceNotFoundError(f"Resource {op.resource_id} not found")

        # Stash the unmodified pre-image so the audit walk can compute changed_fields.
        op.pre_image = current_pre.model_copy(deep=True)

        # Apply updates to produce the next version in memory.
        current = current_pre
        for key, value in (op.updates or {}).items():
            setattr(current, key, value)
        for key in op.clear_fields or []:
            if hasattr(current, key):
                setattr(current, key, None)

        # Increment version
        from datetime import datetime, timezone

        current.version += 1
        current.updated_at = datetime.now(timezone.utc)

        # Stash the new state for post-commit hooks (audit emission, stats).
        op.result_resource = current

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
                delete_item["Delete"]["ExpressionAttributeValues"] = _marshall_values(op.condition_values)

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
                "ExpressionAttributeValues": _marshall_values({":inc": op.value}),
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
                "ExpressionAttributeValues": _marshall_values({":val": op.value, ":empty_list": []}),
            }
        }

        return [update_item]

    def commit(self):
        """Execute all queued operations as a transaction.

        Failure handling:
        - A ``TransactionCanceledException`` with one or more ``ConditionalCheckFailed``
          reasons is raised as :class:`VersionConflictError` (subclass of
          :class:`TransactionConditionFailedError`). The exception carries the raw
          ``cancellation_reasons`` and a list of ``operation_indexes`` identifying which
          queued operations triggered the failure.
        - Any other transaction cancellation, validation error, or unexpected
          ``botocore.exceptions.ClientError`` is raised as :class:`TransactionError`.
          A raw ``ClientError`` is never re-raised from this method.

        Retry policy (when ``auto_retry=True``):
        - If every failed item came from an operation with no user-supplied
          ``condition=`` (i.e., only library-implicit conditions failed — typically a
          version-token collision on a versioned UPDATE), the entire transaction is
          rebuilt and retried up to ``max_retries`` times.
        - If any failed item came from an operation that carried a user-supplied
          ``condition=``, the failure is raised immediately. User-supplied conditions
          encode semantic intent (e.g., "this slot must be empty"); retrying them
          three times is pure latency for what the caller wants to be a fast 409.
        - Transient failures — a cancellation whose reasons are all
          ``TransactionConflict``/throttling, or a top-level throttling /
          ``TransactionInProgressException`` error — are retried up to
          ``max_retries`` times with full-jitter exponential backoff.
        """
        if not self.operations:
            return  # Nothing to commit

        # Check transaction size limits before retry loop (rebuilds reuse the same ops).
        retries = 0
        while True:
            items, item_to_op_index = self._build_transaction_items()

            if len(items) > 100:
                raise TransactionError(f"Transaction has {len(items)} items, exceeds DynamoDB limit of 100")

            try:
                response = self.memory.dynamodb_client.transact_write_items(TransactItems=items)

                # Post-commit hooks: audit emission + stats counter updates.
                # Done before clearing so we can still walk the operations list.
                try:
                    self._run_post_commit_hooks()
                except Exception:  # noqa: BLE001
                    # Post-commit side effects must never mask a successful commit.
                    logger.exception("Post-commit hooks failed; primary transaction already committed")

                # Clear operations after successful commit
                self.operations.clear()
                self.resources_by_type.clear()
                self.pending_creates.clear()

                return response

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")

                if error_code == "TransactionCanceledException":
                    reasons = e.response.get("CancellationReasons", []) or []
                    failed_op_indexes = self._resolve_failed_op_indexes(reasons, item_to_op_index)

                    has_condition_failure = any(r.get("Code") == "ConditionalCheckFailed" for r in reasons)

                    if has_condition_failure:
                        if self._should_retry(reasons, failed_op_indexes) and retries < self.max_retries:
                            retries += 1
                            self._invalidate_cached_state(failed_op_indexes)
                            logger.warning(
                                f"Transaction failed on implicit condition, retrying " f"({retries}/{self.max_retries})"
                            )
                            continue
                        raise VersionConflictError(
                            f"Transaction failed: condition check did not hold for "
                            f"operation index(es) {failed_op_indexes}",
                            cancellation_reasons=reasons,
                            operation_indexes=failed_op_indexes,
                        ) from e

                    # No ConditionalCheckFailed reason. If every real reason is
                    # transient (write-write conflict, throttling), an identical
                    # resend can succeed — retry with backoff.
                    real_reasons = [r.get("Code") for r in reasons if r.get("Code") not in (None, "None")]
                    all_transient = bool(real_reasons) and all(
                        code in _TRANSIENT_CANCELLATION_CODES for code in real_reasons
                    )
                    if all_transient and self.auto_retry and retries < self.max_retries:
                        retries += 1
                        logger.warning(
                            f"Transaction cancelled for transient reason(s) {sorted(set(real_reasons))}, "
                            f"retrying ({retries}/{self.max_retries})"
                        )
                        self._sleep_backoff(retries)
                        continue

                    # Non-transient cancellation (or retries exhausted).
                    raise TransactionError(
                        f"Transaction cancelled: {reasons}",
                        cancellation_reasons=reasons,
                        operation_indexes=failed_op_indexes,
                    ) from e

                elif error_code in _TRANSIENT_ERROR_CODES:
                    if self.auto_retry and retries < self.max_retries:
                        retries += 1
                        logger.warning(
                            f"Transaction failed with transient error {error_code}, "
                            f"retrying ({retries}/{self.max_retries})"
                        )
                        self._sleep_backoff(retries)
                        continue
                    raise TransactionError(f"Transaction failed: {e}") from e

                elif error_code == "ConditionalCheckFailedException":
                    # Surfaces on some single-item transaction paths; normalize the same way.
                    raise VersionConflictError(
                        f"Transaction failed: condition check did not hold ({e})",
                        cancellation_reasons=[{"Code": "ConditionalCheckFailed"}],
                    ) from e

                elif error_code == "ValidationException":
                    raise TransactionError(f"Transaction validation failed: {e}") from e

                else:
                    raise TransactionError(f"Transaction failed: {e}") from e

    @staticmethod
    def _resolve_failed_op_indexes(reasons: List[Dict[str, Any]], item_to_op_index: List[int]) -> List[int]:
        """Map DynamoDB ``CancellationReasons`` (by item index) back to op indexes.

        Preserves order and de-duplicates while leaving the first occurrence first.
        """
        seen: set = set()
        ordered: List[int] = []
        for item_index, reason in enumerate(reasons):
            if reason.get("Code") in (None, "None"):
                continue  # Item was fine; only "real" reasons matter.
            if item_index < len(item_to_op_index):
                op_index = item_to_op_index[item_index]
                if op_index not in seen:
                    seen.add(op_index)
                    ordered.append(op_index)
        return ordered

    @staticmethod
    def _sleep_backoff(attempt: int) -> None:
        """Full-jitter exponential backoff: sleep uniform(0, min(1s, 50ms * 2**attempt))."""
        time.sleep(random.uniform(0, min(1.0, 0.05 * (2**attempt))))

    def _invalidate_cached_state(self, failed_op_indexes: List[int]) -> None:
        """Drop cached pre-images for ops whose implicit conditions failed.

        A retry that rebuilds from ``op.current`` or the read cache re-derives the
        same stale version number and is guaranteed to fail again. Clearing these
        forces the rebuild to re-read fresh state from DynamoDB. If the failed ops
        cannot be resolved back to indexes, all cached state is dropped.
        """
        if failed_op_indexes:
            ops = [self.operations[i] for i in failed_op_indexes if i < len(self.operations)]
        else:
            ops = list(self.operations)
        for op in ops:
            op.current = None
            if op.resource_id:
                self.read_cache.pop(f"{op.resource_class.__name__}#{op.resource_id}", None)

    def _should_retry(self, reasons: List[Dict[str, Any]], failed_op_indexes: List[int]) -> bool:
        """Return True only if every failure came from a library-implicit condition.

        User-supplied conditions are not retried: if a caller wrote
        ``condition="attribute_not_exists(slot)"`` the failure is the semantically
        correct answer ("this slot is taken"), and retrying just adds latency.
        Implicit conditions (e.g. versioned UPDATE's version-token check) can resolve
        on retry because the transaction is rebuilt from scratch.
        """
        if not self.auto_retry:
            return False

        for op_index in failed_op_indexes:
            if op_index >= len(self.operations):
                return False
            op = self.operations[op_index]
            if op.condition:
                # User-supplied condition; do not retry this transaction.
                return False
        return True

    def _run_post_commit_hooks(self) -> None:
        """Emit audit logs and update MemoryStats for committed operations.

        Audit emission mirrors what ``DynamoDbMemory.create_new`` /
        ``update_existing`` / ``delete_existing`` do on the non-transactional path,
        so resources with ``audit_config.enabled=True`` produce the same audit feed
        regardless of whether mutations went through a transaction.

        Stats updates likewise mirror ``create_new`` / ``delete_existing``.
        """
        # Local import avoids cycle (dynamodb_memory imports from this module).
        from .dynamodb_memory import AuditEntry, MemoryStats

        audit_entries: List[AuditEntry] = []
        stats_deltas: Dict[str, int] = defaultdict(int)

        for op in self.operations:
            audit_enabled = bool((op.resource_class.resource_config.get("audit_config") or {}).get("enabled"))
            cache_key = f"{op.resource_class.__name__}#{op.resource_id}" if op.resource_id else None

            if op.operation_type == OperationType.CREATE:
                stats_deltas[op.resource_class.__name__] += 1
                if audit_enabled and op.resource is not None:
                    audit_entries.append(
                        AuditEntry(
                            operation="CREATE",
                            resource=op.resource,
                            changed_by=self.changed_by,
                            audit_metadata=self._audit_metadata_for(op),
                        )
                    )

            elif op.operation_type == OperationType.PUT:
                if audit_enabled and op.resource is not None:
                    old_resource = op.pre_image
                    if old_resource is None and cache_key in self.read_cache:
                        old_resource = self.read_cache[cache_key]
                    audit_entries.append(
                        AuditEntry(
                            operation="UPDATE",
                            resource=op.resource,
                            changed_by=self.changed_by,
                            old_resource=old_resource,
                            audit_metadata=self._audit_metadata_for(op),
                        )
                    )

            elif op.operation_type == OperationType.UPDATE:
                if not audit_enabled:
                    continue
                new_state = op.result_resource
                if new_state is None:
                    # Non-versioned update path didn't pre-compute; re-read.
                    try:
                        new_state = self.memory.get_existing(op.resource_id, op.resource_class)
                    except (ValueError, AttributeError):
                        new_state = None
                if new_state is None:
                    continue
                old_resource = op.pre_image
                if old_resource is None and op.current is not None:
                    old_resource = op.current
                if old_resource is None and cache_key in self.read_cache:
                    old_resource = self.read_cache[cache_key]
                audit_entries.append(
                    AuditEntry(
                        operation="UPDATE",
                        resource=new_state,
                        changed_by=self.changed_by,
                        old_resource=old_resource,
                        audit_metadata=self._audit_metadata_for(op),
                    )
                )

            elif op.operation_type == OperationType.DELETE:
                if not issubclass(op.resource_class, DynamoDbVersionedResource):
                    stats_deltas[op.resource_class.__name__] -= 1
                if audit_enabled:
                    snapshot = op.resource
                    if snapshot is None and cache_key in self.read_cache:
                        snapshot = self.read_cache[cache_key]
                    if snapshot is not None:
                        audit_entries.append(
                            AuditEntry(
                                operation="DELETE",
                                resource=snapshot,
                                changed_by=self.changed_by,
                                audit_metadata=self._audit_metadata_for(op),
                            )
                        )

            # INCREMENT and APPEND are not audited automatically — the resulting state
            # is computed server-side and would require a post-commit read per op. If
            # callers need an audit trail for these, they can call emit_audit_log
            # explicitly with a fresh read.

        if audit_entries:
            self.memory.emit_audit_logs(audit_entries)

        if self.memory.track_stats and stats_deltas:
            stats = MemoryStats.ensure_exists(self.memory)
            for type_name, delta in stats_deltas.items():
                if delta == 0:
                    continue
                self.memory.increment_counter(stats, "counts_by_type." + type_name, delta)

    def _audit_metadata_for(self, op: TransactionOperation) -> Optional[Dict[str, Any]]:
        """Merge transaction-wide audit_metadata with per-op annotations."""
        base = dict(self.audit_metadata) if self.audit_metadata else {}
        # Tag every row from the same transaction so they can be grouped downstream.
        if "transaction_id" not in base:
            transaction_id = getattr(self, "_transaction_id", None)
            if transaction_id is None:
                transaction_id = generate_date_sortable_id()
                self._transaction_id = transaction_id
            base["transaction_id"] = transaction_id
        return base or None

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
    def transaction(
        self,
        isolation_level: str = "read_committed",
        auto_retry: bool = True,
        max_retries: int = 3,
        changed_by: Optional[str] = None,
        audit_metadata: Optional[Dict[str, Any]] = None,
    ):
        """Create a transaction context.

        Args:
            isolation_level: ``"read_committed"`` (default) or ``"snapshot"``.
            auto_retry: When True (default) the transaction is rebuilt and retried
                up to ``max_retries`` times if it failed on a *library-implicit*
                condition (typically a versioned-update version-token collision).
                Failures from user-supplied ``condition=`` checks are never retried —
                those encode semantic intent that retrying cannot resolve.
            max_retries: Maximum implicit-condition retries before raising
                :class:`VersionConflictError`.
            changed_by: Transaction-wide audit attribution applied to every emitted
                audit row (unless the resource's ``audit_config.changed_by_field``
                supplies a more specific value).
            audit_metadata: Free-form dict attached to every audit row emitted by
                this transaction. A ``transaction_id`` (ULID) is auto-added so rows
                can be grouped post-hoc.
        """
        context = TransactionContext(
            memory=self.memory,
            isolation_level=isolation_level,
            auto_retry=auto_retry,
            max_retries=max_retries,
            changed_by=changed_by,
            audit_metadata=audit_metadata,
        )

        try:
            yield context
            # Automatically commit on successful exit
            context.commit()
        except Exception:
            # Rollback on any exception
            context.rollback()
            raise
