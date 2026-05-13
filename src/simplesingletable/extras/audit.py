"""Audit logging utilities for querying and analyzing resource changes."""

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from boto3.dynamodb.conditions import Attr, Key
from ulid import from_timestamp

from ..models import AuditLog, PaginatedList

if TYPE_CHECKING:
    from .. import DynamoDbMemory


class AuditLogQuerier:
    """Helper class for querying and analyzing audit logs.

    Provides convenient methods to query audit logs by resource, resource type,
    operation, time range, and changed_by attribution.

    Example:
        >>> querier = AuditLogQuerier(memory)
        >>> # Get all changes to a specific resource
        >>> logs = querier.get_logs_for_resource("User", "user123")
        >>> # Get all CREATE operations for a resource type
        >>> creates = querier.get_logs_by_operation("User", "CREATE")
        >>> # Get all changes by a specific user
        >>> user_changes = querier.get_logs_by_changer("admin@example.com")
    """

    def __init__(self, memory: "DynamoDbMemory"):
        """Initialize the querier with a DynamoDbMemory instance.

        Args:
            memory: DynamoDbMemory instance to use for queries
        """
        self.memory = memory

    @property
    def audit_memory(self) -> "DynamoDbMemory":
        """A ``DynamoDbMemory`` view that targets the audit table.

        Delegates to the parent ``DynamoDbMemory.audit_view`` so multiple
        ``AuditLogQuerier`` instances against the same memory share a single
        secondary view (previously each querier built its own).
        """
        return self.memory.audit_view

    def get_logs_for_resource(
        self,
        resource_type: str,
        resource_id: str,
        limit: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        pagination_key: Optional[str] = None,
    ) -> PaginatedList[AuditLog]:
        """Get all audit logs for a specific resource.

        Uses gsi1 which is indexed by: {resource_type}#{resource_id} with pk as sort key.
        Uses ULID-based range queries for efficient date filtering.

        Args:
            resource_type: The resource type (e.g., "User", "Order")
            resource_id: The specific resource ID (ULID)
            limit: Maximum number of logs to return (most recent first)
            start_date: Filter logs created after this datetime (inclusive)
            end_date: Filter logs created before this datetime (inclusive)

        Returns:
            List of AuditLog resources, sorted by creation time (newest first)
        """
        prefix = AuditLog.get_unique_key_prefix()
        gsi1_pk_value = f"{prefix}#{resource_type}#{resource_id}"

        # Build key condition with ULID-based date range if provided
        key_condition = Key("gsi1pk").eq(gsi1_pk_value)

        if start_date and end_date:
            # Generate ULID-based pk values for date range
            start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
            end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
            key_condition &= Key("pk").between(start_pk, end_pk)
        elif start_date:
            # Only start date - use gte
            start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
            key_condition &= Key("pk").gte(start_pk)
        elif end_date:
            # Only end date - use lte
            end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
            key_condition &= Key("pk").lte(end_pk)

        # Query using paginated_dynamodb_query (descending=newest first)
        return self.audit_memory.paginated_dynamodb_query(
            key_condition=key_condition,
            index_name=AuditLog.INDEX_BY_RESOURCE,
            resource_class=AuditLog,
            results_limit=limit,
            ascending=False,  # Newest first (descending by pk/ULID)
            pagination_key=pagination_key,
        )

    def get_logs_for_resource_type(
        self,
        resource_type: str,
        limit: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        pagination_key: Optional[str] = None,
    ) -> PaginatedList[AuditLog]:
        """Get all audit logs for a resource type (all instances).

        Uses gsi2 which is indexed by: {resource_type} with pk as sort key.
        Uses ULID-based range queries for efficient date filtering.

        Args:
            resource_type: The resource type (e.g., "User", "Order")
            limit: Maximum number of logs to return (most recent first)
            start_date: Filter logs created after this datetime
            end_date: Filter logs created before this datetime

        Returns:
            List of AuditLog resources, sorted by creation time (newest first)
        """
        # Build key condition with ULID-based date range if provided
        key_condition = Key("gsi2pk").eq(AuditLog.get_unique_key_prefix() + "#" + resource_type)

        if start_date and end_date:
            # Generate ULID-based pk values for date range
            start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
            end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
            key_condition &= Key("pk").between(start_pk, end_pk)
        elif start_date:
            start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
            key_condition &= Key("pk").gte(start_pk)
        elif end_date:
            end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
            key_condition &= Key("pk").lte(end_pk)

        # Query using paginated_dynamodb_query
        return self.audit_memory.paginated_dynamodb_query(
            key_condition=key_condition,
            index_name=AuditLog.INDEX_BY_TYPE,
            resource_class=AuditLog,
            results_limit=limit,
            ascending=False,  # Newest first
            pagination_key=pagination_key,
        )

    def get_logs_by_operation(
        self,
        resource_type: str,
        operation: str,
        limit: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[AuditLog]:
        """Get audit logs filtered by operation type for a resource type.

        Uses gsi2 with ULID-based date range and filter expression for operation.

        Args:
            resource_type: The resource type (e.g., "User", "Order")
            operation: Operation type ("CREATE", "UPDATE", "DELETE", or "RESTORE")
            limit: Maximum number of logs to return (most recent first)
            start_date: Filter logs created after this datetime
            end_date: Filter logs created before this datetime

        Returns:
            List of AuditLog resources for the specified operation
        """
        # Build key condition with ULID-based date range if provided
        key_condition = Key("gsi2pk").eq(AuditLog.get_unique_key_prefix() + "#" + resource_type)

        if start_date and end_date:
            start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
            end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
            key_condition &= Key("pk").between(start_pk, end_pk)
        elif start_date:
            start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
            key_condition &= Key("pk").gte(start_pk)
        elif end_date:
            end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
            key_condition &= Key("pk").lte(end_pk)

        # Filter by operation using filter expression
        filter_expression = Attr("operation").eq(operation)

        # Query using paginated_dynamodb_query
        logs = self.audit_memory.paginated_dynamodb_query(
            key_condition=key_condition,
            index_name=AuditLog.INDEX_BY_TYPE,
            resource_class=AuditLog,
            filter_expression=filter_expression,
            results_limit=limit,
            ascending=False,  # Newest first
        )

        return list(logs)

    def get_logs_by_changer(
        self,
        changed_by: str,
        resource_type: Optional[str] = None,
        limit: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        newest_first: bool = True,
    ) -> List[AuditLog]:
        """Get all audit logs for changes made by a specific user/system.

        - Without ``resource_type``: queries the sparse ``INDEX_BY_CHANGER`` GSI
          (keyed on ``changed_by``) directly — no filter expression, no scan.
        - With ``resource_type``: queries ``INDEX_BY_TYPE`` with a ULID-based date
          range and a ``changed_by`` filter expression (because there is no
          composite ``changed_by + resource_type`` index).

        Args:
            changed_by: The user/system identifier who made the changes
            resource_type: Optional filter by resource type
            limit: Maximum number of logs to return (most recent first)
            start_date: Filter logs created after this datetime
            end_date: Filter logs created before this datetime

        Returns:
            List of AuditLog resources by the specified changer
        """
        prefix = AuditLog.get_unique_key_prefix()

        if resource_type:
            # Use the resource-type GSI plus a filter expression. The
            # changed_by-only path uses the dedicated INDEX_BY_CHANGER index instead.
            key_condition = Key("gsi2pk").eq(f"{prefix}#{resource_type}")

            if start_date and end_date:
                start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
                end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
                key_condition &= Key("pk").between(start_pk, end_pk)
            elif start_date:
                start_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(start_date).timestamp().str)["pk"]
                key_condition &= Key("pk").gte(start_pk)
            elif end_date:
                end_pk = AuditLog.dynamodb_lookup_keys_from_id(from_timestamp(end_date).timestamp().str + "ZZZZZ")["pk"]
                key_condition &= Key("pk").lte(end_pk)

            logs = self.audit_memory.paginated_dynamodb_query(
                key_condition=key_condition,
                index_name=AuditLog.INDEX_BY_TYPE,
                resource_class=AuditLog,
                filter_expression=Attr("changed_by").eq(changed_by),
                results_limit=limit,
                ascending=not newest_first,
            )
            return list(logs)

        # No resource_type filter: query the sparse changed_by GSI directly.
        # Date range filtering uses gsi3sk (created_at isoformat).
        key_condition = Key("gsi3pk").eq(f"{prefix}#changer#{changed_by}")
        if start_date and end_date:
            key_condition &= Key("gsi3sk").between(start_date.isoformat(), end_date.isoformat())
        elif start_date:
            key_condition &= Key("gsi3sk").gte(start_date.isoformat())
        elif end_date:
            key_condition &= Key("gsi3sk").lte(end_date.isoformat())

        logs = self.audit_memory.paginated_dynamodb_query(
            key_condition=key_condition,
            index_name=AuditLog.INDEX_BY_CHANGER,
            resource_class=AuditLog,
            results_limit=limit,
            ascending=not newest_first,
        )
        return list(logs)

    def get_field_history(
        self,
        resource_type: str,
        resource_id: str,
        field_name: str,
    ) -> List[dict]:
        """Get the change history for a specific field of a resource.

        Args:
            resource_type: The resource type (e.g., "User", "Order")
            resource_id: The specific resource ID (ULID)
            field_name: The field name to track (e.g., "email", "status")

        Returns:
            List of dicts with structure:
            [
                {
                    "timestamp": datetime,
                    "operation": "UPDATE",
                    "changed_by": "user@example.com",
                    "old_value": "old@example.com",
                    "new_value": "new@example.com",
                },
                ...
            ]
        """
        logs = self.get_logs_for_resource(resource_type, resource_id)

        field_changes = []
        for log in reversed(logs):  # Process chronologically (oldest first)
            # Check if this field was changed
            if log.changed_fields and field_name in log.changed_fields:
                change = log.changed_fields[field_name]
                field_changes.append(
                    {
                        "timestamp": log.created_at,
                        "operation": log.operation,
                        "changed_by": log.changed_by,
                        "old_value": change.get("old"),
                        "new_value": change.get("new"),
                    }
                )
            # For CREATE operations, capture the initial value from snapshot
            elif log.operation == "CREATE" and log.resource_snapshot:
                if field_name in log.resource_snapshot:
                    field_changes.append(
                        {
                            "timestamp": log.created_at,
                            "operation": "CREATE",
                            "changed_by": log.changed_by,
                            "old_value": None,
                            "new_value": log.resource_snapshot[field_name],
                        }
                    )

        return field_changes

    def get_recent_changes(
        self,
        limit: int = 50,
        resource_type: Optional[str] = None,
    ) -> list[AuditLog]:
        """Get the most recent audit logs across all or specific resource type.

        Args:
            limit: Maximum number of logs to return (default 50)
            resource_type: Optional filter by resource type

        Returns:
            List of most recent AuditLog resources
        """
        if resource_type:
            result = self.get_logs_for_resource_type(
                resource_type=resource_type,
                limit=limit,
            )
            return list(result)
        else:
            # Get all audit logs via gsitype (sorted by updated_at/gsitypesk)
            logs = self.audit_memory.list_type_by_updated_at(AuditLog, results_limit=limit, ascending=False)

            return logs.as_list()
