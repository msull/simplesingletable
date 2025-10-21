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
        logs = self.memory.paginated_dynamodb_query(
            key_condition=key_condition,
            index_name="gsi1",
            resource_class=AuditLog,
            results_limit=limit,
            ascending=False,  # Newest first (descending by pk/ULID)
            pagination_key=pagination_key,
        )

        return logs

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
        logs = self.memory.paginated_dynamodb_query(
            key_condition=key_condition,
            index_name="gsi2",
            resource_class=AuditLog,
            results_limit=limit,
            ascending=False,  # Newest first
            pagination_key=pagination_key,
        )

        return logs

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
        logs = self.memory.paginated_dynamodb_query(
            key_condition=key_condition,
            index_name="gsi2",
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

        Uses DynamoDB filter expressions to filter by changed_by.
        If resource_type is specified, queries gsi2 with ULID-based date range;
        otherwise queries gsitype (all audit logs).

        Args:
            changed_by: The user/system identifier who made the changes
            resource_type: Optional filter by resource type
            limit: Maximum number of logs to return (most recent first)
            start_date: Filter logs created after this datetime
            end_date: Filter logs created before this datetime

        Returns:
            List of AuditLog resources by the specified changer
        """
        # Filter expression for changed_by
        filter_expression = Attr("changed_by").eq(changed_by)

        # Choose index based on whether resource_type is specified
        if resource_type:
            # Query specific resource type via gsi2 with ULID-based date range
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

            logs = self.memory.paginated_dynamodb_query(
                key_condition=key_condition,
                index_name="gsi2",
                resource_class=AuditLog,
                filter_expression=filter_expression,
                results_limit=limit,
                ascending=not newest_first,
            )
        else:
            # Query all audit logs via gsitype
            # Note: gsitype uses gsitypesk (updated_at) for sorting, not pk
            # Date filtering here uses filter expression on created_at
            key_condition = Key("gsitype").eq(AuditLog.db_get_gsitypepk())

            # For gsitype queries, add date filter to filter_expression
            if start_date and end_date:
                key_condition &= Key("gsitypesk").between(start_date.isoformat(), end_date.isoformat())
            elif start_date:
                key_condition &= Key("gsitypesk").gte(start_date.isoformat())
            elif end_date:
                key_condition &= Key("gsitypesk").lte(end_date.isoformat())

            logs = self.memory.paginated_dynamodb_query(
                key_condition=key_condition,
                index_name="gsitype",
                resource_class=AuditLog,
                filter_expression=filter_expression,
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
            return self.get_logs_for_resource_type(
                resource_type=resource_type,
                limit=limit,
            )
        else:
            # Get all audit logs via gsitype (sorted by updated_at/gsitypesk)
            logs = self.memory.list_type_by_updated_at(AuditLog, results_limit=limit, ascending=False)

            return logs.as_list()
