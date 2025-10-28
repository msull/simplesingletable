"""Tests for audit logging to a separate DynamoDB table."""

import logging
from typing import ClassVar

import pytest

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.extras.audit import AuditLogQuerier
from simplesingletable.models import AuditConfig, AuditLog, ResourceConfig


# Test Resources with Audit Config
class SeparateAuditUser(DynamoDbVersionedResource):
    """Versioned resource with audit tracking to separate table."""

    name: str
    email: str
    status: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
    )


class SeparateAuditProject(DynamoDbResource):
    """Non-versioned resource with audit tracking to separate table."""

    name: str
    description: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
    )


@pytest.fixture
def separate_audit_memory(dynamodb_via_docker):
    """Create a DynamoDbMemory with separate audit table."""
    import boto3
    from uuid import uuid4

    # Connect to DynamoDB Local
    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url=dynamodb_via_docker,
        region_name="us-east-1",
        aws_access_key_id="unused",
        aws_secret_access_key="unused",
    )

    # Create main table
    main_table_name = f"test-main-table-{uuid4().hex[:8]}"
    main_table = dynamodb.create_table(
        TableName=main_table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsitype", "AttributeType": "S"},
            {"AttributeName": "gsitypesk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsitype",
                "KeySchema": [
                    {"AttributeName": "gsitype", "KeyType": "HASH"},
                    {"AttributeName": "gsitypesk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Create separate audit table
    audit_table_name = f"test-audit-table-{uuid4().hex[:8]}"
    audit_table = dynamodb.create_table(
        TableName=audit_table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsitype", "AttributeType": "S"},
            {"AttributeName": "gsitypesk", "AttributeType": "S"},
            {"AttributeName": "gsi1pk", "AttributeType": "S"},
            {"AttributeName": "gsi2pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsitype",
                "KeySchema": [
                    {"AttributeName": "gsitype", "KeyType": "HASH"},
                    {"AttributeName": "gsitypesk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            },
            {
                "IndexName": "gsi1",
                "KeySchema": [
                    {"AttributeName": "gsi1pk", "KeyType": "HASH"},
                    {"AttributeName": "pk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            },
            {
                "IndexName": "gsi2",
                "KeySchema": [
                    {"AttributeName": "gsi2pk", "KeyType": "HASH"},
                    {"AttributeName": "pk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    logger = logging.getLogger(__name__)

    # Create memory with separate audit table
    connection_params = {
        "aws_access_key_id": "unused",
        "aws_secret_access_key": "unused",
        "region_name": "us-east-1",
    }
    memory = DynamoDbMemory(
        logger=logger,
        table_name=main_table_name,
        endpoint_url=dynamodb_via_docker,
        connection_params=connection_params,
        audit_table_name=audit_table_name,
        audit_endpoint_url=dynamodb_via_docker,
        audit_connection_params=connection_params,
        track_stats=False,
    )

    yield memory

    # Cleanup
    main_table.delete()
    audit_table.delete()


# === Basic Separate Table Tests ===


def test_audit_writes_to_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test that audit logs are written to the separate audit table, not the main table."""
    # Create a user - should write to main table
    user = separate_audit_memory.create_new(
        SeparateAuditUser,
        {"name": "Alice", "email": "alice@example.com", "status": "active"},
        changed_by="admin@example.com",
    )

    # Verify user is in main table
    main_response = separate_audit_memory.dynamodb_table.get_item(
        Key={"pk": f"SeparateAuditUser#{user.resource_id}", "sk": "v0"}
    )
    assert "Item" in main_response
    assert main_response["Item"]["pk"] == f"SeparateAuditUser#{user.resource_id}"

    # Verify audit log is NOT in main table
    main_audit_items = separate_audit_memory.dynamodb_table.query(
        IndexName="gsitype", KeyConditionExpression="gsitype = :val", ExpressionAttributeValues={":val": "_INTERNAL#AuditLog"}
    )
    assert main_audit_items["Count"] == 0

    # Verify audit log IS in audit table
    audit_response = separate_audit_memory.audit_dynamodb_table.query(
        IndexName="gsitype", KeyConditionExpression="gsitype = :val", ExpressionAttributeValues={":val": "_INTERNAL#AuditLog"}
    )
    assert audit_response["Count"] == 1
    audit_item = audit_response["Items"][0]
    assert audit_item["audited_resource_type"] == "SeparateAuditUser"
    assert audit_item["audited_resource_id"] == user.resource_id
    assert audit_item["operation"] == "CREATE"


def test_audit_queries_from_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test that audit log queries read from the separate audit table."""
    # Create multiple users
    user1 = separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "Bob", "email": "bob@example.com", "status": "active"}, changed_by="admin"
    )
    user2 = separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "Charlie", "email": "charlie@example.com", "status": "active"}, changed_by="admin"
    )

    # Query audit logs using AuditLogQuerier
    querier = AuditLogQuerier(separate_audit_memory)
    logs = querier.get_logs_for_resource_type("SeparateAuditUser")

    assert len(logs) == 2
    assert {log.audited_resource_id for log in logs} == {user1.resource_id, user2.resource_id}


def test_backward_compatibility_no_audit_table(dynamodb_memory: DynamoDbMemory):
    """Test that when audit_table_name is not set, audit logs go to main table (backward compatible)."""

    class CompatUser(DynamoDbVersionedResource):
        name: str
        email: str

        resource_config: ClassVar[ResourceConfig] = ResourceConfig(
            compress_data=True,
            audit_config=AuditConfig(
                enabled=True,
                track_field_changes=True,
            ),
        )

    # Create a user with default memory (no separate audit table)
    user = dynamodb_memory.create_new(
        CompatUser, {"name": "Dave", "email": "dave@example.com"}, changed_by="admin@example.com"
    )

    # Verify audit log IS in main table
    audit_items = dynamodb_memory.dynamodb_table.query(
        IndexName="gsitype", KeyConditionExpression="gsitype = :val", ExpressionAttributeValues={":val": "_INTERNAL#AuditLog"}
    )
    assert audit_items["Count"] == 1
    audit_item = audit_items["Items"][0]
    assert audit_item["audited_resource_type"] == "CompatUser"
    assert audit_item["audited_resource_id"] == user.resource_id


# === CRUD Operations Tests ===


def test_create_update_delete_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test that CREATE, UPDATE, DELETE operations all write to separate audit table."""
    # CREATE
    user = separate_audit_memory.create_new(
        SeparateAuditUser,
        {"name": "Eve", "email": "eve@example.com", "status": "active"},
        changed_by="admin@example.com",
    )

    # UPDATE
    user = separate_audit_memory.update_existing(user, {"status": "inactive"}, changed_by="admin@example.com")

    # DELETE
    separate_audit_memory.delete_existing(user, changed_by="admin@example.com")

    # Query all audit logs for this user from audit table
    querier = AuditLogQuerier(separate_audit_memory)
    logs = querier.get_logs_for_resource("SeparateAuditUser", user.resource_id)

    assert len(logs) == 3
    operations = {log.operation for log in logs}
    assert operations == {"CREATE", "UPDATE", "DELETE"}


def test_audit_field_changes_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test that field-level changes are tracked correctly in separate audit table."""
    user = separate_audit_memory.create_new(
        SeparateAuditUser,
        {"name": "Frank", "email": "frank@example.com", "status": "active"},
        changed_by="admin@example.com",
    )

    # Update with field changes
    separate_audit_memory.update_existing(user, {"name": "Franklin", "status": "inactive"}, changed_by="editor@example.com")

    # Query and verify field changes
    querier = AuditLogQuerier(separate_audit_memory)
    logs = querier.get_logs_for_resource("SeparateAuditUser", user.resource_id)

    update_log = logs[0]  # Newest first
    assert update_log.operation == "UPDATE"
    assert update_log.changed_fields is not None
    assert "name" in update_log.changed_fields
    assert update_log.changed_fields["name"]["old"] == "Frank"
    assert update_log.changed_fields["name"]["new"] == "Franklin"


# === Query Method Tests ===


def test_get_logs_by_operation_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test querying audit logs by operation from separate table."""
    user1 = separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "User1", "email": "user1@example.com", "status": "active"}, changed_by="admin"
    )
    user2 = separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "User2", "email": "user2@example.com", "status": "active"}, changed_by="admin"
    )
    separate_audit_memory.update_existing(user1, {"status": "inactive"}, changed_by="admin")

    querier = AuditLogQuerier(separate_audit_memory)

    # Query CREATE operations
    creates = querier.get_logs_by_operation("SeparateAuditUser", "CREATE")
    assert len(creates) == 2

    # Query UPDATE operations
    updates = querier.get_logs_by_operation("SeparateAuditUser", "UPDATE")
    assert len(updates) == 1


def test_get_logs_by_changer_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test querying audit logs by changed_by from separate table."""
    separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "User1", "email": "user1@example.com", "status": "active"}, changed_by="admin1"
    )
    separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "User2", "email": "user2@example.com", "status": "active"}, changed_by="admin2"
    )
    separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "User3", "email": "user3@example.com", "status": "active"}, changed_by="admin1"
    )

    querier = AuditLogQuerier(separate_audit_memory)
    admin1_logs = querier.get_logs_by_changer("admin1", resource_type="SeparateAuditUser")

    assert len(admin1_logs) == 2
    assert all(log.changed_by == "admin1" for log in admin1_logs)


def test_get_field_history_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test tracking field history from separate audit table."""
    user = separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "Grace", "email": "grace@example.com", "status": "pending"}, changed_by="admin"
    )

    # Update status multiple times
    user = separate_audit_memory.update_existing(user, {"status": "active"}, changed_by="admin")
    user = separate_audit_memory.update_existing(user, {"status": "inactive"}, changed_by="admin")

    querier = AuditLogQuerier(separate_audit_memory)
    status_history = querier.get_field_history("SeparateAuditUser", user.resource_id, "status")

    assert len(status_history) == 3  # CREATE + 2 UPDATEs
    assert status_history[0]["new_value"] == "pending"
    assert status_history[1]["new_value"] == "active"
    assert status_history[2]["new_value"] == "inactive"


def test_get_recent_changes_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test getting recent changes from separate audit table."""
    # Create resources of different types
    separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "User1", "email": "user1@example.com", "status": "active"}, changed_by="admin"
    )
    separate_audit_memory.create_new(
        SeparateAuditProject, {"name": "Project1", "description": "Desc1"}, changed_by="admin"
    )
    separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "User2", "email": "user2@example.com", "status": "active"}, changed_by="admin"
    )

    querier = AuditLogQuerier(separate_audit_memory)
    recent = querier.get_recent_changes(limit=10)

    assert len(recent) == 3
    # Should include both SeparateAuditUser and SeparateAuditProject logs


# === Mixed Scenario Tests ===


def test_multiple_resource_types_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test that multiple resource types can audit to the same separate table."""
    # Create different resource types
    user = separate_audit_memory.create_new(
        SeparateAuditUser, {"name": "Henry", "email": "henry@example.com", "status": "active"}, changed_by="admin"
    )
    project = separate_audit_memory.create_new(
        SeparateAuditProject, {"name": "MyProject", "description": "A test project"}, changed_by="admin"
    )

    # Verify both are in audit table
    querier = AuditLogQuerier(separate_audit_memory)
    user_logs = querier.get_logs_for_resource("SeparateAuditUser", user.resource_id)
    project_logs = querier.get_logs_for_resource("SeparateAuditProject", project.resource_id)

    assert len(user_logs) == 1
    assert len(project_logs) == 1


def test_pagination_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test pagination works correctly with separate audit table."""
    # Create many users
    for i in range(5):
        separate_audit_memory.create_new(
            SeparateAuditUser, {"name": f"User{i}", "email": f"user{i}@example.com", "status": "active"}, changed_by="admin"
        )

    querier = AuditLogQuerier(separate_audit_memory)

    # Query with limit
    logs = querier.get_logs_for_resource_type("SeparateAuditUser", limit=3)
    assert len(logs) == 3

    # Verify pagination key exists or not
    assert hasattr(logs, "next_pagination_key")


# === Property Tests ===


def test_audit_properties_with_separate_table(separate_audit_memory: DynamoDbMemory):
    """Test that audit_dynamodb_client and audit_dynamodb_table properties work correctly."""
    # When audit table is configured, should return separate instances
    assert separate_audit_memory.audit_dynamodb_table is not None
    assert separate_audit_memory.audit_table_name is not None
    assert separate_audit_memory.audit_dynamodb_table.table_name == separate_audit_memory.audit_table_name

    # Main table should still work
    assert separate_audit_memory.dynamodb_table.table_name == separate_audit_memory.table_name


def test_audit_properties_without_separate_table(dynamodb_memory: DynamoDbMemory):
    """Test that audit properties return main table when no separate audit table configured."""
    # When no audit table configured, should return main table
    assert dynamodb_memory.audit_dynamodb_table is dynamodb_memory.dynamodb_table
    assert dynamodb_memory.audit_dynamodb_client is dynamodb_memory.dynamodb_client


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
