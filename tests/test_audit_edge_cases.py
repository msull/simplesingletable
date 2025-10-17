"""Edge case tests for audit logging functionality."""

from datetime import datetime, timedelta, timezone
from typing import ClassVar, Optional

from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.extras.audit import AuditLogQuerier
from simplesingletable.models import AuditConfig, ResourceConfig


# ============================================================================
# Test Resources
# ============================================================================


class SimpleAuditedResource(DynamoDbResource):
    """Simple audited resource for testing."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(enabled=True, track_field_changes=True, include_snapshot=True)
    )

    name: str
    value: int


class NestedModel(BaseModel):
    """Nested Pydantic model for testing."""

    field1: str
    field2: int


class ResourceWithNestedModels(DynamoDbResource):
    """Resource with nested Pydantic models."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(enabled=True, track_field_changes=True, include_snapshot=True)
    )

    name: str
    nested: NestedModel
    optional_nested: Optional[NestedModel] = None


class ResourceWithComplexTypes(DynamoDbResource):
    """Resource with complex field types."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(enabled=True, track_field_changes=True, include_snapshot=True)
    )

    name: str
    tags: list[str]
    metadata: dict[str, str]
    optional_list: Optional[list[int]] = None
    optional_dict: Optional[dict[str, int]] = None


class VersionedAuditedResource(DynamoDbVersionedResource):
    """Versioned resource with audit logging."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(enabled=True, track_field_changes=True, include_snapshot=True)
    )

    name: str
    value: int


# ============================================================================
# Nested Pydantic Model Tests
# ============================================================================


def test_audit_create_with_nested_model(dynamodb_memory: DynamoDbMemory):
    """Test CREATE with nested Pydantic models."""
    resource = dynamodb_memory.create_new(
        ResourceWithNestedModels,
        {
            "name": "Test",
            "nested": NestedModel(field1="value1", field2=42),
            "optional_nested": NestedModel(field1="opt_value", field2=99),
        },
        changed_by="user@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("ResourceWithNestedModels", resource.resource_id)

    create_log = logs[0]
    snapshot = create_log.resource_snapshot

    # Nested models should be serialized as dicts
    assert snapshot["nested"]["field1"] == "value1"
    assert snapshot["nested"]["field2"] == 42
    assert snapshot["optional_nested"]["field1"] == "opt_value"


def test_audit_update_nested_model_field(dynamodb_memory: DynamoDbMemory):
    """Test UPDATE tracking changes to nested model fields."""
    resource = dynamodb_memory.create_new(
        ResourceWithNestedModels,
        {
            "name": "Test",
            "nested": NestedModel(field1="original", field2=1),
        },
        changed_by="user@example.com",
    )

    # Update the nested model
    dynamodb_memory.update_existing(
        resource,
        {"nested": NestedModel(field1="updated", field2=2)},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("ResourceWithNestedModels", resource.resource_id)

    update_log = logs[0]

    # Nested model change should be tracked
    assert "nested" in update_log.changed_fields
    change = update_log.changed_fields["nested"]
    assert change["old"]["field1"] == "original"
    assert change["new"]["field1"] == "updated"


def test_audit_update_add_optional_nested_model(dynamodb_memory: DynamoDbMemory):
    """Test UPDATE adding an optional nested model."""
    resource = dynamodb_memory.create_new(
        ResourceWithNestedModels,
        {
            "name": "Test",
            "nested": NestedModel(field1="value", field2=1),
            # optional_nested not set
        },
        changed_by="user@example.com",
    )

    # Add optional nested model
    dynamodb_memory.update_existing(
        resource,
        {"optional_nested": NestedModel(field1="new", field2=99)},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("ResourceWithNestedModels", resource.resource_id)

    update_log = logs[0]

    assert "optional_nested" in update_log.changed_fields
    change = update_log.changed_fields["optional_nested"]
    assert change["old"] is None
    assert change["new"]["field1"] == "new"


# ============================================================================
# Complex Type Tests
# ============================================================================


def test_audit_update_list_field(dynamodb_memory: DynamoDbMemory):
    """Test UPDATE tracking changes to list fields."""
    resource = dynamodb_memory.create_new(
        ResourceWithComplexTypes,
        {
            "name": "Test",
            "tags": ["tag1", "tag2"],
            "metadata": {"key": "value"},
        },
        changed_by="user@example.com",
    )

    # Update list
    dynamodb_memory.update_existing(
        resource,
        {"tags": ["tag1", "tag2", "tag3"]},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("ResourceWithComplexTypes", resource.resource_id)

    update_log = logs[0]

    assert "tags" in update_log.changed_fields
    change = update_log.changed_fields["tags"]
    assert change["old"] == ["tag1", "tag2"]
    assert change["new"] == ["tag1", "tag2", "tag3"]


def test_audit_update_dict_field(dynamodb_memory: DynamoDbMemory):
    """Test UPDATE tracking changes to dict fields."""
    resource = dynamodb_memory.create_new(
        ResourceWithComplexTypes,
        {
            "name": "Test",
            "tags": [],
            "metadata": {"key1": "value1"},
        },
        changed_by="user@example.com",
    )

    # Update dict
    dynamodb_memory.update_existing(
        resource,
        {"metadata": {"key1": "value1", "key2": "value2"}},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("ResourceWithComplexTypes", resource.resource_id)

    update_log = logs[0]

    assert "metadata" in update_log.changed_fields
    change = update_log.changed_fields["metadata"]
    assert change["old"] == {"key1": "value1"}
    assert change["new"] == {"key1": "value1", "key2": "value2"}


def test_audit_update_empty_list_to_populated(dynamodb_memory: DynamoDbMemory):
    """Test UPDATE from empty list to populated list."""
    resource = dynamodb_memory.create_new(
        ResourceWithComplexTypes,
        {
            "name": "Test",
            "tags": [],
            "metadata": {},
        },
        changed_by="user@example.com",
    )

    dynamodb_memory.update_existing(
        resource,
        {"tags": ["new_tag"]},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("ResourceWithComplexTypes", resource.resource_id)

    update_log = logs[0]

    assert "tags" in update_log.changed_fields
    assert update_log.changed_fields["tags"]["old"] == []
    assert update_log.changed_fields["tags"]["new"] == ["new_tag"]


# ============================================================================
# Versioned Resource Tests
# ============================================================================


def test_audit_versioned_resource_create(dynamodb_memory: DynamoDbMemory):
    """Test audit logging works with versioned resources."""
    resource = dynamodb_memory.create_new(
        VersionedAuditedResource,
        {"name": "Versioned", "value": 1},
        changed_by="user@example.com",
    )

    assert resource.version == 1

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("VersionedAuditedResource", resource.resource_id)

    assert len(logs) == 1
    assert logs[0].operation == "CREATE"


def test_audit_versioned_resource_updates(dynamodb_memory: DynamoDbMemory):
    """Test audit logging tracks each version update."""
    resource = dynamodb_memory.create_new(
        VersionedAuditedResource,
        {"name": "Versioned", "value": 1},
        changed_by="user@example.com",
    )

    # Make multiple updates
    resource = dynamodb_memory.update_existing(resource, {"value": 2}, changed_by="editor1@example.com")
    assert resource.version == 2

    dynamodb_memory.update_existing(resource, {"value": 3}, changed_by="editor2@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("VersionedAuditedResource", resource.resource_id)

    # Should have CREATE + 2 UPDATEs
    assert len(logs) == 3
    assert logs[0].operation == "UPDATE"  # Most recent
    assert logs[0].changed_by == "editor2@example.com"
    assert logs[1].operation == "UPDATE"
    assert logs[1].changed_by == "editor1@example.com"
    assert logs[2].operation == "CREATE"


def test_audit_versioned_resource_delete(dynamodb_memory: DynamoDbMemory):
    """Test DELETE of versioned resource creates audit log."""
    resource = dynamodb_memory.create_new(
        VersionedAuditedResource,
        {"name": "To Delete", "value": 1},
        changed_by="user@example.com",
    )

    dynamodb_memory.delete_existing(resource, changed_by="admin@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("VersionedAuditedResource", resource.resource_id)

    assert len(logs) == 2
    assert logs[0].operation == "DELETE"
    assert logs[1].operation == "CREATE"


# ============================================================================
# Query Performance and Pagination Tests
# ============================================================================


def test_audit_query_large_result_set_pagination(dynamodb_memory: DynamoDbMemory):
    """Test querying large result sets with pagination."""
    # Create 25 resources to test pagination
    resources = []
    for i in range(25):
        resource = dynamodb_memory.create_new(
            SimpleAuditedResource,
            {"name": f"Resource {i}", "value": i},
            changed_by="creator@example.com",
        )
        resources.append(resource)

    querier = AuditLogQuerier(dynamodb_memory)

    # Query with limit
    logs_page1 = querier.get_logs_for_resource_type("SimpleAuditedResource", limit=10)
    assert len(logs_page1) == 10
    assert logs_page1.next_pagination_key

    # Get next page
    logs_page2 = querier.get_logs_for_resource_type(
        "SimpleAuditedResource", limit=10, pagination_key=logs_page1.next_pagination_key
    )
    assert len(logs_page2) == 10
    assert logs_page2.next_pagination_key

    # Get last page
    logs_page3 = querier.get_logs_for_resource_type(
        "SimpleAuditedResource", limit=10, pagination_key=logs_page2.next_pagination_key
    )
    assert len(logs_page3) == 5
    assert not logs_page3.next_pagination_key


def test_audit_query_date_range_boundaries(dynamodb_memory: DynamoDbMemory, mocker):
    """Test date range queries with precise boundaries."""
    # Create resources at specific times
    base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Resource at 12:00
    mocker.patch("simplesingletable.models._now", return_value=base_time)
    resource1 = dynamodb_memory.create_new(
        SimpleAuditedResource, {"name": "First", "value": 1}, changed_by="user@example.com"
    )

    # Resource at 12:30
    mocker.patch("simplesingletable.models._now", return_value=base_time + timedelta(minutes=30))

    resource2 = dynamodb_memory.create_new(
        SimpleAuditedResource, {"name": "Second", "value": 2}, changed_by="user@example.com"
    )

    # Resource at 13:00
    mocker.patch("simplesingletable.models._now", return_value=base_time + timedelta(hours=1))
    resource3 = dynamodb_memory.create_new(
        SimpleAuditedResource, {"name": "Third", "value": 3}, changed_by="user@example.com"
    )

    querier = AuditLogQuerier(dynamodb_memory)

    # Query for resources between 12:00 and 12:30
    logs = querier.get_logs_for_resource_type(
        "SimpleAuditedResource",
        start_date=base_time,
        end_date=base_time + timedelta(minutes=30),
    )

    # Should get first two resources
    assert len(logs) == 2
    resource_ids = [log.audited_resource_id for log in logs]
    assert resource1.resource_id in resource_ids
    assert resource2.resource_id in resource_ids
    assert resource3.resource_id not in resource_ids


# ============================================================================
# Changed By Tracking Tests
# ============================================================================


def test_audit_multiple_users_same_resource(dynamodb_memory: DynamoDbMemory):
    """Test tracking changes from multiple users to same resource."""
    resource = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Shared", "value": 1},
        changed_by="user1@example.com",
    )

    # Different users update
    dynamodb_memory.update_existing(resource, {"value": 2}, changed_by="user2@example.com")
    resource = dynamodb_memory.update_existing(resource, {"value": 3}, changed_by="user3@example.com")
    dynamodb_memory.delete_existing(resource, changed_by="admin@example.com")

    querier = AuditLogQuerier(dynamodb_memory)

    # Query by each user
    user1_logs = querier.get_logs_by_changer("user1@example.com")
    assert len(user1_logs) >= 1
    assert all(log.changed_by == "user1@example.com" for log in user1_logs)

    user2_logs = querier.get_logs_by_changer("user2@example.com")
    assert len(user2_logs) >= 1
    assert all(log.changed_by == "user2@example.com" for log in user2_logs)

    admin_logs = querier.get_logs_by_changer("admin@example.com")
    assert len(admin_logs) >= 1
    assert all(log.changed_by == "admin@example.com" for log in admin_logs)


def test_audit_query_by_changer_with_resource_type_filter(dynamodb_memory: DynamoDbMemory):
    """Test querying by changer with resource type filter."""
    # Create two different resource types
    resource1 = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Resource1", "value": 1},
        changed_by="user@example.com",
    )

    dynamodb_memory.create_new(
        ResourceWithComplexTypes,
        {"name": "Resource2", "tags": [], "metadata": {}},
        changed_by="user@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)

    # Query for user's changes to SimpleAuditedResource only
    logs = querier.get_logs_by_changer("user@example.com", resource_type="SimpleAuditedResource")

    assert len(logs) == 1
    assert logs[0].audited_resource_id == resource1.resource_id


# ============================================================================
# Metadata Tests
# ============================================================================


def test_audit_with_custom_metadata(dynamodb_memory: DynamoDbMemory):
    """Test audit logs can include custom metadata."""
    resource = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Test", "value": 1},
        changed_by="user@example.com",
        audit_metadata={"source": "API", "request_id": "req-12345"},
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("SimpleAuditedResource", resource.resource_id)

    assert logs[0].audit_metadata is not None
    assert logs[0].audit_metadata["source"] == "API"
    assert logs[0].audit_metadata["request_id"] == "req-12345"


def test_audit_update_with_metadata(dynamodb_memory: DynamoDbMemory):
    """Test UPDATE operations can include audit metadata."""
    resource = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Test", "value": 1},
        changed_by="user@example.com",
    )

    dynamodb_memory.update_existing(
        resource,
        {"value": 2},
        changed_by="editor@example.com",
        audit_metadata={"reason": "Bug fix", "ticket": "JIRA-123"},
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("SimpleAuditedResource", resource.resource_id)

    update_log = logs[0]
    assert update_log.audit_metadata is not None
    assert update_log.audit_metadata["reason"] == "Bug fix"
    assert update_log.audit_metadata["ticket"] == "JIRA-123"


# ============================================================================
# No Changes Update Tests
# ============================================================================


def test_audit_update_no_actual_changes(dynamodb_memory: DynamoDbMemory):
    """Test UPDATE with no actual field changes still creates audit log."""
    resource = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Test", "value": 1},
        changed_by="user@example.com",
    )

    # Update with same values
    dynamodb_memory.update_existing(
        resource,
        {"name": "Test", "value": 1},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("SimpleAuditedResource", resource.resource_id)

    # Should have CREATE + UPDATE even though no fields changed
    assert len(logs) == 2
    update_log = logs[0]
    assert update_log.operation == "UPDATE"
    # changed_fields should be empty or contain only base fields
    assert not update_log.changed_fields


# ============================================================================
# Concurrent Modification Tests
# ============================================================================


def test_audit_rapid_sequential_updates(dynamodb_memory: DynamoDbMemory):
    """Test audit logging handles rapid sequential updates correctly."""
    resource = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Test", "value": 0},
        changed_by="user@example.com",
    )

    # Perform 10 rapid updates
    for i in range(1, 11):
        resource = dynamodb_memory.update_existing(resource, {"value": i}, changed_by=f"user{i}@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("SimpleAuditedResource", resource.resource_id)

    # Should have CREATE + 10 UPDATEs = 11 logs
    assert len(logs) == 11
    assert logs[0].operation == "UPDATE"  # Most recent
    assert logs[10].operation == "CREATE"

    # Verify field history shows progression
    history = querier.get_field_history("SimpleAuditedResource", resource.resource_id, "value")
    assert len(history) == 11
    assert history[0]["new_value"] == 0  # CREATE
    assert history[10]["new_value"] == 10  # Last UPDATE


# ============================================================================
# Field History Edge Cases
# ============================================================================


def test_audit_field_history_field_not_changed(dynamodb_memory: DynamoDbMemory):
    """Test field history for a field that was never changed."""
    resource = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Original", "value": 1},
        changed_by="user@example.com",
    )

    # Update different field
    dynamodb_memory.update_existing(resource, {"value": 2}, changed_by="editor@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    # history only present for "CREATE" fields if we have a resource snapshot
    history = querier.get_field_history("SimpleAuditedResource", resource.resource_id, "name")

    # Should only have the CREATE entry
    assert len(history) == 1
    assert history[0]["operation"] == "CREATE"
    assert history[0]["new_value"] == "Original"


def test_audit_field_history_nonexistent_field(dynamodb_memory: DynamoDbMemory):
    """Test field history for a field that doesn't exist in the resource."""
    resource = dynamodb_memory.create_new(
        SimpleAuditedResource,
        {"name": "Test", "value": 1},
        changed_by="user@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    history = querier.get_field_history("SimpleAuditedResource", resource.resource_id, "nonexistent_field")

    # Should return empty list (field never existed in snapshots or changes)
    assert len(history) == 0
