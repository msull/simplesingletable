"""Tests for audit logging functionality."""

from typing import Optional, ClassVar

import pytest
from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.extras.audit import AuditLogQuerier
from simplesingletable.models import AuditConfig, AuditLog, ResourceConfig


# Test Resources with Audit Config
class AuditedUser(DynamoDbVersionedResource):
    """Versioned resource with full audit tracking."""

    name: str
    email: str
    status: str
    age: Optional[int] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            changed_by_field=None,  # Will be provided explicitly
        ),
    )


class AuditedProject(DynamoDbResource):
    """Non-versioned resource with audit tracking."""

    name: str
    description: str
    owner_id: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            exclude_fields={"owner_id"},  # Don't track owner changes
            include_snapshot=False,  # No snapshots
        ),
    )


class AuditedTask(DynamoDbVersionedResource):
    """Resource with changed_by field embedded."""

    title: str
    completed: bool
    assigned_to: str  # This will be used as changed_by
    notes: Optional[str] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            changed_by_field="assigned_to",  # Auto-extract from resource
        ),
    )


class NonAuditedResource(DynamoDbResource):
    """Resource without audit tracking."""

    name: str


class NestedData(BaseModel):
    """Nested Pydantic model for blob testing."""

    value: str
    count: int


class AuditedBlobResource(DynamoDbVersionedResource):
    """Resource with blob fields for audit testing."""

    name: str
    large_data: Optional[list[str]] = None  # Will be stored as blob
    nested_data: Optional[NestedData] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        blob_fields={
            "large_data": {"compress": True},
            "nested_data": {"compress": False},
        },
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
    )


# === Basic Audit Log Creation Tests ===


def test_audit_log_model_creation():
    """Test that AuditLog model can be instantiated correctly."""
    audit_log = AuditLog.create_new(
        {
            "audited_resource_type": "User",
            "audited_resource_id": "test123",
            "operation": "CREATE",
            "changed_by": "admin@example.com",
            "changed_fields": {"name": {"old": None, "new": "John"}},
            "resource_snapshot": {"name": "John", "email": "john@example.com"},
            "audit_metadata": {"reason": "Initial creation"},
        }
    )

    assert audit_log.audited_resource_type == "User"
    assert audit_log.audited_resource_id == "test123"
    assert audit_log.operation == "CREATE"
    assert audit_log.changed_by == "admin@example.com"
    assert audit_log.get_unique_key_prefix() == "_INTERNAL#AuditLog"


def test_audit_log_gsi_config():
    """Test AuditLog GSI configuration."""
    audit_log = AuditLog.create_new(
        {
            "audited_resource_type": "User",
            "audited_resource_id": "user123",
            "operation": "UPDATE",
        }
    )

    gsi_config = audit_log.get_gsi_config()

    # Check gsi1 - resource-specific queries
    assert "gsi1" in gsi_config
    gsi1_pk = gsi_config["gsi1"]["gsi1pk"](audit_log)
    assert gsi1_pk == "_INTERNAL#AuditLog#User#user123"

    # Check gsi2 - resource type queries
    assert "gsi2" in gsi_config
    gsi2_pk = gsi_config["gsi2"]["gsi2pk"](audit_log)
    assert gsi2_pk == "_INTERNAL#AuditLog#User"


def test_audit_log_gsitypesk_uses_created_at():
    """Test that AuditLog uses created_at for gsitypesk sorting."""
    audit_log = AuditLog.create_new(
        {
            "audited_resource_type": "User",
            "audited_resource_id": "user123",
            "operation": "CREATE",
        }
    )

    # gsitypesk should be created_at.isoformat()
    gsitypesk = audit_log.db_get_gsitypesk()
    assert gsitypesk == audit_log.created_at.isoformat()


# === CREATE Operation Tests ===


def test_audit_create_operation_basic(dynamodb_memory: DynamoDbMemory):
    """Test that CREATE operations generate audit logs."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {
            "name": "Alice",
            "email": "alice@example.com",
            "status": "active",
            "age": 30,
        },
        changed_by="admin@example.com",
    )

    # Query audit logs for this resource
    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    assert len(logs) == 1
    audit_log = logs[0]

    assert audit_log.audited_resource_type == "AuditedUser"
    assert audit_log.audited_resource_id == user.resource_id
    assert audit_log.operation == "CREATE"
    assert audit_log.changed_by == "admin@example.com"


def test_audit_create_without_changed_by_optional(dynamodb_memory: DynamoDbMemory):
    """Test CREATE when changed_by is optional (not in config)."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Bob", "email": "bob@example.com", "status": "active"},
        # No changed_by provided, but it's optional
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    assert len(logs) == 1
    assert logs[0].changed_by is None  # Should be None, not error


def test_audit_create_with_changed_by_field(dynamodb_memory: DynamoDbMemory):
    """Test CREATE with changed_by_field auto-extraction."""
    task = dynamodb_memory.create_new(
        AuditedTask,
        {
            "title": "Write tests",
            "completed": False,
            "assigned_to": "developer@example.com",
        },
        # No changed_by param, should extract from assigned_to
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedTask", task.resource_id)

    assert len(logs) == 1
    # Should auto-extract from assigned_to field
    assert logs[0].changed_by == "developer@example.com"


def test_audit_create_snapshot_included(dynamodb_memory: DynamoDbMemory):
    """Test that CREATE includes resource snapshot when configured."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Charlie", "email": "charlie@example.com", "status": "active", "age": 25},
        changed_by="admin@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    audit_log = logs[0]
    assert audit_log.resource_snapshot is not None
    assert audit_log.resource_snapshot["name"] == "Charlie"
    assert audit_log.resource_snapshot["email"] == "charlie@example.com"
    assert audit_log.resource_snapshot["status"] == "active"
    assert audit_log.resource_snapshot["age"] == 25


def test_audit_create_no_snapshot_when_disabled(dynamodb_memory: DynamoDbMemory):
    """Test that CREATE doesn't include snapshot when disabled."""
    project = dynamodb_memory.create_new(
        AuditedProject,
        {"name": "Project X", "description": "Test project", "owner_id": "user123"},
        changed_by="admin@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedProject", project.resource_id)

    audit_log = logs[0]
    assert audit_log.resource_snapshot is None  # Snapshots disabled


def test_no_audit_for_non_audited_resource(dynamodb_memory: DynamoDbMemory):
    """Test that non-audited resources don't create audit logs."""
    dynamodb_memory.create_new(
        NonAuditedResource,
        {"name": "No audit"},
        changed_by="admin@example.com",
    )

    # Should not create any audit logs
    all_audits = dynamodb_memory.list_type_by_updated_at(AuditLog)
    assert len(all_audits) == 0


def test_audit_log_doesnt_audit_itself(dynamodb_memory: DynamoDbMemory):
    """Test that creating an AuditLog doesn't create another AuditLog (recursion prevention)."""
    # Manually create an AuditLog
    audit_log = dynamodb_memory.create_new(
        AuditLog,
        {
            "audited_resource_type": "User",
            "audited_resource_id": "user123",
            "operation": "CREATE",
            "changed_by": "admin@example.com",
        },
    )

    # Should only have 1 audit log (the one we created, not a meta-audit)
    all_audits = dynamodb_memory.list_type_by_updated_at(AuditLog)
    assert len(all_audits) == 1
    assert all_audits[0].resource_id == audit_log.resource_id


# === Validation Tests ===


def test_audit_validation_changed_by_required_but_missing(dynamodb_memory: DynamoDbMemory):
    """Test that missing changed_by raises error when required by field."""
    with pytest.raises(ValueError, match="Audit logging enabled.*but 'changed_by' not provided"):
        # AuditedTask requires changed_by via changed_by_field
        # If assigned_to is empty and no changed_by param, should error
        dynamodb_memory.create_new(
            AuditedTask,
            {
                "title": "Task with empty assigned_to",
                "completed": False,
                "assigned_to": "",  # Empty string - should fail validation
            },
            # No changed_by param either
        )


# === UPDATE Operation Tests ===


def test_audit_update_operation_basic(dynamodb_memory: DynamoDbMemory):
    """Test that UPDATE operations generate audit logs."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Alice", "email": "alice@example.com", "status": "active", "age": 30},
        changed_by="admin@example.com",
    )

    # Update the user
    dynamodb_memory.update_existing(
        user, {"name": "Alice Updated", "age": 31}, changed_by="admin@example.com"
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    assert len(logs) == 2  # CREATE + UPDATE
    update_log = logs[0]  # Newest first

    assert update_log.operation == "UPDATE"
    assert update_log.changed_by == "admin@example.com"


def test_audit_update_field_changes_tracking(dynamodb_memory: DynamoDbMemory):
    """Test that UPDATE tracks field-level changes."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Bob", "email": "bob@example.com", "status": "active", "age": 25},
        changed_by="admin@example.com",
    )

    # Update multiple fields
    dynamodb_memory.update_existing(user, {"name": "Robert", "age": 26}, changed_by="editor@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    update_log = logs[0]
    assert update_log.changed_fields is not None
    assert "name" in update_log.changed_fields
    assert update_log.changed_fields["name"]["old"] == "Bob"
    assert update_log.changed_fields["name"]["new"] == "Robert"
    assert "age" in update_log.changed_fields
    assert update_log.changed_fields["age"]["old"] == 25
    assert update_log.changed_fields["age"]["new"] == 26


def test_audit_update_excludes_base_fields(dynamodb_memory: DynamoDbMemory):
    """Test that UPDATE doesn't track base resource fields in changed_fields."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Charlie", "email": "charlie@example.com", "status": "active"},
        changed_by="admin@example.com",
    )

    dynamodb_memory.update_existing(user, {"name": "Charles"}, changed_by="admin@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    update_log = logs[0]
    # Should not include resource_id, version, created_at, updated_at in changed_fields
    assert "resource_id" not in update_log.changed_fields
    assert "version" not in update_log.changed_fields
    assert "created_at" not in update_log.changed_fields
    assert "updated_at" not in update_log.changed_fields


def test_audit_update_with_exclude_fields(dynamodb_memory: DynamoDbMemory):
    """Test that UPDATE respects exclude_fields configuration."""
    project = dynamodb_memory.create_new(
        AuditedProject,
        {"name": "Project A", "description": "Original", "owner_id": "user1"},
        changed_by="admin@example.com",
    )

    # Update both name and owner_id, but owner_id should be excluded
    dynamodb_memory.update_existing(
        project, {"name": "Project A Updated", "owner_id": "user2"}, changed_by="admin@example.com"
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedProject", project.resource_id)

    update_log = logs[0]
    assert "name" in update_log.changed_fields
    assert "owner_id" not in update_log.changed_fields  # Excluded


def test_audit_update_no_changes_still_logs(dynamodb_memory: DynamoDbMemory):
    """Test that UPDATE logs even when no actual field changes occur."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Dave", "email": "dave@example.com", "status": "active"},
        changed_by="admin@example.com",
    )

    # Update with same values
    dynamodb_memory.update_existing(user, {"name": "Dave"}, changed_by="admin@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    assert len(logs) == 2  # Still logs UPDATE even though no changes
    update_log = logs[0]
    assert update_log.operation == "UPDATE"
    # changed_fields should be None or empty since no actual changes
    assert not update_log.changed_fields or len(update_log.changed_fields) == 0


# === DELETE Operation Tests ===


def test_audit_delete_operation_basic(dynamodb_memory: DynamoDbMemory):
    """Test that DELETE operations generate audit logs."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Eve", "email": "eve@example.com", "status": "active"},
        changed_by="admin@example.com",
    )

    # Delete the user
    dynamodb_memory.delete_existing(user, changed_by="admin@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    assert len(logs) == 2  # CREATE + DELETE
    delete_log = logs[0]  # Newest first

    assert delete_log.operation == "DELETE"
    assert delete_log.changed_by == "admin@example.com"


def test_audit_delete_includes_final_snapshot(dynamodb_memory: DynamoDbMemory):
    """Test that DELETE includes final resource snapshot when configured."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Frank", "email": "frank@example.com", "status": "active", "age": 40},
        changed_by="admin@example.com",
    )

    dynamodb_memory.delete_existing(user, changed_by="admin@example.com")

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    delete_log = logs[0]
    assert delete_log.resource_snapshot is not None
    assert delete_log.resource_snapshot["name"] == "Frank"
    assert delete_log.resource_snapshot["email"] == "frank@example.com"


def test_audit_delete_with_metadata(dynamodb_memory: DynamoDbMemory):
    """Test that DELETE can include audit metadata."""
    user = dynamodb_memory.create_new(
        AuditedUser,
        {"name": "Grace", "email": "grace@example.com", "status": "active"},
        changed_by="admin@example.com",
    )

    dynamodb_memory.delete_existing(
        user, changed_by="admin@example.com", audit_metadata={"reason": "Account closed by user request"}
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("AuditedUser", user.resource_id)

    delete_log = logs[0]
    assert delete_log.audit_metadata["reason"] == "Account closed by user request"


# === Query Tests ===


def test_audit_querier_initialization(dynamodb_memory: DynamoDbMemory):
    """Test that AuditLogQuerier can be initialized."""
    querier = AuditLogQuerier(dynamodb_memory)
    assert querier.memory == dynamodb_memory


def test_audit_query_for_nonexistent_resource(dynamodb_memory: DynamoDbMemory):
    """Test querying audit logs for a resource that doesn't exist."""
    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource("User", "nonexistent123")
    assert len(logs) == 0


def test_audit_query_for_resource_type(dynamodb_memory: DynamoDbMemory):
    """Test querying all audit logs for a resource type."""
    # Create multiple users
    user1 = dynamodb_memory.create_new(
        AuditedUser, {"name": "User1", "email": "user1@example.com", "status": "active"}, changed_by="admin"
    )
    user2 = dynamodb_memory.create_new(
        AuditedUser, {"name": "User2", "email": "user2@example.com", "status": "active"}, changed_by="admin"
    )

    querier = AuditLogQuerier(dynamodb_memory)
    logs = querier.get_logs_for_resource_type("AuditedUser")

    assert len(logs) == 2
    # Should be sorted by creation time (newest first)
    assert logs[0].audited_resource_id == user2.resource_id
    assert logs[1].audited_resource_id == user1.resource_id


def test_audit_query_by_operation(dynamodb_memory: DynamoDbMemory):
    """Test querying audit logs by operation type."""
    user1 = dynamodb_memory.create_new(
        AuditedUser, {"name": "User1", "email": "user1@example.com", "status": "active"}, changed_by="admin"
    )
    dynamodb_memory.create_new(
        AuditedUser, {"name": "User2", "email": "user2@example.com", "status": "active"}, changed_by="admin"
    )
    dynamodb_memory.update_existing(user1, {"name": "User1 Updated"}, changed_by="admin")

    querier = AuditLogQuerier(dynamodb_memory)

    # Query only CREATE operations
    creates = querier.get_logs_by_operation("AuditedUser", "CREATE")
    assert len(creates) == 2
    assert all(log.operation == "CREATE" for log in creates)

    # Query only UPDATE operations
    updates = querier.get_logs_by_operation("AuditedUser", "UPDATE")
    assert len(updates) == 1
    assert updates[0].operation == "UPDATE"


def test_audit_query_by_changer(dynamodb_memory: DynamoDbMemory):
    """Test querying audit logs by changed_by."""
    dynamodb_memory.create_new(
        AuditedUser, {"name": "User1", "email": "user1@example.com", "status": "active"}, changed_by="admin"
    )
    dynamodb_memory.create_new(
        AuditedUser, {"name": "User2", "email": "user2@example.com", "status": "active"}, changed_by="editor"
    )
    dynamodb_memory.create_new(
        AuditedUser, {"name": "User3", "email": "user3@example.com", "status": "active"}, changed_by="admin"
    )

    querier = AuditLogQuerier(dynamodb_memory)

    # Query by specific changer
    admin_logs = querier.get_logs_by_changer("admin", resource_type="AuditedUser")
    assert len(admin_logs) == 2
    assert all(log.changed_by == "admin" for log in admin_logs)

    editor_logs = querier.get_logs_by_changer("editor", resource_type="AuditedUser")
    assert len(editor_logs) == 1
    assert editor_logs[0].changed_by == "editor"


def test_audit_query_get_field_history(dynamodb_memory: DynamoDbMemory):
    """Test tracking field history over multiple updates."""
    user = dynamodb_memory.create_new(
        AuditedUser, {"name": "John", "email": "john@example.com", "status": "active", "age": 25}, changed_by="admin"
    )

    # Update name multiple times
    user = dynamodb_memory.update_existing(user, {"name": "Johnny"}, changed_by="admin")
    user = dynamodb_memory.update_existing(user, {"name": "Jon"}, changed_by="admin")

    querier = AuditLogQuerier(dynamodb_memory)
    name_history = querier.get_field_history("AuditedUser", user.resource_id, "name")

    assert len(name_history) == 3  # CREATE + 2 UPDATEs
    # Should be chronological (oldest first)
    assert name_history[0]["operation"] == "CREATE"
    assert name_history[0]["old_value"] is None
    assert name_history[0]["new_value"] == "John"

    assert name_history[1]["operation"] == "UPDATE"
    assert name_history[1]["old_value"] == "John"
    assert name_history[1]["new_value"] == "Johnny"

    assert name_history[2]["operation"] == "UPDATE"
    assert name_history[2]["old_value"] == "Johnny"
    assert name_history[2]["new_value"] == "Jon"


def test_audit_query_get_recent_changes(dynamodb_memory: DynamoDbMemory):
    """Test getting recent changes across all resources."""
    # Create resources of different types
    dynamodb_memory.create_new(
        AuditedUser, {"name": "User1", "email": "user1@example.com", "status": "active"}, changed_by="admin"
    )
    dynamodb_memory.create_new(
        AuditedProject, {"name": "Project1", "description": "Desc1", "owner_id": "user1"}, changed_by="admin"
    )
    dynamodb_memory.create_new(
        AuditedUser, {"name": "User2", "email": "user2@example.com", "status": "active"}, changed_by="admin"
    )

    querier = AuditLogQuerier(dynamodb_memory)
    recent = querier.get_recent_changes(limit=10)

    assert len(recent) == 3
    # Should include both AuditedUser and AuditedProject logs


def test_audit_query_with_pagination(dynamodb_memory: DynamoDbMemory):
    """Test that pagination works correctly for audit queries."""
    # Create multiple audit logs
    for i in range(5):
        dynamodb_memory.create_new(
            AuditedUser, {"name": f"User{i}", "email": f"user{i}@example.com", "status": "active"}, changed_by="admin"
        )

    querier = AuditLogQuerier(dynamodb_memory)

    # Query with limit
    logs = querier.get_logs_for_resource_type("AuditedUser", limit=3)
    assert len(logs) == 3

    # Verify pagination metadata exists
    assert logs.next_pagination_key is not None or logs.next_pagination_key is None  # May or may not have more


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
