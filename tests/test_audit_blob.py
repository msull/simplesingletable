"""Tests for audit logging with blob fields (S3-stored data)."""

from typing import ClassVar, Optional

from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbResource
from simplesingletable.extras.audit import AuditLogQuerier
from simplesingletable.models import AuditConfig, BlobFieldConfig, ResourceConfig


class ImageMetadata(BaseModel):
    """Example nested Pydantic model for testing."""

    width: int
    height: int
    format: str


class AuditedDocument(DynamoDbResource):
    """Test resource with blob fields and audit logging enabled."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "attachment": BlobFieldConfig(compress=False, content_type="application/octet-stream"),
        },
    )

    title: str
    description: Optional[str] = None
    content: Optional[str] = None  # Large text content stored in S3
    attachment: Optional[bytes] = None
    metadata: Optional[ImageMetadata] = None


class AuditedDocumentWithExclusions(DynamoDbResource):
    """Test resource that excludes blob fields from audit tracking."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            exclude_fields={"content", "attachment"},
        ),
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "attachment": BlobFieldConfig(compress=False, content_type="application/octet-stream"),
        },
    )

    title: str
    content: Optional[str] = None
    attachment: Optional[bytes] = None


# ============================================================================
# CREATE Operation Tests with Blobs
# ============================================================================


def test_audit_create_with_blob_field(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test CREATE operation captures blob field metadata."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Annual Report",
            "description": "Company annual report",
            "content": "This is a very long document content that will be stored in S3..." * 100,
        },
        changed_by="author@example.com",
    )

    # Retrieve audit log
    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    assert len(logs) == 1
    create_log = logs[0]

    assert create_log.operation == "CREATE"
    assert create_log.changed_by == "author@example.com"

    # Verify snapshot includes blob metadata (not full content)
    assert create_log.resource_snapshot is not None
    assert "content" in create_log.resource_snapshot

    # Blob should be represented as metadata
    content_value = create_log.resource_snapshot["content"]
    assert isinstance(content_value, dict)
    assert "__blob_ref__" in content_value
    assert content_value["__blob_ref__"] is True
    assert "size_bytes" in content_value
    assert content_value["size_bytes"] > 0

    # Full content should NOT be in the audit log
    full_content = "This is a very long document content that will be stored in S3..." * 100
    assert full_content not in str(create_log.resource_snapshot)


def test_audit_create_with_multiple_blob_fields(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test CREATE with multiple blob fields captures all blob metadata."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Report with Attachment",
            "content": "Main document content..." * 50,
            "attachment": b"Binary file content..." * 100,
            "metadata": ImageMetadata(width=1920, height=1080, format="PNG"),
        },
        changed_by="author@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    create_log = logs[0]
    snapshot = create_log.resource_snapshot

    # Both blob fields should be represented as metadata
    assert "content" in snapshot
    assert isinstance(snapshot["content"], dict)
    assert "__blob_ref__" in snapshot["content"]
    assert snapshot["content"]["__blob_ref__"] is True

    assert "attachment" in snapshot
    assert isinstance(snapshot["attachment"], dict)
    assert "__blob_ref__" in snapshot["attachment"]
    assert snapshot["attachment"]["__blob_ref__"] is True

    # Non-blob fields should be captured normally
    assert snapshot["title"] == "Report with Attachment"
    assert snapshot["metadata"]["width"] == 1920


def test_audit_create_blob_no_snapshot(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test that blob metadata is not captured when snapshots are disabled."""

    class AuditedDocNoSnapshot(DynamoDbResource):
        resource_config: ClassVar[ResourceConfig] = ResourceConfig(
            audit_config=AuditConfig(
                enabled=True,
                track_field_changes=True,
                include_snapshot=False,  # Disabled
            ),
            blob_fields={
                "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            },
        )
        title: str
        content: Optional[str] = None

    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocNoSnapshot,
        {"title": "Doc", "content": "Content..." * 100},
        changed_by="author@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocNoSnapshot", doc.resource_id)

    create_log = logs[0]
    assert create_log.resource_snapshot is None


# ============================================================================
# UPDATE Operation Tests with Blobs
# ============================================================================


def test_audit_update_blob_field_changed(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test UPDATE operation tracks blob field changes via metadata."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Original",
            "content": "Original content..." * 50,
        },
        changed_by="author@example.com",
    )

    # Update the blob field
    dynamodb_memory_with_s3.update_existing(
        doc,
        {"content": "Updated content..." * 60},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    # Should have CREATE + UPDATE
    assert len(logs) == 2
    update_log = logs[0]  # Most recent

    assert update_log.operation == "UPDATE"
    assert update_log.changed_by == "editor@example.com"

    # Changed fields should show blob metadata change
    assert "content" in update_log.changed_fields
    content_change = update_log.changed_fields["content"]

    # Old and new values should be blob metadata dicts
    assert isinstance(content_change["old"], dict)
    assert "__blob_ref__" in content_change["old"]
    assert content_change["old"]["__blob_ref__"] is True
    assert isinstance(content_change["new"], dict)
    assert "__blob_ref__" in content_change["new"]
    assert content_change["new"]["__blob_ref__"] is True

    # The blob metadata should be different (different sizes since different content)
    assert content_change["old"]["size_bytes"] != content_change["new"]["size_bytes"]


def test_audit_update_blob_field_unchanged(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test that unchanged blob fields are not tracked in changed_fields."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Original",
            "content": "Original content..." * 50,
        },
        changed_by="author@example.com",
    )

    # Update only non-blob field
    dynamodb_memory_with_s3.update_existing(
        doc,
        {"title": "Updated Title"},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    update_log = logs[0]

    # Only title should be in changed_fields
    assert "title" in update_log.changed_fields
    assert "content" not in update_log.changed_fields


def test_audit_update_blob_excluded_from_tracking(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test that excluded blob fields are not tracked even when changed."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocumentWithExclusions,
        {
            "title": "Doc",
            "content": "Content..." * 50,
        },
        changed_by="author@example.com",
    )

    # Update both title and content
    dynamodb_memory_with_s3.update_existing(
        doc,
        {
            "title": "Updated Doc",
            "content": "New content..." * 60,
        },
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocumentWithExclusions", doc.resource_id)

    update_log = logs[0]

    # Only title should be tracked (content is excluded)
    assert "title" in update_log.changed_fields
    assert "content" not in update_log.changed_fields


def test_audit_update_add_blob_field(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test UPDATE adding an optional blob field."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Doc",
            "content": "Content..." * 50,
            # attachment not set initially
        },
        changed_by="author@example.com",
    )

    # Add attachment
    dynamodb_memory_with_s3.update_existing(
        doc,
        {"attachment": b"New attachment..." * 100},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    update_log = logs[0]

    # Attachment should appear as a change (None -> blob metadata)
    assert "attachment" in update_log.changed_fields
    change = update_log.changed_fields["attachment"]
    assert change["old"] is None
    assert isinstance(change["new"], dict)
    assert "__blob_ref__" in change["new"]
    assert change["new"]["__blob_ref__"] is True


def test_audit_update_remove_blob_field(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test UPDATE removing an optional blob field."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Doc",
            "content": "Content..." * 50,
            "attachment": b"Original attachment..." * 100,
        },
        changed_by="author@example.com",
    )

    # Remove attachment
    dynamodb_memory_with_s3.update_existing(
        doc,
        {"attachment": None},
        changed_by="editor@example.com",
        clear_fields={"attachment"},
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    update_log = logs[0]

    # Attachment should appear as a change (blob metadata -> None)
    assert "attachment" in update_log.changed_fields
    change = update_log.changed_fields["attachment"]
    assert isinstance(change["old"], dict)
    assert "__blob_ref__" in change["old"]
    assert change["old"]["__blob_ref__"] is True
    assert change["new"] is None


# ============================================================================
# DELETE Operation Tests with Blobs
# ============================================================================


def test_audit_delete_with_blob_fields(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test DELETE operation captures blob metadata in final snapshot."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "To Be Deleted",
            "content": "Content..." * 50,
            "attachment": b"Attachment..." * 100,
        },
        changed_by="author@example.com",
    )

    # Delete the document
    dynamodb_memory_with_s3.delete_existing(doc, changed_by="admin@example.com")

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    # Should have CREATE + DELETE
    assert len(logs) == 2
    delete_log = logs[0]

    assert delete_log.operation == "DELETE"
    assert delete_log.changed_by == "admin@example.com"

    # Final snapshot should include blob metadata
    assert delete_log.resource_snapshot is not None
    assert "content" in delete_log.resource_snapshot
    assert isinstance(delete_log.resource_snapshot["content"], dict)
    assert "__blob_ref__" in delete_log.resource_snapshot["content"]
    assert delete_log.resource_snapshot["content"]["__blob_ref__"] is True

    assert "attachment" in delete_log.resource_snapshot
    assert isinstance(delete_log.resource_snapshot["attachment"], dict)
    assert "__blob_ref__" in delete_log.resource_snapshot["attachment"]
    assert delete_log.resource_snapshot["attachment"]["__blob_ref__"] is True


# ============================================================================
# Field History with Blobs
# ============================================================================


def test_audit_blob_field_history(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test get_field_history tracks blob field changes over time."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Doc",
            "content": "Version 1..." * 50,
        },
        changed_by="author@example.com",
    )

    # Update content multiple times
    doc = dynamodb_memory_with_s3.update_existing(
        doc,
        {"content": "Version 2..." * 60},
        changed_by="editor1@example.com",
    )

    dynamodb_memory_with_s3.update_existing(
        doc,
        {"content": "Version 3..." * 70},
        changed_by="editor2@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    history = querier.get_field_history("AuditedDocument", doc.resource_id, "content")

    # Should have CREATE + 2 UPDATEs = 3 changes
    assert len(history) == 3

    # CREATE: None -> blob metadata
    assert history[0]["operation"] == "CREATE"
    assert history[0]["old_value"] is None
    assert isinstance(history[0]["new_value"], dict)
    assert "__blob_ref__" in history[0]["new_value"]
    assert history[0]["new_value"]["__blob_ref__"] is True

    # UPDATE 1: blob metadata -> different blob metadata
    assert history[1]["operation"] == "UPDATE"
    assert isinstance(history[1]["old_value"], dict)
    assert isinstance(history[1]["new_value"], dict)
    assert history[1]["old_value"]["size_bytes"] != history[1]["new_value"]["size_bytes"]

    # UPDATE 2: blob metadata -> different blob metadata
    assert history[2]["operation"] == "UPDATE"
    assert history[1]["new_value"]["size_bytes"] != history[2]["new_value"]["size_bytes"]


# ============================================================================
# Edge Cases
# ============================================================================


def test_audit_blob_field_none_to_none(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test that optional blob field remaining None doesn't appear in changes."""
    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Doc",
            "content": "Content..." * 50,
            # attachment is None
        },
        changed_by="author@example.com",
    )

    # Update without touching attachment
    dynamodb_memory_with_s3.update_existing(
        doc,
        {"title": "Updated"},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    update_log = logs[0]

    # Attachment should not be in changed_fields (None -> None)
    assert "attachment" not in update_log.changed_fields


def test_audit_blob_same_content_no_change(dynamodb_memory_with_s3: DynamoDbMemory):
    """Test that updating a blob field with same content doesn't show as changed."""
    original_content = "Content..." * 50

    doc = dynamodb_memory_with_s3.create_new(
        AuditedDocument,
        {
            "title": "Doc",
            "content": original_content,
        },
        changed_by="author@example.com",
    )

    # Update with same content (won't create new S3 object or show as changed)
    dynamodb_memory_with_s3.update_existing(
        doc,
        {"content": original_content},
        changed_by="editor@example.com",
    )

    querier = AuditLogQuerier(dynamodb_memory_with_s3)
    logs = querier.get_logs_for_resource("AuditedDocument", doc.resource_id)

    # Should have CREATE + UPDATE
    assert len(logs) == 2
    update_log = logs[0]

    # Content should NOT be in changed_fields since it didn't actually change
    assert update_log.changed_fields is None or "content" not in update_log.changed_fields
