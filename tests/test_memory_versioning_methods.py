"""Tests for DynamoDbMemory versioning methods: get_all_versions and restore_version.

These tests verify the core versioning functionality added to DynamoDbMemory
to make version management a first-class feature of the library.
"""

from typing import Optional

import pytest

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource


class VersionedDocument(DynamoDbVersionedResource):
    """Test versioned document model."""

    title: str
    content: str
    status: str = "draft"


class NonVersionedResource(DynamoDbResource):
    """Test non-versioned resource for error cases."""

    name: str


class TestGetAllVersions:
    """Test suite for get_all_versions method."""

    def test_get_all_versions_basic(self, dynamodb_memory):
        """Test getting all versions of a resource."""
        memory = dynamodb_memory

        # Create a document
        doc = memory.create_new(VersionedDocument, {"title": "Test Doc", "content": "v1", "status": "draft"})
        doc_id = doc.resource_id

        # Get versions - should have 1
        versions = memory.get_all_versions(doc_id, VersionedDocument)
        assert len(versions) == 1
        assert versions[0].version == 1
        assert versions[0].content == "v1"

    def test_get_all_versions_multiple(self, dynamodb_memory):
        """Test getting multiple versions after updates."""
        memory = dynamodb_memory

        # Create and update document multiple times
        doc = memory.create_new(VersionedDocument, {"title": "Multi-version", "content": "v1"})
        doc_id = doc.resource_id

        doc = memory.update_existing(doc, {"content": "v2"})
        assert doc.version == 2

        doc = memory.update_existing(doc, {"content": "v3", "status": "published"})
        assert doc.version == 3

        doc = memory.update_existing(doc, {"content": "v4"})
        assert doc.version == 4

        # Get all versions
        versions = memory.get_all_versions(doc_id, VersionedDocument)
        assert len(versions) == 4

        # Check ordering (newest first)
        assert versions[0].version == 4
        assert versions[0].content == "v4"
        assert versions[1].version == 3
        assert versions[1].content == "v3"
        assert versions[1].status == "published"
        assert versions[2].version == 2
        assert versions[2].content == "v2"
        assert versions[3].version == 1
        assert versions[3].content == "v1"

    def test_get_all_versions_preserves_all_fields(self, dynamodb_memory):
        """Test that all fields are preserved in version history."""
        memory = dynamodb_memory

        # Create document with all fields
        doc = memory.create_new(
            VersionedDocument, {"title": "Complete Doc", "content": "Full content here", "status": "review"}
        )
        doc_id = doc.resource_id

        # Update to create v2
        doc = memory.update_existing(doc, {"title": "Updated Title", "status": "published"})

        # Get all versions and verify fields
        versions = memory.get_all_versions(doc_id, VersionedDocument)

        # v2 (latest)
        assert versions[0].version == 2
        assert versions[0].title == "Updated Title"
        assert versions[0].content == "Full content here"  # Unchanged
        assert versions[0].status == "published"

        # v1 (original)
        assert versions[1].version == 1
        assert versions[1].title == "Complete Doc"
        assert versions[1].content == "Full content here"
        assert versions[1].status == "review"

    def test_get_all_versions_non_existent_resource(self, dynamodb_memory):
        """Test getting versions for non-existent resource returns empty list."""
        memory = dynamodb_memory

        versions = memory.get_all_versions("non-existent-id", VersionedDocument)
        assert versions == []

    def test_get_all_versions_requires_versioned_resource(self, dynamodb_memory):
        """Test that get_all_versions raises error for non-versioned resources."""
        memory = dynamodb_memory

        # Create a non-versioned resource
        resource = memory.create_new(NonVersionedResource, {"name": "Test"})

        # Try to get versions - should fail
        with pytest.raises(ValueError, match="can only be used with versioned resources"):
            memory.get_all_versions(resource.resource_id, NonVersionedResource)

    def test_get_all_versions_double_digit_versions(self, dynamodb_memory):
        """Test handling of double-digit version numbers."""
        memory = dynamodb_memory

        # Create document and make many updates
        doc = memory.create_new(VersionedDocument, {"title": "Test", "content": "v1"})
        doc_id = doc.resource_id

        # Create versions up to v12
        for i in range(2, 13):
            doc = memory.update_existing(doc, {"content": f"v{i}"})

        # Get all versions
        versions = memory.get_all_versions(doc_id, VersionedDocument)
        assert len(versions) == 12

        # Verify correct numeric ordering (not lexicographic)
        version_numbers = [v.version for v in versions]
        assert version_numbers == list(range(12, 0, -1))  # [12, 11, 10, ..., 2, 1]

        # Verify content
        assert versions[0].content == "v12"
        assert versions[1].content == "v11"
        assert versions[2].content == "v10"

    def test_get_all_versions_with_load_blobs_false(self, dynamodb_memory_with_s3):
        """Test that load_blobs=False doesn't load blob fields."""
        memory = dynamodb_memory_with_s3

        # Create a versioned resource with blob fields
        from simplesingletable.models import BlobFieldConfig, ResourceConfig
        from typing import ClassVar

        class DocWithBlob(DynamoDbVersionedResource):
            resource_config: ClassVar[ResourceConfig] = ResourceConfig(
                blob_fields={"large_content": BlobFieldConfig(compress=True, content_type="text/plain")}
            )

            title: str
            large_content: Optional[str] = None

        doc = memory.create_new(
            DocWithBlob, {"title": "Doc with Blob", "large_content": "Large content here" * 100}
        )

        # Get versions without loading blobs
        versions = memory.get_all_versions(doc.resource_id, DocWithBlob, load_blobs=False)
        assert len(versions) == 1
        assert versions[0].has_unloaded_blobs()

    def test_get_all_versions_with_load_blobs_true(self, dynamodb_memory_with_s3):
        """Test that load_blobs=True loads blob fields."""
        memory = dynamodb_memory_with_s3

        # Create a versioned resource with blob fields
        from simplesingletable.models import BlobFieldConfig, ResourceConfig
        from typing import ClassVar

        class DocWithBlob(DynamoDbVersionedResource):
            resource_config: ClassVar[ResourceConfig] = ResourceConfig(
                blob_fields={"large_content": BlobFieldConfig(compress=True, content_type="text/plain")}
            )

            title: str
            large_content: Optional[str] = None

        large_text = "Large content here" * 100
        doc = memory.create_new(DocWithBlob, {"title": "Doc with Blob", "large_content": large_text})

        # Get versions with loading blobs
        versions = memory.get_all_versions(doc.resource_id, DocWithBlob, load_blobs=True)
        assert len(versions) == 1
        assert not versions[0].has_unloaded_blobs()
        assert versions[0].large_content == large_text


class TestRestoreVersion:
    """Test suite for restore_version method."""

    def test_restore_version_basic(self, dynamodb_memory):
        """Test basic version restoration."""
        memory = dynamodb_memory

        # Create document with multiple versions
        doc = memory.create_new(VersionedDocument, {"title": "Original", "content": "v1 content"})
        doc_id = doc.resource_id

        # Update to v2
        doc = memory.update_existing(doc, {"title": "Updated", "content": "v2 content"})

        # Update to v3
        doc = memory.update_existing(doc, {"content": "v3 content"})

        # Restore v1
        restored = memory.restore_version(doc_id, VersionedDocument, 1)
        assert restored.version == 4  # New version created
        assert restored.title == "Original"
        assert restored.content == "v1 content"

        # Verify all versions exist
        versions = memory.get_all_versions(doc_id, VersionedDocument)
        assert len(versions) == 4

    def test_restore_version_preserves_all_fields(self, dynamodb_memory):
        """Test that restoration preserves all fields from target version."""
        memory = dynamodb_memory

        # Create with all fields set
        doc = memory.create_new(
            VersionedDocument, {"title": "Original Title", "content": "Original content", "status": "draft"}
        )
        doc_id = doc.resource_id

        # Update to v2 (change all fields)
        doc = memory.update_existing(doc, {"title": "V2 Title", "content": "V2 content", "status": "review"})

        # Update to v3 (change all fields again)
        doc = memory.update_existing(doc, {"title": "V3 Title", "content": "V3 content", "status": "published"})

        # Restore v1 - should get original state
        restored = memory.restore_version(doc_id, VersionedDocument, 1)
        assert restored.version == 4
        assert restored.title == "Original Title"
        assert restored.content == "Original content"
        assert restored.status == "draft"

        # Restore v2 - should get v2 state
        restored2 = memory.restore_version(doc_id, VersionedDocument, 2)
        assert restored2.version == 5
        assert restored2.title == "V2 Title"
        assert restored2.content == "V2 content"
        assert restored2.status == "review"

    def test_restore_version_not_found(self, dynamodb_memory):
        """Test restoring a non-existent version."""
        memory = dynamodb_memory

        doc = memory.create_new(VersionedDocument, {"title": "Test", "content": "Test"})

        with pytest.raises(ValueError, match="Version 99 not found"):
            memory.restore_version(doc.resource_id, VersionedDocument, 99)

    def test_restore_version_resource_not_found(self, dynamodb_memory):
        """Test restoring version for non-existent resource."""
        memory = dynamodb_memory

        with pytest.raises(ValueError, match="Version 1 not found"):
            memory.restore_version("non-existent-id", VersionedDocument, 1)

    def test_restore_version_invalid_version_number(self, dynamodb_memory):
        """Test that invalid version numbers are rejected."""
        memory = dynamodb_memory

        doc = memory.create_new(VersionedDocument, {"title": "Test", "content": "Test"})

        # Zero is invalid
        with pytest.raises(ValueError, match="Version must be a positive integer"):
            memory.restore_version(doc.resource_id, VersionedDocument, 0)

        # Negative is invalid
        with pytest.raises(ValueError, match="Version must be a positive integer"):
            memory.restore_version(doc.resource_id, VersionedDocument, -1)

    def test_restore_version_requires_versioned_resource(self, dynamodb_memory):
        """Test that restore_version raises error for non-versioned resources."""
        memory = dynamodb_memory

        resource = memory.create_new(NonVersionedResource, {"name": "Test"})

        with pytest.raises(ValueError, match="can only be used with versioned resources"):
            memory.restore_version(resource.resource_id, NonVersionedResource, 1)

    def test_restore_version_with_changed_by(self, dynamodb_memory):
        """Test that changed_by is passed through for audit logging."""
        memory = dynamodb_memory

        # Create document with audit logging
        from simplesingletable.models import AuditConfig, ResourceConfig
        from typing import ClassVar

        class AuditedDoc(DynamoDbVersionedResource):
            resource_config: ClassVar[ResourceConfig] = ResourceConfig(
                audit_config=AuditConfig(enabled=True, track_field_changes=True)
            )

            title: str
            content: str

        # Create and update
        doc = memory.create_new(AuditedDoc, {"title": "Test", "content": "v1"}, changed_by="creator")
        doc_id = doc.resource_id
        memory.update_existing(doc, {"content": "v2"}, changed_by="updater")

        # Restore with changed_by
        restored = memory.restore_version(doc_id, AuditedDoc, 1, changed_by="restorer")
        assert restored.version == 3
        assert restored.content == "v1"

        # Verify audit log was created
        from simplesingletable import AuditLogQuerier

        querier = AuditLogQuerier(memory)
        logs = querier.get_logs_for_resource("AuditedDoc", doc_id)

        # Should have CREATE, UPDATE, and UPDATE (from restore)
        assert len(logs) >= 3
        assert logs.as_list()[0].changed_by == "restorer"  # Most recent
        assert logs.as_list()[0].operation == "UPDATE"

    def test_restore_version_with_blobs(self, dynamodb_memory_with_s3):
        """Test restoring a version with blob fields."""
        memory = dynamodb_memory_with_s3

        from simplesingletable.models import BlobFieldConfig, ResourceConfig
        from typing import ClassVar

        class DocWithBlob(DynamoDbVersionedResource):
            resource_config: ClassVar[ResourceConfig] = ResourceConfig(
                blob_fields={"large_content": BlobFieldConfig(compress=True, content_type="text/plain")}
            )

            title: str
            large_content: Optional[str] = None

        # Create with blob
        v1_content = "Version 1 large content" * 100
        doc = memory.create_new(DocWithBlob, {"title": "Doc", "large_content": v1_content})
        doc_id = doc.resource_id

        # Update with different blob content
        v2_content = "Version 2 large content" * 100
        doc = memory.update_existing(doc, {"large_content": v2_content})

        # Restore v1 - should restore blob content
        restored = memory.restore_version(doc_id, DocWithBlob, 1)
        assert restored.version == 3

        # Load blobs and verify
        restored = memory.get_existing(doc_id, DocWithBlob, version=0, load_blobs=True)
        assert restored.large_content == v1_content

    def test_restore_creates_new_version_not_rollback(self, dynamodb_memory):
        """Test that restore creates a new version rather than rolling back."""
        memory = dynamodb_memory

        # Create v1, v2, v3
        doc = memory.create_new(VersionedDocument, {"title": "v1", "content": "content v1"})
        doc_id = doc.resource_id
        doc = memory.update_existing(doc, {"title": "v2"})
        doc = memory.update_existing(doc, {"title": "v3"})

        # Restore v1
        restored = memory.restore_version(doc_id, VersionedDocument, 1)

        # Should be v4, not v1
        assert restored.version == 4
        assert restored.title == "v1"

        # All previous versions should still exist
        versions = memory.get_all_versions(doc_id, VersionedDocument)
        assert len(versions) == 4
        assert [v.version for v in versions] == [4, 3, 2, 1]

        # v3 should still exist with its data
        v3 = memory.get_existing(doc_id, VersionedDocument, version=3)
        assert v3.title == "v3"


class TestVersioningEdgeCases:
    """Test edge cases for versioning methods."""

    def test_get_all_versions_after_delete_versions(self, dynamodb_memory):
        """Test get_all_versions after deleting some versions."""
        memory = dynamodb_memory

        # Create multiple versions
        doc = memory.create_new(VersionedDocument, {"title": "Test", "content": "v1"})
        doc_id = doc.resource_id
        for i in range(2, 6):
            doc = memory.update_existing(doc, {"content": f"v{i}"})

        # Delete all versions
        memory.delete_all_versions(doc_id, VersionedDocument)

        # Should return empty list
        versions = memory.get_all_versions(doc_id, VersionedDocument)
        assert versions == []

    def test_restore_then_get_all_versions(self, dynamodb_memory):
        """Test that get_all_versions works correctly after restore."""
        memory = dynamodb_memory

        # Create 3 versions
        doc = memory.create_new(VersionedDocument, {"title": "Test", "content": "v1"})
        doc_id = doc.resource_id
        doc = memory.update_existing(doc, {"content": "v2"})
        doc = memory.update_existing(doc, {"content": "v3"})

        # Restore v1 (creates v4)
        memory.restore_version(doc_id, VersionedDocument, 1)

        # Get all versions
        versions = memory.get_all_versions(doc_id, VersionedDocument)
        assert len(versions) == 4
        assert versions[0].version == 4
        assert versions[0].content == "v1"  # Restored content
        assert versions[1].version == 3
        assert versions[2].version == 2
        assert versions[3].version == 1
