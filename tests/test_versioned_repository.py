"""Tests for the VersionedResourceRepository class."""

from datetime import datetime
from typing import Optional

import pytest
from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource
from simplesingletable.extras.versioned_repository import VersionedResourceRepository, VersionInfo


class Document(DynamoDbVersionedResource):
    """Test versioned document model."""

    title: str
    content: str
    tags: Optional[list[str]] = None


class CreateDocumentSchema(BaseModel):
    """Schema for creating documents."""

    title: str
    content: str
    tags: Optional[list[str]] = None


class UpdateDocumentSchema(BaseModel):
    """Schema for updating documents."""

    title: Optional[str] = None
    content: Optional[str] = None
    tags: Optional[list[str]] = None


@pytest.fixture
def memory(dynamodb_memory):
    """Create a DynamoDbMemory instance."""
    return dynamodb_memory


@pytest.fixture
def doc_repo(memory):
    """Create a VersionedResourceRepository for testing."""
    return VersionedResourceRepository(
        ddb=memory,
        model_class=Document,
        create_schema_class=CreateDocumentSchema,
        update_schema_class=UpdateDocumentSchema,
    )


class TestVersionedResourceRepository:
    """Test suite for VersionedResourceRepository."""

    def test_init_requires_versioned_resource(self, memory):
        """Test that repository requires a versioned resource model."""
        from simplesingletable import DynamoDbResource

        class NonVersionedModel(DynamoDbResource):
            name: str

        with pytest.raises(ValueError, match="can only be used with DynamoDbVersionedResource"):
            VersionedResourceRepository(
                ddb=memory,
                model_class=NonVersionedModel,
                create_schema_class=BaseModel,
                update_schema_class=BaseModel,
            )

    def test_create_and_list_versions(self, doc_repo):
        """Test creating a document and listing its versions."""
        # Create initial document
        doc = doc_repo.create({"title": "Test Document", "content": "Initial content", "tags": ["test", "initial"]})

        assert doc.version == 1
        assert doc.title == "Test Document"

        # List versions - should have one version
        versions = doc_repo.list_versions(doc.resource_id)
        assert len(versions) == 1
        assert versions[0].version_id == "v1"
        assert versions[0].version_number == 1
        assert versions[0].is_latest is True

    def test_multiple_versions(self, doc_repo):
        """Test creating multiple versions through updates."""
        # Create and update document multiple times
        doc = doc_repo.create({"title": "Multi-version Doc", "content": "Version 1"})
        doc_id = doc.resource_id

        doc = doc_repo.update(doc_id, {"content": "Version 2"})
        assert doc.version == 2

        doc = doc_repo.update(doc_id, {"content": "Version 3", "title": "Updated Title"})
        assert doc.version == 3

        # List all versions
        versions = doc_repo.list_versions(doc_id)
        assert len(versions) == 3

        # Check version ordering (newest first)
        assert versions[0].version_number == 3
        assert versions[0].is_latest is True
        assert versions[1].version_number == 2
        assert versions[1].is_latest is False
        assert versions[2].version_number == 1
        assert versions[2].is_latest is False

    def test_get_specific_version(self, doc_repo):
        """Test retrieving specific versions."""
        # Create document with multiple versions
        doc = doc_repo.create({"title": "Versioned Doc", "content": "Content v1"})
        doc_id = doc.resource_id

        doc_repo.update(doc_id, {"content": "Content v2"})
        doc_repo.update(doc_id, {"content": "Content v3", "title": "New Title"})

        # Get specific versions
        v1 = doc_repo.get_version(doc_id, 1)
        assert v1.version == 1
        assert v1.content == "Content v1"
        assert v1.title == "Versioned Doc"

        v2 = doc_repo.get_version(doc_id, 2)
        assert v2.version == 2
        assert v2.content == "Content v2"
        assert v2.title == "Versioned Doc"

        v3 = doc_repo.get_version(doc_id, 3)
        assert v3.version == 3
        assert v3.content == "Content v3"
        assert v3.title == "New Title"

    def test_get_version_not_found(self, doc_repo):
        """Test getting a non-existent version."""
        doc = doc_repo.create({"title": "Test", "content": "Test"})

        # Try to get non-existent version
        result = doc_repo.get_version(doc.resource_id, 99)
        assert result is None

    def test_get_version_invalid_format(self, doc_repo):
        """Test getting version with invalid format."""
        doc = doc_repo.create({"title": "Test", "content": "Test"})

        # Invalid version number (zero or negative)
        with pytest.raises(ValueError, match="Version must be a positive integer"):
            doc_repo.get_version(doc.resource_id, 0)

        with pytest.raises(ValueError, match="Version must be a positive integer"):
            doc_repo.get_version(doc.resource_id, -1)

    def test_restore_version(self, doc_repo):
        """Test restoring a previous version."""
        # Create document with multiple versions
        doc = doc_repo.create({"title": "Original Title", "content": "Original content", "tags": ["original"]})
        doc_id = doc.resource_id

        # Update to v2
        doc_repo.update(doc_id, {"title": "Updated Title", "content": "Updated content", "tags": ["updated", "v2"]})

        # Update to v3
        doc_repo.update(doc_id, {"content": "Latest content", "tags": ["latest", "v3"]})

        # Verify current state
        current = doc_repo.get(doc_id)
        assert current.version == 3
        assert current.title == "Updated Title"
        assert current.content == "Latest content"

        # Restore v1
        restored = doc_repo.restore_version(doc_id, 1)
        assert restored.version == 4  # New version created
        assert restored.title == "Original Title"
        assert restored.content == "Original content"
        assert restored.tags == ["original"]

        # Verify the restoration created a new version
        versions = doc_repo.list_versions(doc_id)
        assert len(versions) == 4
        assert versions[0].version_number == 4
        assert versions[0].is_latest is True

    def test_restore_version_not_found(self, doc_repo):
        """Test restoring a non-existent version."""
        doc = doc_repo.create({"title": "Test", "content": "Test"})

        with pytest.raises(ValueError, match="Version 99 not found"):
            doc_repo.restore_version(doc.resource_id, 99)

    def test_restore_version_item_not_found(self, doc_repo):
        """Test restoring version for non-existent item."""
        with pytest.raises(ValueError, match="Version 1 not found for item non-existent-id"):
            doc_repo.restore_version("non-existent-id", 1)

    def test_version_info_model(self):
        """Test the VersionInfo model."""
        now = datetime.now()
        info = VersionInfo(version_id="v5", version_number=5, created_at=now, updated_at=now, is_latest=True)

        assert info.version_id == "v5"
        assert info.version_number == 5
        assert info.is_latest is True

    def test_list_versions_empty(self, doc_repo):
        """Test listing versions for non-existent item."""
        versions = doc_repo.list_versions("non-existent-id")
        assert versions == []

    def test_double_digit_versions(self, doc_repo):
        """Test handling of double-digit version numbers."""
        # Create document and make many updates to get double-digit versions
        doc = doc_repo.create({"title": "Test", "content": "v1"})
        doc_id = doc.resource_id

        # Create versions up to v12
        for i in range(2, 13):
            doc_repo.update(doc_id, {"content": f"v{i}"})

        # List versions and verify ordering
        versions = doc_repo.list_versions(doc_id)
        assert len(versions) == 12

        # Verify correct numeric ordering (not lexicographic)
        version_numbers = [v.version_number for v in versions]
        assert version_numbers == list(range(12, 0, -1))  # [12, 11, 10, ..., 2, 1]

        # Verify we can get double-digit versions
        v10 = doc_repo.get_version(doc_id, 10)
        assert v10.version == 10
        assert v10.content == "v10"

        v11 = doc_repo.get_version(doc_id, 11)
        assert v11.version == 11
        assert v11.content == "v11"
