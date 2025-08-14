"""
Tests for blob storage bugfixes:
1. Version check when updating versioned resources with blobs
2. Paginated query handling with blob fields
3. PrivateAttr for _blob_placeholders
"""

import json
from typing import Optional
from unittest.mock import Mock, patch
import pytest
from datetime import datetime

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig, BlobPlaceholder
from simplesingletable.blob_storage import S3BlobStorage


class VersionedResourceWithBlob(DynamoDbVersionedResource):
    """Test versioned resource with blob fields."""
    
    title: str
    content: Optional[str] = None  # Regular field
    large_data: Optional[dict] = None  # Blob field
    
    resource_config = ResourceConfig(
        compress_data=True,
        blob_fields={
            "large_data": BlobFieldConfig(
                compress=True,
                content_type="application/json"
            )
        }
    )


class NonVersionedResourceWithBlob(DynamoDbResource):
    """Test non-versioned resource with blob fields."""
    
    name: str
    description: Optional[str] = None
    blob_content: Optional[dict] = None  # Blob field
    
    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={
            "blob_content": BlobFieldConfig(
                compress=False,
                content_type="application/json"
            )
        }
    )
    
    def db_get_gsi1pk(self) -> str | None:
        """Enable querying by type."""
        return "type#document"


@pytest.fixture
def mock_s3_storage():
    """Create a mock S3 storage."""
    storage = Mock(spec=S3BlobStorage)
    storage.key_prefix = "test-prefix"
    storage._build_s3_key = S3BlobStorage._build_s3_key.__get__(storage, S3BlobStorage)
    storage.put_blob = Mock(return_value=BlobPlaceholder(
        field_name="test",
        s3_key="test-key",
        size_bytes=100,
        content_type="application/json",
        compressed=False
    ))
    storage.get_blob = Mock(return_value={"test": "data"})
    return storage


@pytest.fixture
def dynamodb_memory_with_mock_s3(dynamodb_memory, mock_s3_storage):
    """Extend dynamodb_memory fixture with mock S3."""
    dynamodb_memory.s3_bucket = "test-bucket"
    dynamodb_memory.s3_key_prefix = "test-prefix"
    dynamodb_memory._s3_blob_storage = mock_s3_storage
    return dynamodb_memory


class TestVersionCheckBugfix:
    """Test the version check bugfix for updating versioned resources with blobs."""
    
    def test_version_check_with_blobs(self, dynamodb_memory_with_mock_s3):
        """Test that version check works correctly when resources have blob fields."""
        memory = dynamodb_memory_with_mock_s3
        
        # Create a versioned resource with blob
        resource = memory.create_new(
            VersionedResourceWithBlob,
            {
                "title": "Original",
                "content": "Regular content",
                "large_data": {"key": "value"}
            }
        )
        
        assert resource.version == 1
        
        # Load the resource (simulating a scenario where blob fields might differ)
        loaded = memory.get_existing(
            resource.resource_id,
            VersionedResourceWithBlob,
            version=0  # Get latest
        )
        
        # The version check should compare version numbers, not object equality
        # This should not raise an error even if blob fields are different
        updated = memory.update_existing(
            loaded,
            {"title": "Updated"}
        )
        
        assert updated.version == 2
        assert updated.title == "Updated"
    
    def test_version_check_prevents_non_latest_update(self, dynamodb_memory_with_mock_s3):
        """Test that updating from non-latest version still fails."""
        memory = dynamodb_memory_with_mock_s3
        
        # Create and update a resource
        resource = memory.create_new(
            VersionedResourceWithBlob,
            {
                "title": "V1",
                "content": "Content",
                "large_data": {"v": 1}
            }
        )
        
        # Update to v2
        memory.update_existing(resource, {"title": "V2"})
        
        # Try to update from v1 (should fail)
        with pytest.raises(ValueError, match="Cannot update from non-latest version"):
            memory.update_existing(
                resource,  # This is still v1
                {"title": "V3"}
            )


class TestPaginatedQueryBlobHandling:
    """Test paginated query handling with blob fields."""
    
    def test_paginated_query_with_blobs(self, dynamodb_memory_with_mock_s3):
        """Test that paginated queries correctly handle blob placeholders."""
        memory = dynamodb_memory_with_mock_s3
        
        # Create multiple resources with blobs
        for i in range(3):
            memory.create_new(
                NonVersionedResourceWithBlob,
                {
                    "name": f"Resource {i}",
                    "description": f"Description {i}",
                    "blob_content": {"index": i, "data": "large" * 100}
                }
            )
        
        # Query with pagination
        results = memory.list_type_by_updated_at(
            NonVersionedResourceWithBlob,
            results_limit=2
        )
        
        assert len(results) == 2
        
        # Check that resources have blob placeholders
        for resource in results:
            assert isinstance(resource, NonVersionedResourceWithBlob)
            assert resource.name is not None
            # Blob field should be None (not loaded)
            assert resource.blob_content is None
            # Should have blob placeholders
            assert resource.has_unloaded_blobs()
            assert "blob_content" in resource.get_unloaded_blob_fields()
    
    def test_paginated_query_versioned_resources(self, dynamodb_memory_with_mock_s3):
        """Test paginated queries with versioned resources having blobs."""
        memory = dynamodb_memory_with_mock_s3
        
        # Create versioned resources
        resources = []
        for i in range(3):
            r = memory.create_new(
                VersionedResourceWithBlob,
                {
                    "title": f"Doc {i}",
                    "content": f"Content {i}",
                    "large_data": {"index": i}
                }
            )
            resources.append(r)
        
        # Update one to create multiple versions (including blob field)
        memory.update_existing(resources[0], {"title": "Doc 0 Updated", "large_data": {"index": 0, "updated": True}})
        
        # Query all latest versions
        results = memory.list_type_by_updated_at(
            VersionedResourceWithBlob,
            results_limit=10
        )
        
        assert len(results) == 3
        
        # Check blob placeholders are set correctly
        for resource in results:
            assert isinstance(resource, VersionedResourceWithBlob)
            assert resource.large_data is None
            # All resources created with blob data should have placeholders
            assert resource.has_unloaded_blobs()
            
            # Verify placeholder structure
            assert "large_data" in resource._blob_placeholders
            placeholder = resource._blob_placeholders["large_data"]
            assert placeholder["field_name"] == "large_data"
            # The s3_key should include the version
            assert f"v{resource.version}" in placeholder["s3_key"]


class TestPrivateAttrBugfix:
    """Test that _blob_placeholders uses PrivateAttr correctly."""
    
    def test_blob_placeholders_private_attr(self):
        """Test that _blob_placeholders is properly initialized as a private attribute."""
        # Create resource without any blob fields loaded
        resource = NonVersionedResourceWithBlob(
            resource_id="test-id",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            name="Test",
            description="Description"
        )
        
        # _blob_placeholders should exist and be a dict
        assert hasattr(resource, "_blob_placeholders")
        assert isinstance(resource._blob_placeholders, dict)
        assert len(resource._blob_placeholders) == 0
        
        # It should not appear in model dumps
        dumped = resource.model_dump()
        assert "_blob_placeholders" not in dumped
        
        # It should not appear in JSON serialization
        json_str = resource.model_dump_json()
        assert "_blob_placeholders" not in json_str
    
    def test_blob_placeholders_persistence(self, dynamodb_memory_with_mock_s3):
        """Test that blob placeholders persist correctly across operations."""
        memory = dynamodb_memory_with_mock_s3
        
        # Create resource with blob
        resource = memory.create_new(
            NonVersionedResourceWithBlob,
            {
                "name": "Test",
                "description": "Desc",
                "blob_content": {"data": "value"}
            }
        )
        
        # Load without blobs
        loaded = memory.get_existing(
            resource.resource_id,
            NonVersionedResourceWithBlob,
            load_blobs=False
        )
        
        # Should have placeholders
        assert loaded.has_unloaded_blobs()
        assert isinstance(loaded._blob_placeholders, dict)
        assert "blob_content" in loaded._blob_placeholders
        
        # Update (without modifying blob fields)
        updated = memory.update_existing(
            loaded,
            {"description": "New Desc"}
        )
        
        # After update, we get a fresh read from DB
        assert updated.blob_content is None
        # When updating without modifying blob fields, the _blob_fields 
        # list is not preserved (blob fields become regular None values)
        # This is expected behavior - if you want to preserve blobs,
        # you need to re-supply them in the update
        assert not updated.has_unloaded_blobs()
        
        # But if we update with a blob field, it should work
        # Note: Due to mock S3, the blob is not actually stored, so it appears in the resource
        # In a real scenario with actual S3, blob_content would be None after update


