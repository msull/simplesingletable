"""
Integration tests for blob storage with real S3 (MinIO).

These tests use actual S3 storage via MinIO instead of mocks to ensure
the blob storage feature works correctly in a realistic environment.
"""

from typing import Optional

from simplesingletable import DynamoDbVersionedResource, DynamoDbResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig

# Import MinIO fixtures


class DocumentWithBlobs(DynamoDbVersionedResource):
    """Test versioned resource with blob fields."""
    
    title: str
    author: str
    tags: list[str]
    content: Optional[str] = None  # Blob field
    attachments: Optional[dict] = None  # Blob field
    
    resource_config = ResourceConfig(
        compress_data=True,
        max_versions=5,
        blob_fields={
            "content": BlobFieldConfig(
                compress=True,
                content_type="text/plain"
            ),
            "attachments": BlobFieldConfig(
                compress=True,
                content_type="application/json"
            )
        }
    )


class SimpleDocumentWithBlob(DynamoDbResource):
    """Test non-versioned resource with blob field."""
    
    name: str
    data: Optional[str] = None  # Blob field
    
    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={
            "data": BlobFieldConfig(
                compress=False,
                content_type="text/plain"
            )
        }
    )


class TestBlobStorageIntegrationWithMinIO:
    """Integration tests using real S3 storage via MinIO."""
    
    def test_create_and_retrieve_blob_fields(self, dynamodb_memory_with_s3):
        """Test creating a resource with blobs and retrieving them."""
        memory = dynamodb_memory_with_s3
        
        # Create document with blob fields
        large_content = "This is test content. " * 500
        attachments_data = {
            "files": ["doc1.pdf", "doc2.xlsx"],
            "metadata": {"size": 102400, "type": "documents"}
        }
        
        doc = memory.create_new(
            DocumentWithBlobs,
            {
                "title": "Integration Test Document",
                "author": "Test Author",
                "tags": ["test", "integration"],
                "content": large_content,
                "attachments": attachments_data
            }
        )
        
        assert doc.resource_id
        assert doc.version == 1
        assert doc.content is None  # Should be stored in S3
        assert doc.attachments is None
        
        # Load without blobs
        loaded = memory.get_existing(
            doc.resource_id,
            DocumentWithBlobs,
            load_blobs=False
        )
        
        assert loaded.content is None
        assert loaded.attachments is None
        assert loaded.has_unloaded_blobs()
        assert set(loaded.get_unloaded_blob_fields()) == {"content", "attachments"}
        
        # Load with blobs
        loaded_with_blobs = memory.get_existing(
            doc.resource_id,
            DocumentWithBlobs,
            load_blobs=True
        )
        
        assert loaded_with_blobs.content == large_content
        assert loaded_with_blobs.attachments == attachments_data
        assert not loaded_with_blobs.has_unloaded_blobs()
    
    def test_blob_version_preservation(self, dynamodb_memory_with_s3):
        """Test that blob references are preserved across updates."""
        memory = dynamodb_memory_with_s3
        
        # Create initial version with blobs
        content_v1 = "Version 1 content"
        attachments_v1 = {"version": 1, "files": ["v1.txt"]}
        
        doc = memory.create_new(
            DocumentWithBlobs,
            {
                "title": "Version Test",
                "author": "Tester",
                "tags": ["v1"],
                "content": content_v1,
                "attachments": attachments_v1
            }
        )
        
        # Update without changing blobs
        loaded = memory.get_existing(doc.resource_id, DocumentWithBlobs, load_blobs=False)
        updated = memory.update_existing(
            loaded,
            {
                "title": "Version Test - Updated",
                "tags": ["v1", "v2"]
            }
        )
        
        assert updated.version == 2
        assert updated.has_unloaded_blobs()
        
        # Load blobs from v2 - should get v1 blobs
        updated.load_blob_fields(memory)
        assert updated.content == content_v1
        assert updated.attachments == attachments_v1
        
        # Update with new content but keep attachments
        updated2 = memory.update_existing(
            updated,
            {
                "content": "Version 3 content"
            }
        )
        
        assert updated2.version == 3
        
        # Load v3 and check blobs
        v3_loaded = memory.get_existing(
            doc.resource_id,
            DocumentWithBlobs,
            version=3,
            load_blobs=True
        )
        
        assert v3_loaded.content == "Version 3 content"  # New content
        assert v3_loaded.attachments == attachments_v1  # Original attachments
    
    def test_blob_deletion(self, dynamodb_memory_with_s3, minio_s3_client):
        """Test that blobs are deleted when resource is deleted."""
        memory = dynamodb_memory_with_s3
        
        # Create document with blobs
        doc = memory.create_new(
            DocumentWithBlobs,
            {
                "title": "Delete Test",
                "author": "Tester",
                "tags": ["delete"],
                "content": "Content to be deleted",
                "attachments": {"test": "data"}
            }
        )
        
        resource_id = doc.resource_id
        
        # Verify blobs exist in S3
        prefix = f"test-blobs/DocumentWithBlobs/{resource_id}/"
        response = minio_s3_client.list_objects_v2(
            Bucket=memory.s3_bucket,
            Prefix=prefix
        )
        assert 'Contents' in response
        assert len(response['Contents']) == 2  # content and attachments
        
        # Delete all versions
        memory.delete_all_versions(resource_id, DocumentWithBlobs)
        
        # Verify blobs are deleted
        response = minio_s3_client.list_objects_v2(
            Bucket=memory.s3_bucket,
            Prefix=prefix
        )
        assert 'Contents' not in response or len(response.get('Contents', [])) == 0
    
    def test_non_versioned_resource_with_blobs(self, dynamodb_memory_with_s3):
        """Test non-versioned resources with blob fields."""
        memory = dynamodb_memory_with_s3
        
        # Create simple document with blob
        large_data = "Simple document data " * 200
        
        doc = memory.create_new(
            SimpleDocumentWithBlob,
            {
                "name": "Simple Test",
                "data": large_data
            }
        )
        
        # Note: create_new returns the resource with blob data still in memory
        # We need to load it fresh to see the blob behavior
        assert doc.data == large_data  # Still in memory after create
        
        # Load without blob
        loaded = memory.get_existing(
            doc.resource_id,
            SimpleDocumentWithBlob,
            load_blobs=False
        )
        
        assert loaded.data is None
        assert loaded.has_unloaded_blobs()
        
        # Load with blob
        loaded_with_blob = memory.get_existing(
            doc.resource_id,
            SimpleDocumentWithBlob,
            load_blobs=True
        )
        
        assert loaded_with_blob.data == large_data
        
        # Update the blob
        updated = memory.update_existing(
            loaded,
            {"data": "Updated data"}
        )
        
        # Verify update
        final = memory.get_existing(
            doc.resource_id,
            SimpleDocumentWithBlob,
            load_blobs=True
        )
        
        assert final.data == "Updated data"
    
    def test_partial_blob_loading(self, dynamodb_memory_with_s3):
        """Test loading specific blob fields."""
        memory = dynamodb_memory_with_s3
        
        # Create document with multiple blobs
        doc = memory.create_new(
            DocumentWithBlobs,
            {
                "title": "Partial Load Test",
                "author": "Tester",
                "tags": ["partial"],
                "content": "Large content here",
                "attachments": {"file": "attachment.pdf"}
            }
        )
        
        # Load without any blobs
        loaded = memory.get_existing(
            doc.resource_id,
            DocumentWithBlobs,
            load_blobs=False
        )
        
        assert loaded.has_unloaded_blobs()
        assert len(loaded.get_unloaded_blob_fields()) == 2
        
        # Load only content
        loaded.load_blob_fields(memory, fields=["content"])
        
        assert loaded.content == "Large content here"
        assert loaded.attachments is None
        assert loaded.has_unloaded_blobs()
        assert loaded.get_unloaded_blob_fields() == ["attachments"]
        
        # Load remaining blob
        loaded.load_blob_fields(memory, fields=["attachments"])
        
        assert loaded.attachments == {"file": "attachment.pdf"}
        assert not loaded.has_unloaded_blobs()
    
    def test_clear_blob_field(self, dynamodb_memory_with_s3):
        """Test clearing a blob field."""
        memory = dynamodb_memory_with_s3
        
        # Create with blobs
        doc = memory.create_new(
            DocumentWithBlobs,
            {
                "title": "Clear Test",
                "author": "Tester",
                "tags": ["clear"],
                "content": "Content to clear",
                "attachments": {"data": "value"}
            }
        )
        
        # Clear content field
        updated = memory.update_existing(
            doc,
            update_obj={},
            clear_fields={"content"}
        )
        
        assert updated.version == 2

        # Load v2 and verify content is cleared
        v2 = memory.get_existing(
            doc.resource_id,
            DocumentWithBlobs,
            version=2,
            load_blobs=True,
        )

        # Content field is None and has no blob to load
        assert v2.content is None
        assert v2.attachments == {"data": "value"}  # Attachments preserved
        assert not v2.has_unloaded_blobs()  # All available blobs loaded

        # Load v2 and verify content is cleared
        v2 = memory.get_existing(
            doc.resource_id,
            DocumentWithBlobs,
            version=2,
            load_blobs=False  # Don't auto-load blobs
        )
        
        # Content field is None and has no blob to load
        assert v2.content is None
        assert v2.has_unloaded_blobs()  # Attachments still unloaded
        assert v2.get_unloaded_blob_fields() == ["attachments"]
        
        # Load attachments blob
        v2.load_blob_fields(memory, fields=["attachments"])
        assert v2.attachments == {"data": "value"}  # Attachments preserved
        assert not v2.has_unloaded_blobs()  # All available blobs loaded
    
    def test_s3_key_structure(self, dynamodb_memory_with_s3, minio_s3_client):
        """Test the S3 key structure for stored blobs."""
        memory = dynamodb_memory_with_s3
        
        # Create versioned document
        doc = memory.create_new(
            DocumentWithBlobs,
            {
                "title": "S3 Key Test",
                "author": "Tester",
                "tags": ["s3"],
                "content": "Test content for S3"
            }
        )
        
        # Check S3 keys
        prefix = f"test-blobs/DocumentWithBlobs/{doc.resource_id}/"
        response = minio_s3_client.list_objects_v2(
            Bucket=memory.s3_bucket,
            Prefix=prefix
        )
        
        assert 'Contents' in response
        keys = [obj['Key'] for obj in response['Contents']]
        
        # Should have v1/content key
        expected_key = f"test-blobs/DocumentWithBlobs/{doc.resource_id}/v1/content"
        assert expected_key in keys
        
        # Create non-versioned document
        simple_doc = memory.create_new(
            SimpleDocumentWithBlob,
            {
                "name": "Simple S3 Test",
                "data": "Simple data"
            }
        )
        
        # Check S3 keys for non-versioned
        prefix = f"test-blobs/SimpleDocumentWithBlob/{simple_doc.resource_id}/"
        response = minio_s3_client.list_objects_v2(
            Bucket=memory.s3_bucket,
            Prefix=prefix
        )
        
        keys = [obj['Key'] for obj in response['Contents']]
        expected_key = f"test-blobs/SimpleDocumentWithBlob/{simple_doc.resource_id}/data"
        assert expected_key in keys