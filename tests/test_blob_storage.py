import json
from typing import Optional
from unittest.mock import Mock
import pytest
from datetime import datetime

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig
from simplesingletable.blob_storage import S3BlobStorage


class DemoResourceWithBlobs(DynamoDbResource):
    """Test resource with blob fields."""
    
    name: str
    small_data: str
    large_response: Optional[dict] = None  # Blob field
    binary_content: Optional[bytes] = None  # Binary blob field
    
    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={
            "large_response": BlobFieldConfig(
                compress=True,
                content_type="application/json"
            ),
            "binary_content": BlobFieldConfig(
                compress=False,
                content_type="application/octet-stream",
                max_size_bytes=1024 * 1024  # 1MB limit
            )
        }
    )


class DemoVersionedResourceWithBlobs(DynamoDbVersionedResource):
    """Test versioned resource with blob fields."""
    
    title: str
    metadata: dict
    document_content: Optional[str] = None  # Blob field
    
    resource_config = ResourceConfig(
        compress_data=True,
        max_versions=5,
        blob_fields={
            "document_content": BlobFieldConfig(
                compress=True,
                content_type="text/plain"
            )
        }
    )


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client."""
    mock_client = Mock()
    mock_client.put_object = Mock(return_value={})
    mock_client.get_object = Mock()
    mock_client.delete_object = Mock()
    mock_client.delete_objects = Mock(return_value={'Deleted': []})
    mock_client.get_paginator = Mock()
    return mock_client


@pytest.fixture
def s3_blob_storage(mock_s3_client):
    """Create S3BlobStorage instance with mock client."""
    storage = S3BlobStorage(
        bucket_name="test-bucket",
        key_prefix="test-prefix"
    )
    storage._s3_client = mock_s3_client
    return storage


@pytest.fixture
def dynamodb_memory_with_s3(dynamodb_memory, s3_blob_storage):
    """Extend dynamodb_memory fixture to include S3 configuration."""
    dynamodb_memory.s3_bucket = "test-bucket"
    dynamodb_memory.s3_key_prefix = "test-prefix"
    dynamodb_memory._s3_blob_storage = s3_blob_storage
    return dynamodb_memory


class TestBlobStorageIntegration:
    """Test blob storage integration with DynamoDB resources."""
    
    def test_create_resource_with_blobs(self, dynamodb_memory_with_s3):
        """Test creating a resource with blob fields."""
        memory = dynamodb_memory_with_s3
        s3_storage = memory.s3_blob_storage
        
        # Create resource with blob fields
        large_data = {"results": ["item" * 100 for _ in range(100)]}
        binary_data = b"Binary content here"
        
        resource = memory.create_new(
            DemoResourceWithBlobs,
            {
                "name": "Test Resource",
                "small_data": "Regular field",
                "large_response": large_data,
                "binary_content": binary_data
            }
        )
        
        # Verify S3 put_object was called for blob fields
        assert s3_storage.s3_client.put_object.call_count == 2
        
        # Verify the calls
        calls = s3_storage.s3_client.put_object.call_args_list
        
        # Check large_response blob
        call_args = calls[0][1]
        assert call_args['Bucket'] == "test-bucket"
        assert "large_response" in call_args['Key']
        assert call_args['ContentType'] == "application/json"
        
        # Check binary_content blob
        call_args = calls[1][1]
        assert call_args['Bucket'] == "test-bucket"
        assert "binary_content" in call_args['Key']
        assert call_args['ContentType'] == "application/octet-stream"
    
    def test_read_resource_without_blobs(self, dynamodb_memory_with_s3):
        """Test reading a resource without loading blob fields."""
        memory = dynamodb_memory_with_s3
        
        # Create resource
        resource = memory.create_new(
            DemoResourceWithBlobs,
            {
                "name": "Test Resource",
                "small_data": "Regular field",
                "large_response": {"data": "large"},
                "binary_content": b"binary"
            }
        )
        
        # Read without loading blobs
        loaded = memory.get_existing(
            resource.resource_id, 
            DemoResourceWithBlobs,
            load_blobs=False
        )
        
        # Regular fields should be present
        assert loaded.name == "Test Resource"
        assert loaded.small_data == "Regular field"
        
        # Blob fields should be None
        assert loaded.large_response is None
        assert loaded.binary_content is None
        
        # Should have blob placeholders
        assert loaded.has_unloaded_blobs()
        assert set(loaded.get_unloaded_blob_fields()) == {"large_response", "binary_content"}
    
    def test_read_resource_with_blobs(self, dynamodb_memory_with_s3):
        """Test reading a resource with blob fields loaded."""
        memory = dynamodb_memory_with_s3
        s3_storage = memory.s3_blob_storage
        
        # Mock S3 get_object responses
        large_data = {"data": "large"}
        binary_data = b"binary"
        
        def mock_get_object(**kwargs):
            if "large_response" in kwargs['Key']:
                return {
                    'Body': Mock(read=lambda: json.dumps(large_data).encode()),
                    'Metadata': {'compressed': 'false'}
                }
            elif "binary_content" in kwargs['Key']:
                return {
                    'Body': Mock(read=lambda: binary_data),
                    'Metadata': {'compressed': 'false'}
                }
        
        s3_storage.s3_client.get_object = Mock(side_effect=mock_get_object)
        
        # Create resource
        resource = memory.create_new(
            DemoResourceWithBlobs,
            {
                "name": "Test Resource",
                "small_data": "Regular field",
                "large_response": large_data,
                "binary_content": binary_data
            }
        )
        
        # Read with loading blobs
        loaded = memory.get_existing(
            resource.resource_id,
            DemoResourceWithBlobs,
            load_blobs=True
        )
        
        # All fields should be present
        assert loaded.name == "Test Resource"
        assert loaded.small_data == "Regular field"
        assert loaded.large_response == large_data
        assert loaded.binary_content == binary_data
        
        # Should not have unloaded blobs
        assert not loaded.has_unloaded_blobs()
    
    def test_lazy_load_blob_fields(self, dynamodb_memory_with_s3):
        """Test lazy loading specific blob fields."""
        memory = dynamodb_memory_with_s3
        s3_storage = memory.s3_blob_storage
        
        # Mock S3 responses
        large_data = {"data": "large"}
        
        s3_storage.s3_client.get_object = Mock(return_value={
            'Body': Mock(read=lambda: json.dumps(large_data).encode()),
            'Metadata': {'compressed': 'false'}
        })
        
        # Create and read resource without blobs
        resource = memory.create_new(
            DemoResourceWithBlobs,
            {
                "name": "Test",
                "small_data": "data",
                "large_response": large_data,
                "binary_content": b"binary"
            }
        )
        
        loaded = memory.get_existing(resource.resource_id, DemoResourceWithBlobs)
        
        # Load only specific blob field
        loaded.load_blob_fields(memory, fields=["large_response"])
        
        # large_response should be loaded
        assert loaded.large_response == large_data
        
        # binary_content should still be None
        assert loaded.binary_content is None
        assert "binary_content" in loaded.get_unloaded_blob_fields()
    
    def test_versioned_resource_with_blobs(self, dynamodb_memory_with_s3):
        """Test versioned resources with blob fields."""
        memory = dynamodb_memory_with_s3
        s3_storage = memory.s3_blob_storage
        
        # Create versioned resource
        doc_content = "This is a large document content" * 100
        resource = memory.create_new(
            DemoVersionedResourceWithBlobs,
            {
                "title": "Document",
                "metadata": {"author": "Test"},
                "document_content": doc_content
            }
        )
        
        assert resource.version == 1
        
        # Update resource
        updated_content = "Updated document content" * 100
        updated = memory.update_existing(
            resource,
            {"document_content": updated_content}
        )
        
        assert updated.version == 2
        
        # Verify S3 was called for both versions
        assert s3_storage.s3_client.put_object.call_count == 2
        
        # Check that different versions were stored
        calls = s3_storage.s3_client.put_object.call_args_list
        key1 = calls[0][1]['Key']
        key2 = calls[1][1]['Key']
        
        assert "v1" in key1
        assert "v2" in key2
    
    def test_delete_resource_with_blobs(self, dynamodb_memory_with_s3):
        """Test deleting a resource also deletes its blobs."""
        memory = dynamodb_memory_with_s3
        s3_storage = memory.s3_blob_storage
        
        # Mock S3 list and delete
        s3_storage.s3_client.get_paginator = Mock(return_value=Mock(
            paginate=Mock(return_value=[
                {'Contents': [
                    {'Key': 'test-prefix/DemoResourceWithBlobs/id/large_response'},
                    {'Key': 'test-prefix/DemoResourceWithBlobs/id/binary_content'}
                ]}
            ])
        ))
        
        # Create and delete resource
        resource = memory.create_new(
            DemoResourceWithBlobs,
            {
                "name": "Test",
                "small_data": "data",
                "large_response": {"data": "large"},
                "binary_content": b"binary"
            }
        )
        
        memory.delete_existing(resource)
        
        # Verify S3 delete was called
        s3_storage.s3_client.delete_objects.assert_called()
    
    def test_blob_compression(self, s3_blob_storage):
        """Test blob compression functionality."""
        import gzip
        
        # Test with compression enabled
        config = BlobFieldConfig(compress=True)
        data = {"large": "data" * 1000}
        
        placeholder = s3_blob_storage.put_blob(
            resource_type="TestResource",
            resource_id="test-id",
            field_name="compressed_field",
            value=data,
            config=config,
            version=None
        )
        
        assert placeholder['compressed'] is True
        
        # Verify compressed data was sent to S3
        call_args = s3_blob_storage.s3_client.put_object.call_args[1]
        body = call_args['Body']
        
        # Should be compressed
        decompressed = gzip.decompress(body)
        assert json.loads(decompressed) == data
    
    def test_blob_size_limit(self, s3_blob_storage):
        """Test blob size limit enforcement."""
        config = BlobFieldConfig(
            compress=False,
            max_size_bytes=100  # 100 bytes limit
        )
        
        # Data exceeding limit
        large_data = "x" * 200
        
        with pytest.raises(ValueError) as exc_info:
            s3_blob_storage.put_blob(
                resource_type="TestResource",
                resource_id="test-id",
                field_name="limited_field",
                value=large_data,
                config=config,
                version=None
            )
        
        assert "exceeds maximum size" in str(exc_info.value)
    
    def test_missing_blob_handling(self, s3_blob_storage):
        """Test handling of missing blobs in S3."""
        from botocore.exceptions import ClientError
        
        # Mock S3 to raise NoSuchKey error
        s3_blob_storage.s3_client.get_object = Mock(
            side_effect=ClientError(
                {'Error': {'Code': 'NoSuchKey'}},
                'GetObject'
            )
        )
        
        with pytest.raises(ValueError) as exc_info:
            s3_blob_storage.get_blob(
                resource_type="TestResource",
                resource_id="test-id",
                field_name="missing_field",
                version=None
            )
        
        assert "Blob not found" in str(exc_info.value)
    
    def test_s3_key_structure(self, s3_blob_storage):
        """Test S3 key generation structure."""
        # Non-versioned resource
        key = s3_blob_storage._build_s3_key(
            resource_type="MyResource",
            resource_id="resource-123",
            field_name="data_field",
            version=None
        )
        assert key == "test-prefix/MyResource/resource-123/data_field"
        
        # Versioned resource
        key = s3_blob_storage._build_s3_key(
            resource_type="MyVersionedResource",
            resource_id="resource-456",
            field_name="content",
            version=3
        )
        assert key == "test-prefix/MyVersionedResource/resource-456/v3/content"
    
    def test_blob_fields_must_be_optional(self):
        """Test that blob fields must be Optional in the model."""
        # This should work - blob fields are Optional
        resource = DemoResourceWithBlobs(
            resource_id="test",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            name="Test",
            small_data="data"
            # large_response and binary_content are omitted (None)
        )
        
        assert resource.large_response is None
        assert resource.binary_content is None