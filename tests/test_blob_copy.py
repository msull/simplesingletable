"""Tests for blob copy and register_external_blob operations."""

import json
import logging
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from logzero import logger

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import BlobFieldConfig, BlobPlaceholder, ResourceConfig


# --- Test resource definitions ---


class VersionedDocWithBlobs(DynamoDbVersionedResource):
    """Versioned resource with blob fields for testing."""

    title: str
    content: Optional[str] = None
    attachments: Optional[dict] = None

    resource_config = ResourceConfig(
        compress_data=True,
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "attachments": BlobFieldConfig(compress=False, content_type="application/json"),
        },
    )


class SimpleDocWithBlob(DynamoDbResource):
    """Non-versioned resource with blob field for testing."""

    name: str
    data: Optional[str] = None
    summary: Optional[str] = None

    resource_config = ResourceConfig(
        compress_data=False,
        blob_fields={
            "data": BlobFieldConfig(compress=False, content_type="text/plain"),
            "summary": BlobFieldConfig(compress=True, content_type="text/plain"),
        },
    )


class SimpleDocNoBlobConfig(DynamoDbResource):
    """Non-versioned resource with NO blob fields configured."""

    name: str
    data: Optional[str] = None

    resource_config = ResourceConfig(compress_data=False)


# --- Unit tests (mock S3) ---


class TestCopyBlobUnit:
    """Unit tests for copy_blob using mock S3."""

    def test_copy_between_nonversioned_resources(self, dynamodb_memory_with_s3):
        """Test copying a blob between two non-versioned resources."""
        memory = dynamodb_memory_with_s3

        # Create source with blob data
        source = memory.create_new(SimpleDocWithBlob, {"name": "Source", "data": "Hello world"})

        # Create target without blob data
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target"})

        # Copy blob
        placeholder = memory.copy_blob(source, "data", target, "data")

        assert placeholder["field_name"] == "data"
        assert placeholder["size_bytes"] > 0

        # Verify target can load the blob
        loaded_target = memory.get_existing(target.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded_target.data == "Hello world"

    def test_copy_between_versioned_resources(self, dynamodb_memory_with_s3):
        """Test copying a blob between two versioned resources."""
        memory = dynamodb_memory_with_s3

        # Create source
        source = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Source", "content": "Source content"},
        )

        # Create target
        target = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Target"},
        )

        # Copy blob
        placeholder = memory.copy_blob(source, "content", target, "content")

        assert placeholder["field_name"] == "content"

        # Verify target can load the blob
        loaded_target = memory.get_existing(target.resource_id, VersionedDocWithBlobs, load_blobs=True)
        assert loaded_target.content == "Source content"

    def test_cross_resource_type_copy(self, dynamodb_memory_with_s3):
        """Test copying blob from versioned to non-versioned resource."""
        memory = dynamodb_memory_with_s3

        # Create versioned source
        source = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Versioned Source", "content": "Cross-type content"},
        )

        # Create non-versioned target
        target = memory.create_new(SimpleDocWithBlob, {"name": "Non-versioned Target"})

        # Copy blob (versioned content -> non-versioned data)
        # The source is compressed, target data is not compressed -> compression mismatch warning
        placeholder = memory.copy_blob(source, "content", target, "data")

        assert placeholder["field_name"] == "data"

        # Verify target can load the blob
        loaded_target = memory.get_existing(target.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded_target.data == "Cross-type content"

    def test_copy_same_resource_different_field(self, dynamodb_memory_with_s3):
        """Test copying blob to a different field on the same resource."""
        memory = dynamodb_memory_with_s3

        # Create resource with data in one field
        resource = memory.create_new(SimpleDocWithBlob, {"name": "Same Resource", "data": "Copy me"})

        # Copy from data -> summary
        placeholder = memory.copy_blob(resource, "data", resource, "summary")

        assert placeholder["field_name"] == "summary"

        # Verify both fields work
        loaded = memory.get_existing(resource.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded.data == "Copy me"
        assert loaded.summary == "Copy me"

    def test_copy_with_delete_source(self, dynamodb_memory_with_s3):
        """Test copy with delete_source=True (move semantics)."""
        memory = dynamodb_memory_with_s3

        # Create source with blob
        source = memory.create_new(SimpleDocWithBlob, {"name": "Source", "data": "Move me"})

        # Create target
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target"})

        # Move blob (copy + delete source)
        memory.copy_blob(source, "data", target, "data", delete_source=True)

        # Target should have the data
        loaded_target = memory.get_existing(target.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded_target.data == "Move me"

        # Source blob should be gone (loading will fail or return None)
        loaded_source = memory.get_existing(source.resource_id, SimpleDocWithBlob, load_blobs=False)
        assert loaded_source.has_unloaded_blobs()  # placeholder still exists in DynamoDB

        # Attempting to load should raise since the S3 object was deleted
        with pytest.raises(ValueError, match="Blob not found"):
            loaded_source.load_blob_fields(memory)

    def test_validates_source_field_in_blob_config(self, dynamodb_memory_with_s3):
        """Test that source field must be in blob_fields config."""
        memory = dynamodb_memory_with_s3

        source = memory.create_new(SimpleDocWithBlob, {"name": "Source"})
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target"})

        with pytest.raises(ValueError, match="not configured as a blob field"):
            memory.copy_blob(source, "name", target, "data")

    def test_validates_target_field_in_blob_config(self, dynamodb_memory_with_s3):
        """Test that target field must be in blob_fields config."""
        memory = dynamodb_memory_with_s3

        source = memory.create_new(SimpleDocWithBlob, {"name": "Source", "data": "hello"})
        target = memory.create_new(SimpleDocNoBlobConfig, {"name": "Target"})

        with pytest.raises(ValueError, match="not configured as a blob field"):
            memory.copy_blob(source, "data", target, "data")

    def test_validates_s3_configured(self, dynamodb_memory):
        """Test that S3 must be configured."""
        memory = dynamodb_memory  # no S3 configured

        source = SimpleDocWithBlob.create_new({"name": "Source", "data": "hello"})
        target = SimpleDocWithBlob.create_new({"name": "Target"})

        with pytest.raises(ValueError, match="S3 blob storage not configured"):
            memory.copy_blob(source, "data", target, "data")

    def test_source_blob_not_found(self, dynamodb_memory_with_s3):
        """Test error when source blob doesn't exist in S3."""
        memory = dynamodb_memory_with_s3

        # Create source WITHOUT blob data
        source = memory.create_new(SimpleDocWithBlob, {"name": "Source"})
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target"})

        with pytest.raises(ValueError, match="Blob not found"):
            memory.copy_blob(source, "data", target, "data")

    def test_self_copy_guard(self, dynamodb_memory_with_s3):
        """Test that copying to same resource+field raises error."""
        memory = dynamodb_memory_with_s3

        resource = memory.create_new(SimpleDocWithBlob, {"name": "Self", "data": "test"})

        with pytest.raises(ValueError, match="Cannot copy a blob to the same resource and field"):
            memory.copy_blob(resource, "data", resource, "data")

    def test_compression_mismatch_warning(self, dynamodb_memory_with_s3, caplog):
        """Test that compression mismatch produces a warning."""
        memory = dynamodb_memory_with_s3

        # Source: non-compressed data field
        source = memory.create_new(SimpleDocWithBlob, {"name": "Source", "data": "Uncompressed"})

        # Target: compressed summary field
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target"})

        with caplog.at_level(logging.WARNING):
            memory.copy_blob(source, "data", target, "summary")

        assert "Compression mismatch" in caplog.text

    def test_in_memory_resource_state_updated(self, dynamodb_memory_with_s3):
        """Test that in-memory resource state is updated after copy."""
        memory = dynamodb_memory_with_s3

        source = memory.create_new(SimpleDocWithBlob, {"name": "Source", "data": "test"})
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target"})

        memory.copy_blob(source, "data", target, "data")

        # Target should have placeholder in-memory
        assert "data" in target._blob_placeholders
        assert target.data is None  # Marked as unloaded


class TestRegisterExternalBlobUnit:
    """Unit tests for register_external_blob using mock S3."""

    def test_register_basic(self, dynamodb_memory_with_s3, minio_s3_client):
        """Test basic register_external_blob flow."""
        memory = dynamodb_memory_with_s3

        # Upload an external object to S3
        external_key = "external/test-object.txt"
        minio_s3_client.put_object(
            Bucket=memory.s3_bucket,
            Key=external_key,
            Body=json.dumps("External data").encode(),
        )

        # Create resource
        resource = memory.create_new(SimpleDocWithBlob, {"name": "External Test"})

        # Register external blob
        placeholder = memory.register_external_blob(
            resource=resource,
            field_name="data",
            source_s3_key=external_key,
            content_type="text/plain",
        )

        assert placeholder["field_name"] == "data"
        assert placeholder["size_bytes"] > 0

        # Verify loading
        loaded = memory.get_existing(resource.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded.data == "External data"

    def test_register_with_delete_source(self, dynamodb_memory_with_s3, minio_s3_client):
        """Test register_external_blob with delete_source=True."""
        memory = dynamodb_memory_with_s3

        # Upload an external object
        external_key = "external/delete-me.txt"
        minio_s3_client.put_object(
            Bucket=memory.s3_bucket,
            Key=external_key,
            Body=json.dumps("Delete after register").encode(),
        )

        # Create resource
        resource = memory.create_new(SimpleDocWithBlob, {"name": "Delete Source Test"})

        # Register and delete
        memory.register_external_blob(
            resource=resource,
            field_name="data",
            source_s3_key=external_key,
            delete_source=True,
        )

        # Verify external object was deleted
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            minio_s3_client.head_object(Bucket=memory.s3_bucket, Key=external_key)

        # Verify blob is accessible
        loaded = memory.get_existing(resource.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded.data == "Delete after register"

    def test_register_validates_field_config(self, dynamodb_memory_with_s3, minio_s3_client):
        """Test that field must be in blob_fields config."""
        memory = dynamodb_memory_with_s3

        resource = memory.create_new(SimpleDocNoBlobConfig, {"name": "No Blobs"})

        with pytest.raises(ValueError, match="not configured as a blob field"):
            memory.register_external_blob(
                resource=resource,
                field_name="data",
                source_s3_key="external/anything.txt",
            )

    def test_register_validates_s3_configured(self, dynamodb_memory):
        """Test that S3 must be configured."""
        memory = dynamodb_memory

        resource = SimpleDocWithBlob.create_new({"name": "No S3"})

        with pytest.raises(ValueError, match="S3 blob storage not configured"):
            memory.register_external_blob(
                resource=resource,
                field_name="data",
                source_s3_key="external/anything.txt",
            )

    def test_register_source_not_found(self, dynamodb_memory_with_s3):
        """Test error when external source doesn't exist."""
        memory = dynamodb_memory_with_s3

        resource = memory.create_new(SimpleDocWithBlob, {"name": "Missing Source"})

        with pytest.raises(ValueError, match="Source S3 object not found"):
            memory.register_external_blob(
                resource=resource,
                field_name="data",
                source_s3_key="nonexistent/key.txt",
            )

    def test_register_in_memory_state_updated(self, dynamodb_memory_with_s3, minio_s3_client):
        """Test that in-memory resource state is updated after register."""
        memory = dynamodb_memory_with_s3

        # Upload external object
        external_key = "external/state-test.txt"
        minio_s3_client.put_object(
            Bucket=memory.s3_bucket,
            Key=external_key,
            Body=b"state test",
        )

        resource = memory.create_new(SimpleDocWithBlob, {"name": "State Test"})

        memory.register_external_blob(
            resource=resource,
            field_name="data",
            source_s3_key=external_key,
        )

        assert "data" in resource._blob_placeholders
        assert resource.data is None

    def test_register_on_versioned_resource(self, dynamodb_memory_with_s3, minio_s3_client):
        """Test register_external_blob on a versioned resource."""
        memory = dynamodb_memory_with_s3

        external_key = "external/versioned-test.txt"
        minio_s3_client.put_object(
            Bucket=memory.s3_bucket,
            Key=external_key,
            Body=json.dumps("Versioned external").encode(),
        )

        resource = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Versioned External"},
        )

        placeholder = memory.register_external_blob(
            resource=resource,
            field_name="content",
            source_s3_key=external_key,
            compressed=False,
        )

        assert placeholder["field_name"] == "content"

        # Verify version tracking
        assert resource._blob_versions.get("content") == resource.version

        # Verify loading
        loaded = memory.get_existing(resource.resource_id, VersionedDocWithBlobs, load_blobs=True)
        assert loaded.content == "Versioned external"


# --- Integration tests (DynamoDB Local + MinIO) ---


class TestBlobCopyIntegration:
    """Integration tests using real DynamoDB Local + MinIO."""

    def test_end_to_end_copy_and_verify(self, dynamodb_memory_with_s3):
        """End-to-end: copy blob, reload from DB, verify data integrity."""
        memory = dynamodb_memory_with_s3

        # Create source with large content
        large_content = "Integration test content " * 100
        source = memory.create_new(
            SimpleDocWithBlob,
            {"name": "Integration Source", "data": large_content},
        )

        # Create target
        target = memory.create_new(SimpleDocWithBlob, {"name": "Integration Target"})

        # Copy blob
        memory.copy_blob(source, "data", target, "data")

        # Reload target entirely from database
        loaded_target = memory.read_existing(target.resource_id, SimpleDocWithBlob, load_blobs=True)

        assert loaded_target.data == large_content
        assert loaded_target.name == "Integration Target"

    def test_end_to_end_register_external_and_verify(self, dynamodb_memory_with_s3, minio_s3_client):
        """End-to-end: register external blob, reload, verify."""
        memory = dynamodb_memory_with_s3

        # Upload external object
        external_data = {"key": "value", "nested": [1, 2, 3]}
        external_key = "external/integration-test.json"
        minio_s3_client.put_object(
            Bucket=memory.s3_bucket,
            Key=external_key,
            Body=json.dumps(external_data).encode(),
            ContentType="application/json",
        )

        # Create resource
        resource = memory.create_new(SimpleDocWithBlob, {"name": "External Integration"})

        # Register
        memory.register_external_blob(
            resource=resource,
            field_name="data",
            source_s3_key=external_key,
            content_type="application/json",
        )

        # Reload from database
        loaded = memory.read_existing(resource.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded.data == external_data

    def test_versioned_copy_preserves_version_tracking(self, dynamodb_memory_with_s3):
        """Test that versioned blob copy preserves version tracking."""
        memory = dynamodb_memory_with_s3

        # Create source at v1
        source = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Source Doc", "content": "Version 1 content"},
        )

        # Create target at v1
        target = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Target Doc"},
        )

        # Copy content from source to target
        memory.copy_blob(source, "content", target, "content")

        # Reload target
        loaded = memory.read_existing(target.resource_id, VersionedDocWithBlobs, load_blobs=True)

        assert loaded.content == "Version 1 content"
        assert loaded.version == 1
        assert loaded._blob_versions.get("content") == 1

    def test_copy_between_versioned_updated_resources(self, dynamodb_memory_with_s3):
        """Test copy from a resource that's been updated to a newer version."""
        memory = dynamodb_memory_with_s3

        # Create source
        source = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Source", "content": "V1 content"},
        )

        # Update source (creates v2 with new content)
        source_v1 = memory.get_existing(source.resource_id, VersionedDocWithBlobs, load_blobs=False)
        source_v2 = memory.update_existing(source_v1, {"content": "V2 content"})

        # Create target
        target = memory.create_new(VersionedDocWithBlobs, {"title": "Target"})

        # Copy from v2 source
        memory.copy_blob(source_v2, "content", target, "content")

        loaded = memory.read_existing(target.resource_id, VersionedDocWithBlobs, load_blobs=True)
        assert loaded.content == "V2 content"


class TestBlobCopyDynamoDbMetadata:
    """Tests to verify DynamoDB metadata is correctly updated after copy."""

    def test_nonversioned_metadata_updated(self, dynamodb_memory_with_s3):
        """Test that _blob_fields is updated in DynamoDB for non-versioned resources."""
        memory = dynamodb_memory_with_s3

        source = memory.create_new(SimpleDocWithBlob, {"name": "Source", "data": "metadata test"})
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target"})

        memory.copy_blob(source, "data", target, "data")

        # Read raw DynamoDB item
        key = target.dynamodb_lookup_keys_from_id(target.resource_id)
        raw = memory.dynamodb_table.get_item(Key=key)["Item"]

        assert "_blob_fields" in raw
        assert "data" in raw["_blob_fields"]

    def test_versioned_metadata_updated_on_both_items(self, dynamodb_memory_with_s3):
        """Test that _blob_fields and _blob_versions are updated on both v0 and vN."""
        memory = dynamodb_memory_with_s3

        source = memory.create_new(
            VersionedDocWithBlobs,
            {"title": "Source", "content": "versioned metadata test"},
        )
        target = memory.create_new(VersionedDocWithBlobs, {"title": "Target"})

        memory.copy_blob(source, "content", target, "content")

        pk = f"{VersionedDocWithBlobs.get_unique_key_prefix()}#{target.resource_id}"

        # Check v0
        v0_item = memory.dynamodb_table.get_item(Key={"pk": pk, "sk": "v0"})["Item"]
        assert "content" in v0_item.get("_blob_fields", [])
        assert "content" in v0_item.get("_blob_versions", {})

        # Check v1
        v1_item = memory.dynamodb_table.get_item(Key={"pk": pk, "sk": "v1"})["Item"]
        assert "content" in v1_item.get("_blob_fields", [])
        assert "content" in v1_item.get("_blob_versions", {})

    def test_cache_invalidation_on_target(self, dynamodb_memory_with_s3):
        """Test that cache is invalidated for target key after copy."""
        memory = dynamodb_memory_with_s3

        source = memory.create_new(SimpleDocWithBlob, {"name": "Source", "data": "cache test"})
        target = memory.create_new(SimpleDocWithBlob, {"name": "Target", "data": "old data"})

        # Load target blob to populate cache
        loaded = memory.get_existing(target.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert loaded.data == "old data"

        # Copy new data to target
        memory.copy_blob(source, "data", target, "data")

        # Load again - should get new data, not cached old data
        reloaded = memory.get_existing(target.resource_id, SimpleDocWithBlob, load_blobs=True)
        assert reloaded.data == "cache test"
