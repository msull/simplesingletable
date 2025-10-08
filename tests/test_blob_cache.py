"""Tests for blob storage caching functionality."""

import json
import time
from typing import Optional, ClassVar
from unittest.mock import Mock
import pytest

from simplesingletable import DynamoDbResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig
from simplesingletable.blob_storage import S3BlobStorage


class CachedBlobResource(DynamoDbResource):
    """Test resource with blob fields for cache testing."""

    name: str
    content: Optional[str] = None  # Blob field
    metadata: Optional[dict] = None  # Another blob field

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "metadata": BlobFieldConfig(compress=False, content_type="application/json"),
        },
    )


class TestBlobCache:
    """Test blob storage cache functionality."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client that simulates real S3 behavior."""
        mock_client = Mock()

        # Track stored objects
        stored_objects = {}

        def mock_put_object(**kwargs):
            key = kwargs["Key"]
            body = kwargs["Body"]
            metadata = kwargs.get("Metadata", {})
            stored_objects[key] = {
                "Body": body,
                "Metadata": metadata,
                "ContentType": kwargs.get("ContentType"),
            }
            return {}

        def mock_get_object(**kwargs):
            key = kwargs["Key"]
            if key in stored_objects:
                obj = stored_objects[key]
                body_mock = Mock()
                body_mock.read = Mock(return_value=obj["Body"])
                return {
                    "Body": body_mock,
                    "Metadata": obj["Metadata"],
                    "ContentType": obj.get("ContentType"),
                }
            else:
                from botocore.exceptions import ClientError

                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        mock_client.put_object = Mock(side_effect=mock_put_object)
        mock_client.get_object = Mock(side_effect=mock_get_object)
        mock_client.delete_object = Mock()
        mock_client.delete_objects = Mock(return_value={"Deleted": []})
        mock_client.get_paginator = Mock()

        return mock_client

    @pytest.fixture
    def s3_storage_with_cache(self, mock_s3_client):
        """Create S3BlobStorage with cache enabled."""
        storage = S3BlobStorage(
            bucket_name="test-bucket",
            key_prefix="test-prefix",
            cache_enabled=True,
            cache_max_size_bytes=10 * 1024,  # 10KB for testing
            cache_max_items=5,
            cache_ttl_seconds=2,  # 2 seconds for testing TTL
            cache_max_item_size_bytes=5 * 1024,  # 5KB max item size
        )
        storage._s3_client = mock_s3_client
        return storage

    @pytest.fixture
    def s3_storage_no_cache(self, mock_s3_client):
        """Create S3BlobStorage with cache disabled."""
        storage = S3BlobStorage(
            bucket_name="test-bucket",
            key_prefix="test-prefix",
            cache_enabled=False,
        )
        storage._s3_client = mock_s3_client
        return storage

    def test_cache_hit(self, s3_storage_with_cache):
        """Test cache hit scenario."""
        # Put a blob (which also caches it)
        data = {"test": "data"}
        s3_storage_with_cache.put_blob(
            resource_type="TestResource",
            resource_id="test-123",
            field_name="metadata",
            value=data,
            config=BlobFieldConfig(compress=False),
            version=None,
        )

        # Clear cache to test fresh retrieval
        s3_storage_with_cache.clear_cache()

        # First get - should hit S3 (cache miss)
        result1 = s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="test-123",
            field_name="metadata",
            version=None,
        )
        assert result1 == data
        assert s3_storage_with_cache.s3_client.get_object.call_count == 1

        # Second get - should hit cache
        result2 = s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="test-123",
            field_name="metadata",
            version=None,
        )
        assert result2 == data
        assert s3_storage_with_cache.s3_client.get_object.call_count == 1  # Still 1, not 2

        # Check cache statistics
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.hits == 1
        assert stats.misses == 1
        assert stats.hit_rate == 50.0

    def test_cache_miss(self, s3_storage_with_cache):
        """Test cache miss scenario."""
        # Put data directly to S3 (bypassing cache) - use JSON format
        data = "test content"
        s3_storage_with_cache.s3_client.put_object(
            Bucket="test-bucket",
            Key="test-prefix/TestResource/test-456/content",
            Body=json.dumps(data).encode("utf-8"),  # JSON encode like put_blob does
            Metadata={"compressed": "false"},
        )

        # Get should miss cache and fetch from S3
        result = s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="test-456",
            field_name="content",
            version=None,
        )
        assert result == data

        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.misses == 1
        assert stats.hits == 0

    def test_cache_ttl_expiration(self, s3_storage_with_cache):
        """Test cache TTL expiration."""
        # Put a blob
        data = {"expires": "soon"}
        s3_storage_with_cache.put_blob(
            resource_type="TestResource",
            resource_id="ttl-test",
            field_name="metadata",
            value=data,
            config=BlobFieldConfig(compress=False),
            version=None,
        )

        # Clear cache and reset mock to test fresh
        s3_storage_with_cache.clear_cache()
        s3_storage_with_cache.s3_client.get_object.reset_mock()

        # First get - cache miss
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="ttl-test",
            field_name="metadata",
            version=None,
        )
        assert s3_storage_with_cache.s3_client.get_object.call_count == 1

        # Second get within TTL - cache hit
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="ttl-test",
            field_name="metadata",
            version=None,
        )
        assert s3_storage_with_cache.s3_client.get_object.call_count == 1

        # Wait for TTL to expire
        time.sleep(2.5)

        # Third get after TTL - cache miss again
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="ttl-test",
            field_name="metadata",
            version=None,
        )
        assert s3_storage_with_cache.s3_client.get_object.call_count == 2

    def test_lru_eviction_by_count(self, s3_storage_with_cache):
        """Test LRU eviction when max items is reached."""
        # Put 6 items (max is 5)
        for i in range(6):
            data = f"content_{i}"
            s3_storage_with_cache.put_blob(
                resource_type="TestResource",
                resource_id=f"item-{i}",
                field_name="content",
                value=data,
                config=BlobFieldConfig(compress=False),
                version=None,
            )

        # Check cache has only 5 items
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 5
        assert stats.evictions == 1

        # Access first item (item-0) - should be cache miss as it was evicted
        s3_storage_with_cache.s3_client.get_object.reset_mock()
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="item-0",
            field_name="content",
            version=None,
        )
        assert s3_storage_with_cache.s3_client.get_object.call_count == 1

        # Access last item (item-5) - should be cache hit
        s3_storage_with_cache.s3_client.get_object.reset_mock()
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="item-5",
            field_name="content",
            version=None,
        )
        assert s3_storage_with_cache.s3_client.get_object.call_count == 0

    def test_lru_eviction_by_size(self, s3_storage_with_cache):
        """Test LRU eviction when max size is reached."""
        # Create large content (3KB each, cache max is 10KB)
        large_content = "X" * 3000

        # Put 4 items (12KB total, exceeds 10KB limit)
        for i in range(4):
            s3_storage_with_cache.put_blob(
                resource_type="TestResource",
                resource_id=f"large-{i}",
                field_name="content",
                value=large_content,
                config=BlobFieldConfig(compress=False),
                version=None,
            )

        # Check cache size is under limit
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_size_bytes <= 10 * 1024
        assert stats.current_items < 4  # Some items were evicted
        assert stats.evictions > 0

    def test_cache_item_too_large(self, s3_storage_with_cache):
        """Test that items larger than max_item_size are not cached."""
        # Create content larger than max_item_size (5KB)
        huge_content = "X" * 6000

        s3_storage_with_cache.put_blob(
            resource_type="TestResource",
            resource_id="huge-item",
            field_name="content",
            value=huge_content,
            config=BlobFieldConfig(compress=False),
            version=None,
        )

        # Item should not be in cache
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 0

        # Getting it should always hit S3
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="huge-item",
            field_name="content",
            version=None,
        )
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="huge-item",
            field_name="content",
            version=None,
        )
        assert s3_storage_with_cache.s3_client.get_object.call_count == 2

    def test_cache_with_compression(self, s3_storage_with_cache):
        """Test caching with compressed blobs."""
        # Large content that compresses well
        content = "A" * 1000

        s3_storage_with_cache.put_blob(
            resource_type="TestResource",
            resource_id="compressed",
            field_name="content",
            value=content,
            config=BlobFieldConfig(compress=True),
            version=None,
        )

        # First get
        result1 = s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="compressed",
            field_name="content",
            version=None,
        )
        assert result1 == content

        # Second get should hit cache
        s3_storage_with_cache.s3_client.get_object.reset_mock()
        result2 = s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="compressed",
            field_name="content",
            version=None,
        )
        assert result2 == content
        assert s3_storage_with_cache.s3_client.get_object.call_count == 0

    def test_cache_disabled(self, s3_storage_no_cache):
        """Test that cache is bypassed when disabled."""
        data = {"test": "nocache"}

        s3_storage_no_cache.put_blob(
            resource_type="TestResource",
            resource_id="no-cache",
            field_name="metadata",
            value=data,
            config=BlobFieldConfig(compress=False),
            version=None,
        )

        # Multiple gets should all hit S3
        for _ in range(3):
            result = s3_storage_no_cache.get_blob(
                resource_type="TestResource",
                resource_id="no-cache",
                field_name="metadata",
                version=None,
            )
            assert result == data

        assert s3_storage_no_cache.s3_client.get_object.call_count == 3

    def test_delete_blob_clears_cache(self, s3_storage_with_cache):
        """Test that deleting a blob removes it from cache."""
        data = "deleteme"

        # Put and get to populate cache
        s3_storage_with_cache.put_blob(
            resource_type="TestResource",
            resource_id="delete-test",
            field_name="content",
            value=data,
            config=BlobFieldConfig(compress=False),
            version=None,
        )
        s3_storage_with_cache.get_blob(
            resource_type="TestResource",
            resource_id="delete-test",
            field_name="content",
            version=None,
        )

        # Delete the blob
        s3_storage_with_cache.delete_blob(
            resource_type="TestResource",
            resource_id="delete-test",
            field_name="content",
            version=None,
        )

        # Check cache is empty
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 0

    def test_delete_all_blobs_clears_cache(self, s3_storage_with_cache):
        """Test that deleting all blobs for a resource clears them from cache."""
        # Put multiple blobs for same resource
        s3_storage_with_cache.put_blob(
            resource_type="TestResource",
            resource_id="multi-delete",
            field_name="field1",
            value="data1",
            config=BlobFieldConfig(compress=False),
            version=None,
        )
        s3_storage_with_cache.put_blob(
            resource_type="TestResource",
            resource_id="multi-delete",
            field_name="field2",
            value="data2",
            config=BlobFieldConfig(compress=False),
            version=None,
        )

        # Mock paginator for delete_all_blobs
        mock_paginator = Mock()
        mock_paginator.paginate = Mock(return_value=[])
        s3_storage_with_cache.s3_client.get_paginator = Mock(return_value=mock_paginator)

        # Delete all blobs
        s3_storage_with_cache.delete_all_blobs(
            resource_type="TestResource",
            resource_id="multi-delete",
        )

        # Check cache is empty
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 0

    def test_clear_cache(self, s3_storage_with_cache):
        """Test manual cache clearing."""
        # Populate cache
        for i in range(3):
            s3_storage_with_cache.put_blob(
                resource_type="TestResource",
                resource_id=f"clear-{i}",
                field_name="content",
                value=f"data_{i}",
                config=BlobFieldConfig(compress=False),
                version=None,
            )

        # Check cache has items
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 3

        # Clear cache
        s3_storage_with_cache.clear_cache()

        # Check cache is empty
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 0
        assert stats.current_size_bytes == 0

    def test_warm_cache(self, s3_storage_with_cache):
        """Test pre-warming the cache."""
        # Create items via put_blob (needed to properly store in mock)
        items_to_warm = []
        for i in range(3):
            s3_storage_with_cache.put_blob(
                resource_type="TestResource",
                resource_id=f"warm-{i}",
                field_name="content",
                value=f"warm_data_{i}",
                config=BlobFieldConfig(compress=False),
                version=None,
            )
            items_to_warm.append(("TestResource", f"warm-{i}", "content", None))

        # Clear cache before warming
        s3_storage_with_cache.clear_cache()
        s3_storage_with_cache.s3_client.get_object.reset_mock()

        # Warm the cache
        loaded = s3_storage_with_cache.warm_cache(items_to_warm)
        assert loaded == 3

        # Check cache has the items
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 3
        assert stats.misses == 3  # All were misses during warming

        # Getting items should now hit cache
        s3_storage_with_cache.s3_client.get_object.reset_mock()
        for i in range(3):
            result = s3_storage_with_cache.get_blob(
                resource_type="TestResource",
                resource_id=f"warm-{i}",
                field_name="content",
                version=None,
            )
            assert result == f"warm_data_{i}"

        # Should not have called S3 again
        assert s3_storage_with_cache.s3_client.get_object.call_count == 0

    def test_cache_versioned_blobs(self, s3_storage_with_cache):
        """Test caching of versioned blobs."""
        # Put versioned blobs
        s3_storage_with_cache.put_blob(
            resource_type="VersionedResource",
            resource_id="v-123",
            field_name="content",
            value="version_1",
            config=BlobFieldConfig(compress=False),
            version=1,
        )
        s3_storage_with_cache.put_blob(
            resource_type="VersionedResource",
            resource_id="v-123",
            field_name="content",
            value="version_2",
            config=BlobFieldConfig(compress=False),
            version=2,
        )

        # Get different versions
        v1 = s3_storage_with_cache.get_blob(
            resource_type="VersionedResource",
            resource_id="v-123",
            field_name="content",
            version=1,
        )
        v2 = s3_storage_with_cache.get_blob(
            resource_type="VersionedResource",
            resource_id="v-123",
            field_name="content",
            version=2,
        )

        assert v1 == "version_1"
        assert v2 == "version_2"

        # Both should be in cache
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items == 2

        # Getting them again should hit cache
        s3_storage_with_cache.s3_client.get_object.reset_mock()
        v1_again = s3_storage_with_cache.get_blob(
            resource_type="VersionedResource",
            resource_id="v-123",
            field_name="content",
            version=1,
        )
        assert v1_again == "version_1"
        assert s3_storage_with_cache.s3_client.get_object.call_count == 0

    def test_get_cache_info(self, s3_storage_with_cache):
        """Test getting detailed cache information."""
        # Populate cache with some data
        for i in range(3):
            s3_storage_with_cache.put_blob(
                resource_type="TestResource",
                resource_id=f"info-{i}",
                field_name="content",
                value=f"data_{i}",
                config=BlobFieldConfig(compress=False),
                version=None,
            )

        # Access some items to increase access count
        for _ in range(5):
            s3_storage_with_cache.get_blob(
                resource_type="TestResource",
                resource_id="info-1",
                field_name="content",
                version=None,
            )

        info = s3_storage_with_cache.get_cache_info()

        assert info["enabled"] is True
        assert info["max_items"] == 5
        assert info["stats"]["current_items"] == 3
        assert len(info["top_accessed_items"]) == 3

        # Check that info-1 has highest access count
        top_item = info["top_accessed_items"][0]
        assert "info-1" in top_item["key"]
        assert top_item["access_count"] > 0

    def test_thread_safety(self, s3_storage_with_cache):
        """Test cache thread safety with concurrent access."""
        import threading

        def put_and_get(thread_id):
            for i in range(10):
                # Put blob
                s3_storage_with_cache.put_blob(
                    resource_type="TestResource",
                    resource_id=f"thread-{thread_id}-{i}",
                    field_name="content",
                    value=f"data_{thread_id}_{i}",
                    config=BlobFieldConfig(compress=False),
                    version=None,
                )
                # Get blob
                s3_storage_with_cache.get_blob(
                    resource_type="TestResource",
                    resource_id=f"thread-{thread_id}-{i}",
                    field_name="content",
                    version=None,
                )

        # Run multiple threads
        threads = []
        for tid in range(5):
            thread = threading.Thread(target=put_and_get, args=(tid,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Check cache consistency
        stats = s3_storage_with_cache.get_cache_stats()
        assert stats.current_items <= 5  # Should respect max items
        assert stats.current_size_bytes <= 10 * 1024  # Should respect max size
