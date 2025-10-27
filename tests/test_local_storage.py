"""Tests for local storage implementation."""
import tempfile
from pathlib import Path
from typing import ClassVar, Optional

import pytest
from boto3.dynamodb.conditions import Key
from logzero import logger
from pydantic import BaseModel

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource, LocalStorageMemory
from simplesingletable.models import BlobFieldConfig, ResourceConfig


class SimpleResource(DynamoDbResource):
    """Simple non-versioned resource for testing."""

    name: str
    value: int


class SimpleVersionedResource(DynamoDbVersionedResource):
    """Simple versioned resource for testing."""

    title: str
    content: str


class ResourceWithGSI(DynamoDbResource):
    """Resource with GSI for testing queries."""

    name: str
    category: str

    def db_get_gsi1pk(self) -> str | None:
        return f"category#{self.category}"


class PydanticModel(BaseModel):
    """Nested Pydantic model for blob testing."""

    name: str
    value: int


class ResourceWithBlobs(DynamoDbResource):
    """Resource with blob fields for testing."""

    name: str
    data: Optional[list[PydanticModel]] = None
    large_text: Optional[str] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={
            "data": BlobFieldConfig(compress=True),
            "large_text": BlobFieldConfig(compress=False),
        },
    )


class VersionedResourceWithBlobs(DynamoDbVersionedResource):
    """Versioned resource with blob fields."""

    title: str
    document: Optional[str] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        blob_fields={
            "document": BlobFieldConfig(compress=True),
        },
    )


@pytest.fixture
def local_storage():
    """Create a temporary local storage instance."""
    with tempfile.TemporaryDirectory() as tmpdir:
        memory = LocalStorageMemory(
            logger=logger,
            storage_dir=tmpdir,
            track_stats=True,
            use_blob_storage=True,
        )
        yield memory


def test_local_storage__basic_create_read(local_storage: LocalStorageMemory):
    """Test basic create and read operations."""
    # Create a resource
    resource = local_storage.create_new(
        SimpleResource,
        {"name": "test", "value": 42},
    )

    # Read it back
    retrieved = local_storage.read_existing(resource.resource_id, SimpleResource)
    assert retrieved.name == "test"
    assert retrieved.value == 42
    assert retrieved.resource_id == resource.resource_id


def test_local_storage__update_resource(local_storage: LocalStorageMemory):
    """Test updating a resource."""
    # Create
    resource = local_storage.create_new(
        SimpleResource,
        {"name": "original", "value": 1},
    )

    # Update
    updated = local_storage.update_existing(
        resource,
        {"name": "updated", "value": 2},
    )

    # Verify
    assert updated.name == "updated"
    assert updated.value == 2

    # Read back
    retrieved = local_storage.read_existing(resource.resource_id, SimpleResource)
    assert retrieved.name == "updated"
    assert retrieved.value == 2


def test_local_storage__delete_resource(local_storage: LocalStorageMemory):
    """Test deleting a resource."""
    # Create
    resource = local_storage.create_new(
        SimpleResource,
        {"name": "to_delete", "value": 1},
    )

    # Verify exists
    assert local_storage.get_existing(resource.resource_id, SimpleResource) is not None

    # Delete
    local_storage.delete_existing(resource)

    # Verify deleted
    assert local_storage.get_existing(resource.resource_id, SimpleResource) is None


def test_local_storage__versioned_resource(local_storage: LocalStorageMemory):
    """Test versioned resource operations."""
    # Create version 1
    resource = local_storage.create_new(
        SimpleVersionedResource,
        {"title": "v1", "content": "content v1"},
    )
    assert resource.version == 1

    # Update to create version 2
    updated = local_storage.update_existing(
        resource,
        {"title": "v2", "content": "content v2"},
    )
    assert updated.version == 2

    # Read version 1
    v1 = local_storage.read_existing(resource.resource_id, SimpleVersionedResource, version=1)
    assert v1.title == "v1"
    assert v1.content == "content v1"

    # Read version 2
    v2 = local_storage.read_existing(resource.resource_id, SimpleVersionedResource, version=2)
    assert v2.title == "v2"
    assert v2.content == "content v2"

    # Read latest (should be v2)
    latest = local_storage.read_existing(resource.resource_id, SimpleVersionedResource, version=0)
    assert latest.version == 2


def test_local_storage__get_all_versions(local_storage: LocalStorageMemory):
    """Test getting all versions of a resource."""
    # Create and update multiple times
    resource = local_storage.create_new(
        SimpleVersionedResource,
        {"title": "v1", "content": "content v1"},
    )

    for i in range(2, 5):
        resource = local_storage.update_existing(
            resource,
            {"title": f"v{i}", "content": f"content v{i}"},
        )

    # Get all versions
    versions = local_storage.get_all_versions(resource.resource_id, SimpleVersionedResource)

    # Should have versions 1-4, sorted newest first
    assert len(versions) == 4
    assert [v.version for v in versions] == [4, 3, 2, 1]
    assert versions[0].title == "v4"
    assert versions[3].title == "v1"


def test_local_storage__restore_version(local_storage: LocalStorageMemory):
    """Test restoring an old version."""
    # Create versions
    resource = local_storage.create_new(
        SimpleVersionedResource,
        {"title": "v1", "content": "original content"},
    )

    local_storage.update_existing(
        resource,
        {"title": "v2", "content": "modified content"},
    )

    # Restore v1
    restored = local_storage.restore_version(
        resource.resource_id,
        SimpleVersionedResource,
        version=1,
    )

    # Should be version 3 with v1's content
    assert restored.version == 3
    assert restored.title == "v1"
    assert restored.content == "original content"


def test_local_storage__delete_all_versions(local_storage: LocalStorageMemory):
    """Test deleting all versions of a resource."""
    # Create versions
    resource = local_storage.create_new(
        SimpleVersionedResource,
        {"title": "v1", "content": "content"},
    )

    for i in range(2, 4):
        resource = local_storage.update_existing(
            resource,
            {"title": f"v{i}", "content": "content"},
        )

    # Delete all versions
    local_storage.delete_all_versions(resource.resource_id, SimpleVersionedResource)

    # Verify all deleted
    assert local_storage.get_existing(resource.resource_id, SimpleVersionedResource) is None
    assert len(local_storage.get_all_versions(resource.resource_id, SimpleVersionedResource)) == 0


def test_local_storage__query_by_type(local_storage: LocalStorageMemory):
    """Test querying resources by type."""
    # Create multiple resources
    r1 = local_storage.create_new(SimpleResource, {"name": "first", "value": 1})
    r2 = local_storage.create_new(SimpleResource, {"name": "second", "value": 2})
    r3 = local_storage.create_new(SimpleResource, {"name": "third", "value": 3})

    # Query all
    results = local_storage.list_type_by_updated_at(SimpleResource, results_limit=10)

    assert len(results) == 3
    # Should include all three resources
    resource_ids = [r.resource_id for r in results]
    assert r1.resource_id in resource_ids
    assert r2.resource_id in resource_ids
    assert r3.resource_id in resource_ids


def test_local_storage__query_with_filter_fn(local_storage: LocalStorageMemory):
    """Test querying with a filter function."""
    # Create resources with different values
    local_storage.create_new(SimpleResource, {"name": "low", "value": 1})
    high = local_storage.create_new(SimpleResource, {"name": "high", "value": 100})

    # Query with filter
    results = local_storage.list_type_by_updated_at(
        SimpleResource, filter_fn=lambda r: r.value > 50, results_limit=10
    )

    assert len(results) == 1
    assert results[0].resource_id == high.resource_id


def test_local_storage__pagination(local_storage: LocalStorageMemory):
    """Test pagination of query results."""
    # Create 5 resources
    for i in range(5):
        local_storage.create_new(SimpleResource, {"name": f"item_{i}", "value": i})

    # Get first page
    page1 = local_storage.list_type_by_updated_at(SimpleResource, results_limit=2)
    assert len(page1) == 2
    assert page1.next_pagination_key is not None

    # Get second page
    page2 = local_storage.list_type_by_updated_at(SimpleResource, results_limit=2, pagination_key=page1.next_pagination_key)
    assert len(page2) == 2
    assert page2.next_pagination_key is not None

    # Get third page
    page3 = local_storage.list_type_by_updated_at(SimpleResource, results_limit=2, pagination_key=page2.next_pagination_key)
    assert len(page3) == 1  # Only 1 item left
    assert page3.next_pagination_key is None


def test_local_storage__gsi_query(local_storage: LocalStorageMemory):
    """Test querying by GSI."""
    # Create resources with different categories
    cat1_r1 = local_storage.create_new(ResourceWithGSI, {"name": "item1", "category": "A"})
    cat1_r2 = local_storage.create_new(ResourceWithGSI, {"name": "item2", "category": "A"})
    cat2_r1 = local_storage.create_new(ResourceWithGSI, {"name": "item3", "category": "B"})

    # Query by category A
    results = local_storage.paginated_dynamodb_query(
        resource_class=ResourceWithGSI,
        index_name="gsi1",
        key_condition=Key("gsi1pk").eq("category#A"),
        results_limit=10,
    )

    assert len(results) == 2
    resource_ids = [r.resource_id for r in results]
    assert cat1_r1.resource_id in resource_ids
    assert cat1_r2.resource_id in resource_ids
    assert cat2_r1.resource_id not in resource_ids


def test_local_storage__blob_storage(local_storage: LocalStorageMemory):
    """Test blob field storage and retrieval."""
    # Create resource with blob data
    blob_data = [
        PydanticModel(name="item1", value=1),
        PydanticModel(name="item2", value=2),
    ]

    resource = local_storage.create_new(
        ResourceWithBlobs,
        {
            "name": "test_blob",
            "data": blob_data,
            "large_text": "This is a large text field",
        },
    )

    # Read back without loading blobs
    retrieved = local_storage.read_existing(resource.resource_id, ResourceWithBlobs, load_blobs=False)
    assert retrieved.has_unloaded_blobs()
    assert retrieved.data is None
    assert retrieved.large_text is None

    # Read with loading blobs
    retrieved_with_blobs = local_storage.read_existing(resource.resource_id, ResourceWithBlobs, load_blobs=True)
    assert not retrieved_with_blobs.has_unloaded_blobs()
    assert len(retrieved_with_blobs.data) == 2
    assert retrieved_with_blobs.data[0].name == "item1"
    assert retrieved_with_blobs.large_text == "This is a large text field"


def test_local_storage__versioned_blob_storage(local_storage: LocalStorageMemory):
    """Test blob storage with versioned resources."""
    # Create resource with blob
    resource = local_storage.create_new(
        VersionedResourceWithBlobs,
        {
            "title": "v1",
            "document": "Document content v1",
        },
    )

    # Update with new blob content
    local_storage.update_existing(
        resource,
        {
            "title": "v2",
            "document": "Document content v2",
        },
    )

    # Read v1 with blobs
    v1 = local_storage.read_existing(resource.resource_id, VersionedResourceWithBlobs, version=1, load_blobs=True)
    assert v1.document == "Document content v1"

    # Read v2 with blobs
    v2 = local_storage.read_existing(resource.resource_id, VersionedResourceWithBlobs, version=2, load_blobs=True)
    assert v2.document == "Document content v2"


def test_local_storage__increment_counter(local_storage: LocalStorageMemory):
    """Test incrementing counter fields."""
    resource = local_storage.create_new(
        SimpleResource,
        {"name": "counter_test", "value": 0},
    )

    # Increment
    new_value = local_storage.increment_counter(resource, "value", 5)
    assert new_value == 5

    # Read back
    retrieved = local_storage.read_existing(resource.resource_id, SimpleResource)
    assert retrieved.value == 5


def test_local_storage__stats_tracking(local_storage: LocalStorageMemory):
    """Test resource count statistics."""
    # Create some resources
    local_storage.create_new(SimpleResource, {"name": "r1", "value": 1})
    local_storage.create_new(SimpleResource, {"name": "r2", "value": 2})

    # Get stats
    stats = local_storage.get_stats()
    assert stats.counts_by_type["SimpleResource"] == 2

    # Create another type
    local_storage.create_new(SimpleVersionedResource, {"title": "v1", "content": "content"})

    stats = local_storage.get_stats()
    assert stats.counts_by_type["SimpleResource"] == 2
    assert stats.counts_by_type["SimpleVersionedResource"] == 1


def test_local_storage__file_persistence(local_storage: LocalStorageMemory):
    """Test that data persists to files and can be read back."""
    # Create a resource
    resource = local_storage.create_new(
        SimpleResource,
        {"name": "persistent", "value": 99},
    )

    # Check that file was created
    file_path = local_storage._get_resource_file_path(SimpleResource)
    assert file_path.exists()

    # Create a new instance pointing to same storage
    memory2 = LocalStorageMemory(
        logger=logger,
        storage_dir=local_storage.storage_dir,
        track_stats=True,
        use_blob_storage=True,
    )

    # Read the resource from the new instance
    retrieved = memory2.read_existing(resource.resource_id, SimpleResource)
    assert retrieved.name == "persistent"
    assert retrieved.value == 99


def test_local_storage__blob_file_persistence(local_storage: LocalStorageMemory):
    """Test that blob files persist correctly."""
    # Create resource with blob
    blob_data = [PydanticModel(name="persistent", value=123)]
    resource = local_storage.create_new(
        ResourceWithBlobs,
        {
            "name": "blob_persist_test",
            "data": blob_data,
        },
    )

    # Check that blob files were created
    blobs_dir = Path(local_storage.storage_dir) / "blobs"
    assert blobs_dir.exists()

    # Create new instance
    memory2 = LocalStorageMemory(
        logger=logger,
        storage_dir=local_storage.storage_dir,
        track_stats=True,
        use_blob_storage=True,
    )

    # Read with blobs loaded
    retrieved = memory2.read_existing(resource.resource_id, ResourceWithBlobs, load_blobs=True)
    assert len(retrieved.data) == 1
    assert retrieved.data[0].name == "persistent"
    assert retrieved.data[0].value == 123


def test_local_storage__get_nonexistent_resource(local_storage: LocalStorageMemory):
    """Test that getting a non-existent resource returns None."""
    result = local_storage.get_existing("nonexistent_id", SimpleResource)
    assert result is None


def test_local_storage__read_nonexistent_resource(local_storage: LocalStorageMemory):
    """Test that reading a non-existent resource raises ValueError."""
    with pytest.raises(ValueError, match="No item found"):
        local_storage.read_existing("nonexistent_id", SimpleResource)


def test_local_storage__update_from_wrong_version(local_storage: LocalStorageMemory):
    """Test that updating from a non-latest version raises an error."""
    # Create and update
    resource = local_storage.create_new(
        SimpleVersionedResource,
        {"title": "v1", "content": "content"},
    )

    # Get v1
    v1 = local_storage.read_existing(resource.resource_id, SimpleVersionedResource, version=1)

    # Update to create v2
    local_storage.update_existing(resource, {"title": "v2"})

    # Try to update from v1 (should fail)
    with pytest.raises(ValueError, match="Cannot update from non-latest version"):
        local_storage.update_existing(v1, {"title": "v3"})
