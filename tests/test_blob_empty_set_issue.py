"""
Test for blob fields containing list[BaseModel] where the BaseModel has set fields with empty sets.

This test reproduces a specific issue where:
1. A versioned resource has a blob field of type list[BaseModel]
2. The BaseModel contains a set field
3. When the set is empty (set()), serialization/deserialization may fail
4. Upon loading, the TypeAdapter may fail to reconstruct the empty set properly
"""

from typing import Optional
from pydantic import BaseModel, Field

from simplesingletable import DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig


# Pydantic model with a set field
class Item(BaseModel):
    """Example model with a set field."""

    name: str
    description: str
    tags: set[str] = Field(default_factory=set)
    metadata: dict = {}


class ItemWithOptionalSet(BaseModel):
    """Model with optional set field."""

    item_id: str
    categories: Optional[set[str]] = None
    flags: set[str] = set()


# Versioned resource with blob field containing list of models with sets
class ItemCollectionVersioned(DynamoDbVersionedResource):
    """Versioned resource with list[Item] as blob field (compressed)."""

    collection_name: str
    status: str
    items: Optional[list[Item]] = None  # Blob field

    resource_config = ResourceConfig(
        compress_data=True,
        max_versions=5,
        blob_fields={
            "items": BlobFieldConfig(
                compress=True,
                content_type="application/json",
            ),
        },
    )


class ItemCollectionUncompressed(DynamoDbVersionedResource):
    """Versioned resource with list[Item] as blob field (uncompressed)."""

    collection_name: str
    status: str
    items: Optional[list[ItemWithOptionalSet]] = None  # Blob field

    resource_config = ResourceConfig(
        compress_data=False,
        max_versions=3,
        blob_fields={
            "items": BlobFieldConfig(
                compress=False,
                content_type="application/json",
            ),
        },
    )


class TestEmptySetInBlobField:
    """Test handling of empty sets in Pydantic models within blob fields."""

    def test_empty_set_in_nested_model_compressed(self, dynamodb_memory_with_s3):
        """Test that empty sets in nested models are properly handled (compressed blob)."""
        memory = dynamodb_memory_with_s3

        # Create items with empty and non-empty sets
        items = [
            Item(
                name="Item 1",
                description="First item with empty tags",
                tags=set(),  # Empty set - this is the problematic case
                metadata={"priority": "high"},
            ),
            Item(
                name="Item 2",
                description="Second item with tags",
                tags={"tag1", "tag2", "tag3"},  # Non-empty set
                metadata={"priority": "low"},
            ),
            Item(
                name="Item 3",
                description="Third item with empty tags",
                tags=set(),  # Another empty set
                metadata={},
            ),
        ]

        # Create versioned resource with blob containing items
        collection = memory.create_new(
            ItemCollectionVersioned,
            {
                "collection_name": "Test Collection",
                "status": "active",
                "items": items,
            },
        )

        assert collection.version == 1
        assert collection.resource_id

        # Load without blobs first
        loaded_no_blobs = memory.get_existing(collection.resource_id, ItemCollectionVersioned, load_blobs=False)

        assert loaded_no_blobs.items is None
        assert loaded_no_blobs.has_unloaded_blobs()

        # Clear the blob cache to force reading from S3
        # This reveals the serialization issue with empty sets
        memory.s3_blob_storage.clear_cache()

        # Load with blobs - this is where the issue occurs
        loaded = memory.get_existing(collection.resource_id, ItemCollectionVersioned, load_blobs=True)

        # Verify items were loaded correctly
        assert loaded.items is not None
        assert len(loaded.items) == 3

        # Verify empty sets are properly reconstructed
        assert isinstance(loaded.items[0], Item)
        assert loaded.items[0].name == "Item 1"
        assert loaded.items[0].tags == set()  # Should be empty set, not string "set()"
        assert isinstance(loaded.items[0].tags, set)

        # Verify non-empty set is correct
        assert loaded.items[1].tags == {"tag1", "tag2", "tag3"}
        assert isinstance(loaded.items[1].tags, set)

        # Verify second empty set
        assert loaded.items[2].tags == set()
        assert isinstance(loaded.items[2].tags, set)

        # Verify metadata is preserved
        assert loaded.items[0].metadata == {"priority": "high"}
        assert loaded.items[1].metadata == {"priority": "low"}

    def test_empty_set_in_nested_model_uncompressed(self, dynamodb_memory_with_s3):
        """Test that empty sets in nested models are properly handled (uncompressed blob)."""
        memory = dynamodb_memory_with_s3

        # Create items with various set configurations
        items = [
            ItemWithOptionalSet(
                item_id="item-1",
                categories=None,  # None set
                flags=set(),  # Empty set
            ),
            ItemWithOptionalSet(
                item_id="item-2",
                categories={"cat1", "cat2"},  # Non-empty set
                flags=set(),  # Empty set
            ),
            ItemWithOptionalSet(
                item_id="item-3",
                categories=set(),  # Empty set (not None)
                flags={"flag1"},  # Non-empty set
            ),
        ]

        # Create resource
        collection = memory.create_new(
            ItemCollectionUncompressed,
            {
                "collection_name": "Uncompressed Collection",
                "status": "draft",
                "items": items,
            },
        )

        # Clear cache to force reading from S3
        memory.s3_blob_storage.clear_cache()

        # Load with blobs
        loaded = memory.get_existing(collection.resource_id, ItemCollectionUncompressed, load_blobs=True)

        assert loaded.items is not None
        assert len(loaded.items) == 3

        # Verify None vs empty set distinction
        assert loaded.items[0].categories is None
        assert loaded.items[0].flags == set()
        assert isinstance(loaded.items[0].flags, set)

        # Verify mixed sets
        assert loaded.items[1].categories == {"cat1", "cat2"}
        assert loaded.items[1].flags == set()

        # Verify empty set (not None)
        assert loaded.items[2].categories == set()
        assert isinstance(loaded.items[2].categories, set)
        assert loaded.items[2].flags == {"flag1"}

    def test_update_with_empty_sets(self, dynamodb_memory_with_s3):
        """Test updating a versioned resource with empty sets in blob fields."""
        memory = dynamodb_memory_with_s3

        # Create initial version
        initial_items = [
            Item(name="Item A", description="Initial", tags={"old-tag"}),
        ]

        collection = memory.create_new(
            ItemCollectionVersioned,
            {
                "collection_name": "Evolving Collection",
                "status": "active",
                "items": initial_items,
            },
        )

        # Update with items that have empty sets
        updated_items = [
            Item(name="Item A", description="Updated", tags=set()),  # Cleared tags
            Item(name="Item B", description="New item", tags=set()),  # New with empty tags
        ]

        updated = memory.update_existing(collection, {"items": updated_items, "status": "updated"})

        assert updated.version == 2

        # Clear cache to force reading from S3
        memory.s3_blob_storage.clear_cache()

        # Load v1 and verify original
        v1 = memory.get_existing(collection.resource_id, ItemCollectionVersioned, version=1, load_blobs=True)

        assert len(v1.items) == 1
        assert v1.items[0].tags == {"old-tag"}

        # Clear cache again
        memory.s3_blob_storage.clear_cache()

        # Load v2 and verify empty sets
        v2 = memory.get_existing(collection.resource_id, ItemCollectionVersioned, version=2, load_blobs=True)

        assert len(v2.items) == 2
        assert v2.items[0].tags == set()
        assert isinstance(v2.items[0].tags, set)
        assert v2.items[1].tags == set()
        assert isinstance(v2.items[1].tags, set)

    def test_only_empty_sets(self, dynamodb_memory_with_s3):
        """Test extreme case where all items have only empty sets."""
        memory = dynamodb_memory_with_s3

        # Create items with only empty sets
        items = [Item(name=f"Item {i}", description=f"Desc {i}", tags=set()) for i in range(5)]

        collection = memory.create_new(
            ItemCollectionVersioned,
            {
                "collection_name": "All Empty Sets",
                "status": "test",
                "items": items,
            },
        )

        # Clear cache to force reading from S3
        memory.s3_blob_storage.clear_cache()

        # Load and verify
        loaded = memory.get_existing(collection.resource_id, ItemCollectionVersioned, load_blobs=True)

        assert len(loaded.items) == 5
        for item in loaded.items:
            assert isinstance(item, Item)
            assert item.tags == set()
            assert isinstance(item.tags, set)


class TestEmptySetEdgeCases:
    """Test edge cases related to empty sets in blob fields."""

    def test_single_item_with_empty_set(self, dynamodb_memory_with_s3):
        """Test simplest case: single item with empty set."""
        memory = dynamodb_memory_with_s3

        items = [
            Item(name="Solo", description="Single item", tags=set()),
        ]

        collection = memory.create_new(
            ItemCollectionVersioned,
            {
                "collection_name": "Single Item",
                "status": "active",
                "items": items,
            },
        )

        # Clear cache to force reading from S3
        memory.s3_blob_storage.clear_cache()

        loaded = memory.get_existing(collection.resource_id, ItemCollectionVersioned, load_blobs=True)

        assert len(loaded.items) == 1
        assert loaded.items[0].tags == set()
        assert isinstance(loaded.items[0].tags, set)

    def test_empty_list_vs_list_with_empty_sets(self, dynamodb_memory_with_s3):
        """Test distinction between empty list and list containing items with empty sets."""
        memory = dynamodb_memory_with_s3

        # Create with empty list
        collection1 = memory.create_new(
            ItemCollectionVersioned,
            {
                "collection_name": "Empty List",
                "status": "active",
                "items": [],
            },
        )

        # Create with list containing item with empty set
        collection2 = memory.create_new(
            ItemCollectionVersioned,
            {
                "collection_name": "List With Empty Sets",
                "status": "active",
                "items": [Item(name="Item", description="Desc", tags=set())],
            },
        )

        # Clear cache to force reading from S3
        memory.s3_blob_storage.clear_cache()

        # Load both
        loaded1 = memory.get_existing(collection1.resource_id, ItemCollectionVersioned, load_blobs=True)
        loaded2 = memory.get_existing(collection2.resource_id, ItemCollectionVersioned, load_blobs=True)

        # Verify distinction
        assert loaded1.items == []
        assert len(loaded2.items) == 1
        assert loaded2.items[0].tags == set()
