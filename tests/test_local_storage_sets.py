"""Tests for set handling in local storage."""
import tempfile

from logzero import logger

from simplesingletable import DynamoDbResource, LocalStorageMemory


class UserWithTags(DynamoDbResource):
    """User resource with set-based tags."""

    name: str
    email: str
    tags: set[str] = set()


def test_set_serialization():
    """Test that sets are properly serialized and deserialized."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorageMemory(
            logger=logger,
            storage_dir=tmpdir,
            track_stats=False,
        )

        # Create user with tags
        user = storage.create_new(
            UserWithTags,
            {
                "name": "Alice",
                "email": "alice@example.com",
                "tags": {"admin", "developer", "manager"},
            },
        )

        # Read back
        retrieved = storage.read_existing(user.resource_id, UserWithTags)

        # Verify tags are a set
        assert isinstance(retrieved.tags, set)
        assert retrieved.tags == {"admin", "developer", "manager"}


def test_empty_set():
    """Test that empty sets are handled correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorageMemory(
            logger=logger,
            storage_dir=tmpdir,
            track_stats=False,
        )

        # Create user with empty tags
        user = storage.create_new(
            UserWithTags,
            {
                "name": "Bob",
                "email": "bob@example.com",
                "tags": set(),
            },
        )

        # Read back
        retrieved = storage.read_existing(user.resource_id, UserWithTags)

        # Verify tags are a set (should be empty)
        assert isinstance(retrieved.tags, set)
        assert len(retrieved.tags) == 0


def test_set_update():
    """Test updating a set field."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorageMemory(
            logger=logger,
            storage_dir=tmpdir,
            track_stats=False,
        )

        # Create user
        user = storage.create_new(
            UserWithTags,
            {
                "name": "Charlie",
                "email": "charlie@example.com",
                "tags": {"developer"},
            },
        )

        # Update tags
        updated = storage.update_existing(
            user,
            {"tags": {"developer", "senior", "team-lead"}},
        )

        # Verify
        assert isinstance(updated.tags, set)
        assert updated.tags == {"developer", "senior", "team-lead"}

        # Read back to double-check persistence
        retrieved = storage.read_existing(user.resource_id, UserWithTags)
        assert isinstance(retrieved.tags, set)
        assert retrieved.tags == {"developer", "senior", "team-lead"}


def test_set_with_numbers():
    """Test sets containing different types."""

    class ResourceWithMixedSet(DynamoDbResource):
        name: str
        numbers: set[int] = set()

    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorageMemory(
            logger=logger,
            storage_dir=tmpdir,
            track_stats=False,
        )

        # Create resource with number set
        resource = storage.create_new(
            ResourceWithMixedSet,
            {
                "name": "test",
                "numbers": {1, 2, 3, 5, 8, 13},
            },
        )

        # Read back
        retrieved = storage.read_existing(resource.resource_id, ResourceWithMixedSet)

        assert isinstance(retrieved.numbers, set)
        assert retrieved.numbers == {1, 2, 3, 5, 8, 13}
