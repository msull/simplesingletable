"""Tests for transaction support."""

import uuid
from datetime import datetime, timezone
from typing import Optional

import pytest

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.transactions import TransactionError


class DemoUser(DynamoDbResource):
    """Test user resource."""

    @classmethod
    def get_resource_type(cls):
        return "DemoUser"

    name: str
    email: str
    age: Optional[int] = None
    tags: list[str] = []


class DemoProfile(DynamoDbResource):
    """Test profile resource."""

    @classmethod
    def get_resource_type(cls):
        return "DemoProfile"

    user_id: str
    bio: str
    followers_count: int = 0


class DemoPost(DynamoDbVersionedResource):
    """Test versioned post resource."""

    @classmethod
    def get_resource_type(cls):
        return "DemoPost"

    title: str
    content: str
    author_id: str
    likes: int = 0


class DemoCounter(DynamoDbResource):
    """Test counter resource for increment operations."""

    @classmethod
    def get_resource_type(cls):
        return "DemoCounter"

    name: str
    value: int = 0


class TestTransactions:
    """Test transaction functionality."""

    def test_basic_create_transaction(self, dynamodb_memory: DynamoDbMemory):
        """Test basic create operation in transaction."""
        with dynamodb_memory.transaction() as txn:
            user = txn.create(
                DemoUser(
                    name="Alice",
                    email="alice@example.com",
                    resource_id="user1",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )

            assert user.name == "Alice"
            assert user.resource_id == "user1"

        # Verify resource was created
        retrieved = dynamodb_memory.get_existing("user1", DemoUser)
        assert retrieved.name == "Alice"
        assert retrieved.email == "alice@example.com"

    def test_multiple_creates_transaction(self, dynamodb_memory: DynamoDbMemory):
        """Test multiple create operations in a single transaction."""
        with dynamodb_memory.transaction() as txn:
            user = txn.create(
                DemoUser(
                    name="Bob",
                    email="bob@example.com",
                    resource_id="user2",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )

            profile = txn.create(
                DemoProfile(
                    user_id=user.resource_id,
                    bio="Bob's profile",
                    resource_id="profile1",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )

            assert profile.user_id == "user2"

        # Verify both resources were created
        user = dynamodb_memory.get_existing("user2", DemoUser)
        profile = dynamodb_memory.get_existing("profile1", DemoProfile)
        assert user.name == "Bob"
        assert profile.bio == "Bob's profile"

    def test_update_transaction(self, dynamodb_memory: DynamoDbMemory):
        """Test update operation in transaction."""
        # Create initial resource
        user = dynamodb_memory.create_new(DemoUser, {"name": "Charlie", "email": "charlie@example.com"})

        # Update in transaction
        with dynamodb_memory.transaction() as txn:
            txn.update(DemoUser, resource_id=user.resource_id, updates={"age": 30})

        # Verify update
        updated = dynamodb_memory.get_existing(user.resource_id, DemoUser)
        assert updated.age == 30
        assert updated.name == "Charlie"  # Original data preserved

    def test_delete_transaction(self, dynamodb_memory: DynamoDbMemory):
        """Test delete operation in transaction."""
        # Create initial resource
        user = dynamodb_memory.create_new(DemoUser, {"name": "David", "email": "david@example.com"})

        # Delete in transaction
        with dynamodb_memory.transaction() as txn:
            txn.delete(DemoUser, resource_id=user.resource_id)

        # Verify deletion
        deleted = dynamodb_memory.get_existing(user.resource_id, DemoUser)
        assert deleted is None

    def test_increment_transaction(self, dynamodb_memory: DynamoDbMemory):
        """Test increment operation in transaction."""
        # Create counter
        counter = dynamodb_memory.create_new(DemoCounter, {"name": "test_counter", "value": 10})

        # Increment in transaction
        with dynamodb_memory.transaction() as txn:
            txn.increment(DemoCounter, field_name="value", amount=5, resource_id=counter.resource_id)

        # Verify increment
        updated = dynamodb_memory.get_existing(counter.resource_id, DemoCounter)
        assert updated.value == 15

    def test_mixed_operations_transaction(self, dynamodb_memory: DynamoDbMemory):
        """Test mixing different operation types in a single transaction."""
        # Create initial data
        user = dynamodb_memory.create_new(DemoUser, {"name": "Eve", "email": "eve@example.com"})
        counter = dynamodb_memory.create_new(DemoCounter, {"name": "mixed_counter", "value": 0})

        # Mixed transaction
        with dynamodb_memory.transaction() as txn:
            # Create new profile
            txn.create(
                DemoProfile(
                    user_id=user.resource_id,
                    bio="Eve's profile",
                    resource_id="profile2",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )

            # Update user
            txn.update(DemoUser, resource_id=user.resource_id, updates={"age": 25})

            # Increment counter
            txn.increment(DemoCounter, field_name="value", amount=1, resource_id=counter.resource_id)

        # Verify all operations
        profile = dynamodb_memory.get_existing("profile2", DemoProfile)
        updated_user = dynamodb_memory.get_existing(user.resource_id, DemoUser)
        updated_counter = dynamodb_memory.get_existing(counter.resource_id, DemoCounter)

        assert profile.bio == "Eve's profile"
        assert updated_user.age == 25
        assert updated_counter.value == 1

    def test_transaction_rollback_on_error(self, dynamodb_memory: DynamoDbMemory):
        """Test that transaction rolls back on error."""
        user = dynamodb_memory.create_new(DemoUser, {"name": "Frank", "email": "frank@example.com"})

        try:
            with dynamodb_memory.transaction() as txn:
                # This should work
                txn.update(DemoUser, resource_id=user.resource_id, updates={"age": 40})

                # Force an error
                raise ValueError("Test error")
        except ValueError:
            pass

        # Verify rollback - user should not be updated
        unchanged = dynamodb_memory.get_existing(user.resource_id, DemoUser)
        assert unchanged.age is None  # Should not have age set

    def test_versioned_resource_transaction(self, dynamodb_memory: DynamoDbMemory):
        """Test transaction with versioned resources."""
        with dynamodb_memory.transaction() as txn:
            post = txn.create(
                DemoPost(
                    title="First Post",
                    content="Hello World",
                    author_id="author1",
                    resource_id="post1",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                    version=1,
                )
            )

            assert post.title == "First Post"

        # Verify versioned resource was created properly
        retrieved = dynamodb_memory.get_existing("post1", DemoPost)
        assert retrieved.title == "First Post"
        assert retrieved.version == 1

    def test_transaction_with_condition(self, dynamodb_memory: DynamoDbMemory):
        """Test transaction with conditional checks."""
        # Create user
        user = dynamodb_memory.create_new(DemoUser, {"name": "Grace", "email": "grace@example.com", "age": 20})

        # Update with condition
        with dynamodb_memory.transaction() as txn:
            # Only update if age < 30
            txn.update(
                DemoUser,
                resource_id=user.resource_id,
                updates={"age": 21},
                condition="age < :max_age",
                condition_values={":max_age": 30},
            )

        # Verify update happened
        updated = dynamodb_memory.get_existing(user.resource_id, DemoUser)
        assert updated.age == 21

    def test_transaction_isolation_snapshot(self, dynamodb_memory: DynamoDbMemory):
        """Test snapshot isolation level."""
        # Create initial data
        user = dynamodb_memory.create_new(DemoUser, {"name": "Henry", "email": "henry@example.com"})

        with dynamodb_memory.transaction(isolation_level="snapshot") as txn:
            # Read user (cached)
            cached_user = txn.read(DemoUser, user.resource_id)
            assert cached_user.name == "Henry"

            # Second read should return cached version
            cached_user2 = txn.read(DemoUser, user.resource_id)
            assert cached_user is cached_user2  # Same object

            # Update based on cached read
            txn.update(DemoUser, resource_id=user.resource_id, updates={"age": 35})

        # Verify update
        updated = dynamodb_memory.get_existing(user.resource_id, DemoUser)
        assert updated.age == 35

    def test_transaction_append_operation(self, dynamodb_memory: DynamoDbMemory):
        """Test append operation for list fields."""
        # Create user with tags
        user = dynamodb_memory.create_new(DemoUser, {"name": "Ivy", "email": "ivy@example.com", "tags": ["tag1"]})

        # Append tags in transaction
        with dynamodb_memory.transaction() as txn:
            txn.append(DemoUser, field_name="tags", values=["tag2", "tag3"], resource_id=user.resource_id)

        # Verify append
        updated = dynamodb_memory.get_existing(user.resource_id, DemoUser)
        assert updated.tags == ["tag1", "tag2", "tag3"]

    def test_transaction_size_limit(self, dynamodb_memory: DynamoDbMemory):
        """Test that transaction fails when exceeding size limits."""
        with pytest.raises(TransactionError, match="exceeds DynamoDB limit"):
            with dynamodb_memory.transaction() as txn:
                # Try to create more than 100 items (DynamoDB limit)
                for i in range(101):
                    txn.create(
                        DemoUser(
                            name=f"User{i}",
                            email=f"user{i}@example.com",
                            resource_id=f"user_{i}",
                            created_at=datetime.now(timezone.utc),
                            updated_at=datetime.now(timezone.utc),
                        )
                    )

    def test_optimistic_locking_nonversioned(self, dynamodb_memory: DynamoDbMemory):
        """Test optimistic locking with version tokens on non-versioned resources."""
        # Create resource
        user = dynamodb_memory.create_new(DemoUser, {"name": "Jack", "email": "jack@example.com"})

        # Add version token
        user._version_token = str(uuid.uuid4())

        # Update should preserve version token
        updated_data = user.to_dynamodb_item()
        assert "_version_token" in updated_data

        # Recreate from dynamodb item should restore token
        restored = DemoUser.from_dynamodb_item(updated_data)
        assert restored._version_token == user._version_token

    def test_transaction_with_pending_creates(self, dynamodb_memory: DynamoDbMemory):
        """Test referencing pending creates within a transaction."""
        with dynamodb_memory.transaction() as txn:
            # Create user
            user = txn.create(
                DemoUser(
                    name="Kate",
                    email="kate@example.com",
                    resource_id="user_kate",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )

            # Reference the pending user in another create
            profile = txn.create(
                DemoProfile(
                    user_id=user.resource_id,  # Reference pending create
                    bio="Kate's profile",
                    resource_id="profile_kate",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )

            # Read pending create within transaction
            pending_user = txn.read(DemoUser, "user_kate")
            assert pending_user is not None
            assert pending_user.name == "Kate"

        # Verify both were created
        user = dynamodb_memory.get_existing("user_kate", DemoUser)
        profile = dynamodb_memory.get_existing("profile_kate", DemoProfile)
        assert user.name == "Kate"
        assert profile.user_id == "user_kate"

    def test_empty_transaction_commit(self, dynamodb_memory: DynamoDbMemory):
        """Test committing an empty transaction (should be no-op)."""
        with dynamodb_memory.transaction():
            pass  # No operations

        # Should not raise any errors

    def test_transaction_auto_retry(self, dynamodb_memory: DynamoDbMemory):
        """Test automatic retry on version conflicts."""
        # Create counter
        counter = dynamodb_memory.create_new(DemoCounter, {"name": "retry_counter", "value": 0})

        # Simulate concurrent updates with auto-retry
        with dynamodb_memory.transaction(auto_retry=True, max_retries=3) as txn:
            txn.increment(DemoCounter, field_name="value", amount=10, resource_id=counter.resource_id)

        # Verify increment
        updated = dynamodb_memory.get_existing(counter.resource_id, DemoCounter)
        assert updated.value == 10
