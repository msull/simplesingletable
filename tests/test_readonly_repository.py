"""Tests for the read-only repository classes."""

import pytest
from typing import Optional
from pydantic import BaseModel

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.extras.repository import ResourceRepository
from simplesingletable.extras.versioned_repository import VersionedResourceRepository
from simplesingletable.extras.readonly_repository import ReadOnlyResourceRepository
from simplesingletable.extras.readonly_versioned_repository import ReadOnlyVersionedResourceRepository, VersionInfo


class User(DynamoDbResource):
    """Test user model."""

    name: str
    email: str
    age: Optional[int] = None


class CreateUserSchema(BaseModel):
    """Schema for creating users."""

    name: str
    email: str
    age: Optional[int] = None


class UpdateUserSchema(BaseModel):
    """Schema for updating users."""

    name: Optional[str] = None
    email: Optional[str] = None
    age: Optional[int] = None


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
def writable_user_repo(memory):
    """Create a writable user repository for setting up test data."""
    return ResourceRepository(
        ddb=memory,
        model_class=User,
        create_schema_class=CreateUserSchema,
        update_schema_class=UpdateUserSchema,
    )


@pytest.fixture
def readonly_user_repo(memory):
    """Create a read-only user repository."""
    return ReadOnlyResourceRepository(
        ddb=memory,
        model_class=User,
    )


@pytest.fixture
def writable_doc_repo(memory):
    """Create a writable document repository for setting up test data."""
    return VersionedResourceRepository(
        ddb=memory,
        model_class=Document,
        create_schema_class=CreateDocumentSchema,
        update_schema_class=UpdateDocumentSchema,
    )


@pytest.fixture
def readonly_doc_repo(memory):
    """Create a read-only versioned document repository."""
    return ReadOnlyVersionedResourceRepository(
        ddb=memory,
        model_class=Document,
    )


class TestReadOnlyResourceRepository:
    """Test suite for ReadOnlyResourceRepository."""

    def test_get_existing_user(self, writable_user_repo, readonly_user_repo):
        """Test retrieving an existing user with read-only repository."""
        # Create user with writable repo
        user_data = {"name": "Alice", "email": "alice@example.com", "age": 30}
        created_user = writable_user_repo.create(user_data)

        # Retrieve with read-only repo
        retrieved_user = readonly_user_repo.get(created_user.resource_id)
        assert retrieved_user is not None
        assert retrieved_user.resource_id == created_user.resource_id
        assert retrieved_user.name == "Alice"
        assert retrieved_user.email == "alice@example.com"
        assert retrieved_user.age == 30

    def test_get_nonexistent_user(self, readonly_user_repo):
        """Test retrieving a non-existent user returns None."""
        result = readonly_user_repo.get("nonexistent-id")
        assert result is None

    def test_read_existing_user(self, writable_user_repo, readonly_user_repo):
        """Test reading an existing user with read-only repository."""
        # Create user with writable repo
        user_data = {"name": "Bob", "email": "bob@example.com"}
        created_user = writable_user_repo.create(user_data)

        # Read with read-only repo
        read_user = readonly_user_repo.read(created_user.resource_id)
        assert read_user.resource_id == created_user.resource_id
        assert read_user.name == "Bob"
        assert read_user.email == "bob@example.com"

    def test_read_nonexistent_user_raises_error(self, readonly_user_repo):
        """Test reading a non-existent user raises ValueError."""
        with pytest.raises(ValueError, match="User with id nonexistent-id not found"):
            readonly_user_repo.read("nonexistent-id")

    def test_list_users(self, writable_user_repo, readonly_user_repo):
        """Test listing users with read-only repository."""
        # Create multiple users with writable repo
        users_data = [
            {"name": "User1", "email": "user1@example.com", "age": 25},
            {"name": "User2", "email": "user2@example.com", "age": 30},
            {"name": "User3", "email": "user3@example.com", "age": 35},
        ]
        created_users = []
        for ud in users_data:
            created_users.append(writable_user_repo.create(ud))

        # List all users with read-only repo
        all_users = readonly_user_repo.list()
        assert len(all_users) == 3

        # List with limit
        limited_users = readonly_user_repo.list(limit=2)
        assert len(limited_users) == 2

        # Verify data integrity
        user_names = {user.name for user in all_users}
        assert user_names == {"User1", "User2", "User3"}

    def test_list_empty(self, readonly_user_repo):
        """Test listing when no users exist."""
        users = readonly_user_repo.list()
        assert users == []

    def test_no_create_method(self, readonly_user_repo):
        """Test that create method doesn't exist on read-only repository."""
        assert not hasattr(readonly_user_repo, "create")

    def test_no_update_method(self, readonly_user_repo):
        """Test that update method doesn't exist on read-only repository."""
        assert not hasattr(readonly_user_repo, "update")

    def test_no_delete_method(self, readonly_user_repo):
        """Test that delete method doesn't exist on read-only repository."""
        assert not hasattr(readonly_user_repo, "delete")

    def test_no_get_or_create_method(self, readonly_user_repo):
        """Test that get_or_create method doesn't exist on read-only repository."""
        assert not hasattr(readonly_user_repo, "get_or_create")

    def test_logger_configuration(self, memory):
        """Test custom logger configuration."""
        import logging

        custom_logger = logging.getLogger("custom_test_logger")

        repo = ReadOnlyResourceRepository(
            ddb=memory,
            model_class=User,
            logger=custom_logger,
        )

        assert repo.logger == custom_logger

    def test_data_consistency(self, writable_user_repo, readonly_user_repo):
        """Test that read-only repo reflects changes made by writable repo."""
        # Create initial user
        user_data = {"name": "Charlie", "email": "charlie@example.com", "age": 25}
        created_user = writable_user_repo.create(user_data)

        # Verify read-only repo sees it
        read_user = readonly_user_repo.get(created_user.resource_id)
        assert read_user.name == "Charlie"
        assert read_user.age == 25

        # Update with writable repo
        writable_user_repo.update(created_user.resource_id, {"age": 26})

        # Verify read-only repo sees the update
        updated_user = readonly_user_repo.get(created_user.resource_id)
        assert updated_user.age == 26

        # Delete with writable repo
        writable_user_repo.delete(created_user.resource_id)

        # Verify read-only repo sees the deletion
        deleted_user = readonly_user_repo.get(created_user.resource_id)
        assert deleted_user is None


class TestReadOnlyVersionedResourceRepository:
    """Test suite for ReadOnlyVersionedResourceRepository."""

    def test_init_requires_versioned_resource(self, memory):
        """Test that repository requires a versioned resource model."""
        with pytest.raises(ValueError, match="can only be used with DynamoDbVersionedResource"):
            ReadOnlyVersionedResourceRepository(
                ddb=memory,
                model_class=User,  # Non-versioned model
            )

    def test_get_and_read_document(self, writable_doc_repo, readonly_doc_repo):
        """Test basic get and read operations on versioned documents."""
        # Create document with writable repo
        doc_data = {"title": "Test Document", "content": "Test content", "tags": ["test"]}
        created_doc = writable_doc_repo.create(doc_data)

        # Get with read-only repo
        retrieved_doc = readonly_doc_repo.get(created_doc.resource_id)
        assert retrieved_doc is not None
        assert retrieved_doc.title == "Test Document"
        assert retrieved_doc.version == 1

        # Read with read-only repo
        read_doc = readonly_doc_repo.read(created_doc.resource_id)
        assert read_doc.resource_id == created_doc.resource_id
        assert read_doc.content == "Test content"

    def test_list_versions(self, writable_doc_repo, readonly_doc_repo):
        """Test listing versions of a document."""
        # Create and update document multiple times with writable repo
        doc = writable_doc_repo.create({"title": "Versioned Doc", "content": "Version 1"})
        doc_id = doc.resource_id

        writable_doc_repo.update(doc_id, {"content": "Version 2"})
        writable_doc_repo.update(doc_id, {"content": "Version 3", "title": "Updated Title"})

        # List versions with read-only repo
        versions = readonly_doc_repo.list_versions(doc_id)
        assert len(versions) == 3

        # Check version ordering (newest first)
        assert versions[0].version_number == 3
        assert versions[0].is_latest is True
        assert versions[1].version_number == 2
        assert versions[1].is_latest is False
        assert versions[2].version_number == 1
        assert versions[2].is_latest is False

        # Verify VersionInfo structure
        for version in versions:
            assert isinstance(version, VersionInfo)
            assert version.version_id.startswith("v")
            assert version.created_at is not None
            assert version.updated_at is not None

    def test_get_specific_version(self, writable_doc_repo, readonly_doc_repo):
        """Test retrieving specific versions."""
        # Create document with multiple versions
        doc = writable_doc_repo.create({"title": "Multi-version Doc", "content": "Content v1"})
        doc_id = doc.resource_id

        writable_doc_repo.update(doc_id, {"content": "Content v2"})
        writable_doc_repo.update(doc_id, {"content": "Content v3", "title": "New Title"})

        # Get specific versions with read-only repo
        v1 = readonly_doc_repo.get_version(doc_id, 1)
        assert v1.version == 1
        assert v1.content == "Content v1"
        assert v1.title == "Multi-version Doc"

        v2 = readonly_doc_repo.get_version(doc_id, 2)
        assert v2.version == 2
        assert v2.content == "Content v2"
        assert v2.title == "Multi-version Doc"

        v3 = readonly_doc_repo.get_version(doc_id, 3)
        assert v3.version == 3
        assert v3.content == "Content v3"
        assert v3.title == "New Title"

    def test_get_version_not_found(self, writable_doc_repo, readonly_doc_repo):
        """Test getting a non-existent version."""
        doc = writable_doc_repo.create({"title": "Test", "content": "Test"})

        # Try to get non-existent version
        result = readonly_doc_repo.get_version(doc.resource_id, 99)
        assert result is None

    def test_get_version_invalid_format(self, writable_doc_repo, readonly_doc_repo):
        """Test getting version with invalid format."""
        doc = writable_doc_repo.create({"title": "Test", "content": "Test"})

        # Invalid version number (zero or negative)
        with pytest.raises(ValueError, match="Version must be a positive integer"):
            readonly_doc_repo.get_version(doc.resource_id, 0)

        with pytest.raises(ValueError, match="Version must be a positive integer"):
            readonly_doc_repo.get_version(doc.resource_id, -1)

    def test_list_documents(self, writable_doc_repo, readonly_doc_repo):
        """Test listing documents with read-only repository."""
        # Create multiple documents
        docs_data = [
            {"title": "Doc1", "content": "Content 1"},
            {"title": "Doc2", "content": "Content 2"},
            {"title": "Doc3", "content": "Content 3"},
        ]
        for dd in docs_data:
            writable_doc_repo.create(dd)

        # List all documents with read-only repo
        all_docs = readonly_doc_repo.list()
        assert len(all_docs) == 3

        # List with limit
        limited_docs = readonly_doc_repo.list(limit=2)
        assert len(limited_docs) == 2

    def test_list_versions_empty(self, readonly_doc_repo):
        """Test listing versions for non-existent item."""
        versions = readonly_doc_repo.list_versions("non-existent-id")
        assert versions == []

    def test_no_restore_version_method(self, readonly_doc_repo):
        """Test that restore_version method doesn't exist on read-only repository."""
        assert not hasattr(readonly_doc_repo, "restore_version")

    def test_no_mutation_methods(self, readonly_doc_repo):
        """Test that mutation methods don't exist on read-only versioned repository."""
        assert not hasattr(readonly_doc_repo, "create")
        assert not hasattr(readonly_doc_repo, "update")
        assert not hasattr(readonly_doc_repo, "delete")
        assert not hasattr(readonly_doc_repo, "get_or_create")

    def test_double_digit_versions(self, writable_doc_repo, readonly_doc_repo):
        """Test handling of double-digit version numbers."""
        # Create document and make many updates to get double-digit versions
        doc = writable_doc_repo.create({"title": "Test", "content": "v1"})
        doc_id = doc.resource_id

        # Create versions up to v12
        for i in range(2, 13):
            writable_doc_repo.update(doc_id, {"content": f"v{i}"})

        # List versions with read-only repo and verify ordering
        versions = readonly_doc_repo.list_versions(doc_id)
        assert len(versions) == 12

        # Verify correct numeric ordering (not lexicographic)
        version_numbers = [v.version_number for v in versions]
        assert version_numbers == list(range(12, 0, -1))  # [12, 11, 10, ..., 2, 1]

        # Verify we can get double-digit versions
        v10 = readonly_doc_repo.get_version(doc_id, 10)
        assert v10.version == 10
        assert v10.content == "v10"

        v11 = readonly_doc_repo.get_version(doc_id, 11)
        assert v11.version == 11
        assert v11.content == "v11"

    def test_data_consistency_across_updates(self, writable_doc_repo, readonly_doc_repo):
        """Test that read-only repo reflects all changes made by writable repo."""
        # Create initial document
        doc = writable_doc_repo.create({"title": "Consistency Test", "content": "Initial"})
        doc_id = doc.resource_id

        # Verify read-only repo sees v1
        read_doc = readonly_doc_repo.get(doc_id)
        assert read_doc.version == 1
        assert read_doc.content == "Initial"

        # Update to v2
        writable_doc_repo.update(doc_id, {"content": "Updated"})

        # Verify read-only repo sees v2
        read_doc_v2 = readonly_doc_repo.get(doc_id)
        assert read_doc_v2.version == 2
        assert read_doc_v2.content == "Updated"

        # Verify read-only repo can still access v1
        v1 = readonly_doc_repo.get_version(doc_id, 1)
        assert v1.content == "Initial"

        # Verify version list is correct
        versions = readonly_doc_repo.list_versions(doc_id)
        assert len(versions) == 2
        assert versions[0].version_number == 2
        assert versions[0].is_latest is True

    def test_logger_configuration(self, memory):
        """Test custom logger configuration for versioned repository."""
        import logging

        custom_logger = logging.getLogger("custom_versioned_logger")

        repo = ReadOnlyVersionedResourceRepository(
            ddb=memory,
            model_class=Document,
            logger=custom_logger,
        )

        assert repo.logger == custom_logger
