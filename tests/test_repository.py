import pytest
from typing import Optional
from pydantic import BaseModel, Field

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.extras.repository import ResourceRepository


class User(DynamoDbResource):
    name: str
    email: str
    age: Optional[int] = None


class CreateUserSchema(BaseModel):
    name: str = ""
    email: str = "test@example.com"
    age: Optional[int] = Field(None, ge=0, le=150)  # Age must be between 0 and 150


class UpdateUserSchema(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    age: Optional[int] = Field(None, ge=0, le=150)  # Age must be between 0 and 150


class VersionedDocument(DynamoDbVersionedResource):
    title: str
    content: str


class CreateDocumentSchema(BaseModel):
    title: str
    content: str


class UpdateDocumentSchema(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None


class ExpiringResource(DynamoDbResource):
    """Test resource with optional expiration field."""

    name: str
    description: Optional[str] = None
    expires_at: Optional[str] = None


class CreateExpiringResourceSchema(BaseModel):
    name: str
    description: Optional[str] = None
    expires_at: Optional[str] = None


class UpdateExpiringResourceSchema(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    expires_at: Optional[str] = None


@pytest.fixture
def user_repository(dynamodb_memory: DynamoDbMemory):
    return ResourceRepository(
        ddb=dynamodb_memory,
        model_class=User,
        create_schema_class=CreateUserSchema,
        update_schema_class=UpdateUserSchema,
    )


@pytest.fixture
def document_repository(dynamodb_memory: DynamoDbMemory):
    return ResourceRepository(
        ddb=dynamodb_memory,
        model_class=VersionedDocument,
        create_schema_class=CreateDocumentSchema,
        update_schema_class=UpdateDocumentSchema,
    )


@pytest.fixture
def expiring_repository(dynamodb_memory: DynamoDbMemory):
    return ResourceRepository(
        ddb=dynamodb_memory,
        model_class=ExpiringResource,
        create_schema_class=CreateExpiringResourceSchema,
        update_schema_class=UpdateExpiringResourceSchema,
    )


class TestResourceRepository:
    """Test cases for the ResourceRepository class."""

    def test_create_with_dict(self, user_repository):
        """Test creating a resource with a dictionary."""
        user_data = {"name": "John Doe", "email": "john@example.com", "age": 30}
        user = user_repository.create(user_data)

        assert user.name == "John Doe"
        assert user.email == "john@example.com"
        assert user.age == 30
        assert user.resource_id is not None
        assert user.created_at is not None
        assert user.updated_at is not None

    def test_create_with_schema(self, user_repository):
        """Test creating a resource with a Pydantic schema."""
        user_schema = CreateUserSchema(name="Jane Doe", email="jane@example.com")
        user = user_repository.create(user_schema)

        assert user.name == "Jane Doe"
        assert user.email == "jane@example.com"
        assert user.age is None

    def test_get_existing_user(self, user_repository):
        """Test retrieving an existing user."""
        user_data = {"name": "Alice", "email": "alice@example.com"}
        created_user = user_repository.create(user_data)

        retrieved_user = user_repository.get(created_user.resource_id)
        assert retrieved_user is not None
        assert retrieved_user.resource_id == created_user.resource_id
        assert retrieved_user.name == "Alice"

    def test_get_nonexistent_user(self, user_repository):
        """Test retrieving a non-existent user returns None."""
        result = user_repository.get("nonexistent-id")
        assert result is None

    def test_read_existing_user(self, user_repository):
        """Test reading an existing user."""
        user_data = {"name": "Bob", "email": "bob@example.com"}
        created_user = user_repository.create(user_data)

        read_user = user_repository.read(created_user.resource_id)
        assert read_user.resource_id == created_user.resource_id
        assert read_user.name == "Bob"

    def test_read_nonexistent_user_raises_error(self, user_repository):
        """Test reading a non-existent user raises ValueError."""
        with pytest.raises(ValueError, match="User with id nonexistent-id not found"):
            user_repository.read("nonexistent-id")

    def test_update_with_id(self, user_repository):
        """Test updating a user by ID."""
        user_data = {"name": "Charlie", "email": "charlie@example.com", "age": 25}
        created_user = user_repository.create(user_data)

        update_data = {"name": "Charles", "age": 26}
        updated_user = user_repository.update(created_user.resource_id, update_data)

        assert updated_user.name == "Charles"
        assert updated_user.email == "charlie@example.com"  # unchanged
        assert updated_user.age == 26
        assert updated_user.resource_id == created_user.resource_id
        assert updated_user.updated_at > created_user.updated_at

    def test_update_with_object(self, user_repository):
        """Test updating a user by passing the object."""
        user_data = {"name": "Diana", "email": "diana@example.com"}
        created_user = user_repository.create(user_data)

        update_data = UpdateUserSchema(name="Di", age=28)
        updated_user = user_repository.update(created_user, update_data)

        assert updated_user.name == "Di"
        assert updated_user.age == 28
        assert updated_user.email == "diana@example.com"

    def test_delete(self, user_repository):
        """Test deleting a user."""
        user_data = {"name": "Eve", "email": "eve@example.com"}
        created_user = user_repository.create(user_data)

        # Verify user exists
        assert user_repository.get(created_user.resource_id) is not None

        # Delete user
        user_repository.delete(created_user.resource_id)

        # Verify user no longer exists
        assert user_repository.get(created_user.resource_id) is None

    def test_delete_nonexistent_user_raises_error(self, user_repository):
        """Test deleting a non-existent user raises ValueError."""
        with pytest.raises(ValueError, match="User with id nonexistent-id not found"):
            user_repository.delete("nonexistent-id")

    def test_list_users(self, user_repository):
        """Test listing users."""
        # Create multiple users
        users_data = [
            {"name": "User1", "email": "user1@example.com"},
            {"name": "User2", "email": "user2@example.com"},
            {"name": "User3", "email": "user3@example.com"},
        ]
        for ud in users_data:
            user_repository.create(ud)

        # List all users
        all_users = user_repository.list()
        assert len(all_users) == 3

        # List with limit
        limited_users = user_repository.list(limit=2)
        assert len(limited_users) == 2

    def test_get_or_create_existing(self, user_repository):
        """Test get_or_create when user already exists."""
        user_data = {"name": "Frank", "email": "frank@example.com"}
        created_user = user_repository.create(user_data)

        # Should return existing user
        result = user_repository.get_or_create(created_user.resource_id)
        assert result.resource_id == created_user.resource_id
        assert result.name == "Frank"

    def test_get_or_create_new_with_default_schema(self, user_repository):
        """Test get_or_create when user doesn't exist, using default schema."""
        # Should create new user with default schema
        result = user_repository.get_or_create("new-user-id")
        assert result.resource_id == "new-user-id"
        # Default schema should have default values
        assert result.name == ""  # Default from Pydantic
        assert result.email == "test@example.com"  # Default email
        assert result.age is None

    def test_get_or_create_with_default_factory(self, dynamodb_memory):
        """Test get_or_create with a default object factory function."""

        def create_default_user(user_id: str) -> CreateUserSchema:
            return CreateUserSchema(name=f"Default User {user_id}", email=f"{user_id}@default.com")

        repo = ResourceRepository(
            ddb=dynamodb_memory,
            model_class=User,
            create_schema_class=CreateUserSchema,
            update_schema_class=UpdateUserSchema,
            default_create_obj_fn=create_default_user,
        )

        result = repo.get_or_create("default-test-id")
        assert result.resource_id == "default-test-id"
        assert result.name == "Default User default-test-id"
        assert result.email == "default-test-id@default.com"

    def test_with_custom_id_override(self, dynamodb_memory):
        """Test repository with custom ID override function."""

        def custom_id_fn(schema: CreateUserSchema) -> str:
            return f"user-{schema.email.split('@')[0]}"

        repo = ResourceRepository(
            ddb=dynamodb_memory,
            model_class=User,
            create_schema_class=CreateUserSchema,
            update_schema_class=UpdateUserSchema,
            override_id_fn=custom_id_fn,
        )

        user_data = {"name": "Custom User", "email": "custom@example.com"}
        user = repo.create(user_data)

        assert user.resource_id == "user-custom"
        assert user.name == "Custom User"


class TestVersionedResourceRepository:
    """Test cases for versioned resources."""

    def test_create_versioned_document(self, document_repository):
        """Test creating a versioned document."""
        doc_data = {"title": "Test Document", "content": "Initial content"}
        doc = document_repository.create(doc_data)

        assert doc.title == "Test Document"
        assert doc.content == "Initial content"
        assert doc.version == 1

    def test_update_versioned_document(self, document_repository):
        """Test updating a versioned document creates a new version."""
        doc_data = {"title": "Versioned Doc", "content": "Version 1 content"}
        created_doc = document_repository.create(doc_data)

        update_data = {"content": "Version 2 content"}
        updated_doc = document_repository.update(created_doc.resource_id, update_data)

        assert updated_doc.version == 2
        assert updated_doc.title == "Versioned Doc"  # unchanged
        assert updated_doc.content == "Version 2 content"
        assert updated_doc.resource_id == created_doc.resource_id

    def test_delete_versioned_resource_raises_error(self, document_repository):
        """Test that deleting versioned resources raises TypeError."""
        doc_data = {"title": "To Delete", "content": "Will not be deleted"}
        created_doc = document_repository.create(doc_data)

        document_repository.delete(created_doc.resource_id)

        assert document_repository.get(created_doc.resource_id) is None


class TestRepositoryErrorHandling:
    """Test error handling in the repository."""

    def test_invalid_update_schema_raises_error(self, user_repository):
        """Test that invalid update data raises a validation error."""
        user_data = {"name": "Test User", "email": "test@example.com"}
        created_user = user_repository.create(user_data)

        # Try to update with invalid data - age constraint violation
        invalid_update = {"age": 200}  # Age over 150 limit
        with pytest.raises(Exception):  # Pydantic will raise validation error
            user_repository.update(created_user.resource_id, invalid_update)

    def test_invalid_create_schema_raises_error(self, user_repository):
        """Test that invalid create data raises a validation error."""
        # Invalid age constraint
        invalid_data = {"name": "Test User", "email": "test@example.com", "age": -5}  # Negative age
        with pytest.raises(Exception):  # Pydantic will raise validation error
            user_repository.create(invalid_data)


class TestClearFieldsFeature:
    """Test the clear_fields functionality."""

    def test_clear_single_field(self, expiring_repository):
        """Test clearing a single optional field to None."""
        # Create resource with all fields
        resource_data = {"name": "Expiring Resource", "description": "This will expire", "expires_at": "2024-12-31"}
        created = expiring_repository.create(resource_data)

        assert created.expires_at == "2024-12-31"

        # Update name and clear expires_at
        update_data = {"name": "Updated Resource", "expires_at": None}
        updated = expiring_repository.update(created.resource_id, update_data, clear_fields={"expires_at"})

        assert updated.name == "Updated Resource"
        assert updated.description == "This will expire"  # unchanged
        assert updated.expires_at is None  # explicitly cleared

    def test_clear_multiple_fields(self, expiring_repository):
        """Test clearing multiple optional fields to None."""
        # Create resource with all fields
        resource_data = {"name": "Multi Field Resource", "description": "Has description", "expires_at": "2024-12-31"}
        created = expiring_repository.create(resource_data)

        # Clear both optional fields
        update_data = {"description": None, "expires_at": None}
        updated = expiring_repository.update(
            created.resource_id, update_data, clear_fields={"description", "expires_at"}
        )

        assert updated.name == "Multi Field Resource"  # unchanged
        assert updated.description is None  # explicitly cleared
        assert updated.expires_at is None  # explicitly cleared

    def test_clear_fields_with_other_updates(self, user_repository):
        """Test clear_fields works alongside regular updates."""
        # Create user with age
        user_data = {"name": "John Doe", "email": "john@example.com", "age": 30}
        created = user_repository.create(user_data)

        # Update name and email, clear age
        update_data = {"name": "John Updated", "email": "john.updated@example.com", "age": None}
        updated = user_repository.update(created.resource_id, update_data, clear_fields={"age"})

        assert updated.name == "John Updated"
        assert updated.email == "john.updated@example.com"
        assert updated.age is None  # explicitly cleared

    def test_clear_fields_empty_set(self, user_repository):
        """Test that empty clear_fields set behaves normally."""
        user_data = {"name": "Test User", "email": "test@example.com", "age": 25}
        created = user_repository.create(user_data)

        # Update with None value but empty clear_fields
        update_data = {"name": "Updated Name", "age": None}
        updated = user_repository.update(
            created.resource_id,
            update_data,
            clear_fields=set(),  # empty set
        )

        assert updated.name == "Updated Name"
        assert updated.age == 25  # unchanged because age=None was excluded

    def test_clear_fields_none_parameter(self, user_repository):
        """Test that None clear_fields parameter behaves normally."""
        user_data = {"name": "Test User", "email": "test@example.com", "age": 25}
        created = user_repository.create(user_data)

        # Update with None value and no clear_fields
        update_data = {"name": "Updated Name", "age": None}
        updated = user_repository.update(
            created.resource_id,
            update_data,
            clear_fields=None,  # explicitly None
        )

        assert updated.name == "Updated Name"
        assert updated.age == 25  # unchanged because age=None was excluded

    def test_clear_field_not_in_update_data(self, expiring_repository):
        """Test clearing a field that's not in update_data."""
        resource_data = {"name": "Resource", "description": "Has description", "expires_at": "2024-12-31"}
        created = expiring_repository.create(resource_data)

        # Update name only but clear expires_at
        update_data = {"name": "Updated Resource"}
        updated = expiring_repository.update(created.resource_id, update_data, clear_fields={"expires_at"})

        assert updated.name == "Updated Resource"
        assert updated.description == "Has description"  # unchanged
        assert updated.expires_at is None  # cleared even though not in update_data
