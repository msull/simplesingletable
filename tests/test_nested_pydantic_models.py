"""
Test for nested Pydantic models as regular (non-blob) fields.

This test checks if there are serialization warnings when a resource
has a nested Pydantic model as a regular field (not stored in blob storage).
"""

from typing import Optional
from pydantic import BaseModel

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig


# Nested Pydantic models
class Address(BaseModel):
    """Nested Pydantic model."""
    street: str
    city: str
    state: str
    zip_code: str
    tags: set[str] = set()  # Include a set to test


class ContactInfo(BaseModel):
    """Another nested model."""
    email: str
    phone: Optional[str] = None
    preferences: set[str] = set()


# Resource with nested Pydantic model (NOT as blob)
class PersonWithAddress(DynamoDbResource):
    """Resource with nested Pydantic model as regular field."""
    name: str
    age: int
    address: Optional[Address] = None  # Nested model, NOT in blob storage

    resource_config = ResourceConfig(
        compress_data=False
    )


class PersonWithAddressCompressed(DynamoDbResource):
    """Resource with nested Pydantic model and compression."""
    name: str
    age: int
    address: Optional[Address] = None  # Nested model with compression

    resource_config = ResourceConfig(
        compress_data=True
    )


class UserProfile(DynamoDbVersionedResource):
    """Versioned resource with nested models."""
    username: str
    full_name: str
    address: Optional[Address] = None
    contact: Optional[ContactInfo] = None

    resource_config = ResourceConfig(
        compress_data=True,
        max_versions=3
    )


class TestNestedPydanticModels:
    """Test nested Pydantic models in regular fields."""

    def test_create_with_nested_model_uncompressed(self, dynamodb_memory):
        """Test creating resource with nested Pydantic model (uncompressed)."""
        memory = dynamodb_memory

        # Create with nested model
        address = Address(
            street="123 Main St",
            city="Springfield",
            state="IL",
            zip_code="62701",
            tags=set()  # Empty set in nested model
        )

        person = memory.create_new(
            PersonWithAddress,
            {
                "name": "John Doe",
                "age": 30,
                "address": address
            }
        )

        assert person.resource_id
        assert person.name == "John Doe"
        assert person.address is not None
        assert person.address.street == "123 Main St"
        assert person.address.tags == set()

        # Load back
        loaded = memory.get_existing(person.resource_id, PersonWithAddress)

        assert loaded.name == "John Doe"
        assert loaded.address is not None
        assert loaded.address.street == "123 Main St"
        assert loaded.address.city == "Springfield"
        assert loaded.address.tags == set()
        assert isinstance(loaded.address.tags, set)

    def test_create_with_nested_model_compressed(self, dynamodb_memory):
        """Test creating resource with nested Pydantic model (compressed)."""
        memory = dynamodb_memory

        # Create with nested model containing non-empty set
        address = Address(
            street="456 Oak Ave",
            city="Chicago",
            state="IL",
            zip_code="60601",
            tags={"home", "primary"}
        )

        person = memory.create_new(
            PersonWithAddressCompressed,
            {
                "name": "Jane Smith",
                "age": 25,
                "address": address
            }
        )

        # Load back
        loaded = memory.get_existing(person.resource_id, PersonWithAddressCompressed)

        assert loaded.address is not None
        assert loaded.address.street == "456 Oak Ave"
        assert loaded.address.tags == {"home", "primary"}
        assert isinstance(loaded.address.tags, set)

    def test_versioned_with_nested_models(self, dynamodb_memory):
        """Test versioned resource with multiple nested models."""
        memory = dynamodb_memory

        address = Address(
            street="789 Pine Rd",
            city="Boston",
            state="MA",
            zip_code="02101",
            tags=set()
        )

        contact = ContactInfo(
            email="user@example.com",
            phone="+1-555-0100",
            preferences={"email", "sms"}
        )

        user = memory.create_new(
            UserProfile,
            {
                "username": "johndoe",
                "full_name": "John Doe",
                "address": address,
                "contact": contact
            }
        )

        assert user.version == 1

        # Load back
        loaded = memory.get_existing(user.resource_id, UserProfile)

        assert loaded.address is not None
        assert loaded.address.tags == set()
        assert isinstance(loaded.address.tags, set)

        assert loaded.contact is not None
        assert loaded.contact.preferences == {"email", "sms"}
        assert isinstance(loaded.contact.preferences, set)

    def test_update_nested_model(self, dynamodb_memory):
        """Test updating a resource with nested model."""
        memory = dynamodb_memory

        # Create initial
        person = memory.create_new(
            PersonWithAddress,
            {
                "name": "John Doe",
                "age": 30,
                "address": Address(
                    street="123 Main St",
                    city="Springfield",
                    state="IL",
                    zip_code="62701",
                    tags=set()
                )
            }
        )

        # Update with new address
        updated = memory.update_existing(
            person,
            {
                "address": Address(
                    street="999 New St",
                    city="Boston",
                    state="MA",
                    zip_code="02101",
                    tags={"work", "new"}
                )
            }
        )

        # Load and verify
        loaded = memory.get_existing(updated.resource_id, PersonWithAddress)

        assert loaded.address.street == "999 New St"
        assert loaded.address.city == "Boston"
        assert loaded.address.tags == {"work", "new"}
        assert isinstance(loaded.address.tags, set)

    def test_nested_model_with_none(self, dynamodb_memory):
        """Test resource where nested model is None."""
        memory = dynamodb_memory

        person = memory.create_new(
            PersonWithAddress,
            {
                "name": "John Doe",
                "age": 30,
                "address": None
            }
        )

        loaded = memory.get_existing(person.resource_id, PersonWithAddress)

        assert loaded.address is None
