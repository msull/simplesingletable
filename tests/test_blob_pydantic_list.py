"""
Tests for blob fields containing lists of Pydantic BaseModel objects.

This test module verifies that blob storage correctly handles fields
defined as list[BaseModel], testing both compressed and uncompressed scenarios
for both versioned and non-versioned resources.
"""

from typing import Optional, ClassVar
from datetime import datetime

from pydantic import BaseModel

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig, BlobFieldConfig


# Pydantic models to use in lists
class Address(BaseModel):
    """Example Pydantic model for nested data."""

    street: str
    city: str
    state: str
    zip_code: str
    country: str = "USA"


class PhoneNumber(BaseModel):
    """Example Pydantic model for contact information."""

    type: str  # "mobile", "work", "home"
    number: str
    extension: Optional[str] = None
    is_primary: bool = False


class Transaction(BaseModel):
    """Example Pydantic model for transaction records."""

    transaction_id: str
    amount: float
    currency: str
    timestamp: datetime
    description: Optional[str] = None
    metadata: dict = {}


# Resources with list[BaseModel] blob fields
class PersonWithAddresses(DynamoDbResource):
    """Non-versioned resource with compressed list[BaseModel] blob field."""

    name: str
    email: str
    addresses: Optional[list[Address]] = None  # Blob field - compressed

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={
            "addresses": BlobFieldConfig(
                compress=True,  # Test WITH compression
                content_type="application/json",
            ),
        },
    )


class ContactDirectory(DynamoDbResource):
    """Non-versioned resource with uncompressed list[BaseModel] blob field."""

    contact_name: str
    company: str
    phone_numbers: Optional[list[PhoneNumber]] = None  # Blob field - uncompressed

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={
            "phone_numbers": BlobFieldConfig(
                compress=False,  # Test WITHOUT compression
                content_type="application/json",
            ),
        },
    )


class AccountHistory(DynamoDbVersionedResource):
    """Versioned resource with compressed list[BaseModel] blob field."""

    account_id: str
    account_name: str
    status: str
    transactions: Optional[list[Transaction]] = None  # Blob field - compressed

    resource_config = ResourceConfig(
        compress_data=True,
        max_versions=5,
        blob_fields={
            "transactions": BlobFieldConfig(
                compress=True,  # Test WITH compression
                content_type="application/json",
            ),
        },
    )


class CustomerProfile(DynamoDbVersionedResource):
    """Versioned resource with uncompressed list[BaseModel] blob field."""

    customer_id: str
    name: str
    tier: str
    contact_history: Optional[list[PhoneNumber]] = None  # Blob field - uncompressed

    resource_config = ResourceConfig(
        compress_data=False,
        max_versions=3,
        blob_fields={
            "contact_history": BlobFieldConfig(
                compress=False,  # Test WITHOUT compression
                content_type="application/json",
            ),
        },
    )


class MultiFieldBlobResource(DynamoDbResource):
    """Resource with multiple list[BaseModel] blob fields with different compression settings."""

    resource_name: str
    # Compressed blob
    addresses: Optional[list[Address]] = None
    # Uncompressed blob
    phone_numbers: Optional[list[PhoneNumber]] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={
            "addresses": BlobFieldConfig(compress=True, content_type="application/json"),
            "phone_numbers": BlobFieldConfig(compress=False, content_type="application/json"),
        },
    )


class TestBlobPydanticListNonVersioned:
    """Test non-versioned resources with list[BaseModel] blob fields."""

    def test_create_and_retrieve_compressed_list(self, dynamodb_memory_with_s3):
        """Test creating and retrieving a resource with compressed list[BaseModel] blob field."""
        memory = dynamodb_memory_with_s3

        # Create list of Address objects
        addresses = [
            Address(street="123 Main St", city="Springfield", state="IL", zip_code="62701"),
            Address(street="456 Oak Ave", city="Chicago", state="IL", zip_code="60601"),
            Address(street="789 Pine Rd", city="Naperville", state="IL", zip_code="60540"),
        ]

        # Create resource
        person = memory.create_new(
            PersonWithAddresses,
            {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "addresses": addresses,
            },
        )

        assert person.resource_id
        assert person.name == "John Doe"
        # After create, blob data is still in memory
        assert person.addresses == addresses

        # Load without blobs
        loaded = memory.get_existing(person.resource_id, PersonWithAddresses, load_blobs=False)

        assert loaded.addresses is None
        assert loaded.has_unloaded_blobs()
        assert "addresses" in loaded.get_unloaded_blob_fields()

        # Load with blobs
        loaded_with_blobs = memory.get_existing(person.resource_id, PersonWithAddresses, load_blobs=True)

        assert loaded_with_blobs.addresses is not None
        assert len(loaded_with_blobs.addresses) == 3
        assert loaded_with_blobs.addresses[0].street == "123 Main St"
        assert loaded_with_blobs.addresses[1].city == "Chicago"
        assert loaded_with_blobs.addresses[2].zip_code == "60540"
        assert not loaded_with_blobs.has_unloaded_blobs()

    def test_create_and_retrieve_uncompressed_list(self, dynamodb_memory_with_s3):
        """Test creating and retrieving a resource with uncompressed list[BaseModel] blob field."""
        memory = dynamodb_memory_with_s3

        # Create list of PhoneNumber objects
        phone_numbers = [
            PhoneNumber(type="mobile", number="+1-555-0100", is_primary=True),
            PhoneNumber(type="work", number="+1-555-0101", extension="123"),
            PhoneNumber(type="home", number="+1-555-0102"),
        ]

        # Create resource
        contact = memory.create_new(
            ContactDirectory,
            {
                "contact_name": "Jane Smith",
                "company": "Acme Corp",
                "phone_numbers": phone_numbers,
            },
        )

        assert contact.resource_id
        # After create, blob data is still in memory
        assert contact.phone_numbers == phone_numbers

        # Load without blobs
        loaded = memory.get_existing(contact.resource_id, ContactDirectory, load_blobs=False)

        assert loaded.phone_numbers is None
        assert loaded.has_unloaded_blobs()

        # Load with blobs
        loaded_with_blobs = memory.get_existing(contact.resource_id, ContactDirectory, load_blobs=True)

        assert loaded_with_blobs.phone_numbers is not None
        assert len(loaded_with_blobs.phone_numbers) == 3
        assert loaded_with_blobs.phone_numbers[0].type == "mobile"
        assert loaded_with_blobs.phone_numbers[0].is_primary is True
        assert loaded_with_blobs.phone_numbers[1].extension == "123"
        assert not loaded_with_blobs.has_unloaded_blobs()

    def test_update_list_blob_field(self, dynamodb_memory_with_s3):
        """Test updating a list[BaseModel] blob field."""
        memory = dynamodb_memory_with_s3

        # Create initial resource
        initial_addresses = [
            Address(street="123 Main St", city="Springfield", state="IL", zip_code="62701"),
        ]

        person = memory.create_new(
            PersonWithAddresses,
            {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "addresses": initial_addresses,
            },
        )

        # Update with new addresses
        updated_addresses = [
            Address(street="123 Main St", city="Springfield", state="IL", zip_code="62701"),
            Address(street="999 New St", city="Boston", state="MA", zip_code="02101"),
        ]

        updated = memory.update_existing(person, {"addresses": updated_addresses})

        # Load and verify
        loaded = memory.get_existing(updated.resource_id, PersonWithAddresses, load_blobs=True)

        assert len(loaded.addresses) == 2
        assert loaded.addresses[1].city == "Boston"
        assert loaded.addresses[1].state == "MA"

    def test_empty_list(self, dynamodb_memory_with_s3):
        """Test handling of empty list[BaseModel]."""
        memory = dynamodb_memory_with_s3

        # Create with empty list
        person = memory.create_new(
            PersonWithAddresses,
            {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "addresses": [],
            },
        )

        # Load and verify
        loaded = memory.get_existing(person.resource_id, PersonWithAddresses, load_blobs=True)

        assert loaded.addresses == []
        assert isinstance(loaded.addresses, list)
        assert len(loaded.addresses) == 0

    def test_multiple_blob_fields_different_compression(self, dynamodb_memory_with_s3):
        """Test resource with multiple list[BaseModel] blob fields with different compression settings."""
        memory = dynamodb_memory_with_s3

        addresses = [
            Address(street="123 Main St", city="Springfield", state="IL", zip_code="62701"),
        ]

        phone_numbers = [
            PhoneNumber(type="mobile", number="+1-555-0100", is_primary=True),
        ]

        # Create resource with both blob fields
        resource = memory.create_new(
            MultiFieldBlobResource,
            {
                "resource_name": "Test Multi-Field",
                "addresses": addresses,
                "phone_numbers": phone_numbers,
            },
        )

        # Load without blobs
        loaded = memory.get_existing(resource.resource_id, MultiFieldBlobResource, load_blobs=False)

        assert loaded.addresses is None
        assert loaded.phone_numbers is None
        assert len(loaded.get_unloaded_blob_fields()) == 2

        # Load only addresses (compressed)
        loaded.load_blob_fields(memory, fields=["addresses"])

        assert loaded.addresses is not None
        assert len(loaded.addresses) == 1
        assert loaded.phone_numbers is None

        # Load phone_numbers (uncompressed)
        loaded.load_blob_fields(memory, fields=["phone_numbers"])

        assert loaded.phone_numbers is not None
        assert len(loaded.phone_numbers) == 1
        assert not loaded.has_unloaded_blobs()


class TestBlobPydanticListVersioned:
    """Test versioned resources with list[BaseModel] blob fields."""

    def test_create_versioned_with_compressed_list(self, dynamodb_memory_with_s3):
        """Test creating versioned resource with compressed list[BaseModel] blob field."""
        memory = dynamodb_memory_with_s3

        # Create list of Transaction objects
        transactions = [
            Transaction(
                transaction_id="TXN001",
                amount=100.50,
                currency="USD",
                timestamp=datetime(2024, 1, 1, 10, 0, 0),
                description="Payment received",
            ),
            Transaction(
                transaction_id="TXN002",
                amount=50.25,
                currency="USD",
                timestamp=datetime(2024, 1, 2, 14, 30, 0),
                description="Refund processed",
                metadata={"reason": "customer request"},
            ),
        ]

        # Create resource
        account = memory.create_new(
            AccountHistory,
            {
                "account_id": "ACC001",
                "account_name": "Primary Account",
                "status": "active",
                "transactions": transactions,
            },
        )

        assert account.version == 1
        # For versioned resources, blob fields are None after create (stored in S3)
        assert account.transactions is None

        # Load without blobs
        loaded = memory.get_existing(account.resource_id, AccountHistory, load_blobs=False)

        assert loaded.transactions is None
        assert loaded.has_unloaded_blobs()

        # Load with blobs
        loaded_with_blobs = memory.get_existing(account.resource_id, AccountHistory, load_blobs=True)

        assert loaded_with_blobs.transactions is not None
        assert len(loaded_with_blobs.transactions) == 2
        assert loaded_with_blobs.transactions[0].transaction_id == "TXN001"
        assert loaded_with_blobs.transactions[1].metadata == {"reason": "customer request"}

    def test_create_versioned_with_uncompressed_list(self, dynamodb_memory_with_s3):
        """Test creating versioned resource with uncompressed list[BaseModel] blob field."""
        memory = dynamodb_memory_with_s3

        # Create list of PhoneNumber objects
        contact_history = [
            PhoneNumber(type="mobile", number="+1-555-0100", is_primary=True),
            PhoneNumber(type="work", number="+1-555-0101", extension="456"),
        ]

        # Create resource
        customer = memory.create_new(
            CustomerProfile,
            {
                "customer_id": "CUST001",
                "name": "Alice Johnson",
                "tier": "gold",
                "contact_history": contact_history,
            },
        )

        assert customer.version == 1

        # Load with blobs
        loaded = memory.get_existing(customer.resource_id, CustomerProfile, load_blobs=True)

        assert loaded.contact_history is not None
        assert len(loaded.contact_history) == 2
        assert loaded.contact_history[0].is_primary is True

    def test_version_updates_with_list_changes(self, dynamodb_memory_with_s3):
        """Test version management when updating list[BaseModel] blob fields."""
        memory = dynamodb_memory_with_s3

        # Create initial version with one transaction
        initial_transactions = [
            Transaction(
                transaction_id="TXN001",
                amount=100.00,
                currency="USD",
                timestamp=datetime(2024, 1, 1, 10, 0, 0),
            ),
        ]

        account = memory.create_new(
            AccountHistory,
            {
                "account_id": "ACC001",
                "account_name": "Primary Account",
                "status": "active",
                "transactions": initial_transactions,
            },
        )

        assert account.version == 1

        # Update with additional transaction
        updated_transactions = initial_transactions + [
            Transaction(
                transaction_id="TXN002",
                amount=200.00,
                currency="USD",
                timestamp=datetime(2024, 1, 2, 11, 0, 0),
            ),
        ]

        updated = memory.update_existing(account, {"transactions": updated_transactions})

        assert updated.version == 2

        # Load v1 and verify it has original data
        v1 = memory.get_existing(account.resource_id, AccountHistory, version=1, load_blobs=True)

        assert len(v1.transactions) == 1
        assert v1.transactions[0].transaction_id == "TXN001"

        # Load v2 and verify it has updated data
        v2 = memory.get_existing(account.resource_id, AccountHistory, version=2, load_blobs=True)

        assert len(v2.transactions) == 2
        assert v2.transactions[0].transaction_id == "TXN001"
        assert v2.transactions[1].transaction_id == "TXN002"

    def test_version_preservation_across_updates(self, dynamodb_memory_with_s3):
        """Test that blob references are preserved correctly across versions."""
        memory = dynamodb_memory_with_s3

        # Create initial version
        initial_transactions = [
            Transaction(
                transaction_id="TXN001",
                amount=100.00,
                currency="USD",
                timestamp=datetime(2024, 1, 1, 10, 0, 0),
            ),
        ]

        account = memory.create_new(
            AccountHistory,
            {
                "account_id": "ACC001",
                "account_name": "Primary Account",
                "status": "active",
                "transactions": initial_transactions,
            },
        )

        # Update without changing transactions (should preserve blob reference)
        loaded = memory.get_existing(account.resource_id, AccountHistory, load_blobs=False)
        updated = memory.update_existing(loaded, {"status": "suspended"})

        assert updated.version == 2
        assert updated.has_unloaded_blobs()

        # Load v2 blobs - should get v1 transactions
        updated.load_blob_fields(memory)

        assert updated.transactions == initial_transactions
        assert len(updated.transactions) == 1

    def test_clear_list_blob_field(self, dynamodb_memory_with_s3):
        """Test clearing a list[BaseModel] blob field in a versioned resource."""
        memory = dynamodb_memory_with_s3

        # Create with transactions
        transactions = [
            Transaction(
                transaction_id="TXN001",
                amount=100.00,
                currency="USD",
                timestamp=datetime(2024, 1, 1, 10, 0, 0),
            ),
        ]

        account = memory.create_new(
            AccountHistory,
            {
                "account_id": "ACC001",
                "account_name": "Primary Account",
                "status": "active",
                "transactions": transactions,
            },
        )

        # Clear transactions field
        updated = memory.update_existing(account, update_obj={}, clear_fields={"transactions"})

        assert updated.version == 2

        # Load v2 with blobs
        v2 = memory.get_existing(account.resource_id, AccountHistory, version=2, load_blobs=True)

        # Transactions should be None (no blob to load)
        assert v2.transactions is None
        assert not v2.has_unloaded_blobs()

    def test_large_list_compression_benefit(self, dynamodb_memory_with_s3):
        """Test that compression works with large lists of Pydantic models."""
        memory = dynamodb_memory_with_s3

        # Create a large list of transactions
        large_transaction_list = [
            Transaction(
                transaction_id=f"TXN{i:05d}",
                amount=100.00 + i,
                currency="USD",
                timestamp=datetime(2024, 1, 1, 10, i % 60, 0),
                description=f"Transaction number {i} with some repeated text to increase size",
                metadata={"index": i, "batch": i // 10, "category": "test"},
            )
            for i in range(100)  # 100 transactions
        ]

        # Create account with large transaction list
        account = memory.create_new(
            AccountHistory,
            {
                "account_id": "ACC001",
                "account_name": "High Volume Account",
                "status": "active",
                "transactions": large_transaction_list,
            },
        )

        # Load and verify all transactions are preserved
        loaded = memory.get_existing(account.resource_id, AccountHistory, load_blobs=True)

        assert len(loaded.transactions) == 100
        assert loaded.transactions[0].transaction_id == "TXN00000"
        assert loaded.transactions[99].transaction_id == "TXN00099"
        assert loaded.transactions[50].metadata["batch"] == 5

    def test_nested_pydantic_models_in_list(self, dynamodb_memory_with_s3):
        """Test that nested Pydantic models within list items are preserved."""
        memory = dynamodb_memory_with_s3

        # Create transactions with nested metadata
        transactions = [
            Transaction(
                transaction_id="TXN001",
                amount=100.00,
                currency="USD",
                timestamp=datetime(2024, 1, 1, 10, 0, 0),
                metadata={
                    "nested": {
                        "level1": {"level2": {"level3": "deep value"}},
                        "array": [1, 2, 3, 4, 5],
                    },
                    "complex": True,
                },
            ),
        ]

        account = memory.create_new(
            AccountHistory,
            {
                "account_id": "ACC001",
                "account_name": "Complex Account",
                "status": "active",
                "transactions": transactions,
            },
        )

        # Load and verify nested structures are preserved
        loaded = memory.get_existing(account.resource_id, AccountHistory, load_blobs=True)

        assert loaded.transactions[0].metadata["nested"]["level1"]["level2"]["level3"] == "deep value"
        assert loaded.transactions[0].metadata["nested"]["array"] == [1, 2, 3, 4, 5]
        assert loaded.transactions[0].metadata["complex"] is True


class TestBlobPydanticListEdgeCases:
    """Test edge cases for list[BaseModel] blob fields."""

    def test_none_vs_empty_list(self, dynamodb_memory_with_s3):
        """Test distinguishing between None and empty list."""
        memory = dynamodb_memory_with_s3

        # Create with None
        person1 = memory.create_new(
            PersonWithAddresses,
            {
                "name": "Person 1",
                "email": "person1@example.com",
                "addresses": None,
            },
        )

        # Create with empty list
        person2 = memory.create_new(
            PersonWithAddresses,
            {
                "name": "Person 2",
                "email": "person2@example.com",
                "addresses": [],
            },
        )

        # Load both with blobs
        loaded1 = memory.get_existing(person1.resource_id, PersonWithAddresses, load_blobs=True)
        loaded2 = memory.get_existing(person2.resource_id, PersonWithAddresses, load_blobs=True)

        # person1 should have None (no blob stored)
        assert loaded1.addresses is None
        assert not loaded1.has_unloaded_blobs()

        # person2 should have empty list (blob was stored)
        assert loaded2.addresses == []
        assert isinstance(loaded2.addresses, list)

    def test_update_from_none_to_list(self, dynamodb_memory_with_s3):
        """Test updating a blob field from None to a populated list."""
        memory = dynamodb_memory_with_s3

        # Create with None
        person = memory.create_new(
            PersonWithAddresses,
            {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "addresses": None,
            },
        )

        # Update with list
        addresses = [
            Address(street="123 Main St", city="Springfield", state="IL", zip_code="62701"),
        ]

        updated = memory.update_existing(person, {"addresses": addresses})

        # Load and verify
        loaded = memory.get_existing(updated.resource_id, PersonWithAddresses, load_blobs=True)

        assert loaded.addresses is not None
        assert len(loaded.addresses) == 1
        assert loaded.addresses[0].street == "123 Main St"

    def test_update_from_list_to_none(self, dynamodb_memory_with_s3):
        """Test updating a blob field from a list to None using clear_fields."""
        memory = dynamodb_memory_with_s3

        # Create with list
        addresses = [
            Address(street="123 Main St", city="Springfield", state="IL", zip_code="62701"),
        ]

        person = memory.create_new(
            PersonWithAddresses,
            {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "addresses": addresses,
            },
        )

        # Clear addresses
        updated = memory.update_existing(person, update_obj={}, clear_fields={"addresses"})

        # Load and verify
        loaded = memory.get_existing(updated.resource_id, PersonWithAddresses, load_blobs=True)

        assert loaded.addresses is None
