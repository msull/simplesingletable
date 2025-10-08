"""Tests for TTL (Time To Live) functionality."""

from datetime import datetime, timedelta, timezone
from typing import ClassVar, Optional

import pytest
from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig


class ResourceWithDatetimeTTL(DynamoDbResource):
    """Test resource with datetime TTL."""

    name: str
    expires_at: Optional[datetime] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False, ttl_field="expires_at", ttl_attribute_name="ttl"
    )


class ResourceWithIntTTLCustomAttr(DynamoDbResource):
    """Test resource with integer TTL and custom attribute name."""

    title: str
    lifetime_seconds: Optional[int] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False, ttl_field="lifetime_seconds", ttl_attribute_name="expiration"
    )


class ResourceWithIntTTL(DynamoDbResource):
    """Test resource with integer seconds TTL."""

    content: str
    ttl_seconds: Optional[int] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True, ttl_field="ttl_seconds", ttl_attribute_name="ttl"
    )


class VersionedResourceWithTTL(DynamoDbVersionedResource):
    """Test versioned resource with TTL."""

    data: str
    expiry_date: Optional[datetime] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True, max_versions=3, ttl_field="expiry_date", ttl_attribute_name="ttl"
    )


class ResourceWithoutTTL(DynamoDbResource):
    """Test resource without TTL configuration."""

    name: str
    value: int

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)


class ResourceWithOnlyTTLField(DynamoDbResource):
    """Test resource with only ttl_field set (missing ttl_attribute_name)."""

    name: str
    expires_at: Optional[datetime] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        ttl_field="expires_at"
        # Missing ttl_attribute_name
    )


class ResourceWithOnlyTTLAttribute(DynamoDbResource):
    """Test resource with only ttl_attribute_name set (missing ttl_field)."""

    name: str
    expires_at: Optional[datetime] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        ttl_attribute_name="ttl"
        # Missing ttl_field
    )


def test_datetime_ttl(dynamodb_memory: DynamoDbMemory):
    """Test TTL with datetime field."""
    # Create resource with future expiration
    future_expiry = datetime.now(timezone.utc) + timedelta(hours=24)
    resource = dynamodb_memory.create_new(
        ResourceWithDatetimeTTL, {"name": "Test Resource", "expires_at": future_expiry}
    )

    # Verify TTL is set in DynamoDB item
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithDatetimeTTL.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]

    assert "ttl" in item
    assert item["ttl"] == int(future_expiry.timestamp())

    # Verify resource can be retrieved
    retrieved = dynamodb_memory.get_existing(resource.resource_id, ResourceWithDatetimeTTL)
    assert retrieved.name == "Test Resource"
    # Compare timestamps without microseconds due to serialization
    assert retrieved.expires_at.replace(microsecond=0) == future_expiry.replace(microsecond=0)


def test_datetime_ttl_none_value(dynamodb_memory: DynamoDbMemory):
    """Test TTL with None datetime - should not add TTL attribute."""
    resource = dynamodb_memory.create_new(ResourceWithDatetimeTTL, {"name": "No Expiry", "expires_at": None})

    # Verify TTL is NOT set in DynamoDB item
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithDatetimeTTL.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]

    assert "ttl" not in item


def test_int_ttl_custom_attribute(dynamodb_memory: DynamoDbMemory):
    """Test TTL with integer field and custom attribute name."""
    lifetime_seconds = 604800  # 7 days in seconds
    resource = dynamodb_memory.create_new(
        ResourceWithIntTTLCustomAttr, {"title": "Weekly Item", "lifetime_seconds": lifetime_seconds}
    )

    # Verify TTL is set with custom attribute name
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithIntTTLCustomAttr.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]

    assert "expiration" in item
    # TTL should be approximately created_at + 7 days
    expected_ttl = int((resource.created_at + timedelta(seconds=lifetime_seconds)).timestamp())
    assert abs(item["expiration"] - expected_ttl) < 2  # Allow 2 second tolerance


def test_int_seconds_ttl(dynamodb_memory: DynamoDbMemory):
    """Test TTL with integer seconds field."""
    ttl_seconds = 3600  # 1 hour
    resource = dynamodb_memory.create_new(ResourceWithIntTTL, {"content": "Temporary", "ttl_seconds": ttl_seconds})

    # Verify TTL is set (default attribute name "ttl")
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithIntTTL.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]

    assert "ttl" in item
    expected_ttl = int((resource.created_at + timedelta(seconds=ttl_seconds)).timestamp())
    assert abs(item["ttl"] - expected_ttl) < 2


def test_versioned_resource_ttl(dynamodb_memory: DynamoDbMemory):
    """Test TTL on versioned resources - applies to both v0 and version items."""
    expiry = datetime.now(timezone.utc) + timedelta(days=30)

    # Create versioned resource
    resource = dynamodb_memory.create_new(VersionedResourceWithTTL, {"data": "Version 1", "expiry_date": expiry})

    # Check v0 item has TTL
    v0_item = dynamodb_memory.dynamodb_table.get_item(
        Key=VersionedResourceWithTTL.dynamodb_lookup_keys_from_id(resource.resource_id, version=0)
    )["Item"]
    assert "ttl" in v0_item
    assert v0_item["ttl"] == int(expiry.timestamp())

    # Check v1 item has TTL
    v1_item = dynamodb_memory.dynamodb_table.get_item(
        Key=VersionedResourceWithTTL.dynamodb_lookup_keys_from_id(resource.resource_id, version=1)
    )["Item"]
    assert "ttl" in v1_item
    assert v1_item["ttl"] == int(expiry.timestamp())

    # Update resource - new version should also have TTL
    updated = dynamodb_memory.update_existing(resource, {"data": "Version 2"})

    v2_item = dynamodb_memory.dynamodb_table.get_item(
        Key=VersionedResourceWithTTL.dynamodb_lookup_keys_from_id(updated.resource_id, version=2)
    )["Item"]
    assert "ttl" in v2_item
    assert v2_item["ttl"] == int(expiry.timestamp())


def test_update_ttl_value(dynamodb_memory: DynamoDbMemory):
    """Test updating the TTL value on a resource."""
    initial_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    resource = dynamodb_memory.create_new(ResourceWithDatetimeTTL, {"name": "Updatable", "expires_at": initial_expiry})

    # Update with new expiry
    new_expiry = datetime.now(timezone.utc) + timedelta(hours=48)
    updated = dynamodb_memory.update_existing(resource, {"expires_at": new_expiry})

    # Verify new TTL value
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithDatetimeTTL.dynamodb_lookup_keys_from_id(updated.resource_id)
    )["Item"]

    assert item["ttl"] == int(new_expiry.timestamp())


def test_clear_ttl_value(dynamodb_memory: DynamoDbMemory):
    """Test clearing TTL by setting to None."""
    # Create with TTL
    resource = dynamodb_memory.create_new(
        ResourceWithDatetimeTTL, {"name": "Clearable", "expires_at": datetime.now(timezone.utc) + timedelta(days=1)}
    )

    # Verify TTL exists
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithDatetimeTTL.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]
    assert "ttl" in item

    # Clear TTL
    updated = dynamodb_memory.update_existing(resource, {"expires_at": None})

    # Verify TTL is removed
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithDatetimeTTL.dynamodb_lookup_keys_from_id(updated.resource_id)
    )["Item"]
    assert "ttl" not in item


def test_resource_without_ttl_config(dynamodb_memory: DynamoDbMemory):
    """Test that resources without TTL config don't get TTL attributes."""
    resource = dynamodb_memory.create_new(ResourceWithoutTTL, {"name": "No TTL", "value": 42})

    # Verify no TTL attribute
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithoutTTL.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]

    assert "ttl" not in item
    assert "expiration" not in item


def test_invalid_ttl_field_type():
    """Test that invalid TTL field types raise appropriate errors."""

    class InvalidTTLResource(DynamoDbResource):
        name: str
        invalid_ttl: str  # String is not a valid TTL type

        resource_config: ClassVar[ResourceConfig] = ResourceConfig(ttl_field="invalid_ttl", ttl_attribute_name="ttl")

    # This should raise when trying to calculate TTL
    resource = InvalidTTLResource(
        resource_id="test",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        name="Test",
        invalid_ttl="not a valid ttl",
    )

    with pytest.raises(ValueError, match="Unsupported TTL field type"):
        resource._calculate_ttl()


def test_ttl_requires_both_fields(dynamodb_memory: DynamoDbMemory):
    """Test that TTL requires both ttl_field and ttl_attribute_name to be set."""

    # Resource with only ttl_field set
    resource1 = dynamodb_memory.create_new(
        ResourceWithOnlyTTLField,
        {"name": "Missing attribute name", "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)},
    )

    # Verify no TTL attribute is added
    item1 = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithOnlyTTLField.dynamodb_lookup_keys_from_id(resource1.resource_id)
    )["Item"]
    assert "ttl" not in item1
    assert "expiration" not in item1

    # Resource with only ttl_attribute_name set
    resource2 = dynamodb_memory.create_new(
        ResourceWithOnlyTTLAttribute,
        {"name": "Missing field name", "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)},
    )

    # Verify no TTL attribute is added
    item2 = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithOnlyTTLAttribute.dynamodb_lookup_keys_from_id(resource2.resource_id)
    )["Item"]
    assert "ttl" not in item2
    assert "expiration" not in item2


def test_ttl_with_compressed_data(dynamodb_memory: DynamoDbMemory):
    """Test TTL works correctly with compressed resources."""
    ttl_seconds = 7200
    resource = dynamodb_memory.create_new(
        ResourceWithIntTTL,  # This has compress_data=True
        {"content": "Compressed content with TTL", "ttl_seconds": ttl_seconds},
    )

    # Verify TTL is set correctly even with compression
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithIntTTL.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]

    assert "ttl" in item
    assert "data" in item  # Compressed data
    expected_ttl = int((resource.created_at + timedelta(seconds=ttl_seconds)).timestamp())
    assert abs(item["ttl"] - expected_ttl) < 2


def test_ttl_field_not_in_resource(dynamodb_memory: DynamoDbMemory):
    """Test behavior when TTL field is configured but not present in resource."""

    class MissingFieldResource(DynamoDbResource):
        name: str

        resource_config: ClassVar[ResourceConfig] = ResourceConfig(
            ttl_field="nonexistent_field", ttl_attribute_name="ttl"
        )

    resource = dynamodb_memory.create_new(MissingFieldResource, {"name": "Missing TTL Field"})

    # Should not crash, just not add TTL
    item = dynamodb_memory.dynamodb_table.get_item(
        Key=MissingFieldResource.dynamodb_lookup_keys_from_id(resource.resource_id)
    )["Item"]

    assert "ttl" not in item


def test_multiple_resources_different_ttl_configs(dynamodb_memory: DynamoDbMemory):
    """Test multiple resource types with different TTL configurations."""

    # Resource with datetime TTL
    datetime_resource = dynamodb_memory.create_new(
        ResourceWithDatetimeTTL, {"name": "Datetime", "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)}
    )

    # Resource with integer TTL and custom attribute
    int_resource = dynamodb_memory.create_new(
        ResourceWithIntTTLCustomAttr, {"title": "Integer TTL", "lifetime_seconds": 1800}
    )

    # Resource without TTL
    no_ttl_resource = dynamodb_memory.create_new(ResourceWithoutTTL, {"name": "No TTL", "value": 100})

    # Verify each has correct TTL setup
    datetime_item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithDatetimeTTL.dynamodb_lookup_keys_from_id(datetime_resource.resource_id)
    )["Item"]
    assert "ttl" in datetime_item

    int_item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithIntTTLCustomAttr.dynamodb_lookup_keys_from_id(int_resource.resource_id)
    )["Item"]
    assert "expiration" in int_item  # Custom attribute name

    no_ttl_item = dynamodb_memory.dynamodb_table.get_item(
        Key=ResourceWithoutTTL.dynamodb_lookup_keys_from_id(no_ttl_resource.resource_id)
    )["Item"]
    assert "ttl" not in no_ttl_item
    assert "expiration" not in no_ttl_item
