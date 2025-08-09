"""Test that GSI callable functions returning None don't add fields to DynamoDB items."""

from typing import ClassVar, Optional

import pytest
from boto3.dynamodb.conditions import Key

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import IndexFieldConfig


class ConditionalGSIResource(DynamoDbResource):
    """Resource with GSI fields that conditionally return None."""

    name: str
    category: Optional[str] = None
    priority: Optional[int] = None
    owner: Optional[str] = None

    gsi_config: ClassVar[dict[str, IndexFieldConfig]] = {
        "gsi1": {
            "gsi1pk": lambda self: f"category#{self.category}" if self.category else None,
            "gsi1sk": lambda self: self.name if self.category else None,
        },
        "gsi2": {
            "gsi2pk": lambda self: f"priority#{self.priority}" if self.priority else None,
        },
        "gsi3": {
            "gsi3pk": lambda self: f"owner#{self.owner}" if self.owner else None,
            "gsi3sk": lambda self: f"priority#{self.priority}" if self.owner and self.priority else None,
        },
    }


class ConditionalVersionedResource(DynamoDbVersionedResource):
    """Versioned resource with conditional GSI fields."""

    title: str
    status: Optional[str] = None
    assignee: Optional[str] = None
    project: Optional[str] = None

    gsi_config = {
        "gsi1": {
            "gsi1pk": lambda self: f"assignee#{self.assignee}" if self.assignee else None,
        },
        "gsi2": {
            "gsi2pk": lambda self: f"status#{self.status}" if self.status else None,
            "gsi2sk": lambda self: self.title if self.status else None,
        },
        "gsi3": {
            "gsi3pk": lambda self: f"project#{self.project}" if self.project else None,
        },
    }


def test_gsi_none_exclusion_non_versioned(dynamodb_memory: DynamoDbMemory):
    """Test that GSI fields returning None are not included in DynamoDB items."""
    # Create resource with no optional fields set
    resource = dynamodb_memory.create_new(
        ConditionalGSIResource,
        {"name": "Test Resource"},
    )

    # Verify no GSI fields are present when callables return None
    db_item = resource.to_dynamodb_item()
    assert "gsi1pk" not in db_item
    assert "gsi1sk" not in db_item
    assert "gsi2pk" not in db_item
    assert "gsi3pk" not in db_item
    assert "gsi3sk" not in db_item

    # Create resource with only category set
    resource_with_category = dynamodb_memory.create_new(
        ConditionalGSIResource,
        {"name": "Categorized Resource", "category": "development"},
    )

    # Verify only gsi1 fields are present
    db_item = resource_with_category.to_dynamodb_item()
    assert db_item["gsi1pk"] == "category#development"
    assert db_item["gsi1sk"] == "Categorized Resource"
    assert "gsi2pk" not in db_item
    assert "gsi3pk" not in db_item
    assert "gsi3sk" not in db_item

    # Create resource with all fields set
    full_resource = dynamodb_memory.create_new(
        ConditionalGSIResource,
        {
            "name": "Full Resource",
            "category": "testing",
            "priority": 1,
            "owner": "alice",
        },
    )

    # Verify all GSI fields are present
    db_item = full_resource.to_dynamodb_item()
    assert db_item["gsi1pk"] == "category#testing"
    assert db_item["gsi1sk"] == "Full Resource"
    assert db_item["gsi2pk"] == "priority#1"
    assert db_item["gsi3pk"] == "owner#alice"
    assert db_item["gsi3sk"] == "priority#1"


def test_gsi_none_exclusion_versioned(dynamodb_memory: DynamoDbMemory):
    """Test that versioned resources handle None-returning GSI callables correctly."""
    # Create versioned resource with minimal fields
    resource = dynamodb_memory.create_new(
        ConditionalVersionedResource,
        {"title": "Initial Task"},
    )

    # Verify no GSI fields are present in v0 object
    db_item = resource.to_dynamodb_item(v0_object=True)
    assert "gsi1pk" not in db_item
    assert "gsi2pk" not in db_item
    assert "gsi2sk" not in db_item
    assert "gsi3pk" not in db_item

    # Update with assignee only
    updated = dynamodb_memory.update_existing(
        resource,
        {"assignee": "bob"},
    )

    # Verify only gsi1 is present in v0 object
    db_item = updated.to_dynamodb_item(v0_object=True)
    assert db_item["gsi1pk"] == "assignee#bob"
    assert "gsi2pk" not in db_item
    assert "gsi2sk" not in db_item
    assert "gsi3pk" not in db_item

    # Update with all fields
    fully_updated = dynamodb_memory.update_existing(
        updated,
        {"status": "in_progress", "project": "ProjectX"},
    )

    # Verify all GSI fields are present in v0 object
    db_item = fully_updated.to_dynamodb_item(v0_object=True)
    assert db_item["gsi1pk"] == "assignee#bob"
    assert db_item["gsi2pk"] == "status#in_progress"
    assert db_item["gsi2sk"] == "Initial Task"
    assert db_item["gsi3pk"] == "project#ProjectX"


def test_gsi_none_queries(dynamodb_memory: DynamoDbMemory):
    """Test that queries work correctly when some resources have None GSI values."""
    # Create multiple resources with varying GSI field presence
    resources = []
    
    # Resource with no GSI fields
    resources.append(
        dynamodb_memory.create_new(
            ConditionalGSIResource,
            {"name": "Uncategorized 1"},
        )
    )
    
    # Resources with category only
    for i in range(2):
        resources.append(
            dynamodb_memory.create_new(
                ConditionalGSIResource,
                {"name": f"Dev Task {i}", "category": "development"},
            )
        )
    
    # Resources with owner and priority
    resources.append(
        dynamodb_memory.create_new(
            ConditionalGSIResource,
            {"name": "Owned Task", "owner": "charlie", "priority": 2},
        )
    )
    
    # Resources with all fields
    resources.append(
        dynamodb_memory.create_new(
            ConditionalGSIResource,
            {
                "name": "Complete Task",
                "category": "development",
                "priority": 1,
                "owner": "alice",
            },
        )
    )

    # Query by category - should only find resources with category set
    dev_resources = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi1pk").eq("category#development"),
        index_name="gsi1",
        resource_class=ConditionalGSIResource,
    )
    assert len(dev_resources) == 3  # 2 dev-only + 1 complete
    assert all(r.category == "development" for r in dev_resources)

    # Query by owner - should only find resources with owner set
    charlie_resources = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("owner#charlie"),
        index_name="gsi3",
        resource_class=ConditionalGSIResource,
    )
    assert len(charlie_resources) == 1
    assert charlie_resources[0].owner == "charlie"

    # Query by priority - should find resources with priority set
    priority_1_resources = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi2pk").eq("priority#1"),
        index_name="gsi2",
        resource_class=ConditionalGSIResource,
    )
    assert len(priority_1_resources) == 1
    assert priority_1_resources[0].priority == 1


def test_gsi_update_removes_fields_when_none(dynamodb_memory: DynamoDbMemory):
    """Test that updating a resource to have None values removes GSI fields."""
    # Create resource with all fields
    resource = dynamodb_memory.create_new(
        ConditionalGSIResource,
        {
            "name": "Mutable Resource",
            "category": "initial",
            "priority": 5,
            "owner": "david",
        },
    )

    # Verify all GSI fields are present initially
    db_item = resource.to_dynamodb_item()
    assert db_item["gsi1pk"] == "category#initial"
    assert db_item["gsi2pk"] == "priority#5"
    assert db_item["gsi3pk"] == "owner#david"

    # Update to remove category (set to None)
    updated = dynamodb_memory.update_existing(
        resource,
        {"category": None},
    )

    # Verify gsi1 fields are removed but others remain
    db_item = updated.to_dynamodb_item()
    assert "gsi1pk" not in db_item
    assert "gsi1sk" not in db_item
    assert db_item["gsi2pk"] == "priority#5"
    assert db_item["gsi3pk"] == "owner#david"

    # Update to remove all optional fields
    final = dynamodb_memory.update_existing(
        updated,
        {"priority": None, "owner": None},
    )

    # Verify all GSI fields are removed
    db_item = final.to_dynamodb_item()
    assert "gsi1pk" not in db_item
    assert "gsi1sk" not in db_item
    assert "gsi2pk" not in db_item
    assert "gsi3pk" not in db_item
    assert "gsi3sk" not in db_item


def test_mixed_gsi_config_with_none_handling(dynamodb_memory: DynamoDbMemory):
    """Test mixing static values, always-present callables, and conditional callables."""

    class MixedGSIResource(DynamoDbResource):
        """Resource with mixed GSI configuration styles."""

        name: str
        type: str  # Always present
        optional_tag: Optional[str] = None

        gsi_config: ClassVar[dict[str, IndexFieldConfig]] = {
            "gsi1": {
                "gsi1pk": "STATIC_VALUE",  # Static string
                "gsi1sk": lambda self: self.name,  # Always returns a value
            },
            "gsi2": {
                "gsi2pk": lambda self: f"type#{self.type}",  # Always returns a value
                "gsi2sk": lambda self: f"tag#{self.optional_tag}" if self.optional_tag else None,  # Conditional
            },
        }

    # Create resource without optional field
    resource = dynamodb_memory.create_new(
        MixedGSIResource,
        {"name": "Mixed Test", "type": "document"},
    )

    db_item = resource.to_dynamodb_item()
    # Static and always-present fields should exist
    assert db_item["gsi1pk"] == "STATIC_VALUE"
    assert db_item["gsi1sk"] == "Mixed Test"
    assert db_item["gsi2pk"] == "type#document"
    # Conditional field should not exist
    assert "gsi2sk" not in db_item

    # Create resource with optional field
    tagged_resource = dynamodb_memory.create_new(
        MixedGSIResource,
        {"name": "Tagged Test", "type": "image", "optional_tag": "important"},
    )

    db_item = tagged_resource.to_dynamodb_item()
    # All fields should exist
    assert db_item["gsi1pk"] == "STATIC_VALUE"
    assert db_item["gsi1sk"] == "Tagged Test"
    assert db_item["gsi2pk"] == "type#image"
    assert db_item["gsi2sk"] == "tag#important"