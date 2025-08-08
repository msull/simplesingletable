"""Tests for the refactoring features: dynamic GSI, version limits, and improved error handling."""

from typing import ClassVar

import pytest
from boto3.dynamodb.conditions import Attr, Key

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.dynamodb_memory import build_lek_data, transact_write_safe
from simplesingletable.models import IndexFieldConfig


class TaskResource(DynamoDbResource):
    """Example resource with dynamic GSI configuration."""

    title: str
    completed: bool
    category: str
    priority: int

    gsi_config: ClassVar[dict[str, IndexFieldConfig]] = {
        "gsi1": {
            "gsi1pk": lambda self: f"task|{'COMPLETE' if self.completed else 'INCOMPLETE'}",
        },
        "gsi2": {
            "gsi2pk": lambda self: f"category#{self.category}",
        },
        "gsi3": {
            "gsi3pk": lambda self: f"priority#{self.priority}",
            "gsi3sk": lambda self: self.title,
        },
    }


class VersionedTaskResource(DynamoDbVersionedResource):
    """Example versioned resource with dynamic GSI and version limits."""

    title: str
    status: str
    assignee: str

    # Override to enforce max 3 versions
    resource_config = {"max_versions": 3}

    gsi_config = {
        "gsi1": {
            "gsi1pk": lambda self: f"assignee#{self.assignee}",
        },
        "gsi2": {
            "gsi2pk": lambda self: f"status#{self.status}",
        },
    }


def test_dynamic_gsi_configuration(dynamodb_memory: DynamoDbMemory):
    """Test that dynamic GSI configuration works correctly."""
    # Create a task resource
    task = dynamodb_memory.create_new(
        TaskResource,
        {
            "title": "Implement feature X",
            "completed": False,
            "category": "development",
            "priority": 1,
        },
    )

    # Check the DynamoDB item has the correct GSI fields
    db_item = task.to_dynamodb_item()
    assert db_item["gsi1pk"] == "task|INCOMPLETE"
    assert db_item["gsi2pk"] == "category#development"
    assert db_item["gsi3pk"] == "priority#1"
    assert db_item["gsi3sk"] == "Implement feature X"

    # Update the task to completed
    completed_task = dynamodb_memory.update_existing(task, {"completed": True})

    # Check GSI fields are updated
    db_item = completed_task.to_dynamodb_item()
    assert db_item["gsi1pk"] == "task|COMPLETE"


def test_version_limit_enforcement(dynamodb_memory: DynamoDbMemory):
    """Test that version limits are enforced correctly."""
    # Create a versioned resource
    task = dynamodb_memory.create_new(
        VersionedTaskResource,
        {
            "title": "Initial task",
            "status": "pending",
            "assignee": "user1",
        },
    )

    # Create multiple versions (more than the limit of 3)
    for i in range(5):
        task = dynamodb_memory.update_existing(task, {"title": f"Updated task v{i+2}"})

    # Query all versions to check that only 3 are kept (plus v0)
    all_versions = dynamodb_memory.dynamodb_table.query(
        KeyConditionExpression=Key("pk").eq(f"VersionedTaskResource#{task.resource_id}") & Key("sk").begins_with("v"),
        ScanIndexForward=True,
    )["Items"]

    # Should have v0 plus the 3 most recent versions (v4, v5, v6)
    version_numbers = [int(item["version"]) for item in all_versions if item["sk"] != "v0"]
    assert len(version_numbers) == 3
    assert version_numbers == [4, 5, 6]  # The 3 most recent versions


def test_build_lek_data_helper():
    """Test the build_lek_data helper function."""
    # Test with no index (main table)
    db_item = {"pk": "test#123", "sk": "v0"}
    lek_data = build_lek_data(db_item, None, TaskResource)
    assert lek_data == {"pk": "test#123", "sk": "v0"}

    # Test with gsitype index
    db_item = {"pk": "test#123", "sk": "v0", "gsitype": "TaskResource", "gsitypesk": "2023-10-09T12:00:00"}
    lek_data = build_lek_data(db_item, "gsitype", TaskResource)
    assert lek_data == {"pk": "test#123", "sk": "v0", "gsitype": "TaskResource", "gsitypesk": "2023-10-09T12:00:00"}

    # Test with dynamic GSI
    db_item = {
        "pk": "test#123",
        "sk": "v0",
        "gsi1pk": "task|COMPLETE",
    }
    lek_data = build_lek_data(db_item, "gsi1", TaskResource)
    assert lek_data == {
        "pk": "test#123",
        "sk": "v0",
        "gsi1pk": "task|COMPLETE",
    }

    # Test with GSI that has both pk and sk
    db_item = {"pk": "test#123", "sk": "v0", "gsi3pk": "priority#1", "gsi3sk": "Some title"}
    lek_data = build_lek_data(db_item, "gsi3", TaskResource)
    assert lek_data == {"pk": "test#123", "sk": "v0", "gsi3pk": "priority#1", "gsi3sk": "Some title"}


def test_transact_write_safe_error_handling(dynamodb_memory: DynamoDbMemory):
    """Test that transact_write_safe provides better error messages."""
    # This test is a bit tricky since we need to trigger a real transaction error
    # We'll try to create the same item twice, which should fail

    task = dynamodb_memory.create_new(
        TaskResource,
        {
            "title": "Test task",
            "completed": False,
            "category": "test",
            "priority": 1,
        },
    )

    # Try to create the same item again using transact_write_safe
    # This should fail with a better error message
    from simplesingletable.utils import marshall

    with pytest.raises(ValueError) as exc_info:
        transact_write_safe(
            dynamodb_memory.dynamodb_client,
            [
                {
                    "Put": {
                        "TableName": dynamodb_memory.table_name,
                        "Item": marshall(task.to_dynamodb_item()),
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                }
            ],
        )

    assert "Transaction failed:" in str(exc_info.value)


def test_dynamic_gsi_queries(dynamodb_memory: DynamoDbMemory):
    """Test querying using dynamic GSI configuration."""
    # Create multiple tasks with different statuses
    tasks = []
    for i in range(5):
        completed = i % 2 == 0
        task = dynamodb_memory.create_new(
            TaskResource,
            {
                "title": f"Task {i}",
                "completed": completed,
                "category": "dev" if i < 3 else "ops",
                "priority": (i % 3) + 1,
            },
        )
        tasks.append(task)

    # Query by completion status using gsi1
    incomplete_tasks = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi1pk").eq("task|INCOMPLETE"),
        index_name="gsi1",
        resource_class=TaskResource,
    )
    assert len(incomplete_tasks) == 2
    assert all(not task.completed for task in incomplete_tasks)

    # Query by category using gsi2
    dev_tasks = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi2pk").eq("category#dev"),
        index_name="gsi2",
        resource_class=TaskResource,
    )
    assert len(dev_tasks) == 3
    assert all(task.category == "dev" for task in dev_tasks)

    # Query by priority using gsi3 (which has a sort key)
    priority_1_tasks = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("priority#1"),
        index_name="gsi3",
        resource_class=TaskResource,
        ascending=True,  # Sort by title
    )
    assert len(priority_1_tasks) == 2
    assert all(task.priority == 1 for task in priority_1_tasks)
    # Check they're sorted by title
    titles = [task.title for task in priority_1_tasks]
    assert titles == sorted(titles)


def test_filter_expressions_with_refactoring(dynamodb_memory: DynamoDbMemory):
    """Test that filter expressions still work with the refactored code."""
    # Create tasks with various priorities
    for i in range(10):
        dynamodb_memory.create_new(
            TaskResource,
            {
                "title": f"Task {i}",
                "completed": i % 3 == 0,
                "category": "test",
                "priority": i,
            },
        )

    # Query with filter expression for high priority tasks
    high_priority_tasks = dynamodb_memory.list_type_by_updated_at(
        TaskResource,
        filter_expression=Attr("priority").gt(7),
    )

    assert len(high_priority_tasks) == 2
    assert all(task.priority > 7 for task in high_priority_tasks)

    # Query with compound filter
    specific_tasks = dynamodb_memory.list_type_by_updated_at(
        TaskResource,
        filter_expression=Attr("priority").between(3, 6) & Attr("completed").eq(False),
    )

    assert all(3 <= task.priority <= 6 and not task.completed for task in specific_tasks)


def test_backward_compatibility_with_legacy_gsi_methods(dynamodb_memory: DynamoDbMemory):
    """Test that resources using legacy GSI methods still work."""

    class LegacyResource(DynamoDbResource):
        name: str
        parent_id: str

        def db_get_gsi1pk(self) -> str | None:
            return f"parent#{self.parent_id}"

    # Create a resource using legacy methods
    resource = dynamodb_memory.create_new(
        LegacyResource,
        {
            "name": "Legacy test",
            "parent_id": "parent123",
        },
    )

    # Check the GSI field is set correctly
    db_item = resource.to_dynamodb_item()
    assert db_item["gsi1pk"] == "parent#parent123"

    # Query using the GSI
    results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi1pk").eq("parent#parent123"),
        index_name="gsi1",
        resource_class=LegacyResource,
    )
    assert len(results) == 1
    assert results[0].resource_id == resource.resource_id


def test_gsi_config_classmethod_override(dynamodb_memory: DynamoDbMemory):
    """Test that overriding get_gsi_config() classmethod works correctly."""

    class DynamicGSIResource(DynamoDbResource):
        """Resource that overrides get_gsi_config() instead of using classvar."""

        name: str
        status: str
        owner: str

        @classmethod
        def get_gsi_config(cls) -> dict:
            """Override to provide dynamic GSI configuration."""
            return {
                "gsi1": {
                    "gsi1pk": lambda self: f"owner#{self.owner}",
                },
                "gsi3": {
                    "gsi3pk": lambda self: f"status#{self.status}",
                    "gsi3sk": lambda self: self.name,
                },
            }

    # Create a resource using the classmethod override
    resource = dynamodb_memory.create_new(
        DynamicGSIResource,
        {
            "name": "Project Alpha",
            "status": "active",
            "owner": "alice",
        },
    )

    # Verify GSI fields are set correctly
    db_item = resource.to_dynamodb_item()
    assert db_item["gsi1pk"] == "owner#alice"
    assert "gsi1sk" not in db_item  # No sort key for gsi1
    assert db_item["gsi3pk"] == "status#active"
    assert db_item["gsi3sk"] == "Project Alpha"

    # Create another resource with different values
    resource2 = dynamodb_memory.create_new(
        DynamicGSIResource,
        {
            "name": "Project Beta",
            "status": "active",
            "owner": "bob",
        },
    )

    # Query by owner using GSI1
    alice_results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi1pk").eq("owner#alice"),
        index_name="gsi1",
        resource_class=DynamicGSIResource,
    )
    assert len(alice_results) == 1
    assert alice_results[0].name == "Project Alpha"

    # Query active projects using GSI3 (sorted by name)
    active_results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("status#active"),
        index_name="gsi3",
        resource_class=DynamicGSIResource,
        ascending=True,  # Ensure consistent sort order
    )
    assert len(active_results) == 2
    # Results should be sorted by name (sk)
    assert active_results[0].name == "Project Alpha"
    assert active_results[1].name == "Project Beta"


def test_gsi_config_combined_approaches(dynamodb_memory: DynamoDbMemory):
    """Test that classmethod override takes precedence over classvar."""

    class HybridGSIResource(DynamoDbResource):
        """Resource with both classvar and classmethod GSI config."""

        name: str
        category: str
        priority: int

        # Define classvar config
        gsi_config: ClassVar[dict[str, IndexFieldConfig]] = {
            "gsi1": {"gsi1pk": lambda self: f"category#{self.category}"}
        }

        @classmethod
        def get_gsi_config(cls) -> dict:
            """Override should take precedence over classvar."""
            return {
                "gsi2": {
                    "gsi2pk": lambda self: f"category#{self.category}",
                },
                "gsi3": {
                    "gsi3pk": lambda self: f"priority#{self.priority}",  # Different from classvar
                    "gsi3sk": lambda self: self.name,
                },
            }

    # Create a resource
    resource = dynamodb_memory.create_new(
        HybridGSIResource,
        {
            "name": "Task X",
            "category": "urgent",
            "priority": 1,
        },
    )

    # Verify that classmethod override takes precedence
    db_item = resource.to_dynamodb_item()
    assert db_item["gsi3pk"] == "priority#1"  # From classmethod, not "category#urgent" from classvar
    assert db_item["gsi3sk"] == "Task X"  # From classmethod
    assert db_item["gsi2pk"] == "category#urgent"  # From classmethod

    # Query should use the overridden configuration
    results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("priority#1"),
        index_name="gsi3",
        resource_class=HybridGSIResource,
    )
    assert len(results) == 1
    assert results[0].name == "Task X"
