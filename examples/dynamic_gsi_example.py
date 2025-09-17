"""Example demonstrating the new dynamic GSI configuration feature."""

from typing import ClassVar
from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import IndexFieldConfig
from boto3.dynamodb.conditions import Key


class TaskResource(DynamoDbResource):
    """Example resource with dynamic GSI configuration.

    This demonstrates how to configure multiple GSIs dynamically using
    the new gsi_config class variable instead of hardcoded methods.
    """

    title: str
    completed: bool
    category: str
    priority: int
    assigned_to: str

    # Define GSIs using the new configuration system
    gsi_config: ClassVar[dict[str, IndexFieldConfig]] = {
        # GSI1: Partition by completion status
        "gsi1": {
            "pk": lambda self: f"task|{'COMPLETE' if self.completed else 'INCOMPLETE'}",
            "sk": None,  # No sort key for this index
        },
        # GSI2: Partition by category
        "gsi2": {
            "pk": lambda self: f"category#{self.category}",
            "sk": None,
        },
        # GSI3: Partition by priority with title as sort key
        "gsi3": {
            "pk": lambda self: f"priority#{self.priority}",
            "sk": lambda self: self.title,  # Sort by title within each priority
        },
    }


class ProjectResource(DynamoDbVersionedResource):
    """Example versioned resource with dynamic GSI and version limits."""

    name: str
    status: str
    owner: str
    team: str

    # Enforce a maximum of 5 versions (older versions will be automatically deleted)
    model_config = {"extra": "forbid", "max_versions": 5}

    gsi_config: ClassVar[dict[str, IndexFieldConfig]] = {
        # GSI1: Partition by owner
        "gsi1": {
            "pk": lambda self: f"owner#{self.owner}",
            "sk": None,
        },
        # GSI2: Partition by team and status
        "gsi2": {
            "pk": lambda self: f"team#{self.team}",
            "sk": lambda self: f"status#{self.status}",
        },
    }


def demo_dynamic_gsi():
    """Demonstrate usage of dynamic GSI configuration."""

    # Initialize memory (in production, this would connect to real DynamoDB)
    memory = DynamoDbMemory(
        logger=print,
        table_name="test-table",
        # For testing, you might use a local DynamoDB instance
        # endpoint_url="http://localhost:8000"
    )

    # Create some tasks
    memory.create_new(
        TaskResource,
        {
            "title": "Implement login feature",
            "completed": False,
            "category": "backend",
            "priority": 1,
            "assigned_to": "alice",
        },
    )

    memory.create_new(
        TaskResource,
        {
            "title": "Design dashboard UI",
            "completed": True,
            "category": "frontend",
            "priority": 2,
            "assigned_to": "bob",
        },
    )

    memory.create_new(
        TaskResource,
        {
            "title": "Fix authentication bug",
            "completed": False,
            "category": "backend",
            "priority": 1,
            "assigned_to": "alice",
        },
    )

    # Query incomplete tasks using GSI1
    print("\n=== Incomplete Tasks ===")
    incomplete_tasks = memory.paginated_dynamodb_query(
        key_condition=Key("gsi1pk").eq("task|INCOMPLETE"),
        index_name="gsi1",
        resource_class=TaskResource,
    )
    for task in incomplete_tasks:
        print(f"- {task.title} (Priority: {task.priority})")

    # Query backend tasks using GSI2
    print("\n=== Backend Tasks ===")
    backend_tasks = memory.paginated_dynamodb_query(
        key_condition=Key("gsi2pk").eq("category#backend"),
        index_name="gsi2",
        resource_class=TaskResource,
    )
    for task in backend_tasks:
        print(f"- {task.title} (Completed: {task.completed})")

    # Query high priority tasks using GSI3 (sorted by title)
    print("\n=== High Priority Tasks (P1) ===")
    p1_tasks = memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("priority#1"),
        index_name="gsi3",
        resource_class=TaskResource,
        ascending=True,  # Sort by title alphabetically
    )
    for task in p1_tasks:
        print(f"- {task.title}")

    # Create a versioned project
    project = memory.create_new(
        ProjectResource,
        {
            "name": "New Website",
            "status": "planning",
            "owner": "charlie",
            "team": "web",
        },
    )

    # Update the project multiple times (demonstrating version limit)
    for i in range(7):
        project = memory.update_existing(
            project,
            {"status": ["planning", "in_progress", "review", "testing", "staging", "deployed", "maintenance"][i]},
        )
        print(f"Updated project to version {project.version}, status: {project.status}")

    # The oldest versions (beyond the limit of 5) will be automatically deleted
    # Only versions 3, 4, 5, 6, 7 will remain (plus v0)


if __name__ == "__main__":
    # Note: This example requires a DynamoDB table to be set up
    # For local testing, you can use DynamoDB Local
    print("This is a demonstration of the dynamic GSI configuration.")
    print("To run this example, you need a DynamoDB table configured.")

    # Uncomment to run the demo:
    # demo_dynamic_gsi()
