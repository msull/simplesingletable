"""Example demonstrating local file storage for offline demos and testing.

This shows how to use LocalStorageMemory as a drop-in replacement for DynamoDbMemory
for local testing and offline demonstrations.
"""
import tempfile
from typing import ClassVar, Optional

from logzero import logger
from pydantic import BaseModel

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource, LocalStorageMemory
from simplesingletable.models import BlobFieldConfig, ResourceConfig


# Define your resources as usual
class Task(DynamoDbVersionedResource):
    """A simple task with version history."""

    title: str
    description: str
    completed: bool = False
    category: str

    def db_get_gsi1pk(self) -> str | None:
        """Enable querying by category."""
        return f"category#{self.category}"


class Note(BaseModel):
    """Nested Pydantic model for blob storage."""

    author: str
    text: str


class Document(DynamoDbResource):
    """A document with large text stored as blobs."""

    name: str
    tags: list[str]
    notes: Optional[list[Note]] = None

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={
            "notes": BlobFieldConfig(compress=True),
        },
    )


def main():
    """Demonstrate local storage usage."""
    # Create a temporary directory for storage
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"📁 Using local storage directory: {tmpdir}\n")

        # Create local storage instance
        # This has the same interface as DynamoDbMemory!
        storage = LocalStorageMemory(
            logger=logger,
            storage_dir=tmpdir,
            track_stats=True,
            use_blob_storage=True,
        )

        # Create some tasks
        print("📝 Creating tasks...")
        task1 = storage.create_new(
            Task,
            {
                "title": "Write documentation",
                "description": "Document the local storage feature",
                "category": "work",
            },
        )
        print(f"   Created: {task1.title} (v{task1.version})")

        task2 = storage.create_new(
            Task,
            {
                "title": "Buy groceries",
                "description": "Milk, eggs, bread",
                "category": "personal",
            },
        )
        print(f"   Created: {task2.title} (v{task2.version})")

        # Update a task to create version history
        print(f"\n✏️  Updating task: {task1.title}")
        task1_updated = storage.update_existing(
            task1,
            {"completed": True, "description": "Documentation completed!"},
        )
        print(f"   Version {task1_updated.version}: {task1_updated.description}")

        # Query tasks by category
        print("\n🔍 Querying work tasks...")
        from boto3.dynamodb.conditions import Key

        work_tasks = storage.paginated_dynamodb_query(
            resource_class=Task,
            index_name="gsi1",
            key_condition=Key("gsi1pk").eq("category#work"),
            results_limit=10,
        )
        print(f"   Found {len(work_tasks)} work task(s):")
        for task in work_tasks:
            status = "✓" if task.completed else "○"
            print(f"   {status} {task.title}")

        # Get version history
        print(f"\n📜 Version history for '{task1.title}':")
        versions = storage.get_all_versions(task1.resource_id, Task)
        for v in versions:
            print(f"   v{v.version}: {v.description} (completed: {v.completed})")

        # Create a document with blob storage
        print("\n📄 Creating document with blob storage...")
        doc = storage.create_new(
            Document,
            {
                "name": "Meeting Notes",
                "tags": ["meeting", "2025-10"],
                "notes": [
                    Note(author="Alice", text="Need to follow up on action items"),
                    Note(author="Bob", text="Budget approved"),
                ],
            },
        )
        print(f"   Created: {doc.name}")

        # Read document without loading blobs (fast)
        print("\n⚡ Reading document without loading blobs...")
        doc_fast = storage.read_existing(doc.resource_id, Document, load_blobs=False)
        print(f"   Has unloaded blobs: {doc_fast.has_unloaded_blobs()}")
        print(f"   Unloaded fields: {doc_fast.get_unloaded_blob_fields()}")

        # Read document with blobs loaded
        print("\n💾 Reading document with blobs loaded...")
        doc_full = storage.read_existing(doc.resource_id, Document, load_blobs=True)
        print(f"   Loaded {len(doc_full.notes)} notes:")
        for note in doc_full.notes:
            print(f"   - {note.author}: {note.text}")

        # Show stats
        print("\n📊 Storage statistics:")
        stats = storage.get_stats()
        for resource_type, count in stats.counts_by_type.items():
            print(f"   {resource_type}: {count}")

        print("\n✨ Demo complete!")
        print(f"\n💡 Data is stored in: {tmpdir}")
        print("   - resources/*.json files contain the resources")
        print("   - blobs/ directory contains blob field data")


if __name__ == "__main__":
    main()
