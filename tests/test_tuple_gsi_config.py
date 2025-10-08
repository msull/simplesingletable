"""Tests for tuple-based GSI configuration format."""

from datetime import datetime, timezone
from typing import ClassVar

from boto3.dynamodb.conditions import Key

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource


class ChatSession(DynamoDbResource):
    """Example resource using tuple-based GSI configuration."""

    username: str
    active: bool
    last_message_at: datetime | None = None
    channel: str | None = None

    @classmethod
    def get_gsi_config(cls) -> dict:
        """Use tuple format for combined pk/sk definition."""
        return {
            "gsi3": {
                ("gsi3pk", "gsi3sk"): cls._get_gsi3pk_and_sk,
            }
        }

    def _get_gsi3pk_and_sk(self) -> tuple[str, str] | None:
        """Return both pk and sk values for gsi3."""
        if self.active:
            pk = "ChatSession#" + self.username
            sk = (self.last_message_at or self.created_at).isoformat()
            return (pk, sk)
        return None


class MixedGSIResource(DynamoDbResource):
    """Resource using both tuple and regular GSI configuration."""

    project_id: str
    status: str
    priority: int
    owner: str
    updated_by: str | None = None

    @classmethod
    def get_gsi_config(cls) -> dict:
        """Mix of regular and tuple-based configuration."""
        return {
            "gsi1": {
                "gsi1pk": lambda self: f"project#{self.project_id}",
            },
            "gsi2": {
                "gsi2pk": lambda self: f"status#{self.status}",
            },
            "gsi3": {
                ("gsi3pk", "gsi3sk"): cls._get_priority_index,
            },
        }

    def _get_priority_index(self) -> tuple[str, str] | None:
        """Return priority-based index values."""
        pk = f"priority#{self.priority}"
        sk = f"{self.owner}#{self.resource_id}"
        return (pk, sk)


class ConditionalTupleResource(DynamoDbVersionedResource):
    """Resource where tuple method can return None conditionally."""

    category: str
    published: bool
    publish_date: datetime | None = None
    author: str

    resource_config = {"max_versions": 5}

    @classmethod
    def get_gsi_config(cls) -> dict:
        return {
            "gsi1": {
                "gsi1pk": lambda self: f"author#{self.author}",
            },
            "gsi3": {
                ("gsi3pk", "gsi3sk"): cls._get_published_index,
            },
        }

    def _get_published_index(self) -> tuple[str, str] | None:
        """Only index published items."""
        if self.published and self.publish_date:
            pk = f"category#{self.category}"
            sk = self.publish_date.isoformat()
            return (pk, sk)
        return None


def test_basic_tuple_gsi_config(dynamodb_memory: DynamoDbMemory):
    """Test basic tuple-based GSI configuration."""
    # Create a chat session with active status
    now = datetime.now(timezone.utc)
    session = dynamodb_memory.create_new(
        ChatSession,
        {
            "username": "alice",
            "active": True,
            "last_message_at": now,
            "channel": "general",
        },
    )

    # Check that GSI fields are set correctly
    db_item = session.to_dynamodb_item()
    assert db_item["gsi3pk"] == "ChatSession#alice"
    assert db_item["gsi3sk"] == now.isoformat()

    # Create inactive session (should not have GSI values)
    inactive = dynamodb_memory.create_new(
        ChatSession,
        {
            "username": "bob",
            "active": False,
            "channel": "random",
        },
    )

    db_item = inactive.to_dynamodb_item()
    assert "gsi3pk" not in db_item
    assert "gsi3sk" not in db_item


def test_mixed_gsi_configuration(dynamodb_memory: DynamoDbMemory):
    """Test mixing tuple and regular GSI configuration."""
    resource = dynamodb_memory.create_new(
        MixedGSIResource,
        {
            "project_id": "proj-123",
            "status": "in-progress",
            "priority": 1,
            "owner": "alice",
            "updated_by": "bob",
        },
    )

    db_item = resource.to_dynamodb_item()

    # Check regular GSI fields
    assert db_item["gsi1pk"] == "project#proj-123"
    assert db_item["gsi2pk"] == "status#in-progress"

    # Check tuple-based GSI fields
    assert db_item["gsi3pk"] == "priority#1"
    assert db_item["gsi3sk"] == f"alice#{resource.resource_id}"


def test_conditional_tuple_return(dynamodb_memory: DynamoDbMemory):
    """Test that tuple methods can conditionally return None."""
    # Create published resource
    publish_date = datetime.now(timezone.utc)
    published = dynamodb_memory.create_new(
        ConditionalTupleResource,
        {
            "category": "tech",
            "published": True,
            "publish_date": publish_date,
            "author": "alice",
        },
    )

    db_item = published.to_dynamodb_item(v0_object=True)
    assert db_item["gsi1pk"] == "author#alice"
    assert db_item["gsi3pk"] == "category#tech"
    assert db_item["gsi3sk"] == publish_date.isoformat()

    # Create unpublished resource (no GSI3 values)
    unpublished = dynamodb_memory.create_new(
        ConditionalTupleResource,
        {
            "category": "science",
            "published": False,
            "author": "bob",
        },
    )

    db_item = unpublished.to_dynamodb_item(v0_object=True)
    assert db_item["gsi1pk"] == "author#bob"
    assert "gsi3pk" not in db_item
    assert "gsi3sk" not in db_item


def test_querying_with_tuple_gsi(dynamodb_memory: DynamoDbMemory):
    """Test querying using tuple-based GSI configuration."""
    # Create multiple chat sessions
    base_time = datetime.now(timezone.utc)
    sessions = []

    for i in range(5):
        last_message = base_time if i < 3 else None
        session = dynamodb_memory.create_new(
            ChatSession,
            {
                "username": f"user{i}",
                "active": i < 3,  # First 3 are active
                "last_message_at": last_message,
                "channel": "general" if i % 2 == 0 else "random",
            },
        )
        sessions.append(session)

    # Query active sessions for user0
    results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("ChatSession#user0"),
        index_name="gsi3",
        resource_class=ChatSession,
    )
    assert len(results) == 1
    assert results[0].username == "user0"

    # Verify inactive sessions don't appear in GSI
    # Try to query for an inactive user - should not exist in the GSI
    # Since user3 is inactive, there should be no GSI entry at all
    # We need to check differently - let's query all ChatSession entries
    # and verify user3 isn't there
    results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("ChatSession#user3"),
        index_name="gsi3",
        resource_class=ChatSession,
    )
    assert len(results) == 0


def test_update_with_tuple_gsi(dynamodb_memory: DynamoDbMemory):
    """Test updating resources with tuple-based GSI configuration."""
    # Create an inactive session
    session = dynamodb_memory.create_new(
        ChatSession,
        {
            "username": "charlie",
            "active": False,
            "channel": "support",
        },
    )

    # Verify no GSI fields initially
    db_item = session.to_dynamodb_item()
    assert "gsi3pk" not in db_item
    assert "gsi3sk" not in db_item

    # Update to active with last message time
    now = datetime.now(timezone.utc)
    updated = dynamodb_memory.update_existing(
        session,
        {
            "active": True,
            "last_message_at": now,
        },
    )

    # Verify GSI fields are now present
    db_item = updated.to_dynamodb_item()
    assert db_item["gsi3pk"] == "ChatSession#charlie"
    assert db_item["gsi3sk"] == now.isoformat()


def test_complex_sorting_with_tuple_gsi(dynamodb_memory: DynamoDbMemory):
    """Test complex sorting scenarios with tuple-based GSI."""
    # Create resources with different priorities and owners
    resources = []
    for priority in [1, 2, 1, 3, 1]:
        for i, owner in enumerate(["alice", "bob", "charlie"]):
            resource = dynamodb_memory.create_new(
                MixedGSIResource,
                {
                    "project_id": f"proj-{priority}-{i}",
                    "status": "active",
                    "priority": priority,
                    "owner": owner,
                },
            )
            resources.append(resource)

    # Query priority 1 items - should be sorted by owner#resource_id
    priority_1_results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("priority#1"),
        index_name="gsi3",
        resource_class=MixedGSIResource,
        ascending=True,
    )

    # Should have 9 items (3 priority=1 iterations * 3 owners)
    assert len(priority_1_results) == 9

    # Verify all are priority 1
    assert all(r.priority == 1 for r in priority_1_results)

    # Check sorting by sk (owner#resource_id)
    sk_values = [f"{r.owner}#{r.resource_id}" for r in priority_1_results]
    assert sk_values == sorted(sk_values)


def test_versioned_resource_with_tuple_gsi(dynamodb_memory: DynamoDbMemory):
    """Test that tuple GSI works with versioned resources."""
    # Create initial version
    publish_date = datetime.now(timezone.utc)
    resource = dynamodb_memory.create_new(
        ConditionalTupleResource,
        {
            "category": "science-v1",
            "published": True,
            "publish_date": publish_date,
            "author": "einstein",
        },
    )

    # Update multiple times to create versions
    for i in range(2, 5):
        resource = dynamodb_memory.update_existing(resource, {"category": f"science-v{i}"})

    # Query by author (regular GSI)
    author_results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi1pk").eq("author#einstein"),
        index_name="gsi1",
        resource_class=ConditionalTupleResource,
    )
    assert len(author_results) == 1
    assert author_results[0].author == "einstein"

    # Query by category (tuple GSI) - should find the latest version
    category_results = dynamodb_memory.paginated_dynamodb_query(
        key_condition=Key("gsi3pk").eq("category#science-v4"),
        index_name="gsi3",
        resource_class=ConditionalTupleResource,
    )
    assert len(category_results) == 1
    assert category_results[0].category == "science-v4"


def test_exclude_keys_with_tuple_config(dynamodb_memory: DynamoDbMemory):
    """Test that tuple GSI keys are properly excluded when building from item."""
    # Create a resource with all GSI types
    resource = dynamodb_memory.create_new(
        MixedGSIResource,
        {
            "project_id": "test-proj",
            "status": "active",
            "priority": 1,
            "owner": "alice",
        },
    )

    # Get the raw DynamoDB item
    db_item = resource.to_dynamodb_item()

    # Build resource from item
    rebuilt = MixedGSIResource.from_dynamodb_item(db_item)

    # Verify GSI fields are not in the rebuilt resource's dict
    resource_dict = rebuilt.model_dump()
    assert "gsi1pk" not in resource_dict
    assert "gsi2pk" not in resource_dict
    assert "gsi3pk" not in resource_dict
    assert "gsi3sk" not in resource_dict

    # Verify actual fields are present
    assert rebuilt.project_id == "test-proj"
    assert rebuilt.status == "active"
    assert rebuilt.priority == 1
    assert rebuilt.owner == "alice"


def test_empty_tuple_gsi_config():
    """Test that empty tuple GSI config is handled gracefully."""

    class EmptyTupleResource(DynamoDbResource):
        name: str

        @classmethod
        def get_gsi_config(cls) -> dict:
            return {
                "gsi3": {
                    ("gsi3pk", "gsi3sk"): lambda self: None,  # Always returns None
                }
            }

    # Create a minimal resource with required fields
    from datetime import datetime

    resource = EmptyTupleResource(
        resource_id="test-id", created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc), name="test"
    )
    db_item = resource.to_dynamodb_item()

    # Should not have GSI fields since method returns None
    assert "gsi3pk" not in db_item
    assert "gsi3sk" not in db_item


def test_classvar_with_tuple_config(dynamodb_memory: DynamoDbMemory):
    """Test that tuple configuration works with ClassVar gsi_config."""

    class ClassVarTupleResource(DynamoDbResource):
        name: str
        score: int

        gsi_config: ClassVar[dict] = {
            "gsi3": {
                ("gsi3pk", "gsi3sk"): lambda self: (f"score#{self.score}", self.name),
            }
        }

    resource = dynamodb_memory.create_new(
        ClassVarTupleResource,
        {
            "name": "high-scorer",
            "score": 100,
        },
    )

    db_item = resource.to_dynamodb_item()
    assert db_item["gsi3pk"] == "score#100"
    assert db_item["gsi3sk"] == "high-scorer"
