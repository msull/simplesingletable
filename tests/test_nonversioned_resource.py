import ulid

from simplesingletable import DynamoDBMemory, DynamodbResource
from simplesingletable.utils import generate_date_sortable_id


class MyTestResource(DynamodbResource):
    name: str
    group_members: list[str]


def test_dynamodb_memory__basic(dynamodb_memory: DynamoDBMemory):
    id_before_create = ulid.parse(generate_date_sortable_id())
    resource = dynamodb_memory.create_new(
        MyTestResource,
        {"name": "test1", "group_members": []},
    )
    assert dynamodb_memory.read_existing(resource.resource_id, MyTestResource) == resource
    # assert dynamodb_memory.read_existing(resource.resource_id, MyTestResource) == resource

    resource_ulid = resource.resource_id_as_ulid()
    assert id_before_create.timestamp() <= resource_ulid.timestamp()
