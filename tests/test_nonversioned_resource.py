import ulid

from simplesingletable import DynamoDbMemory, DynamoDbResource
from simplesingletable.models import ResourceConfig
from simplesingletable.utils import generate_date_sortable_id


class MyTestResource(DynamoDbResource):
    name: str
    group_members: list[str]

    resource_config = ResourceConfig(compress_data=True)


def test_dynamodb_memory__basic(dynamodb_memory: DynamoDbMemory):
    id_before_create = ulid.parse(generate_date_sortable_id())
    resource = dynamodb_memory.create_new(
        MyTestResource,
        {"name": "test1", "group_members": []},
    )
    assert dynamodb_memory.read_existing(resource.resource_id, MyTestResource) == resource
    # assert dynamodb_memory.read_existing(resource.resource_id, MyTestResource) == resource

    resource_ulid = resource.resource_id_as_ulid()
    assert id_before_create.timestamp() <= resource_ulid.timestamp()

    assert dynamodb_memory.list_type_by_updated_at(MyTestResource) == [resource]
    dynamodb_memory.delete_existing(resource)
    assert dynamodb_memory.list_type_by_updated_at(MyTestResource) == []
