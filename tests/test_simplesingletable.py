from datetime import datetime, timedelta, timezone

import ulid
from boto3.dynamodb.conditions import Key
from pydantic import BaseModel

from simplesingletable import DynamoDbMemory, DynamoDbVersionedResource, DynamoDbResource
from simplesingletable.utils import generate_date_sortable_id


class MyNonversionedTestResource(DynamoDbResource):
    name: str

    def db_get_gsi1pk(self) -> str | None:
        return f"parent_id#{self.parent_id}"


class PydanticAttributeTest(BaseModel):
    attribute_name: str = "default_attribute_name"


class MyVersionedTestResource(DynamoDbVersionedResource):
    some_field: str
    bool_field: bool
    list_of_things: list[str | int | bool | float]
    parent_id: str
    inner_class: PydanticAttributeTest

    def db_get_gsi1pk(self) -> str | None:
        return f"parent_id#{self.parent_id}"


def test_date_id(mocker):
    # Mock datetime.utcnow to return a specific datetime
    mocked_time = datetime(2023, 10, 9, 12, 0, 0, tzinfo=timezone.utc)  # this date is just an example
    mocker.patch("simplesingletable.utils._now", return_value=mocked_time)
    result = generate_date_sortable_id()
    parsed_ulid = ulid.parse(result)

    assert parsed_ulid.timestamp().datetime == mocked_time

    # You can also modify the mocked time as needed in subsequent calls.


def test_dynamodb_memory__basic(dynamodb_memory: DynamoDbMemory):
    id_before_create = ulid.parse(generate_date_sortable_id())
    resource = dynamodb_memory.create_new(
        MyVersionedTestResource,
        {
            "parent_id": "parent1",
            "some_field": "test",
            "bool_field": True,
            # multiple types in the list
            "list_of_things": ["a", False, 1, 1.2],
            "inner_class": PydanticAttributeTest(),
        },
    )
    assert dynamodb_memory.read_existing(resource.resource_id, MyVersionedTestResource) == resource

    resource_ulid = resource.resource_id_as_ulid()
    assert id_before_create.timestamp() <= resource_ulid.timestamp()


def test_dynamodb_memory__queries(dynamodb_memory: DynamoDbMemory, mocker):
    """Somewhat comprehensive test suite that covers most of the basic
    functionality of create, retrieve, update, sorts, versioning, etc."""
    # use mocker to ensure the created objects appear at least 1 second apart, for sort testing purposes
    # create and retrieval of a resource
    first_mock_time = datetime(2023, 10, 9, 12, 0, 0)  # this is arbitrary
    times_increased = 0

    def _incr_time():
        nonlocal times_increased
        mocked_time = first_mock_time + timedelta(minutes=1 * times_increased)
        mocker.patch("simplesingletable.utils._now", return_value=mocked_time)
        times_increased += 1

    _incr_time()
    new_resource_1 = dynamodb_memory.create_new(
        MyVersionedTestResource,
        {
            "parent_id": "parent1",
            "some_field": "test",
            "bool_field": True,
            "list_of_things": [],
            "inner_class": PydanticAttributeTest(),
        },
    )
    assert dynamodb_memory.read_existing(new_resource_1.resource_id, MyVersionedTestResource) == new_resource_1

    # create a second resource with the same parent
    _incr_time()
    new_resource_2 = dynamodb_memory.create_new(
        MyVersionedTestResource,
        {
            "parent_id": "parent1",
            "some_field": "test",
            "bool_field": False,
            "list_of_things": ["adsf"],
            "inner_class": PydanticAttributeTest(),
        },
    )

    def _q(pid, limit=10, pagination_key=None, ascending=True):
        return dynamodb_memory.paginated_dynamodb_query(
            resource_class=MyVersionedTestResource,
            index_name="gsi1",
            key_condition=Key("gsi1pk").eq(f"parent_id#{pid}"),
            results_limit=limit,
            pagination_key=pagination_key,
            ascending=ascending,
        )

    # query by parent ID, confirm ascending appears to sort by created time
    assert _q(new_resource_1.parent_id, ascending=True) == [
        new_resource_1,
        new_resource_2,
    ]
    assert _q(new_resource_1.parent_id, ascending=False) == [
        new_resource_2,
        new_resource_1,
    ]

    # different parent id
    _incr_time()
    new_resource_3 = dynamodb_memory.create_new(
        MyVersionedTestResource,
        {
            "parent_id": "parent2",
            "some_field": "test",
            "bool_field": True,
            "list_of_things": [1, 2, 3],
            "inner_class": PydanticAttributeTest(),
        },
    )

    # first parent id unchanged
    assert _q(new_resource_1.parent_id) == [new_resource_1, new_resource_2]

    assert _q(new_resource_3.parent_id) == [new_resource_3]

    # get all three by type
    by_type = dynamodb_memory.list_type_by_updated_at(MyVersionedTestResource, ascending=False)
    assert by_type == [new_resource_3, new_resource_2, new_resource_1]
    by_type_asc = dynamodb_memory.list_type_by_updated_at(MyVersionedTestResource, ascending=True)
    assert by_type_asc == [new_resource_1, new_resource_2, new_resource_3]

    # update resource 2 and re-check order
    assert new_resource_2.bool_field is False
    _incr_time()
    updated_resource2 = dynamodb_memory.update_existing(new_resource_2, {"bool_field": True})
    assert updated_resource2.bool_field is True
    assert updated_resource2.resource_id == new_resource_2.resource_id
    assert updated_resource2.version == 2
    assert updated_resource2.created_at == new_resource_2.created_at
    assert updated_resource2.updated_at > new_resource_2.updated_at

    # get all three by type again
    by_type = dynamodb_memory.list_type_by_updated_at(MyVersionedTestResource, ascending=False)
    assert by_type == [updated_resource2, new_resource_3, new_resource_1]
    by_type_asc = dynamodb_memory.list_type_by_updated_at(MyVersionedTestResource, ascending=True)
    assert by_type_asc == [new_resource_1, new_resource_3, updated_resource2]

    # read with version identifier
    assert (
        dynamodb_memory.read_existing(new_resource_2.resource_id, MyVersionedTestResource, version=0)
        == updated_resource2
    )
    assert (
        dynamodb_memory.read_existing(new_resource_2.resource_id, MyVersionedTestResource, version=2)
        == updated_resource2
    )
    assert (
        dynamodb_memory.read_existing(new_resource_2.resource_id, MyVersionedTestResource, version=1) == new_resource_2
    )


def test_max_api_calls(dynamodb_memory: DynamoDbMemory, mocker):
    """Ensure that max api calls is respected and calculated correctly,
    particularly when using a server side filter function.

    Also tests that things like filter_limit_multiplier are working properly.
    """
    # use mocker to ensure the created objects appear at least 1 second apart, for sort testing purposes
    first_mock_time = datetime(2023, 10, 9, 12, 0, 0)  # this is arbitrary
    times_increased = 0

    def _incr_time():
        nonlocal times_increased
        mocked_time = first_mock_time + timedelta(seconds=1 * times_increased)
        mocker.patch("simplesingletable.utils._now", return_value=mocked_time)
        times_increased += 1

    # create ten resource with bool_field True
    for _ in range(10):
        _incr_time()
        dynamodb_memory.create_new(
            MyVersionedTestResource,
            {
                "parent_id": "parent1",
                "some_field": "test",
                "bool_field": True,
                "list_of_things": [],
                "inner_class": PydanticAttributeTest(),
            },
        )

    # one false after
    _incr_time()
    match_item = dynamodb_memory.create_new(
        MyVersionedTestResource,
        {
            "parent_id": "parent1",
            "some_field": "test",
            "bool_field": False,
            "list_of_things": [],
            "inner_class": PydanticAttributeTest(),
        },
    )

    # use a server side filter (rather than a dynamodb filter), which operates on
    # the decoded dynamodb object wherever the code is running
    def _filter(x: MyVersionedTestResource) -> bool:
        # find the one that after the ten
        return not x.bool_field

    def _q(max_api, multiplier=1, pagination_key=None):
        """Query for the single False item"""
        return dynamodb_memory.list_type_by_updated_at(
            MyVersionedTestResource,
            max_api_calls=max_api,
            results_limit=1,
            filter_fn=_filter,
            pagination_key=pagination_key,
            ascending=True,
            filter_limit_multiplier=multiplier,
        )

    res = _q(max_api=1)
    assert not res
    assert res.next_pagination_key

    res = _q(max_api=10)
    assert res.next_pagination_key
    assert not res

    res = _q(max_api=11)
    assert res == [match_item]
    assert res.next_pagination_key

    # with a higher multiplier, we can get it in fewer api calls
    res = _q(max_api=3, multiplier=3)
    assert res == []
    res = _q(max_api=4, multiplier=3)
    assert res == [match_item]
    res = _q(max_api=1, multiplier=25)
    assert res == [match_item]
