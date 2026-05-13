"""Tests for ResourceConfig(omit_none_attributes=True) and the PaginatedList.pagination_key alias.

These cover the foot-gun documented in issue #1: by default, Pydantic ``Optional`` fields
set to ``None`` are written to DynamoDB as ``{"NULL": True}`` attributes, which causes
``attribute_not_exists(field)`` to return False after the first PUT. The opt-in
``omit_none_attributes=True`` restores the natural ``attribute_not_exists`` semantics.
"""

from typing import ClassVar, Optional

from boto3.dynamodb.conditions import Key

from simplesingletable import DynamoDbMemory, DynamoDbResource
from simplesingletable.models import ResourceConfig
from simplesingletable.transactions import TransactionError, VersionConflictError


class AssetDefault(DynamoDbResource):
    """Default behavior: None fields are written as NULL attributes."""

    asset_tag: str
    assigned_user_id: Optional[str] = None


class AssetOmitNone(DynamoDbResource):
    """With omit_none_attributes=True: None fields are stripped before write."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        omit_none_attributes=True,
    )

    asset_tag: str
    assigned_user_id: Optional[str] = None
    extras: Optional[dict] = None


def _raw_item(memory: DynamoDbMemory, resource) -> dict:
    """Return the raw DynamoDB item for a resource (bypassing the library's hydration)."""
    pk = f"{type(resource).get_unique_key_prefix()}#{resource.resource_id}"
    response = memory.dynamodb_table.get_item(Key={"pk": pk, "sk": pk})
    return response["Item"]


def test_default_writes_none_as_null_attribute(dynamodb_memory: DynamoDbMemory):
    """Without the flag, an Optional field set to None still becomes a NULL DDB attribute."""
    asset = dynamodb_memory.create_new(AssetDefault, {"asset_tag": "X"})

    item = _raw_item(dynamodb_memory, asset)
    assert "assigned_user_id" in item
    assert item["assigned_user_id"] is None


def test_omit_none_strips_none_from_item(dynamodb_memory: DynamoDbMemory):
    """With the flag, None-valued fields are absent from the DDB item entirely."""
    asset = dynamodb_memory.create_new(AssetOmitNone, {"asset_tag": "X"})

    item = _raw_item(dynamodb_memory, asset)
    assert "assigned_user_id" not in item
    assert "extras" not in item
    assert item["asset_tag"] == "X"


def test_omit_none_allows_attribute_not_exists_in_transaction(dynamodb_memory: DynamoDbMemory):
    """With the flag set, attribute_not_exists works on freshly created resources."""
    asset = dynamodb_memory.create_new(AssetOmitNone, {"asset_tag": "X"})

    # The canonical "claim this slot" pattern: succeed only if the slot is empty.
    with dynamodb_memory.transaction(auto_retry=False) as txn:
        txn.update(
            AssetOmitNone,
            resource_id=asset.resource_id,
            updates={"assigned_user_id": "alice"},
            condition="attribute_not_exists(assigned_user_id)",
        )

    updated = dynamodb_memory.get_existing(asset.resource_id, AssetOmitNone)
    assert updated.assigned_user_id == "alice"


def test_default_breaks_attribute_not_exists_in_transaction(dynamodb_memory: DynamoDbMemory):
    """Without the flag, attribute_not_exists fails because NULL attribute is "present".

    This is the foot-gun the omit_none_attributes flag exists to fix.
    """
    asset = dynamodb_memory.create_new(AssetDefault, {"asset_tag": "X"})

    raised = False
    try:
        with dynamodb_memory.transaction(auto_retry=False) as txn:
            txn.update(
                AssetDefault,
                resource_id=asset.resource_id,
                updates={"assigned_user_id": "alice"},
                condition="attribute_not_exists(assigned_user_id)",
            )
    except (VersionConflictError, TransactionError) as e:
        # A condition failure is the documented foot-gun. The library may surface this as
        # any of several exception types today (normalization is tracked in issue #2).
        raised = True
        assert "Condition" in str(e) or isinstance(e, VersionConflictError) or "Transaction" in str(e)
    assert raised, "Expected attribute_not_exists to fail because NULL is treated as present"


def test_omit_none_preserves_non_none_values(dynamodb_memory: DynamoDbMemory):
    """Setting an optional field to a real value still writes it normally."""
    asset = dynamodb_memory.create_new(
        AssetOmitNone, {"asset_tag": "X", "assigned_user_id": "alice", "extras": {"k": "v"}}
    )

    item = _raw_item(dynamodb_memory, asset)
    assert item["assigned_user_id"] == "alice"
    assert item["extras"] == {"k": "v"}


def test_omit_none_recurses_into_nested_dicts(dynamodb_memory: DynamoDbMemory):
    """None values inside nested dict attributes are also stripped."""
    asset = dynamodb_memory.create_new(
        AssetOmitNone, {"asset_tag": "X", "extras": {"present": 1, "missing": None}}
    )

    item = _raw_item(dynamodb_memory, asset)
    assert item["extras"] == {"present": 1}


def test_pagination_key_alias_returns_next_pagination_key(dynamodb_memory: DynamoDbMemory):
    """PaginatedList.pagination_key mirrors next_pagination_key."""
    # Create enough resources to require pagination.
    for i in range(5):
        dynamodb_memory.create_new(AssetDefault, {"asset_tag": f"asset-{i}"})

    page = dynamodb_memory.paginated_dynamodb_query(
        resource_class=AssetDefault,
        index_name="gsitype",
        key_condition=Key("gsitype").eq(AssetDefault.db_get_gsitypepk()),
        results_limit=2,
    )

    # The alias should always match the underlying attribute, whether or not pagination
    # actually rolled over.
    assert page.pagination_key == page.next_pagination_key
