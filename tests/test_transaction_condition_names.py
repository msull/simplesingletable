"""Tests for issue #8 P3: ``condition_names`` on transactional ops.

DynamoDB reserved words (``status``, ``name``, ``total``, ...) cannot appear
literally in a condition expression; they need a ``#alias`` mapped through
ExpressionAttributeNames. Previously there was no way to supply that mapping,
so conditions on reserved-word attributes were impossible on create/put/delete
and only worked on update by accident when the field was also being updated.
"""

from typing import ClassVar

import pytest

from simplesingletable import DynamoDbMemory, DynamoDbResource
from simplesingletable.models import ResourceConfig
from simplesingletable.transactions import TransactionConditionFailedError


class Order(DynamoDbResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(omit_none_attributes=True)

    # All three field names are DynamoDB reserved words.
    name: str
    status: str = "open"
    total: float = 0.0


def test_update_condition_on_reserved_word_not_in_updates(dynamodb_memory: DynamoDbMemory):
    order = dynamodb_memory.create_new(Order, {"name": "widget", "status": "open"})

    with dynamodb_memory.transaction() as txn:
        txn.update(
            order,
            updates={"name": "widget-2"},
            condition="#status = :expected",
            condition_values={":expected": "open"},
            condition_names={"#status": "status"},
        )

    assert dynamodb_memory.get_existing(order.resource_id, Order).name == "widget-2"

    # And the condition actually guards: a stale expectation fails.
    with pytest.raises(TransactionConditionFailedError):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                order,
                updates={"name": "widget-3"},
                condition="#status = :expected",
                condition_values={":expected": "closed"},
                condition_names={"#status": "status"},
            )


def test_delete_condition_on_reserved_word(dynamodb_memory: DynamoDbMemory):
    order = dynamodb_memory.create_new(Order, {"name": "widget", "total": 9.99})

    with pytest.raises(TransactionConditionFailedError):
        with dynamodb_memory.transaction() as txn:
            txn.delete(
                Order,
                resource_id=order.resource_id,
                condition="#total = :t",
                condition_names={"#total": "total"},
                **{":t": 1.23},
            )

    with dynamodb_memory.transaction() as txn:
        txn.delete(
            Order,
            resource_id=order.resource_id,
            condition="#total = :t",
            condition_names={"#total": "total"},
            **{":t": 9.99},
        )

    assert dynamodb_memory.get_existing(order.resource_id, Order) is None


def test_put_condition_on_reserved_word(dynamodb_memory: DynamoDbMemory):
    order = dynamodb_memory.create_new(Order, {"name": "widget", "status": "open"})

    updated = order.model_copy(deep=True)
    updated.status = "closed"

    with dynamodb_memory.transaction() as txn:
        txn.put(
            updated,
            condition="#status = :expected",
            condition_values={":expected": "open"},
            condition_names={"#status": "status"},
        )

    assert dynamodb_memory.get_existing(order.resource_id, Order).status == "closed"


def test_create_condition_with_reserved_word(dynamodb_memory: DynamoDbMemory):
    order = dynamodb_memory.create_new(Order, {"name": "widget", "status": "open"})

    # Overwrite-style create guarded on a reserved-word attribute.
    replacement = order.model_copy(deep=True)
    replacement.name = "widget-replaced"

    with dynamodb_memory.transaction() as txn:
        txn.create(
            replacement,
            condition="#status = :expected",
            condition_names={"#status": "status"},
            **{":expected": "open"},
        )

    assert dynamodb_memory.get_existing(order.resource_id, Order).name == "widget-replaced"


def test_condition_names_alias_must_start_with_hash(dynamodb_memory: DynamoDbMemory):
    order = dynamodb_memory.create_new(Order, {"name": "widget"})

    with pytest.raises(ValueError, match="must start with '#'"):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                order,
                updates={"name": "x"},
                condition="status = :s",
                condition_values={":s": "open"},
                condition_names={"status": "status"},
            )


def test_update_condition_alias_coexists_with_updated_field(dynamodb_memory: DynamoDbMemory):
    """Alias for a field that is ALSO being updated must not collide with the
    auto-allocated SET placeholder."""
    order = dynamodb_memory.create_new(Order, {"name": "widget", "status": "open"})

    with dynamodb_memory.transaction() as txn:
        txn.update(
            order,
            updates={"status": "closed"},
            condition="#status = :expected",
            condition_values={":expected": "open"},
            condition_names={"#status": "status"},
        )

    assert dynamodb_memory.get_existing(order.resource_id, Order).status == "closed"
