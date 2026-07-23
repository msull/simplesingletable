"""Tests for issue #8 P4: automatic optimistic locking on txn.put.

A full-state put derived from a stale read is a full-state lost update.
``txn.put`` now guards on the resource's ``updated_at`` (captured at queue
time) by default; a concurrent writer causes TransactionConditionFailedError
instead of a silent overwrite. ``optimistic=False`` opts out.
"""

from typing import ClassVar

import pytest

from simplesingletable import DynamoDbMemory, DynamoDbResource
from simplesingletable.models import ResourceConfig
from simplesingletable.transactions import TransactionConditionFailedError


class Ticket(DynamoDbResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(omit_none_attributes=True)

    subject: str
    status: str = "open"


def test_stale_put_raises_instead_of_overwriting(dynamodb_memory: DynamoDbMemory):
    ticket = dynamodb_memory.create_new(Ticket, {"subject": "orig"})
    stale = ticket.model_copy(deep=True)

    # Concurrent writer bumps updated_at.
    dynamodb_memory.update_existing(ticket, {"subject": "concurrent-edit"})

    stale.subject = "my-edit"
    with pytest.raises(TransactionConditionFailedError):
        with dynamodb_memory.transaction() as txn:
            txn.put(stale)

    # The concurrent write survived.
    assert dynamodb_memory.get_existing(ticket.resource_id, Ticket).subject == "concurrent-edit"


def test_fresh_put_succeeds_and_object_stays_current(dynamodb_memory: DynamoDbMemory):
    ticket = dynamodb_memory.create_new(Ticket, {"subject": "orig"})

    ticket.subject = "edit-1"
    with dynamodb_memory.transaction() as txn:
        txn.put(ticket)

    # The builder synced updated_at onto the caller's object, so a second
    # optimistic put with the same object also succeeds.
    ticket.subject = "edit-2"
    with dynamodb_memory.transaction() as txn:
        txn.put(ticket)

    assert dynamodb_memory.get_existing(ticket.resource_id, Ticket).subject == "edit-2"


def test_optimistic_false_overwrites_stale_state(dynamodb_memory: DynamoDbMemory):
    ticket = dynamodb_memory.create_new(Ticket, {"subject": "orig"})
    stale = ticket.model_copy(deep=True)

    dynamodb_memory.update_existing(ticket, {"subject": "concurrent-edit"})

    stale.subject = "last-writer-wins"
    with dynamodb_memory.transaction() as txn:
        txn.put(stale, optimistic=False)

    assert dynamodb_memory.get_existing(ticket.resource_id, Ticket).subject == "last-writer-wins"


def test_optimistic_guard_combines_with_user_condition(dynamodb_memory: DynamoDbMemory):
    ticket = dynamodb_memory.create_new(Ticket, {"subject": "orig", "status": "open"})

    # User condition fails even though the optimistic token is fresh.
    ticket.subject = "should-not-write"
    with pytest.raises(TransactionConditionFailedError):
        with dynamodb_memory.transaction() as txn:
            txn.put(
                ticket,
                condition="#status = :expected",
                condition_values={":expected": "closed"},
                condition_names={"#status": "status"},
            )

    # Both the user condition and the optimistic token hold.
    ticket.subject = "written"
    with dynamodb_memory.transaction() as txn:
        txn.put(
            ticket,
            condition="#status = :expected",
            condition_values={":expected": "open"},
            condition_names={"#status": "status"},
        )

    assert dynamodb_memory.get_existing(ticket.resource_id, Ticket).subject == "written"


def test_stale_put_is_not_auto_retried(dynamodb_memory: DynamoDbMemory, mocker):
    """The optimistic guard must fail fast: re-sending the same stale full-state
    put can never succeed, so burning max_retries on it is pure latency."""
    ticket = dynamodb_memory.create_new(Ticket, {"subject": "orig"})
    stale = ticket.model_copy(deep=True)
    dynamodb_memory.update_existing(ticket, {"subject": "concurrent-edit"})

    spy = mocker.spy(dynamodb_memory.dynamodb_client, "transact_write_items")
    stale.subject = "my-edit"
    with pytest.raises(TransactionConditionFailedError):
        with dynamodb_memory.transaction() as txn:
            txn.put(stale)

    assert spy.call_count == 1
