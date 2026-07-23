"""Tests for issue #8 P1: transactional expression values must get the same
float→Decimal / datetime→isoformat normalization (``clean_data``) as the
non-transactional write path, instead of crashing in boto3's TypeSerializer.

Covers:
- txn.update with float / datetime / nested values
- condition_values containing float / datetime (update, delete)
- txn.append with float list values
- txn.increment against a float-typed (Decimal-stored) field
- empty sets rejected with a clear client-side error
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import ClassVar, List, Optional

import pytest

from simplesingletable import DynamoDbMemory, DynamoDbResource
from simplesingletable.models import ResourceConfig
from simplesingletable.transactions import TransactionConditionFailedError


class Invoice(DynamoDbResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(omit_none_attributes=True)

    customer: str
    grand_total: float = 0.0
    line_amounts: List[float] = []
    due_at: Optional[datetime] = None
    details: Optional[dict] = None
    payment_count: int = 0


def _raw_item(memory: DynamoDbMemory, resource) -> dict:
    pk = f"{type(resource).get_unique_key_prefix()}#{resource.resource_id}"
    return memory.dynamodb_table.get_item(Key={"pk": pk, "sk": pk})["Item"]


def test_update_with_float_and_datetime_values(dynamodb_memory: DynamoDbMemory):
    invoice = dynamodb_memory.create_new(Invoice, {"customer": "acme"})
    due = datetime(2026, 8, 1, 12, 30, tzinfo=timezone.utc)

    with dynamodb_memory.transaction() as txn:
        txn.update(invoice, updates={"grand_total": 123.45, "due_at": due})

    stored = _raw_item(dynamodb_memory, invoice)
    assert stored["grand_total"] == Decimal("123.45")
    assert stored["due_at"] == due.isoformat()

    reloaded = dynamodb_memory.get_existing(invoice.resource_id, Invoice)
    assert reloaded.grand_total == 123.45
    assert reloaded.due_at == due


def test_update_with_nested_float_values(dynamodb_memory: DynamoDbMemory):
    invoice = dynamodb_memory.create_new(Invoice, {"customer": "acme"})

    with dynamodb_memory.transaction() as txn:
        txn.update(invoice, updates={"details": {"tax": 0.0825, "tiers": [1.5, 2.5]}})

    stored = _raw_item(dynamodb_memory, invoice)
    assert stored["details"]["tax"] == Decimal("0.0825")
    assert stored["details"]["tiers"] == [Decimal("1.5"), Decimal("2.5")]


def test_condition_values_with_datetime_optimistic_lock(dynamodb_memory: DynamoDbMemory):
    invoice = dynamodb_memory.create_new(Invoice, {"customer": "acme"})

    # Raw datetime in condition_values matches the stored isoformat string.
    with dynamodb_memory.transaction() as txn:
        txn.update(
            invoice,
            updates={"customer": "acme-2"},
            condition="updated_at = :expected",
            condition_values={":expected": invoice.updated_at},
        )
    assert dynamodb_memory.get_existing(invoice.resource_id, Invoice).customer == "acme-2"

    # Stale datetime fails the condition rather than crashing the serializer.
    with pytest.raises(TransactionConditionFailedError):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                invoice,
                updates={"customer": "acme-3"},
                condition="updated_at = :expected",
                condition_values={":expected": invoice.updated_at},
            )


def test_delete_with_float_condition_value(dynamodb_memory: DynamoDbMemory):
    invoice = dynamodb_memory.create_new(Invoice, {"customer": "acme", "grand_total": 9.99})

    with dynamodb_memory.transaction() as txn:
        txn.delete(Invoice, resource_id=invoice.resource_id, condition="grand_total = :t", **{":t": 9.99})

    assert dynamodb_memory.get_existing(invoice.resource_id, Invoice) is None


def test_append_float_values(dynamodb_memory: DynamoDbMemory):
    invoice = dynamodb_memory.create_new(Invoice, {"customer": "acme", "line_amounts": [1.5]})

    with dynamodb_memory.transaction() as txn:
        txn.append(invoice, "line_amounts", [2.5, 3.75])

    reloaded = dynamodb_memory.get_existing(invoice.resource_id, Invoice)
    assert reloaded.line_amounts == [1.5, 2.5, 3.75]


def test_increment_on_number_field(dynamodb_memory: DynamoDbMemory):
    invoice = dynamodb_memory.create_new(Invoice, {"customer": "acme", "grand_total": 1.5})

    # DynamoDB forbids multiple transaction ops on the same item, so two commits.
    with dynamodb_memory.transaction() as txn:
        txn.increment(invoice, "payment_count")
    with dynamodb_memory.transaction() as txn:
        txn.increment(invoice, "grand_total", amount=2)

    reloaded = dynamodb_memory.get_existing(invoice.resource_id, Invoice)
    assert reloaded.payment_count == 1
    assert reloaded.grand_total == 3.5


def test_empty_set_value_raises_clear_error(dynamodb_memory: DynamoDbMemory):
    invoice = dynamodb_memory.create_new(Invoice, {"customer": "acme"})

    with pytest.raises(ValueError, match="Empty set"):
        with dynamodb_memory.transaction() as txn:
            txn.update(invoice, updates={"details": set()})
