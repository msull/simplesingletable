"""Tests for ``datetime.date`` field support on uncompressed resources.

Regression coverage for https://github.com/msull/simplesingletable/issues/5:
``clean_data`` previously only handled ``datetime.datetime``, so any
Pydantic field typed as plain ``datetime.date`` reached boto3 as a raw
``date`` and raised ``TypeError`` on save.
"""

from datetime import date, datetime
from typing import Optional

from simplesingletable.dynamodb_memory import DynamoDbMemory
from simplesingletable.models import (
    DynamoDbResource,
    DynamoDbVersionedResource,
    ResourceConfig,
    clean_data,
)


class UncompressedDateResource(DynamoDbResource):
    """Non-versioned, uncompressed resource with a plain ``date`` field."""

    name: str
    acquired: Optional[date] = None

    resource_config = ResourceConfig(compress_data=False)


class UncompressedDateVersionedResource(DynamoDbVersionedResource):
    """Versioned but uncompressed resource with a plain ``date`` field."""

    name: str
    acquired: Optional[date] = None

    resource_config = ResourceConfig(compress_data=False, max_versions=None)


class DateContainerResource(DynamoDbResource):
    """Non-versioned resource with date values nested in lists and dicts."""

    name: str
    important_dates: list[date]
    metadata: dict

    resource_config = ResourceConfig(compress_data=False)


def test_clean_data_serializes_date_fields():
    """``clean_data`` should ISO-serialize ``date`` values just like datetimes."""
    payload = {
        "acquired": date(2024, 6, 19),
        "noted_at": datetime(2024, 6, 19, 12, 30, 45),
        "untouched": "string",
    }
    cleaned = clean_data(payload)

    assert cleaned["acquired"] == "2024-06-19"
    assert cleaned["noted_at"] == "2024-06-19T12:30:45"
    assert cleaned["untouched"] == "string"


def test_clean_data_serializes_dates_in_nested_structures():
    """Dates nested in dicts and lists should also be serialized."""
    payload = {
        "nested": {"acquired": date(2024, 6, 19)},
        "history": [date(2024, 1, 1), date(2024, 12, 31)],
        "deeply_nested": [{"d": date(2024, 6, 19)}, [date(2023, 1, 1)]],
    }
    cleaned = clean_data(payload)

    assert cleaned["nested"]["acquired"] == "2024-06-19"
    assert cleaned["history"] == ["2024-01-01", "2024-12-31"]
    assert cleaned["deeply_nested"][0]["d"] == "2024-06-19"
    assert cleaned["deeply_nested"][1] == ["2023-01-01"]


def test_uncompressed_resource_with_date_field_round_trips(dynamodb_memory: DynamoDbMemory):
    """Saving and reading an uncompressed resource with a date field works."""
    resource = dynamodb_memory.create_new(
        UncompressedDateResource,
        {"name": "thing", "acquired": date(2024, 6, 19)},
    )

    fetched = dynamodb_memory.read_existing(resource.resource_id, UncompressedDateResource)
    assert fetched.acquired == date(2024, 6, 19)
    assert isinstance(fetched.acquired, date)


def test_uncompressed_versioned_resource_with_date_field_round_trips(
    dynamodb_memory: DynamoDbMemory,
):
    """Same round-trip behavior for an uncompressed versioned resource."""
    resource = dynamodb_memory.create_new(
        UncompressedDateVersionedResource,
        {"name": "thing", "acquired": date(2024, 6, 19)},
    )

    fetched = dynamodb_memory.read_existing(
        resource.resource_id, UncompressedDateVersionedResource
    )
    assert fetched.acquired == date(2024, 6, 19)


def test_date_values_in_lists_and_dicts_round_trip(dynamodb_memory: DynamoDbMemory):
    """Date values inside list/dict fields also need ISO conversion."""
    resource = dynamodb_memory.create_new(
        DateContainerResource,
        {
            "name": "thing",
            "important_dates": [date(2024, 1, 1), date(2024, 12, 31)],
            "metadata": {"reviewed_on": date(2024, 6, 19), "label": "ok"},
        },
    )

    fetched = dynamodb_memory.read_existing(resource.resource_id, DateContainerResource)
    assert fetched.important_dates == [date(2024, 1, 1), date(2024, 12, 31)]
    assert fetched.metadata == {"reviewed_on": "2024-06-19", "label": "ok"}
