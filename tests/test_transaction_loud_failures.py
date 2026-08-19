"""Tests for issue #12: three silent failures in the transaction path now raise.

Each guard fires at queue time, before anything is sent to DynamoDB, so a caller
learns about the limitation at the call site rather than from an AttributeError
inside marshalling, a guard that quietly did nothing, or a commit that could never
have succeeded.

The negative cases matter as much as the positive ones: none of these guards may
reject a write that works today.
"""

from typing import ClassVar, List, Optional

import pytest

from simplesingletable import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource
from simplesingletable.models import ResourceConfig
from simplesingletable.utils import marshall


class Report(DynamoDbResource):
    """Non-versioned resource with a blob field."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        blob_fields={"payload": {"content_type": "application/pdf"}}
    )

    title: str
    payload: Optional[bytes] = None


class VersionedReport(DynamoDbVersionedResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False,
        blob_fields={"payload": {"content_type": "application/pdf"}},
    )

    title: str
    payload: Optional[bytes] = None


class Plain(DynamoDbResource):
    """No blob fields, uncompressed — the unaffected baseline."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)

    name: str
    tags: List[str] = []


class Compressed(DynamoDbResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=True)

    name: str


class VersionedPlain(DynamoDbVersionedResource):
    resource_config: ClassVar[ResourceConfig] = ResourceConfig(compress_data=False)

    name: str


# ---------------------------------------------------------------------------
# The underlying defect each guard stands in front of
# ---------------------------------------------------------------------------


def test_blob_bearing_item_still_marshals_as_a_tuple():
    """The root cause: to_dynamodb_item returns a tuple, marshall wants a dict."""
    report = Report.create_new({"title": "t", "payload": b"pdf-bytes"})
    item = report.to_dynamodb_item()

    assert isinstance(item, tuple)
    with pytest.raises(AttributeError):
        marshall(item)


def test_compressed_resource_has_no_addressable_updated_at():
    """The root cause for the optimistic guard: updated_at is inside `data`."""
    item = Compressed.create_new({"name": "n"}).to_dynamodb_item()

    assert "updated_at" not in item
    assert "data" in item
    # Uncompressed, for contrast.
    assert "updated_at" in Plain.create_new({"name": "n"}).to_dynamodb_item()


# ---------------------------------------------------------------------------
# has_pending_blob_data — the predicate the blob guard keys on
# ---------------------------------------------------------------------------


def test_has_pending_blob_data_tracks_the_tuple_return():
    with_data = Report.create_new({"title": "t", "payload": b"bytes"})
    without_data = Report.create_new({"title": "t"})

    assert with_data.has_pending_blob_data() is True
    assert isinstance(with_data.to_dynamodb_item(), tuple)

    assert without_data.has_pending_blob_data() is False
    assert isinstance(without_data.to_dynamodb_item(), dict)

    # A resource with no blob config never reports pending data.
    assert Plain.create_new({"name": "n"}).has_pending_blob_data() is False


def test_blob_field_names():
    assert Report.blob_field_names() == {"payload"}
    assert Plain.blob_field_names() == set()


# ---------------------------------------------------------------------------
# Guard 1: blob-bearing writes are refused
# ---------------------------------------------------------------------------


def test_create_with_blob_data_raises(dynamodb_memory: DynamoDbMemory):
    report = Report.create_new({"title": "t", "payload": b"pdf-bytes"})

    with pytest.raises(ValueError, match=r"txn\.create\(\) cannot write blob field\(s\) \['payload'\]"):
        with dynamodb_memory.transaction() as txn:
            txn.create(report)


def test_put_with_blob_data_raises(dynamodb_memory: DynamoDbMemory):
    report = Report.create_new({"title": "t", "payload": b"pdf-bytes"})

    with pytest.raises(ValueError, match=r"txn\.put\(\) cannot write blob field\(s\)"):
        with dynamodb_memory.transaction() as txn:
            txn.put(report, optimistic=False)


def test_update_naming_a_blob_field_raises(dynamodb_memory: DynamoDbMemory):
    """This one never crashed — it silently wrote bytes inline, bypassing S3."""
    with pytest.raises(ValueError, match=r"txn\.update\(\) cannot modify blob field\(s\) \['payload'\]"):
        with dynamodb_memory.transaction() as txn:
            txn.update(Report, resource_id="abc", payload=b"pdf-bytes")


def test_update_clearing_a_blob_field_raises(dynamodb_memory: DynamoDbMemory):
    with pytest.raises(ValueError, match=r"cannot modify blob field\(s\) \['payload'\]"):
        with dynamodb_memory.transaction() as txn:
            txn.update(Report, resource_id="abc", clear_fields=["payload"])


def test_versioned_create_with_blob_data_raises(dynamodb_memory: DynamoDbMemory):
    report = VersionedReport.create_new({"title": "t", "payload": b"pdf-bytes"})

    with pytest.raises(ValueError, match=r"txn\.create\(\) cannot write blob field"):
        with dynamodb_memory.transaction() as txn:
            txn.create(report)


def test_blob_error_message_explains_the_limitation(dynamodb_memory: DynamoDbMemory):
    report = Report.create_new({"title": "t", "payload": b"pdf-bytes"})

    with pytest.raises(ValueError) as exc_info:
        with dynamodb_memory.transaction() as txn:
            txn.create(report)

    message = str(exc_info.value)
    assert "S3 object" in message
    assert "atomic commit" in message


def test_blob_configured_resource_without_data_is_allowed(dynamodb_memory: DynamoDbMemory):
    """The guard must not reject what works today.

    A blob-configured resource whose blob fields are unset marshals as a plain item —
    and that is exactly what reading one back from DynamoDB produces, so rejecting it
    would make blob resources untouchable in transactions.
    """
    report = Report.create_new({"title": "no-payload"})

    with dynamodb_memory.transaction() as txn:
        txn.create(report)

    stored = dynamodb_memory.get_existing(report.resource_id, Report)
    assert stored.title == "no-payload"
    assert stored.payload is None


def test_update_of_nonblob_field_on_blob_resource_is_allowed(dynamodb_memory: DynamoDbMemory):
    report = Report.create_new({"title": "before"})
    with dynamodb_memory.transaction() as txn:
        txn.create(report)

    with dynamodb_memory.transaction() as txn:
        txn.update(Report, resource_id=report.resource_id, title="after")

    assert dynamodb_memory.get_existing(report.resource_id, Report).title == "after"


def test_roundtripped_blob_resource_can_be_put(dynamodb_memory_with_s3: DynamoDbMemory):
    """A resource actually persisted with a blob reads back with the field as None,
    so it stays usable in a transaction."""
    report = dynamodb_memory_with_s3.create_new(Report, {"title": "t", "payload": b"pdf-bytes"})
    loaded = dynamodb_memory_with_s3.get_existing(report.resource_id, Report)

    assert loaded.payload is None
    assert loaded.has_pending_blob_data() is False

    loaded.title = "renamed"
    with dynamodb_memory_with_s3.transaction() as txn:
        txn.put(loaded, optimistic=False)

    assert dynamodb_memory_with_s3.get_existing(report.resource_id, Report).title == "renamed"


# ---------------------------------------------------------------------------
# Guard 2: caller conditions on versioned updates are refused
# ---------------------------------------------------------------------------


def test_versioned_update_with_condition_raises(dynamodb_memory: DynamoDbMemory):
    with pytest.raises(ValueError, match="does not support caller-supplied conditions on versioned"):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                VersionedPlain,
                resource_id="abc",
                updates={"name": "x"},
                condition="attribute_exists(pk)",
            )


def test_versioned_update_with_condition_values_raises(dynamodb_memory: DynamoDbMemory):
    """condition_values alone must raise too — otherwise the guard is bypassable."""
    with pytest.raises(ValueError, match="caller-supplied conditions"):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                VersionedPlain,
                resource_id="abc",
                updates={"name": "x"},
                condition_values={":v": 1},
            )


def test_versioned_update_with_condition_names_raises(dynamodb_memory: DynamoDbMemory):
    with pytest.raises(ValueError, match="caller-supplied conditions"):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                VersionedPlain,
                resource_id="abc",
                updates={"name": "x"},
                condition_names={"#s": "status"},
            )


def test_versioned_update_without_condition_still_works(dynamodb_memory: DynamoDbMemory):
    resource = dynamodb_memory.create_new(VersionedPlain, {"name": "before"})

    with dynamodb_memory.transaction() as txn:
        txn.update(VersionedPlain, resource_id=resource.resource_id, name="after")

    updated = dynamodb_memory.get_existing(resource.resource_id, VersionedPlain)
    assert updated.name == "after"
    assert updated.version == 2


def test_nonversioned_update_with_condition_still_works(dynamodb_memory: DynamoDbMemory):
    """The non-versioned branch does apply caller conditions and must keep doing so."""
    resource = dynamodb_memory.create_new(Plain, {"name": "before"})

    with dynamodb_memory.transaction() as txn:
        txn.update(
            Plain,
            resource_id=resource.resource_id,
            updates={"name": "after"},
            condition="attribute_exists(pk)",
        )

    assert dynamodb_memory.get_existing(resource.resource_id, Plain).name == "after"


# ---------------------------------------------------------------------------
# Guard 3: compressed + optimistic is refused
# ---------------------------------------------------------------------------


def test_compressed_optimistic_put_raises(dynamodb_memory: DynamoDbMemory):
    resource = dynamodb_memory.create_new(Compressed, {"name": "n"})

    with pytest.raises(ValueError, match="optimistic=True is not supported"):
        with dynamodb_memory.transaction() as txn:
            txn.put(resource)


def test_compressed_optimistic_put_raises_by_default(dynamodb_memory: DynamoDbMemory):
    """optimistic defaults to True, so the bare call must raise as well."""
    resource = dynamodb_memory.create_new(Compressed, {"name": "n"})

    with pytest.raises(ValueError) as exc_info:
        with dynamodb_memory.transaction() as txn:
            txn.put(resource)

    assert "compress_data" in str(exc_info.value)


def test_compressed_put_with_optimistic_false_works(dynamodb_memory: DynamoDbMemory):
    """The documented escape hatch must actually work."""
    resource = dynamodb_memory.create_new(Compressed, {"name": "before"})
    resource.name = "after"

    with dynamodb_memory.transaction() as txn:
        txn.put(resource, optimistic=False)

    assert dynamodb_memory.get_existing(resource.resource_id, Compressed).name == "after"


def test_uncompressed_optimistic_put_still_works(dynamodb_memory: DynamoDbMemory):
    resource = dynamodb_memory.create_new(Plain, {"name": "before"})
    resource.name = "after"

    with dynamodb_memory.transaction() as txn:
        txn.put(resource, optimistic=True)

    assert dynamodb_memory.get_existing(resource.resource_id, Plain).name == "after"


def test_guards_fire_before_anything_is_sent(dynamodb_memory: DynamoDbMemory):
    """A rejected op must not leave a partially-built transaction behind."""
    good = Plain.create_new({"name": "good"})

    with pytest.raises(ValueError):
        with dynamodb_memory.transaction() as txn:
            txn.create(good)
            txn.create(Report.create_new({"title": "t", "payload": b"bytes"}))

    # The transaction body raised before commit, so nothing was written.
    assert dynamodb_memory.get_existing(good.resource_id, Plain) is None
