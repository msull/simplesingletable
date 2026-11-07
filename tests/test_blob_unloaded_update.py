"""A non-versioned resource read without its blobs, then written back, keeps its stored blobs listed.

The item's ``_blob_fields`` list is what a later read loads blobs from, so a write that leaves a field off it makes
the blob unreachable even though it is still stored.
"""

import tempfile

import pytest
from logzero import logger

from simplesingletable import DynamoDbMemory, DynamoDbResource, LocalStorageMemory
from simplesingletable.models import BlobFieldConfig, ResourceConfig


class Note(DynamoDbResource):
    title: str
    body: dict | None = None
    attachment: str | None = None

    resource_config = ResourceConfig(
        blob_fields={"body": BlobFieldConfig(compress=False), "attachment": BlobFieldConfig(compress=True)}
    )


BODY = {"text": "kept"}
ATTACHMENT = "attached text"


@pytest.fixture
def local_storage_memory():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield LocalStorageMemory(logger=logger, storage_dir=tmpdir, use_blob_storage=True)


@pytest.fixture(params=["dynamodb", "local"])
def memory(request):
    if request.param == "dynamodb":
        return request.getfixturevalue("dynamodb_memory_with_s3")
    return request.getfixturevalue("local_storage_memory")


def _read(memory, resource_id, load_blobs):
    if isinstance(memory, DynamoDbMemory):
        return memory.get_existing(resource_id, Note, load_blobs=load_blobs)
    return memory.read_existing(resource_id, Note, load_blobs=load_blobs)


def _create(memory):
    return memory.create_new(Note, {"title": "a", "body": BODY, "attachment": ATTACHMENT})


def test_updating_a_resource_read_without_blobs_keeps_them(memory):
    note = _create(memory)

    loaded = _read(memory, note.resource_id, load_blobs=False)
    memory.update_existing(loaded, {"title": "b"})

    reread = _read(memory, note.resource_id, load_blobs=True)
    assert reread.title == "b"
    assert reread.body == BODY
    assert reread.attachment == ATTACHMENT


def test_the_returned_resource_can_load_its_blobs(memory):
    note = _create(memory)

    loaded = _read(memory, note.resource_id, load_blobs=False)
    updated = memory.update_existing(loaded, {"title": "b"})

    assert sorted(updated.get_unloaded_blob_fields()) == ["attachment", "body"]
    updated.load_blob_fields(memory)
    assert updated.body == BODY
    assert updated.attachment == ATTACHMENT


def test_updating_the_returned_resource_again_keeps_its_blobs(memory):
    note = _create(memory)

    loaded = _read(memory, note.resource_id, load_blobs=False)
    updated = memory.update_existing(loaded, {"title": "b"})
    memory.update_existing(updated, {"title": "c"})

    reread = _read(memory, note.resource_id, load_blobs=True)
    assert reread.title == "c"
    assert reread.body == BODY
    assert reread.attachment == ATTACHMENT


def test_a_blob_set_by_the_update_is_written_and_the_other_kept(memory):
    note = _create(memory)

    loaded = _read(memory, note.resource_id, load_blobs=False)
    updated = memory.update_existing(loaded, {"body": {"text": "new"}})

    assert updated.get_unloaded_blob_fields() == ["attachment"]
    reread = _read(memory, note.resource_id, load_blobs=True)
    assert reread.body == {"text": "new"}
    assert reread.attachment == ATTACHMENT


def test_a_cleared_blob_is_no_longer_listed(memory):
    note = _create(memory)

    loaded = _read(memory, note.resource_id, load_blobs=False)
    updated = memory.update_existing(loaded, {"title": "b"}, clear_fields={"body"})

    assert updated.get_unloaded_blob_fields() == ["attachment"]
    reread = _read(memory, note.resource_id, load_blobs=True)
    assert reread.body is None
    assert reread.attachment == ATTACHMENT


def test_a_transaction_put_of_a_resource_read_without_blobs_keeps_them(dynamodb_memory_with_s3):
    memory = dynamodb_memory_with_s3
    note = _create(memory)

    loaded = memory.get_existing(note.resource_id, Note, load_blobs=False)
    loaded.title = "b"
    with memory.transaction() as txn:
        txn.put(loaded, optimistic=False)

    reread = memory.get_existing(note.resource_id, Note, load_blobs=True)
    assert reread.title == "b"
    assert reread.body == BODY
    assert reread.attachment == ATTACHMENT
