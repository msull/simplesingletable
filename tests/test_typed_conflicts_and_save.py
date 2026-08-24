"""Typed conflict errors on the non-transactional write path, plus ``save()``.

Covers issue #13. Two related things:

1. Losing an optimistic-concurrency race raises a :class:`ConflictError` subclass on
   *every* write path -- transactional and not -- instead of a bare ``ValueError``
   that callers had to identify by matching message text.
2. ``ResourceRepository.save(id, obj, expected_version)`` collapses the
   create-or-update-from-a-known-version branch into one call with one failure type.

Every new type subclasses ``ValueError``, so the back-compat assertions here matter as
much as the new ones: existing handlers must keep catching what they caught before.
"""

import logging
import tempfile

import pytest
from pydantic import BaseModel

from simplesingletable import (
    ConflictError,
    DynamoDbResource,
    DynamoDbVersionedResource,
    LocalStorageMemory,
    ResourceExistsError,
    TransactionConditionFailedError,
    TransactionError,
    VersionConflictError,
)
from simplesingletable.extras.repository import ResourceRepository
from simplesingletable.extras.versioned_repository import VersionedResourceRepository


class Doc(DynamoDbVersionedResource):
    title: str
    content: str
    note: str | None = None


class Plain(DynamoDbResource):
    name: str


class CreateDoc(BaseModel):
    title: str
    content: str
    note: str | None = None


class UpdateDoc(BaseModel):
    title: str | None = None
    content: str | None = None
    note: str | None = None


class CreatePlain(BaseModel):
    name: str


class UpdatePlain(BaseModel):
    name: str | None = None


@pytest.fixture
def repo(dynamodb_memory):
    return VersionedResourceRepository(
        ddb=dynamodb_memory,
        model_class=Doc,
        create_schema_class=CreateDoc,
        update_schema_class=UpdateDoc,
    )


@pytest.fixture
def local_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield LocalStorageMemory(logger=logging.getLogger(__name__), storage_dir=tmpdir)


# ---------------------------------------------------------------------------
# The hierarchy itself
# ---------------------------------------------------------------------------


def test_version_conflict_is_both_a_transaction_error_and_a_value_error():
    """The one name callers reach for has to satisfy every pre-existing handler."""
    exc = VersionConflictError("boom")
    assert isinstance(exc, TransactionConditionFailedError)  # pre-existing base
    assert isinstance(exc, TransactionError)  # pre-existing base
    assert isinstance(exc, ConflictError)  # new cross-path base
    assert isinstance(exc, ValueError)  # what the untyped path raised


def test_resource_exists_is_a_conflict_but_not_a_transaction_error():
    """A taken id is a conflict; it is not a statement about transactions."""
    exc = ResourceExistsError("boom")
    assert isinstance(exc, ConflictError)
    assert isinstance(exc, ValueError)
    assert not isinstance(exc, TransactionError)


def test_transactions_module_still_exports_the_same_objects():
    """The types moved to exceptions.py; imports from their original home still work."""
    from simplesingletable import exceptions
    from simplesingletable import transactions as txn

    for name in (
        "TransactionError",
        "TransactionConditionFailedError",
        "VersionConflictError",
        "ResourceNotFoundError",
    ):
        assert getattr(txn, name) is getattr(exceptions, name), name


def test_conflict_carries_structured_context_instead_of_message_text():
    exc = VersionConflictError(
        "nope",
        resource_type="Doc",
        resource_id="abc",
        expected_version=2,
        actual_version=5,
    )
    assert (exc.resource_type, exc.resource_id) == ("Doc", "abc")
    assert (exc.expected_version, exc.actual_version) == (2, 5)
    assert exc.cancellation_reasons == []


# ---------------------------------------------------------------------------
# Non-transactional write path
# ---------------------------------------------------------------------------


def test_update_from_stale_pre_image_raises_version_conflict(dynamodb_memory):
    doc = dynamodb_memory.create_new(Doc, CreateDoc(title="t", content="v1"))
    dynamodb_memory.update_existing(doc, UpdateDoc(content="v2"))

    with pytest.raises(VersionConflictError) as exc_info:
        dynamodb_memory.update_existing(doc, UpdateDoc(content="v3"))

    exc = exc_info.value
    assert exc.expected_version == 1
    assert exc.actual_version == 2
    assert exc.resource_type == "Doc"
    assert exc.resource_id == doc.resource_id


def test_stale_update_message_and_value_error_handling_are_unchanged(dynamodb_memory):
    """A caller still on ``except ValueError`` and message matching keeps working."""
    doc = dynamodb_memory.create_new(Doc, CreateDoc(title="t", content="v1"))
    dynamodb_memory.update_existing(doc, UpdateDoc(content="v2"))

    with pytest.raises(ValueError, match="Cannot update from non-latest version"):
        dynamodb_memory.update_existing(doc, UpdateDoc(content="v3"))


def test_create_with_taken_id_raises_resource_exists(dynamodb_memory):
    dynamodb_memory.create_new(Doc, CreateDoc(title="t", content="c"), override_id="fixed-id")

    with pytest.raises(ResourceExistsError) as exc_info:
        dynamodb_memory.create_new(Doc, CreateDoc(title="t2", content="c2"), override_id="fixed-id")

    assert exc_info.value.resource_id == "fixed-id"
    assert exc_info.value.resource_type == "Doc"


def test_losing_the_write_race_itself_raises_version_conflict(dynamodb_memory):
    """The condition on v0 -- not the pre-read check -- is what actually guards the write.

    ``update_existing`` compares versions before writing, so reaching the transaction's
    condition means driving ``_update_existing_versioned`` directly with a stale
    ``previous_version``. That is the code path a real concurrent writer hits.
    """
    doc = dynamodb_memory.create_new(Doc, CreateDoc(title="t", content="v1"))
    next_version = doc.update_existing(UpdateDoc(content="v2"))

    with pytest.raises(VersionConflictError) as exc_info:
        dynamodb_memory._update_existing_versioned(next_version, previous_version=99)

    assert exc_info.value.cancellation_reasons, "the raw cancellation payload should be attached"


def test_transaction_condition_failure_is_now_catchable_as_a_conflict(dynamodb_memory):
    """One ``except ConflictError`` covers both write paths -- the point of the issue."""
    doc = dynamodb_memory.create_new(Doc, CreateDoc(title="t", content="v1"))
    dynamodb_memory.update_existing(doc, UpdateDoc(content="v2"))

    # Non-transactional path.
    with pytest.raises(ConflictError):
        dynamodb_memory.update_existing(doc, UpdateDoc(content="v3"))

    # Transactional path, same except clause. A user-supplied condition is used because
    # a failed *implicit* condition is retried automatically -- the retry re-reads and
    # succeeds, so it is not a conflict the caller ever sees.
    plain = dynamodb_memory.create_new(Plain, CreatePlain(name="x"))
    with pytest.raises(ConflictError):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                Plain,
                plain.resource_id,
                {"name": "y"},
                condition="#name = :expected",
                condition_names={"#name": "name"},
                condition_values={":expected": "not-what-is-stored"},
            )


def test_local_storage_raises_the_same_types_as_dynamodb(local_storage):
    """Backend parity: code written against one memory must port to the other."""
    doc = local_storage.create_new(Doc, CreateDoc(title="t", content="v1"), override_id="fixed-id")

    with pytest.raises(ResourceExistsError):
        local_storage.create_new(Doc, CreateDoc(title="t", content="c"), override_id="fixed-id")

    local_storage.update_existing(doc, UpdateDoc(content="v2"))
    with pytest.raises(VersionConflictError) as exc_info:
        local_storage.update_existing(doc, UpdateDoc(content="v3"))
    assert (exc_info.value.expected_version, exc_info.value.actual_version) == (1, 2)


# ---------------------------------------------------------------------------
# save()
# ---------------------------------------------------------------------------


def test_save_with_expected_version_zero_creates(repo):
    doc = repo.save("doc-1", {"title": "t", "content": "c"}, expected_version=0)
    assert doc.resource_id == "doc-1"
    assert doc.version == 1
    assert doc.content == "c"


def test_save_with_expected_version_zero_refuses_to_clobber(repo):
    repo.save("doc-1", {"title": "t", "content": "c"}, expected_version=0)

    with pytest.raises(ResourceExistsError):
        repo.save("doc-1", {"title": "other", "content": "other"}, expected_version=0)

    assert repo.read("doc-1").content == "c"


def test_save_from_the_current_version_updates(repo):
    doc = repo.save("doc-1", {"title": "t", "content": "v1"}, expected_version=0)
    updated = repo.save("doc-1", {"content": "v2"}, expected_version=doc.version)

    assert updated.version == 2
    assert updated.content == "v2"
    assert updated.title == "t", "an update schema leaves unset fields alone"


def test_save_from_a_stale_version_conflicts(repo):
    doc = repo.save("doc-1", {"title": "t", "content": "v1"}, expected_version=0)
    repo.save("doc-1", {"content": "v2"}, expected_version=doc.version)

    with pytest.raises(VersionConflictError) as exc_info:
        repo.save("doc-1", {"content": "v3"}, expected_version=doc.version)

    assert exc_info.value.expected_version == 1
    assert exc_info.value.actual_version == 2
    assert repo.read("doc-1").content == "v2", "the losing write must not land"


def test_save_of_a_missing_resource_conflicts_rather_than_creating(repo):
    """``expected_version>0`` means *it was there*; a deleted resource is a lost race."""
    with pytest.raises(VersionConflictError) as exc_info:
        repo.save("never-existed", {"content": "v2"}, expected_version=3)

    assert exc_info.value.expected_version == 3
    assert exc_info.value.actual_version is None
    assert repo.get("never-existed") is None


def test_save_validates_against_the_schema_matching_the_mode(repo):
    """Version 0 means create, so the create schema's required fields apply."""
    with pytest.raises(ValueError, match="title"):
        repo.save("doc-1", {"content": "only content"}, expected_version=0)

    # The same partial payload is fine as an update.
    repo.save("doc-1", {"title": "t", "content": "c"}, expected_version=0)
    assert repo.save("doc-1", {"content": "only content"}, expected_version=1).title == "t"


def test_save_supports_clear_fields(repo):
    doc = repo.save("doc-1", {"title": "t", "content": "c", "note": "n"}, expected_version=0)
    cleared = repo.save("doc-1", {}, expected_version=doc.version, clear_fields={"note"})
    assert cleared.note is None


def test_save_rejects_a_negative_expected_version(repo):
    with pytest.raises(ValueError, match="expected_version"):
        repo.save("doc-1", {"content": "c"}, expected_version=-1)


def test_save_requires_a_versioned_model(dynamodb_memory):
    plain_repo = ResourceRepository(
        ddb=dynamodb_memory,
        model_class=Plain,
        create_schema_class=CreatePlain,
        update_schema_class=UpdatePlain,
    )
    with pytest.raises(TypeError, match="versioned resource"):
        plain_repo.save("id-1", {"name": "x"}, expected_version=0)


def test_save_does_not_trust_a_cached_pre_image(dynamodb_memory):
    """A stale cache entry must not turn the version guard into a rubber stamp."""
    cached_repo = VersionedResourceRepository(
        ddb=dynamodb_memory,
        model_class=Doc,
        create_schema_class=CreateDoc,
        update_schema_class=UpdateDoc,
        cache_ttl_seconds=300,
    )
    doc = cached_repo.save("doc-1", {"title": "t", "content": "v1"}, expected_version=0)
    cached_repo.get("doc-1")  # warm the cache at version 1

    # Another writer moves the resource forward without going through this repository.
    dynamodb_memory.update_existing(doc, UpdateDoc(content="v2"))

    with pytest.raises(VersionConflictError) as exc_info:
        cached_repo.save("doc-1", {"content": "v3"}, expected_version=1)
    assert exc_info.value.actual_version == 2

    # And the conflict evicted the stale entry rather than leaving it to be re-served.
    assert cached_repo.get("doc-1").version == 2


def test_save_replaces_the_message_matching_workaround(repo):
    """The shape #10 asked for: no branch, no string matching, one except clause."""
    doc = repo.save("doc-1", {"title": "t", "content": "v1"}, expected_version=0)
    repo.save("doc-1", {"content": "v2"}, expected_version=doc.version)

    def write(expected_version: int) -> str:
        try:
            repo.save("doc-1", {"content": "v3"}, expected_version=expected_version)
        except ConflictError:
            return "409"
        else:
            return "200"

    assert write(doc.version) == "409"
    assert write(2) == "200"


# ---------------------------------------------------------------------------
# Wrong-argument-type errors (TRY004)
# ---------------------------------------------------------------------------


def test_passing_the_wrong_class_raises_type_error_not_value_error(dynamodb_memory):
    """A wrong class is a programming error, not bad input.

    These used to raise ``ValueError``, which an app mapping ``ValueError -> 400``
    reported to the operator as "bad request" -- the complaint behind #10. They are
    deliberately *not* ``ValueError`` any more, which is the breaking half of this
    change, so the negative assertion is the point.
    """
    plain = dynamodb_memory.create_new(Plain, CreatePlain(name="x"))

    with pytest.raises(TypeError, match="can only be used with versioned resources"):
        dynamodb_memory.get_all_versions(plain.resource_id, Plain)

    with pytest.raises(TypeError):
        dynamodb_memory.delete_all_versions(plain.resource_id, Plain)

    assert not issubclass(TypeError, ValueError), "the whole point is that ValueError no longer catches these"


def test_save_wrong_model_class_is_the_same_kind_of_error(dynamodb_memory):
    """``save()`` follows the same rule as the rest of the library."""
    plain_repo = ResourceRepository(
        ddb=dynamodb_memory,
        model_class=Plain,
        create_schema_class=CreatePlain,
        update_schema_class=UpdatePlain,
    )
    with pytest.raises(TypeError):
        plain_repo.save("id-1", {"name": "x"}, expected_version=0)
