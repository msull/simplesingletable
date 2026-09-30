"""Audit logging tests for LocalStorageMemory (exclude_fields redaction)."""

import json
import tempfile
from collections.abc import Iterator
from typing import Any, ClassVar, Optional

import pytest
from logzero import logger

from simplesingletable import DynamoDbResource, DynamoDbVersionedResource, LocalStorageMemory
from simplesingletable.models import AuditConfig, AuditLog, BlobFieldConfig, ResourceConfig


@pytest.fixture
def local_storage() -> Iterator[LocalStorageMemory]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield LocalStorageMemory(logger=logger, storage_dir=tmpdir, track_stats=True, use_blob_storage=True)


def _audit_logs_for(
    memory: LocalStorageMemory, resource: DynamoDbResource | DynamoDbVersionedResource
) -> list[AuditLog]:
    return [
        log
        for log in memory.list_type_by_updated_at(AuditLog)
        if log.audited_resource_type == type(resource).__name__ and log.audited_resource_id == resource.resource_id
    ]


def _logs_by_op(logs: list[AuditLog]) -> dict[str, list[AuditLog]]:
    by_op: dict[str, list[AuditLog]] = {}
    for log in logs:
        by_op.setdefault(log.operation, []).append(log)
    return by_op


def _assert_account_snapshot(log: AuditLog, email: str, display_name: str) -> None:
    snapshot: dict[str, Any] = log.resource_snapshot
    assert snapshot is not None
    assert "password_hash" not in snapshot
    assert snapshot["email"] == email
    assert snapshot["display_name"] == display_name
    assert "secret-" not in json.dumps(snapshot, default=str)


class LocalAuditedAccount(DynamoDbResource):
    """Non-versioned resource with a sensitive field excluded from audit."""

    email: str
    display_name: str
    password_hash: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            exclude_fields={"password_hash"},
        ),
    )


class LocalAuditedVersionedAccount(DynamoDbVersionedResource):
    """Versioned resource with a sensitive field excluded from audit."""

    email: str
    display_name: str
    password_hash: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            exclude_fields={"password_hash"},
        ),
    )


class LocalAuditedBaseKeysExcluded(DynamoDbVersionedResource):
    """Versioned resource that excludes every base key (plus a secret) from audit."""

    owner: str
    note: str
    secret: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            changed_by_field="owner",
            exclude_fields={"resource_id", "version", "created_at", "updated_at", "secret"},
        ),
    )


class LocalAuditedUnknownExclusion(DynamoDbResource):
    """exclude_fields names a field the model does not have."""

    name: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            exclude_fields={"no_such_field"},
        ),
    )


class LocalAuditedNoneExclusion(DynamoDbResource):
    name: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(enabled=True, include_snapshot=True, exclude_fields=None),
    )


class LocalAuditedEmptyExclusion(DynamoDbResource):
    name: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(enabled=True, include_snapshot=True, exclude_fields=set()),
    )


class LocalAuditedDocument(DynamoDbResource):
    """Excludes one blob field from audit while keeping the other."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
            exclude_fields={"attachment"},
        ),
        blob_fields={
            "content": BlobFieldConfig(compress=True, content_type="text/plain"),
            "attachment": BlobFieldConfig(compress=False, content_type="application/octet-stream"),
        },
    )

    title: str
    content: Optional[str] = None
    attachment: Optional[bytes] = None


def test_local_audit_exclude_fields_redacted_non_versioned(local_storage: LocalStorageMemory) -> None:
    account = local_storage.create_new(
        LocalAuditedAccount,
        {"email": "a@example.com", "display_name": "Alice", "password_hash": "secret-1"},
        changed_by="admin",
    )
    account = local_storage.update_existing(
        account, {"display_name": "Alicia", "password_hash": "secret-2"}, changed_by="admin"
    )
    local_storage.delete_existing(account, changed_by="admin")

    by_op = _logs_by_op(_audit_logs_for(local_storage, account))

    _assert_account_snapshot(by_op["CREATE"][0], "a@example.com", "Alice")
    _assert_account_snapshot(by_op["UPDATE"][0], "a@example.com", "Alicia")
    _assert_account_snapshot(by_op["DELETE"][0], "a@example.com", "Alicia")

    update_log = by_op["UPDATE"][0]
    assert "display_name" in update_log.changed_fields
    assert "password_hash" not in update_log.changed_fields


def test_local_audit_exclude_fields_redacted_versioned(local_storage: LocalStorageMemory) -> None:
    account = local_storage.create_new(
        LocalAuditedVersionedAccount,
        {"email": "v@example.com", "display_name": "Vera", "password_hash": "secret-1"},
        changed_by="admin",
    )
    local_storage.update_existing(
        account, {"display_name": "Veronica", "password_hash": "secret-2"}, changed_by="admin"
    )
    restored = local_storage.restore_version(account.resource_id, LocalAuditedVersionedAccount, 1, changed_by="admin")
    assert restored.password_hash == "secret-1"
    local_storage.delete_existing(restored, changed_by="admin")

    logs = _audit_logs_for(local_storage, account)
    by_op = _logs_by_op(logs)
    assert {op: len(rows) for op, rows in by_op.items()} == {"CREATE": 1, "UPDATE": 2, "DELETE": 1}

    for log in logs:
        assert "password_hash" not in log.resource_snapshot
        assert log.resource_snapshot["email"] == "v@example.com"
        assert "secret-" not in json.dumps(log.resource_snapshot, default=str)

    for update_log in by_op["UPDATE"]:
        assert "display_name" in update_log.changed_fields
        assert "password_hash" not in update_log.changed_fields


def test_local_audit_excluded_blob_field_dropped(local_storage: LocalStorageMemory) -> None:
    doc = local_storage.create_new(
        LocalAuditedDocument,
        {"title": "Doc", "content": "Content..." * 50, "attachment": b"bytes" * 20},
        changed_by="author@example.com",
    )

    create_log = _logs_by_op(_audit_logs_for(local_storage, doc))["CREATE"][0]

    assert "attachment" not in create_log.resource_snapshot
    assert create_log.resource_snapshot["content"]["__blob_ref__"] is True


def test_local_audit_exclude_base_keys_from_snapshot(local_storage: LocalStorageMemory) -> None:
    res = local_storage.create_new(LocalAuditedBaseKeysExcluded, {"owner": "alice", "note": "n1", "secret": "s1"})
    res = local_storage.update_existing(res, {"note": "n2"}, changed_by="bob")

    # Persistence is unaffected by exclude_fields
    stored = local_storage.read_existing(res.resource_id, LocalAuditedBaseKeysExcluded)
    assert stored.version == 2
    assert stored.note == "n2"
    assert stored.secret == "s1"
    assert stored.created_at is not None
    assert stored.updated_at is not None

    local_storage.delete_existing(res)

    logs = _audit_logs_for(local_storage, res)
    by_op = _logs_by_op(logs)
    assert {op: len(rows) for op, rows in by_op.items()} == {"CREATE": 1, "UPDATE": 1, "DELETE": 1}

    for log in logs:
        assert set(log.resource_snapshot) == {"owner", "note"}
        assert log.audited_resource_id == res.resource_id
        assert log.audited_resource_type == "LocalAuditedBaseKeysExcluded"
        assert log.created_at is not None

    # changed_by_field is not affected by exclude_fields
    assert by_op["CREATE"][0].changed_by == "alice"
    update_log = by_op["UPDATE"][0]
    assert update_log.changed_by == "bob"

    assert "note" in update_log.changed_fields
    for excluded in ("resource_id", "version", "created_at", "updated_at", "secret"):
        assert excluded not in update_log.changed_fields


def test_local_audit_exclude_unknown_field_is_ignored(local_storage: LocalStorageMemory) -> None:
    res = local_storage.create_new(LocalAuditedUnknownExclusion, {"name": "one"})
    local_storage.update_existing(res, {"name": "two"})

    by_op = _logs_by_op(_audit_logs_for(local_storage, res))

    create_snapshot = by_op["CREATE"][0].resource_snapshot
    assert {"name", "resource_id", "created_at", "updated_at"} <= set(create_snapshot)
    assert "name" in by_op["UPDATE"][0].changed_fields


@pytest.mark.parametrize("resource_class", [LocalAuditedNoneExclusion, LocalAuditedEmptyExclusion])
def test_local_audit_exclude_fields_none_or_empty_keeps_full_snapshot(
    local_storage: LocalStorageMemory, resource_class: type[DynamoDbResource]
) -> None:
    instance = local_storage.create_new(resource_class, {"name": "full"})

    by_op = _logs_by_op(_audit_logs_for(local_storage, instance))

    assert set(by_op["CREATE"][0].resource_snapshot) == set(instance.model_dump())
