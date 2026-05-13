"""Tests for the issue #2 transaction overhaul.

Covers:
- A1: commit() emits audit logs
- A2: recompute_gsis flag on txn.update
- A3: clear_fields kwarg on txn.update (REMOVE expression)
- A5: condition-aware auto_retry (user conditions never retry)
- A6: TransactionConditionFailedError normalization
- A7: commit() increments MemoryStats
- A8: current= short-circuits the versioned-update read
- A9: txn.put(full_resource) full-state operation
- B3: public emit_audit_log / emit_audit_logs API
"""

from datetime import datetime, timezone
from typing import ClassVar, Optional

import pytest

from simplesingletable import (
    AuditEntry,
    DynamoDbMemory,
    DynamoDbResource,
    DynamoDbVersionedResource,
)
from simplesingletable.models import AuditConfig, AuditLog, ResourceConfig
from simplesingletable.transactions import (
    TransactionConditionFailedError,
    TransactionError,
    VersionConflictError,
)


# --- Resources used across the test cases -----------------------------------------------


class AuditedUser(DynamoDbResource):
    """Non-versioned resource with audit logging fully enabled."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        omit_none_attributes=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
    )

    name: str
    email: str
    role: Optional[str] = None


class Asset(DynamoDbResource):
    """Resource that uses a sparse GSI keyed off ``assigned_user_id``.

    Exercises both ``recompute_gsis`` (GSI key must update when assignment changes)
    and ``clear_fields`` (GSI key must REMOVE when assignment is cleared).
    """

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        omit_none_attributes=True,
    )

    gsi_config: ClassVar[dict] = {
        "by-assignee": {
            "gsi3pk": lambda self: None if not self.assigned_user_id else f"asset#assigned#{self.assigned_user_id}",
            "gsi3sk": lambda self: None if not self.assigned_user_id else self.resource_id,
        },
    }

    asset_tag: str
    assigned_user_id: Optional[str] = None


class AuditedPost(DynamoDbVersionedResource):
    """Versioned resource with audit enabled."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
    )

    title: str
    body: str


# --- Helpers ----------------------------------------------------------------------------


def _raw_item(memory: DynamoDbMemory, resource) -> dict:
    pk = f"{type(resource).get_unique_key_prefix()}#{resource.resource_id}"
    sk = pk if not isinstance(resource, DynamoDbVersionedResource) else "v0"
    return memory.dynamodb_table.get_item(Key={"pk": pk, "sk": sk})["Item"]


def _audit_logs_for(memory: DynamoDbMemory, resource) -> list[AuditLog]:
    return memory.list_type_by_updated_at(AuditLog, ascending=False).as_list() and [
        log
        for log in memory.list_type_by_updated_at(AuditLog, ascending=False)
        if log.audited_resource_type == type(resource).__name__
        and log.audited_resource_id == resource.resource_id
    ]


# --- A6: exception normalization --------------------------------------------------------


def test_user_condition_failure_raises_condition_failed_error(dynamodb_memory: DynamoDbMemory):
    """User-supplied condition failures surface as TransactionConditionFailedError."""
    user = dynamodb_memory.create_new(AuditedUser, {"name": "Alice", "email": "a@x", "role": "admin"})

    with pytest.raises(TransactionConditionFailedError) as exc_info:
        with dynamodb_memory.transaction(auto_retry=False) as txn:
            txn.update(
                AuditedUser,
                resource_id=user.resource_id,
                updates={"role": "owner"},
                condition="attribute_not_exists(#role)",
                condition_values={},
            )

    err = exc_info.value
    # Carries the cancellation payload and the originating op index.
    assert err.cancellation_reasons, "Expected cancellation_reasons to be populated"
    assert err.operation_indexes == [0]


def test_version_conflict_error_remains_back_compat(dynamodb_memory: DynamoDbMemory):
    """Existing ``except VersionConflictError`` blocks still catch condition failures."""
    user = dynamodb_memory.create_new(AuditedUser, {"name": "Bob", "email": "b@x"})

    with pytest.raises(VersionConflictError):
        with dynamodb_memory.transaction(auto_retry=False) as txn:
            txn.update(
                AuditedUser,
                resource_id=user.resource_id,
                updates={"role": "owner"},
                condition="attribute_not_exists(pk)",  # always false
            )


def test_version_conflict_error_is_subclass_of_condition_failed(dynamodb_memory: DynamoDbMemory):
    """Catching the canonical type also catches the back-compat alias."""
    user = dynamodb_memory.create_new(AuditedUser, {"name": "Carol", "email": "c@x"})

    with pytest.raises(TransactionConditionFailedError):
        with dynamodb_memory.transaction(auto_retry=False) as txn:
            txn.update(
                AuditedUser,
                resource_id=user.resource_id,
                updates={"role": "owner"},
                condition="attribute_not_exists(pk)",
            )


# --- A5: retry policy -------------------------------------------------------------------


def test_user_condition_failures_never_retry(dynamodb_memory: DynamoDbMemory, caplog):
    """Even with auto_retry=True (default), user-supplied conditions fail fast."""
    user = dynamodb_memory.create_new(AuditedUser, {"name": "Dan", "email": "d@x", "role": "admin"})

    with caplog.at_level("WARNING"):
        with pytest.raises(VersionConflictError):
            with dynamodb_memory.transaction(max_retries=3) as txn:
                txn.update(
                    AuditedUser,
                    resource_id=user.resource_id,
                    updates={"role": "owner"},
                    condition="attribute_not_exists(pk)",
                )
    # No retry warnings should have been emitted.
    retry_messages = [r for r in caplog.records if "retrying" in r.getMessage().lower()]
    assert retry_messages == []


# --- A3 + A4 combined: clear_fields produces REMOVE, GSI sparsity is preserved ----------


def test_clear_fields_emits_remove_expression(dynamodb_memory: DynamoDbMemory):
    """clear_fields=[...] strips attributes from the item via REMOVE."""
    asset = dynamodb_memory.create_new(
        Asset, {"asset_tag": "tag-1", "assigned_user_id": "alice"}
    )

    item_before = _raw_item(dynamodb_memory, asset)
    assert item_before["assigned_user_id"] == "alice"
    assert item_before.get("gsi3pk") == "asset#assigned#alice"

    with dynamodb_memory.transaction() as txn:
        txn.update(
            Asset,
            resource_id=asset.resource_id,
            clear_fields=["assigned_user_id"],
            recompute_gsis=True,
        )

    item_after = _raw_item(dynamodb_memory, asset)
    assert "assigned_user_id" not in item_after
    # GSI key must be REMOVEd, not left pointing at the old assignee.
    assert "gsi3pk" not in item_after
    assert "gsi3sk" not in item_after


def test_updates_and_clear_fields_cannot_overlap(dynamodb_memory: DynamoDbMemory):
    asset = dynamodb_memory.create_new(Asset, {"asset_tag": "tag-2"})

    with pytest.raises(ValueError, match="cannot reference the same field"):
        with dynamodb_memory.transaction() as txn:
            txn.update(
                Asset,
                resource_id=asset.resource_id,
                updates={"assigned_user_id": "alice"},
                clear_fields=["assigned_user_id"],
            )


# --- A2: GSI recompute under txn.update -------------------------------------------------


def test_recompute_gsis_updates_gsi_key_on_assignment_change(dynamodb_memory: DynamoDbMemory):
    """Changing a GSI-source field updates the GSI key (instead of leaving it stale)."""
    asset = dynamodb_memory.create_new(Asset, {"asset_tag": "tag-3", "assigned_user_id": "alice"})

    with dynamodb_memory.transaction() as txn:
        txn.update(
            Asset,
            resource_id=asset.resource_id,
            updates={"assigned_user_id": "bob"},
            recompute_gsis=True,
        )

    item = _raw_item(dynamodb_memory, asset)
    assert item["assigned_user_id"] == "bob"
    assert item["gsi3pk"] == "asset#assigned#bob"
    assert item["gsi3sk"] == asset.resource_id


def test_recompute_gsis_removes_gsi_key_when_lambda_returns_none(dynamodb_memory: DynamoDbMemory):
    """Clearing the GSI-source field REMOVEs the GSI key (preserves sparseness)."""
    asset = dynamodb_memory.create_new(Asset, {"asset_tag": "tag-4", "assigned_user_id": "alice"})

    with dynamodb_memory.transaction() as txn:
        txn.update(
            Asset,
            resource_id=asset.resource_id,
            updates={"assigned_user_id": None},
            recompute_gsis=True,
        )

    item = _raw_item(dynamodb_memory, asset)
    assert "gsi3pk" not in item
    assert "gsi3sk" not in item


# --- A9: txn.put ------------------------------------------------------------------------


def test_put_writes_full_state_and_recomputes_gsis(dynamodb_memory: DynamoDbMemory):
    """txn.put writes the entire resource, recomputing GSI keys automatically."""
    asset = dynamodb_memory.create_new(Asset, {"asset_tag": "tag-5", "assigned_user_id": "alice"})

    asset.assigned_user_id = "carol"

    with dynamodb_memory.transaction() as txn:
        txn.put(asset)

    item = _raw_item(dynamodb_memory, asset)
    assert item["assigned_user_id"] == "carol"
    assert item["gsi3pk"] == "asset#assigned#carol"


def test_put_rejects_versioned_resources(dynamodb_memory: DynamoDbMemory):
    """Versioned resources must go through txn.update (for version semantics)."""
    post = dynamodb_memory.create_new(AuditedPost, {"title": "T", "body": "B"})

    with pytest.raises(ValueError, match="only supported for non-versioned"):
        with dynamodb_memory.transaction() as txn:
            txn.put(post)


# --- A8: current= short-circuits the versioned-update read ------------------------------


def test_current_short_circuits_versioned_update_read(dynamodb_memory: DynamoDbMemory, mocker):
    """Supplying current= skips the inline get_existing call in the versioned builder."""
    post = dynamodb_memory.create_new(AuditedPost, {"title": "T", "body": "B1"})

    spy = mocker.spy(dynamodb_memory, "get_existing")

    with dynamodb_memory.transaction() as txn:
        txn.update(AuditedPost, resource_id=post.resource_id, updates={"body": "B2"}, current=post)

    # Without current=, the versioned builder would have called get_existing during build.
    # The post-commit audit walk for a versioned UPDATE does NOT re-read because the new
    # state was already computed by the builder.
    assert spy.call_count == 0


# --- A1 + A7: commit-time audit + stats -------------------------------------------------


def test_transactional_create_emits_audit_and_bumps_stats(dynamodb_memory: DynamoDbMemory):
    """Creating an audited resource via a transaction produces the same audit row as create_new."""
    starting_stats = dynamodb_memory.get_stats()
    starting_count = starting_stats.counts_by_type.get("AuditedUser", 0)

    user = AuditedUser(
        name="Eve",
        email="e@x",
        resource_id="user-eve",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    with dynamodb_memory.transaction(changed_by="admin") as txn:
        txn.create(user)

    # Audit row was emitted.
    logs = _audit_logs_for(dynamodb_memory, user)
    assert len(logs) == 1
    assert logs[0].operation == "CREATE"
    assert logs[0].changed_by == "admin"
    assert logs[0].audited_resource_id == user.resource_id
    # Transaction id is auto-attached.
    assert logs[0].audit_metadata.get("transaction_id")

    # Stats bumped.
    after_stats = dynamodb_memory.get_stats()
    assert after_stats.counts_by_type.get("AuditedUser", 0) == starting_count + 1


def test_transactional_update_emits_update_audit_with_diff(dynamodb_memory: DynamoDbMemory):
    """UPDATE in txn captures old/new field changes when track_field_changes is on."""
    user = dynamodb_memory.create_new(AuditedUser, {"name": "Frank", "email": "f@x"}, changed_by="admin")

    with dynamodb_memory.transaction(changed_by="bob") as txn:
        txn.update(AuditedUser, resource_id=user.resource_id, updates={"role": "writer"}, current=user)

    logs = _audit_logs_for(dynamodb_memory, user)
    update_logs = [log for log in logs if log.operation == "UPDATE"]
    assert update_logs, "Expected an UPDATE audit row from the transaction"

    diff = update_logs[0].changed_fields
    assert diff is not None
    assert diff["role"] == {"old": None, "new": "writer"}
    assert update_logs[0].changed_by == "bob"


def test_transactional_versioned_update_emits_audit(dynamodb_memory: DynamoDbMemory):
    """Versioned UPDATE through a transaction emits an UPDATE audit row with field diffs."""
    post = dynamodb_memory.create_new(AuditedPost, {"title": "T", "body": "B1"}, changed_by="admin")

    with dynamodb_memory.transaction(changed_by="editor") as txn:
        txn.update(AuditedPost, resource_id=post.resource_id, updates={"body": "B2"}, current=post)

    logs = _audit_logs_for(dynamodb_memory, post)
    update_logs = [log for log in logs if log.operation == "UPDATE"]
    assert update_logs
    assert update_logs[0].changed_fields["body"] == {"old": "B1", "new": "B2"}


def test_transactional_delete_decrements_stats_for_non_versioned(dynamodb_memory: DynamoDbMemory):
    starting = dynamodb_memory.get_stats().counts_by_type.get("AuditedUser", 0)
    user = dynamodb_memory.create_new(AuditedUser, {"name": "Greta", "email": "g@x"})

    after_create = dynamodb_memory.get_stats().counts_by_type.get("AuditedUser", 0)
    assert after_create == starting + 1

    with dynamodb_memory.transaction() as txn:
        txn.delete(user)

    after_delete = dynamodb_memory.get_stats().counts_by_type.get("AuditedUser", 0)
    assert after_delete == starting


# --- B3: public audit emission API ------------------------------------------------------


def test_emit_audit_log_writes_single_row(dynamodb_memory: DynamoDbMemory):
    """Public emit_audit_log writes an audit row for arbitrary state."""
    user = dynamodb_memory.create_new(AuditedUser, {"name": "Helen", "email": "h@x"})

    audit = dynamodb_memory.emit_audit_log(
        operation="CUSTOM_EVENT",
        resource=user,
        changed_by="admin",
        audit_metadata={"reason": "manual annotation"},
    )

    assert audit is not None
    assert audit.operation == "CUSTOM_EVENT"
    assert audit.audit_metadata == {"reason": "manual annotation"}


def test_emit_audit_log_respects_disabled_config_unless_forced(dynamodb_memory: DynamoDbMemory):
    """Without force=True, emit_audit_log silently skips resources with audit disabled."""

    class Plain(DynamoDbResource):
        name: str

    plain = dynamodb_memory.create_new(Plain, {"name": "noaudit"})

    skipped = dynamodb_memory.emit_audit_log(operation="UPDATE", resource=plain)
    assert skipped is None

    forced = dynamodb_memory.emit_audit_log(operation="UPDATE", resource=plain, force=True)
    assert forced is not None


def test_emit_audit_logs_batches_writes(dynamodb_memory: DynamoDbMemory):
    """Batch variant writes all entries in one BatchWriteItem call."""
    users = [
        dynamodb_memory.create_new(AuditedUser, {"name": f"user{i}", "email": f"u{i}@x"})
        for i in range(3)
    ]

    starting = len(list(dynamodb_memory.list_type_by_updated_at(AuditLog)))

    emitted = dynamodb_memory.emit_audit_logs(
        [AuditEntry(operation="BACKFILL", resource=u, changed_by="batchjob") for u in users]
    )
    assert len(emitted) == 3

    ending = len(list(dynamodb_memory.list_type_by_updated_at(AuditLog)))
    assert ending == starting + 3
