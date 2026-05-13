"""Tests for the issue #3 AuditLog ergonomics overhaul.

Covers:
- B2: AuditLogQuerier uses class constants from AuditLog instead of hardcoded literals.
- B5: AuditLog has a sparse changed_by GSI; get_logs_by_changer queries it directly.
- B6: Secondary DynamoDbMemory audit view is cached on the parent (one view shared
      across all queriers).
"""

from typing import ClassVar, Optional

import pytest

from simplesingletable import AuditLogQuerier, DynamoDbMemory, DynamoDbResource
from simplesingletable.models import AuditConfig, AuditLog, ResourceConfig


class TrackedDoc(DynamoDbResource):
    """Resource with audit fully enabled (field tracking + snapshot)."""

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        omit_none_attributes=True,
        audit_config=AuditConfig(
            enabled=True,
            track_field_changes=True,
            include_snapshot=True,
        ),
    )

    title: str
    body: Optional[str] = None


# --- B2: index name constants -----------------------------------------------------------


def test_audit_log_exposes_index_name_constants():
    """AuditLog class constants are the source of truth for index names."""
    assert AuditLog.INDEX_BY_RESOURCE == "gsi1"
    assert AuditLog.INDEX_BY_TYPE == "gsi2"
    assert AuditLog.INDEX_BY_CHANGER == "gsi3"
    assert AuditLog.INDEX_BY_UPDATED_AT == "gsitype"

    # And the GSI config returned by AuditLog uses these names as its keys.
    gsi_config = AuditLog.get_gsi_config()
    assert AuditLog.INDEX_BY_RESOURCE in gsi_config
    assert AuditLog.INDEX_BY_TYPE in gsi_config
    assert AuditLog.INDEX_BY_CHANGER in gsi_config


def test_audit_querier_does_not_reference_hardcoded_index_names():
    """The querier source no longer contains "gsi1" / "gsi2" string literals.

    Guards against future regressions sneaking hardcoded names back in.
    """
    import simplesingletable.extras.audit as audit_mod

    source = open(audit_mod.__file__).read()
    # The querier should reference AuditLog.INDEX_BY_* rather than bare "gsi1"/"gsi2".
    # Hardcoded literals are only acceptable as part of the AuditLog class itself.
    for literal in ['"gsi1"', '"gsi2"']:
        # Allow no occurrences in audit.py — they should all go through the constants.
        assert literal not in source, f"Querier still references hardcoded {literal}"


# --- B5: sparse changed_by GSI ----------------------------------------------------------


def test_changed_by_gsi_is_sparse(dynamodb_memory: DynamoDbMemory):
    """Audit rows with changed_by=None do not get a gsi3pk/gsi3sk attribute."""
    # Resource has audit enabled but the caller didn't supply changed_by.
    doc = dynamodb_memory.create_new(TrackedDoc, {"title": "anon doc"})

    audit_logs = dynamodb_memory.audit_view.list_type_by_updated_at(AuditLog).as_list()
    for log in audit_logs:
        if log.audited_resource_id != doc.resource_id:
            continue
        # Raw item to inspect the actual DDB attributes.
        pk = f"{AuditLog.get_unique_key_prefix()}#{log.resource_id}"
        item = dynamodb_memory.audit_view.dynamodb_table.get_item(Key={"pk": pk, "sk": pk})["Item"]
        assert "gsi3pk" not in item, "Expected sparse: no gsi3pk when changed_by is None"
        assert "gsi3sk" not in item


def test_changed_by_gsi_populated_when_attribution_provided(dynamodb_memory: DynamoDbMemory):
    """Audit rows with changed_by set DO get the gsi3pk/gsi3sk index keys."""
    doc = dynamodb_memory.create_new(TrackedDoc, {"title": "attributed"}, changed_by="alice")

    # Find the audit row for this doc.
    audit_logs = dynamodb_memory.audit_view.list_type_by_updated_at(AuditLog).as_list()
    log = next(log for log in audit_logs if log.audited_resource_id == doc.resource_id)

    pk = f"{AuditLog.get_unique_key_prefix()}#{log.resource_id}"
    item = dynamodb_memory.audit_view.dynamodb_table.get_item(Key={"pk": pk, "sk": pk})["Item"]
    assert item["gsi3pk"] == f"{AuditLog.get_unique_key_prefix()}#changer#alice"
    # gsi3sk is the created_at isoformat for sortability within a changer's stream.
    assert item["gsi3sk"] == log.created_at.isoformat()


def test_get_logs_by_changer_uses_direct_gsi_query(dynamodb_memory: DynamoDbMemory, mocker):
    """Without resource_type, get_logs_by_changer hits INDEX_BY_CHANGER directly
    (no filter expression fallback)."""
    dynamodb_memory.create_new(TrackedDoc, {"title": "doc1"}, changed_by="alice")
    dynamodb_memory.create_new(TrackedDoc, {"title": "doc2"}, changed_by="alice")
    dynamodb_memory.create_new(TrackedDoc, {"title": "doc3"}, changed_by="bob")

    querier = AuditLogQuerier(dynamodb_memory)

    spy = mocker.spy(dynamodb_memory.audit_view, "paginated_dynamodb_query")
    logs = querier.get_logs_by_changer("alice")

    # Returns only alice's audit rows.
    assert len(logs) == 2
    assert all(log.changed_by == "alice" for log in logs)

    # And the underlying query used the new direct index, no filter expression.
    assert spy.call_count == 1
    call_kwargs = spy.call_args.kwargs
    assert call_kwargs["index_name"] == AuditLog.INDEX_BY_CHANGER
    assert call_kwargs.get("filter_expression") is None


def test_get_logs_by_changer_with_resource_type_uses_type_index(dynamodb_memory: DynamoDbMemory, mocker):
    """With a resource_type filter, falls back to INDEX_BY_TYPE + filter expression
    (no composite index exists for this combination)."""
    dynamodb_memory.create_new(TrackedDoc, {"title": "doc-alice"}, changed_by="alice")

    querier = AuditLogQuerier(dynamodb_memory)
    spy = mocker.spy(dynamodb_memory.audit_view, "paginated_dynamodb_query")

    logs = querier.get_logs_by_changer("alice", resource_type="TrackedDoc")
    assert len(logs) == 1

    call_kwargs = spy.call_args.kwargs
    assert call_kwargs["index_name"] == AuditLog.INDEX_BY_TYPE
    assert call_kwargs.get("filter_expression") is not None


# --- B6: secondary audit view caching ---------------------------------------------------


def test_audit_view_returns_self_when_no_separate_table(dynamodb_memory: DynamoDbMemory):
    """When audit_table_name is not set, audit_view is the memory itself."""
    assert dynamodb_memory.audit_view is dynamodb_memory


def test_multiple_queriers_share_the_same_audit_view(dynamodb_memory: DynamoDbMemory):
    """All AuditLogQuerier instances on the same memory share one audit view."""
    q1 = AuditLogQuerier(dynamodb_memory)
    q2 = AuditLogQuerier(dynamodb_memory)
    q3 = AuditLogQuerier(dynamodb_memory)

    # Same underlying view object — no per-querier duplicate construction.
    assert q1.audit_memory is q2.audit_memory
    assert q2.audit_memory is q3.audit_memory
    assert q1.audit_memory is dynamodb_memory.audit_view
