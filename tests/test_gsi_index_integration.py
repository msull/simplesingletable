"""``GsiIndex`` declarations end to end against DynamoDB Local."""

import warnings
from typing import ClassVar

import pytest
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from simplesingletable import (
    AuditLog,
    AuditLogQuerier,
    DynamoDbMemory,
    DynamoDbResource,
    DynamoDbVersionedResource,
    GsiIndex,
)
from simplesingletable.extras.form_data import (
    FormDataEntryField,
    FormDataManager,
    NewFormRequest,
    StoredFormData,
)
from simplesingletable.models import AuditConfig, ResourceConfig

EXPLICIT_CONFIG = {
    "gsi1": GsiIndex(pk=("gsi1pk", lambda self: f"owner#{self.owner_id}"), sk="pk", description="by owner"),
    "gsi2": GsiIndex(
        pk=("gsi2pk", lambda self: self.is_active and f"active#{self.owner_id}"), sk="pk", description="active only"
    ),
    "gsi3": GsiIndex(
        # Sparse: only assigned things are in this index
        pk=("gsi3pk", lambda self: self.assignee and f"assignee#{self.assignee}"),
        sk=("gsi3sk", lambda self: self.created_at.isoformat()),
        description="by assignee",
    ),
}


class ExplicitThing(DynamoDbResource):
    owner_id: str
    assignee: str | None = None
    is_active: bool = True

    gsi_config: ClassVar[dict] = EXPLICIT_CONFIG


class ExplicitVersionedThing(DynamoDbVersionedResource):
    owner_id: str
    assignee: str | None = None
    is_active: bool = True

    gsi_config: ClassVar[dict] = EXPLICIT_CONFIG


class OwnedThing(DynamoDbResource):
    """Custom attribute names behind a descriptive description: nothing here can be inferred."""

    owner_id: str
    status: str

    gsi_config: ClassVar[dict] = {
        "owner-index": GsiIndex(
            pk=("ownerKey", lambda self: f"owner#{self.owner_id}"),
            sk=("createdKey", lambda self: self.resource_id),
            description="things by owner, oldest first",
        ),
    }


class ScoredThing(DynamoDbResource):
    score: int

    gsi_config: ClassVar[dict] = {"score-index": GsiIndex(pk=("scoreKey", lambda self: self.score))}


class MistypedThing(DynamoDbResource):
    n: int

    gsi_config: ClassVar[dict] = {
        "gsi3": GsiIndex(pk=("gsi3pk", lambda self: self.n), sk=("gsi3sk", lambda self: "x")),
    }


class AuditedThing(DynamoDbResource):
    name: str

    resource_config: ClassVar[ResourceConfig] = ResourceConfig(
        compress_data=False, audit_config=AuditConfig(enabled=True, track_field_changes=True)
    )


BOTH_CLASSES = [ExplicitThing, ExplicitVersionedThing]


def _raw_v0(memory: DynamoDbMemory, resource) -> dict:
    pk = f"{resource.get_unique_key_prefix()}#{resource.resource_id}"
    return memory.dynamodb_table.get_item(Key={"pk": pk, "sk": "v0"})["Item"]


def _raw(memory: DynamoDbMemory, resource) -> dict:
    key = resource.dynamodb_lookup_keys_from_id(resource.resource_id)
    return memory.dynamodb_table.get_item(Key=key)["Item"]


def _page_through(memory: DynamoDbMemory, **query) -> list[str]:
    seen = []
    pagination_key = None
    for _ in range(20):
        page = memory.paginated_dynamodb_query(pagination_key=pagination_key, **query)
        seen.extend(resource.resource_id for resource in page)
        pagination_key = page.next_pagination_key
        if not pagination_key:
            break
    return seen


@pytest.mark.parametrize("cls", BOTH_CLASSES)
class TestWritePath:
    def test_attributes_written(self, dynamodb_memory: DynamoDbMemory, cls):
        unassigned = dynamodb_memory.create_new(cls, {"owner_id": "o1"})
        assigned = dynamodb_memory.create_new(cls, {"owner_id": "o1", "assignee": "a1"})

        item = _raw(dynamodb_memory, unassigned)
        assert item["gsi1pk"] == "owner#o1"
        assert "gsi1sk" not in item
        assert "gsi3pk" not in item and "gsi3sk" not in item

        item = _raw(dynamodb_memory, assigned)
        assert item["gsi3pk"] == "assignee#a1"
        assert item["gsi3sk"] == assigned.created_at.isoformat()

    def test_round_trip_excludes_index_attributes(self, dynamodb_memory: DynamoDbMemory, cls):
        created = dynamodb_memory.create_new(cls, {"owner_id": "o1", "assignee": "a1"})
        assert dynamodb_memory.get_existing(created.resource_id, data_class=cls) == created

    def test_queries(self, dynamodb_memory: DynamoDbMemory, cls):
        unassigned = dynamodb_memory.create_new(cls, {"owner_id": "o1"})
        assigned = dynamodb_memory.create_new(cls, {"owner_id": "o1", "assignee": "a1"})

        by_owner = dynamodb_memory.paginated_dynamodb_query(
            key_condition=Key("gsi1pk").eq("owner#o1"), index_name="gsi1", resource_class=cls
        )
        assert {r.resource_id for r in by_owner} == {unassigned.resource_id, assigned.resource_id}

        by_assignee = dynamodb_memory.paginated_dynamodb_query(
            key_condition=Key("gsi3pk").eq("assignee#a1"), index_name="gsi3", resource_class=cls
        )
        assert [r.resource_id for r in by_assignee] == [assigned.resource_id]

    def test_update_clearing_sparse_value_removes_attributes(self, dynamodb_memory: DynamoDbMemory, cls):
        assigned = dynamodb_memory.create_new(cls, {"owner_id": "o1", "assignee": "a1"})
        updated = dynamodb_memory.update_existing(assigned, {}, clear_fields={"assignee"})

        item = _raw(dynamodb_memory, updated)
        assert "gsi3pk" not in item and "gsi3sk" not in item

    def test_false_is_absent(self, dynamodb_memory: DynamoDbMemory, cls):
        inactive = dynamodb_memory.create_new(cls, {"owner_id": "o1", "is_active": False})
        active = dynamodb_memory.create_new(cls, {"owner_id": "o1", "is_active": True})

        assert "gsi2pk" not in _raw(dynamodb_memory, inactive)
        assert _raw(dynamodb_memory, active)["gsi2pk"] == "active#o1"
        results = dynamodb_memory.paginated_dynamodb_query(
            key_condition=Key("gsi2pk").eq("active#o1"), index_name="gsi2", resource_class=cls
        )
        assert [r.resource_id for r in results] == [active.resource_id]


class TestTransactions:
    def test_recompute_sets_explicit_attributes(self, dynamodb_memory: DynamoDbMemory):
        thing = dynamodb_memory.create_new(ExplicitThing, {"owner_id": "o1"})
        with dynamodb_memory.transaction() as tx:
            tx.update(thing, updates={"assignee": "x"}, recompute_gsis=True)

        item = _raw(dynamodb_memory, thing)
        assert item["gsi3pk"] == "assignee#x"
        assert item["gsi3sk"] == thing.created_at.isoformat()

    @pytest.mark.parametrize(
        "update_kwargs", [{"clear_fields": {"assignee"}}, {"updates": {"assignee": None}}], ids=["clear", "none"]
    )
    def test_recompute_removes_all_attributes_of_absent_index(self, dynamodb_memory: DynamoDbMemory, update_kwargs):
        thing = dynamodb_memory.create_new(ExplicitThing, {"owner_id": "o1", "assignee": "a1"})
        with dynamodb_memory.transaction() as tx:
            tx.update(thing, recompute_gsis=True, **update_kwargs)

        item = _raw(dynamodb_memory, thing)
        assert "gsi3pk" not in item and "gsi3sk" not in item

    def test_recompute_removes_on_false(self, dynamodb_memory: DynamoDbMemory):
        thing = dynamodb_memory.create_new(ExplicitThing, {"owner_id": "o1"})
        assert "gsi2pk" in _raw(dynamodb_memory, thing)
        with dynamodb_memory.transaction() as tx:
            tx.update(thing, updates={"is_active": False}, recompute_gsis=True)

        assert "gsi2pk" not in _raw(dynamodb_memory, thing)

    def test_versioned_update_rebuilds_explicit_attributes(self, dynamodb_memory: DynamoDbMemory):
        """Versioned updates do not use ``_compute_gsi_changes``: both items are rebuilt through
        ``to_dynamodb_item``, so this covers GsiIndex on the write path inside a transaction."""
        thing = dynamodb_memory.create_new(ExplicitVersionedThing, {"owner_id": "o1", "assignee": "a1"})
        with dynamodb_memory.transaction() as tx:
            tx.update(thing, clear_fields={"assignee"})

        item = _raw_v0(dynamodb_memory, thing)
        assert item["version"] == 2
        assert "gsi3pk" not in item and "gsi3sk" not in item


class TestCustomIndex:
    def test_filtered_pagination_on_custom_attributes(self, memory_with_custom_index: DynamoDbMemory):
        """A filter that rejects rows forces the library to synthesize the LEK itself; with custom attribute
        names behind a descriptive description, only the explicit declaration can say what goes in it."""
        for idx in range(10):
            memory_with_custom_index.create_new(
                OwnedThing, {"owner_id": "o1", "status": "active" if idx % 2 == 0 else "archived"}
            )

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            seen = _page_through(
                memory_with_custom_index,
                key_condition=Key("ownerKey").eq("owner#o1"),
                index_name="owner-index",
                resource_class=OwnedThing,
                filter_expression=Attr("status").eq("active"),
                results_limit=2,
            )

        assert len(seen) == 5, "every active row should be seen exactly once"
        assert len(set(seen)) == 5, "pagination restarted and re-served rows"
        assert not [w for w in recorded if issubclass(w.category, DeprecationWarning)]

    def test_zero_valued_key_is_written(self, memory_with_numeric_index: DynamoDbMemory):
        """The dict form drops a falsy 0; a GsiIndex writes it, since 0 is a valid N key value."""
        thing = memory_with_numeric_index.create_new(ScoredThing, {"score": 0})

        assert _raw(memory_with_numeric_index, thing)["scoreKey"] == 0
        results = memory_with_numeric_index.paginated_dynamodb_query(
            key_condition=Key("scoreKey").eq(0), index_name="score-index", resource_class=ScoredThing
        )
        assert [r.resource_id for r in results] == [thing.resource_id]

    def test_value_type_must_match_index_attribute_type(self, dynamodb_memory: DynamoDbMemory):
        """gsi3pk is S on the shared table; a numeric value fails the write rather than being silently dropped."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_memory.create_new(MistypedThing, {"n": 0})
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


class TestAuditLogRegression:
    def test_audit_indexes_paginate(self, dynamodb_memory: DynamoDbMemory):
        thing = dynamodb_memory.create_new(AuditedThing, {"name": "v0"}, changed_by="alice")
        for idx in range(4):
            thing = dynamodb_memory.update_existing(thing, {"name": f"v{idx + 1}"}, changed_by="alice")

        querier = AuditLogQuerier(dynamodb_memory)
        seen = []
        pagination_key = None
        for _ in range(10):
            page = querier.get_logs_for_resource("AuditedThing", thing.resource_id, limit=2, pagination_key=pagination_key)
            seen.extend(log.resource_id for log in page)
            pagination_key = page.next_pagination_key
            if not pagination_key:
                break
        assert len(seen) == 5 and len(set(seen)) == 5

        by_changer = _page_through(
            dynamodb_memory,
            key_condition=Key("gsi3pk").eq(f"{AuditLog.get_unique_key_prefix()}#changer#alice"),
            index_name=AuditLog.INDEX_BY_CHANGER,
            resource_class=AuditLog,
            results_limit=2,
        )
        assert sorted(by_changer) == sorted(seen)
        assert {log.resource_id for log in querier.get_logs_by_changer("alice")} == set(seen)


class TestFormDataWrittenValues:
    def test_form_data_index_values_are_byte_identical(self, dynamodb_memory: DynamoDbMemory):
        """The values Form / FormEntry write to gsi1pk / gsi2pk are pinned as literals, so moving them from
        the legacy hooks to GsiIndex cannot change what existing rows (and queries) depend on."""
        manager = FormDataManager(memory=dynamodb_memory)
        form_type = manager.add_new_type(
            name="pinned-type", schema=[FormDataEntryField(name="title", field_type="str", allowed_values=None)]
        )
        manager.add_form_category("cat-a")
        form = manager.create_form(
            NewFormRequest(
                name="Pinned",
                category="cat-a",
                form_data_type_id=form_type.resource_id,
                form_data_type_version=form_type.version,
                form_data_type_schema=form_type.entry_schema,
                columns=["Column1"],
                groups=["Group1"],
            )
        )
        entry = manager.store_form_data(
            form, StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "x"})
        )

        form_item = _raw_v0(dynamodb_memory, form)
        entry_item = _raw_v0(dynamodb_memory, entry)

        assert form_item["gsi1pk"] == "Form#cat-a"
        assert entry_item["gsi1pk"] == f"FormEntry#{form.resource_id}"
        assert entry_item["gsi2pk"] == f"FormEntry#{form.resource_id}#row1#Group1"

        assert form.gsi1_partition_value() == form_item["gsi1pk"]
        assert entry.gsi1_partition_value() == entry_item["gsi1pk"]
        assert entry.gsi2_partition_value() == entry_item["gsi2pk"]
