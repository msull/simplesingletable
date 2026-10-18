"""``DynamoDbMemory.validate_gsi_config`` against DynamoDB Local tables."""

import warnings
from typing import ClassVar
from uuid import uuid4

import boto3
import pytest
from logzero import logger
from pydantic import BaseModel

from simplesingletable import AuditLog, DynamoDbMemory, DynamoDbResource, GsiConfigError, GsiIndex
from simplesingletable.extras.form_data import Form, FormEntry


def _value(self):
    return "x"


class ExplicitThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {
        "gsi1": GsiIndex(pk=("gsi1pk", _value), sk="pk"),
        "gsi2": GsiIndex(pk=("gsi2pk", _value), sk="pk"),
        "gsi3": GsiIndex(pk=("gsi3pk", _value), sk=("gsi3sk", _value)),
    }


class GsiOneHookMixin(BaseModel):
    def db_get_gsi1pk(self) -> str | None:
        return "x"


with warnings.catch_warnings():
    # Deprecated forms, deliberately: the validator has to handle them during the transition
    warnings.simplefilter("ignore", DeprecationWarning)

    class DictThing(DynamoDbResource):
        gsi_config: ClassVar[dict] = {"gsi1": {"gsi1pk": _value}}

    class HookThing(DynamoDbResource):
        def db_get_gsi1pk(self) -> str | None:
            return "x"

    class HookConflictThing(GsiOneHookMixin, DynamoDbResource):
        gsi_config: ClassVar[dict] = {"gsi1": GsiIndex(pk=("gsi1pk", _value), sk="pk")}


class MisspelledIndexThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {"gsi-3": GsiIndex(pk=("gsi3pk", _value), sk=("gsi3sk", _value))}


class MissingRangeThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {"gsi1": GsiIndex(pk=("gsi1pk", _value))}


class WrongSortThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {"gsi3": GsiIndex(pk=("gsi3pk", _value), sk=("gsi9sk", _value))}


class LabelMismatchThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {"gsi2": {"gsi1pk": _value}}


class UnboundThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {"by-owner": {"gsi9pk": _value}}


class DescriptiveLabelThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {"by-owner": {"gsi3pk": _value, "gsi3sk": _value}}


class TypeKeyedThing(DynamoDbResource):
    gsi_config: ClassVar[dict] = {"gsitype": GsiIndex(pk=("gsi1pk", _value), sk="pk")}


def _kinds(problems) -> list[str]:
    return [p.kind for p in problems]


def test_matching_declarations_pass(dynamodb_memory: DynamoDbMemory):
    assert (
        dynamodb_memory.validate_gsi_config(
            ExplicitThing, AuditLog, Form, FormEntry, DictThing, HookThing, DescriptiveLabelThing
        )
        == []
    )


def test_missing_index_suggests_normalized_match(dynamodb_memory: DynamoDbMemory):
    problems = dynamodb_memory.validate_gsi_config(MisspelledIndexThing, raise_on_error=False)
    assert _kinds(problems) == ["missing_index"]
    assert "did you mean 'gsi3'?" in problems[0].message
    assert problems[0].resource_class == "MisspelledIndexThing"
    assert problems[0].index_name == "gsi-3"


@pytest.mark.parametrize("cls", [MissingRangeThing, WrongSortThing])
def test_key_schema_mismatch(dynamodb_memory: DynamoDbMemory, cls):
    assert _kinds(dynamodb_memory.validate_gsi_config(cls, raise_on_error=False)) == ["key_schema_mismatch"]


def test_label_mismatch(dynamodb_memory: DynamoDbMemory):
    assert _kinds(dynamodb_memory.validate_gsi_config(LabelMismatchThing, raise_on_error=False)) == ["label_mismatch"]


def test_unbound_attributes(dynamodb_memory: DynamoDbMemory):
    assert _kinds(dynamodb_memory.validate_gsi_config(UnboundThing, raise_on_error=False)) == ["unbound_attributes"]


def test_legacy_hook_conflict(dynamodb_memory: DynamoDbMemory):
    problems = dynamodb_memory.validate_gsi_config(HookConflictThing, raise_on_error=False)
    assert _kinds(problems) == ["legacy_hook_conflict"]
    assert "db_get_gsi1pk" in problems[0].message

    with pytest.raises(GsiConfigError) as exc_info:
        dynamodb_memory.validate_gsi_config(HookConflictThing)
    assert _kinds(exc_info.value.problems) == ["legacy_hook_conflict"]


def test_legacy_hook_unbound(memory_with_custom_index: DynamoDbMemory):
    problems = memory_with_custom_index.validate_gsi_config(HookThing, raise_on_error=False)
    assert _kinds(problems) == ["legacy_hook_unbound"]
    assert problems[0].index_name == "db_get_gsi1pk"


@pytest.fixture()
def memory_with_bad_type_index(dynamodb_via_docker):
    """A table whose ``gsitype`` index ranges on the table pk instead of ``gsitypesk``."""
    connection_params = {"aws_access_key_id": "unused", "aws_secret_access_key": "unused", "region_name": "us-west-2"}
    table_name = f"bad-type-index-{uuid4().hex}"
    resource = boto3.resource("dynamodb", endpoint_url=dynamodb_via_docker, **connection_params)
    resource.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsitype", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsitype",
                "KeySchema": [
                    {"AttributeName": "gsitype", "KeyType": "HASH"},
                    {"AttributeName": "pk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resource.Table(table_name).wait_until_exists()
    try:
        yield DynamoDbMemory(
            logger=logger, table_name=table_name, endpoint_url=dynamodb_via_docker, connection_params=connection_params
        )
    finally:
        resource.meta.client.delete_table(TableName=table_name)


def test_type_index_mismatch(memory_with_bad_type_index: DynamoDbMemory):
    problems = memory_with_bad_type_index.validate_gsi_config(DynamoDbResource, raise_on_error=False)
    assert _kinds(problems) == ["type_index_mismatch"]


def test_raises_with_every_problem_across_classes(dynamodb_memory: DynamoDbMemory):
    with pytest.raises(GsiConfigError) as exc_info:
        dynamodb_memory.validate_gsi_config(
            ExplicitThing,
            MisspelledIndexThing,
            WrongSortThing,
            LabelMismatchThing,
            UnboundThing,
            HookConflictThing,
            TypeKeyedThing,
        )

    assert isinstance(exc_info.value, ValueError)
    assert sorted(_kinds(exc_info.value.problems)) == sorted(
        [
            "missing_index",
            "key_schema_mismatch",
            "label_mismatch",
            "unbound_attributes",
            "legacy_hook_conflict",
            "invalid_declaration",
        ]
    )
    assert "MisspelledIndexThing" in str(exc_info.value)


def test_returns_without_raising(dynamodb_memory: DynamoDbMemory):
    problems = dynamodb_memory.validate_gsi_config(UnboundThing, LabelMismatchThing, raise_on_error=False)
    assert len(problems) == 2


def test_requires_a_class(dynamodb_memory: DynamoDbMemory):
    with pytest.raises(TypeError):
        dynamodb_memory.validate_gsi_config()


def test_structural_errors_are_reported_not_short_circuited(dynamodb_memory: DynamoDbMemory):
    problems = dynamodb_memory.validate_gsi_config(
        TypeKeyedThing, HookConflictThing, ExplicitThing, raise_on_error=False
    )
    assert _kinds(problems) == ["invalid_declaration", "legacy_hook_conflict"]
    assert [p.resource_class for p in problems] == ["TypeKeyedThing", "HookConflictThing"]


def test_audit_log_is_checked_against_the_audit_table(
    memory_with_custom_index: DynamoDbMemory, local_dynamodb_test_table, dynamodb_via_docker
):
    """The main table lacks gsi1/gsi2/gsi3; only the (standard) audit table can make AuditLog pass."""
    memory = DynamoDbMemory(
        logger=logger,
        table_name=memory_with_custom_index.table_name,
        endpoint_url=dynamodb_via_docker,
        connection_params=memory_with_custom_index.connection_params,
        audit_table_name=local_dynamodb_test_table.table_name,
    )
    assert memory.validate_gsi_config(AuditLog) == []
    assert _kinds(memory.validate_gsi_config(ExplicitThing, raise_on_error=False)) == ["missing_index"] * 3
