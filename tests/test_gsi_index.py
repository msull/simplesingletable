"""Unit tests for explicit GSI declarations (``GsiIndex``) and the deprecation of inferred bindings."""

import runpy
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest
from pydantic import BaseModel, ValidationError

from simplesingletable import (
    AuditLog,
    DynamoDbResource,
    GsiConfigError,
    GsiConfigProblem,
    GsiIndex,
)
from simplesingletable.dynamodb_memory import build_lek_data
from simplesingletable.extras.form_data import Form, FormEntry
from simplesingletable.gsi import resolve_gsi_config

EXAMPLES_DIR = Path(__file__).parent.parent / "examples"


def _value(*_args):
    return None


def _resource_with(gsi_config):
    return type("ObservedResource", (), {"get_gsi_config": classmethod(lambda cls: gsi_config)})


class _NoDeprecationWarnings:
    """Record every warning in the block, then assert none was a DeprecationWarning.

    pytest runs with ``-p no:warnings``, so recwarn / filterwarnings are unavailable.
    """

    def __enter__(self):
        self._ctx = warnings.catch_warnings(record=True)
        self.recorded = self._ctx.__enter__()
        warnings.simplefilter("always")
        return self

    def __exit__(self, *exc):
        self._ctx.__exit__(*exc)
        if exc[0] is None:
            deprecations = [str(w.message) for w in self.recorded if issubclass(w.category, DeprecationWarning)]
            assert not deprecations, deprecations


def _deprecations(recorded) -> list[str]:
    return [str(w.message) for w in recorded if issubclass(w.category, DeprecationWarning)]


class TestConstruction:
    def test_pk_only(self):
        index = GsiIndex(pk=("gsi1pk", _value))
        assert index.sk is None

    def test_ranges_on_table_pk(self):
        assert GsiIndex(pk=("gsi1pk", _value), sk="pk").sk_attribute == "pk"

    def test_tuple_sk(self):
        assert GsiIndex(pk=("gsi3pk", _value), sk=("gsi3sk", _value)).sk_attribute == "gsi3sk"

    def test_description(self):
        assert GsiIndex(pk=("gsi1pk", _value), description="by owner").description == "by owner"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"pk": "gsi1pk"},
            {"pk": ("gsi1pk", "not callable")},
            {"pk": ("gsi1pk", _value), "sk": 3},
            {"pk": ("gsi1pk", _value), "description": 3},
        ],
    )
    def test_type_errors(self, kwargs):
        with pytest.raises(TypeError):
            GsiIndex(**kwargs)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"pk": ("gsi1pk", _value), "sk": "gsi1sk"},
            {"pk": ("gsitype", _value)},
            {"pk": ("pk", _value)},
            {"pk": ("gsi3pk", _value), "sk": ("gsi3pk", _value)},
        ],
    )
    def test_config_errors(self, kwargs):
        with pytest.raises(GsiConfigError) as exc_info:
            GsiIndex(**kwargs)
        assert isinstance(exc_info.value, ValueError)


class TestCompute:
    @pytest.mark.parametrize("absent", [None, "", False])
    def test_absent_pk(self, absent):
        assert GsiIndex(pk=("a", lambda r: absent)).compute(SimpleNamespace()) is None

    def test_false_sk_with_pk_set(self):
        index = GsiIndex(pk=("a", lambda r: "x"), sk=("b", lambda r: r.is_active and "active"))
        assert index.compute(SimpleNamespace(is_active=False)) is None

    def test_none_sk_with_pk_set_is_all_or_nothing(self):
        assert GsiIndex(pk=("a", lambda r: "x"), sk=("b", lambda r: None)).compute(SimpleNamespace()) is None

    def test_zero_is_written(self):
        assert GsiIndex(pk=("a", lambda r: 0)).compute(SimpleNamespace()) == {"a": 0}

    def test_ranges_on_pk_writes_only_pk_attribute(self):
        assert GsiIndex(pk=("a", lambda r: "x"), sk="pk").compute(SimpleNamespace()) == {"a": "x"}

    def test_both_written(self):
        assert GsiIndex(pk=("a", lambda r: "x"), sk=("b", lambda r: "y")).compute(SimpleNamespace()) == {
            "a": "x",
            "b": "y",
        }

    @pytest.mark.parametrize(
        "sk,attributes,key_attributes",
        [
            (None, ("a",), ("a",)),
            ("pk", ("a",), ("a", "pk")),
            (("b", _value), ("a", "b"), ("a", "b")),
        ],
    )
    def test_attributes(self, sk, attributes, key_attributes):
        index = GsiIndex(pk=("a", _value), sk=sk)
        assert index.attributes == attributes
        assert index.key_attributes == key_attributes


class TestFromPair:
    def test_none_means_absent(self):
        assert GsiIndex.from_pair("a", "b", lambda r: None).compute(SimpleNamespace()) is None

    def test_pair_written(self):
        index = GsiIndex.from_pair("a", "b", lambda r: ("x", "y"), description="pair")
        assert index.compute(SimpleNamespace()) == {"a": "x", "b": "y"}
        assert index.description == "pair"


class GsiOneMixin(BaseModel):
    def db_get_gsi1pk(self) -> str | None:
        return "mixin"


class TestResolve:
    def test_splits_mixed_configs(self):
        explicit = GsiIndex(pk=("gsi1pk", _value), sk="pk")
        legacy = {"gsi3pk": _value, "gsi3sk": _value}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            resolved = resolve_gsi_config(_resource_with({"gsi1": explicit, "by-assignee": legacy}))
        assert resolved.explicit == {"gsi1": explicit}
        assert resolved.legacy == {"by-assignee": legacy}

    @pytest.mark.parametrize(
        "gsi_config",
        [
            {"gsitype": GsiIndex(pk=("gsi1pk", _value))},
            {"": GsiIndex(pk=("gsi1pk", _value))},
            {"gsi1": GsiIndex(pk=("gsi1pk", _value)), "gsi-1": GsiIndex(pk=("gsi1pk", _value))},
            {"gsi1": GsiIndex(pk=("gsi1pk", _value)), "by-owner": {"gsi1pk": _value}},
            {"gsi3": GsiIndex(pk=("gsi3pk", _value), sk=("gsi3sk", _value)), "pair": {("gsi3pk", "gsi3sk"): _value}},
        ],
        ids=["gsitype", "empty-key", "duplicate-explicit", "duplicate-dict", "duplicate-tuple-dict"],
    )
    def test_invalid_declarations(self, gsi_config):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pytest.raises(GsiConfigError) as exc_info:
                resolve_gsi_config(_resource_with(gsi_config))
        assert exc_info.value.problems[0].kind == "invalid_declaration"

    def test_legacy_hook_conflict(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            class Conflicting(GsiOneMixin, DynamoDbResource):
                gsi_config: ClassVar[dict] = {"gsi1": GsiIndex(pk=("gsi1pk", _value), sk="pk")}

            class NoConflict(GsiOneMixin, DynamoDbResource):
                gsi_config: ClassVar[dict] = {"gsi3": GsiIndex(pk=("gsi3pk", _value), sk=("gsi3sk", _value))}

        with pytest.raises(GsiConfigError) as exc_info:
            resolve_gsi_config(Conflicting)
        assert exc_info.value.problems[0].kind == "legacy_hook_conflict"
        assert "db_get_gsi1pk" in str(exc_info.value)
        assert "gsi1pk" in exc_info.value.problems[0].message

        resolve_gsi_config(NoConflict)

    def test_bare_stub_without_hook_introspection(self):
        resolved = resolve_gsi_config(_resource_with({"gsi1": GsiIndex(pk=("gsi1pk", _value), sk="pk")}))
        assert list(resolved.explicit) == ["gsi1"]


class TestGsiConfigProblem:
    def test_frozen(self):
        problem = GsiConfigProblem(resource_class="X", index_name="gsi1", kind="missing_index", message="m")
        with pytest.raises(ValidationError):
            problem.message = "changed"

    def test_kind_is_closed(self):
        with pytest.raises(ValidationError):
            GsiConfigProblem(resource_class="X", index_name="gsi1", kind="nope", message="m")


class TestDeprecationWarnings:
    def test_hook_override_warns_at_class_definition_once(self):
        with pytest.warns(DeprecationWarning, match=r"HookedThing.*'gsi1'"):

            class HookedThing(DynamoDbResource):
                def db_get_gsi1pk(self) -> str | None:
                    return "x"

        with _NoDeprecationWarnings():

            class HookedThingChild(HookedThing):
                pass

    def test_mixin_hook_warns_once_per_function(self):
        class GsiTwoMixin(BaseModel):
            def db_get_gsi2pk(self) -> str | None:
                return "x"

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")

            class A(GsiTwoMixin, DynamoDbResource):
                pass

        messages = _deprecations(recorded)
        assert len(messages) == 1
        assert "GsiTwoMixin.db_get_gsi2pk" in messages[0]
        assert f"(used by {A.__qualname__})" in messages[0]

        with _NoDeprecationWarnings():

            class B(GsiTwoMixin, DynamoDbResource):
                pass

            class C(A):
                pass

    def test_dict_entry_warns_on_first_use_only(self):
        class DictThing(DynamoDbResource):
            owner: str
            gsi_config: ClassVar[dict] = {"gsi1": {"gsi1pk": lambda self: self.owner}}

        thing = DictThing.create_new({"owner": "o"})
        with pytest.warns(DeprecationWarning, match="DictThing.gsi_config entry 'gsi1' uses the dict form"):
            thing.to_dynamodb_item()
        with _NoDeprecationWarnings():
            thing.to_dynamodb_item()

    def test_explicit_class_never_warns(self):
        with _NoDeprecationWarnings():

            class ExplicitOnly(DynamoDbResource):
                owner: str
                gsi_config: ClassVar[dict] = {"gsi1": GsiIndex(pk=("gsi1pk", lambda self: self.owner), sk="pk")}

            ExplicitOnly.create_new({"owner": "o"}).to_dynamodb_item()

    def test_library_resources_do_not_warn(self):
        with _NoDeprecationWarnings():
            AuditLog.create_new(
                {"audited_resource_type": "T", "audited_resource_id": "1", "operation": "CREATE", "changed_by": "u"}
            ).to_dynamodb_item()
            Form.create_new(
                {
                    "name": "f",
                    "category": "c",
                    "form_data_type_id": "t",
                    "form_data_type_version": 1,
                    "form_data_type_schema": [],
                    "columns": ["c"],
                    "groups": ["g"],
                }
            ).to_dynamodb_item()
            FormEntry.create_new(
                {"col_idx": 0, "row_identifier": "r", "group_identifier": "g", "data": {}, "form_id": "f"}
            ).to_dynamodb_item()

    @pytest.mark.parametrize("script", ["local_storage_example.py", "blob_storage_example.py"])
    def test_examples_do_not_warn(self, script):
        with _NoDeprecationWarnings():
            runpy.run_path(str(EXAMPLES_DIR / script), run_name="not_main")

    def test_inferred_pagination_key_warns_once(self):
        resource = _resource_with({})
        item = {"pk": "p", "sk": "s", "gsi1pk": "x"}
        with pytest.warns(DeprecationWarning, match="declares no GsiIndex for index 'gsi1'"):
            build_lek_data(item, "gsi1", resource)
        with _NoDeprecationWarnings():
            build_lek_data(item, "gsi1", resource)

    def test_declared_pagination_key_does_not_warn(self):
        resource = _resource_with({"gsi1": GsiIndex(pk=("gsi1pk", _value), sk="pk")})
        with _NoDeprecationWarnings():
            build_lek_data({"pk": "p", "sk": "s", "gsi1pk": "x"}, "gsi1", resource)


ITEM = {
    "pk": "ObservedResource#01ABC",
    "sk": "v0",
    "gsitype": "ObservedResource",
    "gsitypesk": "2026-01-01T00:00:00",
    "gsi1pk": "<gsi1pk>",
    "gsi1sk": "<gsi1sk>",
    "ownerKey": "<ownerKey>",
    "createdKey": "<createdKey>",
}


def _lek(*attributes):
    return {"pk": ITEM["pk"], "sk": ITEM["sk"], **{a: ITEM[a] for a in attributes}}


class TestBuildLekData:
    def test_custom_attributes_behind_descriptive_description(self):
        """The case the inference path cannot resolve: neither name nor attributes follow a convention."""
        resource = _resource_with(
            {"owner-index": GsiIndex(pk=("ownerKey", _value), sk=("createdKey", _value), description="by owner")}
        )
        assert build_lek_data(ITEM, "owner-index", resource) == _lek("ownerKey", "createdKey")

    def test_ranges_on_pk(self):
        resource = _resource_with({"gsi1": GsiIndex(pk=("gsi1pk", _value), sk="pk")})
        assert build_lek_data(ITEM, "gsi1", resource) == _lek("gsi1pk")

    def test_pk_only(self):
        resource = _resource_with({"gsi1": GsiIndex(pk=("gsi1pk", _value))})
        assert build_lek_data(ITEM, "gsi1", resource) == _lek("gsi1pk")

    def test_exact_match_only(self):
        """An explicit ``gsi-1`` entry does not answer for ``gsi1``: the query falls through to inference."""
        resource = _resource_with({"gsi-1": GsiIndex(pk=("gsi1pk", _value), sk=("gsi1sk", _value))})
        with pytest.warns(DeprecationWarning, match="declares no GsiIndex"):
            lek = build_lek_data(ITEM, "gsi1", resource)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            expected = build_lek_data(ITEM, "gsi1", _resource_with({"gsi-1": {("gsi1pk", "gsi1sk"): _value}}))
        assert lek == expected

    def test_item_missing_declared_attribute(self):
        resource = _resource_with({"owner-index": GsiIndex(pk=("ownerKey", _value), sk=("createdKey", _value))})
        with pytest.raises(RuntimeError, match="ownerKey"):
            build_lek_data({"pk": "p", "sk": "s"}, "owner-index", resource)

    def test_gsitype_unchanged(self):
        resource = _resource_with({"gsi1": GsiIndex(pk=("gsi1pk", _value), sk="pk")})
        assert build_lek_data(ITEM, "gsitype", resource) == _lek("gsitype", "gsitypesk")
