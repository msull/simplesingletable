"""Explicit GSI declarations.

A ``gsi_config`` entry can be a :class:`GsiIndex`, keyed by the deployed index name, which states
the index's partition and sort attributes outright. Everything about such an entry -- which
attributes it writes, which values, whether the resource is in the index at all, and which
attributes make up its pagination key -- comes from the declaration, with no inference from
index names or attribute naming conventions.

The dict form (``{"label": {"gsi1pk": fn}}``) and the ``db_get_gsi1pk`` / ``db_get_gsi2pk`` /
``db_get_gsi3pk_and_sk`` hooks are still accepted, unchanged, but are deprecated: they bind
attributes to indexes by inference.

This module must not import ``models`` (models imports it).
"""

import os
import sys
import warnings
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict

from .exceptions import GsiConfigError

TABLE_KEY_ATTRIBUTES = frozenset({"pk", "sk"})
"""The table's own key attributes, which an index may range on but the library never writes as index values."""

RESERVED_ATTRIBUTES = frozenset({"pk", "sk", "gsitype", "gsitypesk"})
"""Attributes a ``GsiIndex`` may not write: the table keys and the built-in type index's keys."""

TYPE_INDEX_NAME = "gsitype"
"""The built-in type index. It is written for every resource and cannot be declared."""

LEGACY_GSI_HOOK_ATTRIBUTES: dict[str, tuple[str, ...]] = {
    "db_get_gsi1pk": ("gsi1pk",),
    "db_get_gsi2pk": ("gsi2pk",),
    "db_get_gsi3pk_and_sk": ("gsi3pk", "gsi3sk"),
}
"""The attributes each deprecated GSI hook writes."""

LEGACY_GSI_HOOK_INDEXES: dict[str, str] = {
    "db_get_gsi1pk": "gsi1",
    "db_get_gsi2pk": "gsi2",
    "db_get_gsi3pk_and_sk": "gsi3",
}
"""The index each deprecated GSI hook is bound to by convention."""

GsiKeyValue = tuple[str, Callable[[Any], Any]]
"""An ``(attribute name, value function)`` pair; the function receives the resource and returns the value."""

GsiProblemKind = Literal[
    "invalid_declaration",
    "legacy_hook_conflict",
    "missing_index",
    "key_schema_mismatch",
    "label_mismatch",
    "unbound_attributes",
    "legacy_hook_unbound",
    "type_index_mismatch",
]
"""What a :class:`GsiConfigProblem` is about.

Structural, from the declarations alone (``resolve_gsi_config``):

- ``invalid_declaration``: a ``GsiIndex`` is keyed badly or shares an attribute with another entry.
- ``legacy_hook_conflict``: an overridden ``db_get_gsi*`` hook writes an attribute a ``GsiIndex`` declares.

From comparing with the table (``DynamoDbMemory.validate_gsi_config``):

- ``missing_index``: a ``GsiIndex`` names an index the table does not have.
- ``key_schema_mismatch``: a ``GsiIndex``'s attributes differ from the index's key schema.
- ``label_mismatch``: a dict entry's label names an index whose keys differ from the entry's attributes.
- ``unbound_attributes``: a dict entry's attributes are the keys of no index on the table.
- ``legacy_hook_unbound``: an overridden hook's attributes are the keys of no index on the table.
- ``type_index_mismatch``: the table's ``gsitype`` index is not keyed ``gsitype`` / ``gsitypesk``.
"""


class GsiConfigProblem(BaseModel):
    """One mismatch between a resource's GSI declarations and the table (or within the declarations)."""

    model_config = ConfigDict(frozen=True)

    resource_class: str
    """The resource class's ``__qualname__``."""

    index_name: str
    """The index name, dict label, or legacy hook name being checked."""

    kind: GsiProblemKind
    """What the problem is about; see :data:`GsiProblemKind`. Match on this rather than on ``message``."""

    message: str
    """A human-readable description of the problem."""


def _is_absent(value: Any) -> bool:
    """Whether a value function's result means "not in this index".

    ``""`` is absent because DynamoDB rejects an empty string as an index key value, and
    ``False`` because a BOOL is never a valid key value (``self.is_active and f"..."``). ``False`` is
    tested by identity so that ``0`` (which ``== False``) stays present: it is a valid ``N`` key value.
    """
    return value is None or value is False or (isinstance(value, str) and value == "")


def _is_key_value(value: Any) -> bool:
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], str)
        and bool(value[0])
        and callable(value[1])
    )


@dataclass(frozen=True)
class GsiIndex:
    """An explicitly declared GSI. The ``gsi_config`` dict key holding it is the deployed index name.

    A frozen dataclass rather than a Pydantic model: it holds value functions, which Pydantic can
    neither validate nor serialise, and it is a class-level declaration, never a returned value.

    pk: (attribute name, value function). The function receives the resource and returns the
        partition value, or None / "" / False when the resource is not in this (sparse) index.
    sk: (attribute name, value function) for a sort attribute the library writes;
        "pk" or "sk" when the index ranges on one of the table's own key attributes;
        None when the index has a hash key only.
    description: free text naming the access pattern (what dict labels such as "by-owner" used to carry).

    The entry is written all-or-nothing: when any function-backed value is None, "" or False, none of
    its attributes are written (and a transaction with ``recompute_gsis=True`` REMOVEs all of them).
    """

    pk: GsiKeyValue
    sk: GsiKeyValue | str | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        if not _is_key_value(self.pk):
            raise TypeError(f"GsiIndex pk must be a (attribute name, value function) tuple, got {self.pk!r}")
        if not (self.sk is None or isinstance(self.sk, str) or _is_key_value(self.sk)):
            raise TypeError(
                f'GsiIndex sk must be None, "pk", "sk" or a (attribute name, value function) tuple, got {self.sk!r}'
            )
        if self.description is not None and not isinstance(self.description, str):
            raise TypeError(f"GsiIndex description must be a str, got {self.description!r}")

        if isinstance(self.sk, str) and self.sk not in TABLE_KEY_ATTRIBUTES:
            raise GsiConfigError(
                f'GsiIndex sk={self.sk!r} is not a table key attribute; use "pk" or "sk" for an index that ranges '
                f"on the table's own key, or ({self.sk!r}, value function) for a sort attribute the library writes"
            )
        for attribute in self.attributes:
            if attribute in RESERVED_ATTRIBUTES:
                raise GsiConfigError(f"GsiIndex may not write the reserved attribute {attribute!r}")
        if self.pk_attribute == self.sk_attribute:
            raise GsiConfigError(f"GsiIndex pk and sk name the same attribute {self.pk_attribute!r}")

    @classmethod
    def from_pair(
        cls,
        pk_attribute: str,
        sk_attribute: str,
        fn: Callable[[Any], tuple[Any, Any] | None],
        *,
        description: str | None = None,
    ) -> "GsiIndex":
        """Build an entry from one function returning ``(pk, sk)``, or None when the resource is not indexed.

        This is the migration path for the dict form's tuple keys (``{("gsi3pk", "gsi3sk"): fn}``).
        ``fn`` is called once per key, so it must be pure.
        """
        return cls(
            pk=(pk_attribute, lambda resource: (fn(resource) or (None, None))[0]),
            sk=(sk_attribute, lambda resource: (fn(resource) or (None, None))[1]),
            description=description,
        )

    @property
    def pk_attribute(self) -> str:
        """The index's HASH attribute name."""
        return self.pk[0]

    @property
    def sk_attribute(self) -> str | None:
        """The index's RANGE attribute name: the declared one, "pk"/"sk", or None."""
        if isinstance(self.sk, tuple):
            return self.sk[0]
        return self.sk

    @property
    def attributes(self) -> tuple[str, ...]:
        """Attributes the library writes for this index (pk attribute, plus sk attribute when function-backed).

        Never includes "pk"/"sk".
        """
        if isinstance(self.sk, tuple):
            return (self.pk[0], self.sk[0])
        return (self.pk[0],)

    @property
    def key_attributes(self) -> tuple[str, ...]:
        """The index's key schema attributes, in (HASH, RANGE) order."""
        if self.sk_attribute is None:
            return (self.pk_attribute,)
        return (self.pk_attribute, self.sk_attribute)

    def compute(self, resource: Any) -> dict[str, Any] | None:
        """Values to write for ``resource``, keyed by attribute, or None when the resource is absent from the index."""
        pk_value = self.pk[1](resource)
        if _is_absent(pk_value):
            return None
        if isinstance(self.sk, tuple):
            sk_value = self.sk[1](resource)
            if _is_absent(sk_value):
                return None
            return {self.pk[0]: pk_value, self.sk[0]: sk_value}
        return {self.pk[0]: pk_value}


class ResolvedGsiConfig(NamedTuple):
    """A resource class's ``get_gsi_config()`` split into explicit declarations and deprecated dict entries."""

    explicit: dict[str, GsiIndex]
    """Index name -> declaration."""

    legacy: dict[Any, dict]
    """Label -> dict-form entry, unchanged."""


_WARNED: "weakref.WeakKeyDictionary[type, set[str]]" = weakref.WeakKeyDictionary()
"""Per class, the deprecation warnings already emitted, so each warns once."""

_WARNED_LEGACY_HOOK_FUNCTIONS: "weakref.WeakSet[Callable]" = weakref.WeakSet()
"""Legacy hook overrides already warned about, so each overriding function warns once."""


def _first_warning(resource_class: type, key: str) -> bool:
    """Record ``key`` for ``resource_class``; True the first time only."""
    seen = _WARNED.setdefault(resource_class, set())
    if key in seen:
        return False
    seen.add(key)
    return True


def dict_entry_attributes(fields: dict) -> list[str]:
    """The attribute names a dict-form entry writes, flattening tuple-declared pk/sk pairs."""
    attributes = []
    for key in fields:
        # Combined definitions declare their pair as a tuple, e.g. ("gsi3pk", "gsi3sk")
        if isinstance(key, tuple):
            attributes.extend(key)
        else:
            attributes.append(key)
    return attributes


def resolve_gsi_config(resource_class: type) -> ResolvedGsiConfig:
    """Split ``resource_class.get_gsi_config()`` into explicit ``GsiIndex`` entries and legacy dict entries.

    Raises :class:`GsiConfigError` listing every structural problem: a ``GsiIndex`` keyed by
    something other than a non-empty index name or by ``"gsitype"``, an attribute written by a
    ``GsiIndex`` and another entry, or an attribute written by a ``GsiIndex`` and an overridden
    legacy hook (which runs last and would silently overwrite it).

    Emits no warnings: callers that use the dict entries call :func:`warn_legacy_entries`.
    """
    resolved = ResolvedGsiConfig(explicit={}, legacy={})
    qualname = resource_class.__qualname__
    problems: list[GsiConfigProblem] = []

    def problem(index_name: Any, kind: GsiProblemKind, message: str) -> None:
        problems.append(
            GsiConfigProblem(resource_class=qualname, index_name=str(index_name), kind=kind, message=message)
        )

    for name, value in resource_class.get_gsi_config().items():
        if not isinstance(value, GsiIndex):
            resolved.legacy[name] = value
            continue
        if not isinstance(name, str) or not name:
            problem(name, "invalid_declaration", f"GsiIndex must be keyed by its deployed index name, got {name!r}")
            continue
        if name == TYPE_INDEX_NAME:
            problem(
                name,
                "invalid_declaration",
                f"GsiIndex may not be keyed {TYPE_INDEX_NAME!r}: the type index is built in; "
                "customise it with db_get_gsitypepk / db_get_gsitypesk",
            )
            continue
        resolved.explicit[name] = value

    # Which entries write each attribute, so a GsiIndex sharing one with any other entry is caught.
    writers: dict[str, list[str]] = {}
    for name, index in resolved.explicit.items():
        for attribute in index.attributes:
            writers.setdefault(attribute, []).append(repr(name))
    for label, fields in resolved.legacy.items():
        for attribute in dict_entry_attributes(fields):
            writers.setdefault(attribute, []).append(repr(label))
    for name, index in resolved.explicit.items():
        for attribute in index.attributes:
            others = [w for w in writers[attribute] if w != repr(name)]
            if others:
                problem(
                    name,
                    "invalid_declaration",
                    f"GsiIndex {name!r} writes attribute {attribute!r}, which entry {', '.join(others)} also writes",
                )

    hooks_fn = getattr(resource_class, "_overridden_legacy_gsi_hooks", None)
    if hooks_fn is not None:
        for hook in hooks_fn():
            for name, index in resolved.explicit.items():
                for attribute in index.attributes:
                    if attribute in LEGACY_GSI_HOOK_ATTRIBUTES[hook]:
                        problem(
                            hook,
                            "legacy_hook_conflict",
                            f"{hook} is overridden and writes attribute {attribute!r}, which GsiIndex {name!r} "
                            f"also declares; the hook would overwrite it. Delete the {hook} override.",
                        )

    if problems:
        raise GsiConfigError(
            f"Invalid GSI configuration on {qualname}:\n" + "\n".join(f"- {p.message}" for p in problems), problems
        )

    return resolved


_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__)) + os.sep


def _warn_at_caller(message: str) -> None:
    """Emit a DeprecationWarning attributed to the nearest frame outside this package.

    Library call depth varies by entry point (a write, a read, a transaction, a paginated query), so a
    fixed ``stacklevel`` would land on library code; this attributes it to the user's call instead.
    """
    stacklevel = 2  # sys._getframe(1), our caller, is stacklevel 2 to warnings.warn
    frame = sys._getframe(1)
    while frame is not None and os.path.abspath(frame.f_code.co_filename).startswith(_PACKAGE_DIR):
        frame = frame.f_back
        stacklevel += 1
    warnings.warn(message, DeprecationWarning, stacklevel=stacklevel)


def warn_legacy_entries(resource_class: type, resolved: ResolvedGsiConfig) -> None:
    """Warn, once per (class, label), that ``resolved`` contains deprecated dict-form entries."""
    for label in resolved.legacy:
        if _first_warning(resource_class, f"entry:{label!r}"):
            _warn_at_caller(
                f"{resource_class.__qualname__}.gsi_config entry {label!r} uses the dict form, whose index and "
                "partition/sort keys are inferred. Declare it as GsiIndex(pk=(...), sk=(...)) keyed by the deployed "
                "index name. The dict form will be removed in the next major release."
            )


def legacy_view(resolved: ResolvedGsiConfig) -> dict[Any, dict]:
    """The config as the deprecated LEK heuristic expects it: explicit entries shown as equivalent dict entries."""
    view = dict(resolved.legacy)
    for name, index in resolved.explicit.items():
        view[name] = {attribute: None for attribute in index.attributes}
    return view


def warn_inferred_index(resource_class: type, index_name: str) -> None:
    """Warn, once per (class, index), that a pagination key is being inferred for an undeclared index."""
    if _first_warning(resource_class, f"index:{index_name}"):
        _warn_at_caller(
            f"{resource_class.__qualname__} declares no GsiIndex for index {index_name!r}; its pagination key is "
            "inferred from attribute naming conventions. Declare it with GsiIndex."
        )


def warn_legacy_hook(resource_class: type, hook: str) -> None:
    """Warn that ``resource_class`` picks up an override of a deprecated GSI hook, once per overriding function."""
    fn = getattr(resource_class, hook)
    fn = getattr(fn, "__func__", fn)
    if fn in _WARNED_LEGACY_HOOK_FUNCTIONS:
        return
    _WARNED_LEGACY_HOOK_FUNCTIONS.add(fn)
    attributes = " and ".join(LEGACY_GSI_HOOK_ATTRIBUTES[hook])
    message = (
        f"{fn.__qualname__} (used by {resource_class.__qualname__}) overrides {hook}, which is deprecated: it binds "
        f"attribute {attributes} to index {LEGACY_GSI_HOOK_INDEXES[hook]!r} by convention. Declare a GsiIndex in "
        "gsi_config instead. Legacy GSI hooks will be removed in the next major release."
    )
    code = getattr(fn, "__code__", None)
    if code is None:
        warnings.warn(message, DeprecationWarning, stacklevel=2)
        return
    warnings.warn_explicit(
        message,
        DeprecationWarning,
        filename=code.co_filename,
        lineno=code.co_firstlineno,
        module=fn.__module__,
    )
