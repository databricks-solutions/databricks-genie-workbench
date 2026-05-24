"""Trial 14 — :class:`BlameEntry` dataclass + :data:`BLAME_KINDS`
vocabulary contract.

Locks the wire surface every Trial 14 + future caller relies on:

* ``BLAME_KINDS`` is frozen and contains exactly the five tagged
  kinds (column, table, join, filter, instruction).
* ``BlameEntry`` is frozen + slotted + ``JsonRoundTrip``.
* Schema-resolvable kinds (column/table/join) require a non-empty
  ``ref``; constructor raises ``ValueError`` otherwise.
* Filter/instruction kinds accept ``ref=None`` so prose can survive
  with the original text in ``description``.
* ``to_dict`` matches ``to_json`` shape (single canonical form for
  the writer paths in ``build_asi_metadata`` and the workbench
  capture serializer).
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.blame_entry import (
    BLAME_KINDS,
    SCHEMA_RESOLVABLE_KINDS,
    BlameEntry,
    kinds_distribution,
)


def test_blame_kinds_vocabulary_is_frozen_and_exact() -> None:
    expected = {"column", "table", "join", "filter", "instruction"}
    assert BLAME_KINDS == expected
    assert isinstance(BLAME_KINDS, frozenset)
    assert SCHEMA_RESOLVABLE_KINDS == {"column", "table", "join"}
    assert SCHEMA_RESOLVABLE_KINDS.issubset(BLAME_KINDS)


def test_blame_entry_round_trips_via_json() -> None:
    entry = BlameEntry(
        kind="column",
        ref="cat.sch.tbl.col",
        description="missing GROUP BY dimension",
    )
    payload = entry.to_json()
    restored = BlameEntry.from_json(payload)
    assert restored == entry
    assert restored.to_dict() == entry.to_dict()
    assert restored.to_dict() == {
        "kind": "column",
        "ref": "cat.sch.tbl.col",
        "description": "missing GROUP BY dimension",
    }


def test_blame_entry_is_frozen() -> None:
    entry = BlameEntry(kind="column", ref="a.b.c.d")
    with pytest.raises((AttributeError, TypeError)):
        entry.kind = "table"  # type: ignore[misc]


def test_schema_resolvable_kind_requires_non_empty_ref() -> None:
    for kind in ("column", "table", "join"):
        with pytest.raises(ValueError):
            BlameEntry(kind=kind, ref=None)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            BlameEntry(kind=kind, ref="   ")  # type: ignore[arg-type]


def test_filter_instruction_kinds_accept_ref_none() -> None:
    f = BlameEntry(
        kind="filter", ref=None, description="col = 'USD'"
    )
    i = BlameEntry(
        kind="instruction", ref=None, description="prefer metric view"
    )
    assert f.kind == "filter"
    assert f.ref is None
    assert i.kind == "instruction"
    assert i.ref is None
    assert not f.is_schema_resolvable()
    assert not i.is_schema_resolvable()


def test_schema_resolvable_predicate() -> None:
    assert BlameEntry(kind="column", ref="a.b.c.d").is_schema_resolvable()
    assert BlameEntry(kind="table", ref="a.b.c").is_schema_resolvable()
    assert BlameEntry(kind="join", ref="a.b.c.d=e.f.g.h").is_schema_resolvable()
    assert not BlameEntry(
        kind="filter", ref=None, description="x = 1"
    ).is_schema_resolvable()
    assert not BlameEntry(
        kind="instruction", ref=None, description="rule"
    ).is_schema_resolvable()


def test_unknown_kind_is_rejected_at_construction() -> None:
    """Defensive guard: even internal callers must not slip an unknown
    kind past construction. The coercer is responsible for
    collapsing unknown vocab onto ``instruction``.
    """
    with pytest.raises(ValueError):
        BlameEntry(kind="bogus", ref="a.b.c.d")  # type: ignore[arg-type]


def test_kinds_distribution_histogram() -> None:
    entries = [
        BlameEntry(kind="column", ref="a.b.c.d"),
        BlameEntry(kind="column", ref="a.b.c.e"),
        BlameEntry(kind="filter", ref=None, description="x = 1"),
    ]
    assert kinds_distribution(entries) == {"column": 2, "filter": 1}
    assert kinds_distribution([]) == {}
    assert kinds_distribution(tuple(entries)) == {"column": 2, "filter": 1}
