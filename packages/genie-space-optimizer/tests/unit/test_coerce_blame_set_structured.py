"""Trial 14 — ``coerce_blame_entries`` covers the production-wild
shapes catalogued by Trial 13k.

The headline Trial 13k canary was that production ASI judges emit
``blame_set`` as free-text that the FQN normalizer drops. Trial 14
promotes the field to a typed list of ``BlameEntry``; the coercer
is the funnel that takes any of the wild input shapes and produces
a clean ``list[BlameEntry]`` ready for storage and Stage 1
consumption.

Coverage anchors:

* Production-wild legacy ``list[str]`` shapes from Trial 13k —
  ``["LIMIT 10 vs RANK() <= 10"]``,
  ``["PAYMENT_CURRENCY_CD = 'USD' filter incorrectly added"]``,
  ``["time_window = 'mtd'"]``, ``["zone_vp_name", "RANK filtering"]``.
* Valid ``list[dict]`` from a well-behaved judge.
* JSON-encoded string from the flat-metadata path.
* Mixed list (dict + string + None + int).
* Unknown ``kind`` from a drifted judge → collapses to ``instruction``.

The classifier is intentionally permissive — the worst case is
misrouting prose to ``filter`` / ``instruction``, both of which
Stage 1 handles as non-schema blame.
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.blame_entry import (
    BlameEntry,
    coerce_blame_entries,
    legacy_blame_set_from_entries,
)


def _kinds(entries: list[BlameEntry]) -> list[str]:
    return [e.kind for e in entries]


# ── Legacy list[str] — Trial 13k production-wild shapes ──────────────


def test_legacy_sql_fragment_routes_to_filter() -> None:
    entries = coerce_blame_entries(
        legacy_strings=["LIMIT 10 vs RANK() <= 10"],
    )
    assert len(entries) == 1
    assert entries[0].kind == "filter"
    assert entries[0].ref is None
    assert entries[0].description == "LIMIT 10 vs RANK() <= 10"


def test_legacy_predicate_with_equals_routes_to_filter() -> None:
    entries = coerce_blame_entries(
        legacy_strings=["PAYMENT_CURRENCY_CD = 'USD' filter incorrectly added"],
    )
    assert _kinds(entries) == ["filter"]
    assert entries[0].description == (
        "PAYMENT_CURRENCY_CD = 'USD' filter incorrectly added"
    )


def test_legacy_time_window_predicate_routes_to_filter() -> None:
    entries = coerce_blame_entries(legacy_strings=["time_window = 'mtd'"])
    assert _kinds(entries) == ["filter"]


def test_legacy_mixed_bare_and_prose_separates_routing() -> None:
    """``["zone_vp_name", "RANK filtering"]`` — bare ident -> column,
    "RANK filtering" contains the ``RANK`` SQL keyword and routes to
    ``filter`` (the plan's predicted Trial 14 outcome for dc89/gs_026).
    """
    entries = coerce_blame_entries(legacy_strings=["zone_vp_name", "RANK filtering"])
    assert _kinds(entries) == ["column", "filter"]
    assert entries[0].ref == "zone_vp_name"
    assert entries[1].description == "RANK filtering"


def test_legacy_plain_english_prose_routes_to_instruction() -> None:
    """Free-text without predicate-shaped tokens or SQL keywords
    routes to ``instruction`` (the catch-all)."""
    entries = coerce_blame_entries(legacy_strings=["missing user intent clarity"])
    assert _kinds(entries) == ["instruction"]
    assert entries[0].description == "missing user intent clarity"


def test_legacy_bare_identifier_routes_to_column() -> None:
    entries = coerce_blame_entries(legacy_strings=["DEST_AIRPORT_CD"])
    assert _kinds(entries) == ["column"]
    assert entries[0].ref == "DEST_AIRPORT_CD"


def test_legacy_4_part_fqn_routes_to_column_with_ref() -> None:
    entries = coerce_blame_entries(legacy_strings=["main.airline.fact_flights.dest_airport_cd"])
    assert _kinds(entries) == ["column"]
    assert entries[0].ref == "main.airline.fact_flights.dest_airport_cd"


def test_legacy_3_part_fqn_routes_to_table() -> None:
    entries = coerce_blame_entries(legacy_strings=["main.airline.fact_flights"])
    assert _kinds(entries) == ["table"]
    assert entries[0].ref == "main.airline.fact_flights"


# ── Structured list[dict] — trust path ────────────────────────────────


def test_valid_list_of_dicts_is_trusted() -> None:
    entries = coerce_blame_entries(
        raw_structured=[
            {"kind": "column", "ref": "a.b.c.d", "description": "missing"},
            {"kind": "filter", "ref": None, "description": "x = 1"},
            {"kind": "instruction", "ref": None, "description": "prefer mv"},
        ],
    )
    assert _kinds(entries) == ["column", "filter", "instruction"]
    assert entries[0].ref == "a.b.c.d"
    assert entries[1].description == "x = 1"


def test_dict_with_unknown_kind_collapses_to_instruction() -> None:
    entries = coerce_blame_entries(
        raw_structured=[{"kind": "schema_mismatch", "ref": None, "description": "weird"}],
    )
    assert _kinds(entries) == ["instruction"]
    assert entries[0].description == "weird"


def test_dict_with_schema_kind_missing_ref_demotes_to_instruction() -> None:
    """A judge may emit ``{kind: column, ref: None}``. We must not
    drop the entry — preserve the description on an
    ``instruction`` entry instead.
    """
    entries = coerce_blame_entries(
        raw_structured=[{"kind": "column", "ref": None, "description": "missing"}],
    )
    assert _kinds(entries) == ["instruction"]
    assert entries[0].description == "missing"


def test_dict_with_schema_kind_missing_ref_and_desc_is_dropped() -> None:
    entries = coerce_blame_entries(
        raw_structured=[{"kind": "column", "ref": None}],
    )
    assert entries == []


# ── JSON-encoded string (flat-metadata path) ────────────────────────


def test_json_string_of_list_of_dicts_is_parsed() -> None:
    payload = json.dumps(
        [
            {"kind": "column", "ref": "a.b.c.d", "description": None},
            {"kind": "filter", "ref": None, "description": "y > 0"},
        ]
    )
    entries = coerce_blame_entries(raw_structured=payload)
    assert _kinds(entries) == ["column", "filter"]


def test_invalid_json_string_falls_back_to_legacy() -> None:
    entries = coerce_blame_entries(
        raw_structured="this is not json",
        legacy_strings=["fallback_col"],
    )
    assert _kinds(entries) == ["column"]
    assert entries[0].ref == "fallback_col"


# ── Mixed shapes ────────────────────────────────────────────────────


def test_mixed_list_of_dicts_and_strings() -> None:
    entries = coerce_blame_entries(
        raw_structured=[
            {"kind": "column", "ref": "a.b.c.d"},
            "bare_col",
            None,
            42,
            "LIMIT 10 vs RANK()",
        ],
    )
    assert _kinds(entries) == ["column", "column", "filter"]
    assert entries[0].ref == "a.b.c.d"
    assert entries[1].ref == "bare_col"


def test_structured_wins_over_legacy_when_both_present() -> None:
    entries = coerce_blame_entries(
        raw_structured=[{"kind": "column", "ref": "a.b.c.d"}],
        legacy_strings=["legacy_token"],
    )
    assert _kinds(entries) == ["column"]
    assert entries[0].ref == "a.b.c.d"


def test_empty_inputs_return_empty_list() -> None:
    assert coerce_blame_entries() == []
    assert coerce_blame_entries(raw_structured=[]) == []
    assert coerce_blame_entries(raw_structured=None, legacy_strings=[]) == []
    assert coerce_blame_entries(raw_structured="", legacy_strings=None) == []


def test_deduplication_preserves_first_occurrence() -> None:
    entries = coerce_blame_entries(
        legacy_strings=["col_a", "col_a", "col_a"],
    )
    assert _kinds(entries) == ["column"]
    assert entries[0].ref == "col_a"


# ── legacy_blame_set_from_entries projection ────────────────────────


def test_legacy_mirror_emits_only_schema_resolvable_refs() -> None:
    entries = [
        BlameEntry(kind="column", ref="a.b.c.d"),
        BlameEntry(kind="filter", ref=None, description="x = 1"),
        BlameEntry(kind="table", ref="a.b.c"),
        BlameEntry(kind="instruction", ref=None, description="rule"),
        BlameEntry(kind="join", ref="a.b.c.d=e.f.g.h"),
    ]
    assert legacy_blame_set_from_entries(entries) == [
        "a.b.c.d",
        "a.b.c",
        "a.b.c.d=e.f.g.h",
    ]


def test_legacy_mirror_is_empty_for_all_filter_kind() -> None:
    entries = [
        BlameEntry(kind="filter", ref=None, description="x = 1"),
        BlameEntry(kind="instruction", ref=None, description="rule"),
    ]
    assert legacy_blame_set_from_entries(entries) == []
