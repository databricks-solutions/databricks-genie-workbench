"""Trial 14 — Stage 1 hydration prefers structured ASI blame over
the legacy free-text path.

Locks the reader-migration contract introduced in Phase D:

* ``_collect_blame_entries_from_asi`` returns typed entries from
  both the nested ``<judge>/metadata.blame_set_structured`` and the
  flat ``metadata/<judge>/blame_set_structured`` surfaces.
* ``_collect_blame_set_from_asi`` prefers structured entries (FQN
  ``ref`` only from kind in {column, table, join}); falls back to
  the legacy free-text path when no structured entries exist.
* ``build_stage1_evidence_card`` stamps ``_blame_structured`` on
  the card so the contract validator and marker can see WHY a seed
  list is empty (all-filter-kind vs all-dropped).
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.eval_row_access import (
    _collect_blame_entries_from_asi,
    _collect_blame_set_from_asi,
    build_stage1_evidence_card,
)


def _row_with_structured_nested(blame_structured: list[dict]) -> dict:
    """Build a minimal row that exercises the nested ``<judge>/metadata``
    surface."""
    return {
        "request": {
            "question": "What is total revenue by region?",
            "kwargs": {"question_id": "test_qid_001"},
        },
        "response": {"response": "SELECT region, SUM(revenue) FROM sales"},
        "expected_response": {"value": "SELECT region, SUM(revenue) FROM sales"},
        "schema_accuracy/metadata": {
            "blame_set": ["fallback_col"],
            "blame_set_structured": blame_structured,
            "failure_type": "wrong_column",
        },
        "schema_accuracy/rationale": "Schema mismatch detected",
    }


def _row_with_structured_flat(blame_structured: list[dict]) -> dict:
    """Build a row exercising the flat ``metadata/<judge>/...`` surface.

    Mirrors what the workbench v2.1 capture writer emits and what
    production eval rows look like — the value is the JSON-encoded
    payload (the Trial 14 writer JSON-encodes structured lists).
    """
    return {
        "request": {
            "question": "What is total revenue by region?",
            "kwargs": {"question_id": "test_qid_002"},
        },
        "response": {"response": "SELECT region, SUM(revenue) FROM sales"},
        "expected_response": {"value": "SELECT region, SUM(revenue) FROM sales"},
        "metadata/schema_accuracy/blame_set": "fallback_col",
        "metadata/schema_accuracy/blame_set_structured": json.dumps(blame_structured),
        "metadata/schema_accuracy/failure_type": "wrong_column",
        "schema_accuracy/rationale": "Schema mismatch detected",
    }


# ── _collect_blame_entries_from_asi ─────────────────────────────────


def test_collect_blame_entries_from_nested_surface() -> None:
    row = _row_with_structured_nested(
        [
            {"kind": "column", "ref": "main.airline.fact.dest_col"},
            {"kind": "filter", "ref": None, "description": "x = 1"},
        ]
    )
    entries = _collect_blame_entries_from_asi(row)
    assert [e.kind for e in entries] == ["column", "filter"]
    assert entries[0].ref == "main.airline.fact.dest_col"


def test_collect_blame_entries_from_flat_surface() -> None:
    row = _row_with_structured_flat(
        [{"kind": "column", "ref": "main.airline.fact.col"}]
    )
    entries = _collect_blame_entries_from_asi(row)
    assert [e.kind for e in entries] == ["column"]
    assert entries[0].ref == "main.airline.fact.col"


def test_collect_blame_entries_dedupes_across_surfaces() -> None:
    """Same entry on both surfaces should appear once."""
    blame = [{"kind": "column", "ref": "a.b.c.d", "description": None}]
    row: dict = {
        "schema_accuracy/metadata": {"blame_set_structured": blame},
        "metadata/schema_accuracy/blame_set_structured": json.dumps(blame),
    }
    entries = _collect_blame_entries_from_asi(row)
    assert len(entries) == 1


def test_collect_blame_entries_empty_for_legacy_only_row() -> None:
    row = {
        "schema_accuracy/metadata": {"blame_set": ["legacy_col"]},
    }
    assert _collect_blame_entries_from_asi(row) == []


# ── _collect_blame_set_from_asi — prefer-structured behaviour ─────


def test_collect_blame_set_prefers_structured_refs() -> None:
    """When structured entries exist, the seed list MUST be drawn from
    their schema-resolvable refs — not the legacy free-text mirror.
    """
    row = _row_with_structured_nested(
        [
            {"kind": "column", "ref": "main.airline.fact.dest_col"},
            {"kind": "table", "ref": "main.airline.fact"},
            {"kind": "filter", "ref": None, "description": "x = 1"},
        ]
    )
    seeds = _collect_blame_set_from_asi(row)
    assert seeds == ["main.airline.fact.dest_col", "main.airline.fact"]


def test_collect_blame_set_returns_empty_when_all_structured_are_non_schema() -> None:
    """All-filter-kind structured payload → empty seed list (so the
    contract can fire ``seeds_all_filter_kind`` instead of falling
    back to the legacy path)."""
    row = _row_with_structured_nested(
        [
            {"kind": "filter", "ref": None, "description": "x = 1"},
            {"kind": "instruction", "ref": None, "description": "rule"},
        ]
    )
    seeds = _collect_blame_set_from_asi(row)
    assert seeds == []


def test_collect_blame_set_falls_back_to_legacy_when_no_structured() -> None:
    row = {
        "schema_accuracy/metadata": {
            "blame_set": ["legacy_col_a", "legacy_col_b"],
        },
    }
    seeds = _collect_blame_set_from_asi(row)
    assert seeds == ["legacy_col_a", "legacy_col_b"]


def test_collect_blame_set_legacy_string_list_still_parses() -> None:
    """Trial 13k JSON-quoted parser must keep working on the legacy
    fallback path (no structured field present)."""
    row = {
        "schema_accuracy/metadata": {
            "blame_set": '["zone_name", "region_director_name"]',
        },
    }
    seeds = _collect_blame_set_from_asi(row)
    assert seeds == ["zone_name", "region_director_name"]


# ── build_stage1_evidence_card stamps _blame_structured ─────────────


def test_build_stage1_evidence_card_stamps_blame_structured() -> None:
    row = _row_with_structured_nested(
        [
            {"kind": "column", "ref": "main.airline.fact.dest_col"},
            {"kind": "filter", "ref": None, "description": "x = 1"},
        ]
    )
    card = build_stage1_evidence_card("test_qid_001", row)
    assert "_blame_structured" in card
    payload = card["_blame_structured"]
    assert isinstance(payload, list)
    assert len(payload) == 2
    assert payload[0] == {
        "kind": "column",
        "ref": "main.airline.fact.dest_col",
        "description": None,
    }
    assert payload[1]["kind"] == "filter"


def test_build_stage1_evidence_card_blame_structured_empty_for_legacy_only() -> None:
    row = {
        "request": {
            "question": "test q",
            "kwargs": {"question_id": "qid"},
        },
        "response": {"response": "SELECT 1"},
        "expected_response": {"value": "SELECT 1"},
        "schema_accuracy/metadata": {"blame_set": ["legacy_col"]},
        "schema_accuracy/rationale": "legacy",
    }
    card = build_stage1_evidence_card("qid", row)
    assert card.get("_blame_structured") == []
    # And the legacy fallback path still populates the seed list.
    assert card["blame_set_seed"] == ["legacy_col"]
