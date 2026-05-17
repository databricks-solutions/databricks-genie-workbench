"""Phase 2 (2026-05-16) — extend ``capture_iter_ag_context`` with a
``blame_set`` key so the harness can build a ``TerminalSignature``
without manually scraping the AG dict at every terminal-emit site.

The pattern mirrors the existing ``cluster_ids`` extraction in
``iteration_ag_context.py`` — string-coerce, drop empties, sort for
canonical ordering.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.iteration_ag_context import (
    capture_iter_ag_context,
)


def test_capture_returns_blame_set_key():
    ctx = capture_iter_ag_context(ag={}, ag_id="X")
    assert "blame_set" in ctx, (
        f"capture_iter_ag_context must include 'blame_set' key. Got: "
        f"{sorted(ctx.keys())}"
    )


def test_blame_set_from_blame_set_field():
    ag = {
        "id": "AG1",
        "blame_set": ["catalog.schema.orders", "catalog.schema.customers"],
    }
    ctx = capture_iter_ag_context(ag=ag, ag_id="AG1")
    assert ctx["blame_set"] == (
        "catalog.schema.customers", "catalog.schema.orders",
    ), f"blame_set must be canonically sorted tuple. Got: {ctx['blame_set']}"


def test_blame_set_falls_back_to_blamed_assets():
    """Some strategist responses use ``blamed_assets`` instead of
    ``blame_set``. Widen the fallback chain to match the harness's
    historical use of either field name."""
    ag = {
        "id": "AG1",
        "blamed_assets": ["catalog.schema.orders"],
    }
    ctx = capture_iter_ag_context(ag=ag, ag_id="AG1")
    assert ctx["blame_set"] == ("catalog.schema.orders",)


def test_blame_set_drops_empty_strings():
    ag = {"id": "AG1", "blame_set": ["catalog.schema.orders", "", None, "  "]}
    ctx = capture_iter_ag_context(ag=ag, ag_id="AG1")
    assert ctx["blame_set"] == ("catalog.schema.orders",), (
        f"Empty/None entries must be dropped. Got: {ctx['blame_set']}"
    )


def test_blame_set_empty_when_neither_field_present():
    ctx = capture_iter_ag_context(ag={"id": "AG1"}, ag_id="AG1")
    assert ctx["blame_set"] == ()


def test_blame_set_coerces_non_string_entries_to_string():
    ag = {"id": "AG1", "blame_set": [123, "catalog.schema.orders"]}
    ctx = capture_iter_ag_context(ag=ag, ag_id="AG1")
    assert ctx["blame_set"] == ("123", "catalog.schema.orders")


def test_existing_keys_still_present_and_unchanged():
    """Defensive: Phase 1's five keys must keep their semantics."""
    ag = {
        "id": "AG1",
        "source_cluster_ids": ["H001"],
        "target_qids": ["gs_017"],
        "lever_directives": {"5": {"name": "L5"}},
        "root_cause_summary": "missing_join",
        "blame_set": ["catalog.schema.orders"],
    }
    ctx = capture_iter_ag_context(ag=ag, ag_id="AG1")
    assert ctx["ag_id"] == "AG1"
    assert ctx["cluster_ids"] == ("H001",)
    assert ctx["target_qids"] == ("gs_017",)
    assert ctx["levers"] == (5,)
    assert ctx["root_cause"] == "missing_join"
    assert ctx["blame_set"] == ("catalog.schema.orders",)
