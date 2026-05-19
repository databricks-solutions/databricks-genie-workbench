"""Plan 5 Task 11 — cross-lever router.

Roadmap.md:337-343 — "This started as L5b, but the correct patch is L6."
Plan 5 LLM may emit a patch_type that maps to a different per-lever
generator than the stage-1 pick implied. The router:
  - looks up the patch_type → per-lever generator callable.
  - validates the compatible-shape check (patch_type must be in the AG's
    supported-override set).
  - returns (generator, override_event) tuple OR None when the override
    fails the check (caller falls back to intent_from_archetype).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.cross_lever_router import (
    CrossLeverOverrideEvent,
    SUPPORTED_OVERRIDE_TARGETS,
    cross_lever_dispatch_table,
    route_to_per_lever_generator,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def _make_proposal(patch_type: PatchType) -> RepairProposal:
    return RepairProposal(
        intent_id="intent_H001_AG3_001",
        intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=patch_type, rationale="x", confidence="high",
        patch_body={"example_question": "q", "example_sql": "s",
                     "name": "n", "sql_expression": "e",
                     "left": "l", "right": "r", "on": "k",
                     "instruction_text": "i", "instruction_id": "id",
                     "new_text": "t",
                     "table": "tbl", "column": "col",
                     "description": "d"},
        blame_set=("sales.fact_sales.revenue",),
    )


def test_dispatch_table_includes_in_lane_add_example_sql() -> None:
    table = cross_lever_dispatch_table()
    assert PatchType.ADD_EXAMPLE_SQL in table


def test_dispatch_table_includes_l6_add_sql_snippet_expression() -> None:
    """Roadmap's canonical override target — L5b → L6."""
    table = cross_lever_dispatch_table()
    assert PatchType.ADD_SQL_SNIPPET_EXPRESSION in table


def test_supported_override_targets_documented() -> None:
    """Frozen set documenting what Plan 5 allows the LLM to override to.
    Adding a new override target requires updating this set + the
    dispatch table together."""
    assert isinstance(SUPPORTED_OVERRIDE_TARGETS, frozenset)
    assert PatchType.ADD_EXAMPLE_SQL in SUPPORTED_OVERRIDE_TARGETS
    assert PatchType.ADD_SQL_SNIPPET_EXPRESSION in SUPPORTED_OVERRIDE_TARGETS


def test_route_for_in_lane_add_example_sql_returns_l5b_generator() -> None:
    proposal = _make_proposal(PatchType.ADD_EXAMPLE_SQL)
    result = route_to_per_lever_generator(proposal)
    assert result is not None
    generator, event = result
    assert callable(generator)
    assert event is None


def test_route_for_cross_lever_add_sql_snippet_expression_returns_l6_generator() -> None:
    proposal = _make_proposal(PatchType.ADD_SQL_SNIPPET_EXPRESSION)
    result = route_to_per_lever_generator(proposal)
    assert result is not None
    generator, event = result
    assert callable(generator)
    assert isinstance(event, CrossLeverOverrideEvent)
    assert event.from_lever == "lever-5b-example-sql"
    assert event.to_lever == "lever-6-sql-expression"
    assert event.from_patch_type is PatchType.ADD_EXAMPLE_SQL
    assert event.to_patch_type is PatchType.ADD_SQL_SNIPPET_EXPRESSION
    assert event.intent_id == proposal.intent_id


def test_route_for_unsupported_patch_type_returns_none() -> None:
    """ADD_TVF is not in SUPPORTED_OVERRIDE_TARGETS (not a Plan-5
    override target yet). Router returns None → caller falls back to
    intent_from_archetype."""
    proposal = _make_proposal(PatchType.ADD_TVF)
    assert route_to_per_lever_generator(proposal) is None


def test_route_event_carries_provenance_for_postmortem() -> None:
    proposal = _make_proposal(PatchType.ADD_SQL_SNIPPET_EXPRESSION)
    result = route_to_per_lever_generator(proposal)
    assert result is not None
    _, event = result
    assert event is not None
    event_dict = event.to_dict()
    assert event_dict["from_lever"] == "lever-5b-example-sql"
    assert event_dict["to_lever"] == "lever-6-sql-expression"
    assert event_dict["from_patch_type"] == "add_example_sql"
    assert event_dict["to_patch_type"] == "add_sql_snippet_expression"
    assert event_dict["intent_id"] == proposal.intent_id


def test_dispatch_table_l5b_generator_returns_l5b_proposal_dict_shape() -> None:
    """L5b generator wraps RepairProposal.to_proposal_dict() — same
    4-key shape the existing lean L5b adapter returns (matches the
    L5b output contract at optimizer.py:10406-10412)."""
    proposal = _make_proposal(PatchType.ADD_EXAMPLE_SQL)
    table = cross_lever_dispatch_table()
    generator = table[PatchType.ADD_EXAMPLE_SQL]
    out = generator(proposal)
    assert set(out.keys()) == {
        "example_question", "example_sql", "parameters", "usage_guidance",
    }


def test_dispatch_table_l6_generator_returns_l6_proposal_dict_shape() -> None:
    """L6 generator wraps RepairProposal.to_proposal_dict() — name +
    sql_expression + usage_guidance."""
    proposal = _make_proposal(PatchType.ADD_SQL_SNIPPET_EXPRESSION)
    table = cross_lever_dispatch_table()
    generator = table[PatchType.ADD_SQL_SNIPPET_EXPRESSION]
    out = generator(proposal)
    assert set(out.keys()) == {
        "name", "sql_expression", "usage_guidance",
    }
