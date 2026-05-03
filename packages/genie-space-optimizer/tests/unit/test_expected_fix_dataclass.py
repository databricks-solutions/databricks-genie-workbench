"""Phase C Task 2 — ExpectedFix frozen dataclass + RcaExecutionPlan
migration.

The RCA contract types live in ``rca_execution.py``. This test pins:

* ``ExpectedFix`` is frozen and immutable.
* ``ExpectedFix.from_dict`` round-trips: ``from_dict(as_dict()) == fix``.
* ``RcaExecutionPlan.patch_intents`` is now ``tuple[ExpectedFix, ...]``
  rather than ``tuple[dict, ...]``.
* ``build_rca_execution_plans`` emits typed intents from a theme's
  patch dicts.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 2.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest


def test_expected_fix_is_frozen() -> None:
    from genie_space_optimizer.optimization.rca_execution import ExpectedFix

    fix = ExpectedFix(
        patch_type="add_sql_snippet_filter",
        target="main.gold.order_summary",
        intent="Add WHERE active = true.",
        lever=6,
        grounding_terms=("active", "order_summary"),
    )
    with pytest.raises(FrozenInstanceError):
        fix.lever = 5  # type: ignore[misc]


def test_expected_fix_from_dict_round_trip() -> None:
    from genie_space_optimizer.optimization.rca_execution import ExpectedFix

    raw = {
        "type": "add_instruction",
        "lever": 3,
        "intent": "Route fn_mtd_or_mtday requests to the TVF.",
        "target": "fn_mtd_or_mtday",
    }
    fix = ExpectedFix.from_dict(raw)
    assert fix.patch_type == "add_instruction"
    assert fix.target == "fn_mtd_or_mtday"
    assert fix.intent == "Route fn_mtd_or_mtday requests to the TVF."
    assert fix.lever == 3
    # ``as_dict`` round-trips canonical fields. Unknown fields like
    # ``snippet_name`` come back via ``extras``.
    round_tripped = fix.as_dict()
    assert round_tripped["type"] == raw["type"]
    assert round_tripped["lever"] == raw["lever"]
    assert round_tripped["intent"] == raw["intent"]
    assert round_tripped["target"] == raw["target"]


def test_expected_fix_from_dict_collects_extras() -> None:
    from genie_space_optimizer.optimization.rca_execution import ExpectedFix

    raw = {
        "type": "add_sql_snippet_filter",
        "lever": 6,
        "intent": "Add WHERE active = true.",
        "target": "main.gold.order_summary",
        "snippet_name": "active_orders_filter",
        "expression": "WHERE active = true",
    }
    fix = ExpectedFix.from_dict(raw)
    assert fix.extras_dict["snippet_name"] == "active_orders_filter"
    assert fix.extras_dict["expression"] == "WHERE active = true"


def test_rca_execution_plan_patch_intents_are_typed() -> None:
    from genie_space_optimizer.optimization.rca import RcaPatchTheme, RcaKind
    from genie_space_optimizer.optimization.rca_execution import (
        ExpectedFix,
        build_rca_execution_plans,
    )

    theme = RcaPatchTheme(
        rca_id="rca_filter_a",
        rca_kind=RcaKind.FILTER_LOGIC_MISMATCH,
        patch_family="filter_logic_repair",
        patches=(
            {
                "type": "add_sql_snippet_filter",
                "lever": 6,
                "intent": "Add active filter.",
                "target": "main.gold.order_summary",
                "snippet_name": "active_orders_filter",
                "expression": "WHERE active = true",
            },
        ),
        target_qids=("q_active_1",),
        touched_objects=("main.gold.order_summary",),
        confidence=0.9,
        evidence_summary="Filter omitted.",
    )
    plans = build_rca_execution_plans([theme])
    assert len(plans) == 1
    plan = plans[0]
    assert isinstance(plan.patch_intents, tuple)
    assert all(isinstance(p, ExpectedFix) for p in plan.patch_intents)
    assert plan.patch_intents[0].patch_type == "add_sql_snippet_filter"
    assert plan.patch_intents[0].lever == 6
