"""Phase 3 Action 3.2 — NearMissReflection builder tests."""

from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.near_miss_reflection import (
    AGShapeSignature,
    NearMissReflection,
    build_diagnostic_hold_reflection,
    build_net_win_with_debt_reflection,
)
from genie_space_optimizer.optimization.target_scope import TargetScope


def _shape() -> AGShapeSignature:
    return AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.SINGLE_QID,
        primary_cluster_id="cluster_h002",
        target_qids=("gs_021",),
    )


def test_net_win_with_debt_reflection_kind_and_required_change() -> None:
    nmr = build_net_win_with_debt_reflection(
        iteration=1,
        target_qids=("gs_021",),
        prior_ag_shape=_shape(),
        prior_lever_directives=({"lever": "L6_sql_expression"},),
        regression_debt={"unknown_to_hard": ["gs_012"]},
    )
    assert isinstance(nmr, NearMissReflection)
    assert nmr.kind == "net_win_with_debt"
    assert nmr.iteration == 1
    assert nmr.target_qids == ("gs_021",)
    assert nmr.required_next_iter_change == "different_repair_archetype"
    assert "default_time_window_filter" in nmr.reflection_text
    assert "gs_021" in nmr.reflection_text


def test_net_win_with_debt_reflection_is_frozen() -> None:
    nmr = build_net_win_with_debt_reflection(
        iteration=1, target_qids=(), prior_ag_shape=_shape(),
        prior_lever_directives=(), regression_debt={},
    )
    try:
        nmr.iteration = 2  # type: ignore[misc]
        raised = False
    except dataclasses.FrozenInstanceError:
        raised = True
    assert raised


def test_diagnostic_hold_reflection_required_change_is_either() -> None:
    nmr = build_diagnostic_hold_reflection(
        iteration=1,
        target_qids=("gs_026",),
        prior_ag_shape=_shape(),
        prior_lever_directives=({"lever": "L6_sql_expression"},),
        regression_debt={"unknown_to_hard": ["gs_012"]},
    )
    assert nmr.kind == "diagnostic_hold"
    assert nmr.required_next_iter_change == "either"
    text_lower = nmr.reflection_text.lower()
    assert "scope" in text_lower or "narrower" in text_lower
    assert "fix" in text_lower or "more" in text_lower


def test_diagnostic_hold_reflection_carries_typed_debt() -> None:
    nmr = build_diagnostic_hold_reflection(
        iteration=2,
        target_qids=("gs_026",),
        prior_ag_shape=_shape(),
        prior_lever_directives=(),
        regression_debt={
            "unknown_to_hard": ["gs_012"],
            "soft_to_hard": ["gs_099"],
        },
    )
    assert nmr.regression_debt == {
        "unknown_to_hard": ["gs_012"],
        "soft_to_hard": ["gs_099"],
    }
    assert "gs_012" in nmr.reflection_text
    assert "gs_099" in nmr.reflection_text
