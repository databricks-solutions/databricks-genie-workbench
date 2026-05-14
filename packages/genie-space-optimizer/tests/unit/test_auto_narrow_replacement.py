"""Phase 2.4 — auto narrow-replacement on collateral drop."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.auto_narrow_replacement import (
    try_narrow_replacement,
    NarrowReplacementResult,
)


def test_no_dropped_patches_returns_no_attempt():
    result = try_narrow_replacement(
        dropped_patches=[],
        outside_target_qids=(),
        cluster=None,
        rca_card=None,
        synthesis_callable_l6=lambda **_: None,
        synthesis_callable_l5=lambda **_: None,
    )
    assert result.attempted is False
    assert result.replacement_patch is None


def test_non_collateral_drop_returns_no_attempt():
    """Drops with a reason other than ``high_collateral_risk_flagged``
    do not trigger replacement."""
    result = try_narrow_replacement(
        dropped_patches=[{
            "patch_id": "p1",
            "patch_type": "sql_snippet",
            "drop_reason": "applyability_failed",
        }],
        outside_target_qids=("gs_003",),
        cluster={"cluster_id": "c1"},
        rca_card={"root_cause": "r"},
        synthesis_callable_l6=lambda **_: None,
        synthesis_callable_l5=lambda **_: None,
    )
    assert result.attempted is False


def test_collateral_drop_l6_triggers_narrow_l6():
    """Broad L6 dropped for collateral → call build_narrow_l6_replacement
    with protected_dependents set to outside_target_qids."""
    calls = []

    def fake_l6(**kwargs):
        calls.append(("l6", kwargs))
        return {"patch_id": "narrow_l6", "patch_type": "narrow_l6_sql"}

    result = try_narrow_replacement(
        dropped_patches=[{
            "patch_id": "p1",
            "patch_type": "sql_snippet",
            "drop_reason": "high_collateral_risk_flagged",
        }],
        outside_target_qids=("gs_003", "gs_005"),
        cluster={"cluster_id": "c1", "target_qids": ["gs_001"]},
        rca_card={"root_cause": "missing_metric_view"},
        synthesis_callable_l6=fake_l6,
        synthesis_callable_l5=lambda **_: None,
    )
    assert result.attempted is True
    assert result.replacement_patch is not None
    assert result.replacement_patch["patch_type"] == "narrow_l6_sql"
    assert len(calls) == 1
    assert calls[0][1]["protected_dependents"] == ("gs_003", "gs_005")


def test_collateral_drop_example_sql_triggers_narrow_l5():
    """A dropped example-SQL patch → call build_l5_example_sql_replacement."""
    calls = []

    def fake_l5(**kwargs):
        calls.append(("l5", kwargs))
        return {"patch_id": "narrow_l5", "patch_type": "example_sql_per_question"}

    result = try_narrow_replacement(
        dropped_patches=[{
            "patch_id": "p2",
            "patch_type": "example_sql_per_question",
            "drop_reason": "high_collateral_risk_flagged",
        }],
        outside_target_qids=("gs_003",),
        cluster={"cluster_id": "c1", "target_qids": ["gs_001"]},
        rca_card={"root_cause": "r"},
        synthesis_callable_l6=lambda **_: None,
        synthesis_callable_l5=fake_l5,
    )
    assert result.attempted is True
    assert result.replacement_patch["patch_type"] == "example_sql_per_question"
    assert len(calls) == 1


def test_synthesis_returns_none_yields_no_structural_alternative():
    """When the synthesis helper returns None, the result records
    BLAST_RADIUS_REJECTED so the caller can emit the typed
    terminal marker."""
    result = try_narrow_replacement(
        dropped_patches=[{
            "patch_id": "p1",
            "patch_type": "sql_snippet",
            "drop_reason": "high_collateral_risk_flagged",
        }],
        outside_target_qids=("gs_003",),
        cluster={"cluster_id": "c1"},
        rca_card={"root_cause": "r"},
        synthesis_callable_l6=lambda **_: None,
        synthesis_callable_l5=lambda **_: None,
    )
    assert result.attempted is True
    assert result.replacement_patch is None
    assert result.terminal_reason == "blast_radius_rejected"
