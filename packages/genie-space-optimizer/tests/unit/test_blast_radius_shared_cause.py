"""Cycle 2 Task 2 — shared-cause-aware blast radius.

When a high-collateral patch's only ``outside_target`` qids are
themselves currently-hard, downgrade the reject to a warning. Two
hard failures sharing a cause should not block each other's fix.

Reproducer for run 2afb0be2-88b6-4832-99aa-c7e78fbc90f7 iter 1:
P001#2 dropped with outside_target=['gs_003'] where gs_003 is itself
hard (H002 missing_filter).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.proposal_grounding import (
    patch_blast_radius_is_safe,
)


def _patch(passing_dependents: list[str]) -> dict:
    return {
        "patch_type": "add_sql_snippet_filter",
        "passing_dependents": passing_dependents,
        "high_collateral_risk": True,
    }


def test_outside_target_all_hard_downgrades_to_warning(monkeypatch) -> None:
    monkeypatch.setenv("GSO_SHARED_CAUSE_BLAST_RADIUS", "1")
    result = patch_blast_radius_is_safe(
        _patch(["gs_003"]),
        ag_target_qids=("gs_024",),
        live_hard_qids=("gs_003", "gs_009", "gs_024"),
    )
    assert result["safe"] is True
    assert result["reason"] == "shared_cause_collateral_warning"
    assert result["passing_dependents_outside_target"] == ["gs_003"]


def test_outside_target_mixed_hard_and_passing_still_blocks(monkeypatch) -> None:
    monkeypatch.setenv("GSO_SHARED_CAUSE_BLAST_RADIUS", "1")
    result = patch_blast_radius_is_safe(
        _patch(["gs_003", "gs_005"]),  # gs_005 currently passes
        ag_target_qids=("gs_024",),
        live_hard_qids=("gs_003", "gs_009", "gs_024"),
    )
    assert result["safe"] is False
    assert result["reason"] == "high_collateral_risk_flagged"


def test_outside_target_all_passing_still_blocks(monkeypatch) -> None:
    monkeypatch.setenv("GSO_SHARED_CAUSE_BLAST_RADIUS", "1")
    result = patch_blast_radius_is_safe(
        _patch(["gs_005", "gs_019"]),
        ag_target_qids=("gs_024",),
        live_hard_qids=("gs_024",),  # only the target is hard
    )
    assert result["safe"] is False
    assert result["reason"] == "high_collateral_risk_flagged"


def test_no_live_hard_qids_argument_preserves_existing_behaviour(
    monkeypatch,
) -> None:
    """When the call site does not pass ``live_hard_qids`` (legacy
    callers), the new branch must not trigger. This protects in-tree
    tests that exist before harness wiring lands."""
    monkeypatch.setenv("GSO_SHARED_CAUSE_BLAST_RADIUS", "1")
    result = patch_blast_radius_is_safe(
        _patch(["gs_003"]),
        ag_target_qids=("gs_024",),
    )
    assert result["safe"] is False
    assert result["reason"] == "high_collateral_risk_flagged"
