from __future__ import annotations

from genie_space_optimizer.optimization.proposal_grounding import (
    patch_blast_radius_is_safe,
)


def test_safe_when_no_passing_dependents_outside_target() -> None:
    patch = {
        "type": "add_sql_snippet_filter",
        "passing_dependents": ["q009", "q021"],
    }
    decision = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("q009", "q021"),
        max_outside_target=0,
    )
    assert decision["safe"] is True
    assert decision["reason"] == "no_passing_dependents_outside_target"


def test_unsafe_when_passing_dependents_exceed_threshold() -> None:
    patch = {
        "type": "add_sql_snippet_filter",
        "passing_dependents": ["q001", "q004", "q006", "q008"],
    }
    decision = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("q009", "q021"),
        max_outside_target=0,
    )
    assert decision["safe"] is False
    assert decision["reason"] == "blast_radius_exceeds_threshold"
    assert decision["passing_dependents_outside_target"] == [
        "q001", "q004", "q006", "q008",
    ]


def test_safe_when_high_collateral_risk_unset_and_dependents_within_threshold() -> None:
    patch = {
        "type": "update_column_description",
        "passing_dependents": ["q011"],
    }
    decision = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("q009",),
        max_outside_target=2,
    )
    assert decision["safe"] is True
    assert decision["reason"] == "within_threshold"


def test_high_collateral_risk_overrides_threshold_to_zero(monkeypatch) -> None:
    """Legacy uniform-rejection contract — pinned with the lever-aware
    flag explicitly disabled. The default-on lever-aware behaviour
    (non-semantic patches downgrade to a warning) is pinned by
    ``test_blast_radius_lever_aware.py``."""
    monkeypatch.setenv("GSO_LEVER_AWARE_BLAST_RADIUS", "0")
    patch = {
        "type": "update_column_description",
        "passing_dependents": ["q011"],
        "high_collateral_risk": True,
    }
    decision = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("q009",),
        max_outside_target=2,
    )
    assert decision["safe"] is False
    assert decision["reason"] == "high_collateral_risk_flagged"
    assert decision["passing_dependents_outside_target"] == ["q011"]


def test_safe_when_no_passing_dependents_field() -> None:
    patch = {"type": "add_instruction"}
    decision = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("q009",),
        max_outside_target=0,
    )
    assert decision["safe"] is True
    assert decision["reason"] == "no_passing_dependents_field"


def test_gate_returns_safe_for_patches_with_empty_passing_dependents() -> None:
    patch = {"type": "add_instruction", "passing_dependents": []}
    decision = patch_blast_radius_is_safe(
        patch,
        ag_target_qids=("q009",),
        max_outside_target=0,
    )
    assert decision["safe"] is True
    assert decision["reason"] in {
        "no_passing_dependents_outside_target",
        "within_threshold",
    }
