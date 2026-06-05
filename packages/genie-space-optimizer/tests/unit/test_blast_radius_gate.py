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


def test_safe_when_no_passing_dependents_field(monkeypatch) -> None:
    # Legacy fail-open branch: a MISSING ``passing_dependents`` field is
    # safe-by-default only when the Trial 20 E2 mandatory gate is off.
    # With the gate on (the default), a missing stamp is treated as a
    # plumbing regression and fails closed (covered by the E2 tests);
    # the empty-list safe path is covered separately below.
    monkeypatch.setenv("GSO_TRIAL20_BLAST_RADIUS_MANDATORY", "0")
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
