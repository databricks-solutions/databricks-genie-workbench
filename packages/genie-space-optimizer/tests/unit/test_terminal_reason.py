"""Phase 1.2 — TerminalReason closed-vocabulary enum.

Vocabulary locked by ``docs/final_plan/2026-05-13-final-closeout-
contract-spec.md`` Section 3.2.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)


def test_enum_has_exactly_seventeen_spec_values():
    expected = {
        "no_rca_ground",
        "no_action_group_emitted",
        "ag_collision_with_forbidden_set",
        "no_structural_candidate",
        "proposal_generation_empty",
        "structural_gate_dropped_instruction_only",
        "applyability_rejected",
        "blast_radius_rejected",
        "collateral_risk_rejected",
        "all_selected_patches_dropped_by_applier",
        "no_applied_patches",
        "target_qids_not_improved",
        "content_regression_rollback",
        "multi_patch_regression_no_isolation",
        "directive_outcome_violation",
        "invariant_violation",
        "unknown",
    }
    actual = {r.value for r in TerminalReason}
    assert actual == expected, f"diff: {actual ^ expected}"


def test_enum_is_str_enum():
    assert TerminalReason.PROPOSAL_GENERATION_EMPTY == "proposal_generation_empty"
    assert isinstance(TerminalReason.PROPOSAL_GENERATION_EMPTY.value, str)


def test_enum_values_match_phase_0_3_terminal_marker_payload():
    """Phase 0.3 iteration_no_candidate_marker emits
    terminal_reason=<TerminalReason.value>. The strings must be
    identical so the postmortem can group on them.
    """
    expected_substrings = (
        "proposal_generation_empty", "no_applied_patches",
        "no_structural_candidate", "blast_radius_rejected",
        "structural_gate_dropped_instruction_only",
        "no_rca_ground", "ag_collision_with_forbidden_set",
    )
    values = {r.value for r in TerminalReason}
    for sub in expected_substrings:
        assert sub in values


def test_unknown_is_defensive_only():
    """``UNKNOWN`` is reserved for defensive paths (spec Section 3.2).
    Producers should never emit it in healthy runs; we just verify it
    exists in the vocabulary."""
    assert TerminalReason.UNKNOWN.value == "unknown"
