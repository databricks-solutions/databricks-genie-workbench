"""Track A / A2 — I26 cross-surface lifecycle coherence invariant.

The e94376a3 postmortem showed an "accepted" decision coexisting with a
zero-gain scoreboard, no selected proposal, zero applied patches, and a
``hard_failure_unresolved`` journey terminal state — because the three
terminate surfaces (decision_trace / scoreboard / journey_validation)
were assembled from disjoint pipelines with no reconciler. I26 ties them
together: a genuinely accepted iteration MUST agree across all five
facts.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.invariants import (
    check_i26_lifecycle_coherence,
)


def test_absent_evidence_is_silent() -> None:
    """Pre-I26 fixtures (no lifecycle_* keys) stay green."""
    assert check_i26_lifecycle_coherence({}) == []


def test_non_accepted_iteration_is_silent() -> None:
    """I26 only reconciles ACCEPTED iterations — a non-accept that
    applied nothing is not a contradiction."""
    evidence = {
        "lifecycle_accepted": False,
        "lifecycle_scoreboard_delta_pp": 0.0,
        "lifecycle_selected_proposal_id": "",
        "lifecycle_patches_applied": 0,
        "lifecycle_journey_terminal_states": ["hard_failure_unresolved"],
    }
    assert check_i26_lifecycle_coherence(evidence) == []


def test_coherent_accept_has_no_violation() -> None:
    """A real win: accepted, positive delta, a selected proposal, an
    applied patch, and the target QID resolved."""
    evidence = {
        "lifecycle_accepted": True,
        "lifecycle_scoreboard_delta_pp": 12.5,
        "lifecycle_selected_proposal_id": "p_real_win",
        "lifecycle_patches_applied": 1,
        "lifecycle_journey_terminal_states": ["hard_failure_resolved"],
    }
    assert check_i26_lifecycle_coherence(evidence) == []


def test_e943_phantom_accept_is_flagged() -> None:
    """The exact e943 contradiction: accepted decision, but delta=0,
    no selected proposal, zero applied patches, QID still hard. Every
    diverging fact must be named in the single I26 violation."""
    evidence = {
        "lifecycle_accepted": True,
        "lifecycle_scoreboard_delta_pp": 0.0,
        "lifecycle_selected_proposal_id": "",
        "lifecycle_patches_applied": 0,
        "lifecycle_journey_terminal_states": ["hard_failure_unresolved"],
    }
    violations = check_i26_lifecycle_coherence(evidence)
    assert len(violations) == 1, violations
    v = violations[0]
    assert v["invariant"] == "I26"
    msg = v["message"]
    assert "scoreboard_delta_pp" in msg
    assert "selected_proposal_id" in msg
    assert "patches_applied" in msg
    assert "hard_failure_unresolved" in msg


def test_partial_contradiction_applied_but_zero_delta() -> None:
    """A patch was applied and selected, but the accept claims a gain
    the scoreboard does not show — still a contradiction (the
    applied-but-inert phantom Track B addresses)."""
    evidence = {
        "lifecycle_accepted": True,
        "lifecycle_scoreboard_delta_pp": 0.0,
        "lifecycle_selected_proposal_id": "p_inert",
        "lifecycle_patches_applied": 1,
        "lifecycle_journey_terminal_states": ["hard_failure_unresolved"],
    }
    violations = check_i26_lifecycle_coherence(evidence)
    assert len(violations) == 1
    msg = violations[0]["message"]
    assert "scoreboard_delta_pp" in msg
    assert "hard_failure_unresolved" in msg
    # The applied/selected facts are coherent here, so they must NOT be
    # named as diverging.
    assert "selected_proposal_id empty" not in msg
    assert "patches_applied=" not in msg


def test_i26_registered_in_run_invariants() -> None:
    """I26 must run as part of the aggregate invariant pass."""
    from genie_space_optimizer.optimization.invariants import run_invariants

    evidence = {
        "lifecycle_accepted": True,
        "lifecycle_scoreboard_delta_pp": 0.0,
        "lifecycle_selected_proposal_id": "",
        "lifecycle_patches_applied": 0,
        "lifecycle_journey_terminal_states": ["hard_failure_unresolved"],
    }
    violations = run_invariants(evidence)
    assert any(v.get("invariant") == "I26" for v in violations), violations
