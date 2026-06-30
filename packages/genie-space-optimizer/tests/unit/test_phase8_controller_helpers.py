"""GSO v2 Phase 8 — pure two-mode controller helpers (arch §5.1 / §5.2).

These cover the §5.1 control-flow decisions that live in ``control_plane`` so the
two-mode loop semantics are unit-testable without a Databricks workspace:

  * ``decide_attempt_mode``        — attempt 1 = coverage, 2..N = surgical
  * ``decide_coverage_outcome``    — coverage measured AS A UNIT; Δacc<0 ⇒ rollback
  * ``decide_loop_terminal_reason``— TARGET_REACHED vs MAX_ATTEMPTS
  * ``build_loop_state``           — the per-attempt loop-state column map
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.control_plane import (
    LOOP_TERMINAL_REASONS,
    build_loop_state,
    decide_attempt_mode,
    decide_coverage_outcome,
    decide_loop_terminal_reason,
)


# ── decide_attempt_mode: two-mode sequencing ────────────────────────────────
def test_attempt_one_is_coverage_rest_are_surgical() -> None:
    assert decide_attempt_mode(1) == "coverage"
    assert decide_attempt_mode(2) == "surgical"
    assert decide_attempt_mode(3) == "surgical"
    assert decide_attempt_mode(10) == "surgical"


def test_attempt_mode_guards_non_positive_to_coverage() -> None:
    # Defensive: a 0/negative display counter must not silently become surgical.
    assert decide_attempt_mode(0) == "coverage"
    assert decide_attempt_mode(-1) == "coverage"


def test_two_mode_sequence_over_a_run() -> None:
    # Attempt 1 coverage, then surgical 2..N — the canonical ladder ordering.
    seq = [decide_attempt_mode(n) for n in range(1, 5)]
    assert seq == ["coverage", "surgical", "surgical", "surgical"]


# ── decide_coverage_outcome: measured-as-a-unit + rollback on Δacc<0 ────────
def test_coverage_regression_rolls_back_as_a_unit() -> None:
    out = decide_coverage_outcome(
        frozen_baseline_accuracy=80.0,
        post_coverage_accuracy=74.0,
        had_candidates=True,
    )
    assert out.decision == "reject"
    assert out.should_rollback is True
    assert out.delta_pp == -6.0
    assert "rolled_back" in out.decision_reason


def test_coverage_lift_becomes_champion_no_rollback() -> None:
    out = decide_coverage_outcome(
        frozen_baseline_accuracy=80.0,
        post_coverage_accuracy=88.0,
        had_candidates=True,
    )
    assert out.decision == "accept"
    assert out.should_rollback is False
    assert out.delta_pp == 8.0


def test_coverage_zero_delta_keeps_additive_enrichment() -> None:
    out = decide_coverage_outcome(
        frozen_baseline_accuracy=80.0,
        post_coverage_accuracy=80.0,
        had_candidates=True,
    )
    assert out.decision == "continue"
    assert out.should_rollback is False
    assert out.delta_pp == 0.0


def test_coverage_no_candidates_renders_free_probe_rung() -> None:
    # §13.5: a warm/well-documented space finds nothing — the rung still renders.
    out = decide_coverage_outcome(
        frozen_baseline_accuracy=91.7,
        post_coverage_accuracy=91.7,
        had_candidates=False,
    )
    assert out.decision == "continue"
    assert out.should_rollback is False
    assert out.decision_reason == "no_enrichment_candidates"


def test_coverage_no_candidates_never_rolls_back_even_if_acc_differs() -> None:
    # Without candidates there is nothing to roll back; we never measure a delta.
    out = decide_coverage_outcome(
        frozen_baseline_accuracy=80.0,
        post_coverage_accuracy=50.0,
        had_candidates=False,
    )
    assert out.should_rollback is False
    assert out.decision_reason == "no_enrichment_candidates"


# ── decide_loop_terminal_reason: target vs budget ──────────────────────────
def test_terminal_target_reached_takes_priority() -> None:
    assert (
        decide_loop_terminal_reason(
            best_accuracy=90.0, target_accuracy=90.0,
            surgical_used=3, max_attempts=3,
        )
        == "TARGET_REACHED"
    )


def test_terminal_max_attempts_when_budget_exhausted() -> None:
    assert (
        decide_loop_terminal_reason(
            best_accuracy=82.0, target_accuracy=90.0,
            surgical_used=3, max_attempts=3,
        )
        == "MAX_ATTEMPTS"
    )


def test_terminal_none_when_budget_remains_and_target_unmet() -> None:
    assert (
        decide_loop_terminal_reason(
            best_accuracy=82.0, target_accuracy=90.0,
            surgical_used=1, max_attempts=3,
        )
        is None
    )


# ── build_loop_state: the per-attempt column contract ──────────────────────
def test_build_loop_state_surfaces_phase8_columns() -> None:
    ls = build_loop_state(
        attempt_no=2,
        attempt_mode="surgical",
        surgical_attempts_used=1,
        max_attempts=3,
        target_accuracy=90.0,
        best_accuracy=88.5,
        best_iteration=2,
        current_hypothesis={"ag_id": "AG1", "levers": ["5"]},
        decision="accept",
        decision_reason="surgical_lift",
        terminal_reason=None,
    )
    assert ls["attempt_no"] == 2
    assert ls["attempt_mode"] == "surgical"
    assert ls["surgical_attempts_used"] == 1
    assert ls["max_attempts"] == 3
    assert ls["target_accuracy"] == 90.0
    assert ls["best_accuracy"] == 88.5
    assert ls["decision"] == "accept"


def test_build_loop_state_rejects_unknown_terminal_reason() -> None:
    with pytest.raises(ValueError):
        build_loop_state(
            attempt_no=2, attempt_mode="surgical", surgical_attempts_used=1,
            max_attempts=3, target_accuracy=90.0, terminal_reason="NOPE",
        )


def test_loop_terminal_reasons_is_the_arch_vocabulary() -> None:
    # The arch §5.1 five + EVAL_BUDGET_EXHAUSTED (Phase-8 NB1): the eval-budget
    # wall can stop the loop while surgical attempts remain, so it is its own
    # typed reason rather than being mislabeled MAX_ATTEMPTS.
    assert LOOP_TERMINAL_REASONS == frozenset(
        {
            "TARGET_REACHED",
            "MAX_ATTEMPTS",
            "NO_NEW_HYPOTHESIS",
            "EVAL_INVALID",
            "LOOP_STATE_INVALID",
            "EVAL_BUDGET_EXHAUSTED",
        }
    )
