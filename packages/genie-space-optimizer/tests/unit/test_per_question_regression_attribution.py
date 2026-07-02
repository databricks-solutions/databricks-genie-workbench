"""Tests for Task 4: per-question pass/fail transition tracker.

The retail run accepted AG2 with a net regression because aggregate
averages hid that some previously-passing qids flipped to failing.
These tests pin the contract that:

* Every ``pass_map_after`` qid maps to one of the four typed
  transitions.
* A non-suppressed ``pass_to_fail`` produces a non-empty
  ``blocking_qids`` and ``accepted=False``.
* Suppressed qids (GT correction queue / quarantine) produce a
  recorded transition but never block acceptance.
* Persisted rows omit ``hold_pass`` (noise floor) and carry the
  cluster/proposal/patch attribution chain so the rollback row in the
  decision audit can be joined back to the patch responsible.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.per_question_regression import (
    FAIL_TO_PASS,
    HOLD_FAIL,
    HOLD_PASS,
    PASS_TO_FAIL,
    RegressionVerdict,
    compute_question_transitions,
)


# ── compute_question_transitions ─────────────────────────────────────


def test_classifies_all_four_transitions():
    verdict = compute_question_transitions(
        pass_map_before={"a": True, "b": True, "c": False, "d": False},
        pass_map_after={"a": True, "b": False, "c": True, "d": False},
    )

    assert verdict.transitions == {
        "a": HOLD_PASS,
        "b": PASS_TO_FAIL,
        "c": FAIL_TO_PASS,
        "d": HOLD_FAIL,
    }
    assert verdict.blocking_qids == ["b"]
    assert verdict.fixed_qids == ["c"]
    assert verdict.accepted is False


def test_accepts_when_no_blocking_pass_to_fail():
    verdict = compute_question_transitions(
        pass_map_before={"a": True, "b": False},
        pass_map_after={"a": True, "b": True},
    )

    assert verdict.blocking_qids == []
    assert verdict.fixed_qids == ["b"]
    assert verdict.accepted is True


def test_suppressed_pass_to_fail_does_not_block_acceptance():
    """A qid in the GT-correction queue (or quarantine) flipping
    pass_to_fail is recorded but doesn't roll back the AG."""
    verdict = compute_question_transitions(
        pass_map_before={"q11": True, "q19": True},
        pass_map_after={"q11": False, "q19": False},
        suppressed_qids={"q11"},
    )

    # Both transitions recorded
    assert verdict.transitions == {"q11": PASS_TO_FAIL, "q19": PASS_TO_FAIL}
    # Only the non-suppressed one blocks
    assert verdict.blocking_qids == ["q19"]
    assert verdict.accepted is False


def test_all_pass_to_fail_suppressed_yields_acceptance():
    verdict = compute_question_transitions(
        pass_map_before={"q11": True},
        pass_map_after={"q11": False},
        suppressed_qids={"q11"},
    )

    assert verdict.blocking_qids == []
    assert verdict.accepted is True


def test_missing_qid_in_before_treated_as_failing():
    """Brand-new qids that pass count as fail_to_pass; brand-new
    failing qids count as hold_fail. Either way, never blocking."""
    verdict = compute_question_transitions(
        pass_map_before={},
        pass_map_after={"new_pass": True, "new_fail": False},
    )

    assert verdict.transitions == {
        "new_pass": FAIL_TO_PASS,
        "new_fail": HOLD_FAIL,
    }
    assert verdict.blocking_qids == []
    assert verdict.accepted is True


def test_empty_after_map_is_trivially_accepted():
    verdict = compute_question_transitions(
        pass_map_before={"a": True}, pass_map_after={},
    )

    assert verdict.transitions == {}
    assert verdict.accepted is True


def test_verdict_is_a_frozen_dataclass():
    verdict = compute_question_transitions(
        pass_map_before={"a": True}, pass_map_after={"a": True},
    )

    assert isinstance(verdict, RegressionVerdict)
    try:
        verdict.accepted = False  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("RegressionVerdict must be frozen")
