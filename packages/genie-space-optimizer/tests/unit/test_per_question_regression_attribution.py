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
    build_question_regression_rows,
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


# ── build_question_regression_rows ─────────────────────────────────


def test_persisted_rows_exclude_hold_pass_noise():
    verdict = compute_question_transitions(
        pass_map_before={"a": True, "b": True, "c": False},
        pass_map_after={"a": True, "b": False, "c": True},
    )

    rows = build_question_regression_rows(
        run_id="r-1",
        iteration=2,
        ag_id="AG2",
        verdict=verdict,
    )

    qids = {r["question_id"] for r in rows}
    transitions = {r["question_id"]: r["transition"] for r in rows}
    # ``a`` was a hold_pass — excluded.
    assert qids == {"b", "c"}
    assert transitions == {"b": PASS_TO_FAIL, "c": FAIL_TO_PASS}


def test_persisted_rows_carry_cluster_proposal_patch_chain():
    """The plan's attribution chain: a rollback row in the decision
    audit must be joinable back via these fields to the patch that
    broke the qid."""
    verdict = compute_question_transitions(
        pass_map_before={"q19": True}, pass_map_after={"q19": False},
    )

    rows = build_question_regression_rows(
        run_id="r-1",
        iteration=2,
        ag_id="AG2",
        verdict=verdict,
        cluster_ids_by_qid={"q19": ["H001"]},
        proposal_ids_by_qid={"q19": ["p1"]},
        applied_patch_ids=["applied_xyz"],
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["transition"] == PASS_TO_FAIL
    assert row["was_passing"] is True
    assert row["is_passing"] is False
    assert row["source_cluster_ids"] == ["H001"]
    assert row["source_proposal_ids"] == ["p1"]
    assert row["applied_patch_ids"] == ["applied_xyz"]
    assert row["suppressed"] is False


def test_persisted_rows_mark_suppression():
    verdict = compute_question_transitions(
        pass_map_before={"q11": True, "q19": True},
        pass_map_after={"q11": False, "q19": False},
        suppressed_qids={"q11"},
    )

    rows = build_question_regression_rows(
        run_id="r", iteration=1, ag_id="AG1",
        verdict=verdict, suppressed_qids={"q11"},
    )

    by_qid = {r["question_id"]: r for r in rows}
    assert by_qid["q11"]["suppressed"] is True
    assert by_qid["q19"]["suppressed"] is False


def test_carries_pre_and_post_arbiter_pass_states():
    verdict = compute_question_transitions(
        pass_map_before={"q1": True}, pass_map_after={"q1": False},
    )

    rows = build_question_regression_rows(
        run_id="r", iteration=1, ag_id="AG1", verdict=verdict,
        pre_arbiter_before={"q1": False},  # arbiter rescued it before
        pre_arbiter_after={"q1": False},
        post_arbiter_before={"q1": True},
        post_arbiter_after={"q1": False},
    )

    row = rows[0]
    assert row["pre_arbiter_before"] is False
    assert row["pre_arbiter_after"] is False
    assert row["post_arbiter_before"] is True
    assert row["post_arbiter_after"] is False
