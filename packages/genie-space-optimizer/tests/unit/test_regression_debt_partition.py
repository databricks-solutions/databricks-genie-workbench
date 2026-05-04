"""Regression-debt accounting completeness.

Today decide_control_plane_acceptance produces soft_to_hard and
passing_to_hard buckets but no third bucket for the residual case.
This suite pins:

1. The new ``unknown_to_hard_regressed_qids`` field exists.
2. The union of the three buckets equals ``out_of_target_regressed``.
3. Each new-hard qid lands in exactly one bucket (no double-count).
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    assert_regression_debt_partition_complete,
    decide_control_plane_acceptance,
)


def _hard_row(qid: str) -> dict:
    """Build a row that ``row_is_hard_failure`` classifies as hard."""
    return {
        "question_id": qid,
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "neither_correct",
    }


def _passing_row(qid: str) -> dict:
    """Build a row classified as passing (rc=yes, no judges fail)."""
    return {
        "question_id": qid,
        "feedback/result_correctness/value": "yes",
        "feedback/arbiter/value": "both_correct",
        "feedback/completeness/value": "yes",
        "feedback/response_quality/value": "yes",
        "feedback/logical_accuracy/value": "yes",
    }


def _soft_row(qid: str) -> dict:
    """Build a row that is arbiter-rescued but has a judge=no
    (qualifies as actionable soft signal).
    """
    return {
        "question_id": qid,
        "feedback/result_correctness/value": "yes",
        "feedback/arbiter/value": "both_correct",
        "feedback/completeness/value": "no",
    }


def test_unknown_to_hard_field_exists_on_dataclass() -> None:
    fields = ControlPlaneAcceptance.__dataclass_fields__
    assert "unknown_to_hard_regressed_qids" in fields
    assert fields["unknown_to_hard_regressed_qids"].default == ()


def test_passing_to_hard_when_pre_row_was_passing() -> None:
    decision = decide_control_plane_acceptance(
        baseline_accuracy=100.0,
        candidate_accuracy=95.0,
        target_qids=["target_qid"],
        pre_rows=[_passing_row("non_target")],
        post_rows=[_hard_row("non_target")],
    )
    assert decision.passing_to_hard_regressed_qids == ("non_target",)
    assert decision.soft_to_hard_regressed_qids == ()
    assert decision.unknown_to_hard_regressed_qids == ()


def test_soft_to_hard_when_pre_row_was_soft_signal() -> None:
    """Reproducer for the gs_001-style attribution gap. A qid that
    was a soft signal pre-iter and became hard post-iter must land
    in soft_to_hard.
    """
    decision = decide_control_plane_acceptance(
        baseline_accuracy=100.0,
        candidate_accuracy=95.0,
        target_qids=["target_qid"],
        pre_rows=[_soft_row("non_target")],
        post_rows=[_hard_row("non_target")],
    )
    assert decision.soft_to_hard_regressed_qids == ("non_target",)
    assert decision.passing_to_hard_regressed_qids == ()
    assert decision.unknown_to_hard_regressed_qids == ()


def test_unknown_to_hard_when_pre_row_missing() -> None:
    """A qid that has no pre-row at all should not silently inflate
    passing_to_hard. The residual bucket catches it so the operator
    can see something is off with the input data.
    """
    decision = decide_control_plane_acceptance(
        baseline_accuracy=100.0,
        candidate_accuracy=95.0,
        target_qids=["target_qid"],
        pre_rows=[],  # empty - the qid has no pre-state
        post_rows=[_hard_row("non_target")],
    )
    # missing_pre_rows reason fires before the bucket logic, so the
    # invariant still holds. Sub-test: when pre_rows is non-empty
    # but the specific qid is absent, the residual bucket fires.
    decision = decide_control_plane_acceptance(
        baseline_accuracy=100.0,
        candidate_accuracy=95.0,
        target_qids=["target_qid"],
        pre_rows=[_passing_row("other_qid")],  # non-empty, but missing non_target
        post_rows=[_hard_row("non_target"), _passing_row("other_qid")],
    )
    assert decision.unknown_to_hard_regressed_qids == ("non_target",)
    assert decision.soft_to_hard_regressed_qids == ()
    assert decision.passing_to_hard_regressed_qids == ()


def test_partition_invariant_holds_on_mixed_inputs() -> None:
    decision = decide_control_plane_acceptance(
        baseline_accuracy=100.0,
        candidate_accuracy=70.0,
        target_qids=["target_qid"],
        pre_rows=[
            _passing_row("a_passing"),
            _soft_row("b_soft"),
            _passing_row("c_other"),
        ],
        post_rows=[
            _hard_row("a_passing"),
            _hard_row("b_soft"),
            _hard_row("d_unknown"),
            _passing_row("c_other"),
        ],
    )

    union = (
        set(decision.soft_to_hard_regressed_qids)
        | set(decision.passing_to_hard_regressed_qids)
        | set(decision.unknown_to_hard_regressed_qids)
    )
    out_of_target = set(decision.out_of_target_regressed_qids)
    assert union == out_of_target

    soft = set(decision.soft_to_hard_regressed_qids)
    passing = set(decision.passing_to_hard_regressed_qids)
    unknown = set(decision.unknown_to_hard_regressed_qids)
    assert (soft & passing) == set()
    assert (soft & unknown) == set()
    assert (passing & unknown) == set()


def test_assert_regression_debt_partition_complete_passes_on_valid_decision() -> None:
    decision = decide_control_plane_acceptance(
        baseline_accuracy=100.0,
        candidate_accuracy=95.0,
        target_qids=["target_qid"],
        pre_rows=[_passing_row("non_target")],
        post_rows=[_hard_row("non_target")],
    )
    assert_regression_debt_partition_complete(decision)


def test_assert_regression_debt_partition_complete_raises_on_orphan_qid(monkeypatch) -> None:
    """Construct a malformed ControlPlaneAcceptance where a qid is in
    out_of_target_regressed but in NO sub-bucket. The assertion must
    raise.
    """
    monkeypatch.setenv("GSO_REGRESSION_DEBT_INVARIANT", "1")
    bad = ControlPlaneAcceptance(
        accepted=False,
        reason_code="rejected_unbounded_collateral",
        baseline_accuracy=100.0,
        candidate_accuracy=90.0,
        delta_pp=-10.0,
        target_qids=("target_qid",),
        target_fixed_qids=(),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=("orphan_qid",),
        soft_to_hard_regressed_qids=(),
        passing_to_hard_regressed_qids=(),
        unknown_to_hard_regressed_qids=(),
    )
    with pytest.raises(AssertionError, match="regression-debt partition incomplete"):
        assert_regression_debt_partition_complete(bad)


