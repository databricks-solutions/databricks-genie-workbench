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


def test_existing_hard_still_hard_outside_target_field_exists_on_dataclass() -> None:
    """C16-T3 adds a first-class bucket for QIDs that were hard at
    baseline, remained hard at candidate, and are not in the AG's
    declared target set. Default empty tuple preserves byte-stability
    on legacy replay fixtures.
    """
    fields = ControlPlaneAcceptance.__dataclass_fields__
    assert "existing_hard_still_hard_outside_target_qids" in fields
    assert fields["existing_hard_still_hard_outside_target_qids"].default == ()


def test_existing_hard_outside_target_bucket_populated_when_baseline_hard_non_target_stays_hard() -> None:
    """Baseline-hard non-target QIDs that remain hard at candidate
    must land in `existing_hard_still_hard_outside_target_qids`. They
    are NOT new regressions, so they must NOT appear in
    `out_of_target_regressed_qids`.
    """
    decision = decide_control_plane_acceptance(
        baseline_accuracy=80.0,
        candidate_accuracy=85.0,
        target_qids=["target_qid"],
        pre_rows=[
            _hard_row("target_qid"),       # target, hard pre
            _hard_row("non_target_stays"), # non-target, hard pre, stays hard
            _passing_row("non_target_ok"), # non-target, passing pre
        ],
        post_rows=[
            _passing_row("target_qid"),    # target fixed
            _hard_row("non_target_stays"), # non-target still hard
            _passing_row("non_target_ok"), # non-target still passing
        ],
    )
    assert decision.existing_hard_still_hard_outside_target_qids == ("non_target_stays",)
    assert decision.out_of_target_regressed_qids == ()
    assert decision.target_fixed_qids == ("target_qid",)


def test_existing_hard_bucket_excludes_target_qids_and_new_regressions() -> None:
    """Disjointness check: the new bucket must not double-count
    QIDs that already land in target_still_hard_qids or in
    out_of_target_regressed_qids.
    """
    decision = decide_control_plane_acceptance(
        baseline_accuracy=80.0,
        candidate_accuracy=70.0,
        target_qids=["target_still"],
        pre_rows=[
            _hard_row("target_still"),      # target, hard pre, stays hard
            _hard_row("non_target_stuck"),  # non-target, hard pre, stays hard
            _passing_row("non_target_ok"),  # non-target, passing pre
        ],
        post_rows=[
            _hard_row("target_still"),      # target still hard
            _hard_row("non_target_stuck"),  # non-target still hard
            _hard_row("non_target_ok"),     # non-target NEW hard (regression)
        ],
    )
    assert decision.target_still_hard_qids == ("target_still",)
    assert decision.existing_hard_still_hard_outside_target_qids == ("non_target_stuck",)
    assert decision.out_of_target_regressed_qids == ("non_target_ok",)
    new_bucket = set(decision.existing_hard_still_hard_outside_target_qids)
    assert not (new_bucket & set(decision.target_still_hard_qids))
    assert not (new_bucket & set(decision.out_of_target_regressed_qids))


def test_soft_signal_baseline_routes_to_soft_to_hard_not_unknown() -> None:
    """C16-T3 second sub-task: a non-target QID that was a soft signal
    (arbiter-rescued with judge=no) pre-iter and became hard post-iter
    must land in `soft_to_hard_regressed_qids`, never in
    `unknown_to_hard_regressed_qids`. Lock-in regression — the routing
    is already implemented in decide_control_plane_acceptance; this
    test pins it so a future cleanup cannot silently fold soft into
    unknown.
    """
    decision = decide_control_plane_acceptance(
        baseline_accuracy=100.0,
        candidate_accuracy=80.0,
        target_qids=["target_qid"],
        pre_rows=[
            _passing_row("target_qid"),
            _soft_row("non_target_soft"),
        ],
        post_rows=[
            _passing_row("target_qid"),
            _hard_row("non_target_soft"),
        ],
    )
    assert decision.soft_to_hard_regressed_qids == ("non_target_soft",)
    assert decision.passing_to_hard_regressed_qids == ()
    assert decision.unknown_to_hard_regressed_qids == ()


