"""Unit tests for _compute_arbiter_adjusted_accuracy (Bug #2 + Bug #3).

These tests lock in the denominator contract and per-row exclusion reasons:

* evaluated_count is the denominator of overall_accuracy
* excluded rows are NOT counted as failures or successes
* Per-row `exclusions` carry stable reason_codes matching EXCLUSION_*

Regression target: the "12/14 = 85.7% on one screen, different on another"
UI mismatch described in Bug #2.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.evaluation import (
    EXCLUSION_BOTH_EMPTY,
    EXCLUSION_GENIE_RESULT_UNAVAILABLE,
    EXCLUSION_GT_EXCLUDED,
    EXCLUSION_QUARANTINED,
    EXCLUSION_TEMPORAL_STALE,
    _compute_arbiter_adjusted_accuracy,
)


def _row(
    qid: str,
    *,
    question: str = "",
    correct: bool = True,
    gt_excluded: bool = False,
    both_empty: bool = False,
    genie_unavailable: bool = False,
    err_message: str = "",
    arbiter: str | None = None,
) -> dict:
    """Build a minimal evaluation row in the shape _extract_row_signals expects."""
    row: dict = {
        "inputs/question_id": qid,
        "inputs/question": question or f"Question {qid}",
    }
    if gt_excluded:
        row["result_correctness/value"] = "excluded"
    elif correct:
        row["result_correctness/value"] = "yes"
    else:
        row["result_correctness/value"] = "no"

    if both_empty:
        row["outputs/comparison/error_type"] = "both_empty"
    if genie_unavailable:
        row["outputs/comparison/error_type"] = "genie_result_unavailable"
    if err_message:
        row["outputs/comparison/error"] = err_message
    if arbiter:
        row["arbiter/value"] = arbiter
    return row


def test_baseline_denominator_matches_correct_count() -> None:
    """12 correct / 14 rows with 0 exclusions = 85.71% accuracy.

    This is the exact scenario the user called out: the main screen and KPI
    screen must use the same denominator. The contract: evaluated_count is
    the denominator of accuracy_pct, full stop.
    """
    rows = [_row(f"q{i}", correct=True) for i in range(12)] + [
        _row(f"q{i}", correct=False) for i in range(12, 14)
    ]
    result = _compute_arbiter_adjusted_accuracy(rows)
    assert result.evaluated_count == 14
    assert result.correct_count == 12
    assert result.excluded_count == 0
    # 12/14 = 85.71... (rounded to 2dp in the API, 1dp in the UI card)
    assert round(result.accuracy_pct, 2) == 85.71
    assert result.exclusions == []


def test_exclusions_shrink_denominator_not_numerator() -> None:
    """Excluded rows must not count against or toward the score."""
    rows = [
        _row("q1", correct=True),
        _row("q2", correct=True),
        _row("q3", correct=False),
        _row("q4", gt_excluded=True),
        _row("q5", both_empty=True),
        _row("q6", genie_unavailable=True),
    ]
    result = _compute_arbiter_adjusted_accuracy(rows)
    assert result.evaluated_count == 3
    assert result.correct_count == 2
    assert result.excluded_count == 3
    assert round(result.accuracy_pct, 2) == 66.67


def test_exclusions_carry_stable_reason_codes() -> None:
    """Bug #3: the drill-down needs a stable reason_code per excluded row."""
    rows = [
        _row("q_gt_excl", gt_excluded=True, err_message="table not found"),
        _row("q_both_empty", both_empty=True),
        _row(
            "q_genie_unavail",
            genie_unavailable=True,
            err_message="SQL execution timeout",
        ),
    ]
    # Quarantined qids arrive via the keyword arg, not the row payload.
    result = _compute_arbiter_adjusted_accuracy(
        rows + [_row("q_quarantined")],
        quarantined_qids={"q_quarantined"},
    )
    by_qid = {ex.question_id: ex for ex in result.exclusions}

    assert by_qid["q_gt_excl"].reason_code == EXCLUSION_GT_EXCLUDED
    assert "table not found" in by_qid["q_gt_excl"].reason_detail
    assert by_qid["q_both_empty"].reason_code == EXCLUSION_BOTH_EMPTY
    assert by_qid["q_genie_unavail"].reason_code == EXCLUSION_GENIE_RESULT_UNAVAILABLE
    assert "timeout" in by_qid["q_genie_unavail"].reason_detail
    assert by_qid["q_quarantined"].reason_code == EXCLUSION_QUARANTINED

    for ex in result.exclusions:
        assert ex.reason_detail, f"Missing reason_detail for {ex.question_id}"


def test_temporal_stale_excluded() -> None:
    """Temporally stale question IDs (from preflight signal) are excluded."""
    rows = [_row("q1", correct=True), _row("q2", correct=True)]
    result = _compute_arbiter_adjusted_accuracy(
        rows, temporal_stale_qids={"q2"}
    )
    assert result.evaluated_count == 1
    assert result.correct_count == 1
    assert result.excluded_count == 1
    assert result.exclusions[0].question_id == "q2"
    assert result.exclusions[0].reason_code == EXCLUSION_TEMPORAL_STALE


def test_arbiter_override_counts_correct_when_rc_is_no() -> None:
    """Arbiter override flips an rc=no row back into correct without shrinking
    the denominator — this is the "Genie was actually right" case.
    """
    rows = [
        _row("q1", correct=False, arbiter="genie_correct"),
        _row("q2", correct=False, arbiter="skipped"),
        _row("q3", correct=True),
    ]
    result = _compute_arbiter_adjusted_accuracy(rows)
    assert result.evaluated_count == 3
    assert result.correct_count == 2
    assert "q1" not in result.failure_ids
    assert "q2" in result.failure_ids


def test_empty_input_safe() -> None:
    """Empty evaluation results must not crash and must return 0s."""
    result = _compute_arbiter_adjusted_accuracy([])
    assert result.evaluated_count == 0
    assert result.correct_count == 0
    assert result.excluded_count == 0
    assert result.accuracy_pct == 0
    assert result.exclusions == []


def test_all_rows_excluded_yields_zero_accuracy() -> None:
    """Edge case: if every row is excluded, we must not divide by zero."""
    rows = [_row(f"q{i}", gt_excluded=True) for i in range(5)]
    result = _compute_arbiter_adjusted_accuracy(rows)
    assert result.evaluated_count == 0
    assert result.excluded_count == 5
    assert result.accuracy_pct == 0
