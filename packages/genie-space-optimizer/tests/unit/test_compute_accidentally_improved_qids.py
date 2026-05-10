"""Cycle 14-C T1 — `compute_accidentally_improved_qids` pure helper.

Anchor: airline run 1105451933925748 iter 1 — `target_qids=[gs_024]`
remained STILL_HARD; the candidate's +12.5pp gain came from QIDs
the strategist did not name. Those QIDs are
`accidentally_improved`.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    compute_accidentally_improved_qids,
)


def _row(qid: str, *, status: str) -> dict:
    """Build a minimal eval row that ``hard_failure_qids`` understands."""
    return {
        "question_id": qid,
        "row_status": status,
    }


def test_empty_inputs_return_empty_tuple() -> None:
    assert compute_accidentally_improved_qids(
        pre_rows=(), post_rows=(), target_qids=(),
    ) == ()


def test_target_only_fixed_returns_empty_tuple() -> None:
    """Strategist named gs_001; gs_001 flipped from hard to passing.
    No accidental improvements."""
    pre = [_row("gs_001", status="hard")]
    post = [_row("gs_001", status="passing")]
    assert compute_accidentally_improved_qids(
        pre_rows=pre, post_rows=post, target_qids=("gs_001",),
    ) == ()


def test_airline_anchor_shape_returns_non_target_improvements() -> None:
    """Airline anchor: target=gs_024 stays hard; gs_007/gs_009/gs_013
    were hard at baseline and passing in candidate. The three
    flipped QIDs are accidentally_improved."""
    pre = [
        _row("gs_024", status="hard"),
        _row("gs_007", status="hard"),
        _row("gs_009", status="hard"),
        _row("gs_013", status="hard"),
        _row("gs_999", status="passing"),
    ]
    post = [
        _row("gs_024", status="hard"),
        _row("gs_007", status="passing"),
        _row("gs_009", status="passing"),
        _row("gs_013", status="passing"),
        _row("gs_999", status="passing"),
    ]
    result = compute_accidentally_improved_qids(
        pre_rows=pre, post_rows=post, target_qids=("gs_024",),
    )
    assert tuple(sorted(result)) == ("gs_007", "gs_009", "gs_013")


def test_soft_passing_at_post_not_counted_as_improved() -> None:
    """Helper requires a post-row to be `passing` (not `soft`) to
    count as accidentally improved. SOFT signals stay separate; the
    canonical-render `target_soft_passing_qids` field surfaces them
    via a different bucket."""
    pre = [_row("gs_005", status="hard")]
    post = [_row("gs_005", status="soft")]
    assert compute_accidentally_improved_qids(
        pre_rows=pre, post_rows=post, target_qids=(),
    ) == ()


def test_missing_pre_rows_treated_as_passing_skipped() -> None:
    """If a QID has no pre_row at all, it cannot be classified as
    baseline-failed; therefore it cannot have flipped."""
    pre: list = []
    post = [_row("gs_010", status="passing")]
    assert compute_accidentally_improved_qids(
        pre_rows=pre, post_rows=post, target_qids=(),
    ) == ()


def test_target_qid_passing_in_candidate_excluded_from_accidental() -> None:
    """If a named target ALSO flipped, it counts as a target_fixed_qid
    (separate field) and must NOT also appear in accidentally_improved
    — the reattribution must be disjoint."""
    pre = [_row("gs_001", status="hard"), _row("gs_002", status="hard")]
    post = [_row("gs_001", status="passing"), _row("gs_002", status="passing")]
    result = compute_accidentally_improved_qids(
        pre_rows=pre, post_rows=post, target_qids=("gs_001",),
    )
    assert tuple(sorted(result)) == ("gs_002",)


def test_accidentally_improved_with_production_shape_rows() -> None:
    """Anchor regression test for D-3 ext.

    Production rows use ``result_correctness`` + ``arbiter``, NOT
    ``row_status``. The pre-fix implementation read ``row_status``
    only and silently returned () in production. After the fix,
    both shapes resolve via EvalRow.is_passing().
    """
    pre_rows = [
        {"question_id": "gs_001", "result_correctness": "no",  "arbiter": "hard"},
        {"question_id": "gs_007", "result_correctness": "no",  "arbiter": "hard"},
        {"question_id": "gs_009", "result_correctness": "no",  "arbiter": "hard"},
        {"question_id": "gs_016", "result_correctness": "no",  "arbiter": "hard"},
        {"question_id": "gs_024", "result_correctness": "no",  "arbiter": "hard"},
    ]
    post_rows = [
        {"question_id": "gs_001", "result_correctness": "yes", "arbiter": "n/a"},
        {"question_id": "gs_007", "result_correctness": "yes", "arbiter": "n/a"},
        {"question_id": "gs_009", "result_correctness": "yes", "arbiter": "n/a"},
        {"question_id": "gs_016", "result_correctness": "yes", "arbiter": "n/a"},
        {"question_id": "gs_024", "result_correctness": "no",  "arbiter": "hard"},
    ]
    from genie_space_optimizer.optimization.control_plane import (
        compute_accidentally_improved_qids,
    )

    out = compute_accidentally_improved_qids(
        pre_rows=pre_rows,
        post_rows=post_rows,
        target_qids=("gs_024",),
    )
    # Target gs_024 stayed hard (attribution drift). Four other
    # baseline-hard QIDs flipped to passing without being targeted.
    assert out == ("gs_001", "gs_007", "gs_009", "gs_016")


def test_result_is_sorted_for_byte_stability() -> None:
    """The helper's return order is canonical (sorted) so the
    resulting `ControlPlaneAcceptance` field is byte-stable across
    runs."""
    pre = [
        _row("gs_zzz", status="hard"), _row("gs_aaa", status="hard"),
        _row("gs_mmm", status="hard"),
    ]
    post = [
        _row("gs_zzz", status="passing"), _row("gs_aaa", status="passing"),
        _row("gs_mmm", status="passing"),
    ]
    result = compute_accidentally_improved_qids(
        pre_rows=pre, post_rows=post, target_qids=(),
    )
    assert result == ("gs_aaa", "gs_mmm", "gs_zzz")
