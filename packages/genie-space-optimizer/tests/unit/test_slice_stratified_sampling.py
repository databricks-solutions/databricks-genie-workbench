"""Phase 5.1: stratified slice composition.

The slice gate previously sampled only from currently-failing
benchmarks, so any patch targeting those failures rubber-stamped the
slice while still regressing baseline-passing rows. With stratified
mode the slice is augmented with a 40% sample of baseline-passing
questions, turning the slice gate into a regression detector.
"""

from __future__ import annotations

from genie_space_optimizer.optimization import evaluation as evl


def _bench(qid: str, *, tables: list[str] | None = None) -> dict:
    return {
        "id": qid,
        "required_tables": tables or [],
        "required_columns": [],
    }


def test_stratified_slice_includes_baseline_passing_rows() -> None:
    benchmarks = [_bench(f"q{i}") for i in range(1, 11)]
    failing_qids = {"q1", "q2", "q3"}
    baseline_passing = {b["id"] for b in benchmarks} - failing_qids

    slice_b = evl.filter_benchmarks_by_scope(
        benchmarks,
        scope="slice",
        patched_objects=None,
        affected_question_ids=failing_qids,
        baseline_passing_qids=baseline_passing,
        stratified=True,
    )
    slice_qids = {b["id"] for b in slice_b}
    # All targeted qids included.
    assert failing_qids.issubset(slice_qids)
    # At least one baseline-passing qid appears as a regression detector.
    assert slice_qids & baseline_passing


def test_stratified_slice_60_40_ratio_when_pool_is_large_enough() -> None:
    benchmarks = [_bench(f"q{i}") for i in range(1, 21)]
    failing_qids = {f"q{i}" for i in range(1, 7)}  # 6 targeted
    baseline_passing = {b["id"] for b in benchmarks} - failing_qids

    slice_b = evl.filter_benchmarks_by_scope(
        benchmarks,
        scope="slice",
        affected_question_ids=failing_qids,
        baseline_passing_qids=baseline_passing,
        stratified=True,
    )
    n_targeted_in = sum(1 for b in slice_b if b["id"] in failing_qids)
    n_passing_in = sum(1 for b in slice_b if b["id"] in baseline_passing)
    # Targeted: 6. Regression: floor(6 * 40/60) = 4.
    assert n_targeted_in == 6
    assert n_passing_in == 4


def test_stratified_disabled_returns_targeted_only() -> None:
    benchmarks = [_bench(f"q{i}") for i in range(1, 11)]
    failing_qids = {"q1", "q2"}
    baseline_passing = {b["id"] for b in benchmarks} - failing_qids

    slice_b = evl.filter_benchmarks_by_scope(
        benchmarks,
        scope="slice",
        affected_question_ids=failing_qids,
        baseline_passing_qids=baseline_passing,
        stratified=False,
    )
    slice_qids = {b["id"] for b in slice_b}
    assert slice_qids == failing_qids


def test_stratified_no_op_when_no_baseline_passing_pool() -> None:
    benchmarks = [_bench(f"q{i}") for i in range(1, 5)]
    failing_qids = {"q1", "q2"}
    slice_b = evl.filter_benchmarks_by_scope(
        benchmarks,
        scope="slice",
        affected_question_ids=failing_qids,
        baseline_passing_qids=None,
        stratified=True,
    )
    slice_qids = {b["id"] for b in slice_b}
    # Falls back to targeted-only when baseline_passing_qids is None.
    assert slice_qids == failing_qids


def test_full_scope_unchanged_by_stratified_flag() -> None:
    benchmarks = [_bench(f"q{i}") for i in range(1, 6)]
    out = evl.filter_benchmarks_by_scope(
        benchmarks,
        scope="full",
        baseline_passing_qids={"q1"},
        stratified=True,
    )
    assert out == benchmarks
