"""Unit tests for the subset-first 3-gate eval orchestration (GSO v2 Phase 1)."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.eval_budget import EvalBudget
from genie_space_optimizer.optimization.eval_gates import (
    GateOutcome,
    ThreeGateResult,
    make_regression_check,
    run_three_gate,
    select_p0_qids,
    select_slice_qids,
)
from genie_space_optimizer.optimization.eval_runner import EvalRunResult


class RecordingRunner:
    """An EvalRunner that returns canned accuracies and records its calls."""

    def __init__(self, accuracy_by_scope: dict, *, wall=60.0, status="DONE"):
        self._acc = accuracy_by_scope
        self._wall = wall
        self._status = status
        self.calls: list = []

    def run(self, space_id, benchmark_question_ids=None, *, eval_scope="full"):
        self.calls.append((eval_scope, benchmark_question_ids))
        acc = self._acc[eval_scope]
        # Encode accuracy as num_correct/num_questions over 100 questions.
        return EvalRunResult(
            eval_run_id=f"er-{eval_scope}",
            status=self._status,
            num_correct=int(round(acc)),
            num_done=100,
            num_needs_review=0,
            num_questions=100,
            rows=[],
            wall_clock_seconds=self._wall,
            eval_scope=eval_scope,
            requested_question_ids=(tuple(benchmark_question_ids) if benchmark_question_ids is not None else None),
        )


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# ── subset selection ─────────────────────────────────────────────────────────
def test_select_slice_qids_caps_and_dedupes() -> None:
    qids = select_slice_qids(["a", "b", "b", "c", "d"], max_questions=3)
    assert qids == ["a", "b", "c"]


def test_select_slice_qids_falls_back_to_all_when_no_failing() -> None:
    qids = select_slice_qids([], all_qids=["x", "y", "z"], max_questions=2)
    assert qids == ["x", "y"]


def test_select_p0_qids_caps() -> None:
    assert select_p0_qids(["p1", "p2", "p3"], max_questions=2) == ["p1", "p2"]


# ── regression predicate ─────────────────────────────────────────────────────
def test_make_regression_check() -> None:
    regressed = make_regression_check(80.0, tolerance_pp=2.0)  # floor = 78
    below = EvalRunResult("e", "DONE", 70, 100, 0, 100, [], 1.0)
    at_floor = EvalRunResult("e", "DONE", 78, 100, 0, 100, [], 1.0)
    above = EvalRunResult("e", "DONE", 90, 100, 0, 100, [], 1.0)
    failed = EvalRunResult("e", "EVALUATION_FAILED", 100, 100, 0, 100, [], 1.0)
    assert regressed(below) is True
    assert regressed(at_floor) is False
    assert regressed(above) is False
    assert regressed(failed) is True  # non-success always regresses


# ── 3-gate sequencing ────────────────────────────────────────────────────────
def _passing_check(_result) -> bool:
    return False  # nothing regresses


def test_all_gates_pass_runs_full() -> None:
    runner = RecordingRunner({"slice": 90, "p0": 90, "full": 90})
    out = run_three_gate(
        runner,
        "space-1",
        slice_qids=["q1", "q2"],
        p0_qids=["q3"],
        full_qids=None,
        regressed=_passing_check,
    )
    assert isinstance(out, ThreeGateResult)
    assert out.accepted is True
    assert out.ran_full is True
    assert out.eval_runs == 3
    # Slice/P0 pass subsets; full passes None (run all).
    assert runner.calls == [
        ("slice", ["q1", "q2"]),
        ("p0", ["q3"]),
        ("full", None),
    ]


def test_slice_regression_short_circuits() -> None:
    runner = RecordingRunner({"slice": 50, "p0": 90, "full": 90})
    regressed = make_regression_check(80.0)
    out = run_three_gate(
        runner,
        "space-1",
        slice_qids=["q1"],
        p0_qids=["q3"],
        regressed=regressed,
    )
    assert out.accepted is False
    assert out.ran_full is False
    assert out.eval_runs == 1  # only the slice ran
    assert [c[0] for c in runner.calls] == ["slice"]


def test_p0_regression_skips_full() -> None:
    runner = RecordingRunner({"slice": 90, "p0": 50, "full": 90})
    regressed = make_regression_check(80.0)
    out = run_three_gate(
        runner,
        "space-1",
        slice_qids=["q1"],
        p0_qids=["q3"],
        regressed=regressed,
    )
    assert out.accepted is False
    assert out.ran_full is False
    assert out.eval_runs == 2
    assert [c[0] for c in runner.calls] == ["slice", "p0"]


def test_run_full_false_defers_full_benchmark() -> None:
    runner = RecordingRunner({"slice": 90, "p0": 90, "full": 90})
    out = run_three_gate(
        runner,
        "space-1",
        slice_qids=["q1"],
        p0_qids=["q3"],
        regressed=_passing_check,
        run_full=False,
    )
    assert out.accepted is True
    assert out.ran_full is False
    assert out.eval_runs == 2  # full benchmark NOT run
    assert [c[0] for c in runner.calls] == ["slice", "p0"]


def test_empty_subset_gate_is_skipped_not_widened() -> None:
    runner = RecordingRunner({"slice": 90, "p0": 90, "full": 90})
    out = run_three_gate(
        runner,
        "space-1",
        slice_qids=[],  # nothing to slice ⇒ skip, do NOT run all
        p0_qids=["q3"],
        regressed=_passing_check,
    )
    # slice skipped (no eval run), p0 + full ran.
    assert [c[0] for c in runner.calls] == ["p0", "full"]
    skipped = [o for o in out.outcomes if o.gate == "slice"][0]
    assert skipped.skipped is True
    assert skipped.result is None


def test_budget_records_each_gate_wall_clock() -> None:
    runner = RecordingRunner({"slice": 90, "p0": 90, "full": 90}, wall=120.0)
    budget = EvalBudget(hard_wall_seconds=10_000, finalize_reserve_seconds=0, clock=_Clock())
    run_three_gate(
        runner,
        "space-1",
        slice_qids=["q1"],
        p0_qids=["q2"],
        regressed=_passing_check,
        budget=budget,
    )
    # 3 gates × 120s recorded.
    assert budget.spent() == pytest.approx(360.0)


def test_gate_outcome_shape() -> None:
    runner = RecordingRunner({"slice": 90, "p0": 90, "full": 90})
    out = run_three_gate(
        runner, "space-1", slice_qids=["q1"], p0_qids=["q2"], regressed=_passing_check
    )
    assert all(isinstance(o, GateOutcome) for o in out.outcomes)
    assert [o.gate for o in out.outcomes] == ["slice", "p0", "full"]
    assert all(o.passed for o in out.outcomes)
