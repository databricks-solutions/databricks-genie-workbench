"""Eval-run budget guard for the lever loop (GSO Optimizer v2, Phase 1 §3.4).

Native eval-runs are asynchronous server jobs (~15–20 min / 30 questions, scaling
with space complexity, warehouse size, and question count) and the DABs job has a
**hard 2-hour wall**. Eval-runs are **sequential** — the SDK exposes no concurrency
flag — so the binding constraint is the *sum of every run's wall-clock*, not the
iteration count.

This module tracks cumulative eval wall-clock against that 2-hour wall and reserves
budget for the finalize run. ``max_iterations`` stays an upper bound; the budget can
stop the loop earlier when the remaining wall can't fund another subset-first gate
cycle (slice → P0 → full) plus the reserved finalize run.

It also exposes the bounded 30–40-question working-set check as a *recommendation*
(D8 / §3.4: recommend a prune/top-up, never silently delete).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

from genie_space_optimizer.common import config as _config


def estimate_eval_run_seconds(
    question_count: int,
    *,
    per_question_seconds: float | None = None,
) -> float:
    """Estimate a single eval-run's wall-clock from its question count.

    Calibrated from §3.4 (~17 min / 30 Qs ⇒ ~34 s/question). Returns ``0`` for a
    non-positive count.
    """
    if question_count <= 0:
        return 0.0
    per_q = (
        per_question_seconds
        if per_question_seconds is not None
        else _config.EVAL_PER_QUESTION_SECONDS
    )
    return float(question_count) * float(per_q)


def estimate_three_gate_seconds(
    *,
    working_set_size: int,
    slice_size: int | None = None,
    p0_size: int | None = None,
    per_question_seconds: float | None = None,
    run_full: bool = True,
) -> float:
    """Estimate the wall-clock of one subset-first 3-gate cycle (slice + P0 + full).

    Gate sizes default to the configured slice/P0 caps, each clamped to the working
    set. Eval-runs are sequential, so the cycle estimate is the sum of the per-gate
    estimates. When ``run_full`` is False (slice/P0-only iterations) the full-run
    term is dropped — most iterations should not pay for a full benchmark.
    """
    ws = max(0, int(working_set_size))
    s = slice_size if slice_size is not None else _config.SLICE_GATE_MAX_QUESTIONS
    p = p0_size if p0_size is not None else _config.P0_GATE_MAX_QUESTIONS
    s = min(int(s), ws) if ws else int(s)
    p = min(int(p), ws) if ws else int(p)
    total = estimate_eval_run_seconds(s, per_question_seconds=per_question_seconds)
    total += estimate_eval_run_seconds(p, per_question_seconds=per_question_seconds)
    if run_full:
        total += estimate_eval_run_seconds(ws, per_question_seconds=per_question_seconds)
    return total


@dataclass
class EvalBudget:
    """Cumulative eval wall-clock budget against the hard job wall.

    ``spent()`` is the sum of every recorded eval-run wall-clock (sequential model).
    ``can_afford(est)`` is the guard the lever loop checks before each iteration:
    it leaves ``finalize_reserve_seconds`` untouched for the finalize run.
    """

    hard_wall_seconds: float
    finalize_reserve_seconds: float
    clock: Callable[[], float] = time.monotonic
    _recorded_seconds: float = field(default=0.0, init=False)
    _start: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self._start = self.clock()

    @classmethod
    def from_config(cls, *, clock: Callable[[], float] = time.monotonic) -> "EvalBudget":
        return cls(
            hard_wall_seconds=float(_config.EVAL_JOB_WALL_SECONDS),
            finalize_reserve_seconds=float(_config.EVAL_FINALIZE_RESERVE_SECONDS),
            clock=clock,
        )

    def record(self, seconds: float) -> None:
        """Record one eval-run's wall-clock (sequential ⇒ summed)."""
        self._recorded_seconds += max(0.0, float(seconds))

    def spent(self) -> float:
        """Cumulative recorded eval wall-clock."""
        return self._recorded_seconds

    def elapsed(self) -> float:
        """Wall-clock since the budget was created (diagnostic only)."""
        return self.clock() - self._start

    def remaining(self) -> float:
        """Eval wall-clock remaining against the hard wall."""
        return max(0.0, self.hard_wall_seconds - self._recorded_seconds)

    def remaining_after_reserve(self) -> float:
        """Remaining wall after reserving the finalize run."""
        return max(0.0, self.remaining() - self.finalize_reserve_seconds)

    def can_afford(self, estimated_seconds: float) -> bool:
        """True when the next cycle fits without eating the finalize reserve."""
        return self.remaining_after_reserve() >= max(0.0, float(estimated_seconds))


@dataclass(frozen=True)
class WorkingSetAdvice:
    """Recommendation for the bounded 30–40-question working set (never auto-deletes)."""

    count: int
    within_bounds: bool
    recommendation: str  # "ok" | "prune" | "topup"
    detail: str


def assess_working_set(
    count: int,
    *,
    lo: int | None = None,
    hi: int | None = None,
) -> WorkingSetAdvice:
    """Recommend prune/top-up to keep the working benchmark within 30–40 (D8).

    Returns a recommendation only — preflight surfaces it in the UI; GSO never
    silently deletes user-authored benchmark rows.
    """
    low = int(lo if lo is not None else _config.WORKING_SET_MIN)
    high = int(hi if hi is not None else _config.WORKING_SET_MAX)
    n = int(count)
    if n > high:
        return WorkingSetAdvice(
            count=n,
            within_bounds=False,
            recommendation="prune",
            detail=(
                f"{n} questions exceeds the {high}-question ceiling; recommend pruning "
                f"{n - high} (EXPLAIN-invalid first, then near-duplicates)."
            ),
        )
    if n < low:
        return WorkingSetAdvice(
            count=n,
            within_bounds=False,
            recommendation="topup",
            detail=(
                f"{n} questions is below the {low}-question floor; recommend topping up "
                f"{low - n} via synthesis."
            ),
        )
    return WorkingSetAdvice(
        count=n,
        within_bounds=True,
        recommendation="ok",
        detail=f"{n} questions is within the {low}-{high} working-set window.",
    )
