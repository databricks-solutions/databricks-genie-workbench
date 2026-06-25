"""Subset-first 3-gate eval orchestration (GSO Optimizer v2, Phase 1 §3.4).

The lever loop must NOT run a full benchmark every iteration — native eval-runs are
slow and sequential, and the job has a hard 2-hour wall. Instead each iteration runs
a *subset-first* 3-gate cycle through an :class:`~genie_space_optimizer.optimization.eval_runner.EvalRunner`:

    slice (~5–10 failing-cluster Qs) → P0 (~10–15 priority Qs) → full

The full benchmark runs only when both cheaper gates pass (ideally only at
acceptance). A gate that regresses short-circuits the cycle — the remaining gates,
including the expensive full run, are skipped.

This module owns only the *subset selection* and the *gate sequencing*. The pass/fail
verdict per gate is delegated to a caller-supplied ``regressed`` predicate so the
acceptance policy stays in one place (``acceptance_policy`` / the harness) — Phase 1
does not change acceptance semantics.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Sequence

from genie_space_optimizer.common import config as _config
from genie_space_optimizer.optimization.eval_budget import EvalBudget
from genie_space_optimizer.optimization.eval_runner import (
    FULL,
    P0,
    SLICE,
    EvalRunner,
    EvalRunResult,
)

logger = logging.getLogger(__name__)


def _ordered_unique(ids: Sequence[str]) -> list[str]:
    """De-duplicate while preserving first-seen order (deterministic subsets)."""
    seen: set[str] = set()
    out: list[str] = []
    for i in ids:
        s = str(i)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def select_slice_qids(
    failing_cluster_qids: Sequence[str],
    all_qids: Sequence[str] | None = None,
    *,
    max_questions: int | None = None,
) -> list[str]:
    """Pick the slice gate's question ids — the failing-cluster questions, capped.

    Falls back to (a prefix of) ``all_qids`` only when no failing-cluster ids are
    supplied, so the slice gate is never empty when there is anything to evaluate.
    """
    cap = int(max_questions if max_questions is not None else _config.SLICE_GATE_MAX_QUESTIONS)
    picked = _ordered_unique(failing_cluster_qids)[:cap]
    if not picked and all_qids:
        picked = _ordered_unique(all_qids)[:cap]
    return picked


def select_p0_qids(
    priority_qids: Sequence[str],
    all_qids: Sequence[str] | None = None,
    *,
    max_questions: int | None = None,
) -> list[str]:
    """Pick the P0 gate's question ids — the priority questions, capped."""
    cap = int(max_questions if max_questions is not None else _config.P0_GATE_MAX_QUESTIONS)
    picked = _ordered_unique(priority_qids)[:cap]
    if not picked and all_qids:
        picked = _ordered_unique(all_qids)[:cap]
    return picked


def make_regression_check(
    baseline_accuracy: float,
    *,
    tolerance_pp: float = 0.0,
) -> Callable[[EvalRunResult], bool]:
    """Build a ``regressed`` predicate: True when accuracy drops below baseline−tolerance.

    Phase 1 default check used when a caller does not supply its own policy. The
    harness keeps gating acceptance through ``acceptance_policy.decide_acceptance``;
    this helper is for callers (and tests) that need a simple accuracy-delta gate.
    """
    floor = float(baseline_accuracy) - float(tolerance_pp)

    def _regressed(result: EvalRunResult) -> bool:
        if not result.succeeded:
            return True
        return result.accuracy < floor

    return _regressed


@dataclass
class GateOutcome:
    gate: str
    result: EvalRunResult | None
    passed: bool
    skipped: bool
    reason: str


@dataclass
class ThreeGateResult:
    """Outcome of a subset-first 3-gate cycle."""

    outcomes: list[GateOutcome] = field(default_factory=list)
    accepted: bool = False
    final: EvalRunResult | None = None  # the deciding eval (full when reached)
    ran_full: bool = False

    @property
    def eval_runs(self) -> int:
        return sum(1 for o in self.outcomes if o.result is not None)


def run_three_gate(
    runner: EvalRunner,
    space_id: str,
    *,
    slice_qids: Sequence[str] | None,
    p0_qids: Sequence[str] | None,
    full_qids: Sequence[str] | None = None,
    regressed: Callable[[EvalRunResult], bool],
    run_full: bool = True,
    budget: EvalBudget | None = None,
) -> ThreeGateResult:
    """Run slice → P0 → full, short-circuiting on the first regression.

    ``full_qids=None`` evaluates every benchmark question (the only gate that should
    pass ``None`` to the runner). When ``budget`` is supplied each gate's wall-clock
    is recorded so the loop's iteration cap reflects sequential eval cost.

    Returns a :class:`ThreeGateResult`; ``accepted`` is True only when the deciding
    gate passed (the full run when ``run_full`` else the last cheaper gate).
    """
    out = ThreeGateResult()

    def _run_gate(gate: str, qids: Sequence[str] | None, *, subset: bool) -> bool:
        """Run one gate. Returns True to continue, False to short-circuit."""
        # A subset gate with no question ids contributes no signal — skip it
        # rather than accidentally evaluating the whole benchmark.
        if subset and not qids:
            out.outcomes.append(GateOutcome(gate, None, True, True, "no_subset_questions"))
            return True
        result = runner.run(
            space_id,
            (list(qids) if qids is not None else None),
            eval_scope=gate,
        )
        if budget is not None:
            budget.record(result.wall_clock_seconds)
        passed = not regressed(result)
        out.outcomes.append(
            GateOutcome(
                gate,
                result,
                passed,
                False,
                "passed" if passed else "regressed",
            )
        )
        out.final = result
        if not passed:
            out.accepted = False
            logger.info(
                "3-gate: %s gate regressed (accuracy=%.1f, status=%s) — "
                "short-circuiting, skipping remaining gates.",
                gate,
                result.accuracy,
                result.status,
            )
            return False
        return True

    if not _run_gate(SLICE, slice_qids, subset=True):
        return out
    if not _run_gate(P0, p0_qids, subset=True):
        return out

    if not run_full:
        # Slice/P0 both passed and the caller deferred the full run.
        out.accepted = True
        return out

    result = runner.run(
        space_id,
        (list(full_qids) if full_qids is not None else None),
        eval_scope=FULL,
    )
    if budget is not None:
        budget.record(result.wall_clock_seconds)
    passed = not regressed(result)
    out.outcomes.append(
        GateOutcome(FULL, result, passed, False, "passed" if passed else "regressed")
    )
    out.final = result
    out.ran_full = True
    out.accepted = passed
    return out
