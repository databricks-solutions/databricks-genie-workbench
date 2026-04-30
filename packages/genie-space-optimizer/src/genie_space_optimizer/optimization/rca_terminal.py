"""Terminal-state classification for the RCA-driven optimizer loop."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RcaTerminalStatus(str, Enum):
    CONVERGED = "converged"
    PATCHABLE_IN_PROGRESS = "patchable_in_progress"
    BENCHMARK_BROKEN = "benchmark_broken"
    JUDGE_UNRELIABLE = "judge_unreliable"
    UNPATCHABLE_WITH_SIX_LEVERS = "unpatchable_with_six_levers"
    EXHAUSTED_BUDGET = "exhausted_budget"
    UNRESOLVED_HARD_FAILURES_QUARANTINED = "unresolved_hard_failures_quarantined"
    UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA = "unresolved_hard_failure_with_untried_sql_delta"
    DIMINISHING_RETURNS_WITH_OPEN_DEBT = "diminishing_returns_with_open_debt"
    PLATEAU_NO_OPEN_FAILURES = "plateau_no_open_failures"


@dataclass(frozen=True)
class RcaTerminalDecision:
    status: RcaTerminalStatus
    should_continue: bool
    reason: str


def classify_terminal_state(
    *,
    post_arbiter_accuracy: float,
    max_iterations: int,
    iteration_counter: int,
    actionable_plan_count: int,
    repeated_failure_count: int,
    judge_failure_count: int,
    benchmark_issue_count: int,
    unpatchable_count: int,
    target_accuracy: float = 100.0,
    judge_failure_limit: int = 3,
) -> RcaTerminalDecision:
    """Classify whether the optimizer should continue or terminate.

    This is pure and intentionally conservative. Actionable RCA plans keep
    the loop alive until convergence or budget exhaustion. Non-actionable
    failures terminate with an explicit diagnosis instead of another retry.
    """
    if float(post_arbiter_accuracy) >= float(target_accuracy):
        return RcaTerminalDecision(
            RcaTerminalStatus.CONVERGED,
            False,
            f"post-arbiter accuracy reached {target_accuracy:.1f}%",
        )

    if int(judge_failure_count) >= int(judge_failure_limit):
        return RcaTerminalDecision(
            RcaTerminalStatus.JUDGE_UNRELIABLE,
            False,
            f"arbiter or judge signal failed {judge_failure_count} times",
        )

    if benchmark_issue_count > 0 and actionable_plan_count == 0:
        return RcaTerminalDecision(
            RcaTerminalStatus.BENCHMARK_BROKEN,
            False,
            f"{benchmark_issue_count} hard failures require benchmark review",
        )

    if actionable_plan_count == 0 and unpatchable_count > 0:
        return RcaTerminalDecision(
            RcaTerminalStatus.UNPATCHABLE_WITH_SIX_LEVERS,
            False,
            f"{unpatchable_count} hard failures are outside the six Genie levers",
        )

    if int(iteration_counter) >= int(max_iterations):
        return RcaTerminalDecision(
            RcaTerminalStatus.EXHAUSTED_BUDGET,
            False,
            (
                f"reached {max_iterations} lever-loop iterations with "
                f"{actionable_plan_count} actionable plans and "
                f"{repeated_failure_count} repeated failures"
            ),
        )

    return RcaTerminalDecision(
        RcaTerminalStatus.PATCHABLE_IN_PROGRESS,
        True,
        f"{actionable_plan_count} actionable RCA plans remain",
    )


def legacy_plateau_allows_stop(
    *,
    plateau_detected: bool,
    terminal_decision: RcaTerminalDecision | None,
) -> bool:
    """Return whether the old plateau gate may stop the RCA loop.

    Plateau is advisory while actionable RCA plans remain. It becomes a
    stopping signal only after the explicit terminal classifier has already
    determined that the loop should not continue.
    """
    if not plateau_detected:
        return False
    if terminal_decision is None:
        return True
    return terminal_decision.should_continue is False


def resolve_terminal_on_plateau(
    *,
    quarantined_qids: set[str],
    current_hard_qids: set[str],
    regression_debt_qids: set[str],
    sql_delta_qids: set[str] | None = None,
) -> RcaTerminalDecision:
    """Resolve the plateau terminal status from current eval state.

    Priority: hard failures with concrete SQL deltas (still patchable) >
    hard failures still in quarantine > open regression debt > clean
    plateau. The result replaces the old ``(unknown)`` plateau label so
    downstream consumers see a typed status.
    """
    still_patchable = sorted(set(sql_delta_qids or set()) & set(current_hard_qids))
    if still_patchable:
        return RcaTerminalDecision(
            status=RcaTerminalStatus.UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA,
            should_continue=True,
            reason=(
                f"{len(still_patchable)} hard failure(s) have concrete SQL deltas "
                f"remaining: {still_patchable}"
            ),
        )
    quarantined_and_hard = sorted(set(quarantined_qids) & set(current_hard_qids))
    if quarantined_and_hard:
        return RcaTerminalDecision(
            status=RcaTerminalStatus.UNRESOLVED_HARD_FAILURES_QUARANTINED,
            should_continue=False,
            reason=(
                f"{len(quarantined_and_hard)} hard failure(s) remain in "
                f"quarantine: {quarantined_and_hard}"
            ),
        )
    open_debt = sorted(set(regression_debt_qids) & set(current_hard_qids))
    if open_debt:
        return RcaTerminalDecision(
            status=RcaTerminalStatus.DIMINISHING_RETURNS_WITH_OPEN_DEBT,
            should_continue=False,
            reason=(
                f"{len(open_debt)} regression debt qid(s) still hard: "
                f"{open_debt}"
            ),
        )
    return RcaTerminalDecision(
        status=RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES,
        should_continue=False,
        reason="no hard failures or open regression debt remain",
    )
