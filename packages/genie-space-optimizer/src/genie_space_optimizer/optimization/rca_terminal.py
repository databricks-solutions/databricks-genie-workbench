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
