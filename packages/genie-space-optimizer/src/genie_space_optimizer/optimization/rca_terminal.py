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
    # Track G — plateau detector cannot fire while a queued diagnostic
    # or buffered AG still covers a live hard qid (signature overlap).
    PLATEAU_PENDING_DIAGNOSTIC_AGS = "plateau_pending_diagnostic_ags"


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
    pending_diagnostic_ags: list[dict] | None = None,
) -> RcaTerminalDecision:
    """Resolve the plateau terminal status from current eval state.

    Priority:
        1. Hard failures with concrete SQL deltas (still patchable).
        2. **Track G** — pending diagnostic / buffered AGs whose stable
           signature still overlaps the live hard set (still patchable
           via the queue, no need to terminate).
        3. Hard failures still in quarantine.
        4. Open regression debt.
        5. Clean plateau.

    ``pending_diagnostic_ags`` is an iterable of AG dicts as stored in
    ``harness.py``'s ``pending_action_groups`` and
    ``diagnostic_action_queue``. Each AG should carry a
    ``_stable_signature`` (Track D) — the resolver reads
    ``ag["_stable_signature"][1]`` for the qid set. AGs without a
    signature fall back to ``ag["affected_questions"]``.
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

    # Track G — refuse plateau when a queued AG covers a live hard qid.
    overlapping_ags: list[tuple[str, set[str]]] = []
    for ag in pending_diagnostic_ags or []:
        sig = ag.get("_stable_signature")
        if sig and len(sig) >= 2:
            ag_qids = {str(q) for q in (sig[1] or ()) if str(q)}
        else:
            ag_qids = {
                str(q)
                for q in (ag.get("affected_questions") or [])
                if str(q)
            }
        overlap = ag_qids & set(current_hard_qids)
        if overlap:
            overlapping_ags.append((str(ag.get("id") or ""), overlap))

    if overlapping_ags:
        ag_summary = ", ".join(
            f"{ag_id}=>{sorted(qids)}" for ag_id, qids in overlapping_ags
        )
        return RcaTerminalDecision(
            status=RcaTerminalStatus.PLATEAU_PENDING_DIAGNOSTIC_AGS,
            should_continue=True,
            reason=(
                f"plateau suppressed — pending_diagnostic_ags=[{ag_summary}] "
                f"overlap live hard set"
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
