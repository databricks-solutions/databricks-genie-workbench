"""Phase 1.6 — reserved recovery iteration budget.

Of ``max_iterations`` total, reserve the LAST one as "recovery only" —
only spent on regressed or uncovered clusters that emerged from
prior rollback. If no recovery work exists, this iteration is
skipped (early termination) rather than consumed on a no-op.
"""
from __future__ import annotations

from enum import StrEnum


class RecoveryBudgetAction(StrEnum):
    PROCEED = "proceed"
    """Run this iteration as normal."""
    SKIP_EARLY_TERMINATE = "skip_early_terminate"
    """Skip this iteration and terminate the loop with the
    explanatory ``GSO_CONVERGENCE_V1`` reason."""


def is_recovery_iteration(*, iteration: int, max_iterations: int) -> bool:
    """True iff ``iteration`` is the reserved recovery slot."""
    return int(iteration) == int(max_iterations)


def has_recovery_work(
    *,
    regressed_qids_count: int,
    uncovered_cluster_ids_count: int,
) -> bool:
    """True iff there is any cluster the recovery iteration would
    work on."""
    return (
        int(regressed_qids_count) + int(uncovered_cluster_ids_count) > 0
    )


def skip_or_proceed(
    *,
    iteration: int,
    max_iterations: int,
    regressed_qids_count: int,
    uncovered_cluster_ids_count: int,
) -> RecoveryBudgetAction:
    """Decide whether to skip the recovery iteration or proceed.

    Non-recovery iterations always PROCEED. Recovery iterations
    PROCEED iff there is work; otherwise SKIP_EARLY_TERMINATE.
    """
    if not is_recovery_iteration(
        iteration=iteration, max_iterations=max_iterations,
    ):
        return RecoveryBudgetAction.PROCEED
    if has_recovery_work(
        regressed_qids_count=regressed_qids_count,
        uncovered_cluster_ids_count=uncovered_cluster_ids_count,
    ):
        return RecoveryBudgetAction.PROCEED
    return RecoveryBudgetAction.SKIP_EARLY_TERMINATE
