"""Phase 1.6 — reserved recovery iteration budget."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.recovery_budget import (
    is_recovery_iteration,
    has_recovery_work,
    skip_or_proceed,
    RecoveryBudgetAction,
)


def test_iter_5_is_recovery_when_max_iterations_5():
    """Of 5 iterations, iteration 5 is reserved for recovery only."""
    assert is_recovery_iteration(iteration=5, max_iterations=5) is True
    assert is_recovery_iteration(iteration=4, max_iterations=5) is False
    assert is_recovery_iteration(iteration=1, max_iterations=5) is False


def test_iter_n_is_recovery_when_max_iterations_n():
    """Generalizes: the LAST iteration is always reserved."""
    assert is_recovery_iteration(iteration=3, max_iterations=3) is True
    assert is_recovery_iteration(iteration=7, max_iterations=7) is True


def test_has_recovery_work_true_when_regressed_or_uncovered_present():
    assert has_recovery_work(
        regressed_qids_count=1, uncovered_cluster_ids_count=0,
    ) is True
    assert has_recovery_work(
        regressed_qids_count=0, uncovered_cluster_ids_count=2,
    ) is True
    assert has_recovery_work(
        regressed_qids_count=0, uncovered_cluster_ids_count=0,
    ) is False


def test_skip_or_proceed_recovery_iter_with_work():
    """Recovery iter + work exists → PROCEED."""
    action = skip_or_proceed(
        iteration=5, max_iterations=5,
        regressed_qids_count=1, uncovered_cluster_ids_count=0,
    )
    assert action == RecoveryBudgetAction.PROCEED


def test_skip_or_proceed_recovery_iter_without_work():
    """Recovery iter + no work → SKIP_EARLY_TERMINATE."""
    action = skip_or_proceed(
        iteration=5, max_iterations=5,
        regressed_qids_count=0, uncovered_cluster_ids_count=0,
    )
    assert action == RecoveryBudgetAction.SKIP_EARLY_TERMINATE


def test_skip_or_proceed_non_recovery_iter_proceeds_always():
    """Non-recovery iter → PROCEED regardless of work state."""
    action = skip_or_proceed(
        iteration=3, max_iterations=5,
        regressed_qids_count=0, uncovered_cluster_ids_count=0,
    )
    assert action == RecoveryBudgetAction.PROCEED
