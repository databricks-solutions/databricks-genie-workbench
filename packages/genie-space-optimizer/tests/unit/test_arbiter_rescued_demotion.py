"""Pin that arbiter-rescued rows never become soft-signal clusters."""

from __future__ import annotations

from genie_space_optimizer.optimization.ground_truth_corrections import (
    should_cluster_as_soft_signal,
)


def test_both_correct_arbiter_blocks_soft_clustering() -> None:
    """arbiter=both_correct must skip the soft-signal path even with judge dissent."""
    row = {
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": "yes",
        "feedback/semantic_equivalence/value": "no",
    }
    assert should_cluster_as_soft_signal(row) is False


def test_genie_correct_arbiter_blocks_soft_clustering() -> None:
    """arbiter=genie_correct must skip the soft-signal path."""
    row = {
        "feedback/arbiter/value": "genie_correct",
        "feedback/result_correctness/value": "yes",
        "feedback/semantic_equivalence/value": "no",
    }
    assert should_cluster_as_soft_signal(row) is False


def test_neither_correct_arbiter_with_judge_dissent_still_soft() -> None:
    """A genuinely failing soft signal (no arbiter rescue) is still clustered."""
    row = {
        "feedback/arbiter/value": "neither_correct",
        "feedback/result_correctness/value": "yes",
        "feedback/semantic_equivalence/value": "no",
    }
    assert should_cluster_as_soft_signal(row) is True
