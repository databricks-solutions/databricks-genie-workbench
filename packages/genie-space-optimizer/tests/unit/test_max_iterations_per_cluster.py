"""Pin iteration budget scaling."""

from __future__ import annotations

from genie_space_optimizer.common.config import (
    MAX_ITERATIONS,
    MAX_ITERATIONS_PER_CLUSTER,
    MAX_ITERATIONS_HARD_CEILING,
)
from genie_space_optimizer.optimization.harness import compute_iteration_budget


def test_constants_present() -> None:
    assert isinstance(MAX_ITERATIONS_PER_CLUSTER, int)
    assert isinstance(MAX_ITERATIONS_HARD_CEILING, int)
    assert MAX_ITERATIONS_PER_CLUSTER >= 1
    assert MAX_ITERATIONS_HARD_CEILING >= MAX_ITERATIONS


def test_scales_with_hard_clusters() -> None:
    assert compute_iteration_budget(
        hard_cluster_count=5, requested_max_iterations=MAX_ITERATIONS,
    ) >= 5


def test_explicit_request_wins_over_scaled() -> None:
    assert compute_iteration_budget(
        hard_cluster_count=2, requested_max_iterations=20,
    ) == 20


def test_respects_hard_ceiling() -> None:
    assert compute_iteration_budget(
        hard_cluster_count=1000, requested_max_iterations=MAX_ITERATIONS,
    ) == MAX_ITERATIONS_HARD_CEILING


def test_zero_clusters_still_gets_max_iterations() -> None:
    assert compute_iteration_budget(
        hard_cluster_count=0, requested_max_iterations=MAX_ITERATIONS,
    ) == MAX_ITERATIONS
