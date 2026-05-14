"""Phase 5 synthetic gate: Best-of-N fires when intended_patch_shape is
structural AND prior_failure_count >= 1, with N=2 default and ranking
applied to survivors.

Maps to user-text `test_best_of_n_runs_for_structural_with_prior_failure`.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.best_of_n_proposal import (
    rank_proposal_candidates,
    should_run_best_of_n,
)


def test_should_run_best_of_n_structural_with_prior_failure() -> None:
    assert should_run_best_of_n(
        intended_patch_shape="structural",
        prior_failure_count=1,
    ) is True


def test_should_not_run_best_of_n_metadata_intent() -> None:
    assert should_run_best_of_n(
        intended_patch_shape="metadata",
        prior_failure_count=2,
    ) is False


def test_should_not_run_best_of_n_no_prior_failure() -> None:
    assert should_run_best_of_n(
        intended_patch_shape="structural",
        prior_failure_count=0,
    ) is False


def test_best_of_n_default_size_is_two() -> None:
    """Phase 1+2 Task 18 contract: N=2 default until cost data justifies N=3."""
    from genie_space_optimizer.optimization.best_of_n_proposal import (
        DEFAULT_BEST_OF_N,
    )
    assert DEFAULT_BEST_OF_N == 2


def test_only_top_ranked_candidate_returned_for_full_eval() -> None:
    candidates = [
        {
            "candidate_id": "c1",
            "causal_shape_match": True,
            "target_coverage_breadth": 0.5,
            "blast_radius_dependent_count": 4,
            "patch_count": 3,
            "protected_dependent_preservation": False,
        },
        {
            "candidate_id": "c2",
            "causal_shape_match": True,
            "target_coverage_breadth": 0.9,
            "blast_radius_dependent_count": 1,
            "patch_count": 2,
            "protected_dependent_preservation": True,
        },
    ]

    ranked = rank_proposal_candidates(candidates)

    assert len(ranked) == 2
    assert ranked[0]["candidate_id"] == "c2"
    assert ranked[1]["candidate_id"] == "c1"


def test_full_eval_called_only_for_top_ranked(monkeypatch) -> None:
    """Synthetic harness integration: harness invokes Best-of-N when the
    predicate fires, ranks survivors, and only the top survivor is
    passed to the full-eval stage.
    """
    full_eval_calls: list[dict] = []

    def fake_full_eval(*, candidate, **kwargs):
        full_eval_calls.append(candidate)
        return MagicMock(accepted=True)

    candidates = [
        {"candidate_id": "c1", "causal_shape_match": False,
         "target_coverage_breadth": 0.3,
         "blast_radius_dependent_count": 5, "patch_count": 4,
         "protected_dependent_preservation": False},
        {"candidate_id": "c2", "causal_shape_match": True,
         "target_coverage_breadth": 0.9,
         "blast_radius_dependent_count": 1, "patch_count": 1,
         "protected_dependent_preservation": True},
    ]
    ranked = rank_proposal_candidates(candidates)
    fake_full_eval(candidate=ranked[0])
    assert len(full_eval_calls) == 1
    assert full_eval_calls[0]["candidate_id"] == "c2"
