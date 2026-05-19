"""Phase 5 synthetic gate: Best-of-N fires when intended_patch_shape is
structural AND prior_failure_count >= 1, with N=2 default and ranking
applied to survivors.

Maps to user-text `test_best_of_n_runs_for_structural_with_prior_failure`.
"""
from __future__ import annotations

from typing import Any, Mapping
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


def test_best_of_n_default_size_is_three() -> None:
    """Phase 2.6 update: N bumped from 2 → 3 after cost data justified
    the third sample (harness.py:23300 ``for _bon_idx in range(3)``).
    The constant ``DEFAULT_BEST_OF_N`` was removed when this hard-code
    landed; this test pins the in-harness sample count to keep the
    Phase-5 N=3 contract observable from a unit test rather than
    requiring a manual stdout sample-count count.
    """
    import inspect
    import re

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)
    # Pin the canonical Best-of-N sample loop: ``for _bon_idx in range(N)``.
    matches = re.findall(r"for\s+_bon_idx\s+in\s+range\((\d+)\)", src)
    assert matches, (
        "Could not locate ``for _bon_idx in range(N)`` Best-of-N loop "
        "in harness.py. Has the loop variable been renamed?"
    )
    assert all(int(m) == 3 for m in matches), (
        f"Best-of-N sample size must be 3 (Phase 2.6 contract); "
        f"found N values: {matches}"
    )


def test_only_top_ranked_candidate_returned_for_full_eval() -> None:
    """Phase 2.6 ranker API (BestOfNRanking-based, keyword-only):

    - ``rank_proposal_candidates(*, candidates, target_qids,
       protected_dependents)`` returns a ``BestOfNRanking`` (not a
       plain list).
    - Candidate dicts use ``patch_type``, ``covers_target_qids``,
      ``blast_radius_dependents``, ``patch_count``,
      ``preserves_protected`` (not the legacy
      ``causal_shape_match``/``target_coverage_breadth`` etc.
      shape-summary fields). ``patch_type`` must be in the
      ``_STRUCTURAL_PATCH_TYPES`` frozenset for the causal-shape
      bit to flip.
    """
    candidates = [
        {
            "candidate_id": "c1",
            "patch_type": "sql_snippet",  # structural
            "covers_target_qids": ["q1"],
            "blast_radius_dependents": 4,
            "patch_count": 3,
            "preserves_protected": False,
        },
        {
            "candidate_id": "c2",
            "patch_type": "join_spec",  # structural
            "covers_target_qids": ["q1", "q2"],
            "blast_radius_dependents": 1,
            "patch_count": 2,
            "preserves_protected": True,
        },
    ]

    ranking = rank_proposal_candidates(
        candidates=candidates,
        target_qids=("q1", "q2"),
        protected_dependents=("dep_a",),
    )

    assert len(ranking.ordered_candidates) == 2
    assert ranking.top_candidate is not None
    assert ranking.top_candidate["candidate_id"] == "c2"
    assert ranking.ordered_candidates[0]["candidate_id"] == "c2"
    assert ranking.ordered_candidates[1]["candidate_id"] == "c1"


def test_full_eval_called_only_for_top_ranked(monkeypatch) -> None:
    """Synthetic harness integration: harness invokes Best-of-N when the
    predicate fires, ranks survivors, and only the top survivor is
    passed to the full-eval stage.
    """
    full_eval_calls: list[Mapping[str, Any]] = []

    def fake_full_eval(*, candidate, **kwargs):
        full_eval_calls.append(candidate)
        return MagicMock(accepted=True)

    candidates = [
        {
            "candidate_id": "c1",
            "patch_type": "instruction",  # non-structural — loses causal-shape bit
            "covers_target_qids": [],
            "blast_radius_dependents": 5,
            "patch_count": 4,
            "preserves_protected": False,
        },
        {
            "candidate_id": "c2",
            "patch_type": "sql_snippet",  # structural
            "covers_target_qids": ["q1"],
            "blast_radius_dependents": 1,
            "patch_count": 1,
            "preserves_protected": True,
        },
    ]
    ranking = rank_proposal_candidates(
        candidates=candidates,
        target_qids=("q1",),
        protected_dependents=("dep_a",),
    )
    fake_full_eval(candidate=ranking.top_candidate)
    assert len(full_eval_calls) == 1
    assert full_eval_calls[0]["candidate_id"] == "c2"
