"""Phase 2.6 — Best-of-N proposal generation for hard structural AGs."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.best_of_n_proposal import (
    should_run_best_of_n,
    rank_proposal_candidates,
    BestOfNRanking,
)


def test_should_not_run_for_non_structural_intent():
    assert should_run_best_of_n(
        intended_patch_shape="metadata",
        prior_failure_count=2,
    ) is False


def test_should_not_run_for_structural_with_zero_failures():
    """First attempt at a structural AG — single-shot is enough."""
    assert should_run_best_of_n(
        intended_patch_shape="structural",
        prior_failure_count=0,
    ) is False


def test_should_run_for_structural_with_prior_failure():
    assert should_run_best_of_n(
        intended_patch_shape="structural",
        prior_failure_count=1,
    ) is True
    assert should_run_best_of_n(
        intended_patch_shape="structural",
        prior_failure_count=5,
    ) is True


def test_rank_candidates_returns_empty_for_empty_input():
    ranking = rank_proposal_candidates(
        candidates=[],
        target_qids=("gs_001",),
        protected_dependents=(),
    )
    assert ranking.ordered_candidates == ()
    assert ranking.top_candidate is None


def test_rank_prefers_causal_shape_match():
    """Candidate with structural patch_type outranks metadata."""
    candidates = [
        {"patch_id": "p1", "patch_type": "uc_table_description",
         "covers_target_qids": ["gs_001"], "blast_radius_dependents": 1,
         "preserves_protected": True, "patch_count": 1},
        {"patch_id": "p2", "patch_type": "narrow_l6_sql",
         "covers_target_qids": ["gs_001"], "blast_radius_dependents": 1,
         "preserves_protected": True, "patch_count": 1},
    ]
    ranking = rank_proposal_candidates(
        candidates=candidates,
        target_qids=("gs_001",),
        protected_dependents=(),
    )
    assert ranking.top_candidate["patch_id"] == "p2"


def test_rank_prefers_protected_dependent_preservation_when_tied_on_causal():
    candidates = [
        {"patch_id": "p1", "patch_type": "narrow_l6_sql",
         "covers_target_qids": ["gs_001"], "blast_radius_dependents": 1,
         "preserves_protected": False, "patch_count": 1},
        {"patch_id": "p2", "patch_type": "narrow_l6_sql",
         "covers_target_qids": ["gs_001"], "blast_radius_dependents": 1,
         "preserves_protected": True, "patch_count": 1},
    ]
    ranking = rank_proposal_candidates(
        candidates=candidates,
        target_qids=("gs_001",),
        protected_dependents=("gs_003",),
    )
    assert ranking.top_candidate["patch_id"] == "p2"


def test_rank_prefers_lower_blast_radius_on_tie():
    candidates = [
        {"patch_id": "p1", "patch_type": "narrow_l6_sql",
         "covers_target_qids": ["gs_001"], "blast_radius_dependents": 5,
         "preserves_protected": True, "patch_count": 1},
        {"patch_id": "p2", "patch_type": "narrow_l6_sql",
         "covers_target_qids": ["gs_001"], "blast_radius_dependents": 1,
         "preserves_protected": True, "patch_count": 1},
    ]
    ranking = rank_proposal_candidates(
        candidates=candidates,
        target_qids=("gs_001",),
        protected_dependents=(),
    )
    assert ranking.top_candidate["patch_id"] == "p2"
