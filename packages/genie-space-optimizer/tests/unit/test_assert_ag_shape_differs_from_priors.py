"""Phase 3 Action 3.2 — assert_ag_shape_differs_from_priors tests."""

from __future__ import annotations

from genie_space_optimizer.optimization.near_miss_reflection import (
    AGShapeSignature,
    AGShapeAssertionResult,
    assert_ag_shape_differs_from_priors,
)
from genie_space_optimizer.optimization.target_scope import TargetScope


def _shape(archetype: str, scope: TargetScope) -> AGShapeSignature:
    return AGShapeSignature(
        repair_archetype=archetype, target_scope=scope,
        primary_cluster_id="c", target_qids=("gs_001",),
    )


def test_passes_when_no_prior_shapes_exist() -> None:
    result = assert_ag_shape_differs_from_priors(
        candidate_shape=_shape("default_time_window_filter", TargetScope.SINGLE_QID),
        prior_shapes=(),
        required_next_iter_change="either",
    )
    assert result.differs is True
    assert result.matched_prior_shape is None


def test_fails_when_archetype_and_scope_repeat_a_prior_either_clause() -> None:
    prior = _shape("default_time_window_filter", TargetScope.SINGLE_QID)
    result = assert_ag_shape_differs_from_priors(
        candidate_shape=_shape("default_time_window_filter", TargetScope.SINGLE_QID),
        prior_shapes=(prior,),
        required_next_iter_change="either",
    )
    assert result.differs is False
    assert result.matched_prior_shape == prior


def test_passes_when_only_scope_changed_under_either_clause() -> None:
    prior = _shape("default_time_window_filter", TargetScope.SINGLE_QID)
    candidate = _shape("default_time_window_filter", TargetScope.CLUSTER_SCOPED)
    result = assert_ag_shape_differs_from_priors(
        candidate_shape=candidate,
        prior_shapes=(prior,),
        required_next_iter_change="either",
    )
    assert result.differs is True


def test_passes_when_only_archetype_changed() -> None:
    prior = _shape("default_time_window_filter", TargetScope.SINGLE_QID)
    candidate = _shape("enforce_explicit_top_n_cardinality", TargetScope.SINGLE_QID)
    result = assert_ag_shape_differs_from_priors(
        candidate_shape=candidate,
        prior_shapes=(prior,),
        required_next_iter_change="different_repair_archetype",
    )
    assert result.differs is True


def test_assertion_result_carries_matched_prior() -> None:
    prior_a = _shape("default_time_window_filter", TargetScope.SINGLE_QID)
    prior_b = _shape("enforce_explicit_top_n_cardinality", TargetScope.SINGLE_QID)
    candidate = _shape("enforce_explicit_top_n_cardinality", TargetScope.SINGLE_QID)
    result = assert_ag_shape_differs_from_priors(
        candidate_shape=candidate,
        prior_shapes=(prior_a, prior_b),
        required_next_iter_change="either",
    )
    assert result.differs is False
    assert result.matched_prior_shape == prior_b
    assert isinstance(result, AGShapeAssertionResult)
