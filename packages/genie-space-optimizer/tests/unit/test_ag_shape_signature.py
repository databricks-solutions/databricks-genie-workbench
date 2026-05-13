"""Phase 3 Action 3.2 — AGShapeSignature + compute_ag_shape_signature tests."""

from __future__ import annotations

from genie_space_optimizer.optimization.near_miss_reflection import (
    AGShapeSignature,
    compute_ag_shape_signature,
    shapes_are_equal,
)
from genie_space_optimizer.optimization.target_scope import TargetScope


def _cluster(cluster_id: str, qids: tuple[str, ...]) -> dict:
    return {
        "primary_cluster_id": cluster_id,
        "target_qids": qids,
        "affected_questions": list(qids),
    }


def _ag(*, primary_cluster_id: str, target_qids: list[str]) -> dict:
    return {
        "primary_cluster_id": primary_cluster_id,
        "target_qids": target_qids,
        "lever_directives": [{"lever": "L6_sql_expression"}],
    }


def test_compute_signature_pulls_repair_archetype_from_kit_lookup() -> None:
    ag = _ag(primary_cluster_id="cluster_h002", target_qids=["gs_021"])
    clusters = [_cluster("cluster_h002", ("gs_021",))]
    kit_lookup = {"cluster_h002": {"repair_archetype": "default_time_window_filter"}}
    sig = compute_ag_shape_signature(ag, clusters, kit_lookup)
    assert sig.repair_archetype == "default_time_window_filter"
    assert sig.target_scope == TargetScope.SINGLE_QID
    assert sig.primary_cluster_id == "cluster_h002"
    assert sig.target_qids == ("gs_021",)


def test_compute_signature_returns_unknown_when_no_kit() -> None:
    ag = _ag(primary_cluster_id="cluster_h002", target_qids=["gs_021"])
    sig = compute_ag_shape_signature(ag, [_cluster("cluster_h002", ("gs_021",))], {})
    assert sig.repair_archetype == "unknown"


def test_shapes_are_equal_compares_only_archetype_and_scope() -> None:
    a = AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.SINGLE_QID,
        primary_cluster_id="cluster_h002", target_qids=("gs_021",),
    )
    b = AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.SINGLE_QID,
        primary_cluster_id="cluster_h004", target_qids=("gs_055",),
    )
    assert shapes_are_equal(a, b)


def test_shapes_are_not_equal_when_archetype_differs() -> None:
    a = AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.SINGLE_QID,
        primary_cluster_id="c", target_qids=(),
    )
    b = AGShapeSignature(
        repair_archetype="enforce_explicit_top_n_cardinality",
        target_scope=TargetScope.SINGLE_QID,
        primary_cluster_id="c", target_qids=(),
    )
    assert not shapes_are_equal(a, b)


def test_shapes_are_not_equal_when_scope_differs() -> None:
    a = AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.SINGLE_QID,
        primary_cluster_id="c", target_qids=(),
    )
    b = AGShapeSignature(
        repair_archetype="default_time_window_filter",
        target_scope=TargetScope.CLUSTER_SCOPED,
        primary_cluster_id="c", target_qids=(),
    )
    assert not shapes_are_equal(a, b)
