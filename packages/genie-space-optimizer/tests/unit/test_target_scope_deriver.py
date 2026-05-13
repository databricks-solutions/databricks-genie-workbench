"""Phase 3 Action 3.2 — derive_target_scope tests."""

from __future__ import annotations

from genie_space_optimizer.optimization.target_scope import (
    TargetScope,
    derive_target_scope,
)


def _cluster(cluster_id: str, qids: tuple[str, ...]) -> dict:
    return {
        "primary_cluster_id": cluster_id,
        "target_qids": qids,
        "affected_questions": list(qids),
    }


def test_global_scoped_when_global_instruction_rewrite_present() -> None:
    ag = {
        "primary_cluster_id": "cluster_h001",
        "target_qids": ["gs_001"],
        "global_instruction_rewrite": {"text": "Always sort by date"},
    }
    assert derive_target_scope(ag, [_cluster("cluster_h001", ("gs_001",))]) \
        == TargetScope.GLOBAL_SCOPED


def test_global_scoped_when_lever_directive_is_global() -> None:
    ag = {
        "primary_cluster_id": "cluster_h001",
        "target_qids": ["gs_001"],
        "lever_directives": [{"lever": "L5_global_instruction"}],
    }
    assert derive_target_scope(ag, [_cluster("cluster_h001", ("gs_001",))]) \
        == TargetScope.GLOBAL_SCOPED


def test_single_qid_when_target_qids_has_one_entry() -> None:
    ag = {
        "primary_cluster_id": "cluster_h001",
        "target_qids": ["gs_001"],
        "lever_directives": [{"lever": "L6_sql_expression"}],
    }
    assert derive_target_scope(ag, [_cluster("cluster_h001", ("gs_001", "gs_002"))]) \
        == TargetScope.SINGLE_QID


def test_cluster_scoped_when_target_qids_match_one_cluster_exactly() -> None:
    cluster = _cluster("cluster_h001", ("gs_001", "gs_002"))
    ag = {
        "primary_cluster_id": "cluster_h001",
        "target_qids": ["gs_001", "gs_002"],
        "lever_directives": [{"lever": "L6_sql_expression"}],
    }
    assert derive_target_scope(ag, [cluster]) == TargetScope.CLUSTER_SCOPED


def test_multi_cluster_scoped_when_target_qids_span_multiple_clusters() -> None:
    clusters = [
        _cluster("cluster_h001", ("gs_001",)),
        _cluster("cluster_h002", ("gs_002",)),
    ]
    ag = {
        "primary_cluster_id": "cluster_h001",
        "target_qids": ["gs_001", "gs_002"],
        "lever_directives": [{"lever": "L6_sql_expression"}],
    }
    assert derive_target_scope(ag, clusters) == TargetScope.MULTI_CLUSTER_SCOPED


def test_falls_back_to_affected_questions_when_target_qids_missing() -> None:
    cluster = _cluster("cluster_h001", ("gs_001",))
    ag = {
        "primary_cluster_id": "cluster_h001",
        "affected_questions": ["gs_001"],
        "lever_directives": [{"lever": "L6_sql_expression"}],
    }
    assert derive_target_scope(ag, [cluster]) == TargetScope.SINGLE_QID


def test_empty_targets_default_to_single_qid() -> None:
    ag = {
        "primary_cluster_id": "cluster_unknown",
        "target_qids": [],
        "lever_directives": [{"lever": "L6_sql_expression"}],
    }
    assert derive_target_scope(ag, []) == TargetScope.SINGLE_QID
