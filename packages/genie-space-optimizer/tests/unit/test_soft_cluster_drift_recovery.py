"""Soft-cluster drift recovery (Cycle 5 T5).

Reproducer for the run-aborting AssertionError in
``2423b960-16e8-41d4-a0cb-74c563378e05`` early task attempts: a soft
cluster carried a qid that the current eval no longer flagged as
judge-failing. Today raises and aborts the run; T5 recovers by
dropping the drifted qid (or the entire cluster if all qids drifted)
and continuing.
"""
from __future__ import annotations


def test_recover_drops_drifted_qids_and_returns_clean_clusters() -> None:
    from genie_space_optimizer.optimization.cluster_formation_recovery import (
        recover_from_soft_cluster_drift,
    )
    soft_clusters = [
        {
            "cluster_id": "S001",
            # gs_001 drifted (passes all judges in current eval);
            # gs_002 is a legitimate soft signal.
            "question_ids": ["gs_001", "gs_002"],
            "root_cause": "wrong_table",
        },
        {
            "cluster_id": "S002",
            "question_ids": ["gs_003"],  # legitimate
            "root_cause": "wrong_column",
        },
    ]
    judge_failing_qids = ["gs_002", "gs_003"]
    result = recover_from_soft_cluster_drift(
        soft_clusters=soft_clusters,
        judge_failing_qids=judge_failing_qids,
    )
    assert len(result.recovered_clusters) == 2
    assert result.recovered_clusters[0]["question_ids"] == ["gs_002"]
    assert result.recovered_clusters[1]["question_ids"] == ["gs_003"]
    assert result.drifted_qids_by_cluster == {"S001": ("gs_001",)}
    assert result.dropped_cluster_ids == ()


def test_recover_drops_wholly_drifted_clusters() -> None:
    from genie_space_optimizer.optimization.cluster_formation_recovery import (
        recover_from_soft_cluster_drift,
    )
    soft_clusters = [
        {
            "cluster_id": "S001",
            "question_ids": ["gs_001"],  # drifted
            "root_cause": "wrong_table",
        },
    ]
    result = recover_from_soft_cluster_drift(
        soft_clusters=soft_clusters,
        judge_failing_qids=["gs_002"],  # gs_001 not present
    )
    assert result.recovered_clusters == []
    assert result.dropped_cluster_ids == ("S001",)
    assert result.drifted_qids_by_cluster == {"S001": ("gs_001",)}


def test_recover_no_drift_returns_clusters_unchanged() -> None:
    from genie_space_optimizer.optimization.cluster_formation_recovery import (
        recover_from_soft_cluster_drift,
    )
    soft_clusters = [{
        "cluster_id": "S001",
        "question_ids": ["gs_001", "gs_002"],
        "root_cause": "wrong_table",
    }]
    result = recover_from_soft_cluster_drift(
        soft_clusters=soft_clusters,
        judge_failing_qids=["gs_001", "gs_002"],
    )
    assert len(result.recovered_clusters) == 1
    assert result.recovered_clusters[0]["question_ids"] == ["gs_001", "gs_002"]
    assert result.drifted_qids_by_cluster == {}
    assert result.dropped_cluster_ids == ()


def test_recover_strips_benchmark_suffix_for_matching() -> None:
    """Benchmark-suffix variants (`q_002:v2`) match a soft cluster's
    bare `q_002`. Mirrors control_plane._base_qid behavior."""
    from genie_space_optimizer.optimization.cluster_formation_recovery import (
        recover_from_soft_cluster_drift,
    )
    soft_clusters = [{
        "cluster_id": "S001",
        "question_ids": ["q_002"],
        "root_cause": "wrong_table",
    }]
    result = recover_from_soft_cluster_drift(
        soft_clusters=soft_clusters,
        judge_failing_qids=["q_002:v2"],
    )
    assert result.drifted_qids_by_cluster == {}
    assert result.recovered_clusters == soft_clusters


def test_recover_handles_empty_input() -> None:
    from genie_space_optimizer.optimization.cluster_formation_recovery import (
        recover_from_soft_cluster_drift,
    )
    result = recover_from_soft_cluster_drift(
        soft_clusters=[],
        judge_failing_qids=[],
    )
    assert result.recovered_clusters == []
    assert result.drifted_qids_by_cluster == {}
    assert result.dropped_cluster_ids == ()


def test_recover_does_not_mutate_input() -> None:
    from genie_space_optimizer.optimization.cluster_formation_recovery import (
        recover_from_soft_cluster_drift,
    )
    inp = [{
        "cluster_id": "S001",
        "question_ids": ["gs_001", "gs_002"],
        "root_cause": "wrong_table",
    }]
    recover_from_soft_cluster_drift(
        soft_clusters=inp, judge_failing_qids=["gs_002"],
    )
    assert inp[0]["question_ids"] == ["gs_001", "gs_002"]
