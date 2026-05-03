"""Phase B delta — Task 1.

Pins the cluster -> rca_id derivation that the harness uses to populate
``_iter_rca_id_by_cluster`` for every Phase B producer.

The harness previously initialized this map to ``{}`` (see
``harness.py:11636-11639`` pre-Task-1), which silently zeroed out the
RCA-grounding contract on every cluster-bound decision type. The
helper here is the single source of truth for that derivation; the
harness imports and wires it.

Plan: ``docs/2026-05-03-phase-b-decision-trace-completion-plan.md`` Task 1.
"""
from __future__ import annotations


def test_rca_id_by_cluster_from_findings_picks_first_finding_per_cluster() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_id_by_cluster_from_findings,
    )

    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1", "q2"]},
        {"cluster_id": "H002", "question_ids": ["q3"]},
    ]

    class _Finding:
        def __init__(self, rca_id: str, qids: tuple[str, ...]) -> None:
            self.rca_id = rca_id
            self.target_qids = qids

    findings = [
        _Finding("rca_h001_missing_filter", ("q1", "q2")),
        _Finding("rca_h002_wrong_column", ("q3",)),
    ]

    mapping = rca_id_by_cluster_from_findings(
        clusters=clusters, findings=findings,
    )

    assert mapping == {
        "H001": "rca_h001_missing_filter",
        "H002": "rca_h002_wrong_column",
    }


def test_rca_id_by_cluster_from_findings_omits_clusters_with_no_finding() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_id_by_cluster_from_findings,
    )

    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"]},
        {"cluster_id": "H002", "question_ids": ["q3"]},
    ]

    class _Finding:
        def __init__(self, rca_id: str, qids: tuple[str, ...]) -> None:
            self.rca_id = rca_id
            self.target_qids = qids

    mapping = rca_id_by_cluster_from_findings(
        clusters=clusters,
        findings=[_Finding("rca_h001_x", ("q1",))],
    )

    assert mapping == {"H001": "rca_h001_x"}


def test_rca_id_by_cluster_from_findings_handles_dict_findings() -> None:
    """``rca_findings_from_clusters`` returns dataclasses, but other call
    sites supply dict findings; the helper must accept both."""
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_id_by_cluster_from_findings,
    )

    mapping = rca_id_by_cluster_from_findings(
        clusters=[{"cluster_id": "H001", "question_ids": ["q1"]}],
        findings=[{"rca_id": "rca_h001", "target_qids": ["q1"]}],
    )

    assert mapping == {"H001": "rca_h001"}
