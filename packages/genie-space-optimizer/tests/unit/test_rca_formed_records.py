"""Phase B delta — Task 2.

Pins ``rca_formed_records``: one ``RCA_FORMED`` decision per cluster
that has been routed to an RCA card / theme.

Plan: ``docs/2026-05-03-phase-b-decision-trace-completion-plan.md`` Task 2.
"""
from __future__ import annotations


def test_rca_formed_records_one_per_cluster_with_rca() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_formed_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    records = rca_formed_records(
        run_id="run_1",
        iteration=1,
        clusters=[
            {"cluster_id": "H001", "question_ids": ["q1", "q2"], "root_cause": "missing_filter"},
            {"cluster_id": "H002", "question_ids": ["q3"], "root_cause": "wrong_column"},
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
    )

    assert len(records) == 1
    rec = records[0]
    assert rec.decision_type == DecisionType.RCA_FORMED
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.RCA_GROUNDED
    assert rec.cluster_id == "H001"
    assert rec.rca_id == "rca_h001"
    assert rec.root_cause == "missing_filter"
    assert rec.target_qids == ("q1", "q2")
    assert rec.affected_qids == ("q1", "q2")
    assert rec.evidence_refs == ("cluster:H001", "rca:rca_h001")
    assert rec.expected_effect
    assert rec.next_action


def test_rca_formed_records_skips_clusters_without_rca_id() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_formed_records,
    )

    records = rca_formed_records(
        run_id="run_1",
        iteration=1,
        clusters=[{"cluster_id": "H001", "question_ids": ["q1"], "root_cause": "x"}],
        rca_id_by_cluster={},
    )

    assert records == []


def test_rca_formed_records_passes_cross_checker() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_formed_records,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = rca_formed_records(
        run_id="run_1",
        iteration=1,
        clusters=[{"cluster_id": "H001", "question_ids": ["q1"], "root_cause": "x"}],
        rca_id_by_cluster={"H001": "rca_h001"},
    )
    events = [QuestionJourneyEvent(question_id="q1", stage="evaluated")]

    violations = validate_decisions_against_journey(records=records, events=events)
    assert violations == []


def test_rca_formed_records_with_dataclass_findings_pipeline() -> None:
    """End-to-end through ``rca_id_by_cluster_from_findings`` —
    matches the harness call shape so a wiring drift is caught here."""
    from genie_space_optimizer.optimization.decision_emitters import (
        rca_formed_records,
        rca_id_by_cluster_from_findings,
    )
    from genie_space_optimizer.optimization.rca import (
        rca_findings_from_clusters,
    )

    # Use a root_cause that ``rca.py:_CLUSTER_ROOT_TO_RCA_KIND`` maps
    # to a typed RcaKind, so ``rca_findings_from_clusters`` emits a
    # finding (otherwise it filters the cluster out).
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["q1"],
            "root_cause": "top_n_cardinality_collapse",
        },
    ]
    findings = rca_findings_from_clusters(clusters)
    rca_id_by_cluster = rca_id_by_cluster_from_findings(
        clusters=clusters, findings=findings,
    )

    records = rca_formed_records(
        run_id="run_1",
        iteration=1,
        clusters=clusters,
        rca_id_by_cluster=rca_id_by_cluster,
    )

    # rca_findings_from_clusters synthesises a stable rca_id per
    # cluster, so we should get exactly one RCA_FORMED record.
    assert len(records) == 1
    assert records[0].cluster_id == "H001"
    assert records[0].rca_id != ""
