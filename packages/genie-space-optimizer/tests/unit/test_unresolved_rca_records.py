"""Phase C Task 7 — unresolved_rca_records producer.

Pins:

* One ``RCA_FORMED`` + ``UNRESOLVED`` + ``RCA_UNGROUNDED`` record per
  cluster that has hard failures but no matching RCA finding.
* The record carries the cluster's ``question_ids`` as ``target_qids``
  (so the validator's ``target_qids`` check passes).
* Empty ``rca_id`` is allowed (the validator exemption from the
  validator-widening step covers this).
* Clusters that DO have findings produce no record (the existing
  ``rca_formed_records`` producer covers them).
* Empty input → empty list.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 7.
"""
from __future__ import annotations


def test_one_unresolved_record_per_cluster_without_finding() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        unresolved_rca_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"], "root_cause": "missing_filter"},
        {"cluster_id": "H002", "question_ids": ["q2"], "root_cause": ""},
    ]
    # rca_id_by_cluster only covers H001 — H002 is the "no RCA" case.
    rca_id_by_cluster = {"H001": "rca_a"}

    records = unresolved_rca_records(
        run_id="run_1",
        iteration=2,
        clusters=clusters,
        rca_id_by_cluster=rca_id_by_cluster,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec.decision_type == DecisionType.RCA_FORMED
    assert rec.outcome == DecisionOutcome.UNRESOLVED
    assert rec.reason_code == ReasonCode.RCA_UNGROUNDED
    assert rec.cluster_id == "H002"
    assert rec.rca_id == ""
    assert rec.target_qids == ("q2",)
    assert rec.affected_qids == ("q2",)
    # ``root_cause`` is empty in the cluster; the producer stamps a
    # placeholder so the validator's root_cause exemption applies
    # cleanly when reason_code == RCA_UNGROUNDED.
    assert rec.root_cause == "" or rec.root_cause == "unknown"


def test_validator_passes_for_unresolved_records() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        unresolved_rca_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    clusters = [{"cluster_id": "H_ORPHAN", "question_ids": ["q_o"], "root_cause": ""}]
    records = unresolved_rca_records(
        run_id="r",
        iteration=1,
        clusters=clusters,
        rca_id_by_cluster={},
    )
    violations = validate_decisions_against_journey(
        records=list(records), events=[],
    )
    # No "has no rca_id" violations on the new record.
    rca_id_violations = [v for v in violations if "has no rca_id" in v]
    assert rca_id_violations == [], (
        f"Validator should not flag rca_id on RCA_FORMED+UNRESOLVED+RCA_UNGROUNDED; "
        f"got: {rca_id_violations}"
    )


def test_empty_clusters_yields_empty_list() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        unresolved_rca_records,
    )

    assert unresolved_rca_records(
        run_id="r", iteration=1, clusters=[], rca_id_by_cluster={},
    ) == []


def test_clusters_with_findings_produce_no_unresolved_record() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        unresolved_rca_records,
    )

    clusters = [{"cluster_id": "H_OK", "question_ids": ["q_ok"], "root_cause": "missing_filter"}]
    rca_id_by_cluster = {"H_OK": "rca_ok"}

    assert unresolved_rca_records(
        run_id="r",
        iteration=1,
        clusters=clusters,
        rca_id_by_cluster=rca_id_by_cluster,
    ) == []
