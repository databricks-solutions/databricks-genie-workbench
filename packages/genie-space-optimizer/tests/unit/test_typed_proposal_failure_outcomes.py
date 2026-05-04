"""Typed proposal-failure outcomes.

Today the optimizer collapses three distinct failure modes into the
same "no candidate state" path:
  * proposer returned zero proposals
  * proposer returned a proposal but the lever-5 structural gate
    dropped it (SQL-shape RCA + no example_sql)
  * synthesis attempted but no fallback existed

This suite pins the new ReasonCode values and the emitter helpers
that distinguish them.
"""
from __future__ import annotations


def test_reason_code_proposal_generation_empty_exists() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert ReasonCode.PROPOSAL_GENERATION_EMPTY.value == "proposal_generation_empty"


def test_reason_code_structural_gate_dropped_instruction_only_exists() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert (
        ReasonCode.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
        == "structural_gate_dropped_instruction_only"
    )


def test_reason_code_no_structural_candidate_exists() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert ReasonCode.NO_STRUCTURAL_CANDIDATE.value == "no_structural_candidate"


def test_proposal_generation_empty_record_shape() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generation_empty_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        DecisionOutcome,
        ReasonCode,
    )

    rec = proposal_generation_empty_record(
        run_id="r1",
        iteration=3,
        ag_id="AG_COVERAGE_H001",
        cluster_id="H001",
        rca_id="rca_h001",
        root_cause="wrong_aggregation",
        target_qids=("gs_026",),
    )
    assert rec.decision_type == DecisionType.PROPOSAL_GENERATED
    assert rec.outcome == DecisionOutcome.DROPPED
    assert rec.reason_code == ReasonCode.PROPOSAL_GENERATION_EMPTY
    assert rec.ag_id == "AG_COVERAGE_H001"
    assert rec.cluster_id == "H001"
    assert rec.target_qids == ("gs_026",)


def test_lever5_structural_gate_record_uses_specific_reason() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    drops = [
        {
            "ag_id": "AG_COVERAGE_H002",
            "cluster_id": "H002",
            "root_causes": ["missing_filter"],
            "target_qids": ["gs_021"],
            "patch_type": "rewrite_instruction",
            "proposal_id": "P001",
        }
    ]
    records = lever5_structural_gate_records(
        run_id="r1",
        iteration=2,
        ag_id="AG_COVERAGE_H002",
        rca_id="rca_h002",
        root_cause="missing_filter",
        target_qids=["gs_021"],
        drops=drops,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec.reason_code == ReasonCode.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY
