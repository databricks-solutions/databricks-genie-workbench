"""Phase C Task 5 — groundedness_gate_records producer.

Pins:

* One ``GATE_DECISION`` record per gate-rejected AG or proposal.
* ``DecisionOutcome.DROPPED``.
* ``ReasonCode`` matches the gate verdict
  (``RCA_UNGROUNDED`` / ``NO_CAUSAL_TARGET`` / ``MISSING_TARGET_QIDS``).
* ``gate=`` field is ``"rca_groundedness"``.
* Empty input → empty list.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 5.
"""
from __future__ import annotations


def _verdict(reason: str, finding_id: str = "") -> object:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    from genie_space_optimizer.optimization.rca_groundedness import GroundednessVerdict

    code = {
        "rca_ungrounded": ReasonCode.RCA_UNGROUNDED,
        "no_causal_target": ReasonCode.NO_CAUSAL_TARGET,
        "missing_target_qids": ReasonCode.MISSING_TARGET_QIDS,
    }[reason]
    return GroundednessVerdict(False, code, finding_id)


def test_one_record_per_dropped_target() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        groundedness_gate_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    drops = [
        {
            "ag_id": "AG1",
            "proposal_id": "",
            "target_qids": ["q1"],
            "rca_id": "",
            "root_cause": "missing_filter",
            "target_kind": "ag",
            "verdict": _verdict("rca_ungrounded"),
        },
        {
            "ag_id": "AG2",
            "proposal_id": "P002",
            "target_qids": ["q_unrelated"],
            "rca_id": "rca_a",
            "root_cause": "missing_filter",
            "target_kind": "proposal",
            "verdict": _verdict("no_causal_target"),
        },
    ]
    records = groundedness_gate_records(
        run_id="run_1", iteration=2, drops=drops,
    )
    assert len(records) == 2
    for r in records:
        assert r.decision_type == DecisionType.GATE_DECISION
        assert r.outcome == DecisionOutcome.DROPPED
        assert r.gate == "rca_groundedness"
    assert records[0].reason_code == ReasonCode.RCA_UNGROUNDED
    assert records[0].ag_id == "AG1"
    assert records[1].reason_code == ReasonCode.NO_CAUSAL_TARGET
    assert records[1].ag_id == "AG2"
    assert records[1].proposal_id == "P002"


def test_empty_drops_yields_empty_list() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        groundedness_gate_records,
    )

    assert groundedness_gate_records(run_id="r", iteration=1, drops=[]) == []
