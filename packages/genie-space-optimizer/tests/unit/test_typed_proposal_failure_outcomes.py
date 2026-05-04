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
