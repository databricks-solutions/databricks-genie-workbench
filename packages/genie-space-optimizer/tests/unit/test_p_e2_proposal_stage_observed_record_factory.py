"""P-E2 — proposal-stage forbidden-AG observe-only record + marker."""
from __future__ import annotations


def test_reason_code_registered():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    members = {rc.value for rc in ReasonCode}
    assert "proposal_stage_forbidden_ag_observed" in members
