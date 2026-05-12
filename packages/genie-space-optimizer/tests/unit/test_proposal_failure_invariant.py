"""Plan P-F — operator-transcript projection + iteration invariant tests."""

from __future__ import annotations


def test_proposal_generation_stage_projects_failure_decided_type() -> None:
    """Stage 6 (proposal_generation) carries both PROPOSAL_GENERATED and
    PROPOSAL_FAILURE_DECIDED so the operator transcript renders the
    failure record + the typed next-action label adjacent."""
    from genie_space_optimizer.optimization.operator_process_transcript import (
        _STAGE_DECISION_TYPE_MAP,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    types = _STAGE_DECISION_TYPE_MAP.get("proposal_generation", ())
    assert DecisionType.PROPOSAL_GENERATED in types
    assert DecisionType.PROPOSAL_FAILURE_DECIDED in types
