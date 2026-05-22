"""Witness marker builders for state machine transitions and run outcomes."""
from __future__ import annotations

from genie_space_optimizer.optimization.run_analysis_contract import marker_line
from genie_space_optimizer.optimization.state_machine.records import (
    StageTransition,
)


def qstate_transition_marker(
    *,
    run_id: str,
    iteration: int,
    qid: str,
    transition: StageTransition,
) -> str:
    """Emit one GSO_QSTATE_TRANSITION_V1 marker per state machine transition."""
    payload = {
        "run_id": run_id,
        "iteration": iteration,
        "qid": qid,
        "from_stage": transition.from_stage.value,
        "to_stage": transition.to_stage.value,
        "at_ms": transition.at_ms,
        "transformer_name": transition.transformer_name,
        "transition_kind": transition.transition_kind,
        "reason": transition.reason,
        "proposal_attempt_index": (
            transition.proposal_attempt_index
            if transition.proposal_attempt_index is not None
            else -1
        ),
    }
    return marker_line("GSO_QSTATE_TRANSITION_V1", payload)


def optimizer_outcome_marker(
    *,
    run_id: str,
    outcome: str,
    hard_qids_count: int,
    deepest_stage_by_qid: dict[str, str],
) -> str:
    """Emit one GSO_OPTIMIZER_OUTCOME_V1 marker per lever-loop run."""
    payload = {
        "run_id": run_id,
        "outcome": outcome,
        "hard_qids_count": hard_qids_count,
        "deepest_stage_by_qid": deepest_stage_by_qid,
    }
    return marker_line("GSO_OPTIMIZER_OUTCOME_V1", payload)
