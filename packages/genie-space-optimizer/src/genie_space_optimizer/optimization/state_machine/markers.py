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


def patch_outcome_marker_from_attempt(
    *,
    run_id: str,
    iteration: int,
    qid: str,
    attempt,
) -> str:
    """Emit GSO_PATCH_OUTCOME_V1 derived from a ProposalAttempt terminal outcome.

    Plan 12's PatchOutcome contract is preserved as the over-the-wire
    shape, but the state machine is the source of truth: the marker is
    emitted at the same moment ``ProposalAttempt`` is appended to the
    state. This eliminates the "marker fires without state" and "state
    advances without marker" drift modes.
    """
    payload = {
        "run_id": run_id,
        "iteration": iteration,
        "qid": qid,
        "intent_id": attempt.intent_id,
        "patch_type": attempt.patch_type,
        "outcome": attempt.outcome,
        "outcome_reason": attempt.outcome_reason,
        "deepest_stage_in_attempt": attempt.deepest_stage_in_attempt.value,
        "attempt_index": attempt.attempt_index,
        "patch_outcome_id": attempt.patch_outcome_id or "",
    }
    return marker_line("GSO_PATCH_OUTCOME_V1", payload)


_CANARY_EXIT_REASONS = frozenset({
    "flag_off",
    "empty_eval_rows",
    "dispatch_input_empty",
    "import_failed",
    "run_succeeded",
    "run_failed",
    "persist_failed",
})


def canary_exit_marker(
    *, iteration: int, reason: str,
    states: int = 0, terminated: int = 0, applied: int = 0,
    exc: str = "",
) -> str:
    """Emit GSO_PLAN_V3_CANARY_V1 with a closed-vocabulary reason.

    Every exit path from maybe_run_state_machine_canary_iteration must
    call this. Replaces the prior silent ``return ()`` paths.
    """
    if reason not in _CANARY_EXIT_REASONS:
        raise ValueError(f"unknown canary exit reason: {reason}")
    return (
        f"GSO_PLAN_V3_CANARY_V1 iteration={iteration} reason={reason} "
        f"states={states} terminated={terminated} applied={applied} "
        f"exc={exc!r}"
    )
