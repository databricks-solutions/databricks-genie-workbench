"""Witness marker builders for state machine transitions and run outcomes."""
from __future__ import annotations

import json

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


def gate_reasoning_marker(
    *, gate: str, qid: str, verdict: str,
    predicate_inputs: dict, reason: str,
) -> str:
    """Emit GSO_GATE_REASONING_V1 whenever a transformer rejects.

    predicate_inputs MUST contain the field-level inputs that produced
    the verdict so postmortems can RCA without code spelunking.
    """
    inputs_json = json.dumps(predicate_inputs, sort_keys=True, default=str)
    return (
        f"GSO_GATE_REASONING_V1 gate={gate} qid={qid} verdict={verdict} "
        f"reason={reason} predicate_inputs={inputs_json}"
    )


def canary_input_marker(
    *, iteration: int, eval_rows: int, hard_rows: int, initial_states: int,
) -> str:
    """Emit GSO_PLAN_V3_CANARY_INPUT_V1 at the canary's entry point.

    Distinguishes "didn't run" from "ran on the wrong data" from "ran on
    nothing." Without this the silent-empty-states exit is invisible.
    """
    return (
        f"GSO_PLAN_V3_CANARY_INPUT_V1 iteration={iteration} "
        f"eval_rows={eval_rows} hard_rows={hard_rows} "
        f"initial_states={initial_states}"
    )
