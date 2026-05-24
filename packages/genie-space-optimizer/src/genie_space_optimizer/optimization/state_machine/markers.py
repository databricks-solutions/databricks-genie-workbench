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


def input_projection_parity_marker(
    *,
    iteration: int,
    harness_hard_qids: list[str],
    plan11_hard_qids: list[str],
    state_machine_hard_qids: list[str],
    missing_from_plan11: list[str],
    missing_from_sm: list[str],
) -> str:
    """Emit one GSO_INPUT_PROJECTION_PARITY_V1 per iteration that has hard rows.

    Reports the three hard-qid sets the harness, the Plan 11 dispatch
    adapter, and the v4 state-machine entry adapter independently produced
    from the same eval rows. Drift between them is observable here BEFORE
    it becomes silent starvation. The companion fail-closed marker
    ``GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1`` fires only when BOTH
    authoritative consumers see zero while the harness sees more than zero.
    """
    payload = {
        "iteration": iteration,
        "harness_hard_qids": sorted(harness_hard_qids),
        "plan11_hard_qids": sorted(plan11_hard_qids),
        "state_machine_hard_qids": sorted(state_machine_hard_qids),
        "missing_from_plan11": sorted(missing_from_plan11),
        "missing_from_sm": sorted(missing_from_sm),
    }
    return marker_line("GSO_INPUT_PROJECTION_PARITY_V1", payload)


def input_projection_contract_violation_marker(
    *,
    iteration: int,
    harness_hard_count: int,
    plan11_hard_count: int,
    sm_hard_count: int,
) -> str:
    """Emit one GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1 on starvation.

    Fires only when the harness saw N>0 hard rows AND both Plan 11 dispatch
    AND the state machine entry adapter received zero. Companion to the
    typed :class:`InputProjectionContractViolation` exception so the run
    aborts loudly instead of silently degrading to the legacy lane.
    """
    payload = {
        "iteration": iteration,
        "harness_hard_count": harness_hard_count,
        "plan11_hard_count": plan11_hard_count,
        "sm_hard_count": sm_hard_count,
    }
    return marker_line("GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1", payload)


def _classify_dispatch_drift(
    sm_hard_qids: list[str] | None,
    plan11_dispatch_qids: list[str] | None,
    *,
    sm_hard_qid_count_fallback: int,
    plan11_failing_qids_count_fallback: int,
) -> str:
    """Classify SM-vs-Plan11 dispatch QID drift.

    Set-based classification (Trial 13): cardinality alone cannot tell
    "Plan 11 emitted ``gs_009`` while SM emitted
    ``airline_..._gs_009``" from a clean run. The classifier walks the
    sets directly when both lists are provided; otherwise it falls
    back to the legacy count-based "starved" check so the older call
    sites continue to behave the same.

    Returns one of: ``"none"``, ``"starved"``, ``"namespace_mismatch"``,
    ``"partial_drift"``.
    """
    sm_set = (
        set(q for q in (sm_hard_qids or []) if str(q or "").strip())
        if sm_hard_qids is not None
        else None
    )
    p11_set = (
        set(q for q in (plan11_dispatch_qids or []) if str(q or "").strip())
        if plan11_dispatch_qids is not None
        else None
    )

    # Set-aware classification when both lists are supplied.
    if sm_set is not None and p11_set is not None:
        if not sm_set and not p11_set:
            return "none"
        if sm_set and not p11_set:
            return "starved"
        if not sm_set and p11_set:
            # Symmetric to "starved": Plan 11 sees QIDs the SM never
            # admitted. Treat as a drift event so postmortems can
            # attribute it.
            return "namespace_mismatch"
        intersection = sm_set & p11_set
        if not intersection:
            return "namespace_mismatch"
        if sm_set ^ p11_set:
            return "partial_drift"
        return "none"

    # Legacy count-based fallback (call sites that haven't been
    # migrated yet).
    if (
        plan11_failing_qids_count_fallback == 0
        and sm_hard_qid_count_fallback > 0
    ):
        return "starved"
    return "none"


def plan11_dispatch_starved_marker(
    *,
    run_id: str,
    iteration: int,
    plan11_failing_qids_count: int,
    sm_hard_qid_count: int,
    harness_hard_qid_count: int = 0,
    sm_hard_qids: list[str] | None = None,
    plan11_dispatch_qids: list[str] | None = None,
) -> str:
    """Emit GSO_PLAN11_DISPATCH_STARVED_V1 with a typed ``drift_kind``.

    Trial 13 widening: the marker now classifies the QID-set drift
    between the state machine and the Plan 11 dispatch lane:

    * ``starved`` — Plan 11 saw zero QIDs while SM had ≥1.
    * ``namespace_mismatch`` — both sides non-empty AND the
      intersection is empty (e.g. SM emits
      ``airline_ticketing_and_fare_analysis_gs_009`` while dispatch
      emits ``gs_009``). The 98ec8950 + dc89d1a9 postmortems
      observed this shape; cardinality alone did not catch it.
    * ``partial_drift`` — intersection non-empty AND symmetric
      difference non-empty (some QIDs drift between lanes).
    * ``none`` — sets match. Returns ``""`` (caller skips emission).

    Backwards-compatible: callers that pass only the counts (no
    ``sm_hard_qids`` / ``plan11_dispatch_qids`` lists) fall back to
    the Trial-12 count-based ``starved`` check.
    """
    drift_kind = _classify_dispatch_drift(
        sm_hard_qids,
        plan11_dispatch_qids,
        sm_hard_qid_count_fallback=int(sm_hard_qid_count),
        plan11_failing_qids_count_fallback=int(plan11_failing_qids_count),
    )
    if drift_kind == "none":
        return ""
    payload = {
        "run_id": run_id,
        "iteration": iteration,
        "plan11_failing_qids_count": int(plan11_failing_qids_count),
        "sm_hard_qid_count": int(sm_hard_qid_count),
        "harness_hard_qid_count": int(harness_hard_qid_count),
        "drift_kind": drift_kind,
        "sm_hard_qids": (
            [str(q) for q in sm_hard_qids] if sm_hard_qids is not None else []
        ),
        "plan11_dispatch_qids": (
            [str(q) for q in plan11_dispatch_qids]
            if plan11_dispatch_qids is not None
            else []
        ),
    }
    return marker_line("GSO_PLAN11_DISPATCH_STARVED_V1", payload)


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


def sm_legacy_equivalence_marker(
    *, iteration: int, qid: str,
    sm_terminal: str, legacy_terminal: str,
) -> str:
    """Emit GSO_PLAN_V3_EQUIVALENCE_V1 per QID per iteration.

    Agreement: SM and legacy reached the same terminal.
    Divergence reason taxonomy:
      sm_advanced_legacy_stalled: SM reached APPLIED/ACCEPTED, legacy did not.
      legacy_advanced_sm_stalled: legacy reached APPLIED/ACCEPTED, SM did not.
      different_rejection: both rejected but for different reasons.
    """
    agreement = "yes" if sm_terminal == legacy_terminal else "no"
    divergence = "none"
    if agreement == "no":
        sm_advanced = sm_terminal in ("applied", "accepted")
        legacy_advanced = legacy_terminal in ("applied", "accepted")
        if sm_advanced and not legacy_advanced:
            divergence = "sm_advanced_legacy_stalled"
        elif legacy_advanced and not sm_advanced:
            divergence = "legacy_advanced_sm_stalled"
        else:
            divergence = "different_rejection"
    return (
        f"GSO_PLAN_V3_EQUIVALENCE_V1 iteration={iteration} qid={qid} "
        f"sm_terminal={sm_terminal} legacy_terminal={legacy_terminal} "
        f"agreement={agreement} divergence_reason={divergence}"
    )
