"""Stable stdout markers for MLflow artifact persistence outcomes.

Phase E.0 Task 3. Each marker is a single line of the form
`GSO_<NAME>_V1 <json_payload>` matching the existing GSO_*_V1
convention so the postmortem analyzer can grep them deterministically.
"""

from __future__ import annotations

import json


def phase_a_artifact_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    anchor_run_id: str,
    artifact_path: str,
    success: bool,
    exception_class: str,
) -> str:
    """One-line marker emitted after each Phase A artifact persistence attempt."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "anchor_run_id": str(anchor_run_id),
        "artifact_path": str(artifact_path),
        "success": bool(success),
        "exception_class": str(exception_class),
    }
    return "GSO_PHASE_A_ARTIFACT_V1 " + json.dumps(payload, sort_keys=True)


def phase_b_artifact_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    anchor_run_id: str,
    decision_trace_path: str,
    operator_transcript_path: str,
    success: bool,
    exception_class: str,
) -> str:
    """One-line marker emitted after Phase B decision-trace + transcript persistence."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "anchor_run_id": str(anchor_run_id),
        "decision_trace_path": str(decision_trace_path),
        "operator_transcript_path": str(operator_transcript_path),
        "success": bool(success),
        "exception_class": str(exception_class),
    }
    return "GSO_PHASE_B_ARTIFACT_V1 " + json.dumps(payload, sort_keys=True)


def iteration_budget_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    consumed: bool,
    no_op_cause: str,
    applied_patches: int,
    iteration_counter_after: int,
) -> str:
    """Cycle 5 T1 — one-line marker emitted at each iteration's
    productive-budget decision so the postmortem analyzer can audit
    which iterations consumed budget and why."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "consumed": bool(consumed),
        "no_op_cause": str(no_op_cause),
        "applied_patches": int(applied_patches),
        "iteration_counter_after": int(iteration_counter_after),
    }
    return "GSO_ITERATION_BUDGET_V1 " + json.dumps(payload, sort_keys=True)


def lever6_forced_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    target_qids: tuple[str, ...],
    recommended_levers: tuple[int, ...],
    existing_patch_types: tuple[str, ...],
) -> str:
    """Cycle 7 N3 — one-line marker emitted at each forced-L6
    decision so the postmortem analyzer can audit which AGs got a
    forced Lever-6 candidate and what slate they had before."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause),
        "target_qids": [str(q) for q in target_qids],
        "recommended_levers": [int(L) for L in recommended_levers],
        "existing_patch_types": [str(p) for p in existing_patch_types],
    }
    return "GSO_LEVER6_FORCED_V1 " + json.dumps(payload, sort_keys=True)


def lever6_force_llm_declined_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
) -> str:
    """Cycle 10 W3 — Cycle 7 N3 force-L6 LLM returned no candidate."""
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause),
    }
    return "GSO_LEVER6_FORCE_LLM_DECLINED_V1 " + json.dumps(payload, sort_keys=True)


def lever6_force_raised_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    exception_repr: str,
) -> str:
    """Cycle 10 W3 — Cycle 7 N3 force-L6 raised an exception."""
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause),
        "exception_repr": str(exception_repr)[:512],
    }
    return "GSO_LEVER6_FORCE_RAISED_V1 " + json.dumps(payload, sort_keys=True)


def narrow_not_applicable_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    original_patch_type: str,
    reason: str,
) -> str:
    """Cycle 10 W4 — narrow-L6 replacement does not apply for this
    patch_type / cluster combination."""
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause),
        "original_patch_type": str(original_patch_type),
        "reason": str(reason),
    }
    return "GSO_NARROW_NOT_APPLICABLE_V1 " + json.dumps(payload, sort_keys=True)


def narrow_replacement_synthesized_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    original_patch_type: str,
    narrowing_strategy: str,
    narrow_proposal_id: str,
) -> str:
    """P0: typed marker emitted at the narrow-replacement survivor site
    in _run_narrow_l6_replacement_loop. Mirror of
    narrow_not_applicable_marker so the marker stream carries both the
    "tried and produced a survivor" and "tried and declined" outcomes.
    """
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause),
        "original_patch_type": str(original_patch_type),
        "narrowing_strategy": str(narrowing_strategy),
        "narrow_proposal_id": str(narrow_proposal_id),
    }
    return "GSO_NARROW_REPLACEMENT_SYNTHESIZED_V1 " + json.dumps(payload, sort_keys=True)


def ag_levers_unioned_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    levers_before: tuple,
    levers_after: tuple,
) -> str:
    """Cycle 10 W8 — AG had levers added by Cycle 10 W2 union."""
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "levers_before": [str(l) for l in (levers_before or ())],
        "levers_after": [str(l) for l in (levers_after or ())],
    }
    return "GSO_AG_LEVERS_UNIONED_V1 " + json.dumps(payload, sort_keys=True)


def plateau_input_source_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    source: str,
    qids_count: int,
    last_acceptance_was_rollback: bool,
) -> str:
    """Cycle 11 — emit at every plateau decision the source of the
    currently_failing input ('candidate_eval' or 'journey_ledger')."""
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "source": str(source),
        "qids_count": int(qids_count),
        "last_acceptance_was_rollback": bool(last_acceptance_was_rollback),
    }
    return "GSO_PLATEAU_INPUT_SOURCE_V1 " + json.dumps(payload, sort_keys=True)


def narrow_replacement_branch_c_synthesized_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    original_patch_type: str,
    narrow_proposal_id: str,
    target_qid: str,
) -> str:
    """Cycle 16 T3 — stdout marker for one Branch C L5 survivor.

    Mirror of ``narrow_replacement_synthesized_marker`` (Branch A) with
    the additional ``branch`` discriminator and per-QID
    ``target_qid``.
    """
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause),
        "original_patch_type": str(original_patch_type),
        "narrowing_strategy": "l5_example_sql_per_qid",
        "branch": "C",
        "narrow_proposal_id": str(narrow_proposal_id),
        "target_qid": str(target_qid),
    }
    return (
        "GSO_NARROW_REPLACEMENT_BRANCH_C_SYNTHESIZED_V1 "
        + json.dumps(payload, sort_keys=True)
    )


def structural_causal_dropped_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    rca_id: str,
    original_proposal_id: str,
    original_patch_type: str,
    original_target: str,
    drop_reason: str,
) -> str:
    """Cycle 16 T4 — one stdout marker per structural-causal blast-
    radius drop that lost causal continuity."""
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause),
        "rca_id": str(rca_id),
        "original_proposal_id": str(original_proposal_id),
        "original_patch_type": str(original_patch_type),
        "original_target": str(original_target),
        "drop_reason": str(drop_reason),
    }
    return (
        "GSO_STRUCTURAL_CAUSAL_DROPPED_V1 "
        + json.dumps(payload, sort_keys=True)
    )


def no_structural_alternative_marker(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    rca_id: str,
    root_cause: str,
    dropped_proposal_count: int,
) -> str:
    """Cycle 16 T4 — one stdout marker per AG halted because Branch C
    synthesis could not replace any structural-causal drop."""
    payload = {
        "run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "rca_id": str(rca_id),
        "root_cause": str(root_cause),
        "dropped_proposal_count": int(dropped_proposal_count),
    }
    return (
        "GSO_NO_STRUCTURAL_ALTERNATIVE_V1 "
        + json.dumps(payload, sort_keys=True)
    )
