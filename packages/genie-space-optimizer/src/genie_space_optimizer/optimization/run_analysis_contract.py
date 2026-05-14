"""CLI-readable output contract for GSO lever-loop run analysis.

This module is intentionally Spark/Databricks/MLflow free. It only builds
stable single-line JSON markers that the run-analysis skill can parse from
Databricks task stdout.
"""

from __future__ import annotations

import json
import re
from typing import Any, Mapping, Sequence


_MARKER_NAME_RE = re.compile(r"^GSO_[A-Z0-9_]+_V[1-9]\d*$")


def _clean(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(k): _clean(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple, set)):
        return [_clean(v) for v in value]
    return str(value)


def marker_line(marker: str, payload: Mapping[str, Any]) -> str:
    """Return one stable stdout marker line."""
    clean_marker = str(marker).strip()
    if not _MARKER_NAME_RE.match(clean_marker):
        raise ValueError(f"invalid GSO marker name: {marker!r}")
    clean_payload = {str(k): _clean(v) for k, v in payload.items()}
    encoded = json.dumps(clean_payload, sort_keys=True, separators=(",", ":"))
    return f"{clean_marker} {encoded}"


def collect_effective_flags() -> dict[str, Any]:
    """Snapshot every public ``*_enabled()`` accessor in ``common/config``.

    Returns a dict mapping the accessor's stem (e.g. ``"target_aware_acceptance"``
    for ``target_aware_acceptance_enabled``) to its boolean return value.
    Accessors that raise are recorded as ``None`` so the manifest still
    surfaces their existence.
    """
    import inspect

    from genie_space_optimizer.common import config as _config

    flags: dict[str, Any] = {}
    for name, member in inspect.getmembers(_config, callable):
        if not name.endswith("_enabled"):
            continue
        if name.startswith("_"):
            continue
        # Only zero-required-arg callables qualify.
        try:
            sig = inspect.signature(member)
        except (TypeError, ValueError):
            continue
        required = [
            p for p in sig.parameters.values()
            if p.default is inspect.Parameter.empty
            and p.kind not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        ]
        if required:
            continue
        stem = name[: -len("_enabled")]
        try:
            flags[stem] = bool(member())
        except Exception:
            flags[stem] = None
    return flags


def run_manifest_marker(
    *,
    optimization_run_id: str,
    databricks_job_id: str = "",
    databricks_parent_run_id: str = "",
    lever_loop_task_run_id: str = "",
    mlflow_experiment_id: str = "",
    space_id: str = "",
    event: str,
) -> str:
    return marker_line(
        "GSO_RUN_MANIFEST_V1",
        {
            "optimization_run_id": optimization_run_id,
            "databricks_job_id": databricks_job_id,
            "databricks_parent_run_id": databricks_parent_run_id,
            "lever_loop_task_run_id": lever_loop_task_run_id,
            "mlflow_experiment_id": mlflow_experiment_id,
            "space_id": space_id,
            "event": event,
        },
    )


def run_manifest_v2_marker(
    *,
    optimization_run_id: str,
    databricks_job_id: str = "",
    databricks_parent_run_id: str = "",
    lever_loop_task_run_id: str = "",
    mlflow_experiment_id: str = "",
    space_id: str = "",
    event: str,
    wheel_sha: str = "",
    git_sha: str = "",
    effective_flags: Mapping[str, Any] | None = None,
    python_version: str = "",
    domain: str = "",
) -> str:
    """Cycle 12-T1 — extended run manifest with experimental-setup metadata.

    Emitted alongside (not in place of) ``GSO_RUN_MANIFEST_V1`` so existing
    parsers and replay fixtures keep working unchanged. The V2 line carries
    every V1 field plus ``wheel_sha`` / ``git_sha`` / ``effective_flags`` /
    ``python_version`` / ``domain`` so a postmortem can answer
    "what code/flags actually ran?" by reading exactly one record.
    """
    return marker_line(
        "GSO_RUN_MANIFEST_V2",
        {
            "optimization_run_id": optimization_run_id,
            "databricks_job_id": databricks_job_id,
            "databricks_parent_run_id": databricks_parent_run_id,
            "lever_loop_task_run_id": lever_loop_task_run_id,
            "mlflow_experiment_id": mlflow_experiment_id,
            "space_id": space_id,
            "event": event,
            "wheel_sha": wheel_sha,
            "git_sha": git_sha,
            "effective_flags": dict(effective_flags) if effective_flags else {},
            "python_version": python_version,
            "domain": domain,
        },
    )


def iteration_summary_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    accepted_count: int,
    rolled_back_count: int,
    skipped_count: int,
    gate_drop_count: int,
    decision_record_count: int,
    journey_violation_count: int,
) -> str:
    return marker_line(
        "GSO_ITERATION_SUMMARY_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "accepted_count": int(accepted_count),
            "rolled_back_count": int(rolled_back_count),
            "skipped_count": int(skipped_count),
            "gate_drop_count": int(gate_drop_count),
            "decision_record_count": int(decision_record_count),
            "journey_violation_count": int(journey_violation_count),
        },
    )


def phase_b_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    decision_record_count: int,
    decision_validation_count: int,
    transcript_chars: int,
    decision_trace_artifact: str,
    operator_transcript_artifact: str,
    persist_ok: bool,
) -> str:
    return marker_line(
        "GSO_PHASE_B_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "decision_record_count": int(decision_record_count),
            "decision_validation_count": int(decision_validation_count),
            "transcript_chars": int(transcript_chars),
            "decision_trace_artifact": decision_trace_artifact,
            "operator_transcript_artifact": operator_transcript_artifact,
            "persist_ok": bool(persist_ok),
        },
    )


def convergence_marker(
    *,
    optimization_run_id: str,
    reason: str,
    iteration_counter: int,
    best_accuracy: float | None,
    thresholds_met: bool,
) -> str:
    return marker_line(
        "GSO_CONVERGENCE_V1",
        {
            "optimization_run_id": optimization_run_id,
            "reason": reason,
            "iteration_counter": int(iteration_counter),
            "best_accuracy": "" if best_accuracy is None else float(best_accuracy),
            "thresholds_met": bool(thresholds_met),
        },
    )


def phase_b_no_records_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    reason: str,
    producer_exceptions: Mapping[str, int] | None = None,
    contract_version: str = "v1",
) -> str:
    """Marker emitted when an iteration produces zero ``DecisionRecord``s.

    Distinguishes "Phase B ran but had nothing to record" from "Phase B
    never ran" (deploy is stale; ``contract_version`` tag absent) and
    from a silent producer error (``producer_exceptions`` carries the
    counters). The reason string is drawn from the closed
    ``NoRecordsReason`` vocabulary in
    ``optimization/decision_emitters.py``.
    """
    return marker_line(
        "GSO_PHASE_B_NO_RECORDS_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "reason": str(reason or ""),
            "producer_exceptions": dict(producer_exceptions or {}),
            "contract_version": str(contract_version or ""),
        },
    )


def phase_b_end_marker(
    *,
    optimization_run_id: str,
    total_records: int,
    iter_record_counts: list[int],
    iter_violation_counts: list[int],
    no_records_iterations: list[int],
    contract_version: str,
) -> str:
    """Marker emitted once at lever-loop terminate.

    Carries the per-iter record/violation counts plus a list of
    iterations that produced zero records (so the analyzer can correlate
    the end-of-loop view with per-iter ``GSO_PHASE_B_NO_RECORDS_V1``
    markers). Fires on every termination path (plateau, max-iterations,
    convergence, raise) — see harness exit-path audit test.
    """
    return marker_line(
        "GSO_PHASE_B_END_V1",
        {
            "optimization_run_id": optimization_run_id,
            "total_records": int(total_records),
            "iter_record_counts": [int(n) for n in (iter_record_counts or [])],
            "iter_violation_counts": [int(n) for n in (iter_violation_counts or [])],
            "no_records_iterations": [int(n) for n in (no_records_iterations or [])],
            "contract_version": str(contract_version or ""),
        },
    )


def bundle_assembly_incomplete_marker(
    *,
    optimization_run_id: str,
    parent_bundle_run_id: str = "",
    total_declared: int,
    total_materialized: int,
    missing_count: int,
    parent_level_missing: Sequence[str],
    unmigrated_per_iteration_missing: Sequence[str],
) -> str:
    """Cycle 12-T3 — emit a typed gap report after the parent-bundle
    upload completes.

    Distinct from ``GSO_BUNDLE_ASSEMBLY_FAILED_V1`` (which fires when the
    whole assembly block raised). This marker fires when the assembler
    completed but did not produce every declared path.
    """
    return marker_line(
        "GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1",
        {
            "optimization_run_id": optimization_run_id,
            "parent_bundle_run_id": parent_bundle_run_id,
            "total_declared": int(total_declared),
            "total_materialized": int(total_materialized),
            "missing_count": int(missing_count),
            "parent_level_missing": list(parent_level_missing),
            "unmigrated_per_iteration_missing": list(unmigrated_per_iteration_missing),
        },
    )


def patch_isolation_diagnostic_marker(
    *,
    optimization_run_id: str = "",
    iteration: int,
    ag_id: str,
    regressed_qid: str,
    attribution_status: str,
    attribution_confidence: float,
    expanded_patch_id: str = "",
    live_mode: bool = False,
) -> str:
    """Cycle 14B-T3 — diagnostic-only marker emitted by the patch-
    subset isolation orchestrator when the partial-harvest gate
    rejected and isolation attribution was attempted.

    ``attribution_status`` is one of ``single_patch`` /
    ``multi_patch`` / ``no_attribution``. ``attribution_confidence``
    matches ``SinglePatchAttribution.confidence`` (1.0 / 0.5 / 0.0).
    ``live_mode`` indicates whether the LIVE flag was on at emit
    time (does not imply a live re-eval ran — the substrate check
    may still have skipped the live arm).
    """
    return marker_line(
        "GSO_PATCH_ISOLATION_DIAGNOSTIC_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "regressed_qid": str(regressed_qid),
            "attribution_status": str(attribution_status),
            "attribution_confidence": float(attribution_confidence),
            "expanded_patch_id": str(expanded_patch_id),
            "live_mode": bool(live_mode),
        },
    )


def patch_isolation_outcome_marker(
    *,
    optimization_run_id: str = "",
    iteration: int,
    ag_id: str,
    outcome: str,
    subset_aggregate_gain_pp: float,
    subset_debt_qids: Sequence[str],
    expanded_patch_id_removed: str,
) -> str:
    """Cycle 14B-T3 — marker emitted after a live patch-subset
    isolation re-eval completes (or the live arm declines to run).

    ``outcome`` is one of ``IsolationVerdict.outcome`` values
    (``subset_accepts_clean`` / ``subset_accepts_with_debt`` /
    ``subset_still_over_policy`` / ``subset_regresses_aggregate``)
    plus a sentinel ``live_arm_disabled_stub`` for the diagnostic-
    only stub path.
    """
    return marker_line(
        "GSO_PATCH_ISOLATION_OUTCOME_V1",
        {
            "optimization_run_id": optimization_run_id,
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "outcome": str(outcome),
            "subset_aggregate_gain_pp": float(subset_aggregate_gain_pp),
            "subset_debt_qids": list(subset_debt_qids),
            "expanded_patch_id_removed": str(expanded_patch_id_removed),
        },
    )


def full_eval_marker(
    *,
    optimization_run_id: str,
    payload: dict,
) -> str:
    """Cycle 14-T2 — canonical typed stdout marker for one AG's full
    eval outcome.

    Emitted alongside (not in place of) the human-readable FULL EVAL
    [{ag_id}] print block. Both surfaces consume
    ``format_full_eval_marker_payload`` so divergence between the
    typed marker and the human text is structurally impossible.

    Behind ``GSO_CANONICAL_ACCEPTANCE_RENDER`` (default on); on
    flag-off the marker is not emitted and only the legacy text
    block survives, preserving byte-stable replay of pre-T2 fixtures.
    """
    return marker_line(
        "GSO_FULL_EVAL_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "payload": dict(payload or {}),
        },
    )


# ── Cycle 14-V — shadow-mode + regression-rail markers ───────────────


def forbidden_ag_admission_observe_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    rollback_class: str,
    rollback_reason: str,
    root_cause: str,
    blame_set: tuple,
    lever_set: tuple,
    would_admit: bool,
    behavior_flag_on: bool,
    suppressed_by_admit_no_action_off: bool,
) -> str:
    """Cycle 14-V T1 — shadow emission for the C13 admission predicate.

    Emitted once per ``NO_ACTION`` reflection processed by
    ``_compute_forbidden_ag_set`` when
    ``forbidden_ag_admission_observe_enabled()`` is True. The
    payload records what the predicate decided AND why; the most
    important field is ``suppressed_by_admit_no_action_off``,
    which is True when the predicate would have admitted under
    ``admit_no_action=True`` but did not under the live flag value.
    """
    return marker_line(
        "GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "rollback_class": str(rollback_class or ""),
            "rollback_reason": str(rollback_reason or ""),
            "root_cause": str(root_cause or ""),
            "blame_set": [str(b) for b in (blame_set or ())],
            "lever_set": [int(l) for l in (lever_set or ())],
            "would_admit_with_admit_no_action_on": bool(would_admit),
            "behavior_flag_on": bool(behavior_flag_on),
            "suppressed_by_admit_no_action_off": bool(
                suppressed_by_admit_no_action_off
            ),
        },
    )


def patch_isolation_observe_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    reason_code: str,
    regressed_qid: str,
    attribution_status: str,
    attribution_confidence: float,
    expanded_patch_id: str,
    behavior_flag_on: bool,
    suppressed_by_isolation_flag_off: bool,
) -> str:
    """Cycle 14-V T2 — shadow emission for the C14B-T3 diagnostic
    orchestrator.

    Emitted once per acceptance decision whose ``reason_code`` is
    in the canonical isolation-eligible set, when
    ``patch_isolation_observe_enabled()`` is True.
    """
    return marker_line(
        "GSO_PATCH_ISOLATION_OBSERVE_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "ag_id": str(ag_id or ""),
            "reason_code": str(reason_code or ""),
            "regressed_qid": str(regressed_qid or ""),
            "attribution_status": str(attribution_status or ""),
            "attribution_confidence": float(attribution_confidence),
            "expanded_patch_id": str(expanded_patch_id or ""),
            "behavior_flag_on": bool(behavior_flag_on),
            "suppressed_by_isolation_flag_off": bool(
                suppressed_by_isolation_flag_off
            ),
        },
    )


def canonical_render_invariant_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    violation_class: str,
    contradicting_qids: tuple,
    detail: str,
) -> str:
    """Cycle 14-V T4 — emitted by ``format_full_eval_marker_payload``
    when its own output contains a same-QID contradiction across
    rendered fields. Silent on clean payloads.

    ``violation_class`` enum:
      - ``fixed_and_still_hard_overlap``
      - ``target_in_out_of_target_set``
      - ``delta_state_disagrees_with_bucket``
    """
    return marker_line(
        "GSO_CANONICAL_RENDER_INVARIANT_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "ag_id": str(ag_id or ""),
            "violation_class": str(violation_class or ""),
            "contradicting_qids": [str(q) for q in (contradicting_qids or ())],
            "detail": str(detail or ""),
        },
    )


def databricks_ids_resolved_marker(
    *,
    resolution_path: str,
    fields_resolved: int,
    fields_total: int,
    dbutils_attempted: bool,
    dbutils_succeeded: bool,
    jobs_api_attempted: bool = False,
    jobs_api_succeeded: bool = False,
    sample_field: str = "",
    sample_value: str = "",
) -> str:
    """Cycle 14-W T3 + P-B Tier-3 — resolution-path tracing for
    ``_databricks_ids_from_env`` / ``stages.run_manifest``.

    ``resolution_path`` ∈ {``env``, ``dbutils``, ``mixed``,
    ``jobs_api``, ``mixed_jobs_api``, ``sentinel``}.

    ``jobs_api_attempted`` / ``jobs_api_succeeded`` (P-B) record
    whether the Tier-3 Jobs-API fallback fired and whether it
    populated any ID. Defaults are False so existing call sites
    (which predate Tier-3) keep emitting valid markers.
    """
    return marker_line(
        "GSO_DATABRICKS_IDS_RESOLVED_V1",
        {
            "resolution_path": str(resolution_path or ""),
            "fields_resolved": int(fields_resolved),
            "fields_total": int(fields_total),
            "dbutils_attempted": bool(dbutils_attempted),
            "dbutils_succeeded": bool(dbutils_succeeded),
            "jobs_api_attempted": bool(jobs_api_attempted),
            "jobs_api_succeeded": bool(jobs_api_succeeded),
            "sample_field": str(sample_field or ""),
            "sample_value": str(sample_value or ""),
        },
    )


def check_iteration_summary_totality(
    *,
    iteration_counter: int,
    iteration_summary_count: int,
    phase_b_iter_record_counts_length: int,
) -> dict | None:
    """Cycle 14-W T5 — pure helper that detects the
    iteration-summary totality violation.

    Returns ``None`` on a clean run (all three values equal) or
    a violation dict suitable for direct emission via
    :func:`iteration_summary_totality_marker` when any pair
    disagrees.
    """
    if (
        int(iteration_counter)
        == int(iteration_summary_count)
        == int(phase_b_iter_record_counts_length)
    ):
        return None
    return {
        "iteration_counter": int(iteration_counter),
        "iteration_summary_count": int(iteration_summary_count),
        "phase_b_iter_record_counts_length": int(
            phase_b_iter_record_counts_length
        ),
    }


def iteration_summary_totality_marker(
    *,
    optimization_run_id: str = "",
    iteration_counter: int,
    iteration_summary_count: int,
    phase_b_iter_record_counts_length: int,
) -> str:
    """Cycle 14-W T5 — invariant alarm: ``iteration_counter`` must
    equal both the number of emitted ``GSO_ITERATION_SUMMARY_V1``
    markers AND the length of
    ``GSO_PHASE_B_END_V1.iter_record_counts``. Silent on clean
    runs; emits when any of the three disagree.
    """
    return marker_line(
        "GSO_ITERATION_SUMMARY_TOTALITY_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration_counter": int(iteration_counter),
            "iteration_summary_count": int(iteration_summary_count),
            "phase_b_iter_record_counts_length": int(
                phase_b_iter_record_counts_length
            ),
            "expected_equality": (
                "iteration_counter == iteration_summary_count == "
                "phase_b_iter_record_counts_length"
            ),
        },
    )


def detect_phase_h_acceptance_drift(
    *,
    canonical_outcome: str,
    canonical_reason_code: str,
    phase_h_outcome: str,
    phase_h_reason_code: str,
) -> bool:
    """Cycle 14-W T6 — pure helper that returns True iff the
    Phase H acceptance writer's output disagrees with the canonical
    ``ControlPlaneAcceptance`` decision on either ``outcome`` or
    ``reason_code``.

    Anchor: airline run 1105451933925748 F8 — stdout says iter 1
    ACCEPTED, downloaded acceptance bundle says
    ``outcome=rolled_back, reason_code=missing_pre_rows``.
    """
    return (
        str(canonical_outcome or "").strip().lower()
        != str(phase_h_outcome or "").strip().lower()
    ) or (
        str(canonical_reason_code or "").strip().lower()
        != str(phase_h_reason_code or "").strip().lower()
    )


def detect_phase_h_journey_drift(
    *,
    canonical_violation_count: int,
    phase_h_violation_count: int,
) -> bool:
    """Cycle 14-W T6 — pure helper that returns True iff the
    Phase H journey-validator output disagrees with the canonical
    replay validator on the violation count.

    Anchor: 7Now run 960148942255012 F8 — local replay reports 25
    journey violations; Phase H ``journey_validation_all.json``
    reports 0.
    """
    return int(canonical_violation_count) != int(phase_h_violation_count)


def phase_h_acceptance_drift_marker(
    *,
    optimization_run_id: str = "",
    iteration: int,
    canonical_outcome: str,
    canonical_reason_code: str,
    phase_h_outcome: str,
    phase_h_reason_code: str,
) -> str:
    """Plan P-C — runtime shadow marker. SHOULD be zero on every
    production run after Plan P-C lands.

    Plan P-C unified the canonical and Phase-H acceptance render
    paths around one ``ControlPlaneAcceptance`` instance and one
    pure renderer (``render_acceptance_decision``). With both paths
    consuming the same decision, this drift detector should never
    fire on a healthy production run.

    Runtime emission is deliberately KEPT ACTIVE as defense-in-depth.
    The runtime path will only be demoted (or silenced) after the
    corpus proves silence across at least one full deployed cycle
    — concretely, zero firings across N>=10 consecutive production
    runs spanning at least one calendar week. Until that bar is
    cleared, treat any firing as a HIGH-tier contract-health
    violation requiring a postmortem and an entry in the
    runid_analysis/ tree.

    Replay-test equivalence: ``check_i9_acceptance_render_byte_equality``
    asserts byte-equal rendering between ``acceptance_decision`` and
    ``full_eval_marker`` per iteration; this is the test-side
    counterpart of the runtime shadow.

    Cycle 14-W T6 origin: alarm fired when Phase H acceptance writer
    disagreed with the canonical ``ControlPlaneAcceptance`` decision.
    """
    return marker_line(
        "GSO_PHASE_H_ACCEPTANCE_DRIFT_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "canonical_outcome": str(canonical_outcome or ""),
            "canonical_reason_code": str(canonical_reason_code or ""),
            "phase_h_outcome": str(phase_h_outcome or ""),
            "phase_h_reason_code": str(phase_h_reason_code or ""),
        },
    )


def phase_h_journey_drift_marker(
    *,
    optimization_run_id: str = "",
    iteration: int,
    canonical_violation_count: int,
    phase_h_violation_count: int,
) -> str:
    """Cycle 14-W T6 — alarm: Phase H journey-validator output
    disagrees with the canonical replay validator.
    """
    return marker_line(
        "GSO_PHASE_H_JOURNEY_DRIFT_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "canonical_violation_count": int(canonical_violation_count),
            "phase_h_violation_count": int(phase_h_violation_count),
        },
    )


def attribution_drift_marker(
    *,
    optimization_run_id: str = "",
    iteration: int,
    ag_id: str,
    baseline_accuracy: float,
    candidate_accuracy: float,
    delta_pp: float,
    target_qids: Sequence[str],
    accidentally_improved_qids: Sequence[str],
    unresolved_target_debt_qids: Sequence[str],
) -> str:
    """Cycle 14-C T5 — diagnostic marker emitted on every
    ``accepted_with_attribution_drift`` acceptance.

    Records the reattribution payload so postmortem tooling can grep
    for the branch's firings without parsing every
    ``GSO_FULL_EVAL_V1`` marker. Diagnostic-only; subset of
    ``GSO_FULL_EVAL_V1``'s payload.

    Anchor: airline run 1105451933925748 iter 1 — first
    in-production demonstration of keep-the-win behaviour on a
    target-drift case.
    """
    return marker_line(
        "GSO_ATTRIBUTION_DRIFT_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "ag_id": str(ag_id or ""),
            "baseline_accuracy": float(baseline_accuracy),
            "candidate_accuracy": float(candidate_accuracy),
            "delta_pp": float(delta_pp),
            "target_qids": [str(q) for q in (target_qids or ())],
            "accidentally_improved_qids": [
                str(q) for q in (accidentally_improved_qids or ())
            ],
            "unresolved_target_debt_qids": [
                str(q) for q in (unresolved_target_debt_qids or ())
            ],
        },
    )


def bundle_assembly_list_normalized_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    stage_key: str,
    original_type: str,
    normalized_to: str,
) -> str:
    """Cycle 14-V T5 — diagnostic for the bundle assembler list-value
    normalization safety net. Emitted whenever
    ``_normalize_stage_capture`` discards a non-dict-shaped value
    so postmortem tooling can measure how often this engages.
    """
    return marker_line(
        "GSO_BUNDLE_ASSEMBLY_LIST_NORMALIZED_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "stage_key": str(stage_key or ""),
            "original_type": str(original_type or ""),
            "normalized_to": str(normalized_to or ""),
        },
    )


def phase_h_strict_validation_marker(
    *,
    optimization_run_id: str,
    flag_enabled: bool,
    declared_count: int,
    materialized_count: int,
    self_write_count: int,
    missing_count: int,
    listing_status: str,
    validator_status: str,
    exception_class: str = "",
) -> str:
    """Cycle 12-T2 — typed observability for the Phase H strict path validator.

    Statuses are one of:
      ``listing_status``: ``"ok"`` | ``"skipped"`` | ``"failed"``
      ``validator_status``: ``"ok"`` | ``"skipped"`` | ``"failed"``

    A non-empty ``exception_class`` paired with ``listing_status="failed"``
    or ``validator_status="failed"`` names the specific exception type that
    was caught. ``"skipped"`` indicates the stage didn't run because an
    upstream stage failed or the flag was off.
    """
    return marker_line(
        "GSO_PHASE_H_STRICT_VALIDATION_V1",
        {
            "optimization_run_id": optimization_run_id,
            "flag_enabled": bool(flag_enabled),
            "declared_count": int(declared_count),
            "materialized_count": int(materialized_count),
            "self_write_count": int(self_write_count),
            "missing_count": int(missing_count),
            "listing_status": str(listing_status),
            "validator_status": str(validator_status),
            "exception_class": str(exception_class),
        },
    )


def artifact_index_marker(
    *,
    optimization_run_id: str,
    parent_bundle_run_id: str,
    artifact_index_path: str,
    iterations: list[int],
) -> str:
    """Emit GSO_ARTIFACT_INDEX_V1 with parent bundle pointers (Phase H).

    Read by tools.marker_parser.parse_markers; consumed by evidence_bundle
    and the gso-postmortem skill to locate the parent bundle in MLflow
    even when stdout is truncated.
    """
    return marker_line(
        "GSO_ARTIFACT_INDEX_V1",
        {
            "optimization_run_id": optimization_run_id,
            "parent_bundle_run_id": parent_bundle_run_id,
            "artifact_index_path": artifact_index_path,
            "iterations": [int(n) for n in (iterations or [])],
        },
    )


def lever_loop_exit_manifest(
    *,
    optimization_run_id: str,
    mlflow_experiment_id: str,
    accuracy: float,
    iteration_counter: int,
    levers_attempted: list,
    levers_accepted: list,
    levers_rolled_back: list,
    per_iteration_decision_counts: list[int],
    per_iteration_journey_violations: list[int],
    no_decision_record_reasons: list[str],
    phase_b_decision_artifacts: list[str],
    phase_b_transcript_artifacts: list[str],
    # Phase F+H C18 (v2) — Phase H T13 bundle pointers.
    # Optional: defaults preserve the existing JSON shape so callers
    # that don't yet plumb the bundle (replay, legacy paths) emit the
    # same payload as before.
    parent_bundle_run_id: str | None = None,
    artifact_index_path: str | None = None,
    iterations_completed: list[int] | None = None,
) -> str:
    """Build the JSON string passed to ``dbutils.notebook.exit`` from
    the lever-loop task.

    Surfaces decision counts, journey violations, and Phase B artifact
    paths so ``databricks jobs get-run-output`` reveals the same numbers
    MLflow has. Returned as a JSON string (not dict) so the call site
    stays a single ``dbutils.notebook.exit(lever_loop_exit_manifest(...))``.

    The Phase F+H C18 (v2) bundle pointers are optional: when provided
    by the harness, the parent bundle's MLflow run id and the artifact
    index path land in the exit JSON so postmortem tooling can locate
    the gso_postmortem_bundle/ artifacts even when stdout is truncated.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "mlflow_experiment_id": str(mlflow_experiment_id),
        "accuracy": float(accuracy),
        "iteration_counter": int(iteration_counter),
        "levers_attempted": list(levers_attempted),
        "levers_accepted": list(levers_accepted),
        "levers_rolled_back": list(levers_rolled_back),
        "per_iteration_decision_counts": [
            int(n) for n in (per_iteration_decision_counts or [])
        ],
        "per_iteration_journey_violations": [
            int(n) for n in (per_iteration_journey_violations or [])
        ],
        "no_decision_record_reasons": [
            str(r) for r in (no_decision_record_reasons or [])
        ],
        "phase_b_decision_artifacts": [
            str(p) for p in (phase_b_decision_artifacts or [])
        ],
        "phase_b_transcript_artifacts": [
            str(p) for p in (phase_b_transcript_artifacts or [])
        ],
    }
    if parent_bundle_run_id:
        payload["parent_bundle_run_id"] = str(parent_bundle_run_id)
    if artifact_index_path:
        payload["artifact_index_path"] = str(artifact_index_path)
    if iterations_completed is not None:
        payload["iterations_completed"] = [
            int(n) for n in iterations_completed
        ]
    return json.dumps(payload, default=str)


def finalize_exit_manifest(
    *,
    optimization_run_id: str,
    status: str,
    convergence_reason: str,
    repeatability_pct: float,
    elapsed_seconds: float,
    report_path: str,
    promoted_to_champion: bool,
) -> str:
    """Build the JSON string passed to ``dbutils.notebook.exit`` from
    the finalize task.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "status": str(status),
        "convergence_reason": str(convergence_reason),
        "repeatability_pct": float(repeatability_pct),
        "elapsed_seconds": float(elapsed_seconds),
        "report_path": str(report_path),
        "promoted_to_champion": bool(promoted_to_champion),
    }
    return json.dumps(payload, default=str)


def bundle_assembly_failed_marker(
    *,
    optimization_run_id: str,
    parent_bundle_run_id: str | None,
    error_type: str,
    error_message: str,
) -> str:
    """Stable stdout marker emitted when Phase H ``gso_postmortem_bundle``
    assembly fails. The postmortem skill (``gso-postmortem``) and
    ``mlflow_audit`` recognize it as authoritative evidence that a
    Phase H run was intended but the bundle did not land.

    Parsed by ``tools.marker_parser`` alongside ``GSO_RUN_MANIFEST_V1``
    and ``GSO_ARTIFACT_INDEX_V1``.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "parent_bundle_run_id": (
            str(parent_bundle_run_id) if parent_bundle_run_id else None
        ),
        "error_type": str(error_type),
        "error_message": str(error_message)[:2000],
    }
    return "GSO_BUNDLE_ASSEMBLY_FAILED_V1 " + json.dumps(payload, sort_keys=True)


def proposal_generation_empty_marker(
    *,
    ag_id: str,
    iteration: int,
    target_qids: Sequence[str] | None = None,
) -> str:
    """P4 — stdout marker emitted when an AG produces zero proposals.

    Distinct from ``GSO_STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY_V1``
    (proposal existed but was dropped) and
    ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` (synthesis attempted but no
    archetype produced a candidate). Parsed by
    ``tools.marker_parser.parse_proposal_generation_empty_marker``.
    """
    return marker_line(
        "GSO_PROPOSAL_GENERATION_EMPTY_V1",
        {
            "ag_id": str(ag_id),
            "iteration": int(iteration),
            "target_qids": list(target_qids or ()),
        },
    )


def proposal_failure_decided_marker(
    *,
    ag_id: str,
    iteration: int,
    failure_mode: str,
    next_action: str,
    cluster_signature: str = "",
    prior_failure_count: int = 0,
) -> str:
    """Plan P-F — stdout marker emitted alongside every
    ``DecisionType.PROPOSAL_FAILURE_DECIDED`` record so postmortem
    skills can scan stdout without rehydrating the trace JSON.

    Schema: ``{"ag_id", "iteration", "failure_mode", "next_action",
    "cluster_signature", "prior_failure_count"}``. Parsed by
    ``tools.marker_parser.parse_proposal_failure_decided_marker``.
    """
    return marker_line(
        "GSO_PROPOSAL_FAILURE_DECIDED_V1",
        {
            "ag_id": str(ag_id),
            "iteration": int(iteration),
            "failure_mode": str(failure_mode),
            "next_action": str(next_action),
            "cluster_signature": str(cluster_signature or ""),
            "prior_failure_count": int(prior_failure_count or 0),
        },
    )


def structural_gate_dropped_marker(
    *,
    ag_id: str,
    iteration: int,
    root_causes: Sequence[str] | None = None,
    target_qids: Sequence[str] | None = None,
) -> str:
    """P4 — stdout marker emitted when the lever-5 structural gate
    drops an instruction-only proposal because the dominant cluster
    root cause is SQL-shape but no ``example_sql`` is attached.
    """
    return marker_line(
        "GSO_STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY_V1",
        {
            "ag_id": str(ag_id),
            "iteration": int(iteration),
            "root_causes": list(root_causes or ()),
            "target_qids": list(target_qids or ()),
        },
    )


def no_structural_candidate_marker(
    *,
    ag_id: str,
    iteration: int,
    attempted_archetypes: Sequence[str] | None = None,
) -> str:
    """P4 — stdout marker emitted when synthesis was attempted but no
    archetype produced a viable structural candidate.
    """
    return marker_line(
        "GSO_NO_STRUCTURAL_CANDIDATE_V1",
        {
            "ag_id": str(ag_id),
            "iteration": int(iteration),
            "attempted_archetypes": list(attempted_archetypes or ()),
        },
    )


def replay_fixture_empty_marker(
    *,
    optimization_run_id: str,
    iterations_data_count: int,
    fixture_iterations_count: int,
    iterations_with_zero_eval_rows: tuple[int, ...] = (),
) -> str:
    """B5 (2026-05-13) — diagnostic marker for empty replay fixture.

    Emitted at end-of-run when serialization succeeded (so the resilience
    wrapper in ``_build_fixture`` did not silently drop everything) but
    the resulting fixture is semantically empty: either ``iterations`` is
    empty, or one or more iterations have zero ``eval_rows``. Both shapes
    block replay-fixture intake.

    Single-line ``GSO_REPLAY_FIXTURE_EMPTY_V1 <json>`` format consistent
    with other GSO markers. Postmortem skill greps for this marker to
    flag empty-fixture runs without re-parsing the whole stderr stream.

    Anchor:
    docs/runid_analysis/2314bb2c-95a1-4d60-8226-09e5155aee2a/postmortem.md F8
    """
    import json as _json

    payload = {
        "optimization_run_id": str(optimization_run_id or ""),
        "iterations_data_count": int(iterations_data_count or 0),
        "fixture_iterations_count": int(fixture_iterations_count or 0),
        "iterations_with_zero_eval_rows": list(iterations_with_zero_eval_rows),
    }
    return "GSO_REPLAY_FIXTURE_EMPTY_V1 " + _json.dumps(
        payload, sort_keys=True
    )


def directive_outcome_marker(
    *,
    optimization_run_id: str,
    ledger: "Any",
) -> str:
    """Phase 3 (2026-05-13) — single-shape stdout marker for per-AG
    directive outcomes.

    One marker per AG per iteration. Payload carries the closed-vocabulary
    outcome for every lever in ``ag.lever_directives``. Consumers parse
    the line via the canonical ``GSO_<NAME>_V1 <json>`` regex.

    Anchor:
    docs/runid_analysis/2314bb2c-95a1-4d60-8226-09e5155aee2a/postmortem.md
    """
    import json as _json

    payload = ledger.to_marker_payload()
    payload["optimization_run_id"] = str(optimization_run_id or "")
    return "GSO_DIRECTIVE_OUTCOME_V1 " + _json.dumps(payload, sort_keys=True)


def gso_invariant_violation_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    invariant_name: str,
    offending_qids: Sequence[str] | None = None,
    degradation: str = "",
    payload: Mapping[str, Any] | None = None,
) -> str:
    """Plan N4 — single-shape stdout marker for every invariant
    violation downgraded by the warn-and-degrade policy.

    Postmortem skills pivot on the typed ``invariant_name`` field
    (closed vocabulary: ``quarantine_attribution_drift``,
    ``regression_debt_partition_incomplete``,
    ``soft_cluster_currency_drift``, ``cap_conservation_violated``,
    ``non_canonical_judge_row``). Marker frequency itself is the
    production health signal — a single line per violation lets
    operators ``grep`` for the marker and pivot the histogram.
    """
    return marker_line(
        "GSO_INVARIANT_VIOLATION_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "invariant_name": str(invariant_name),
            "offending_qids": list(offending_qids or ()),
            "degradation": str(degradation),
            "payload": dict(payload or {}),
        },
    )


def assemble_run_manifest_v2_line(
    *,
    optimization_run_id: str,
    databricks_job_id: str = "",
    databricks_parent_run_id: str = "",
    lever_loop_task_run_id: str = "",
    mlflow_experiment_id: str = "",
    space_id: str = "",
    event: str,
    # Phase 0.1 — resolver-ladder kwargs (new, optional). When any are
    # provided, the resolver runs in this precedence: dbutils > env >
    # jobs_run_snapshot > the legacy scalar kwarg > "unknown".
    env_resolved: Mapping[str, Any] | None = None,
    dbutils_resolved: Mapping[str, Any] | None = None,
    jobs_run_snapshot: Mapping[str, Any] | None = None,
) -> str:
    """Convenience wrapper used by the harness — collects build_metadata +
    effective flags and renders the V2 line in one call."""
    from genie_space_optimizer.common.build_metadata import (
        read_domain,
        read_git_sha,
        read_python_version,
        read_wheel_sha,
    )

    def _pick(*tiers: str) -> str:
        for tier_val in tiers:
            if tier_val:
                return tier_val
        return "unknown"

    use_ladder = (
        env_resolved is not None
        or dbutils_resolved is not None
        or jobs_run_snapshot is not None
    )
    if use_ladder:
        _env = env_resolved or {}
        _dbu = dbutils_resolved or {}
        _snap = jobs_run_snapshot or {}
        databricks_job_id = _pick(
            str(_dbu.get("databricks_job_id") or ""),
            str(_env.get("databricks_job_id") or ""),
            str(_snap.get("job_id") or ""),
            databricks_job_id,  # legacy scalar kwarg as last fallback
        )
        databricks_parent_run_id = _pick(
            str(_dbu.get("databricks_parent_run_id") or ""),
            str(_env.get("databricks_parent_run_id") or ""),
            str(_snap.get("parent_run_id") or ""),
            databricks_parent_run_id,
        )
        lever_loop_task_run_id = _pick(
            str(_dbu.get("lever_loop_task_run_id") or ""),
            str(_env.get("lever_loop_task_run_id") or ""),
            str(_snap.get("task_run_id") or ""),
            lever_loop_task_run_id,
        )

    return run_manifest_v2_marker(
        optimization_run_id=optimization_run_id,
        databricks_job_id=databricks_job_id,
        databricks_parent_run_id=databricks_parent_run_id,
        lever_loop_task_run_id=lever_loop_task_run_id,
        mlflow_experiment_id=mlflow_experiment_id,
        space_id=space_id,
        event=event,
        wheel_sha=read_wheel_sha("genie_space_optimizer"),
        git_sha=read_git_sha(),
        effective_flags=collect_effective_flags(),
        python_version=read_python_version(),
        domain=read_domain(),
    )


def contract_health_summary_marker(summary) -> str:
    """RCO-2a — emit ``GSO_CONTRACT_HEALTH_V1`` carrying the typed
    end-of-run health summary.

    ``summary`` must be a ``contract_health.ContractHealthSummary``;
    we don't import the type at module load to keep
    ``run_analysis_contract.py`` free of optimization-package imports
    (it is intentionally Spark/Databricks/MLflow free).

    The merge-gate categories carried in the payload are **wired but
    not enforced** in RCO-2a — the production job still exits with
    success on ``merge_gate_blocked``. RCO-2b flips that posture.
    """
    payload = summary.to_json_dict()
    return marker_line("GSO_CONTRACT_HEALTH_V1", payload)


# ---------------------------------------------------------------------------
# Phase 0.3 — typed terminal markers for the lever-loop iteration body.
# ---------------------------------------------------------------------------


def iteration_no_candidate_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    terminal_reason: str,
    cluster_ids: tuple[str, ...] | list[str],
    ag_id: str,
) -> str:
    """Phase 0.3 — typed terminal marker for an iteration that ended
    without an evaluated candidate.

    Every iteration that does NOT emit ``GSO_FULL_EVAL_V1`` MUST emit
    this marker with a typed ``terminal_reason``. The value is drawn
    from the closed vocabulary in
    ``optimization/run_analysis_contract.py`` (see ``ReasonCode``
    enum from ``rca_decision_trace.py`` for the canonical set;
    future Phase 1 work introduces ``TerminalReason`` as a strict
    subset).
    """
    return marker_line(
        "GSO_ITERATION_NO_CANDIDATE_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "terminal_reason": str(terminal_reason or "unknown"),
            "cluster_ids": [str(c) for c in (cluster_ids or ())],
            "ag_id": str(ag_id or ""),
        },
    )


def iteration_faulted_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    exception_class: str,
    exception_message: str,
    traceback_head: str,
) -> str:
    """Phase 0.3 — typed terminal marker for an iteration that ended
    with an uncaught exception.

    Emitted from a ``finally`` block when neither ``GSO_FULL_EVAL_V1``
    nor ``GSO_ITERATION_NO_CANDIDATE_V1`` was emitted before the
    finally ran. Captures the exception class, repr, and the first
    2048 chars of traceback so the postmortem can answer "what
    failed?" without re-running.
    """
    truncated_tb = str(traceback_head or "")[:2048]
    return marker_line(
        "GSO_ITERATION_FAULTED_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "exception_class": str(exception_class or ""),
            "exception_message": str(exception_message or ""),
            "traceback_head": truncated_tb,
        },
    )


def check_iteration_terminal_exhaustiveness(
    *,
    stdout: str,
) -> dict | None:
    """Phase 0.3 — contract-health check: every iteration_summary
    marker is paired with exactly one terminal marker
    (full_eval | iteration_no_candidate | iteration_faulted).

    Pure: no I/O. Returns ``None`` on a clean run, or a violation
    dict suitable for emission via the contract-health summary.
    """
    iter_summary_count = stdout.count("GSO_ITERATION_SUMMARY_V1 ")
    terminal_count = (
        stdout.count("GSO_FULL_EVAL_V1 ")
        + stdout.count("GSO_ITERATION_NO_CANDIDATE_V1 ")
        + stdout.count("GSO_ITERATION_FAULTED_V1 ")
    )
    if iter_summary_count == terminal_count:
        return None
    return {
        "iteration_summary_count": iter_summary_count,
        "terminal_marker_count": terminal_count,
        "delta": iter_summary_count - terminal_count,
    }


# ---------------------------------------------------------------------------
# Phase 0.4 — candidate ledger stdout marker.
# ---------------------------------------------------------------------------


def candidate_ledger_entry_marker(
    *,
    optimization_run_id: str,
    entry: dict,
) -> str:
    """Phase 0.4 — stdout mirror of one candidate-ledger JSONL line.

    The full entry payload is embedded so a postmortem that only has
    stdout (no Phase H artifacts) can still reconstruct the ledger.
    """
    return marker_line(
        "GSO_CANDIDATE_LEDGER_ENTRY_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "entry": dict(entry or {}),
        },
    )


# ---------------------------------------------------------------------------
# Phase 1.2 — iteration_terminal_policy router decision marker.
# ---------------------------------------------------------------------------


def iteration_terminal_decided_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    terminal_reason: str,
    terminal_signature: Mapping[str, Any],
    next_step: str,
    add_to_forbidden_set: bool,
    forbidden_set_size_after: int,
) -> str:
    """Plan A / Spec Section 9.2.1 — emit
    ``GSO_ITERATION_TERMINAL_DECIDED_V1``.

    Pure: returns the stdout marker line. Caller is responsible for
    ``print(..., flush=True)``. The harness calls
    :func:`~genie_space_optimizer.optimization.iteration_terminal_policy.decide_iteration_terminal_action`
    in the Task 9 finally block and pipes the result through this
    producer; the first deploy is OBSERVE-ONLY (the harness does NOT
    yet mutate ``_forbidden_ag_set`` from ``add_to_forbidden_set``).
    """
    return marker_line(
        "GSO_ITERATION_TERMINAL_DECIDED_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "terminal_reason": str(terminal_reason or ""),
            "terminal_signature": dict(terminal_signature or {}),
            "next_step": str(next_step or ""),
            "add_to_forbidden_set": bool(add_to_forbidden_set),
            "forbidden_set_size_after": int(forbidden_set_size_after),
        },
    )


def check_iteration_terminal_present(
    *,
    stdout: str,
) -> dict | None:
    """Phase 1.2 — verify every ``GSO_ITERATION_NO_CANDIDATE_V1``
    payload carries a ``terminal_reason`` value from the
    :class:`TerminalReason` closed vocabulary.

    Pure: no I/O. Returns ``None`` on a clean run, or a violation
    dict listing the unknown reasons.

    Distinct from the Phase 0.3 ``check_iteration_terminal_exhaustiveness``
    (which counts markers); this one validates the typed payload
    content.
    """
    import json
    from genie_space_optimizer.optimization.terminal_reason import TerminalReason

    valid_values = {r.value for r in TerminalReason}
    unknown: list[str] = []

    for raw in stdout.splitlines():
        if not raw.startswith("GSO_ITERATION_NO_CANDIDATE_V1 "):
            continue
        _, _, json_blob = raw.partition(" ")
        try:
            payload = json.loads(json_blob)
        except json.JSONDecodeError:
            continue
        reason = str(payload.get("terminal_reason") or "")
        if reason and reason not in valid_values:
            unknown.append(reason)

    if not unknown:
        return None
    return {
        "unknown_terminal_reasons": sorted(set(unknown)),
        "valid_values_count": len(valid_values),
    }
