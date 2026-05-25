"""CLI-readable output contract for GSO lever-loop run analysis.

This module is intentionally Spark/Databricks/MLflow free. It only builds
stable single-line JSON markers that the run-analysis skill can parse from
Databricks task stdout.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable, Mapping, Sequence


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
    skipped_reason: str = "",
) -> str:
    """P4 — stdout marker emitted when synthesis was attempted but no
    archetype produced a viable structural candidate.

    Phase 6.5 (2026-05-17) — adds ``skipped_reason``, a short closed-
    vocabulary string indicating WHY synthesis returned no candidate
    (e.g. ``"no_top_n_archetype"``, ``"format_afs_failed"``,
    ``"validate_afs_rejected"``, ``"safety_cap_reached"``). Empty
    string means "no specific cause recorded".

    Phase 1.5 (2026-05-17) — refuses construction when both
    ``skipped_reason`` and ``attempted_archetypes`` are empty. The
    synthesizer always knows something:

    - It ran and produced no candidate (typed skipped_reason)
    - It ran and gates rejected attempted archetypes (non-empty
      attempted_archetypes)
    - It never ran due to RCA-card pre-flight
      (skipped_reason="missing_rca_card")

    Double-empty is the exact failure pattern seen in airline
    59a173d3 and 7now ab65fefe before Phase 0.2 wired the fields
    through. Refuse so CI catches future regressions.
    """
    if not skipped_reason and not (attempted_archetypes or ()):
        raise ValueError(
            f"no_structural_candidate_marker: synthesizer must "
            f"report either a non-empty skipped_reason or a "
            f"non-empty attempted_archetypes tuple. Got both empty "
            f"for ag_id={ag_id!r}, iteration={iteration}. This is "
            f"the Phase 1 refuse-on-empty invariant; if you reach "
            f"this branch, upstream causal context was dropped."
        )
    return marker_line(
        "GSO_NO_STRUCTURAL_CANDIDATE_V1",
        {
            "ag_id": str(ag_id),
            "iteration": int(iteration),
            "attempted_archetypes": list(attempted_archetypes or ()),
            "skipped_reason": str(skipped_reason or ""),
        },
    )


def slate_authoritative_skip_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    reason: str,
    source_cluster_ids: Sequence[str],
) -> str:
    """WU-1 (2026-05-18) — stdout marker emitted when the harness
    refuses to process the current ``ag`` because the slate's
    ``apply_admission_trace`` denied it OR the grounding-gate's
    ``blocked_cluster_ids`` intersects its source clusters.

    ``reason`` is a closed-vocabulary string:

    * ``ag_denied_by_admission_trace``
    * ``cluster_blocked_no_rca``
    * ``ag_retired_pivot``

    Postmortem extractors grep this marker to confirm the harness
    stopped an ungrounded AG BEFORE ``forced_synthesis_dispatch``
    (the defensive backstop). When zero markers fire across a run
    that produced ``missing_rca_card``, the WU-1 wiring is broken.
    """
    return marker_line(
        "GSO_SLATE_AUTHORITATIVE_SKIP_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "reason": str(reason),
            "source_cluster_ids": list(
                str(c) for c in (source_cluster_ids or ()) if c
            ),
        },
    )


def rca_regen_retry_verdict_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    rca_id: str,
    succeeded: bool,
    attempted_sources: Sequence[str],
) -> str:
    """WU-2 (2026-05-18) — stdout marker emitted once per blocked
    cluster after the single-shot RCA regeneration retry.

    Postmortem extractors use this to confirm Plan P-D + Phase 2.2
    + WU-2 actually attempted regeneration for every ungrounded
    cluster, and to see WHICH sources were tried.
    """
    return marker_line(
        "GSO_RCA_REGEN_RETRY_VERDICT_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "cluster_id": str(cluster_id),
            "rca_id": str(rca_id or ""),
            "succeeded": bool(succeeded),
            "attempted_sources": list(
                str(s) for s in (attempted_sources or ()) if s
            ),
        },
    )


def run_aborted_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    terminal_reason: str,
    next_step: str,
    reason: str,
) -> str:
    """Phase 6.2 (2026-05-17) — stdout marker emitted when
    ``TerminalAction.next_step == "abort_run"`` and the harness
    consequently breaks the lever loop.

    ``reason`` is a short cause string for the marker consumer
    (postmortem, dashboards). Today's writer uses one of:

    * ``"terminal_router_decision"`` — the router decided abort_run
      from the routing table (e.g. ``INVARIANT_VIOLATION``).
    * ``"iteration_budget_exhausted"`` — the budget-boundary rule in
      ``decide_iteration_terminal_action`` collapsed a non-abort
      next_step to abort_run on the final iteration index.
    """
    return marker_line(
        "GSO_RUN_ABORTED_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "terminal_reason": str(terminal_reason),
            "next_step": str(next_step),
            "reason": str(reason),
        },
    )


def structural_repair_decision_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    target_qids: Sequence[str] | None = None,
    rca_root_cause: str,
    intended_patch_shape: str,
    emitted_patch_shape: str,
    gate_verdict: str,
    terminal_reason: str = "",
    narrow_replacement_available: bool = False,
    repairability_score: float = 0.0,
    component_scores: Mapping[str, Any] | None = None,
) -> str:
    """Phase 2.3 / spec Section 12.4 — stdout marker emitted on EVERY
    firing of the structural-repair-shape gate (admitted AND rejected).

    Payload mirrors the spec Section 9.2.5 schema so postmortem
    consumers can detect admission/rejection of instruction-only
    survivors when ``rca_card.intended_patch_shape == "structural"``.
    Parsed by ``tools.marker_parser`` (where applicable).
    """
    return marker_line(
        "GSO_STRUCTURAL_REPAIR_DECISION_V1",
        {
            "optimization_run_id": str(optimization_run_id or ""),
            "iteration": int(iteration),
            "ag_id": str(ag_id or ""),
            "cluster_id": str(cluster_id or ""),
            "target_qids": list(target_qids or ()),
            "rca_root_cause": str(rca_root_cause or ""),
            "intended_patch_shape": str(intended_patch_shape or ""),
            "emitted_patch_shape": str(emitted_patch_shape or ""),
            "gate_verdict": str(gate_verdict or ""),
            "terminal_reason": str(terminal_reason or ""),
            "narrow_replacement_available": bool(narrow_replacement_available),
            "repairability_score": float(repairability_score or 0.0),
            "component_scores": dict(component_scores or {}),
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


def llm_contract_failure_marker(
    *,
    schema_name: str,
    failing_fields: Sequence[str],
    raw_payload: Mapping[str, Any] | None = None,
    optimization_run_id: str = "",
    iteration: int = 0,
    cluster_id: str = "",
    ag_id: str = "",
    skill_name: str = "",
    error_repr: str = "",
) -> str:
    """Plan 10 Phase B3 — single-shape stdout marker emitted every time
    an LLM response is rejected by its typed Pydantic
    ``response_model``.

    Paired with ``decision_emitters.llm_contract_failure_record`` (the
    in-process DecisionRecord). The marker exists separately so the
    postmortem stdout grep path catches the failure even when the
    decision-records persistence layer is not yet wired (the
    historical defect: ``synthesize_example_sqls`` swallowed
    ``ValidationError`` with a broad ``except Exception`` and the
    postmortem reader saw a generic ``"empty example_question or
    example_sql"`` gate rejection instead of the actual contract bug).

    ``raw_payload`` is best-effort: callers pass the parsed dict they
    fed to ``model_validate``. We coerce non-dict payloads (a free-form
    string from a non-JSON LLM response) by wrapping under a ``raw``
    key so the stdout marker is always JSON-serialisable.

    ``failing_fields`` is the closed-vocabulary list of dotted field
    paths the validator rejected (e.g. ``["example_sql"]``).
    """
    if isinstance(raw_payload, Mapping):
        payload_out: dict[str, Any] = {
            str(k): raw_payload[k] for k in raw_payload
        }
    elif raw_payload is None:
        payload_out = {}
    else:
        payload_out = {"raw": str(raw_payload)[:2048]}
    return marker_line(
        "GSO_LLM_CONTRACT_FAILURE_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "cluster_id": str(cluster_id),
            "ag_id": str(ag_id),
            "skill_name": str(skill_name),
            "schema_name": str(schema_name),
            "failing_fields": [str(f) for f in (failing_fields or ()) if f],
            "error_repr": str(error_repr)[:2048],
            "raw_payload": payload_out,
        },
    )


STAGE1_BLAME_SET_SOURCES = frozenset({"llm", "seed_backfill", "empty"})


# Trial 13i — closed vocabulary for the ``schema_columns`` provenance
# label emitted on ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1``. Mirrors
# ``schema_columns.SCHEMA_COLUMNS_SOURCE_LABELS`` (kept in sync via
# the unit test ``test_plan11_stage1_input_quality_marker.py``).
STAGE1_INPUT_QUALITY_SOURCES = frozenset({
    "metadata_snapshot",
    "typed_evidence_union",
    "identifier_allowlist",
    "empty",
})


# Trial 13k — closed vocabulary for the derived
# ``seed_normalization_verdict`` field emitted on
# ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1``. Lets postmortems and CI
# canaries grep one field instead of computing
# ``seeds_pre_normalize`` vs ``seeds_post_normalize`` arithmetic.
STAGE1_SEED_NORMALIZATION_VERDICTS = frozenset({
    "no_seeds",      # seeds_pre_normalize == 0
    "all_dropped",   # pre > 0 and post == 0  (Trial 13k canary)
    "partial_drop",  # pre > post > 0
    "ok",            # post == pre and pre > 0
})


def _seed_normalization_verdict(
    seeds_pre_normalize: int, seeds_post_normalize: int
) -> str:
    """Map ``(pre, post)`` seed counts onto the closed verdict vocabulary."""
    pre = int(seeds_pre_normalize or 0)
    post = int(seeds_post_normalize or 0)
    if pre <= 0:
        return "no_seeds"
    if post <= 0:
        return "all_dropped"
    if post < pre:
        return "partial_drop"
    return "ok"


def plan11_stage1_input_quality_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    schema_columns_source: str,
    schema_columns_size: int,
    seeds_pre_normalize: int = 0,
    seeds_post_normalize: int = 0,
    seeds_normalized: int = 0,
    seeds_dropped: int = 0,
    contract_violation: str = "",
    blame_kind_distribution: dict[str, int] | None = None,
) -> str:
    """Plan 11 — per-QID Stage 1 input-quality marker (Trial 13i).

    Emitted exactly once per QID at Stage 1 pre-flight, BEFORE the LLM
    is invoked, so postmortems can see the input shape that drove the
    downstream outcome regardless of whether Stage 1 succeeded,
    abstained, or short-circuited.

    Fields:

      * ``schema_columns_source`` — one of
        :data:`STAGE1_INPUT_QUALITY_SOURCES`. Reflects where the
        run-level ``ctx.schema_columns`` list originated:

        - ``"metadata_snapshot"``: caller populated
          ``metadata_snapshot["schema_columns"]`` explicitly. Trial 13l
          made this the production-default — the harness calls
          :func:`genie_space_optimizer.optimization.schema_columns.inject_schema_columns_into_metadata_snapshot`
          at the top of every lever-loop iteration, so the per-QID
          Stage 1 input quality marker should record this source on
          every iteration of every space whose Genie API fetch
          succeeds. A run where ``schema_columns_source`` is anything
          else after Trial 13l implies the injector emitted
          ``api_error`` / ``empty_extract`` / ``no_space_id`` — cross-
          reference the ``GSO_PLAN11_SCHEMA_COLUMNS_INJECTION_V1``
          marker for the same iteration to confirm.
        - ``"typed_evidence_union"``: union of
          ``rca_evidence_typed[*].blame_set``. Healthy default for the
          SM lane when Plan 3 typed evidence is present.
        - ``"identifier_allowlist"``: re-projected from
          ``_build_identifier_allowlist`` because typed evidence was
          empty. Acceptable for the batch lane but signals the SM lane
          fell back to the secondary source.
        - ``"empty"``: every source returned nothing. Deploy-block
          canary — Stage 1 will abstain with
          ``missing_schema_columns`` and no patches can be applied.
      * ``schema_columns_size`` — length of ``ctx.schema_columns``;
        cross-checks the source label (``"empty"`` ⇔ size 0).
      * ``seeds_pre_normalize`` / ``seeds_post_normalize`` —
        ``blame_set_seed`` size before and after the FQN normalizer ran.
        A drop of > 50% on production is a warn canary.
      * ``seeds_normalized`` — bare-identifier tokens that were
        successfully swapped for 4-part FQNs by suffix-matching against
        ``schema_columns``. Sustained ``seeds_normalized > 0`` on the
        capture/SM lane signals the ASI judges are emitting column-name
        tokens (current 2026-05 wild shape) rather than schema FQNs.
      * ``seeds_dropped`` — tokens rejected by the normalizer
        (compound text or ambiguous suffix). Sustained
        ``seeds_dropped > 0`` rate > 5% is a drift signal —
        ``STAGE1_SEED_DRIFT`` postmortem verdict.
      * ``contract_violation`` — non-empty when the pre-flight
        contract rejected the input (e.g. ``"missing_schema_columns"``).
        Caller may also short-circuit Stage 1 with the same field tag.
      * ``blame_kind_distribution`` (Trial 14) — per-kind histogram of
        the typed ``blame_set_structured`` payload pre-normalization,
        e.g. ``{"column": 2, "filter": 1}``. Keys are restricted to
        the closed :data:`BLAME_KINDS` vocabulary from
        ``blame_entry.py``. Empty dict when no judge emitted a
        structured payload (legacy free-text path). Postmortems use
        this to distinguish "judges identified schema blame but the
        normalizer dropped it" (kind=column with all_dropped verdict)
        from "judges identified only behaviour blame" (kind=filter /
        instruction only — ``seeds_all_filter_kind`` contract tag).
    """
    # Trial 14 — normalize the kind distribution onto the closed
    # vocabulary so postmortems never see drifted keys leak into the
    # marker payload. Unknown kinds are dropped silently — the
    # coercer in ``blame_entry.coerce_blame_entries`` already
    # collapsed them onto ``instruction`` before we get here.
    from genie_space_optimizer.optimization.blame_entry import BLAME_KINDS

    raw_distribution = blame_kind_distribution or {}
    distribution: dict[str, int] = {
        str(k): int(v)
        for k, v in raw_distribution.items()
        if str(k) in BLAME_KINDS and int(v or 0) > 0
    }

    return marker_line(
        "GSO_PLAN11_STAGE1_INPUT_QUALITY_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "schema_columns_source": str(schema_columns_source),
            "schema_columns_size": int(schema_columns_size),
            "seeds_pre_normalize": int(seeds_pre_normalize),
            "seeds_post_normalize": int(seeds_post_normalize),
            "seeds_normalized": int(seeds_normalized),
            "seeds_dropped": int(seeds_dropped),
            # Trial 13k — derived single-field verdict for postmortem
            # triage. Closed vocabulary: see
            # ``STAGE1_SEED_NORMALIZATION_VERDICTS``.
            "seed_normalization_verdict": _seed_normalization_verdict(
                seeds_pre_normalize, seeds_post_normalize
            ),
            # Trial 14 — typed blame kind histogram. Empty dict when
            # the upstream judges only emitted legacy free-text.
            "blame_kind_distribution": distribution,
            "contract_violation": str(contract_violation),
        },
    )


def plan11_schema_columns_injection_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    space_id: str,
    injected: bool,
    source: str,
    column_count: int,
    latency_ms: int,
) -> str:
    """Plan 11 — per-iteration ``schema_columns`` injection marker (Trial 13l).

    Emitted exactly once per lever-loop iteration, immediately after
    :func:`genie_space_optimizer.optimization.schema_columns.inject_schema_columns_into_metadata_snapshot`
    returns. Postmortems use this to attribute the run's
    ``schema_columns`` provenance to a concrete producer (or its
    absence) and to detect post-apply schema drift between iterations.

    Fields:

      * ``injected`` — True iff this call mutated
        ``metadata_snapshot["schema_columns"]``. Only ``source ==
        "genie_api"`` writes; every other source preserves any prior
        iteration's value.
      * ``source`` — one of
        :data:`genie_space_optimizer.optimization.schema_columns.SCHEMA_COLUMNS_INJECTION_SOURCES`:

        - ``"genie_api"`` (healthy default) — fetch succeeded and at
          least one 4-part FQN was derived from the live Genie Space.
        - ``"no_space_id"`` — caller bug; the lever-loop bootstrap
          should always pass a non-empty space id.
        - ``"api_error"`` — Genie API call raised or returned a
          non-Mapping. At iter-0 this is severe (Stage 1 will fall
          through to ``typed_evidence_union`` → ``identifier_allowlist``).
          At iter-N>0 a prior iteration's value is retained so Stage 1
          is unaffected; monitor cadence.
        - ``"empty_extract"`` — Genie Space had a parseable config but
          no tables with 4-part-resolvable column entries. Indicates a
          misconfigured space.
      * ``column_count`` — number of FQNs written when ``injected``
        is True; 0 otherwise. Drift between consecutive iterations
        cross-checked against the apply marker is the Trial 13l
        signal that a Stage 3 patch mutated ``data_sources.tables``.
      * ``latency_ms`` — wall-clock latency of the injection call
        (Genie GET round-trip + parse + mutate). Per-iteration budget
        guard: a millisecond-scale call dwarfed by F1 eval / LLM cost.
    """
    return marker_line(
        "GSO_PLAN11_SCHEMA_COLUMNS_INJECTION_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "space_id": str(space_id),
            "injected": bool(injected),
            "source": str(source),
            "column_count": int(column_count),
            "latency_ms": int(latency_ms),
        },
    )


def plan11_stage1_diagnosis_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    outcome: str,
    rca_kind_label: str = "",
    confidence: str = "",
    blame_set_size: int = 0,
    evidence_summary_chars: int = 0,
    abstain_reason: str = "",
    abstain_explanation: str = "",
    duration_ms: int = 0,
    tokens_input: int = 0,
    tokens_output: int = 0,
    error_kind: str = "",
    exception_class: str = "",
    error_message: str = "",
    endpoint: str = "",
    blame_set_source: str = "",
    blame_set_llm_emitted: int = 0,
    blame_set_post_schema_dropped: int = 0,
) -> str:
    """Plan 11 — per-QID Stage 1 diagnosis outcome marker.

    2026-05-23 SM Cutover Phase 1.C — when ``outcome == "llm_error"`` the
    marker MUST carry a structured ``error_kind`` so postmortems can
    distinguish ``client_construction`` from ``empty_prompt`` from
    ``endpoint_decline`` etc. Zero-token ``llm_error`` records with no
    ``error_kind`` are a fail-loud signal that the call site did not
    classify the exception — the marker emitter does not silently
    forgive missing classification.

    2026-05-23 PR-A (Stage 1 BadRequest diagnostic instrumentation) —
    on ``llm_error`` the marker now also carries:
      * ``error_message`` — first 500 chars of the underlying exception
        text (already populated by ``LlmReasoningCall.invoke`` as
        ``"ClassName: <message>"``). This is what makes a 400 BadRequest
        actionable in postmortems instead of an opaque ``error_kind``.
      * ``endpoint`` — the model-serving endpoint name the call was
        routed to. Postmortems join on this when triaging endpoint
        decommissions / region mismatches.

    Trial 13h — Stage 1 blame_set provenance fields. The post-13g
    workbench replay surfaced QIDs where the Stage 1 LLM emitted a
    confident diagnosis with ``blame_set: []`` (or with entries that all
    failed the ``schema_columns`` filter), which the non-actionable gate
    correctly but unnecessarily terminated. ``diagnose_failing_qids``
    now backfills from ``blame_set_seed`` whenever the post-schema-filter
    result is empty, and these three fields surface where the final
    ``blame_set`` came from:

      * ``blame_set_source`` — one of ``STAGE1_BLAME_SET_SOURCES``:
        - ``"llm"``: LLM-emitted entries survived schema filter (healthy).
        - ``"seed_backfill"``: LLM-emitted entries were empty or all
          schema-dropped, and we filled from ``blame_set_seed``. A
          sustained rate of this signals Stage 1 LLM drift.
        - ``"empty"``: both LLM and seed yielded zero schema-valid
          entries. The diagnosis will be classified
          ``non_actionable_diagnosis:zero_blame_set`` downstream.
      * ``blame_set_llm_emitted`` — raw count from the LLM before the
        schema filter. Sustained ``blame_set_llm_emitted == 0`` means
        the model is omitting the field; sustained
        ``blame_set_post_schema_dropped > 0`` means it is hallucinating
        schema symbols.
      * ``blame_set_post_schema_dropped`` — count of LLM-emitted entries
        dropped by the ``schema_columns`` filter.

    These three fields default to ``""`` / ``0`` for backward
    compatibility — pre-13h call sites (e.g. ``outcome != "diagnosed"``
    paths and the legacy batch lane) leave them unset and the marker
    payload simply records the defaults.
    """
    if outcome == "llm_error" and not error_kind:
        # Fail loud: do not emit ambiguous zero-token llm_error.
        raise ValueError(
            "plan11_stage1_diagnosis_marker emitted outcome='llm_error' "
            "without error_kind. The caller must classify the underlying "
            "exception (client_construction / empty_prompt / "
            "endpoint_decline / timeout / parse / unknown).",
        )
    # Trial 12 Track 4: typed semantic-success boolean. A mechanically
    # diagnosed call ("outcome=diagnosed") is NOT actionable when:
    #   * evidence_summary_chars == 0 (empty narrative — nothing for
    #     downstream stages to read), OR
    #   * blame_set_size == 0 (no objects to focus on), OR
    #   * rca_kind_label is the "insufficient evidence" sentinel
    #     (Stage 1 explicitly admitted it could not classify).
    # Non-"diagnosed" outcomes are always non-actionable; postmortems
    # use this field to distinguish mechanical success from semantic
    # success without parsing rca_kind_label heuristics.
    insufficient_evidence_sentinel = (
        "insufficient evidence to determine root cause"
    )
    diagnosis_actionable = bool(
        outcome == "diagnosed"
        and int(evidence_summary_chars) > 0
        and int(blame_set_size) > 0
        and str(rca_kind_label).strip().lower()
        != insufficient_evidence_sentinel
    )
    return marker_line(
        "GSO_PLAN11_STAGE1_DIAGNOSIS_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "outcome": str(outcome),
            "rca_kind_label": str(rca_kind_label),
            "confidence": str(confidence),
            "blame_set_size": int(blame_set_size),
            "evidence_summary_chars": int(evidence_summary_chars),
            "abstain_reason": str(abstain_reason),
            "abstain_explanation": str(abstain_explanation),
            "duration_ms": int(duration_ms),
            "tokens_input": int(tokens_input),
            "tokens_output": int(tokens_output),
            "error_kind": str(error_kind),
            "exception_class": str(exception_class),
            "error_message": str(error_message)[:500],
            "endpoint": str(endpoint),
            "diagnosis_actionable": diagnosis_actionable,
            "blame_set_source": str(blame_set_source),
            "blame_set_llm_emitted": int(blame_set_llm_emitted),
            "blame_set_post_schema_dropped": int(blame_set_post_schema_dropped),
        },
    )


_NON_ACTIONABLE_REASONS = frozenset({
    "zero_blame_set",
    "zero_evidence",
    "insufficient_evidence_sentinel",
})


_INSUFFICIENT_EVIDENCE_SENTINEL = (
    "insufficient evidence to determine root cause"
)


def classify_non_actionable_reason(
    *,
    rca_kind_label: str,
    evidence_summary: str,
    blame_set: Iterable[str] | None,
) -> str:
    """Return the typed non-actionable reason, or ``""`` when the
    diagnosis is actionable.

    Trial 13 Track 3 hard gate — the dc89d1a9 shadow batch path
    advanced 21/24 ``diagnosed`` markers carrying
    ``diagnosis_actionable=false`` into Stage 2, producing
    ``empty_synthesis`` at Stage 3 and zero applied patches. Trial 12
    pinned the marker; Trial 13 ships the gate. The classifier mirrors
    the ``diagnosis_actionable`` boolean inside
    :func:`plan11_stage1_diagnosis_marker` but returns the per-axis
    typed reason so the rejection marker can name *why* the diagnosis
    failed the gate.

    Closed-vocabulary return values:

    * ``"insufficient_evidence_sentinel"`` — Stage 1 admitted it could
      not classify the failure (``rca_kind_label`` is the sentinel
      string; the LLM signed off explicitly).
    * ``"zero_blame_set"`` — no objects to focus downstream stages on.
    * ``"zero_evidence"`` — empty narrative; downstream cannot reason.
    * ``""`` — actionable; gate passes.

    The sentinel check fires first because it is the strongest signal:
    when the LLM admits it cannot classify, downstream blame_set /
    evidence checks are redundant.
    """
    label = str(rca_kind_label or "").strip().lower()
    if label == _INSUFFICIENT_EVIDENCE_SENTINEL:
        return "insufficient_evidence_sentinel"
    blame_seq = tuple(b for b in (blame_set or ()) if str(b).strip())
    if not blame_seq:
        return "zero_blame_set"
    if not str(evidence_summary or "").strip():
        return "zero_evidence"
    return ""


def plan11_stage1_non_actionable_reject_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    reason: str,
    rca_kind_label: str,
    blame_set_size: int,
    evidence_summary_chars: int,
) -> str:
    """Plan 11 — Stage 1 hard-gate rejection on non-actionable diagnosis.

    Trial 13 Track 3 — emitted by the SM Stage 1 transformer (and the
    Plan 11 batch lane pre-flight) when a mechanically-successful
    Stage 1 diagnosis fails the actionability gate. The marker is the
    typed observability signal postmortems join on to confirm "QID
    terminated at Stage 1 because the diagnosis itself was empty",
    distinguishing it from ``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1``
    (input hydration gap) and ``GSO_PLAN11_STAGE1_DIAGNOSIS_V1
    outcome="abstained"`` (LLM-side decline).

    Closed vocabulary for ``reason`` — see
    :func:`classify_non_actionable_reason`. Unknown values raise.
    """
    if reason not in _NON_ACTIONABLE_REASONS:
        raise ValueError(
            f"unknown non-actionable reason: {reason!r} (allowed: "
            f"{sorted(_NON_ACTIONABLE_REASONS)})"
        )
    return marker_line(
        "GSO_PLAN11_STAGE1_NON_ACTIONABLE_REJECT_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "reason": str(reason),
            "rca_kind_label": str(rca_kind_label),
            "blame_set_size": int(blame_set_size),
            "evidence_summary_chars": int(evidence_summary_chars),
        },
    )


def plan11_post_parse_field_truncate_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    skill_id: str,
    field_path: str,
    original_length: int,
    truncated_length: int,
) -> str:
    """Plan 11 — Stage 1/2/3 post-parse field truncation marker.

    Trial 13 Track 4 — the Plan 11 output schemas now truncate
    oversize string fields gracefully (trailing ``"..."``) instead of
    raising ``string_too_long``. This marker fires when truncation
    actually happens so postmortems can surface the abnormal length
    without re-running the LLM call.
    """
    return marker_line(
        "GSO_PLAN11_POST_PARSE_FIELD_TRUNCATE_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "skill_id": str(skill_id),
            "field_path": str(field_path),
            "original_length": int(original_length),
            "truncated_length": int(truncated_length),
        },
    )


def plan11_stage1_input_card_empty_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    violations: list[str],
    field_sources: dict[str, str],
) -> str:
    """Plan 11 — Stage 1 input evidence card pre-flight rejection.

    Emitted when :class:`Stage1InputEvidenceContract` rejects the
    payload :func:`build_stage1_evidence_card` produced for ``qid``,
    BEFORE the Stage 1 LLM is invoked. The marker carries the typed
    list of violation tags and a per-field provenance dict so the
    postmortem skill can attribute the empty card back to a specific
    upstream hydration gap.

    Trial 11 root cause: every Stage 1 call burned tokens to return
    the same ``missing_schema_context`` decline because the input
    payload was empty. With this marker in place, future trials will
    surface the empty card as a typed, field-tagged event instead of
    a generic LLM-side decline.
    """
    return marker_line(
        "GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "violations": [str(v) for v in violations or ()],
            "field_sources": {
                str(k): str(v) for k, v in (field_sources or {}).items()
            },
        },
    )


def plan11_stage1_request_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    skill_id: str,
    call_id: str,
    system_msg_chars: int,
    user_prompt_chars: int,
    max_tokens: int,
    response_format_keywords: list[str],
    endpoint: str,
    constraint_violations: list[dict[str, str]] | None = None,
) -> str:
    """Plan 11 — Stage 1 LLM request fingerprint marker.

    Emitted alongside every ``llm_error`` Stage 1 outcome so postmortems
    can see *what request shape* triggered the endpoint rejection
    without re-running the lever loop. Carries only sizes and structural
    fingerprints — never the prompt body — so it stays in stdout-safe
    bounds.

    Fields:
      * ``skill_id`` — the reasoning skill the call routed to.
      * ``call_id`` — the per-invocation identifier (joins to MLflow
        traces and the on-disk ``llm_errors/`` dump).
      * ``system_msg_chars`` / ``user_prompt_chars`` — raw char counts
        of the rendered prompts as they were sent.
      * ``max_tokens`` — the requested completion budget.
      * ``response_format_keywords`` — top-level JSON-schema keywords
        in the response_format the request bound (``["type", "schema",
        "name", "strict"]`` etc.). When the endpoint rejects a request
        because of an unsupported keyword, this is what tells us which
        keyword was in play.
      * ``endpoint`` — the model-serving endpoint name.
      * ``constraint_violations`` (PR-2C, 2026-05-23) — when
        ``RequestEnvelopeInvalidError`` is raised by the local
        pre-flight (``DatabricksEndpointRequestContract.validate``),
        this carries the structured list of failing rules so the
        postmortem doesn't need to parse the error body. Empty list
        on every other ``llm_error`` outcome.

    PR-A (2026-05-23) — pure diagnostic; no behavior change.
    PR-2C (2026-05-23) — adds ``constraint_violations`` (still pure
    diagnostic; populated only when pre-flight refuses to dispatch).
    """
    return marker_line(
        "GSO_PLAN11_STAGE1_REQUEST_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "skill_id": str(skill_id),
            "call_id": str(call_id),
            "system_msg_chars": int(system_msg_chars),
            "user_prompt_chars": int(user_prompt_chars),
            "max_tokens": int(max_tokens),
            "response_format_keywords": [
                str(k) for k in (response_format_keywords or [])
            ],
            "endpoint": str(endpoint),
            "constraint_violations": [
                {"field": str(v.get("field", "")),
                 "constraint": str(v.get("constraint", ""))}
                for v in (constraint_violations or [])
                if isinstance(v, dict)
            ],
        },
    )


def plan11_stage2_clustering_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    namespace: str,
    outcome: str,
    input_qids_count: int = 0,
    clusters_count: int = 0,
    cluster_ids: list[str] | None = None,
    abstain_reason: str = "",
    abstain_explanation: str = "",
    duration_ms: int = 0,
    tokens_input: int = 0,
    tokens_output: int = 0,
    primary_blame_set_backfilled: int = 0,
) -> str:
    """Plan 11 — per-iteration Stage 2 clustering outcome marker.

    ``primary_blame_set_backfilled`` (Trial 13g) counts the clusters
    whose ``primary_blame_set`` the LLM left empty and that the Stage 2
    handler backfilled from the union of member QIDs' diagnosis
    blame_sets. A non-zero value is observability, not a failure — it
    surfaces upstream prompt drift before it cascades into empty Stage
    3 proposals.
    """
    return marker_line(
        "GSO_PLAN11_STAGE2_CLUSTERING_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "namespace": str(namespace),
            "outcome": str(outcome),
            "input_qids_count": int(input_qids_count),
            "clusters_count": int(clusters_count),
            "cluster_ids": list(cluster_ids or []),
            "abstain_reason": str(abstain_reason),
            "abstain_explanation": str(abstain_explanation),
            "duration_ms": int(duration_ms),
            "tokens_input": int(tokens_input),
            "tokens_output": int(tokens_output),
            "primary_blame_set_backfilled": int(primary_blame_set_backfilled),
        },
    )


SYNTHESIS_EMPTY_REASONS = frozenset({
    "no_applicable_archetype",
    "all_candidates_unsafe",
    "prompt_constraint_collision",
    "parse_returned_zero",
})


PROPOSALS_BLAME_SET_SOURCES = frozenset({
    "llm",
    "cluster",
    "member_union",
    "empty",
})


_TRIAL17_BEHAVIORAL_CHANGE_MAX_CHARS = 200


def plan11_stage3_synthesis_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    outcome: str,
    proposals_count: int = 0,
    proposal_ids: list[str] | None = None,
    patch_types: list[str] | None = None,
    target_qids_union: list[str] | None = None,
    abstain_reason: str = "",
    abstain_explanation: str = "",
    duration_ms: int = 0,
    tokens_input: int = 0,
    tokens_output: int = 0,
    synthesis_empty_reason: str = "",
    synthesis_rejected_patch_types: Mapping[str, int] | None = None,
    proposals_blame_set_source: Mapping[str, int] | None = None,
    # Trial 17.1 — surface the LLM's lever selection so operators can
    # see *which lever was picked and why* from structured logs alone.
    # These four arrays are index-parallel to ``proposal_ids`` /
    # ``patch_types``: position ``i`` describes the i-th surviving
    # proposal. They are additive (default empty lists) so legacy
    # callers and the ``outcome == "empty_synthesis"`` branch are
    # unaffected.
    selected_levers: list[str] | None = None,
    expected_behavioral_changes: list[str] | None = None,
    fallback_levers: list[str] | None = None,
    bundle_ids: list[str] | None = None,
) -> str:
    """Plan 11 — per-cluster Stage 3 synthesis outcome marker.

    Trial 13 Track 5 — when ``outcome="empty_synthesis"`` the marker
    MUST carry a typed ``synthesis_empty_reason``. The dc89d1a9 trial
    emitted 6 ``empty_synthesis`` markers with no diagnostic signal;
    postmortems could not tell "no archetype matched" from "all
    candidates unsafe" from "prompt constraint collision". Closed
    vocabulary — see :data:`SYNTHESIS_EMPTY_REASONS`. ``target_qids_union``
    is also populated from the input cluster even when proposals is
    empty so the marker still attributes the bail-out to a QID set.

    Trial 13e — when the reason is ``"all_candidates_unsafe"`` the
    marker MUST also carry ``synthesis_rejected_patch_types``: a
    ``{raw_patch_type: count}`` map of every ``patch_type`` string
    the LLM emitted that failed the :class:`PatchType` enum after
    case-folding. This is the permanent canary for the case-mismatch
    defect class Trial 13e closed: if the map is non-empty on a clean
    iteration the postmortem skill must surface the top raw strings
    as a deploy-blocking alarm. The field is additive (default empty
    map) so the happy path (``outcome="synthesized"``) and the other
    three empty reasons are unaffected.

    Trial 13g — ``proposals_blame_set_source`` (also additive)
    attributes each surviving proposal's ``blame_set`` to one of the
    closed-vocabulary sources in :data:`PROPOSALS_BLAME_SET_SOURCES`:

      * ``llm`` — the Stage 3 LLM emitted a non-empty blame_set.
      * ``cluster`` — empty LLM blame_set; backfilled from
        ``cluster.primary_blame_set``.
      * ``member_union`` — both LLM and cluster blame sets were
        empty; backfilled from the union of
        ``member_qid_evidence[*].blame_set``.
      * ``empty`` — all three sources were empty (proposal will be
        rejected by the Plan 12 survival contract; this is a canary
        for upstream drift).

    The map sums to ``proposals_count`` on the happy path; a non-zero
    ``empty`` entry on a clean iteration is a deploy-blocking signal
    that Stage 1 typed evidence stopped flowing to Stage 3.
    """
    rejected_map = (
        {str(k): int(v) for k, v in (synthesis_rejected_patch_types or {}).items()}
    )
    source_map: dict[str, int] = {}
    for k, v in (proposals_blame_set_source or {}).items():
        key = str(k)
        if key not in PROPOSALS_BLAME_SET_SOURCES:
            raise ValueError(
                f"unknown proposals_blame_set_source key: {key!r} "
                f"(allowed: {sorted(PROPOSALS_BLAME_SET_SOURCES)})"
            )
        source_map[key] = int(v)
    if outcome == "empty_synthesis":
        if not synthesis_empty_reason:
            raise ValueError(
                "plan11_stage3_synthesis_marker emitted "
                "outcome='empty_synthesis' without synthesis_empty_reason. "
                f"Allowed: {sorted(SYNTHESIS_EMPTY_REASONS)}."
            )
        if synthesis_empty_reason not in SYNTHESIS_EMPTY_REASONS:
            raise ValueError(
                f"unknown synthesis_empty_reason: "
                f"{synthesis_empty_reason!r} (allowed: "
                f"{sorted(SYNTHESIS_EMPTY_REASONS)})"
            )
        if synthesis_empty_reason == "all_candidates_unsafe" and not rejected_map:
            raise ValueError(
                "plan11_stage3_synthesis_marker emitted "
                "synthesis_empty_reason='all_candidates_unsafe' without a "
                "non-empty synthesis_rejected_patch_types map. Trial 13e "
                "requires the rejected raw patch_type strings be captured "
                "so future drift is visible at marker time."
            )
    selected_levers_list = [str(x) for x in (selected_levers or [])]
    fallback_levers_list = [str(x) for x in (fallback_levers or [])]
    bundle_ids_list = [str(x) for x in (bundle_ids or [])]
    expected_behavioral_changes_list = [
        # Cap each entry so marker lines stay parseable; full text lives
        # on the RepairProposal dataclass for debuggers.
        (str(x)[:_TRIAL17_BEHAVIORAL_CHANGE_MAX_CHARS])
        for x in (expected_behavioral_changes or [])
    ]
    # When the four lever arrays are populated, they must be parallel to
    # ``proposal_ids``. The check is gated on non-emptiness so the
    # ``empty_synthesis`` branch (no proposals, no lever arrays) and
    # legacy callers that pass nothing for the new fields stay valid.
    proposal_id_list = list(proposal_ids or [])
    for label, arr in (
        ("selected_levers", selected_levers_list),
        ("expected_behavioral_changes", expected_behavioral_changes_list),
        ("fallback_levers", fallback_levers_list),
        ("bundle_ids", bundle_ids_list),
    ):
        if arr and len(arr) != len(proposal_id_list):
            raise ValueError(
                f"plan11_stage3_synthesis_marker: {label} (len={len(arr)}) "
                f"must be index-parallel to proposal_ids "
                f"(len={len(proposal_id_list)}) when supplied"
            )
    return marker_line(
        "GSO_PLAN11_STAGE3_SYNTHESIS_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "cluster_id": str(cluster_id),
            "outcome": str(outcome),
            "proposals_count": int(proposals_count),
            "proposal_ids": proposal_id_list,
            "patch_types": list(patch_types or []),
            "target_qids_union": list(target_qids_union or []),
            "abstain_reason": str(abstain_reason),
            "abstain_explanation": str(abstain_explanation),
            "duration_ms": int(duration_ms),
            "tokens_input": int(tokens_input),
            "tokens_output": int(tokens_output),
            "synthesis_empty_reason": str(synthesis_empty_reason),
            "synthesis_rejected_patch_types": rejected_map,
            "proposals_blame_set_source": source_map,
            "selected_levers": selected_levers_list,
            "expected_behavioral_changes": expected_behavioral_changes_list,
            "fallback_levers": fallback_levers_list,
            "bundle_ids": bundle_ids_list,
        },
    )


def plan11_repair_loop_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    patch_id: str,
    attempt: int,
    max_attempts: int,
    outcome: str,
    error_kinds: list[str] | None = None,
    error_count: int = 0,
    duration_ms: int = 0,
    tokens_input: int = 0,
    tokens_output: int = 0,
) -> str:
    """Plan 11 — per-attempt repair loop outcome marker."""
    return marker_line(
        "GSO_PLAN11_REPAIR_LOOP_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "cluster_id": str(cluster_id),
            "patch_id": str(patch_id),
            "attempt": int(attempt),
            "max_attempts": int(max_attempts),
            "outcome": str(outcome),
            "error_kinds": list(error_kinds or []),
            "error_count": int(error_count),
            "duration_ms": int(duration_ms),
            "tokens_input": int(tokens_input),
            "tokens_output": int(tokens_output),
        },
    )


def plan11_narrow_replacement_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    patch_id: str,
    attempt: int,
    max_attempts: int,
    outcome: str,
    collateral_qids_count: int = 0,
    target_qids: list[str] | None = None,
    duration_ms: int = 0,
    tokens_input: int = 0,
    tokens_output: int = 0,
) -> str:
    """Plan 11 — per-attempt narrow-replacement outcome marker."""
    return marker_line(
        "GSO_PLAN11_NARROW_REPLACEMENT_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "cluster_id": str(cluster_id),
            "patch_id": str(patch_id),
            "attempt": int(attempt),
            "max_attempts": int(max_attempts),
            "outcome": str(outcome),
            "collateral_qids_count": int(collateral_qids_count),
            "target_qids": list(target_qids or []),
            "duration_ms": int(duration_ms),
            "tokens_input": int(tokens_input),
            "tokens_output": int(tokens_output),
        },
    )


VALID_PLAN11_SKIP_REASONS: frozenset[str] = frozenset({
    "flag_disabled",
    "no_failing_qids",
    "build_failing_qids_empty",
    "stage1_llm_declined",
    "stage2_llm_declined",
    "stage1_returned_no_diagnoses",
    "stage2_returned_no_clusters",
})


def plan11_dispatch_decision_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    namespace: str,
    outcome: str,
    skip_reason: str = "",
    failing_qids_count: int = 0,
    rca_evidence_typed_present: bool = False,
) -> str:
    """Plan 12 — explicit dispatch-decision marker.

    Emitted at exactly one point: the entry to the Plan 11 LLM lane in
    optimizer.py (after PR 1 splits the silent ``and rca_evidence_typed``
    short-circuit). Either ``outcome="entered"`` (we ran Stage 1) or
    ``outcome="skipped"`` with a typed ``skip_reason``. Replaces the
    silent fallthrough that both 2026-05-20 postmortems observed.
    """
    if outcome not in ("entered", "skipped"):
        raise ValueError(
            f"outcome must be 'entered' or 'skipped', got {outcome!r}"
        )
    if outcome == "skipped" and skip_reason not in VALID_PLAN11_SKIP_REASONS:
        raise ValueError(
            f"unknown skip_reason {skip_reason!r}; must be one of "
            f"{sorted(VALID_PLAN11_SKIP_REASONS)}"
        )
    return marker_line(
        "GSO_PLAN11_DISPATCH_DECISION_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "namespace": str(namespace),
            "outcome": str(outcome),
            "skip_reason": str(skip_reason),
            "failing_qids_count": int(failing_qids_count),
            "rca_evidence_typed_present": bool(rca_evidence_typed_present),
        },
    )


_VALID_PATCH_OUTCOME_KINDS: frozenset[str] = frozenset({
    "applied",
    "validator_rejected",
    "blast_radius_rejected",
    "contract_failed",
})


def patch_outcome_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    intent_id: str,
    outcome_kind: str,
    terminal_reason: str = "",
    validator_errors: list[str] | None = None,
    collateral_qids: list[str] | None = None,
    narrow_replacement_attempted: bool = False,
    narrow_outcome: str = "",
    applied_patch_id: str = "",
) -> str:
    """Plan 12 — exactly-once-per-intent_id terminal outcome marker.

    Emitted by ``patch_survival_emitter.emit_patch_outcome`` (the single
    canonical emission point — see that module for idempotency). Invariant
    I22 enforces 1:1 coverage with Stage 3 proposal IDs; double-emit
    is a contract violation.
    """
    if outcome_kind not in _VALID_PATCH_OUTCOME_KINDS:
        raise ValueError(
            f"unknown outcome_kind {outcome_kind!r}; must be one of "
            f"{sorted(_VALID_PATCH_OUTCOME_KINDS)}"
        )
    return marker_line(
        "GSO_PATCH_OUTCOME_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "cluster_id": str(cluster_id),
            "intent_id": str(intent_id),
            "outcome_kind": str(outcome_kind),
            "terminal_reason": str(terminal_reason),
            "validator_errors": list(validator_errors or []),
            "collateral_qids": list(collateral_qids or []),
            "narrow_replacement_attempted": bool(
                narrow_replacement_attempted
            ),
            "narrow_outcome": str(narrow_outcome),
            "applied_patch_id": str(applied_patch_id),
        },
    )


def plan12_ag_pivot_decided_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    prior_terminal_reason: str,
    prior_patch_family: str,
    recommended_patch_family: str,
    pivot_recommended: bool,
    pivot_applied: bool,
) -> str:
    """Plan 12 PR 5 deferred — observation marker for the AG-retry
    patch-family pivot policy.

    Emitted at the harness's AG-construction site (immediately after
    the strategist returns the next iteration's AGs) when
    :func:`plan12_live_ag_retry_pivot_enabled` is True. One marker
    per AG-with-prior-terminal-signature; the postmortem renderer
    reads the stream to confirm the policy is firing correctly
    before a follow-up commit promotes ``pivot_applied=True`` to an
    actual AG mutation.

    Fields:

      - ``prior_terminal_reason`` — the terminal reason from the
        prior iteration's terminal signature for this cluster (e.g.
        ``no_applied_patches`` or ``structural_gate_dropped_instruction_only``).
      - ``prior_patch_family`` — the patch family the strategist
        chose for this AG (derived from its ``lever_directives``).
      - ``recommended_patch_family`` — the family
        :func:`next_patch_family_for_cluster` recommends. Differs
        from ``prior_patch_family`` only when the pivot policy fires.
      - ``pivot_recommended`` — True when the recommendation differs
        from the prior choice.
      - ``pivot_applied`` — currently always False (observation-only).
        A future commit flips this when the AG mutation is wired in.
    """
    return marker_line(
        "GSO_PLAN12_AG_PIVOT_DECIDED_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "cluster_id": str(cluster_id),
            "prior_terminal_reason": str(prior_terminal_reason),
            "prior_patch_family": str(prior_patch_family),
            "recommended_patch_family": str(recommended_patch_family),
            "pivot_recommended": bool(pivot_recommended),
            "pivot_applied": bool(pivot_applied),
        },
    )


def plan12_evidence_routing_decided_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    evidence_kind: str,
    target_lever_before: int,
    target_lever_after: int,
    reroute_applied: bool,
) -> str:
    """Plan 12 PR 6 deferred — observation marker for the evidence→lever
    routing policy.

    Emitted by the harness's lever loop immediately before each call
    to :func:`generate_proposals_from_strategy` when
    :func:`plan12_live_evidence_routing_enabled` is True. One marker
    per lever-call (BoN and single-shot both emit). The marker
    records what
    :func:`_apply_evidence_to_lever_policy` decided for this
    ``(target_lever_before, evidence_kind)`` pair so postmortem
    replays can audit every routing decision before a high-tier
    invariant codifies the policy.

    Fields:

      - ``evidence_kind`` — the value the policy looked up, drawn
        from ``ag.asi_failure_type`` (preferred) or ``ag.root_cause``
        (fallback) or the empty string (unknown bucket).
      - ``target_lever_before`` — the integer lever the harness's
        ``lever_keys`` loop chose for this directive.
      - ``target_lever_after`` — the integer lever the policy
        recommends. Equals ``target_lever_before`` when the policy
        passes through (target_lever ≠ 1 or evidence permits Lever 1).
      - ``reroute_applied`` — True when ``target_lever_after`` differs
        from ``target_lever_before``. The harness uses the rerouted
        lever in the subsequent
        :func:`generate_proposals_from_strategy` call.
    """
    return marker_line(
        "GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "ag_id": str(ag_id),
            "cluster_id": str(cluster_id),
            "evidence_kind": str(evidence_kind),
            "target_lever_before": int(target_lever_before),
            "target_lever_after": int(target_lever_after),
            "reroute_applied": bool(reroute_applied),
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
