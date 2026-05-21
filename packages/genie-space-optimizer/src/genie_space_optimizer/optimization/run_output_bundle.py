"""Pure bundle assembly helpers (Phase H).

Produces JSON payloads for manifest.json, run_summary.json,
artifact_index.json from completed run state. No MLflow / Spark / I/O —
the harness wire-up calls these helpers and pushes the result to MLflow.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.run_output_contract import (
    PROCESS_STAGE_ORDER,
    bundle_artifact_paths,
    iteration_bundle_prefix,
    stage_artifact_paths,
)
from genie_space_optimizer.optimization.stages import STAGES


SCHEMA_VERSION = "v1"


def _normalize_stage_capture(
    value: object, *, stage_key: str = "", iteration: int = 0,
) -> dict:
    """Cycle 14-V T5 + Cycle 14-W T2 — normalize a stage-capture
    value to a dict so downstream ``.get()`` access is safe.

    The harness's ``stage_io_capture`` wrapper occasionally produces
    list-valued captures (when a stage emits multiple decisions in
    a single call). The bundle assembler historically called
    ``.get()`` directly, raising ``AttributeError`` on lists.

    Anchor evidence: airline run 833709971504406 F7 + airline run
    1105451933925748 F7 (regressed in C14-V) — both emit
    ``GSO_BUNDLE_ASSEMBLY_FAILED_V1`` with
    ``AttributeError: 'list' object has no attribute 'get'``.

    Behaviour:
      - dict → returned unchanged.
      - list-of-dict → first dict in the list returned (lossy
        collapse; emits ``GSO_BUNDLE_ASSEMBLY_LIST_NORMALIZED_V1``
        when called from the audited assembler call sites so
        postmortem tooling can quantify how often this safety net
        engages).
      - list-of-non-dict / empty list / non-dict / None → empty dict.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        for index, item in enumerate(value):
            if isinstance(item, dict):
                # Cycle 14-W T2: signal the lossy collapse so the
                # postmortem analyzer can track regression rate.
                if stage_key:
                    try:
                        from genie_space_optimizer.optimization.run_analysis_contract import (
                            bundle_assembly_list_normalized_marker,
                        )
                        print(bundle_assembly_list_normalized_marker(
                            optimization_run_id="",
                            iteration=int(iteration),
                            stage_key=str(stage_key),
                            original_type="list",
                            normalized_to=f"dict_at_index_{index}",
                        ))
                    except Exception:
                        # Defensive: marker emit must never break
                        # the assembler's normalization path.
                        pass
                return item
        return {}
    return {}


def _normalize_accuracy_pct(value: Any) -> Any:
    """Cycle 6 F-6 — collapse 0-1 fraction inputs and 0-100 percent
    inputs to a single canonical 0-100 representation, rounded to one
    decimal. The harness has historically passed both shapes for
    ``overall_accuracy`` and ``accuracy_delta_pp`` depending on call
    site; the bundle write must speak one unit so the operator
    transcript no longer prints ``Baseline accuracy: 8947.0%``.

    Non-numeric values pass through unchanged so the helper is safe
    to call on partial/legacy payloads.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return value
    if 0.0 <= f <= 1.0:
        f = f * 100.0
    return round(f, 1)


def build_manifest(
    *,
    optimization_run_id: str,
    databricks_job_id: str,
    databricks_parent_run_id: str,
    lever_loop_task_run_id: str,
    iterations: list[int],
    missing_pieces: list[dict[str, Any]],
    databricks_ids_resolution_path: str = "",
) -> dict[str, Any]:
    """Build manifest.json for the parent bundle.

    ``databricks_ids_resolution_path`` (P-B) records which resolver
    tier produced the IDs (``env`` / ``dbutils`` / ``mixed`` /
    ``jobs_api`` / ``mixed_jobs_api`` / ``sentinel`` / ``""``). The
    empty default applies to legacy callers — ``""`` is "I don't
    know" rather than implying any particular tier.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "optimization_run_id": optimization_run_id,
        "databricks_job_id": databricks_job_id,
        "databricks_parent_run_id": databricks_parent_run_id,
        "lever_loop_task_run_id": lever_loop_task_run_id,
        "databricks_ids_resolution_path": databricks_ids_resolution_path,
        "iteration_count": len(iterations),
        "iterations": list(iterations),
        "missing_pieces": missing_pieces,
        # Phase H Fidelity Task 6 — manifest stage order must mirror the
        # 11-entry transcript contract (``PROCESS_STAGE_ORDER``) so
        # postmortem skills walk every stage the operator transcript
        # renders, not just the 9 executable stages. The executable
        # subset is published separately for consumers that need to
        # locate stage I/O artifacts.
        "stage_keys_in_process_order": [s.key for s in PROCESS_STAGE_ORDER],
        "executable_stage_keys": [e.stage_key for e in STAGES],
    }


def _stage_dir_name_for(paths: dict[str, str]) -> str:
    """Extract the ``NN_<stage_key>`` directory name from a stage path."""
    parts = paths["input"].split("/")
    # Path shape: gso_postmortem_bundle/iterations/iter_NN/stages/<NN_key>/input.json
    # The component immediately after "stages" is the dir name.
    return parts[parts.index("stages") + 1]


def build_artifact_index(*, iterations: list[int]) -> dict[str, Any]:
    """Build artifact_index.json — a flat path map for postmortem skills.

    Includes per-stage paths so the gso-postmortem skill can
    deterministically reach every iteration's stage I/O without
    walking directories.

    Plan P-A (2026-05-12): ``stages_index`` is a leaf path satisfying
    the bundle contract; ``stage_artifacts`` is a structured map keyed
    by ``NN_<stage_key>`` so the postmortem skill can still reach
    every stage's input/output/decisions JSONs.
    """
    base = bundle_artifact_paths(iterations=iterations)
    flat: dict[str, Any] = {
        "manifest":               base["manifest"],
        "run_summary":            base["run_summary"],
        "operator_transcript":    base["operator_transcript"],
        "decision_trace_all":     base["decision_trace_all"],
        "journey_validation_all": base["journey_validation_all"],
        "replay_fixture":         base["replay_fixture"],
        "scoreboard":             base["scoreboard"],
        "failure_buckets":        base["failure_buckets"],
        "iterations": {},
    }
    for iteration in iterations:
        prefix = iteration_bundle_prefix(iteration)
        per_iter: dict[str, Any] = {
            "summary":             f"{prefix}/summary.json",
            "operator_transcript": f"{prefix}/operator_transcript.md",
            "decision_trace":      f"{prefix}/decision_trace.json",
            "journey_validation":  f"{prefix}/journey_validation.json",
            "rca_ledger":          f"{prefix}/rca_ledger.json",
            "proposal_inventory":  f"{prefix}/proposal_inventory.json",
            "patch_survival":      f"{prefix}/patch_survival.json",
            "stages_index":        f"{prefix}/stages/index.json",
            "stage_artifacts":     {},
        }
        for entry in STAGES:
            stage_paths = stage_artifact_paths(iteration, entry.stage_key)
            per_iter["stage_artifacts"][_stage_dir_name_for(stage_paths)] = stage_paths
        flat["iterations"][str(iteration)] = per_iter
    return flat


def build_run_summary(
    *,
    baseline: dict[str, Any],
    terminal_state: dict[str, Any],
    iteration_count: int,
    accuracy_delta_pp: float,
    eval_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build run_summary.json — the high-level run outcome.

    Cycle 6 F-6: accuracy fields are normalized to 0-100 percent
    units at the boundary so downstream renderers never multiply.

    Plan 12 PR 7 Task 7.5: ``hard_failures_count`` and
    ``soft_failures_count`` are derived from ``eval_result.rows`` when
    the kwarg is supplied. Closes the stale-write bug both 2026-05-20
    postmortems flagged where the legacy counter diverged from the
    actual eval result under retry. When ``eval_result`` is absent
    (legacy callers not yet wired), both counts are 0 — byte-stable
    against existing callers that haven't been threaded through.
    """
    normalized_baseline = dict(baseline or {})
    if "overall_accuracy" in normalized_baseline:
        normalized_baseline["overall_accuracy"] = _normalize_accuracy_pct(
            normalized_baseline["overall_accuracy"]
        )

    hard = 0
    soft = 0
    rows = list((eval_result or {}).get("rows") or [])
    for row in rows:
        raw_score = row.get("score") if row.get("score") is not None else 0.0
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            score = 0.0
        if score < 0.5:
            hard += 1
        elif score < 1.0:
            soft += 1

    return {
        "schema_version": SCHEMA_VERSION,
        "baseline": normalized_baseline,
        "terminal_state": terminal_state,
        "iteration_count": iteration_count,
        "accuracy_delta_pp": _normalize_accuracy_pct(accuracy_delta_pp),
        "hard_failures_count": hard,
        "soft_failures_count": soft,
    }


def build_decision_trace_all(
    *, iter_traces: list[dict[str, Any]],
) -> dict[str, Any]:
    """Cycle 12-T3 — parent-bundle decision_trace_all.json.

    Aggregates per-iteration decision-trace dicts into one document so a
    postmortem can read every iteration's typed records from one path.
    """
    safe = list(iter_traces or [])
    total = sum(len((t or {}).get("records") or []) for t in safe)
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration_count": len(safe),
        "total_record_count": total,
        "iterations": safe,
    }


def build_journey_validation_all(
    *, iter_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    """Cycle 12-T3 — parent-bundle journey_validation_all.json."""
    safe = list(iter_reports or [])
    total_v = sum(len((r or {}).get("violations") or []) for r in safe)
    any_invalid = any(not bool((r or {}).get("is_valid", True)) for r in safe)
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration_count": len(safe),
        "total_violation_count": total_v,
        "any_invalid": any_invalid,
        "iterations": safe,
    }


def build_scoreboard(
    *,
    iter_record_counts: list[int],
    iter_violation_counts: list[int],
    no_records_iterations: list[int],
    levers_attempted: dict[int, int],
    levers_accepted: dict[int, int],
    levers_rolled_back: dict[int, int],
    best_accuracy: float | None,
    baseline_accuracy: float | None,
    iteration_count: int,
) -> dict[str, Any]:
    """Cycle 12-T3 — minimal scoreboard.json.

    A 7-second read of the loop's outcome: per-iteration record/violation
    counts, lever attempt/accept/rollback distribution, and a single
    accuracy_delta_pp summary. Richer LoopSnapshot-based fields are a
    follow-up; this is enough to materialize the contract-declared path
    with structured content.
    """
    if best_accuracy is None or baseline_accuracy is None:
        delta = None
    else:
        delta = round(_normalize_accuracy_pct(best_accuracy)
                      - _normalize_accuracy_pct(baseline_accuracy), 1)
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration_count": int(iteration_count),
        "best_accuracy": (
            _normalize_accuracy_pct(best_accuracy)
            if best_accuracy is not None else None
        ),
        "baseline_accuracy": (
            _normalize_accuracy_pct(baseline_accuracy)
            if baseline_accuracy is not None else None
        ),
        "accuracy_delta_pp": delta,
        "iter_record_counts": list(iter_record_counts or []),
        "iter_violation_counts": list(iter_violation_counts or []),
        "no_records_iterations": list(no_records_iterations or []),
        "levers_attempted": {str(k): int(v) for k, v in (levers_attempted or {}).items()},
        "levers_accepted":  {str(k): int(v) for k, v in (levers_accepted or {}).items()},
        "levers_rolled_back": {str(k): int(v) for k, v in (levers_rolled_back or {}).items()},
    }


def build_failure_buckets(
    *, iter_assignments: list[dict[str, Any]],
) -> dict[str, Any]:
    """Cycle 12-T3 — minimal failure_buckets.json.

    Captures per-iteration failed-QID bucket assignments and a single
    cross-iteration count summary. The richer "per-bucket QID list across
    the whole run" view is a follow-up; this is enough to materialize the
    contract-declared path with structured content.
    """
    safe = list(iter_assignments or [])
    total = 0
    bucket_counts: dict[str, int] = {}
    for entry in safe:
        buckets = (entry or {}).get("buckets") or {}
        for name, qids in buckets.items():
            n = len(qids or [])
            bucket_counts[str(name)] = bucket_counts.get(str(name), 0) + n
            total += n
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration_count": len(safe),
        "total_failed_qid_events": total,
        "bucket_counts": bucket_counts,
        "iterations": safe,
    }


def assemble_bundle_for_replay(replay_fixture: dict) -> dict:
    """Cycle 14-W hardening — drive the full bundle-assembly pipeline
    against a captured replay fixture so an integration test can
    assert no ``GSO_BUNDLE_ASSEMBLY_FAILED_V1`` markers emit across
    every per-iteration call site.

    The seam mirrors what the harness's terminate path does in
    ``harness.py:25067-25411``: build the manifest, run summary,
    artifact index, decision-trace aggregate, journey-validation
    aggregate, scoreboard, failure-buckets aggregate, and the
    per-iteration summaries. Every stage capture is routed through
    ``_normalize_stage_capture`` so a list-valued capture (the
    C14-V D-4 regression shape) cannot raise ``AttributeError``.

    RCO-1 — ``failure_buckets`` is sourced from each iteration's
    ``bucket_assignments`` field, mirroring the harness terminate
    path at ``harness.py:25400-25406``. Iterations that omit the
    field contribute an empty buckets dict, so the seam stays robust
    for partial fixtures.

    Returns a dict keyed on the parent-bundle artifact paths so the
    integration test can assert structural well-formedness.

    Anchor: airline run 1105451933925748 F7 (regressed C14-V D-4) —
    captured stage I/O contains list-valued shapes; the assembler
    must survive without raising. Discipline A applies: this is the
    end-to-end fixture replay that proves the production-shape
    failure no longer occurs.
    """
    iterations = list(replay_fixture.get("iterations") or [])
    iter_indices = [
        int(i.get("iteration") or idx + 1)
        for idx, i in enumerate(iterations)
    ]

    iter_traces: list[dict[str, Any]] = []
    iter_reports: list[dict[str, Any]] = []
    iter_record_counts: list[int] = []
    iter_violation_counts: list[int] = []
    no_records_iterations: list[int] = []
    levers_attempted: dict[int, int] = {}
    levers_accepted: dict[int, int] = {}
    levers_rolled_back: dict[int, int] = {}
    # RCO-1 — Per-iteration bucket assignments feed
    # ``build_failure_buckets``. Shape mirrors the harness terminate
    # path at harness.py:25400-25406, where ``buckets`` comes from each
    # iteration's journey-validation report under ``bucket_assignments``.
    iter_assignments: list[dict[str, Any]] = []
    for idx, blob in enumerate(iterations, start=1):
        records = list(blob.get("decision_records") or [])
        iter_traces.append({"iteration": idx, "records": records})
        violations = list(blob.get("journey_violations") or [])
        iter_reports.append({
            "iteration": idx,
            "violations": violations,
            "is_valid": not bool(violations),
        })
        iter_record_counts.append(len(records))
        iter_violation_counts.append(len(violations))
        if not records:
            no_records_iterations.append(idx)
        # RCO-1 — Capture bucket assignments per iteration so the
        # assembler can emit a failure_buckets payload identical in
        # shape to the harness terminate-path emission.
        iter_assignments.append({
            "iteration": idx,
            "buckets": dict(blob.get("bucket_assignments") or {}),
        })
        # Walk every stage capture through _normalize_stage_capture.
        # This is the critical D-4 invariant: list-valued captures
        # must survive normalisation without raising AttributeError.
        stages = blob.get("stages") or {}
        if isinstance(stages, dict):
            for stage_key, capture in stages.items():
                _normalize_stage_capture(
                    capture,
                    stage_key=str(stage_key),
                    iteration=int(idx),
                )

    return {
        "manifest": build_manifest(
            optimization_run_id=str(replay_fixture.get("fixture_id") or ""),
            databricks_job_id="",
            databricks_parent_run_id="",
            lever_loop_task_run_id="",
            iterations=iter_indices,
            missing_pieces=[],
        ),
        "run_summary": build_run_summary(
            baseline={
                "overall_accuracy": replay_fixture.get("baseline_accuracy", 0.0),
            },
            terminal_state={
                "final_accuracy": replay_fixture.get("final_accuracy", 0.0),
            },
            iteration_count=len(iter_indices),
            accuracy_delta_pp=float(replay_fixture.get("delta_pp", 0.0)),
        ),
        "decision_trace_all": build_decision_trace_all(iter_traces=iter_traces),
        "journey_validation_all": build_journey_validation_all(
            iter_reports=iter_reports,
        ),
        "scoreboard": build_scoreboard(
            iter_record_counts=iter_record_counts,
            iter_violation_counts=iter_violation_counts,
            no_records_iterations=no_records_iterations,
            levers_attempted=levers_attempted,
            levers_accepted=levers_accepted,
            levers_rolled_back=levers_rolled_back,
            best_accuracy=replay_fixture.get("final_accuracy"),
            baseline_accuracy=replay_fixture.get("baseline_accuracy"),
            iteration_count=len(iter_indices),
        ),
        # RCO-1 — failure_buckets parity with harness terminate path
        # (harness.py:25392-25411). Wired here so contract-health
        # (RCO-2) can consume a complete parent bundle from replay.
        "failure_buckets": build_failure_buckets(
            iter_assignments=iter_assignments,
        ),
        "artifact_index": build_artifact_index(iterations=iter_indices),
        "iteration_summaries": [
            {"iteration": i, "record_count": c, "violation_count": v}
            for i, c, v in zip(
                iter_indices, iter_record_counts, iter_violation_counts,
            )
        ],
    }


def aggregate_per_iteration_artifacts(
    *,
    iterations: list[int],
    kind: str,
    fetch_fn,
) -> list[dict[str, Any]]:
    """Cycle 12-T3 — Pull per-iteration artifacts via ``fetch_fn(iteration, kind)``
    and return a flat list, skipping ``None`` returns (treated as absent).

    ``fetch_fn`` signature: ``(iteration: int, kind: str) -> dict | None``.
    Pure: no I/O; the caller injects the fetch implementation.
    """
    aggregated: list[dict[str, Any]] = []
    for it in iterations or []:
        try:
            entry = fetch_fn(int(it), str(kind))
        except Exception:
            entry = None
        if entry is None:
            continue
        aggregated.append(entry)
    return aggregated


# ---------------------------------------------------------------------------
# Plan P-A (2026-05-12) — per-iteration artifact builders
#
# Six pure builders consumed by ``_materialize_per_iter_contract_paths``
# (harness.py) to write the eight per-iter contract paths for every
# iteration in ``_phase_h_iterations_completed``, regardless of
# ``exit_path``. Each builder is dict-in, dict-out, no I/O. Missing
# in-memory state is gracefully tolerated — the file always materializes
# with an empty-but-well-formed payload so the assembler completeness
# check reports ``complete=True`` even on skipped iterations.
# ---------------------------------------------------------------------------


def build_iteration_summary_payload(
    *,
    iteration: int,
    iter_summary: dict[str, Any],
    invariant_violations: tuple[dict, ...] = (),
) -> dict[str, Any]:
    """Plan P-A — Build the per-iteration ``summary.json`` payload.

    Mirrors the in-memory ``iter_summary`` dict produced by
    ``_build_iteration_summary_dict`` (harness.py) so postmortem
    skills can read a single per-iter file with the same fields the
    aggregate operator transcript renders. ``invariant_violations``
    are projected as a list so the per-iter file is the single source
    of truth for "what went wrong in iteration N" — independent of
    the run-level aggregation.
    """
    safe_summary = dict(iter_summary or {})
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration": int(iteration),
        "exit_path": str(safe_summary.get("exit_path") or "in_progress"),
        "accepted_count": int(safe_summary.get("accepted_count") or 0),
        "rolled_back_count": int(safe_summary.get("rolled_back_count") or 0),
        "skipped_count": int(safe_summary.get("skipped_count") or 0),
        "gate_drop_count": int(safe_summary.get("gate_drop_count") or 0),
        "decision_record_count": int(
            safe_summary.get("decision_record_count") or 0
        ),
        "journey_violation_count": int(
            safe_summary.get("journey_violation_count") or 0
        ),
        "iteration_accuracy": safe_summary.get("iteration_accuracy"),
        "invariant_violations": [
            dict(v) for v in (invariant_violations or ())
        ],
    }


def build_phase_h_aggregate_iteration_summaries_payload(
    *,
    optimization_run_id: str,
    iteration_counter: int,
    replay_fixture_iterations: list[dict] | None = None,
) -> dict:
    """Phase 0.2 — aggregate Phase H ``iteration_summaries.json`` payload.

    Sources every per-iteration summary entry from the SAME in-memory
    ``replay_fixture_iterations`` list used to build the
    ``PHASE_A_REPLAY_FIXTURE_JSON`` stderr blob, guaranteeing parity
    between stderr fixture and Phase H artifact. The output payload
    is the canonical contents of
    ``gso_postmortem_bundle/iteration_summaries.json``.

    Distinct from :func:`build_iteration_summary_payload` which writes
    one file per iteration. This aggregate variant is the single
    Phase H artifact for the whole run.

    Pure: no I/O. Empty ``replay_fixture_iterations`` yields an
    empty ``iteration_summaries`` list.
    """
    iterations = list(replay_fixture_iterations or [])
    summaries: list[dict] = []
    for entry in iterations:
        summaries.append({
            "iteration": int(entry.get("iteration") or 0),
            "ag_id": str(entry.get("ag_id") or ""),
            "decision_record_count": int(entry.get("decision_record_count") or 0),
            "accepted_count": int(entry.get("accepted_count") or 0),
            "rolled_back_count": int(entry.get("rolled_back_count") or 0),
            "skipped_count": int(entry.get("skipped_count") or 0),
            "gate_drop_count": int(entry.get("gate_drop_count") or 0),
            "journey_violation_count": int(entry.get("journey_violation_count") or 0),
            "eval_rows": int(entry.get("eval_rows") or 0),
        })

    return {
        "optimization_run_id": str(optimization_run_id or ""),
        "iteration_counter": int(iteration_counter),
        "iteration_summaries": summaries,
        "schema_version": "v1",
    }


def build_iteration_decision_trace_payload(
    *,
    iteration: int,
    decision_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Plan P-A — Build the per-iteration ``decision_trace.json`` payload.

    The harness terminate path provides ``decision_records`` as the
    list of ``DecisionRecord.to_dict()`` outputs from the iteration's
    ``OptimizationTrace.decision_records`` tuple. Skipped iterations
    contribute an empty list rather than no file — the file always
    exists so postmortem skills can iterate ``iter_NN`` without
    branching on exit_path.
    """
    safe = list(decision_records or [])
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration": int(iteration),
        "record_count": len(safe),
        "records": safe,
    }


def build_iteration_journey_validation_payload(
    *,
    iteration: int,
    journey_report: dict[str, Any] | None,
) -> dict[str, Any]:
    """Plan P-A — Build the per-iteration ``journey_validation.json``
    payload. ``journey_report`` is the dict produced by
    ``JourneyValidationReport.to_dict()`` for the iteration, or
    ``None`` when the iteration exited before journey validation ran.

    ``is_valid`` is always derived from ``violations`` so a producer
    bug in one cannot let the other drift.
    """
    safe = dict(journey_report or {})
    violations = list(safe.get("violations") or [])
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration": int(iteration),
        "is_valid": len(violations) == 0,
        "violation_count": len(violations),
        "violations": violations,
        "bucket_assignments": dict(safe.get("bucket_assignments") or {}),
    }


def build_iteration_rca_ledger_payload(
    *,
    iteration: int,
    rca_ledger: dict[str, Any] | None,
) -> dict[str, Any]:
    """Plan P-A — Build the per-iteration ``rca_ledger.json`` payload.

    ``rca_ledger`` is the dict produced by
    ``regression_mining.build_rca_ledger`` and stamped on
    ``metadata_snapshot["_rca_ledger"]`` during the iteration. When
    the iteration exits before RCA evidence runs, the dict is None
    and the builder produces an empty-but-well-formed payload so
    postmortem totality holds.
    """
    safe = dict(rca_ledger or {})
    themes = list(safe.get("themes") or [])
    conflicts = list(safe.get("conflicts") or [])
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration": int(iteration),
        "theme_count": len(themes),
        "conflict_count": len(conflicts),
        "themes": themes,
        "conflicts": conflicts,
        "cards_by_cluster": dict(safe.get("cards_by_cluster") or {}),
    }


def build_iteration_proposal_inventory_payload(
    *,
    iteration: int,
    proposal_inventory: dict[str, Any] | None,
) -> dict[str, Any]:
    """Plan P-A — Build the per-iteration ``proposal_inventory.json``
    payload. ``proposal_inventory`` is the dict stamped on
    ``current_iter_inputs["proposal_inventory"]`` by the proposal
    stage; missing for iterations that exited at strategy_zero_ags
    or earlier.

    ``disposition_counts`` is derived from ``proposals[*].disposition``
    so a postmortem can read a single number per disposition without
    walking the full proposal list.
    """
    safe = dict(proposal_inventory or {})
    proposals = list(safe.get("proposals") or [])
    counts: dict[str, int] = {}
    for p in proposals:
        if not isinstance(p, dict):
            continue
        d = str(p.get("disposition") or "unknown")
        counts[d] = counts.get(d, 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration": int(iteration),
        "proposal_count": len(proposals),
        "disposition_counts": counts,
        "proposals": proposals,
        "by_ag": dict(safe.get("by_ag") or {}),
    }


def build_iteration_stage_index_payload(
    *,
    iteration: int,
    captured_stage_keys: tuple[str, ...] | list[str],
) -> dict[str, Any]:
    """Plan P-A — Build the per-iteration ``stages/index.json`` leaf.

    Replaces the contract's directory-as-declared-path entry
    (``f"{prefix}/stages"``) with a real leaf file so
    ``assembler_completeness_check`` can satisfy it via MLflow listing.

    ``captured_stage_keys`` is provided by ``stage_io_capture
    .consume_stage_capture_index()``; the builder cross-references
    against ``PROCESS_STAGE_ORDER`` to compute the ``skipped`` set
    so postmortem can see at a glance how far the iteration got
    before exiting.
    """
    captured = sorted({str(k) for k in (captured_stage_keys or ())})
    declared = [s.key for s in PROCESS_STAGE_ORDER]
    captured_set = set(captured)
    skipped = [s for s in declared if s not in captured_set]
    return {
        "schema_version": SCHEMA_VERSION,
        "iteration": int(iteration),
        "captured_count": len(captured),
        "captured": captured,
        "skipped": skipped,
    }
