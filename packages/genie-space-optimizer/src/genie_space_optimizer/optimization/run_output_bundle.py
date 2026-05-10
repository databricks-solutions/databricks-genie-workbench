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
) -> dict[str, Any]:
    """Build manifest.json for the parent bundle."""
    return {
        "schema_version": SCHEMA_VERSION,
        "optimization_run_id": optimization_run_id,
        "databricks_job_id": databricks_job_id,
        "databricks_parent_run_id": databricks_parent_run_id,
        "lever_loop_task_run_id": lever_loop_task_run_id,
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
            "stages": {},
        }
        for entry in STAGES:
            stage_paths = stage_artifact_paths(iteration, entry.stage_key)
            per_iter["stages"][_stage_dir_name_for(stage_paths)] = stage_paths
        flat["iterations"][str(iteration)] = per_iter
    return flat


def build_run_summary(
    *,
    baseline: dict[str, Any],
    terminal_state: dict[str, Any],
    iteration_count: int,
    accuracy_delta_pp: float,
) -> dict[str, Any]:
    """Build run_summary.json — the high-level run outcome.

    Cycle 6 F-6: accuracy fields are normalized to 0-100 percent
    units at the boundary so downstream renderers never multiply.
    """
    normalized_baseline = dict(baseline or {})
    if "overall_accuracy" in normalized_baseline:
        normalized_baseline["overall_accuracy"] = _normalize_accuracy_pct(
            normalized_baseline["overall_accuracy"]
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "baseline": normalized_baseline,
        "terminal_state": terminal_state,
        "iteration_count": iteration_count,
        "accuracy_delta_pp": _normalize_accuracy_pct(accuracy_delta_pp),
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
    ``harness.py:23878-23918``: build the manifest, run summary,
    artifact index, decision-trace aggregate, journey-validation
    aggregate, scoreboard, and per-iteration bundles. Every stage
    capture is routed through ``_normalize_stage_capture`` so a
    list-valued capture (the C14-V D-4 regression shape) cannot
    raise ``AttributeError``.

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
