"""
Optimization Harness — stage functions for the 6-task Databricks Job.

The canonical execution path is the **6-task DAG** launched via
``submit_optimization()`` in ``job_launcher.py``.  Each DAG notebook is a
thin wrapper that deserializes task values, calls a single harness function,
and publishes outputs.

Each ``_run_*`` / ``_prepare_*`` function encapsulates all business logic
for its stage so that both the DAG notebooks and the ``optimize_genie_space()``
convenience function (used for dev/test only) share identical code paths.

Architecture: ``preflight`` → ``baseline_eval`` → ``enrichment`` →
``lever_loop`` → ``finalize`` → ``deploy``.  Inter-task data flows via
``dbutils.jobs.taskValues``.  Detailed state goes to Delta.

Lever loop ordering: each iteration drains the per-run
``diagnostic_action_queue`` (coverage-gap AGs from
``uncovered_patchable_clusters``) before invoking the strategist; live
clusters bypass diagnostics that have since resolved.

Control-plane diagnostics: lever-loop iterations emit
- ``Baseline source for control plane``
- ``Pre row iteration id``
- ``Post row iteration id``
so operators can audit which baseline rows fed the acceptance gate.

GT-correction diagnostics: the visible "GT correction candidates" line
counts unique question ids via ``_gt_correction_candidate_qids`` set
(see ``len(_gt_correction_candidate_qids)``) instead of raw row count.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from collections import Counter

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    ARBITER_CORRECTION_TRIGGER,
    CONSECUTIVE_ESCALATION_LIMIT,
    CONSECUTIVE_ROLLBACK_LIMIT,
    INFRA_RETRY_BUDGET,
    DEFAULT_LEVER_ORDER,
    DEFAULT_THRESHOLDS,
    DIMINISHING_RETURNS_EPSILON,
    DIMINISHING_RETURNS_LOOKBACK,
    ENABLE_CONTROL_PLANE_ACCEPTANCE,
    ENABLE_REGRESSION_MINING_RCA_LEDGER,
    ENABLE_REGRESSION_MINING_STRATEGIST,
    ENABLE_RCA_LEDGER,
    ENABLE_RCA_THEME_SELECTION,
    ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS,
    ENABLE_PROMPT_MATCHING_AUTO_APPLY,
    ENABLE_SLICE_GATE,
    SLICE_GATE_SMALL_CORPUS_ROWS,
    SLICE_GATE_TOLERANCE_SMALL_CORPUS,
    FINALIZE_REPEATABILITY_PASSES,
    GENIE_CORRECT_CONFIRMATION_THRESHOLD,
    GT_REPAIR_PROMPT,
    INLINE_EVAL_DELAY,
    INSTRUCTION_PROMPT_NAME_TEMPLATE,
    LEVER_NAMES,
    MAX_AG_PATCHES,
    MAX_BENCHMARK_COUNT,
    MAX_ITERATIONS,
    MAX_NOISE_FLOOR,
    NEITHER_CORRECT_QUARANTINE_THRESHOLD,
    NEITHER_CORRECT_REPAIR_THRESHOLD,
    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS,
    PROPAGATION_WAIT_SECONDS,
    RCA_MAX_THEME_PATCHES_PER_ITERATION,
    RCA_MAX_THEMES_PER_ITERATION,
    REGRESSION_MINING_STRATEGIST_MIN_CONFIDENCE,
    REGRESSION_THRESHOLD,
    SHADOW_APPLY,
    SLICE_GATE_MIN_REDUCTION,
    SLICE_GATE_TOLERANCE,
    format_mlflow_template,
)
from genie_space_optimizer.common.warehouse import resolve_warehouse_id
from genie_space_optimizer.optimization.applier import (
    _get_general_instructions,
    apply_patch_set,
    auto_apply_prompt_matching,
    proposals_to_patches,
    rollback,
)
from genie_space_optimizer.optimization.evaluation import (
    _arbiter_str as _eval_arbiter_str,
    _extract_genie_sql_from_trace,
    _rc_str as _eval_rc_str,
    all_thresholds_met,
    extract_reference_sqls,
    extract_reference_result_hashes,
    filter_benchmarks_by_scope,
    log_asi_feedback_on_traces,
    log_gate_feedback_on_traces,
    log_judge_verdicts_on_traces,
    log_persistence_context_on_traces,
    make_predict_fn,
    normalize_scores,
    register_instruction_version,
    run_evaluation,
    run_repeatability_evaluation,
)
from genie_space_optimizer.optimization.models import (
    create_genie_model_version,
    promote_best_model,
)
from genie_space_optimizer.optimization.optimizer import (
    _call_llm_for_adaptive_strategy,
    _collect_blank_columns,
    _collect_insufficient_tables,
    _diagnose_lever3_directive_emission,
    _enrich_blank_descriptions,
    _enrich_table_descriptions,
    _generate_holistic_strategy,
    _iq_scan_strategist_enabled,
    _strategist_memo_key,
    cluster_failures,
    detect_regressions,
    enrich_metadata_with_uc_types,
    format_reflection_buffer,
    generate_metadata_proposals,
    generate_proposals_from_strategy,
    rank_clusters,
)
from genie_space_optimizer.optimization.example_safety import (
    check_teaching_safety,
)
from genie_space_optimizer.optimization.example_smoke_test import (
    SmokeTestResult,
    run_pre_promotion_smoke_test,
)
from genie_space_optimizer.optimization.preflight import run_preflight
from genie_space_optimizer.optimization.repeatability import run_repeatability_test
from genie_space_optimizer.optimization.report import generate_report
from genie_space_optimizer.optimization.scorers import make_all_scorers
from genie_space_optimizer.optimization.state import (
    create_run,
    ensure_optimization_tables,
    load_all_full_iterations,
    load_latest_full_iteration,
    load_latest_state_iteration,
    load_run,
    load_stages,
    mark_patches_rolled_back,
    update_iteration_reflection,
    update_provenance_gate,
    update_provenance_proposals,
    update_run_status,
    write_asi_results,
    write_gt_correction_candidates,
    write_iteration,
    write_patch,
    write_provenance,
    write_stage,
    write_suggestion,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

FINALIZE_TIMEOUT_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS", "6600"),
)
FINALIZE_HEARTBEAT_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS", "30"),
)


_W = 78

def _section(title: str, char: str = "=") -> str:
    pad = max(0, _W - len(title) - 4)
    return f"\n{char * 2} {title} {char * pad}"


def _kv(key: str, value: object, indent: int = 2) -> str:
    return f"{' ' * indent}{'| ' if indent == 0 else '|  '}{key + ':':<28s} {value}"


def _bar(char: str = "-") -> str:
    return char * _W


def _build_baseline_overview_dict(
    *,
    prev_accuracy_percent: float,
    prev_scores: dict[str, float] | None,
    hard_failure_count: int,
    soft_signal_count: int,
) -> dict[str, Any]:
    """Build the baseline dict consumed by ``render_run_overview``.

    ``render_run_overview`` multiplies ``overall_accuracy`` and
    ``all_judge_pass_rate`` by 100 to format them as percentages, so
    callers must hand fractions in the [0.0, 1.0] range. The harness
    historically tracks ``prev_accuracy`` as a 0-100 percentage; this
    helper converts and clamps so the transcript no longer prints
    values like ``8947.0%``.

    ``all_judge_pass_rate`` is approximated as the minimum per-judge
    pass rate (a lower bound on "every judge agreed pass"). When
    ``prev_scores`` is empty, the value is 0.0 — but the renderer
    always emits the row so humans can still see the placeholder.
    """
    try:
        acc_fraction = float(prev_accuracy_percent) / 100.0
    except (TypeError, ValueError):
        acc_fraction = 0.0
    if acc_fraction < 0.0:
        acc_fraction = 0.0
    elif acc_fraction > 1.0:
        acc_fraction = 1.0

    judge_values: list[float] = []
    if prev_scores:
        for v in prev_scores.values():
            try:
                judge_values.append(float(v))
            except (TypeError, ValueError):
                continue
    if judge_values:
        min_judge = min(judge_values) / 100.0
        if min_judge < 0.0:
            min_judge = 0.0
        elif min_judge > 1.0:
            min_judge = 1.0
    else:
        min_judge = 0.0

    return {
        "overall_accuracy": acc_fraction,
        "all_judge_pass_rate": min_judge,
        "hard_failures": int(hard_failure_count or 0),
        "soft_signals": int(soft_signal_count or 0),
    }


def _build_iteration_summary_dict(
    *,
    iteration: int,
    accepted_count: int,
    rolled_back_count: int,
    skipped_count: int,
    gate_drop_count: int,
    decision_record_count: int,
    journey_violation_count: int,
    iteration_accuracy_percent: float | None = None,
) -> dict[str, Any]:
    """Build the per-iteration summary dict for the operator transcript.

    The transcript renderer iterates ``sorted(iteration_summary.items())``
    and emits ``- {k}: {v}`` lines, so this returns small primitives
    only. ``iteration_accuracy`` is rendered as a percent string when
    known and omitted when ``None`` so the transcript does not show a
    misleading 0.0% for iterations whose post-eval is unavailable.
    """
    out: dict[str, Any] = {
        "iteration": int(iteration),
        "accepted_count": int(accepted_count or 0),
        "rolled_back_count": int(rolled_back_count or 0),
        "skipped_count": int(skipped_count or 0),
        "gate_drop_count": int(gate_drop_count or 0),
        "decision_record_count": int(decision_record_count or 0),
        "journey_violation_count": int(journey_violation_count or 0),
    }
    if iteration_accuracy_percent is not None:
        try:
            out["iteration_accuracy"] = f"{float(iteration_accuracy_percent):.1f}%"
        except (TypeError, ValueError):
            pass
    return out


def _build_loop_out_with_pretty_print(
    *,
    loop_out_base: dict,
    phase_h_full_transcript: str | None,
    phase_h_anchor_run_id: str | None,
) -> dict:
    """Add ``pretty_print_transcript`` to the lever-loop return dict
    whenever Phase H actually rendered a transcript.

    Stdout rendering is deliberately decoupled from MLflow artifact
    upload: the notebook prints the transcript to stdout (recoverable
    via ``databricks jobs export-run``) whether or not a parent MLflow
    anchor could be resolved. Absence of a transcript is silent for the
    stdout path, but the dict always carries
    ``phase_h_pretty_print_status`` / ``phase_h_pretty_print_reason``
    diagnostics so the notebook fallback log can explain what happened
    — even when stdout is unavailable (e.g. when a caller only reads
    ``dbutils.notebook.exit(...)`` JSON via ``jobs get-run-output``).

    Status vocabulary:

    - ``rendered_and_uploaded`` — transcript rendered and an MLflow
      parent anchor was available for bundle upload.
    - ``rendered_stdout_only`` — transcript rendered but no anchor was
      available; the bundle upload was skipped, but stdout still gets
      the transcript.
    - ``skipped`` — Phase H did not produce a transcript (replay path,
      legacy harness, or an exception inside the rendering block).
    """
    out = dict(loop_out_base)
    if phase_h_full_transcript:
        out["pretty_print_transcript"] = str(phase_h_full_transcript)
        if phase_h_anchor_run_id:
            out["phase_h_pretty_print_status"] = "rendered_and_uploaded"
            out["phase_h_pretty_print_reason"] = "ok"
        else:
            out["phase_h_pretty_print_status"] = "rendered_stdout_only"
            out["phase_h_pretty_print_reason"] = "no_mlflow_anchor"
    else:
        out["phase_h_pretty_print_status"] = "skipped"
        out["phase_h_pretty_print_reason"] = "no_transcript_rendered"
    return out


def _format_scoreboard_banner(*, loop_snapshot: dict) -> str:
    """Render an end-of-iteration scoreboard banner.

    Phase D: prefers the trace-derived ``build_scoreboard`` path when
    ``loop_snapshot["trace"]`` is populated; falls back to the legacy
    synthetic-LoopSnapshot path otherwise so behaviour is backward-
    compatible during the migration cycle. Any exception renders an
    "unavailable" banner so the loop never breaks on rendering.
    """
    snap = loop_snapshot or {}
    iteration_label = snap.get("iteration", "?")

    trace = snap.get("trace")
    if trace is not None:
        try:
            from genie_space_optimizer.optimization.scoreboard import (
                build_scoreboard,
            )
            scoreboard_snap = build_scoreboard(
                trace=trace,
                iteration=int(snap.get("iteration") or 0),
                baseline_accuracy=float(snap.get("baseline_accuracy") or 0.0),
                candidate_accuracy=float(snap.get("candidate_accuracy") or 0.0),
                run_id=str(snap.get("run_id") or ""),
            )
            result = scoreboard_snap.to_dict()
        except Exception:
            return (
                _section(
                    f"SCOREBOARD UNAVAILABLE  iteration_{iteration_label}", "-",
                )
                + "\n" + _kv("dominant_signal", "unavailable")
                + "\n" + _bar("-")
            )
    else:
        try:
            from genie_space_optimizer.optimization.scoreboard import (
                LoopSnapshot,
                compute_scoreboard,
            )
            passing = list(snap.get("passing_qids") or [])
            hard = list(snap.get("hard_failure_qids") or [])
            applied = int(snap.get("applied_patch_count") or 0)
            rolled_back = int(snap.get("rolled_back_patch_count") or 0)
            loop_snap = LoopSnapshot(
                question_ids=list(passing) + list(hard),
                hard_cluster_qids={qid: "c1" for qid in hard},
                journey_events_per_qid={qid: ["accepted"] for qid in passing},
                proposed_patches=[
                    {"proposal_id": f"p{i}"} for i in range(applied + rolled_back)
                ],
                applied_patches=[{"proposal_id": f"p{i}"} for i in range(applied)],
                rolled_back_patches=[
                    {"proposal_id": f"r{i}"} for i in range(rolled_back)
                ],
                malformed_proposals_at_cap_count=0,
                rollback_records=[],
                terminal_unactionable_qids=set(),
                baseline_accuracy=0.0,
                candidate_accuracy=0.0,
                trace_id_fallback_recovered=int(
                    snap.get("trace_id_fallback_count") or 0,
                ),
                trace_id_fallback_total=int(snap.get("trace_id_total") or 0),
            )
            result = compute_scoreboard(loop_snap)
        except Exception:
            return (
                _section(
                    f"SCOREBOARD UNAVAILABLE  iteration_{iteration_label}", "-",
                )
                + "\n" + _kv("dominant_signal", "unavailable")
                + "\n" + _bar("-")
            )

    lines = [
        _section(f"END-OF-ITERATION SCOREBOARD  iteration_{iteration_label}", "="),
    ]
    lines.append(_kv("dominant_signal", result.get("dominant_signal", "?")))
    for k in (
        "journey_completeness_pct",
        "hard_cluster_coverage_pct",
        "causal_patch_survival_pct",
        "decision_trace_completeness_pct",
        "rca_loop_closure_pct",
        "trace_id_fallback_rate",
        "accuracy_delta",
    ):
        if k in result:
            lines.append(_kv(k, result[k]))
    lines.append(_bar("="))
    return "\n".join(lines)


def _build_iteration_trace(
    *,
    decision_records,
    journey_events,
):
    """Wrap the iteration's accumulated DecisionRecords + journey events
    into an OptimizationTrace for ``build_scoreboard`` consumption."""
    from typing import Mapping
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord,
        OptimizationTrace,
    )
    rec_objects: tuple[DecisionRecord, ...] = tuple(
        DecisionRecord.from_dict(row) if isinstance(row, Mapping) else row
        for row in (decision_records or ())
    )
    return OptimizationTrace(
        decision_records=rec_objects,
        journey_events=tuple(journey_events or ()),
    )


def format_evaluation_summary_block(
    *,
    iteration: int,
    ag_id: str,
    baseline_pre_arbiter: float,
    candidate_pre_arbiter: float,
    baseline_post_arbiter: float,
    candidate_post_arbiter: float,
    target_fixed_qids: tuple[str, ...],
) -> str:
    """Render the evaluation summary block with pre-arbiter delta and regression flag.

    The ``regressed_only_pre_arbiter`` flag is ``yes`` when the candidate's
    pre-arbiter accuracy regressed but its post-arbiter accuracy did not,
    AND no target question id was fixed by the candidate. This catches
    the pathological case where Genie's NL→SQL routing degraded but the
    arbiter masked the regression by rescuing rows post-hoc.
    """
    pre_delta = round(candidate_pre_arbiter - baseline_pre_arbiter, 1)
    post_delta = round(candidate_post_arbiter - baseline_post_arbiter, 1)
    pre_regressed = pre_delta < 0
    post_regressed = post_delta < 0
    has_target_fix = bool(target_fixed_qids)
    only_pre_regressed = (
        "yes" if (pre_regressed and not post_regressed and not has_target_fix) else "no"
    )
    bar = "─" * 88
    return "\n".join([
        f"┌{bar}",
        f"│  EVALUATION SUMMARY  iter={iteration}  ag={ag_id}",
        f"├{bar}",
        f"│  pre_arbiter  baseline={baseline_pre_arbiter:.1f}%  candidate={candidate_pre_arbiter:.1f}%  delta={pre_delta:+.1f}pp",
        f"│  post_arbiter baseline={baseline_post_arbiter:.1f}%  candidate={candidate_post_arbiter:.1f}%  delta={post_delta:+.1f}pp",
        f"│  target_fixed_qids: {sorted(target_fixed_qids) if target_fixed_qids else 'none'}",
        f"│  regressed_only_pre_arbiter: {only_pre_regressed}",
        f"└{bar}",
    ])


# ── Journey-contract emit helpers ───────────────────────────────────
# Each helper is pure: it takes an emit callback (typically the
# ``_journey_emit`` closure inside ``_run_lever_loop``) and stamps the
# canonical contract events for one stage of the loop. Defining them at
# module scope keeps harness call sites one-liners and makes the helpers
# unit-testable without standing up a full lever-loop scope.


from genie_space_optimizer.optimization.eval_entry import (  # noqa: E402,F401
    _emit_eval_entry_journey,
)


# ── Phase E.0 anchored artifact persistence ───────────────────────────
# Resolve a stable lever_loop sibling run for every decision-trail
# upload so phase_a/ and phase_b/ artifacts land on the same operator-
# discoverable run regardless of which stage's MLflow run is currently
# active. See `2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`.

from dataclasses import dataclass as _dataclass


@_dataclass(frozen=True)
class _ArtifactPersistResult:
    success: bool
    anchor_run_id: str
    exception_class: str


def _persist_phase_a_artifact_to_anchor(
    *,
    opt_run_id: str,
    iteration: int,
    report_dict: dict,
) -> _ArtifactPersistResult:
    """Phase E.0 Task 5: anchor phase_a/journey_validation/ to the lever_loop run."""
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
        from genie_space_optimizer.tools.mlflow_artifact_anchor import (
            resolve_anchor_run_id,
        )
    except ImportError as exc:
        return _ArtifactPersistResult(
            success=False, anchor_run_id="",
            exception_class=type(exc).__name__,
        )

    artifact_path = f"phase_a/journey_validation/iter_{int(iteration)}.json"
    try:
        client = MlflowClient()
        active = mlflow.active_run()
        experiment_ids: list[str] = []
        if active is not None:
            experiment_ids.append(active.info.experiment_id)
        else:
            for e in client.search_experiments():
                experiment_ids.append(e.experiment_id)
        anchor = resolve_anchor_run_id(
            client=client,
            opt_run_id=opt_run_id,
            experiment_ids=experiment_ids,
        )
        if not anchor:
            return _ArtifactPersistResult(
                success=False, anchor_run_id="",
                exception_class="NoSiblingRun",
            )
        import json as _json
        client.log_text(
            run_id=anchor,
            text=_json.dumps(report_dict, sort_keys=True, separators=(",", ":")),
            artifact_file=artifact_path,
        )
        client.set_tag(
            anchor,
            f"journey_validation.iter_{int(iteration)}.violations",
            str(len(report_dict.get("violations") or [])),
        )
        client.set_tag(
            anchor,
            f"journey_validation.iter_{int(iteration)}.is_valid",
            str(report_dict.get("is_valid", False)).lower(),
        )
        return _ArtifactPersistResult(
            success=True, anchor_run_id=anchor, exception_class="",
        )
    except Exception as exc:
        return _ArtifactPersistResult(
            success=False, anchor_run_id="",
            exception_class=type(exc).__name__,
        )


def _persist_phase_b_artifacts_to_anchor(
    *,
    opt_run_id: str,
    iteration: int,
    decision_json: str,
    transcript: str,
    record_count: int,
    violation_count: int,
) -> _ArtifactPersistResult:
    """Phase E.0 Task 5: anchor phase_b/ artifacts to the lever_loop run."""
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
        from genie_space_optimizer.tools.mlflow_artifact_anchor import (
            resolve_anchor_run_id,
        )
    except ImportError as exc:
        return _ArtifactPersistResult(
            success=False, anchor_run_id="",
            exception_class=type(exc).__name__,
        )

    decision_path = f"phase_b/decision_trace/iter_{int(iteration)}.json"
    transcript_path = f"phase_b/operator_transcript/iter_{int(iteration)}.txt"
    try:
        client = MlflowClient()
        active = mlflow.active_run()
        experiment_ids: list[str] = []
        if active is not None:
            experiment_ids.append(active.info.experiment_id)
        else:
            for e in client.search_experiments():
                experiment_ids.append(e.experiment_id)
        anchor = resolve_anchor_run_id(
            client=client,
            opt_run_id=opt_run_id,
            experiment_ids=experiment_ids,
        )
        if not anchor:
            return _ArtifactPersistResult(
                success=False, anchor_run_id="",
                exception_class="NoSiblingRun",
            )
        client.log_text(run_id=anchor, text=decision_json, artifact_file=decision_path)
        client.log_text(run_id=anchor, text=transcript, artifact_file=transcript_path)
        client.set_tag(
            anchor,
            f"decision_trace.iter_{int(iteration)}.records",
            str(record_count),
        )
        client.set_tag(
            anchor,
            f"decision_trace.iter_{int(iteration)}.violations",
            str(violation_count),
        )
        return _ArtifactPersistResult(
            success=True, anchor_run_id=anchor, exception_class="",
        )
    except Exception as exc:
        return _ArtifactPersistResult(
            success=False, anchor_run_id="",
            exception_class=type(exc).__name__,
        )


# ── Phase D.5 alternatives-capture helpers ────────────────────────────
# Build the alternatives_by_id maps that the three trace-aware producers
# (cluster_records, strategist_ag_records, proposal_generated_records)
# now accept. Each helper is pure and lazy-imports AlternativeOption /
# RejectReason so the harness module-load order is unaffected.


def _build_cluster_alternatives_by_id(
    *,
    candidate_clusters,
    promoted_cluster_ids,
):
    """Build {chosen_cluster_id: tuple[AlternativeOption, ...]} for stamping.

    Phase D.5 Task 5: used at the cluster-selection site to record which
    candidate clusters were demoted/dropped in favor of each chosen
    hard cluster. The same tuple of rejections is stamped on every
    promoted cluster of the same batch — operators reading the
    transcript see "this hard cluster won out over these others"
    consistently across the batch.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        AlternativeOption,
        RejectReason,
    )

    _REASON_MAP = {
        "below_hard_threshold": RejectReason.BELOW_HARD_THRESHOLD,
        "insufficient_qids": RejectReason.INSUFFICIENT_QIDS,
    }

    promoted_set = {str(cid) for cid in (promoted_cluster_ids or []) if cid}
    rejections = []
    for cluster in candidate_clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        if not cid or cid in promoted_set:
            continue
        reason_str = str(cluster.get("demoted_reason") or "")
        rejections.append(
            AlternativeOption(
                option_id=cid,
                kind="cluster",
                reject_reason=_REASON_MAP.get(reason_str, RejectReason.OTHER),
                reject_detail=reason_str,
            )
        )
    rejections_tuple = tuple(rejections)
    return {cid: rejections_tuple for cid in sorted(promoted_set)}


def _build_ag_alternatives_by_id(
    *,
    strategist_returned_ags,
    emitted_ag_ids,
):
    """Build {chosen_ag_id: tuple[AlternativeOption, ...]} for stamping."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        AlternativeOption,
        RejectReason,
    )

    _REASON_MAP = {
        "buffered": RejectReason.BUFFERED,
        "lower_score": RejectReason.LOWER_SCORE,
        "missing_target_qids": RejectReason.MISSING_TARGET_QIDS,
        "rca_ungrounded": RejectReason.RCA_UNGROUNDED,
    }

    emitted_set = {str(aid) for aid in (emitted_ag_ids or []) if aid}
    rejections = []
    for ag in strategist_returned_ags or []:
        ag_id = str(ag.get("id") or ag.get("ag_id") or "")
        if not ag_id or ag_id in emitted_set or not ag.get("rejected"):
            continue
        reason_str = str(ag.get("reject_reason") or "")
        score_raw = ag.get("_score")
        rejections.append(
            AlternativeOption(
                option_id=ag_id,
                kind="ag",
                score=float(score_raw) if score_raw is not None else None,
                reject_reason=_REASON_MAP.get(reason_str, RejectReason.OTHER),
                reject_detail=(
                    reason_str if reason_str not in _REASON_MAP else ""
                ),
            )
        )
    rejections_tuple = tuple(rejections)
    return {ag_id: rejections_tuple for ag_id in sorted(emitted_set)}


def _build_proposal_alternatives_for_ag(
    *,
    raw_proposals,
    surviving_proposal_ids,
):
    """Build the alternatives tuple shared across an AG's surviving proposals."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        AlternativeOption,
        RejectReason,
    )

    _REASON_MAP = {
        "malformed": RejectReason.MALFORMED,
        "patch_cap_dropped": RejectReason.PATCH_CAP_DROPPED,
        "rca_ungrounded": RejectReason.RCA_UNGROUNDED,
        "missing_target_qids": RejectReason.MISSING_TARGET_QIDS,
        "lower_score": RejectReason.LOWER_SCORE,
    }

    surviving_set = {str(pid) for pid in (surviving_proposal_ids or []) if pid}
    out = []
    for proposal in raw_proposals or []:
        pid = str(proposal.get("proposal_id") or proposal.get("id") or "")
        if not pid or pid in surviving_set or not proposal.get("_dropped"):
            continue
        reason_str = str(proposal.get("_drop_reason") or "")
        score_raw = proposal.get("_score")
        out.append(
            AlternativeOption(
                option_id=pid,
                kind="proposal",
                score=float(score_raw) if score_raw is not None else None,
                reject_reason=_REASON_MAP.get(reason_str, RejectReason.OTHER),
                reject_detail=str(proposal.get("_drop_detail") or ""),
            )
        )
    return tuple(out)


_GATE_TO_STAGE: dict[str, str] = {
    "grounding": "dropped_at_grounding",
    "normalize": "dropped_at_normalize",
    "applyability": "dropped_at_applyability",
    "alignment": "dropped_at_alignment",
    "reflection": "dropped_at_reflection",
}


def _emit_gate_drop_journey(*, emit, gate: str, dropped: list[dict]) -> None:
    """Stamp dropped_at_<gate> events for proposals filtered by a gate.

    ``dropped`` is the list of proposal dicts the gate removed. Each dict must
    carry either ``_grounding_target_qids`` or ``target_qids``, and may carry
    ``_drop_reason`` for diagnostics.
    """
    stage = _GATE_TO_STAGE.get(gate)
    if not stage:
        return
    for prop in dropped or []:
        qids = list(prop.get("_grounding_target_qids") or [])
        if not qids:
            qids = list(prop.get("target_qids") or [])
        qids = [str(q) for q in qids if q]
        if not qids:
            continue
        emit(
            stage,
            question_ids=qids,
            proposal_id=str(prop.get("proposal_id") or ""),
            patch_type=str(prop.get("patch_type") or ""),
            cluster_id=str(prop.get("cluster_id") or ""),
            reason=str(prop.get("_drop_reason") or ""),
        )


from genie_space_optimizer.optimization.ag_outcome import (  # noqa: E402,F401
    _emit_ag_outcome_journey,
)


def _emit_ag_assignment_journey(
    *,
    emit,
    ag_id: str,
    affected_qids,
) -> None:
    """Stamp the ag_assigned event for every qid the AG targets.

    Called immediately after the strategist response is consumed and the
    AG's affected_questions list is finalized, before any 'proposed' event
    fires for that AG. The contract requires AG_ASSIGNED between CLUSTERED
    and PROPOSED.
    """
    if not ag_id:
        return
    qids = [str(q) for q in (affected_qids or []) if q]
    if not qids:
        return
    emit("ag_assigned", question_ids=qids, ag_id=str(ag_id))


def _validate_journeys_at_iteration_end(
    *,
    events,
    eval_qids,
    iteration: int,
    raise_on_violation: bool,
):
    """Validate journey completeness; warn or raise per the toggle.

    Returns the ``JourneyValidationReport`` regardless of validity so the
    caller can persist it (fixture, MLflow, future telemetry sinks). The
    warn/raise behaviour for invalid reports is unchanged.

    Phase 2 wires this with ``raise_on_violation=False`` so a real run logs
    gaps without aborting. Phase 4 flips the toggle to True after Task 8 has
    burned the real-run violation count to zero.
    """
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyContractViolationError,
        validate_question_journeys,
    )

    report = validate_question_journeys(
        events=events,
        eval_qids=eval_qids,
    )
    if report.is_valid:
        return report

    summary = (
        f"Iteration {iteration}: {len(report.violations)} journey contract "
        f"violations across {len(set(v.question_id for v in report.violations))} "
        f"qid(s); missing_qids={list(report.missing_qids)}"
    )
    if raise_on_violation:
        raise JourneyContractViolationError(report)
    logger.warning(summary)
    for v in report.violations[:20]:  # cap to keep logs readable
        logger.warning(
            "  qid=%s kind=%s detail=%s",
            v.question_id, v.kind, v.detail,
        )
    return report


from genie_space_optimizer.optimization.post_eval import (  # noqa: E402,F401
    _emit_post_eval_journey,
)


def _resolve_lever_loop_exit_reason(
    plateau_decision,
    divergence_label: str | None,
) -> str:
    """Resolve the canonical convergence ``reason`` for a Lever Loop run.

    Priority:
        1. ``plateau_decision`` (an ``RcaTerminalDecision``) when the loop
           broke out of the body via the plateau path. We project the
           typed ``status`` enum onto a stable ``"plateau_<status>"`` slug
           so the marker reader sees the same vocabulary as the
           human-readable ``RCA terminal status:`` print.
        2. ``divergence_label`` when the loop broke via the consecutive-
           rollback divergence path. The label is already typed (e.g.
           ``"divergence_consecutive_rollbacks"``).
        3. ``"lever_loop_completed"`` when neither was set — the loop ran
           to its natural body end without an exception.

    Returns:
        A string fit for the ``GSO_CONVERGENCE_V1`` marker's ``reason``.
    """
    if plateau_decision is not None:
        try:
            status_value = plateau_decision.status.value
        except AttributeError:
            status_value = str(plateau_decision.status)
        return f"plateau_{status_value}"
    if divergence_label:
        return str(divergence_label)
    return "lever_loop_completed"


def _drop_proposals_matching_rolled_back_content_fingerprints(
    *,
    proposals: list[dict],
    rolled_back_patches: list[dict],
) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Drop any proposal whose ``patch_retry_signature.content_fingerprint``
    matches a fingerprint already present in the rolled-back set.

    Runs *before* the existing ``_patch_forbidden`` keyed match in the
    proposal-grounding stage. Closes the iter-3/iter-4 same-content gap
    where a non-CONTENT_REGRESSION rollback left ``do_not_retry`` empty
    but the proposal stage still re-emitted byte-identical text.

    Returns ``(kept, dropped)`` where ``dropped`` is a list of
    ``(proposal, reason)`` pairs with reason
    ``"content_fingerprint_seen_in_rolled_back_set"``.
    """
    from genie_space_optimizer.optimization.reflection_retry import (
        patch_retry_signature,
    )

    rolled_back_fingerprints: set[str] = set()
    for p in rolled_back_patches or []:
        if isinstance(p, dict):
            sig = patch_retry_signature(p)
            if len(sig) >= 6 and sig[5]:
                rolled_back_fingerprints.add(sig[5])

    if not rolled_back_fingerprints:
        return list(proposals or []), []

    kept: list[dict] = []
    dropped: list[tuple[dict, str]] = []
    for p in proposals or []:
        if not isinstance(p, dict):
            kept.append(p)
            continue
        sig = patch_retry_signature(p)
        fingerprint = sig[5] if len(sig) >= 6 else ""
        if fingerprint and fingerprint in rolled_back_fingerprints:
            dropped.append((p, "content_fingerprint_seen_in_rolled_back_set"))
        else:
            kept.append(p)
    return kept, dropped


def _extract_eval_result_from_gate(gate_result: dict) -> dict:
    """Return the eval-result payload from a gate_result, regardless of outcome.

    `_run_gate_checks` returns the eval payload under ``full_result`` when the
    gate accepts and under ``failed_eval_result`` when the gate rolls back.
    The carrier (`_latest_eval_result` in `_run_lever_loop`) needs whichever
    is present so downstream consumers (replay-fixture snapshot, post_eval
    journey emit, journey-contract validator, soft-cluster currency check)
    see the most recent measurement of reality regardless of acceptance state.

    Returns ``{}`` when neither key is populated (e.g., the gate failed before
    eval ran). Callers should treat an empty return as "do not overwrite the
    existing carrier value" so a previously-good measurement is not clobbered.
    """
    if not isinstance(gate_result, dict):
        return {}
    candidate = gate_result.get("full_result") or gate_result.get("failed_eval_result")
    if isinstance(candidate, dict) and candidate:
        return candidate
    return {}


def _build_fixture_eval_rows(eval_result: dict) -> list[dict]:
    """Convert an eval-result payload into the replay-fixture eval_rows shape.

    Mirrors the inline derivation at the iteration-start snapshot site so a
    rolled-back iteration can backfill its own snapshot from the gate's eval
    result rather than waiting for the next iteration to inherit it. Returns
    an empty list when the payload has no `question_ids`.
    """
    if not isinstance(eval_result, dict):
        return []
    qids = eval_result.get("question_ids") or []
    if not qids:
        return []
    scores = eval_result.get("scores") or {}
    arbiter = eval_result.get("arbiter_verdicts") or {}
    failures = {str(q) for q in (eval_result.get("failure_question_ids") or [])}
    rows: list[dict] = []
    for q in qids:
        qid = str(q)
        if not qid:
            continue
        if isinstance(scores, dict) and qid in scores:
            v = str(scores[qid]).lower()
            correctness = "yes" if v in ("yes", "true", "1", "pass") else "no"
        else:
            correctness = "no" if qid in failures else "yes"
        row: dict = {"question_id": qid, "result_correctness": correctness}
        if isinstance(arbiter, dict) and qid in arbiter:
            row["arbiter"] = str(arbiter[qid])
        rows.append(row)
    return rows


def _patch_snapshot_target_qids(
    proposal: dict,
    ag_affected_qids: list[str],
) -> list[str]:
    """Return the ``target_qids`` to record on a replay-fixture patch entry.

    Mirrors the same defaulting chain used by
    ``_backfill_patch_causal_metadata`` so the fixture captures the patch's
    *effective* causal scope at apply time, not the proposal's pre-backfill
    scope. Cycle 8 lesson: standard L1-L4 proposal-construction sites do
    not stamp ``target_qids``, but the backfill helper later defaults them
    to the AG's ``affected_questions``. Capturing the same default here
    keeps the fixture honest.

    Lookup order:
      1. ``proposal._grounding_target_qids`` (RCA-bridge / cluster-driven
         narrowing wins).
      2. ``proposal.target_qids`` (explicit narrow scope).
      3. ``ag_affected_qids`` (AG-scoped fallback — the conservative
         default the applier and acceptance-gate already use).
    """
    raw = (
        proposal.get("_grounding_target_qids")
        or proposal.get("target_qids")
        or ag_affected_qids
    )
    return [str(q) for q in (raw or []) if q]


def _baseline_row_qid(row: dict) -> str:
    """Extract the question identifier from a persisted baseline eval row.

    Lookup order (canonical-qid sources first, trace-id aliases last):

    * ``question_id``: replay-fixture / canonical eval shape.
    * ``id``: alternative canonical key.
    * ``inputs/question_id`` (flat): some judges' inputs-namespaced shape.
    * ``inputs.question_id`` (nested): MLflow eval-table inputs payload.
    * ``request.kwargs.question_id`` (nested or JSON-encoded): the
      predict_fn request envelope.
    * ``client_request_id``: MLflow eval-table request column. CYCLE 7
      LESSON: this is an MLflow trace ID like ``tr-...``, NOT a benchmark
      qid. Try canonical sources first; only use this as a last-resort
      fallback so the carrier doesn't go empty silently.
    * ``request_id``: legacy alias for client_request_id.

    Returns "" when none of those keys is present. The diagnostic warning
    Phase A burn-down cycle 5 added ("baseline payload had N rows but 0
    carried a question_id/id key") fires precisely when this returns "" for
    every row, so a future row shape with yet another id key will be visible
    immediately rather than producing a silent empty fixture.

    Cycle 8 consolidation: the lookup logic now lives in
    :mod:`genie_space_optimizer.optimization._qid_extraction` and is
    shared with ``ground_truth_corrections._extract_question_id`` so
    the two extractors cannot diverge again. This wrapper preserves
    the string-return contract used throughout the lever loop.
    """
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    qid, _source = extract_question_id(row)
    return qid


def _drain_buffered_action_groups(
    *,
    failed_ag: dict,
    buffered: list[dict],
    reason: str,
) -> tuple[list[dict], list[dict]]:
    """Selectively drop buffered AGs that share affected_questions
    with the failed AG.

    Cycle 9 burndown: three skip paths in ``_run_lever_loop``
    (dead-on-arrival, pre-AG-snapshot-failure, applier-rejection
    no-applied-patches) used to unconditionally clear
    ``pending_action_groups``, discarding every buffered AG even when
    they targeted unrelated clusters. This helper keeps buffered AGs
    whose ``affected_questions`` are disjoint from the failed AG's,
    because the failure of one AG tells us nothing about proposals for
    other clusters.

    Returns ``(survivors, dropped)`` so the caller can log / emit a
    DecisionRecord for the dropped subset.
    """
    failed_qids = {
        str(q)
        for q in (failed_ag.get("affected_questions") or [])
        if str(q)
    }
    survivors: list[dict] = []
    dropped: list[dict] = []
    for ag in buffered or []:
        ag_qids = {
            str(q)
            for q in (ag.get("affected_questions") or [])
            if str(q)
        }
        if failed_qids and ag_qids & failed_qids:
            dropped.append(ag)
        else:
            survivors.append(ag)
    if dropped:
        logger.warning(
            "Selective drain (%s): dropped %d buffered AG(s) overlapping "
            "with %s; %d survived",
            reason,
            len(dropped),
            failed_ag.get("id", "?"),
            len(survivors),
        )
    return survivors, dropped


def _record_dead_on_arrival_signature(
    *,
    seen: set[tuple[str, ...]],
    signature: tuple[str, ...],
    reason: str,
) -> None:
    """Record a dead-on-arrival patch signature, but only if it is
    informative.

    Cycle 9 burndown: the empty tuple ``()`` is never recorded — it
    represents "every candidate patch was dropped before the applier
    saw it" (today only the blast-radius gate causes this). Caching it
    in the dead-on-arrival ledger short-circuits every subsequent
    iteration that computes the same empty signature, blocking the
    strategist from getting another attempt with a different patch
    shape. The right next step is to ask for new shapes, not to mark
    the AG terminally dead.
    """
    if not signature:
        logger.info(
            "Dead-on-arrival ledger skipped empty signature (reason=%s); "
            "strategist will get another attempt.",
            reason,
        )
        return
    seen.add(signature)


def _consume_structural_synthesis_buffer(buffer: list[dict]) -> list[dict]:
    """P3 task 4 — drain the structural-synthesis buffer and return its
    contents.

    Called at the start of each iteration after action-group selection
    so a synthesis proposal queued at the prior iteration's lever-5
    drop becomes a forced AG candidate this iteration. Mutates the
    input buffer (clears it).
    """
    consumed = list(buffer)
    buffer.clear()
    return consumed


def _should_force_structural_synthesis(
    *,
    gate_drop_reason: str,
    cluster_root_cause: str,
) -> bool:
    """P3 — return True when a lever-5 structural gate drop should
    mandatorily invoke cluster-driven synthesis.

    Conditions:
      1. ``GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP`` is on.
      2. Gate drop reason indicates the lever-5 structural gate.
      3. Cluster root cause is in the SQL-shape set (the same
         ``_SQL_SHAPE_ROOT_CAUSES`` used by the gate itself).
    """
    from genie_space_optimizer.common.config import (
        force_structural_synthesis_on_lever5_drop_enabled,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _SQL_SHAPE_ROOT_CAUSES,
    )

    if not force_structural_synthesis_on_lever5_drop_enabled():
        return False
    if not str(gate_drop_reason or "").startswith("lever5_structural"):
        return False
    return str(cluster_root_cause or "") in _SQL_SHAPE_ROOT_CAUSES


_LEVER6_PATCH_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_measure",
    "add_sql_snippet_filter",
    "add_sql_snippet_expression",
})


def _should_force_lever6_proposal(
    *,
    cluster_root_cause: str,
    cluster_recommended_levers: tuple[int, ...],
    ag_target_qids: tuple[str, ...],
    ag_proposals_so_far: list[dict],
) -> bool:
    """Cycle 7 N3 — return True when this AG must receive a forced
    Lever-6 candidate to close the run-to-run variance on SQL-shape
    hard failures.

    All five conditions must hold:

    1. ``GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA`` is on.
    2. ``cluster_root_cause`` ∈ ``_SQL_SHAPE_ROOT_CAUSES`` (the same
       set the lever-5 structural gate and B2 weighted tie-break use).
    3. ``cluster_recommended_levers`` includes 6 (so we never override
       Cycle 2 Task 4's per-question-shape preference of (3, 5)).
    4. ``ag_target_qids`` is non-empty (only force on AGs that target
       hard qids — diagnostic-AG path is not the variance lane).
    5. ``ag_proposals_so_far`` contains zero ``add_sql_snippet_*``
       patches (don't duplicate a Lever-6 the strategist already
       emitted).
    """
    from genie_space_optimizer.common.config import (
        require_lever6_for_sql_shape_rca_enabled,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _SQL_SHAPE_ROOT_CAUSES,
    )

    if not require_lever6_for_sql_shape_rca_enabled():
        return False
    if str(cluster_root_cause or "") not in _SQL_SHAPE_ROOT_CAUSES:
        return False
    if 6 not in tuple(cluster_recommended_levers or ()):
        return False
    if not tuple(ag_target_qids or ()):
        return False
    for proposal in ag_proposals_so_far or ():
        ptype = str((proposal or {}).get("patch_type") or "")
        if ptype in _LEVER6_PATCH_TYPES:
            return False
    return True


_PRODUCTIVE_ITERATION_NO_OP_REASON_CODES: tuple[str, ...] = (
    "proposal_generation_empty",
    "structural_gate_dropped_instruction_only",
    "no_structural_candidate",
)


def _emit_idempotency_key(record: dict) -> tuple:
    """Cycle 6 F-1 — idempotency key for the per-iteration emit-dedup
    set. A record duplicate is detected on::

      (decision_type, reason_code, cluster_id, iteration, proposal_id)

    ``proposal_id`` is included so ``proposal_generated`` records
    (intentionally one-per-proposal) don't collapse, while
    ``iteration_budget_decision``, ``soft_cluster_drift_recovered``,
    and ``rca_regeneration_*`` records (which carry no
    ``proposal_id``) collapse correctly when emitted twice.

    Reproducer: run 833969815458299 emitted two
    ``iteration_budget_consumed`` records and two
    ``soft_cluster_drift_recovered`` records for S001 in iter 1
    because the Cycle 5 emit sites were wired at both the AG-
    materialization loop and the iteration-end consolidation.
    """
    return (
        str(record.get("decision_type") or ""),
        str(record.get("reason_code") or ""),
        str(record.get("cluster_id") or ""),
        int(record.get("iteration") or 0),
        str(record.get("proposal_id") or ""),
    )


def _emit_diagnostic_ag_trunk_events(
    *,
    journey_emit,
    cluster_qids: tuple[str, ...],
    cluster_id: str,
) -> None:
    """Cycle 6 F-7 — emit a ``diagnostic_ag`` trunk event for every
    qid in a cluster being routed to a diagnostic AG (whether the AG
    succeeds, exhausts via T3 RCA regen, or is retired). The
    classifier in ``question_journey_contract._classify_terminal_state``
    uses the diagnostic_ag stage to pick HARD_FAILURE_UNRESOLVED over
    the misleading TERMINAL_UNACTIONABLE state. Run 833969815458299
    misclassified gs_021 because the T3 regen path emitted
    ``rca_regeneration_*`` decision records but no ``diagnostic_ag``
    trunk event.
    """
    for q in cluster_qids or ():
        try:
            journey_emit(
                "diagnostic_ag",
                question_id=str(q),
                cluster_id=str(cluster_id),
            )
        except Exception:
            logger.debug(
                "F-7: diagnostic_ag trunk emit failed (non-fatal)",
                exc_info=True,
            )


def _regenerate_rca_for_cluster(
    *,
    spark,
    run_id: str,
    cluster: dict,
    metadata_snapshot: dict,
) -> dict:
    """Cycle 5 T3 / Cycle 6 F-2 — re-invoke RCA on a single cluster
    with broader evidence after the diagnostic-AG path flagged it as
    ``needs_rca_regeneration``.

    Two attempts in priority order:

      1. ``failure_buckets``: bucket signature for cluster qids.
      2. ``asi``: ASI-mined hints for cluster qids.

    Returns ``{"rca_id": str, "attempted_sources": tuple[str, ...]}``.
    The T3 emit site uses ``attempted_sources`` so the
    ``rca_regeneration_exhausted`` record reflects what was actually
    tried (run 833969815458299 emitted ``attempted_evidence_sources=[]``
    which is wrong — both packs were available).
    """
    from genie_space_optimizer.optimization.rca import build_rca_card

    _ = spark, run_id  # accepted for future LLM wiring; not used today
    cluster_id = str(cluster.get("primary_cluster_id") or "")
    qids = tuple(str(q) for q in (cluster.get("target_qids") or ()))
    attempted: list[str] = []

    attempted.append("failure_buckets")
    card = build_rca_card(
        cluster_id=cluster_id,
        qids=qids,
        failure_buckets=metadata_snapshot.get("_failure_buckets") or {},
        asi_metadata={},
    )
    if card and str(card.get("rca_id") or ""):
        return {
            "rca_id": str(card["rca_id"]),
            "attempted_sources": tuple(attempted),
        }

    attempted.append("asi")
    card = build_rca_card(
        cluster_id=cluster_id,
        qids=qids,
        failure_buckets=metadata_snapshot.get("_failure_buckets") or {},
        asi_metadata=metadata_snapshot.get("_asi_metadata") or {},
    )
    if card and str(card.get("rca_id") or ""):
        return {
            "rca_id": str(card["rca_id"]),
            "attempted_sources": tuple(attempted),
        }

    return {"rca_id": "", "attempted_sources": tuple(attempted)}


def _classify_iteration_no_op_cause(
    records: list[dict] | None,
) -> str:
    """Cycle 5 T1 — return the most recent typed P4 no-progress reason
    code from the iteration's decision records, or empty string when
    none of the typed P4 outcomes were emitted.

    The classifier walks the iteration's local decision-record
    accumulator from newest to oldest so the most recent typed
    no-progress reason wins. Used by the productive-iteration budget
    accounting site to decide whether the iteration's no-op is a
    deterministic-skip-worthy cause.
    """
    if not records:
        return ""
    typed = _PRODUCTIVE_ITERATION_NO_OP_REASON_CODES
    for rec in reversed(records):
        rc = str(rec.get("reason_code") or "")
        if rc in typed:
            return rc
    return ""


def _compute_selected_proposal_signature(
    proposals: list[dict] | None,
) -> tuple[str, ...]:
    """Cycle 2 Task 3 — stable signature over selected (not applied)
    proposal IDs. ``_selected_patch_signature`` is keyed on applied
    patches and therefore empty when blast-radius drops everything;
    this signature stays informative in that case.

    P2: prefer ``expanded_patch_id`` over ``proposal_id`` so two
    patches under the same parent but different levers (e.g.
    ``L1:P001#2`` and ``L5:P001#2``) produce distinct signature
    entries. Without this, the DOA dedup ledger silently conflates
    cross-lever collisions on the legacy unqualified ``proposal_id``.
    """
    if not proposals:
        return ()
    ids = sorted(
        str(p.get("expanded_patch_id") or p.get("proposal_id") or p.get("id") or "")
        for p in proposals
        if (p.get("expanded_patch_id") or p.get("proposal_id") or p.get("id"))
    )
    return tuple(i for i in ids if i)


def _record_doa_selected_signature(
    *,
    seen: dict[str, set[tuple[str, ...]]],
    ag_id: str,
    signature: tuple[str, ...],
) -> None:
    """Cycle 2 Task 3 — record a selected-proposal-ID signature on a
    per-AG ledger. No-ops when the flag is off or when the signature
    is empty.
    """
    from genie_space_optimizer.common.config import (
        doa_selected_proposal_signature_enabled,
    )

    if not doa_selected_proposal_signature_enabled():
        return
    if not signature:
        return
    seen.setdefault(str(ag_id), set()).add(signature)


def _is_doa_selected_signature_blocked(
    *,
    seen: dict[str, set[tuple[str, ...]]],
    ag_id: str,
    signature: tuple[str, ...],
) -> bool:
    """Cycle 2 Task 3 — return True when the given AG has already
    been retired with the same selected-proposal-ID signature."""
    from genie_space_optimizer.common.config import (
        doa_selected_proposal_signature_enabled,
    )

    if not doa_selected_proposal_signature_enabled():
        return False
    if not signature:
        return False
    return signature in seen.get(str(ag_id), set())


def _t24_counterfactual_scan(
    *,
    all_proposals: list[dict],
    benchmarks: list[dict],
    ag: dict,
    prev_failure_qids: set[str] | frozenset[str],
) -> list[tuple[str, str, list[str]]]:
    """Stamp passing_dependents and high_collateral_risk on each proposal.

    Phase 3c Task A: every proposal the scan visits is stamped with
    ``passing_dependents`` (possibly the empty list) so downstream gates
    can distinguish "scan ran, found nothing" from "scan never ran".
    Proposals without a ``target`` table are stamped with the empty
    list; instruction rewrites belong to that bucket today and the
    instruction-scope gate's split-child propagation check needs the
    explicit ``[]`` to stop failing loud.

    Returns the legacy ``_collateral_details`` list of
    ``(patch_type, target, sample_dependents)`` tuples for the operator
    transcript renderer.
    """
    passing_qids = {b.get("id") for b in benchmarks if b.get("id")} - set(
        prev_failure_qids or ()
    )
    affected_qids = set(ag.get("affected_questions", []) or [])
    affected_n = max(len(affected_qids), 1)
    collateral_details: list[tuple[str, str, list[str]]] = []

    def _sql_text_for_benchmark(_b: dict) -> str:
        return " ".join(
            str(_b.get(k, "")) for k in
            ("expected_response", "expected_sql", "ground_truth_sql")
        ).lower()

    for _p in all_proposals:
        _ptype = str(_p.get("type") or _p.get("patch_type") or "")
        _target = str(
            _p.get("target") or _p.get("target_object")
            or _p.get("target_table") or _p.get("table") or ""
        ).lower()
        _target_column = str(_p.get("column") or "").lower()

        # Phase 3c Task A: always stamp passing_dependents so split-children
        # of any rewrite_instruction inherit a real value (even if []).
        if not _target:
            _p["passing_dependents"] = []
            continue

        _target_tail = _target.split(".")[-1] if "." in _target else _target
        _target_candidates = {_target, _target_tail}
        if _target_column:
            _target_candidates.add(f"{_target_tail}.{_target_column}")
        _dependents: list[str] = []
        for _b in benchmarks:
            _bid = _b.get("id", "")
            if not _bid or _bid not in passing_qids:
                continue
            _bench_assets = [
                str(t).lower() for t in
                (_b.get("required_tables") or []) + (_b.get("required_columns") or [])
            ]
            _matched = any(
                any(c == _ba or c in _ba or _ba in c for c in _target_candidates)
                for _ba in _bench_assets
            )
            if not _matched:
                _sql_text = _sql_text_for_benchmark(_b)
                if _sql_text and any(
                    c and c in _sql_text for c in _target_candidates
                ):
                    _matched = True
            if _matched:
                _dependents.append(_bid)
        # Phase 3c Task A: stamp even when empty so the gate sees [] not None.
        _p["passing_dependents"] = _dependents[:50]
        if _dependents and len(_dependents) >= 2 * affected_n:
            _p["high_collateral_risk"] = True
            collateral_details.append(
                (_ptype, _target, _dependents[:10])
            )
            # Phase 3c Task C: per-proposal threshold log for follow-up
            # diagnosis when the gate fires unexpectedly.
            logger.info(
                "[%s] high_collateral_risk: proposal_id=%s target=%s "
                "dependents=%s threshold=%d",
                ag.get("id", "?"),
                _p.get("proposal_id") or _p.get("id") or "?",
                _target,
                _dependents,
                2 * affected_n,
            )
    return collateral_details


def _seed_eval_result_from_baseline_iter(baseline_iter: dict | None) -> dict:
    """Build an `_latest_eval_result`-shaped dict from a persisted baseline row.

    Reads `baseline_iter["rows_json"]` (the persisted per-question eval result
    rows from iter_00) and returns the carrier shape:
    ``{question_ids, scores, arbiter_verdicts, failure_question_ids}``.

    Returns ``{}`` when `baseline_iter` is None, has no `rows_json`, or
    `rows_json` contains no rows with extractable question IDs (delegated to
    `_baseline_row_qid`, which knows the realistic key aliases). Caller treats
    `{}` as "no baseline data available" and falls through.

    Centralized so the carrier seed at `_run_lever_loop` setup AND the
    iteration-snapshot fallback at iteration-start can share the same logic.
    Without the snapshot fallback, runs where every iteration short-circuits
    before `_run_gate_checks` (e.g. all patches dropped by the applier
    blast-radius gate, dead-on-arrival AG retry blocked) produce empty
    `eval_rows` in the replay fixture.

    Correctness defaulting: rows that don't carry any of the
    `_eval_rc_str`-recognised keys default to ``"no"`` (treated as a
    failure). This is intentional — the replay fixture only requires that
    `eval_rows` be populated; the canonical correctness for those rows is
    refreshed by every gate result via `_extract_eval_result_from_gate`,
    so a baseline-only "no" defaults are corrected the moment the loop
    actually evaluates anything.
    """
    if not isinstance(baseline_iter, dict) or not baseline_iter:
        return {}
    rows_json = baseline_iter.get("rows_json")
    if isinstance(rows_json, str):
        try:
            rows_json = json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return {}
    if not isinstance(rows_json, list) or not rows_json:
        return {}
    rows = [r for r in rows_json if isinstance(r, dict)]
    if not rows:
        return {}
    qids: list[str] = []
    scores: dict[str, str] = {}
    arbiter: dict[str, str] = {}
    failures: list[str] = []
    for r in rows:
        qid = _baseline_row_qid(r)
        if not qid:
            continue
        qids.append(qid)
        rc = _eval_rc_str(r)
        scores[qid] = "yes" if rc in ("yes", "true", "1", "pass") else "no"
        arb = _eval_arbiter_str(r)
        if arb:
            arbiter[qid] = arb
        if scores[qid] == "no":
            failures.append(qid)
    if not qids:
        return {}
    return {
        "question_ids": qids,
        "scores": scores,
        "arbiter_verdicts": arbiter,
        "failure_question_ids": failures,
    }


def _merge_bug4_counters(eval_result: dict) -> dict:
    """Inject Bug #4 (benchmark leakage) counters into an eval_result before
    it is written via ``write_iteration`` for the 'full' scope.

    Reads the module-level counters from ``optimizer._BUG4_COUNTERS`` and
    resets them. Intended to be called once per iteration, at the
    ``eval_scope="full"`` write site, so each iteration row carries the
    counters observed since the prior iteration.

    For 'slice' / 'p0' intermediate evals we deliberately leave counters
    untouched so the 'full' write aggregates them. If leakage is suppressed
    on a slice attempt and the iteration gets rolled back, the counts still
    surface on the full write.
    """
    try:
        from genie_space_optimizer.optimization.optimizer import (
            get_bug4_counters, reset_bug4_counters,
        )
        # Cycle 8 Bug 1 Phase 3b Task B: reset the Lever 5 gate-drop
        # ledger alongside Bug-4 counters since they share an
        # iteration-scoped lifecycle.
        try:
            from genie_space_optimizer.optimization.optimizer import (
                reset_lever5_gate_drops as _reset_lever5_gate_drops,
            )
        except ImportError:
            _reset_lever5_gate_drops = None
    except ImportError:
        return eval_result
    snapshot = get_bug4_counters()
    eval_result = dict(eval_result)
    # The shape-by-type maps are collected elsewhere (see
    # count_example_sql_leaks and firewall_rejection_count_by_type); here we
    # fold in the flat counter for secondary-mining blocks and a summary
    # entry for firewall rejections.
    eval_result.setdefault("secondary_mining_blocked", 0)
    eval_result["secondary_mining_blocked"] = (
        int(eval_result.get("secondary_mining_blocked") or 0)
        + int(snapshot.get("secondary_mining_blocked", 0) or 0)
    )
    # Cycle 8 Bug 1 Phase 3b Task A — surface the Lever 5 structural
    # gate counter so the iteration banner can render it. The gate
    # increments _BUG4_COUNTERS["lever5_text_only_blocked"] at
    # optimizer.py:13962 every time it drops an instruction-only
    # proposal whose dominant cluster root cause is SQL-shape; without
    # this fold, future cycles can hit the gate hundreds of times
    # invisibly.
    eval_result.setdefault("lever5_text_only_blocked", 0)
    eval_result["lever5_text_only_blocked"] = (
        int(eval_result.get("lever5_text_only_blocked") or 0)
        + int(snapshot.get("lever5_text_only_blocked", 0) or 0)
    )
    existing_fw = eval_result.get("firewall_rejection_count_by_type") or {}
    if not isinstance(existing_fw, dict):
        existing_fw = {}
    flat_rejections = int(snapshot.get("firewall_rejections", 0) or 0)
    if flat_rejections:
        existing_fw = dict(existing_fw)
        existing_fw["_total"] = int(existing_fw.get("_total", 0)) + flat_rejections
    eval_result["firewall_rejection_count_by_type"] = existing_fw
    reset_bug4_counters()
    if _reset_lever5_gate_drops is not None:
        _reset_lever5_gate_drops()
    return eval_result


_PATCH_TYPE_LABELS: dict[str, str] = {
    "add_instruction": "Add Instruction",
    "update_instruction": "Update Instruction",
    "remove_instruction": "Remove Instruction",
    "rewrite_instruction": "Rewrite Instruction",
    "add_example_sql": "Add Example SQL",
    "update_example_sql": "Update Example SQL",
    "remove_example_sql": "Remove Example SQL",
    "add_description": "Add Table Description",
    "update_description": "Update Table Description",
    "add_column_description": "Add Column Description",
    "update_column_description": "Update Column Description",
    "add_column_synonym": "Add Column Synonym",
    "add_join_spec": "Add Join Spec",
    "update_join_spec": "Update Join Spec",
    "remove_join_spec": "Remove Join Spec",
    "update_tvf_sql": "Update TVF SQL",
}


def _compute_lever_efficacy_prior(
    reflection_buffer: list[dict],
) -> dict:
    """T2.3: Summarise lever efficacy from the current run's history.

    Returns a dict keyed on ``"lever_N"`` with stats:
      - ``attempts``:        number of times this lever was tried
      - ``accepted``:        number of attempts that passed the gate
      - ``acceptance_rate``: accepted / attempts
      - ``mean_delta``:      mean accuracy_delta across all attempts
      - ``examples``:        up to 3 recent (iteration, accepted, delta,
                             root_cause) tuples for the strategist prompt

    The summary is computed in-memory from the reflection buffer; a
    future iteration can persist this across runs as
    ``genie_opt_lever_efficacy`` (Delta table) and seed the strategist
    with cross-run priors. For now it makes the current-run evidence
    visible which is already more signal than the strategist has today.
    """
    stats: dict[str, dict] = {}
    for entry in reflection_buffer:
        _levers = entry.get("lever_set") or entry.get("levers") or []
        if not _levers:
            continue
        _accepted = bool(entry.get("accepted"))
        _delta = float(entry.get("accuracy_delta", 0.0) or 0.0)
        _rc = str(entry.get("root_cause", ""))[:40]
        _iter = entry.get("iteration")
        for _l in _levers:
            try:
                _key = f"lever_{int(_l)}"
            except (TypeError, ValueError):
                continue
            bucket = stats.setdefault(
                _key,
                {"attempts": 0, "accepted": 0, "delta_sum": 0.0, "examples": []},
            )
            bucket["attempts"] += 1
            if _accepted:
                bucket["accepted"] += 1
            bucket["delta_sum"] += _delta
            bucket["examples"].append({
                "iteration": _iter,
                "accepted": _accepted,
                "accuracy_delta": round(_delta, 1),
                "root_cause": _rc,
            })
    # Post-process: compute rates + trim examples.
    out: dict[str, dict] = {}
    for _key, b in stats.items():
        _att = max(b["attempts"], 1)
        out[_key] = {
            "attempts": b["attempts"],
            "accepted": b["accepted"],
            "acceptance_rate": round(b["accepted"] / _att, 3),
            "mean_delta": round(b["delta_sum"] / _att, 2),
            "examples": b["examples"][-3:],
        }
    return out


def _compute_eval_variance(
    full_result_1: dict,
    full_result_2: dict | None,
) -> dict:
    """T0.2: Estimate run-to-run variance of two confirmation evals.

    Compares the per-question pre-arbiter verdicts from two evaluation
    runs against the same Genie space. Returns a dict with:

    - ``disagreed_qids``: list[str] — questions that flipped pass<->fail.
    - ``disagreement_ratio``: float in [0,1] — |disagreed| / total_scored.
    - ``total_scored``: int — questions scored in both runs.
    - ``mean_pre_arbiter_acc``: float — average pre-arbiter rc accuracy.

    When ``full_result_2`` is ``None`` (confirmation skipped because the
    first run already improved) the helper returns zeros — variance
    cannot be estimated from a single run and the gate falls back to the
    legacy single-run comparison.
    """
    if not full_result_2:
        return {
            "disagreed_qids": [],
            "disagreement_ratio": 0.0,
            "total_scored": 0,
            "mean_pre_arbiter_acc": float(
                full_result_1.get("pre_arbiter_accuracy",
                                  full_result_1.get("overall_accuracy", 0.0))
            ),
        }

    def _pre_arbiter_verdicts(fr: dict) -> dict[str, bool]:
        """Build ``qid -> pass`` map using raw result_correctness only."""
        out: dict[str, bool] = {}
        for row in fr.get("rows", []) or []:
            rq = row.get("request") or {}
            if isinstance(rq, str):
                try:
                    rq = json.loads(rq)
                except (json.JSONDecodeError, TypeError):
                    rq = {}
            rqk = rq.get("kwargs", {}) if isinstance(rq, dict) else {}
            qid = str(
                row.get("inputs/question_id")
                or (row.get("inputs") or {}).get("question_id", "")
                or row.get("question_id")
                or rqk.get("question_id")
                or (rq.get("question_id") if isinstance(rq, dict) else None)
                or ""
            )
            if not qid:
                continue
            rc = str(
                row.get("result_correctness/value", row.get("result_correctness", ""))
            ).lower()
            if rc == "excluded":
                continue
            err_type = str(
                row.get("outputs/comparison/error_type")
                or row.get("comparison/error_type")
                or row.get("comparison.error_type")
                or ""
            ).lower()
            if err_type in ("both_empty", "genie_result_unavailable"):
                continue
            out[qid] = rc in ("yes", "true", "1", "1.0")
        return out

    v1 = _pre_arbiter_verdicts(full_result_1)
    v2 = _pre_arbiter_verdicts(full_result_2)
    shared = set(v1) & set(v2)
    disagreed = sorted(qid for qid in shared if v1[qid] != v2[qid])
    total_scored = len(shared)
    ratio = len(disagreed) / total_scored if total_scored else 0.0
    mean_pre = (
        float(full_result_1.get("pre_arbiter_accuracy", 0.0))
        + float(full_result_2.get("pre_arbiter_accuracy", 0.0))
    ) / 2.0
    return {
        "disagreed_qids": disagreed,
        "disagreement_ratio": ratio,
        "total_scored": total_scored,
        "mean_pre_arbiter_acc": mean_pre,
    }


def _paired_question_test(
    prev_failures: set[str],
    new_failures: set[str],
) -> dict:
    """T0.2: Paired per-question sign test on pre-arbiter verdicts.

    Given the set of failing question ids before and after a patch
    application, compute:
      - ``flipped_to_pass``:  questions failing before but passing after
      - ``flipped_to_fail``:  questions passing before but failing after
      - ``stable_fail``:      failing in both
      - ``net_improvement``:  flipped_to_pass - flipped_to_fail
      - ``significant``:      True iff the sign of the difference is
        unambiguous given the minimum-effect threshold. For our typical
        20–30 row corpus we accept ``net_improvement >= 2`` as
        significant, and ``net_improvement >= max(3, K * 0.15)`` for
        larger corpora. This is a cheap stand-in for a real binomial
        test; once we're off the tiny-corpus regime we can tighten it
        to a proper McNemar computation.
    """
    _flipped_to_pass = prev_failures - new_failures
    _flipped_to_fail = new_failures - prev_failures
    _stable_fail = prev_failures & new_failures
    net = len(_flipped_to_pass) - len(_flipped_to_fail)
    _total_touched = len(_flipped_to_pass) + len(_flipped_to_fail)
    # Minimum-effect threshold — on a 22-row corpus even a 2-question
    # swing (~9%) is outside the run-to-run variance we've observed, so
    # require net >= 2 or net <= -2. On larger corpora demand a larger
    # effect.
    _min_effect = max(2, int(round(_total_touched * 0.15)))
    significant = abs(net) >= _min_effect
    return {
        "flipped_to_pass": sorted(_flipped_to_pass),
        "flipped_to_fail": sorted(_flipped_to_fail),
        "stable_fail": sorted(_stable_fail),
        "net_improvement": net,
        "significant": significant,
        "min_effect": _min_effect,
    }


def _rows_from_iteration_payload(iteration_row: dict | None) -> list[dict]:
    """Decode the ``rows_json`` payload of a persisted iteration row."""
    if not iteration_row:
        return []
    rows_json = iteration_row.get("rows_json")
    if isinstance(rows_json, str):
        try:
            rows_json = json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            rows_json = []
    if not isinstance(rows_json, list):
        return []
    return [row for row in rows_json if isinstance(row, dict)]


_COLUMN_TARGETED_PATCH_TYPES = frozenset({
    "update_column_description",
    "add_column_synonym",
})


def _classify_proposal_log_status(proposal: dict) -> str:
    """Return the operator-facing status for the proposal log block.

    Earlier code looked only at the rationale ("not valid JSON" / "non-JSON")
    so a proposal with a malformed column target showed ``Status: OK`` even
    though it would be silently dropped by ``applier.proposals_to_patches``.
    This helper makes invalid column targets visible at log time.
    """
    rationale = str(proposal.get("rationale") or "")
    if "not valid JSON" in rationale or "non-JSON" in rationale.lower():
        return "FAILED (non-JSON)"
    ptype = str(proposal.get("type") or proposal.get("patch_type") or "")
    if ptype in _COLUMN_TARGETED_PATCH_TYPES:
        from genie_space_optimizer.optimization.applier import (
            _single_column_target,
        )

        col = _single_column_target(
            proposal.get("column")
            or proposal.get("column_name")
            or proposal.get("target_column")
            or proposal.get("target")
        )
        if not col:
            return "INVALID_TARGET"
    return "OK"


def _iteration_label(counter: int) -> str:
    """T3.17: Unified label for iteration_counter in log banners.

    The lever-loop's ``iteration_counter`` is 0-indexed (0 is the first
    loop body iteration, attempt #1). Historically some banners printed
    the raw counter ("Iteration 0", "Iteration 1") while others printed
    ``counter + 1`` ("Iteration 1", "Iteration 2"), and operators had
    no reliable way to tell which one they were reading. This helper
    renders a single canonical form: ``index 0 / attempt 1``.
    """
    try:
        idx = int(counter)
    except (TypeError, ValueError):
        idx = 0
    attempt = idx + 1
    return f"index {idx} / attempt {attempt}"


def _fmt_patch(idx: int, patch: dict, action: dict, entry: dict | None = None) -> str:
    """Format a single applied patch into a readable multi-line string.

    T2.13: when ``entry`` is provided and carries a distinct
    ``applied_patch_type`` (i.e. the applier transformed the proposal,
    as with the rewrite_instruction downgrade splitter), the header
    prints both types so the log is honest about what actually ran.
    """
    ptype = patch.get("type", action.get("action_type", "?"))
    applied_ptype = None
    applied_detail = None
    if entry:
        applied_ptype = entry.get("applied_patch_type")
        applied_detail = entry.get("applied_patch_detail")
    label = _PATCH_TYPE_LABELS.get(ptype, ptype)
    table = patch.get("table") or patch.get("target") or ""
    column = patch.get("column", "")
    target = f"{table}.{column}" if column else table

    if applied_ptype and applied_ptype != ptype:
        applied_label = _PATCH_TYPE_LABELS.get(applied_ptype, applied_ptype)
        lines = [f"|  [{idx}] {label} -> {applied_label} (T2.13 applier-side transform)"]
    else:
        lines = [f"|  [{idx}] {label}"]
    if target:
        lines.append(f"|      Target: {target}")
    if applied_detail:
        lines.append(f"|      Applied: {applied_detail}")

    struct = patch.get("structured_sections") or {}
    if struct and isinstance(struct, dict):
        for sk, sv in struct.items():
            sv_flat = str(sv).replace("\n", " ")[:100]
            lines.append(f"|      {sk}: {sv_flat}")
    else:
        new_text = patch.get("new_text", "")
        if new_text:
            lines.append(f"|      Value: {new_text.replace(chr(10), ' ')[:120]}")

    eq = patch.get("example_question", "")
    esql = patch.get("example_sql", "")
    if eq:
        lines.append(f"|      Question: {eq[:100]}")
    if esql:
        lines.append(f"|      SQL: {esql[:100]}")

    js = patch.get("join_spec")
    if js and isinstance(js, dict):
        left = js.get("left", {}).get("identifier", "?")
        right = js.get("right", {}).get("identifier", "?")
        sql_cond = (js.get("sql") or ["?"])[0][:80]
        lines.append(f"|      Join: {left} <-> {right}")
        lines.append(f"|        ON {sql_cond}")

    return "\n".join(lines)


def _scorecard(scores: dict[str, float], prefix: str = "|  ") -> str:
    parts = [f"{j}={v:.1f}" for j, v in sorted(scores.items())]
    line = "  ".join(parts)
    return f"{prefix}Scores:  {line}"


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_sql_context(spark: SparkSession, catalog: str, schema: str) -> None:
    """Set Spark SQL catalog/schema context explicitly for SQL Connect stability."""
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")


# ── Result Dataclass ──────────────────────────────────────────────────


class FailedRollbackVerification(RuntimeError):
    """Raised after rollback leaves live Genie config in an untrusted state.

    Task 2 — once the parsed-config rollback verifier reports a real
    mismatch we cannot continue the run safely: subsequent AGs would
    re-cluster against a polluted live space. Raise terminally; the run
    is marked ``FAILED`` with ``convergence_reason="failed_rollback_verification"``.
    """


@dataclass
class OptimizationResult:
    """Outcome of an optimization run (used by convenience function)."""

    run_id: str
    space_id: str
    domain: str
    status: str  # CONVERGED | STALLED | MAX_ITERATIONS | FAILED
    best_iteration: int
    best_accuracy: float
    best_repeatability: float
    best_model_id: str | None
    convergence_reason: str | None
    total_iterations: int
    levers_attempted: list[int] = field(default_factory=list)
    levers_accepted: list[int] = field(default_factory=list)
    levers_rolled_back: list[int] = field(default_factory=list)
    final_scores: dict[str, float] = field(default_factory=dict)
    experiment_name: str = ""
    experiment_id: str = ""
    report_path: str | None = None
    error: str | None = None


# ── Error Handling ────────────────────────────────────────────────────


def _safe_stage(
    state_spark: Any,
    run_id: str,
    stage_name: str,
    fn: Any,
    state_catalog: str,
    state_schema: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Wrap a stage function — on exception write FAILED to Delta and re-raise."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("Stage %s FAILED for run %s", stage_name, run_id)
        try:
            write_stage(
                state_spark, run_id, stage_name, "FAILED",
                error_message=err_msg[:500],
                catalog=state_catalog, schema=state_schema,
            )
            update_run_status(
                state_spark, run_id, state_catalog, state_schema,
                status="FAILED",
                convergence_reason=f"error_in_{stage_name}",
            )
        except Exception:
            logger.exception("Failed to write FAILED state for %s", run_id)
        raise


# ── Stage 1: PREFLIGHT ───────────────────────────────────────────────


def _run_preflight(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    experiment_name: str | None = None,
    apply_mode: str = "genie_config",
    warehouse_id: str = "",
) -> dict:
    """Stage 1: Fetch config, UC metadata, generate/load benchmarks, create experiment.

    Returns a dict of task values to pass downstream.
    """
    warehouse_id = resolve_warehouse_id(warehouse_id)
    config, benchmarks, model_id, exp_name, human_corrections = _safe_stage(
        spark, run_id, "PREFLIGHT", run_preflight,
        catalog, schema,
        w, spark, run_id, space_id, catalog, schema, domain, experiment_name,
        apply_mode, warehouse_id,
    )

    import mlflow
    exp = mlflow.get_experiment_by_name(exp_name)
    experiment_id = exp.experiment_id if exp else ""

    update_run_status(
        spark, run_id, catalog, schema,
        status="IN_PROGRESS",
        experiment_name=exp_name,
        experiment_id=experiment_id,
    )

    iq_scan_recommended_levers = (
        config.get("_gso_iq_scan_recommended_levers", [])
        if isinstance(config, dict) else []
    )
    iq_scan_summary = (
        config.get("_gso_iq_scan_summary")
        if isinstance(config, dict) else None
    )

    return {
        "benchmarks": benchmarks,
        "config": config,
        "model_id": model_id,
        "experiment_name": exp_name,
        "experiment_id": experiment_id,
        "human_corrections": human_corrections,
        "iq_scan_recommended_levers": iq_scan_recommended_levers,
        "iq_scan_summary": iq_scan_summary,
    }


# ── Stage 2: BASELINE EVAL ──────────────────────────────────────────


def _build_predict_and_scorers(
    w: WorkspaceClient,
    spark: SparkSession,
    space_id: str,
    catalog: str,
    schema: str,
    exp_name: str,
    model_id: str | None,
    domain: str = "",
    *,
    banner_title: str = "BASELINE — EVALUATION SETUP",
) -> dict:
    """Build predict_fn + scorers for a Genie space evaluation.

    No side effects on ``genie_opt_stages`` — callers own stage lifecycle.
    Returns the same dict shape that ``baseline_setup_scorers`` returns so
    downstream code (``run_evaluation`` / ``baseline_run_evaluation``) is
    unchanged.
    """
    _ensure_sql_context(spark, catalog, schema)

    _instr_prompt = format_mlflow_template(
        INSTRUCTION_PROMPT_NAME_TEMPLATE,
        uc_schema=f"{catalog}.{schema}", space_id=space_id,
    )
    predict_fn = make_predict_fn(
        w, space_id, spark, catalog, schema,
        warehouse_id=resolve_warehouse_id(""),
        instruction_prompt_name=_instr_prompt,
    )

    from genie_space_optimizer.common.genie_client import fetch_space_config as _fetch_cfg
    try:
        _bl_config = _fetch_cfg(w, space_id)
        _bl_parsed = _bl_config.get("_parsed_space", _bl_config)
        _bl_instr = _bl_parsed.get("instructions", {}) if isinstance(_bl_parsed, dict) else {}
        _bl_instr_text = _bl_instr.get("text_instructions", "") if isinstance(_bl_instr, dict) else ""
    except Exception:
        _bl_instr_text = ""
    scorers = make_all_scorers(w, spark, catalog, schema, instruction_context=_bl_instr_text)

    _lines = [_section(banner_title, "-")]
    _lines.append(_kv("Space ID", space_id))
    _lines.append(_kv("Model ID", model_id))
    _lines.append(_kv("Experiment", exp_name))
    _lines.append(_kv("Scorers", len(scorers)))
    _lines.append(_kv("Instruction context", f"{len(_bl_instr_text)} chars" if _bl_instr_text else "(none)"))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return {
        "predict_fn": predict_fn,
        "scorers": scorers,
        "model_id": model_id,
        "exp_name": exp_name,
        "space_id": space_id,
        "domain": domain,
    }


def baseline_setup_scorers(
    w: WorkspaceClient,
    spark: SparkSession,
    space_id: str,
    run_id: str,
    catalog: str,
    schema: str,
    exp_name: str,
    model_id: str | None,
    domain: str = "",
) -> dict:
    """Sub-step 2a: Create predict function and scorers. Writes STARTED stage.

    Thin wrapper: emits ``BASELINE_EVAL_STARTED / STARTED`` and delegates
    the actual setup to :func:`_build_predict_and_scorers`. Callers running
    a non-baseline eval (e.g. post-enrichment) should call the helper
    directly and own their own stage lifecycle so they don't leak an
    unclosed ``BASELINE_EVAL_STARTED`` row that pins Step 2 to "Running".
    """
    write_stage(
        spark, run_id, "BASELINE_EVAL_STARTED", "STARTED",
        task_key="baseline_eval", catalog=catalog, schema=schema,
    )
    return _build_predict_and_scorers(
        w, spark, space_id, catalog, schema, exp_name, model_id, domain,
    )


def baseline_run_evaluation(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    benchmarks: list[dict],
    setup_ctx: dict,
    w: WorkspaceClient | None = None,
    model_creation_kwargs: dict | None = None,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
) -> dict:
    """Sub-step 2b: Run 9-judge evaluation with retry."""
    _ensure_sql_context(spark, catalog, schema)
    # Tier 4: v2 name — ``<run_short>/baseline``.
    from genie_space_optimizer.common.mlflow_names import (
        baseline_run_name,
        default_tags as _v2_tags_baseline,
    )
    eval_result = _safe_stage(
        spark, run_id, "BASELINE_EVAL", run_evaluation,
        catalog, schema,
        setup_ctx["space_id"], setup_ctx["exp_name"], 0, benchmarks,
        setup_ctx["domain"], setup_ctx.get("model_id"), "full",
        setup_ctx["predict_fn"], setup_ctx["scorers"],
        spark=spark, w=w, catalog=catalog, gold_schema=schema,
        uc_schema=f"{catalog}.{schema}",
        model_creation_kwargs=model_creation_kwargs,
        max_benchmark_count=max_benchmark_count,
        run_name=baseline_run_name(run_id),
        extra_tags=_v2_tags_baseline(
            run_id,
            space_id=str(setup_ctx.get("space_id", "")),
            stage="baseline", iteration=0,
        ),
    )
    return eval_result


def baseline_display_scorecard(
    eval_result: dict,
    thresholds: dict[str, float] | None = None,
) -> dict:
    """Sub-step 2c: Print per-judge scorecard and return scores summary."""
    _thresholds = thresholds or DEFAULT_THRESHOLDS
    scores = eval_result.get("scores", {})
    overall = eval_result.get("overall_accuracy", 0.0)
    thresholds_met = eval_result.get("thresholds_met", False)

    _lines = [_section("BASELINE EVALUATION — 9-JUDGE SCORECARD", "-")]
    _lines.append(_kv("Overall accuracy", f"{overall:.1f}%"))
    _lines.append("")
    _lines.append(f"  {'Judge':<28s} {'Score':>8s}  {'Threshold':>10s}  {'Status'}")
    for judge in sorted(_thresholds.keys()):
        score_val = scores.get(judge)
        threshold_val = _thresholds[judge]
        if score_val is not None:
            passed = score_val >= threshold_val if threshold_val > 0 else True
            status = "PASS" if passed else "FAIL  <--"
            t_str = f"{threshold_val:.1f}%" if threshold_val > 0 else "--"
            _lines.append(f"  {judge:<28s} {score_val:>7.1f}%  {t_str:>10s}  {status}")
    _lines.append("")
    _lines.append(_kv("Thresholds met", thresholds_met))
    _lines.append(_kv("Eval attempts", eval_result.get("harness_retry_count", 0) + 1))
    _lines.append(_kv("Quarantined questions", eval_result.get("invalid_benchmark_count", 0)))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return {
        "scores": scores,
        "overall_accuracy": overall,
        "thresholds_met": thresholds_met,
    }


def baseline_persist_state(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    model_id: str,
    catalog: str,
    schema: str,
    eval_result: dict,
    scorecard: dict,
) -> dict:
    """Sub-step 2d: Write iteration, link scores, log expectations."""
    scores = scorecard["scores"]
    thresholds_met = scorecard["thresholds_met"]

    eval_result = _merge_bug4_counters(eval_result)
    write_iteration(
        spark, run_id, 0, eval_result,
        catalog=catalog, schema=schema,
        eval_scope="full", model_id=model_id,
    )

    # Tier 1.7: anchor ``best_accuracy`` to the stricter of
    # ``overall_accuracy`` and ``both_correct_rate``. ``overall_accuracy``
    # can be inflated when Genie's SQL is semantically wrong but happens
    # to return the same row set as GT (rc=yes); the arbiter flags those
    # rows as ``ground_truth_correct``. Anchoring to ``both_correct_rate``
    # ensures later iterations can't be rejected by an artificially high
    # ceiling (the "ghost ceiling" regression loop).
    _overall_acc = float(eval_result.get("overall_accuracy", 0.0) or 0.0)
    _both_correct_rate = eval_result.get("both_correct_rate")
    if _both_correct_rate is not None:
        _anchored_best = min(_overall_acc, float(_both_correct_rate))
    else:
        _anchored_best = _overall_acc

    update_run_status(
        spark, run_id, catalog, schema,
        best_iteration=0,
        best_accuracy=_anchored_best,
        best_model_id=model_id,
    )

    write_stage(
        spark, run_id, "BASELINE_EVAL_STARTED", "COMPLETE",
        task_key="baseline_eval",
        detail={
            "overall_accuracy": _overall_acc,
            "both_correct_rate": _both_correct_rate,
            "anchored_best_accuracy": _anchored_best,
            "thresholds_met": thresholds_met,
            "invalid_benchmark_count": eval_result.get("invalid_benchmark_count", 0),
            "permission_blocked_count": eval_result.get("permission_blocked_count", 0),
            "unresolved_column_count": eval_result.get("unresolved_column_count", 0),
            "harness_retry_count": eval_result.get("harness_retry_count", 0),
        },
        catalog=catalog, schema=schema,
    )

    try:
        from genie_space_optimizer.optimization.evaluation import log_expectations_on_traces
        log_expectations_on_traces(eval_result)
    except Exception:
        logger.debug("Failed to log expectations on baseline traces", exc_info=True)

    try:
        log_judge_verdicts_on_traces(eval_result)
    except Exception:
        logger.debug("Failed to log judge verdicts on baseline traces", exc_info=True)

    _lines = [_section("BASELINE — STATE PERSISTENCE", "-")]
    _lines.append(_kv("Iteration written", 0))
    _lines.append(_kv("Model linked", model_id))
    _lines.append(_kv("Stage", "BASELINE_EVAL_STARTED -> COMPLETE"))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return {
        "scores": scores,
        "overall_accuracy": eval_result.get("overall_accuracy", 0.0),
        "thresholds_met": thresholds_met,
        "model_id": model_id,
        "eval_result": eval_result,
    }


def _run_baseline(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    benchmarks: list[dict],
    exp_name: str,
    model_id: str,
    catalog: str,
    schema: str,
    domain: str = "",
) -> dict:
    """Stage 2: Run full 8-judge evaluation, check thresholds.

    Wrapper that calls sub-steps in sequence. Returns a dict with scores,
    thresholds_met flag, and model_id.
    """
    try:
        setup_ctx = baseline_setup_scorers(
            w, spark, space_id, run_id, catalog, schema, exp_name, model_id, domain,
        )
        eval_result = baseline_run_evaluation(
            spark, run_id, catalog, schema, benchmarks, setup_ctx, w=w,
        )
        scorecard = baseline_display_scorecard(eval_result)
        return baseline_persist_state(
            w, spark, run_id, model_id, catalog, schema, eval_result, scorecard,
        )
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("BASELINE_EVAL FAILED for run %s", run_id)
        try:
            write_stage(
                spark, run_id, "BASELINE_EVAL", "FAILED",
                task_key="baseline_eval",
                error_message=err_msg[:500],
                catalog=catalog, schema=schema,
            )
            update_run_status(
                spark, run_id, catalog, schema,
                status="FAILED",
                convergence_reason="error_in_BASELINE_EVAL",
            )
        except Exception:
            logger.exception("Failed to write FAILED state for baseline %s", run_id)
        raise


# ── Stage 2.5: PROMPT MATCHING AUTO-CONFIG ──────────────────────────


def _run_prompt_matching_setup(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    catalog: str,
    schema: str,
    *,
    benchmarks: list[dict] | None = None,
) -> dict:
    """Stage 2.5: Enable format assistance and entity matching as best practice.

    Runs between baseline eval and lever loop.  Deterministic (no LLM).
    Returns summary dict with counts of changes applied.

    ``benchmarks`` (optional) is forwarded to the scorer for the
    benchmark-column-reference boost. Callers that don't have the
    benchmark corpus yet can pass ``None`` — the scorer falls back to
    name-based scoring in that case. The first run where benchmarks
    aren't available still produces sensible results, just without the
    benchmark boost.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config

    write_stage(
        spark, run_id, "PROMPT_MATCHING_SETUP", "STARTED",
        task_key="prompt_matching_setup", catalog=catalog, schema=schema,
    )

    try:
        _parsed = config.get("_parsed_space", {})
        _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
        _raw_tbl_count = len(_ds.get("tables", []))
        _raw_mv_count = len(_ds.get("metric_views", []))
        try:
            from genie_space_optimizer.common.asset_semantics import (
                effective_data_source_split,
            )
            _split = effective_data_source_split(config)
            _eff_tbl_count = len(_split.tables)
            _eff_mv_count = len(_split.metric_views)
            _unknown_count = len(_split.unknown)
        except Exception:
            _eff_tbl_count = _raw_tbl_count
            _eff_mv_count = _raw_mv_count
            _unknown_count = 0
        print(
            f"\n[PROMPT MATCHING] Starting auto-config — "
            f"raw tables: {_raw_tbl_count}, raw metric_views: {_raw_mv_count}, "
            f"effective tables: {_eff_tbl_count}, "
            f"effective metric_views: {_eff_mv_count}, "
            f"unknown: {_unknown_count}, "
            f"total raw data sources: {_raw_tbl_count + _raw_mv_count}"
        )

        apply_log = auto_apply_prompt_matching(
            w, space_id, config, benchmarks=benchmarks,
        )

        applied = apply_log.get("applied", [])
        fa_count = apply_log.get("format_assistance_count", 0)
        em_count = apply_log.get("entity_matching_count", 0)
        em_disabled_count = apply_log.get("entity_matching_disabled_count", 0)

        # Map entry type to the inverse operation so standard rollback can
        # restore disabled slots (and conversely, undo enables). Format
        # assistance doesn't need a paired inverse in this table — operator
        # rollbacks of FA go through the standard genie_config path.
        _INVERSE_BY_TYPE = {
            "enable_value_dictionary": {"enable_entity_matching": False},
            "disable_value_dictionary": {"enable_entity_matching": True},
            "enable_example_values": {"enable_format_assistance": False},
        }
        for idx, entry in enumerate(applied):
            etype = entry.get("type", "unknown")
            tbl = entry.get("table", "")
            col = entry.get("column", "")
            inverse = _INVERSE_BY_TYPE.get(etype)
            rollback_payload = None
            if inverse is not None:
                rollback_payload = json.dumps({
                    "op": "update",
                    "section": "column_configs",
                    "table": tbl,
                    "column": col,
                    **inverse,
                })
            write_patch(
                spark, run_id, 0, 0, idx,
                {
                    "patch_type": etype,
                    "scope": "genie_config",
                    "risk_level": "low",
                    "target_object": f"{tbl}.{col}",
                    "patch": entry,
                    "command": None,
                    "rollback": rollback_payload,
                    "proposal_id": "prompt_matching_auto_config",
                },
                catalog, schema,
            )

        if applied:
            refreshed = fetch_space_config(w, space_id)
            config["_parsed_space"] = refreshed.get("_parsed_space", refreshed)

        _pm_lines = [_section("PROMPT MATCHING", "-")]
        _pm_lines.append(_kv("Total changes", len(applied)))
        _pm_lines.append(_kv("Format assistance", f"{fa_count} columns"))
        _pm_lines.append(_kv("Entity matching enabled", f"{em_count} columns"))
        _pm_lines.append(_kv("Entity matching disabled", f"{em_disabled_count} columns"))
        _pm_lines.append(_kv("Tables patched", apply_log.get('patched_objects', [])))
        _pm_lines.append(_kv("Genie API PATCH sent", "YES" if applied else "NO"))
        _pm_lines.append(_kv("Config refreshed", "YES" if applied else "N/A"))
        _pm_lines.append(_bar("-"))
        print("\n".join(_pm_lines))

        write_stage(
            spark, run_id, "PROMPT_MATCHING_SETUP", "COMPLETE",
            task_key="prompt_matching_setup",
            detail={
                "format_assistance_enabled": fa_count,
                "entity_matching_enabled": em_count,
                "entity_matching_disabled": em_disabled_count,
                "total_changes": len(applied),
                "patched_objects": apply_log.get("patched_objects", []),
            },
            catalog=catalog, schema=schema,
        )

        return {
            "format_assistance_count": fa_count,
            "entity_matching_count": em_count,
            "entity_matching_disabled_count": em_disabled_count,
            "total_changes": len(applied),
        }

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("PROMPT_MATCHING_SETUP FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "PROMPT_MATCHING_SETUP", "FAILED",
            task_key="prompt_matching_setup",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return {
            "format_assistance_count": 0,
            "entity_matching_count": 0,
            "entity_matching_disabled_count": 0,
            "total_changes": 0,
        }


# ── Stage 2.75: PROACTIVE DESCRIPTION ENRICHMENT ───────────────────


def _run_description_enrichment(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.75: Generate structured descriptions for columns and tables.

    Runs after UC type enrichment and before the strategist.  Targets
    columns and tables whose descriptions are insufficient (< 10 chars)
    in both the Genie Space and Unity Catalog.
    Applies patches via update_sections with lever=0 (pre-optimization).

    Returns summary dict with column and table enrichment counts.
    """
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        patch_space_config,
    )
    from genie_space_optimizer.optimization.structured_metadata import (
        entity_type_for_column,
        update_sections,
    )

    write_stage(
        spark, run_id, "DESCRIPTION_ENRICHMENT", "STARTED",
        task_key="description_enrichment", catalog=catalog, schema=schema,
    )

    result = {
        "total_eligible": 0,
        "total_patches_generated": 0,
        "total_failed_llm": 0,
        "total_enriched": 0,
        "total_skipped": 0,
        "tables_eligible": 0,
        "tables_patches_generated": 0,
        "tables_failed_llm": 0,
        "tables_enriched": 0,
        "tables_skipped": 0,
    }

    try:
        # ── Column description enrichment ────────────────────────────
        _data_profile = metadata_snapshot.get("_data_profile", {})
        # Compute ORIGINAL eligibility before calling the LLM so silent
        # batch drops (e.g. unparseable JSON after validator retries)
        # become visible in the stage summary.
        _blank_columns = _collect_blank_columns(metadata_snapshot)
        col_patches = _enrich_blank_descriptions(metadata_snapshot, w, data_profile=_data_profile)
        result["total_eligible"] = len(_blank_columns)
        result["total_patches_generated"] = len(col_patches)
        result["total_failed_llm"] = max(
            0, len(_blank_columns) - len(col_patches),
        )

        ds = metadata_snapshot.get("data_sources", {})
        if not isinstance(ds, dict):
            ds = {}
        tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
        mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
        all_objects = list(tables) + list(mvs)
        tbl_lookup: dict[str, dict] = {}
        for tbl in all_objects:
            if isinstance(tbl, dict):
                ident = tbl.get("identifier", "") or tbl.get("name", "")
                tbl_lookup[ident] = tbl

        col_enriched = 0
        col_skipped = 0
        col_enriched_items: list[dict] = []

        for patch in col_patches:
            tbl_id = patch["table"]
            col_name = patch["column"]
            sections = patch.get("structured_sections", {})
            etype = patch.get("column_entity_type", "")

            tbl = tbl_lookup.get(tbl_id)
            if not tbl:
                logger.warning("Description enrichment: table %s not found — skipping", tbl_id)
                col_skipped += 1
                continue

            cols = tbl.get("column_configs", tbl.get("columns", []))
            cc = None
            for c in cols:
                if isinstance(c, dict) and (c.get("column_name", c.get("name", "")) == col_name):
                    cc = c
                    break

            if cc is None:
                logger.warning(
                    "Description enrichment: column %s.%s not found — skipping", tbl_id, col_name,
                )
                col_skipped += 1
                continue

            if not etype:
                data_type = cc.get("data_type", "")
                etype = entity_type_for_column(col_name, data_type)

            try:
                synonym_value = sections.pop("synonyms", "")

                new_desc = update_sections(
                    cc.get("description"),
                    sections,
                    lever=0,
                    entity_type=etype,
                )
                cc["description"] = new_desc

                if synonym_value:
                    new_syns = [s.strip() for s in str(synonym_value).split(",") if s.strip()]
                    existing = cc.get("synonyms") or []
                    for s in new_syns:
                        if s not in existing:
                            existing.append(s)
                    cc["synonyms"] = existing

                col_enriched += 1
                col_enriched_items.append(patch)
            except Exception:
                logger.warning(
                    "Description enrichment: failed to apply sections for %s.%s",
                    tbl_id, col_name, exc_info=True,
                )
                col_skipped += 1

        result["total_enriched"] = col_enriched
        result["total_skipped"] = col_skipped

        # ── Table description enrichment ─────────────────────────────
        _insufficient_tables = _collect_insufficient_tables(metadata_snapshot)
        tbl_patches = _enrich_table_descriptions(metadata_snapshot, w, data_profile=_data_profile)
        result["tables_eligible"] = len(_insufficient_tables)
        result["tables_patches_generated"] = len(tbl_patches)
        result["tables_failed_llm"] = max(
            0, len(_insufficient_tables) - len(tbl_patches),
        )

        tbl_enriched = 0
        tbl_skipped = 0
        tbl_enriched_items: list[dict] = []

        for patch in tbl_patches:
            tbl_id = patch["table"]
            sections = patch.get("structured_sections", {})
            entity_type = patch.get("table_entity_type", "table")

            tbl = tbl_lookup.get(tbl_id)
            if not tbl:
                logger.warning("Table description enrichment: table %s not found — skipping", tbl_id)
                tbl_skipped += 1
                continue

            try:
                new_desc = update_sections(
                    tbl.get("description"),
                    sections,
                    lever=0,
                    entity_type=entity_type,
                )
                tbl["description"] = new_desc
                tbl_enriched += 1
                tbl_enriched_items.append(patch)
            except Exception:
                logger.warning(
                    "Table description enrichment: failed to apply sections for %s",
                    tbl_id, exc_info=True,
                )
                tbl_skipped += 1

        result["tables_enriched"] = tbl_enriched
        result["tables_skipped"] = tbl_skipped

        # ── PATCH the Genie Space if anything changed ────────────────
        anything_enriched = col_enriched > 0 or tbl_enriched > 0
        if anything_enriched:
            parsed = config.get("_parsed_space", config)
            patch_space_config(w, space_id, parsed)

            patch_idx = 0
            for patch in col_enriched_items:
                write_patch(
                    spark, run_id, 0, 0, patch_idx,
                    {
                        "patch_type": "proactive_description_enrichment",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": f"{patch['table']}.{patch['column']}",
                        "patch": patch,
                        "command": None,
                        "rollback": None,
                        "proposal_id": "description_enrichment",
                    },
                    catalog, schema,
                )
                patch_idx += 1
            for patch in tbl_enriched_items:
                write_patch(
                    spark, run_id, 0, 0, patch_idx,
                    {
                        "patch_type": "proactive_table_description_enrichment",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": patch["table"],
                        "patch": patch,
                        "command": None,
                        "rollback": None,
                        "proposal_id": "table_description_enrichment",
                    },
                    catalog, schema,
                )
                patch_idx += 1

        # ── Logging ──────────────────────────────────────────────────
        _de_lines = [_section("DESCRIPTION ENRICHMENT", "-")]
        _de_lines.append(_kv("Eligible columns", len(_blank_columns)))
        _de_lines.append(_kv("LLM patches generated", len(col_patches)))
        if result["total_failed_llm"]:
            _de_lines.append(_kv(
                "LLM batch failures",
                result["total_failed_llm"],
            ))
        _de_lines.append(_kv("Columns enriched", col_enriched))
        _de_lines.append(_kv("Columns skipped", col_skipped))
        if col_enriched_items:
            _de_lines.append("|")
            for ei, ep in enumerate(col_enriched_items, 1):
                _tbl_short = ep["table"].rsplit(".", 1)[-1]
                _col = ep["column"]
                _sects = ep.get("structured_sections", {})
                _defn = _sects.get("definition", "")[:80]
                _de_lines.append(f"|  [{ei}] {_tbl_short}.{_col}")
                if _defn:
                    _de_lines.append(f"|      definition: {_defn}")
        _de_lines.append(_kv("Eligible tables", len(_insufficient_tables)))
        _de_lines.append(_kv("Table patches generated", len(tbl_patches)))
        if result["tables_failed_llm"]:
            _de_lines.append(_kv(
                "Table LLM batch failures",
                result["tables_failed_llm"],
            ))
        _de_lines.append(_kv("Tables enriched", tbl_enriched))
        _de_lines.append(_kv("Tables skipped", tbl_skipped))
        if tbl_enriched_items:
            _de_lines.append("|")
            for ei, ep in enumerate(tbl_enriched_items, 1):
                _tbl_short = ep["table"].rsplit(".", 1)[-1]
                _sects = ep.get("structured_sections", {})
                _purpose = _sects.get("purpose", "")[:80]
                _de_lines.append(f"|  [{ei}] {_tbl_short}")
                if _purpose:
                    _de_lines.append(f"|      purpose: {_purpose}")
        _de_lines.append(_kv("Genie API PATCH sent", "YES" if anything_enriched else "NO"))
        _de_lines.append(_bar("-"))
        print("\n".join(_de_lines))

        write_stage(
            spark, run_id, "DESCRIPTION_ENRICHMENT", "COMPLETE",
            task_key="description_enrichment",
            detail=result, catalog=catalog, schema=schema,
        )

        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("DESCRIPTION_ENRICHMENT FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "DESCRIPTION_ENRICHMENT", "FAILED",
            task_key="description_enrichment",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Stage 2.85: PROACTIVE JOIN DISCOVERY ─────────────────────────────


def _run_proactive_join_discovery(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.85: Discover execution-proven joins from baseline eval.

    Parses JOIN clauses from successful baseline eval queries (arbiter =
    ``both_correct`` or ``genie_correct``), corroborates with UC column
    type metadata, and codifies them as Genie Space join specifications.

    Only proposes joins that have Tier 1 (execution-proven) evidence.
    """
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        patch_space_config,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _build_join_specs_from_proven,
        _convert_fk_to_candidates,
        _corroborate_with_uc_metadata,
        _extract_proven_joins,
        _short_name,
    )

    write_stage(
        spark, run_id, "JOIN_DISCOVERY", "STARTED",
        task_key="join_discovery", catalog=catalog, schema=schema,
    )

    result: dict = {
        "existing_specs": 0,
        "fk_candidates": 0,
        "execution_candidates": 0,
        "candidates_found": 0,
        "already_defined": 0,
        "type_incompatible": 0,
        "total_applied": 0,
        "total_skipped": 0,
    }

    try:
        # 1. Load baseline eval rows
        baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
        if not baseline_iter:
            print(
                f"\n-- JOIN DISCOVERY " + "-" * 34 + "\n"
                f"  No baseline eval rows found — skipping.\n"
                + "-" * 52
            )
            write_stage(
                spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
                task_key="join_discovery", detail=result,
                catalog=catalog, schema=schema,
            )
            return result

        rows_json = baseline_iter.get("rows_json")
        if isinstance(rows_json, str):
            try:
                rows_json = json.loads(rows_json)
            except (json.JSONDecodeError, TypeError):
                rows_json = []
        if not isinstance(rows_json, list):
            rows_json = []

        if not rows_json:
            print(
                f"\n-- JOIN DISCOVERY " + "-" * 34 + "\n"
                f"  Baseline eval has 0 rows — skipping.\n"
                + "-" * 52
            )
            write_stage(
                spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
                task_key="join_discovery", detail=result,
                catalog=catalog, schema=schema,
            )
            return result

        # 2. Gather existing join specs
        _inst = metadata_snapshot.get("instructions", {})
        if not isinstance(_inst, dict):
            _inst = {}
        existing_specs = _inst.get("join_specs", [])
        if not isinstance(existing_specs, list):
            existing_specs = []
        result["existing_specs"] = len(existing_specs)

        existing_pairs: set[tuple[str, str]] = set()
        for spec in existing_specs:
            if not isinstance(spec, dict):
                continue
            left_obj = spec.get("left", {})
            right_obj = spec.get("right", {})
            lt = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
            rt = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
            if lt and rt:
                _a, _b = sorted((lt, rt))
                existing_pairs.add((_a, _b))

        # 3a. Tier 0: FK constraint candidates (authoritative)
        fk_rows = config.get("_uc_foreign_keys") or []
        fk_candidates = _convert_fk_to_candidates(fk_rows) if fk_rows else []
        result["fk_candidates"] = len(fk_candidates)

        # 3b. Tier 1: Execution-proven joins from baseline eval
        exec_candidates, exec_diagnostics = _extract_proven_joins(rows_json, metadata_snapshot)
        result["execution_candidates"] = len(exec_candidates)
        result["extraction_diagnostics"] = exec_diagnostics

        # 3c. Merge: FK candidates take precedence for shared table pairs.
        #     For pairs that appear in both, keep the FK candidate's ON
        #     condition (authoritative) but inherit frequency/agreed from
        #     the execution-proven candidate.
        fk_pairs: dict[tuple[str, str], dict] = {}
        for fc in fk_candidates:
            key = tuple(sorted((fc["left_table"], fc["right_table"])))
            fk_pairs[key] = fc

        merged: list[dict] = list(fk_candidates)
        for ec in exec_candidates:
            key = tuple(sorted((ec["left_table"], ec["right_table"])))
            if key in fk_pairs:
                fk_pairs[key]["frequency"] = ec.get("frequency", 0)
                fk_pairs[key]["agreed"] = ec.get("agreed", False)
                fk_pairs[key]["source_questions"] = ec.get("source_questions", [])
            else:
                merged.append(ec)

        candidates = merged
        result["candidates_found"] = len(candidates)

        # 4. Filter out already-defined pairs
        new_candidates = []
        for cand in candidates:
            pair_key = tuple(sorted((cand["left_table"], cand["right_table"])))
            if pair_key in existing_pairs:
                result["already_defined"] += 1
            else:
                new_candidates.append(cand)

        # 5. Corroborate with UC metadata (type check).
        #    FK-sourced candidates bypass this check — the database's own
        #    constraints are authoritative about type compatibility.
        fk_cands = [c for c in new_candidates if c.get("fk_constraint")]
        exec_cands = [c for c in new_candidates if not c.get("fk_constraint")]
        before_uc = len(exec_cands)
        exec_cands = _corroborate_with_uc_metadata(exec_cands, metadata_snapshot)
        result["type_incompatible"] = before_uc - len(exec_cands)
        new_candidates = fk_cands + exec_cands

        # 6. Build join specs
        new_specs = _build_join_specs_from_proven(new_candidates, metadata_snapshot)

        # PR 33 — drop joins where either side is a metric view per
        # ``_asset_semantics``. Direct joins on MVs raise
        # ``METRIC_VIEW_JOIN_NOT_SUPPORTED`` at execute time and surface
        # in benchmark eval as gate.execute rejections; gating at
        # discovery prevents the bad join from ever being PATCHed onto
        # the Genie space.
        from genie_space_optimizer.optimization.optimizer import (
            filter_join_specs_by_semantics,
        )
        _mv_skip_counters: dict[str, int] = {}
        _mv_skip_examples: list[tuple[str, str]] = []
        _specs_before_mv_filter = len(new_specs) if isinstance(new_specs, list) else 0
        new_specs = filter_join_specs_by_semantics(
            metadata_snapshot,
            new_specs,
            counters=_mv_skip_counters,
            skipped_examples=_mv_skip_examples,
        )
        _specs_after_mv_filter = len(new_specs) if isinstance(new_specs, list) else 0
        result["joins_skipped_metric_view"] = (
            _specs_before_mv_filter - _specs_after_mv_filter
        )

        # 7. Gate: nothing to apply
        if not new_specs:
            _jd_lines = [_section("JOIN DISCOVERY", "-")]
            _jd_lines.append(_kv("Existing join specs", result['existing_specs']))
            _jd_lines.append(_kv("FK constraint candidates", result['fk_candidates']))
            _jd_lines.append(_kv("Execution-proven candidates", result['execution_candidates']))
            _diag = result.get("extraction_diagnostics", {})
            if _diag:
                _jd_lines.append(_kv("  Eval rows scanned", _diag.get("total_rows", "?")))
                _jd_lines.append(_kv("  Positive verdicts", _diag.get("positive_verdicts", "?")))
                _jd_lines.append(_kv("  SQL with JOIN", _diag.get("sql_with_join", "?")))
                _jd_lines.append(_kv("  FROM unresolved", _diag.get("no_from_resolved", "?")))
                _jd_lines.append(_kv("  Joined unresolved", _diag.get("no_joined_resolved", "?")))
            _jd_lines.append(_kv("Merged candidates", result['candidates_found']))
            _jd_lines.append(_kv("Already defined", result['already_defined']))
            _jd_lines.append(_kv("Type-incompatible", result['type_incompatible']))
            if result.get("joins_skipped_metric_view"):
                _jd_lines.append(_kv(
                    "Skipped (metric_view)", result["joins_skipped_metric_view"],
                ))
            _jd_lines.append(_kv("New joins to apply", 0))
            _jd_lines.append(_bar("-"))
            print("\n".join(_jd_lines))
            write_stage(
                spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
                task_key="join_discovery", detail=result,
                catalog=catalog, schema=schema,
            )
            return result

        # 8. Apply join specs to config
        parsed = config.get("_parsed_space", config)
        inst_block = parsed.setdefault("instructions", {})
        spec_list = inst_block.setdefault("join_specs", [])

        applied_lines: list[str] = []
        for spec in new_specs:
            meta = spec.pop("_proactive_metadata", {})
            spec_list.append(spec)

            left_short = _short_name(spec["left"]["identifier"])
            right_short = _short_name(spec["right"]["identifier"])
            freq = meta.get("frequency", 0)
            agreed_tag = "agreed" if meta.get("agreed") else "single_source"
            applied_lines.append(
                f"    {left_short} <-> {right_short}"
                f" ON {spec['sql'][0][:60] if spec.get('sql') else '?'}"
                f" (freq={freq}, {agreed_tag})"
            )
            result["total_applied"] += 1

        # 9. PATCH Genie Space
        patch_space_config(w, space_id, parsed)

        # 10. Write patch provenance
        for idx, spec in enumerate(new_specs):
            write_patch(
                spark, run_id, 0, 0, idx,
                {
                    "patch_type": "proactive_join_discovery",
                    "scope": "genie_config",
                    "risk_level": "low",
                    "target_object": (
                        f"{spec['left']['identifier']}"
                        f" <-> {spec['right']['identifier']}"
                    ),
                    "patch": spec,
                    "command": None,
                },
                catalog=catalog, schema=schema,
            )

        # 11. Summary
        _jd_lines = [_section("JOIN DISCOVERY", "-")]
        _jd_lines.append(_kv("Existing join specs", result['existing_specs']))
        _jd_lines.append(_kv("FK constraint candidates", result['fk_candidates']))
        _jd_lines.append(_kv("Execution-proven candidates", result['execution_candidates']))
        _diag = result.get("extraction_diagnostics", {})
        if _diag:
            _jd_lines.append(_kv("  Eval rows scanned", _diag.get("total_rows", "?")))
            _jd_lines.append(_kv("  Positive verdicts", _diag.get("positive_verdicts", "?")))
            _jd_lines.append(_kv("  SQL with JOIN", _diag.get("sql_with_join", "?")))
            _jd_lines.append(_kv("  FROM unresolved", _diag.get("no_from_resolved", "?")))
            _jd_lines.append(_kv("  Joined unresolved", _diag.get("no_joined_resolved", "?")))
        _jd_lines.append(_kv("Merged candidates", result['candidates_found']))
        _jd_lines.append(_kv("Already defined", result['already_defined']))
        _jd_lines.append(_kv("Type-incompatible", result['type_incompatible']))
        if result.get("joins_skipped_metric_view"):
            _jd_lines.append(_kv(
                "Skipped (metric_view)", result["joins_skipped_metric_view"],
            ))
        _jd_lines.append(_kv("New joins applied", result['total_applied']))
        if applied_lines:
            _jd_lines.append("|")
            _jd_lines.extend(f"|  {al.strip()}" for al in applied_lines)
        _jd_lines.append(_kv("Genie API PATCH sent", "YES"))
        _jd_lines.append(_bar("-"))
        print("\n".join(_jd_lines))

        write_stage(
            spark, run_id, "JOIN_DISCOVERY", "COMPLETE",
            task_key="join_discovery", detail=result,
            catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("JOIN_DISCOVERY FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "JOIN_DISCOVERY", "FAILED",
            task_key="join_discovery",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Iterative join mining (runs after each accepted iteration) ────────


def _empty_join_discovery_result() -> dict:
    """Initial counter dict for join discovery banners.

    Operators previously saw a single ``new_joins_applied`` line; this
    structure exposes per-source candidate counts (FK rows vs FK
    candidates vs execution-proven vs example-SQL-derived) and explicit
    rejection buckets so a non-zero ``new_joins_applied`` can be
    correlated to its provenance without grep'ing debug logs.
    """
    return {
        "existing_specs": 0,
        "fk_rows_available": 0,
        "fk_candidates_built": 0,
        "fk_candidates": 0,
        "execution_candidates": 0,
        "example_sql_join_candidates": 0,
        "candidates_found": 0,
        "already_defined": 0,
        "type_incompatible": 0,
        "spec_validation_rejected": 0,
        "joins_skipped_metric_view": 0,
        "total_applied": 0,
        "total_skipped": 0,
    }


def _collect_examples_for_join_mining(
    *,
    unified_example_result: dict,
    preflight_example_result: dict,
) -> list[dict]:
    """Flatten accepted examples from unified + fallback paths into a
    single list keyed for ``_mine_and_apply_joins_from_example_sqls``.
    """
    examples: list[dict] = []
    for source, result in (
        ("unified_example_sql", unified_example_result or {}),
        ("preflight_example_synthesis", preflight_example_result or {}),
    ):
        for ex in result.get("accepted_examples", []) or []:
            if not isinstance(ex, dict):
                continue
            question = str(
                ex.get("question") or ex.get("example_question") or ""
            ).strip()
            sql = str(
                ex.get("expected_sql")
                or ex.get("example_sql")
                or ex.get("sql")
                or ""
            ).strip()
            if not sql:
                continue
            examples.append({
                "question": question,
                "expected_sql": sql,
                "source": source,
            })
    return examples


def _example_sqls_to_positive_eval_rows(examples: list[dict]) -> list[dict]:
    """Convert accepted example SQLs into synthetic eval rows for join mining.

    Each example becomes a single row with ``arbiter/value`` set to
    ``synthetic_example`` so downstream structural mining can require
    explicit corroboration (UC FK or ``both_correct`` benchmark) before
    promoting joins inferred from synthetic origin. The previous
    ``ground_truth_correct`` tag is reserved for rows where the
    ground-truth SQL was actually executed and matched.
    """
    rows: list[dict] = []
    for idx, ex in enumerate(examples or []):
        if not isinstance(ex, dict):
            continue
        question = str(
            ex.get("question")
            or ex.get("example_question")
            or ""
        ).strip()
        sql = str(
            ex.get("expected_sql")
            or ex.get("example_sql")
            or ex.get("sql")
            or ""
        ).strip()
        if not sql:
            continue
        qid = f"example_sql_{idx + 1}"
        rows.append({
            "question_id": qid,
            "inputs/question": question,
            "inputs/expected_sql": sql,
            "request": {
                "question": question,
                "expected_sql": sql,
            },
            "response": {"response": ""},
            "arbiter/value": "synthetic_example",
            "feedback/arbiter/value": "synthetic_example",
            "_synthetic_origin": "accepted_example_sql",
        })
    return rows


_EXAMPLE_SQL_JOIN_PAIR_RE = re.compile(
    r"FROM\s+([A-Za-z_][\w\.]*)(?:\s+\w+)?\s+JOIN\s+([A-Za-z_][\w\.]*)",
    re.IGNORECASE,
)


def _example_sql_join_pairs(sql: str) -> set[tuple[str, str]]:
    """Extract (left, right) table pairs from a SQL string.

    Used for corroboration: identifies which join pairs the example SQL
    teaches so we can compare against UC FKs and ``both_correct`` baseline
    pairs without round-tripping through the full proven-join extractor.
    """
    pairs: set[tuple[str, str]] = set()
    if not sql:
        return pairs
    for match in _EXAMPLE_SQL_JOIN_PAIR_RE.finditer(sql):
        left = str(match.group(1) or "").strip()
        right = str(match.group(2) or "").strip()
        if left and right:
            pairs.add(tuple(sorted((left, right))))
    return pairs


def _mine_and_apply_joins_from_example_sqls(
    *,
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    space_id: str,
    metadata_snapshot: dict,
    examples: list[dict],
    catalog: str,
    schema: str,
    baseline_both_correct_rows: list[dict] | None = None,
) -> dict:
    """Mine table-table joins from accepted example SQLs, corroborated.

    A join pair (A, B) is promoted only when corroborated by either
    (a) a UC foreign key between A and B, or (b) a ``both_correct``
    baseline row whose SQL contains the same join pair. Uncorroborated
    pairs are dropped — they fall under high-risk lane and require
    independent evidence before they reach the Genie space.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _extract_proven_joins,
    )

    baseline_rows = list(baseline_both_correct_rows or [])
    examples = list(examples or [])

    result: dict = {
        "total_applied": 0,
        "examples_scanned": len(examples),
        "synthetic_rows": 0,
        "source": "accepted_example_sqls",
        "extraction_diagnostics": {},
        "dropped_uncorroborated": 0,
        "corroboration_source": "",
    }
    if not examples:
        return result

    fk_pairs: set[tuple[str, str]] = set()
    for fk in metadata_snapshot.get("_uc_foreign_keys", []) or []:
        if not isinstance(fk, dict):
            continue
        a = str(fk.get("left_table") or "")
        b = str(fk.get("right_table") or "")
        if a and b:
            fk_pairs.add(tuple(sorted((a, b))))

    baseline_pairs: set[tuple[str, str]] = set()
    for row in baseline_rows:
        if not isinstance(row, dict):
            continue
        verdict = str(
            (row.get("arbiter") or {}).get("value")
            if isinstance(row.get("arbiter"), dict)
            else row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or ""
        )
        if verdict != "both_correct":
            continue
        sql = str(
            (row.get("request") or {}).get("expected_sql")
            or (row.get("response") or {}).get("response")
            or ""
        )
        baseline_pairs.update(_example_sql_join_pairs(sql))

    corroboration_set = fk_pairs | baseline_pairs
    if not corroboration_set:
        result["dropped_uncorroborated"] = len(examples)
        result["corroboration_source"] = "none_available"
        return result

    pseudo_rows: list[dict] = []
    pair_to_row: dict[tuple[str, str], dict] = {}
    dropped = 0
    for idx, ex in enumerate(examples):
        if not isinstance(ex, dict):
            continue
        question = str(ex.get("question") or ex.get("example_question") or "").strip()
        sql = str(
            ex.get("expected_sql") or ex.get("example_sql") or ex.get("sql") or ""
        ).strip()
        if not sql:
            continue
        row = {
            "question_id": f"example_sql_{idx + 1}",
            "arbiter/value": "both_correct",
            "request": {"question": question, "expected_sql": sql},
            "response": {"response": sql},
            "_synthetic_origin": "accepted_example_sql",
        }
        pairs = _example_sql_join_pairs(sql)
        if not pairs:
            continue
        any_corroborated = False
        for pair in pairs:
            if pair in corroboration_set:
                pair_to_row.setdefault(pair, row)
                any_corroborated = True
            else:
                dropped += 1
        if any_corroborated:
            pseudo_rows.append(row)
    result["synthetic_rows"] = len(pseudo_rows)
    result["dropped_uncorroborated"] = dropped
    if not pseudo_rows:
        return result

    seen_qids: set[str] = set()
    apply_rows: list[dict] = []
    for row in pseudo_rows:
        qid = str(row.get("question_id") or "")
        if qid in seen_qids:
            continue
        seen_qids.add(qid)
        apply_rows.append(row)

    mined = _mine_and_apply_proven_joins(
        w, spark, run_id, space_id, metadata_snapshot, apply_rows,
        catalog, schema, iteration=0,
    )
    result.update(mined or {})
    sources: list[str] = []
    if fk_pairs:
        sources.append("uc_fk")
    if baseline_pairs:
        sources.append("baseline_both_correct")
    result["corroboration_source"] = (
        "mixed" if len(sources) > 1 else (sources[0] if sources else "")
    )
    return result


def _mine_and_apply_proven_joins(
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    space_id: str,
    metadata_snapshot: dict,
    eval_rows: list[dict],
    catalog: str,
    schema: str,
    *,
    iteration: int = 0,
) -> dict:
    """Mine execution-proven joins from eval rows and apply new ones.

    Lightweight wrapper around ``_extract_proven_joins`` →
    ``_corroborate_with_uc_metadata`` → ``_build_join_specs_from_proven``
    that can be called after any accepted iteration.

    Returns a result dict with ``total_applied`` count and ``new_specs``.
    Does NOT re-read from Delta — operates on the in-memory *eval_rows*.
    """
    from genie_space_optimizer.common.genie_client import (
        patch_space_config,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _build_join_specs_from_proven,
        _corroborate_with_uc_metadata,
        _extract_proven_joins,
        _short_name,
    )

    result: dict = {"total_applied": 0, "new_specs": [], "extraction_diagnostics": {}}
    if not eval_rows:
        return result

    exec_candidates, exec_diagnostics = _extract_proven_joins(eval_rows, metadata_snapshot)
    result["extraction_diagnostics"] = exec_diagnostics
    if not exec_candidates:
        return result

    _inst = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst, dict):
        _inst = {}
    existing_specs = _inst.get("join_specs", [])
    if not isinstance(existing_specs, list):
        existing_specs = []

    existing_pairs: set[tuple[str, str]] = set()
    for spec in existing_specs:
        if not isinstance(spec, dict):
            continue
        left_obj = spec.get("left", {})
        right_obj = spec.get("right", {})
        lt = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
        rt = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
        if lt and rt:
            _a, _b = sorted((lt, rt))
            existing_pairs.add((_a, _b))

    new_candidates = []
    for cand in exec_candidates:
        pair_key = tuple(sorted((cand["left_table"], cand["right_table"])))
        if pair_key not in existing_pairs:
            new_candidates.append(cand)

    new_candidates = _corroborate_with_uc_metadata(new_candidates, metadata_snapshot)
    if not new_candidates:
        return result

    new_specs = _build_join_specs_from_proven(new_candidates, metadata_snapshot)

    # PR 33 — Drop join specs touching metric views before validation
    # / PATCH. Iterative join mining inherits the same guard as
    # ``_run_proactive_join_discovery`` so MV-MV joins never reach the
    # Genie space regardless of which path produced them.
    from genie_space_optimizer.optimization.optimizer import (
        filter_join_specs_by_semantics,
    )
    _mv_skip_counters: dict[str, int] = {}
    _specs_before_mv_filter = len(new_specs) if isinstance(new_specs, list) else 0
    new_specs = filter_join_specs_by_semantics(
        metadata_snapshot,
        new_specs,
        counters=_mv_skip_counters,
    )
    _specs_after_mv_filter = len(new_specs) if isinstance(new_specs, list) else 0
    result["joins_skipped_metric_view"] = (
        _specs_before_mv_filter - _specs_after_mv_filter
    )
    if not new_specs:
        return result

    parsed = metadata_snapshot
    inst_block = parsed.setdefault("instructions", {})
    spec_list = inst_block.setdefault("join_specs", [])

    # Defence-in-depth EXPLAIN: these specs come from eval rows that
    # already executed successfully on the warehouse, so EXPLAIN rarely
    # fires. Keeping it here for symmetry with
    # :func:`_apply_instruction_join_specs` and to catch corner cases
    # (FQ-resolution drift, column renames between eval and PATCH).
    warehouse_id = resolve_warehouse_id("")
    validated_specs: list[dict] = []
    explain_rejected = 0
    for spec in new_specs:
        left_id = (spec.get("left") or {}).get("identifier", "")
        right_id = (spec.get("right") or {}).get("identifier", "")
        sql_field = spec.get("sql", [])
        join_cond = (
            sql_field[0] if isinstance(sql_field, list) and sql_field
            else str(sql_field or "")
        )
        is_valid, err = _explain_join_candidate(
            w, spark, left_id, right_id, join_cond,
            catalog=catalog, gold_schema=schema,
            warehouse_id=warehouse_id,
        )
        if not is_valid:
            logger.info(
                "Proven-join EXPLAIN rejected: %s <-> %s ON %s — %s",
                left_id, right_id, (join_cond or "")[:80], err,
            )
            explain_rejected += 1
            continue
        validated_specs.append(spec)
    new_specs = validated_specs
    if explain_rejected:
        result["explain_rejected"] = explain_rejected

    applied_lines: list[str] = []
    for spec in new_specs:
        meta = spec.pop("_proactive_metadata", {})
        spec_list.append(spec)
        left_short = _short_name(spec["left"]["identifier"])
        right_short = _short_name(spec["right"]["identifier"])
        freq = meta.get("frequency", 0)
        agreed_tag = "agreed" if meta.get("agreed") else "single_source"
        applied_lines.append(
            f"{left_short} <-> {right_short}"
            f" ON {spec['sql'][0][:60] if spec.get('sql') else '?'}"
            f" (freq={freq}, {agreed_tag})"
        )
        result["total_applied"] += 1

    patch_space_config(w, space_id, parsed)
    result["new_specs"] = new_specs

    for idx, spec in enumerate(new_specs):
        write_patch(
            spark, run_id, iteration, 4, idx,
            {
                "patch_type": "iterative_join_mining",
                "scope": "genie_config",
                "risk_level": "low",
                "target_object": (
                    f"{spec['left']['identifier']}"
                    f" <-> {spec['right']['identifier']}"
                ),
                "patch": spec,
                "command": None,
            },
            catalog, schema,
        )

    _jm_lines = [_section(f"ITERATIVE JOIN MINING (iter {iteration})", "-")]
    _diag = result.get("extraction_diagnostics", {})
    _jm_lines.append(_kv("Eval rows scanned", _diag.get("total_rows", "?")))
    _jm_lines.append(_kv("Positive verdicts", _diag.get("positive_verdicts", "?")))
    _jm_lines.append(_kv("SQL with JOIN", _diag.get("sql_with_join", "?")))
    _jm_lines.append(_kv("Existing join specs", len(existing_specs)))
    _jm_lines.append(_kv("New candidates", len(new_candidates)))
    _jm_lines.append(_kv("New joins applied", result["total_applied"]))
    for al in applied_lines:
        _jm_lines.append(f"|    {al}")
    _jm_lines.append(_bar("-"))
    print("\n".join(_jm_lines))

    write_stage(
        spark, run_id, f"JOIN_MINING_ITER_{iteration}", "COMPLETE",
        task_key="lever_loop", iteration=iteration,
        detail=result,
        catalog=catalog, schema=schema,
    )

    return result


# ── Stage 2.9: PROACTIVE SPACE METADATA ENRICHMENT ──────────────────


def _run_space_metadata_enrichment(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.9: Generate Space description and sample questions if empty.

    Runs after join discovery and before the lever loop.  Only fires when
    the top-level ``description`` or ``config.sample_questions`` are absent.
    """
    from genie_space_optimizer.common.genie_client import (
        patch_space_config,
        update_space_description,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _generate_sample_questions,
        _generate_space_description,
    )

    from genie_space_optimizer.optimization.optimizer import _MIN_DESCRIPTION_LENGTH
    _desc_text = (config.get("description") or "").strip()
    needs_description = len(_desc_text) < _MIN_DESCRIPTION_LENGTH
    parsed = config.get("_parsed_space", config)
    existing_sqs = (parsed.get("config") or {}).get("sample_questions")
    needs_questions = not existing_sqs

    result: dict = {
        "description_generated": False,
        "questions_generated": False,
        "questions_count": 0,
    }

    if not needs_description and not needs_questions:
        _sm_lines = [_section("SPACE METADATA ENRICHMENT", "-")]
        _sm_lines.append(_kv("Description", "already present"))
        _sm_lines.append(_kv("Sample questions", f"already present ({len(existing_sqs or [])})"))
        _sm_lines.append(_kv("Status", "No enrichment needed"))
        _sm_lines.append(_bar("-"))
        print("\n".join(_sm_lines))
        return result

    write_stage(
        spark, run_id, "SPACE_METADATA_ENRICHMENT", "STARTED",
        task_key="space_metadata_enrichment", catalog=catalog, schema=schema,
    )

    try:
        desc_text = ""
        if needs_description:
            desc_text = _generate_space_description(metadata_snapshot, w)
            if desc_text:
                try:
                    update_space_description(w, space_id, desc_text)
                    result["description_generated"] = True
                    write_patch(
                        spark, run_id, 0, 0, 0,
                        {
                            "patch_type": "proactive_space_description",
                            "scope": "genie_space",
                            "risk_level": "low",
                            "target_object": "space.description",
                            "patch": {"description": desc_text[:200] + "..."},
                            "command": None,
                            "rollback": None,
                            "proposal_id": "space_metadata_enrichment",
                        },
                        catalog, schema,
                    )
                except Exception:
                    logger.warning(
                        "Space metadata enrichment: description PATCH failed",
                        exc_info=True,
                    )

        effective_desc = desc_text or (config.get("description") or "")

        new_sqs: list[dict] = []
        if needs_questions:
            new_sqs = _generate_sample_questions(metadata_snapshot, effective_desc, w)
            if new_sqs:
                cfg_block = parsed.setdefault("config", {})
                cfg_block["sample_questions"] = new_sqs
                try:
                    patch_space_config(w, space_id, parsed)
                    result["questions_generated"] = True
                    result["questions_count"] = len(new_sqs)
                    for idx, sq in enumerate(new_sqs):
                        q_text = sq.get("question", [""])[0] if sq.get("question") else ""
                        write_patch(
                            spark, run_id, 0, 0, idx + 1,
                            {
                                "patch_type": "proactive_sample_question",
                                "scope": "genie_config",
                                "risk_level": "low",
                                "target_object": f"config.sample_questions[{idx}]",
                                "patch": {"question": q_text[:100]},
                                "command": None,
                                "rollback": None,
                                "proposal_id": "space_metadata_enrichment",
                            },
                            catalog, schema,
                        )
                except Exception:
                    logger.warning(
                        "Space metadata enrichment: sample_questions PATCH failed",
                        exc_info=True,
                    )

        _desc_status = (
            f"generated ({len(desc_text)} chars)"
            if result['description_generated']
            else ("already present" if not needs_description else "FAILED")
        )
        _sq_status = (
            f"generated ({len(new_sqs)} questions)"
            if result['questions_generated']
            else ("already present" if not needs_questions else "FAILED")
        )
        _sm_lines = [_section("SPACE METADATA ENRICHMENT", "-")]
        _sm_lines.append(_kv("Description", _desc_status))
        if result['description_generated'] and desc_text:
            _sm_lines.append(f"|      {desc_text[:120]}...")
        _sm_lines.append(_kv("Sample questions", _sq_status))
        if result['questions_generated'] and new_sqs:
            for sq in new_sqs:
                q_text = sq.get("question", [""])[0] if sq.get("question") else ""
                if q_text:
                    _sm_lines.append(f"|      - {q_text[:100]}")
        _sm_lines.append(_bar("-"))
        print("\n".join(_sm_lines))

        write_stage(
            spark, run_id, "SPACE_METADATA_ENRICHMENT", "COMPLETE",
            task_key="space_metadata_enrichment",
            detail=result, catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("SPACE_METADATA_ENRICHMENT FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "SPACE_METADATA_ENRICHMENT", "FAILED",
            task_key="space_metadata_enrichment",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── High-risk lane gate helpers (deterministic + smoke test) ────────


def _filter_candidates_by_teaching_safety(
    *,
    candidates: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Drop candidates that fail any deterministic teaching-safety gate.

    Returns survivors. Callers should keep their own counter for the
    drop count (the banner in :func:`_print_enrichment_risk_lane_banner`
    expects an integer)."""
    survivors: list[dict] = []
    for cand in candidates or []:
        question = str(
            cand.get("question") or cand.get("example_question") or ""
        )
        sql = str(
            cand.get("expected_sql")
            or cand.get("example_sql")
            or cand.get("sql")
            or ""
        )
        result = check_teaching_safety(
            question=question, sql=sql, metadata_snapshot=metadata_snapshot,
        )
        if result.safe:
            survivors.append(cand)
        else:
            logger.info(
                "[teaching-safety] dropped candidate q=%r reasons=%s",
                question[:80], result.reasons,
            )
    return survivors


def _build_staged_config(
    *,
    base_config: dict,
    candidate_examples: list[dict],
) -> dict:
    """Produce an in-memory copy of ``base_config`` with candidates merged.

    Used by the pre-promotion smoke test. Does NOT call patch APIs —
    the staged config is consumed by the local evaluator only and is
    discarded after the smoke verdict is computed.
    """
    staged = copy.deepcopy(base_config or {})
    parsed = staged.get("_parsed_space") or staged
    existing = list(parsed.get("example_question_sqls") or [])
    for cand in candidate_examples or []:
        existing.append({
            "example_question": str(
                cand.get("question") or cand.get("example_question") or ""
            ),
            "example_sql": str(
                cand.get("expected_sql") or cand.get("example_sql") or ""
            ),
        })
    parsed["example_question_sqls"] = existing
    return staged


def _gate_candidates_with_smoke_test(
    *,
    candidates: list[dict],
    baseline_both_correct_rows: list[dict],
    metadata_snapshot: dict,
    staged_config_builder,
    w,
    spark,
    catalog: str,
    schema: str,
    space_id: str,
) -> list[dict]:
    """Run the pre-promotion smoke test on the candidate batch.

    Returns the candidate list unchanged when the smoke test accepts
    or is disabled. Returns an empty list when the smoke test rejects
    the batch (atomic: the entire batch is dropped to keep the
    apply-step idempotent — partial promotion is not supported)."""
    enabled = os.environ.get(
        "GSO_EXAMPLE_SQL_SMOKE_TEST_ENABLED", "true",
    ).lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return list(candidates or [])
    if not candidates:
        return []
    if not baseline_both_correct_rows:
        logger.info("[smoke-test] skipped: no baseline both_correct pool")
        return list(candidates)

    staged_config = staged_config_builder(candidates)

    def _eval_runner(**kwargs):
        from genie_space_optimizer.optimization.evaluation import (
            run_evaluation,
        )
        return run_evaluation(
            staged_config=kwargs["staged_config"],
            question_ids=kwargs["question_ids"],
            baseline_rows=kwargs["baseline_rows"],
            w=w, spark=spark, catalog=catalog, schema=schema,
            space_id=space_id, smoke_test=True,
        )

    result = run_pre_promotion_smoke_test(
        candidates=candidates,
        baseline_both_correct_rows=baseline_both_correct_rows,
        staged_config=staged_config,
        run_eval_fn=_eval_runner,
    )
    if not result.accept:
        logger.warning(
            "[smoke-test] REJECTED batch: %s (regressions=%d/%d, pp=%.2f)",
            result.reason, result.regressions,
            result.sample_size, result.regression_pp,
        )
        return []
    return list(candidates)


# ── Unified example-SQL generation (Phase 4.R4) ─────────────────────


def _run_unified_example_sql_generation(
    *,
    w: WorkspaceClient,
    spark: Any,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    uc_columns: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    full_firewall_corpus: list[dict],
    data_profile: dict | None = None,
    baseline_both_correct_rows: list[dict] | None = None,
) -> dict:
    """Run the unified (benchmark-engine-based) example-SQL generator.

    Builds the leakage oracle from the benchmark corpus + the space's
    already-installed ``example_question_sqls``, calls
    ``generate_example_sqls``, and applies survivors via the shared
    ``_apply_proactive_example_sqls`` (which runs the last-mile
    firewall one more time, by design).

    Returns a dict with ``applied``, ``rejection_counters``, and a
    short list of applied examples for the pretty summary.
    """
    from genie_space_optimizer.common.config import (
        PREFLIGHT_EXAMPLE_SQL_TARGET,
    )
    from genie_space_optimizer.optimization.evaluation import (
        generate_example_sqls,
    )
    from genie_space_optimizer.optimization.leakage import (
        BenchmarkCorpus,
        LeakageOracle,
    )

    out: dict = {
        "applied": 0,
        "rejection_counters": {},
        "unified_generated": 0,
        "archetype_fallback": False,
        "accepted_examples": [],
    }

    try:
        # ── Firewall: benchmark corpus ∪ existing example_sqls ──────
        bench_corpus = BenchmarkCorpus.from_benchmarks(full_firewall_corpus)

        existing_sqls = (
            (metadata_snapshot.get("instructions") or {})
            .get("example_question_sqls") or []
        )
        existing_corpus_rows = [
            {
                "id": f"existing_{i}",
                "question": (e or {}).get("question", "") or "",
                "expected_sql": (e or {}).get("sql", "") or "",
            }
            for i, e in enumerate(existing_sqls)
            if isinstance(e, dict)
        ]
        existing_corpus = BenchmarkCorpus.from_benchmarks(existing_corpus_rows)
        oracle = LeakageOracle(bench_corpus, existing_corpus)

        candidates, rejection_counters = generate_example_sqls(
            w=w, spark=spark,
            config=config, uc_columns=uc_columns,
            uc_tags=[], uc_routines=[],
            domain=domain, catalog=catalog, schema=schema,
            warehouse_id=resolve_warehouse_id(""),
            target_count=PREFLIGHT_EXAMPLE_SQL_TARGET,
            existing_example_sqls=existing_sqls,
            leakage_oracle=oracle,
        )
    except Exception:
        logger.warning(
            "unified example-sql generation raised; "
            "archetype fallback will run",
            exc_info=True,
        )
        out["rejection_counters"] = {"pipeline_exception": 1}
        return out

    out["unified_generated"] = len(candidates)
    out["rejection_counters"] = rejection_counters or {}

    if not candidates:
        _print_unified_example_summary(
            run_id=run_id,
            target=PREFLIGHT_EXAMPLE_SQL_TARGET,
            existing=len(existing_sqls),
            applied_examples=[],
            rejection_counters=rejection_counters or {},
            config=config,
        )
        return out

    # ── High-risk lane gates ────────────────────────────────────────
    # Operators see the funnel via ``_print_enrichment_risk_lane_banner``;
    # each gate decrements survivors and increments its own counter.
    from genie_space_optimizer.common.config import (
        EXAMPLE_SQL_SMOKE_MAX_QUESTIONS,
        EXAMPLE_SQL_TEACHING_SAFETY_ENABLED,
    )

    candidates_in = len(candidates)

    # Gate 1: deterministic teaching safety
    survivors = _filter_candidates_by_teaching_safety(
        candidates=candidates,
        metadata_snapshot=metadata_snapshot,
    )
    deterministic_safety_rejected = candidates_in - len(survivors)

    # Gate 2: teaching-safety LLM judge (skipped if knob off)
    teaching_safety_rejected = 0
    if EXAMPLE_SQL_TEACHING_SAFETY_ENABLED and survivors:
        from genie_space_optimizer.optimization.scorers.arbiter import (
            score_example_sql_teaching_safety,
        )
        kept: list[dict] = []
        for cand in survivors:
            q = str(
                cand.get("question") or cand.get("example_question") or ""
            )
            s = str(
                cand.get("expected_sql") or cand.get("example_sql") or ""
            )
            try:
                verdict = score_example_sql_teaching_safety(
                    question=q, sql=s, w=w,
                    metadata_snapshot=metadata_snapshot,
                )
            except Exception:
                logger.debug(
                    "[teaching-safety-judge] judge crashed, defaulting to reject",
                    exc_info=True,
                )
                verdict = {"value": "uncertain", "rationale": "judge_crashed"}
            if (verdict or {}).get("value") == "yes":
                kept.append(cand)
            else:
                teaching_safety_rejected += 1
                logger.info(
                    "[teaching-safety-judge] rejected q=%r v=%s rationale=%s",
                    q[:80], (verdict or {}).get("value"),
                    str((verdict or {}).get("rationale") or "")[:200],
                )
        survivors = kept

    # Gate 3: pre-promotion smoke test
    pre_smoke_count = len(survivors)
    survivors = _gate_candidates_with_smoke_test(
        candidates=survivors,
        baseline_both_correct_rows=list(baseline_both_correct_rows or []),
        metadata_snapshot=metadata_snapshot,
        staged_config_builder=lambda cs: _build_staged_config(
            base_config=config, candidate_examples=cs,
        ),
        w=w, spark=spark,
        catalog=catalog, schema=schema, space_id=space_id,
    )
    smoke_test_rejected_batch = (
        pre_smoke_count > 0 and len(survivors) == 0
    )

    rc = rejection_counters or {}
    _print_enrichment_risk_lane_banner(
        candidates_in=candidates_in,
        firewall_blocked=int(
            (rc.get("firewall_joint_similarity") or 0)
            + (rc.get("firewall_question_echo") or 0)
            + (rc.get("firewall_block") or 0)
        ),
        firewall_warned=int(
            (rc.get("firewall_sql_pattern_warning") or 0)
            + (rc.get("firewall_warning") or 0)
        ),
        correctness_rejected=int(rc.get("arbiter_no") or 0),
        deterministic_safety_rejected=deterministic_safety_rejected,
        teaching_safety_rejected=teaching_safety_rejected,
        smoke_test_rejected_batch=smoke_test_rejected_batch,
        smoke_test_regressions=0,
        smoke_test_sample_size=min(
            len(baseline_both_correct_rows or []),
            EXAMPLE_SQL_SMOKE_MAX_QUESTIONS,
        ),
        applied=len(survivors),
    )

    if not survivors:
        _print_unified_example_summary(
            run_id=run_id,
            target=PREFLIGHT_EXAMPLE_SQL_TARGET,
            existing=len(existing_sqls),
            applied_examples=[],
            rejection_counters=rejection_counters or {},
            config=config,
        )
        return out

    candidates = survivors

    # ── Apply via the shared pipeline (runs last-mile firewall) ─────
    proposals = [
        {
            "patch_type": "add_example_sql",
            "example_question": c.get("question", ""),
            "example_sql": c.get("expected_sql", ""),
            "usage_guidance": c.get("usage_guidance", ""),
            "risk_level": "low",
            "provenance": c.get("provenance", "synthetic_example_sql"),
        }
        for c in candidates
    ]

    try:
        apply_log = _apply_proactive_example_sqls(
            w, spark, run_id, space_id, proposals,
            metadata_snapshot, config, catalog, schema,
            benchmarks=full_firewall_corpus,
        )
        applied_entries = (
            apply_log.get("applied", []) if isinstance(apply_log, dict) else []
        )
        out["applied"] = len(applied_entries)
        out["accepted_examples"] = [
            {
                "question": str((entry.get("patch") or {}).get("example_question") or ""),
                "expected_sql": str((entry.get("patch") or {}).get("example_sql") or ""),
                "source": "unified_example_sql",
            }
            for entry in applied_entries
            if isinstance(entry, dict)
        ]
    except Exception:
        logger.warning(
            "unified applier raised; proposals not deployed",
            exc_info=True,
        )

    _print_unified_example_summary(
        run_id=run_id,
        target=PREFLIGHT_EXAMPLE_SQL_TARGET,
        existing=len(existing_sqls),
        applied_examples=candidates,
        rejection_counters=rejection_counters or {},
        config=config,
    )

    return out


def _print_unified_example_summary(
    *,
    run_id: str,
    target: int,
    existing: int,
    applied_examples: list[dict],
    rejection_counters: dict[str, int],
    config: dict | None = None,
) -> None:
    """Pretty-print the unified example-SQL generator outcome.

    Seven distinct rejection classes are surfaced so operators can
    attribute yield shortfall to a specific stage (metadata vs MV vs
    execute vs arbiter vs firewall vs dedup) rather than one bucket.

    PR 21 — when ``config`` is provided, the banner also surfaces a
    one-line MV-detection summary (``MVs detected: N (config: a,
    column-flags: b, catalog: c)``) and the adaptive-overdraw
    short-circuit reason, so log readers see at a glance whether a
    cluster of ``mv_missing_measure_function`` rejections is rooted in
    "no MVs at all" vs "MVs detected but not by the path the rewriter
    consults".
    """
    _lines = [_section("EXAMPLE SQL GENERATION (unified)", "=")]
    _lines.append(_kv("Target", target))
    _lines.append(_kv("Existing examples", existing))
    _lines.append(_kv("Applied (new)", len(applied_examples)))
    _mv_total_for_hint = 0
    if isinstance(config, dict):
        try:
            from genie_space_optimizer.optimization.evaluation import (
                _count_mv_detection_sources,
            )
            _mv_counts = _count_mv_detection_sources(config)
            _mv_total = (
                _mv_counts.get("config", 0)
                + _mv_counts.get("column_flags", 0)
                + _mv_counts.get("catalog", 0)
            )
            _mv_total_for_hint = _mv_total
            _lines.append(_kv(
                "MVs detected",
                f"{_mv_total} (config: {_mv_counts.get('config', 0)}, "
                f"column-flags: {_mv_counts.get('column_flags', 0)}, "
                f"catalog: {_mv_counts.get('catalog', 0)})",
            ))
        except Exception:
            pass
    # PR 23 — when zero MVs are detected but the run produced any
    # ``mv_*`` rejection bucket, the catalog-detection helper either
    # silently no-op'd (DBR < 16.2, permission failure, JSON envelope
    # mismatch) or the unified pipeline has a stale cache. Surface a
    # one-line hint so operators reading the banner are pointed at
    # the catalog-detection summary log line for the underlying cause.
    rc_for_hint = rejection_counters or {}
    _has_mv_reject = False
    _exec_subbuckets = rc_for_hint.get("explain_or_execute_subbuckets") or {}
    if isinstance(_exec_subbuckets, dict):
        for _reason in _exec_subbuckets:
            if isinstance(_reason, str) and _reason.startswith("mv_"):
                _has_mv_reject = True
                break
    if (
        _mv_total_for_hint == 0
        and _has_mv_reject
        and isinstance(config, dict)
    ):
        _lines.append(
            "|  hint: 0 MVs detected but mv_* rejections present — "
            "see catalog-detection summary log line",
        )
    # PR 33 — invariant: when the semantics map has metric views but
    # zero MVs are surfaced through the detection sources, the only
    # known root cause is a config refresh that clobbered the
    # ``_metric_view_yaml`` / ``_asset_semantics`` caches mid-run.
    # ``_refresh_config_preserving_mv_state`` exists specifically to
    # prevent that; if this warning ever fires the helper has been
    # bypassed and the recurring ``MVs detected: 0`` cluster is back.
    if _mv_total_for_hint == 0 and isinstance(config, dict):
        try:
            from genie_space_optimizer.common.asset_semantics import (
                metric_view_identifiers as _sem_mv_ids,
            )
            _sem_mv_count = len(_sem_mv_ids(config))
            if _sem_mv_count > 0:
                _lines.append(
                    "|  WARNING: 0 MVs in detection sources but "
                    f"_asset_semantics has {_sem_mv_count} — "
                    "config-refresh dropped the MV cache",
                )
        except Exception:
            pass
    # PR 27 — surface the unified asset-semantics block plus an
    # invariant warning when zero MVs are stamped despite mv_*
    # rejections. The block survives package INFO-log filtering because
    # the banner is rendered with ``print()`` at the call site.
    if isinstance(config, dict):
        try:
            from genie_space_optimizer.common.asset_semantics import (
                format_semantics_block,
                get_asset_semantics,
                invariant_warning_lines,
            )
            _sem = get_asset_semantics(config)
            if _sem:
                for _sl in format_semantics_block(_sem):
                    _lines.append(f"|  {_sl}")
            for _wl in invariant_warning_lines(_sem, rejection_counters):
                _lines.append(f"|  {_wl}")
        except Exception:
            pass
    _lines.append("|")
    rc = rejection_counters or {}
    _lines.append(_kv("Metadata rejected", rc.get("metadata", 0)))
    _lines.append(_kv("MV guard rejected", rc.get("mv_select_star", 0)))
    _lines.append(_kv(
        "EXPLAIN/execute rejected", rc.get("explain_or_execute", 0),
    ))
    # PR 18 — split EXPLAIN/execute rejected into sub-buckets keyed by
    # the validation reason code so operators can see at a glance
    # whether the dominant failure class is unknown columns vs
    # missing-MEASURE() vs alias collisions vs join issues. Also
    # surface up to 3 example questions per bucket so the log is
    # immediately actionable.
    _subbuckets = rc.get("explain_or_execute_subbuckets") or {}
    _examples = rc.get("explain_or_execute_examples") or {}
    if isinstance(_subbuckets, dict) and _subbuckets:
        _ordered = sorted(
            _subbuckets.items(), key=lambda kv: (-kv[1], kv[0]),
        )
        for _reason, _count in _ordered:
            _lines.append(_kv(f"  {_reason}", _count))
            _ex_list = _examples.get(_reason) or []
            if isinstance(_ex_list, list):
                for _ex in _ex_list[:3]:
                    if not isinstance(_ex, dict):
                        continue
                    _q = str(_ex.get("question", ""))[:80]
                    _err = str(_ex.get("error", ""))[:120]
                    _lines.append(f"|     [{_reason}] {_q} — {_err}")
    _lines.append(_kv(
        "Arbiter verdict=no", rc.get("arbiter_no", 0),
    ))
    # F12 — row-capture-side failures get differentiated counters so
    # the banner distinguishes "judge said no" from "judge never saw
    # rows because capture raised". The subquery-unsupported bucket
    # is the diagnostic signal for PR 13's metric-view-safe fallback;
    # the exec-failed bucket flags timeouts / permissions / other
    # infrastructure issues. Both are emitted unconditionally so a
    # zero count is itself a reassurance signal in the log.
    _lines.append(_kv(
        "Arbiter row-capture: subquery unsupported",
        rc.get("arbiter_row_capture_subquery_unsupported", 0),
    ))
    _lines.append(_kv(
        "Arbiter row-capture: exec failed",
        rc.get("arbiter_row_capture_exec_failed", 0),
    ))
    _lines.append(_kv(
        "Firewall: SQL pattern warnings",
        rc.get("firewall_sql_pattern_warning", 0),
    ))
    _lines.append(_kv(
        "Firewall: joint question+SQL block",
        rc.get("firewall_joint_similarity", 0),
    ))
    _lines.append(_kv(
        "Firewall: question echo", rc.get("firewall_question_echo", 0),
    ))
    if rc.get("selection_input") or rc.get("selection_output"):
        _lines.append(_kv(
            "Final selection",
            f"{rc.get('selection_output', 0)} of {rc.get('selection_input', 0)} candidates",
        ))
    _lines.append(_kv("Dedup (in-corpus)", rc.get("dedup_in_corpus", 0)))
    # F8 — deterministic repairs applied inside the correction loop.
    # Shown only when non-zero so the banner stays terse when the LLM
    # returns clean output. These mirror the F4/F5 counters the
    # preflight banner surfaces.
    _stem_repairs = rc.get("repaired_stemmed_identifiers", 0)
    _measure_repairs = rc.get("repaired_measure_refs", 0)
    _alias_repairs = rc.get("measure_alias_collisions_repaired", 0)
    _overdraw_rounds = rc.get("adaptive_overdraw_rounds_used", 0)
    if (
        _stem_repairs or _measure_repairs or _alias_repairs
        or (isinstance(_overdraw_rounds, int) and _overdraw_rounds > 1)
    ):
        _lines.append("|")
        if _stem_repairs:
            _lines.append(_kv(
                "Stemmed identifiers repaired", _stem_repairs,
            ))
        if _measure_repairs:
            _lines.append(_kv(
                "MEASURE() refs repaired", _measure_repairs,
            ))
        # PR 15 — alias collisions deterministically repaired via
        # ``_repair_measure_alias_collisions``.
        if _alias_repairs:
            _lines.append(_kv(
                "MEASURE() alias collisions repaired", _alias_repairs,
            ))
        # PR 17 — number of adaptive overdraw rounds we used. Show
        # only when > 1 (i.e. the first round under-produced and we
        # had to re-ask the LLM).
        if isinstance(_overdraw_rounds, int) and _overdraw_rounds > 1:
            _lines.append(_kv(
                "Adaptive overdraw rounds used", _overdraw_rounds,
            ))
    # PR 21 — surface the short-circuit reason whenever it is set,
    # independent of whether any deterministic-repair counters
    # fired. Operators reading the banner should always see the
    # signal that we cut overdraw short and why.
    _short_circuit = rc.get("adaptive_overdraw_short_circuited")
    if _short_circuit:
        _lines.append(_kv(
            "Adaptive overdraw short-circuited", str(_short_circuit),
        ))
    if applied_examples:
        _lines.append("|")
        for idx, c in enumerate(applied_examples[:10], 1):
            q = str(c.get("question", ""))[:80]
            _lines.append(f"|  [{idx}] {q}")
        if len(applied_examples) > 10:
            _lines.append(f"|  … and {len(applied_examples) - 10} more")
    _lines.append(_bar("="))
    print("\n".join(_lines))


def _print_enrichment_risk_lane_banner(
    *,
    candidates_in: int,
    firewall_blocked: int,
    firewall_warned: int,
    correctness_rejected: int,
    deterministic_safety_rejected: int,
    teaching_safety_rejected: int,
    smoke_test_rejected_batch: bool,
    smoke_test_regressions: int,
    smoke_test_sample_size: int,
    applied: int,
) -> None:
    """Per-gate funnel banner for the example-SQL high-risk enrichment lane.

    Operators reading the log should be able to attribute yield
    shortfall to a specific gate (firewall vs correctness vs
    deterministic safety vs teaching-safety judge vs smoke test)
    rather than seeing one opaque "rejected N" number.
    """
    _lines = [_section("ENRICHMENT — HIGH-RISK LANE", "=")]
    _lines.append(_kv("Candidates considered", candidates_in))
    _lines.append(_kv(
        "Firewall: blocked",
        f"{firewall_blocked} (warned: {firewall_warned})",
    ))
    _lines.append(_kv("Correctness arbiter: rejected", correctness_rejected))
    _lines.append(_kv(
        "Deterministic safety: rejected", deterministic_safety_rejected,
    ))
    _lines.append(_kv(
        "Teaching-safety judge: rejected", teaching_safety_rejected,
    ))
    if smoke_test_rejected_batch:
        _lines.append(_kv(
            "Smoke test",
            f"REJECTED batch — {smoke_test_regressions}/{smoke_test_sample_size} regressions",
        ))
    elif smoke_test_sample_size:
        _lines.append(_kv(
            "Smoke test",
            f"accepted — {smoke_test_regressions}/{smoke_test_sample_size} regressions",
        ))
    else:
        _lines.append(_kv("Smoke test", "skipped (no baseline pool)"))
    _lines.append(_kv("Applied (gold standard)", applied))
    _lines.append(_bar("="))
    print("\n".join(_lines))


# ── Proactive Benchmark Example SQL Application ─────────────────────


def _apply_proactive_example_sqls(
    w: WorkspaceClient,
    spark: Any,
    run_id: str,
    space_id: str,
    mined_proposals: list[dict],
    metadata_snapshot: dict,
    config: dict,
    catalog: str,
    schema: str,
    benchmarks: list[dict] | None = None,
) -> dict:
    """Apply mined benchmark example SQLs proactively via the Genie API.

    Bug #4 firewall — every proposal is passed through ``is_benchmark_leak``
    before ``proposals_to_patches`` when ``benchmarks`` is provided. Leaky
    proposals are dropped with a counter increment. Callers are expected to
    pass the benchmark corpus so the firewall can run; older call sites
    that omit it degrade gracefully (no firewall, logs a warning).

    Returns the applier ``apply_log`` dict so downstream join-mining and
    observability can see which proposals actually landed.
    """
    if not mined_proposals:
        return {"applied": []}
    from genie_space_optimizer.optimization.applier import (
        proposals_to_patches,
        apply_patch_set,
    )

    if benchmarks is None:
        logger.warning(
            "_apply_proactive_example_sqls called without benchmarks — "
            "Bug #4 firewall skipped. Caller should pass benchmarks.",
        )
    else:
        from genie_space_optimizer.optimization.leakage import (
            BenchmarkCorpus,
            is_benchmark_leak,
            is_example_sql_benchmark_leak,
        )
        from genie_space_optimizer.optimization.optimizer import _incr_bug4_counter

        corpus = BenchmarkCorpus.from_benchmarks(benchmarks)
        filtered: list[dict] = []
        for p in mined_proposals:
            patch_type = p.get("patch_type", "add_example_sql")
            if patch_type in {"add_example_sql", "update_example_sql"}:
                is_leak, reason = is_example_sql_benchmark_leak(p, corpus)
            else:
                is_leak, reason = is_benchmark_leak(p, patch_type, corpus)
            if is_leak:
                _incr_bug4_counter("firewall_rejections")
                logger.info(
                    "Bug #4 firewall: dropped proactive proposal %s (%s) - %s",
                    p.get("proposal_id", "?"),
                    str(p.get("example_question", ""))[:80],
                    reason,
                )
                continue
            filtered.append(p)
        mined_proposals = filtered

    patches = proposals_to_patches(mined_proposals)
    apply_log = apply_patch_set(w, space_id, patches, metadata_snapshot, apply_mode="api")

    applied = apply_log.get("applied", [])
    _lines = [_section("PROACTIVE BENCHMARK EXAMPLE SQLs", "-")]
    _lines.append(_kv("Mined proposals", len(mined_proposals)))
    _lines.append(_kv("Applied", len(applied)))
    if apply_log.get("patch_error"):
        _lines.append(_kv("Error", str(apply_log["patch_error"])[:200]))
    for idx, entry in enumerate(applied, 1):
        _ap = entry.get("patch", {})
        q = _ap.get("example_question", _ap.get("question", ""))
        if isinstance(q, list):
            q = q[0] if q else ""
        _lines.append(f"|  [{idx}] {q[:80]}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    for idx, entry in enumerate(applied):
        _ap = entry.get("patch", {})
        _action = entry.get("action", {})
        write_patch(
            spark, run_id, 0, 0, idx,
            {
                "patch_type": "proactive_example_sql",
                "scope": "genie_config",
                "risk_level": "low",
                "target_object": f"example_question_sqls[{idx}]",
                "patch": {
                    "question": str(_ap.get("example_question", ""))[:200],
                    "sql": str(_ap.get("example_sql", "")),
                },
                "command": _action.get("command"),
                "rollback": _action.get("rollback_command"),
                "proposal_id": "proactive_benchmark_mining",
            },
            catalog, schema,
        )

    return apply_log


# ── Stage 2.95: PROACTIVE INSTRUCTION SEEDING ────────────────────────


def _run_proactive_instruction_seeding(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Two-phase proactive instruction management (Task B.5).

    **Seed phase** — only runs when existing instructions are empty or
    below ``_INSTRUCTION_SEED_THRESHOLD`` chars. Calls
    :func:`_generate_proactive_instructions` (5-section schema),
    validates strictly, writes on pass.

    **Expand phase** — always runs after seed. Parses the (possibly
    freshly seeded) instructions to determine which canonical sections
    are missing, calls :func:`_expand_instructions`, validates the
    merged result strictly, writes on pass. Never overwrites existing
    canonical sections — expansion only *fills gaps*.

    Compared to the pre-B.5 behaviour this solves the "instructions not
    comprehensive" bug: a space whose existing prose was ≥50 chars
    (but only covered PURPOSE) previously got no enrichment at all.
    Now the expand phase fills the other four canonical sections.
    """
    from genie_space_optimizer.common.config import (
        CANONICAL_SECTION_HEADERS, MAX_TEXT_INSTRUCTIONS_CHARS,
        MIN_EXPAND_BUDGET,
    )
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.optimization.applier import (
        _get_general_instructions, _set_general_instructions,
        _trim_bullets_to_budget, _trim_rendered_to_cap,
        parse_canonical_sections, render_canonical_sections,
        validate_instruction_text,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _expand_instructions, _generate_proactive_instructions,
    )

    _INSTRUCTION_SEED_THRESHOLD = 50
    parsed = config.get("_parsed_space", config)
    current_instructions = _get_general_instructions(parsed)

    result: dict = {
        "instructions_seeded": False,
        "instructions_expanded": False,
        "instruction_chars": 0,
        "seeded_sections": [],
        "expanded_sections": [],
        "skipped_reason": None,
        # C3 decline-log UX: track disposition of each phase so the summary
        # line surfaces the actual reason instead of the misleading "no-op".
        "seed_outcome": "skipped",
        "expand_outcome": "not_attempted",
    }

    write_stage(
        spark, run_id, "PROACTIVE_INSTRUCTION_SEEDING", "STARTED",
        task_key="instruction_seeding", catalog=catalog, schema=schema,
    )

    try:
        # ── Seed phase ──────────────────────────────────────────────
        needs_seeding = (
            not current_instructions
            or len(current_instructions.strip()) < _INSTRUCTION_SEED_THRESHOLD
        )
        if needs_seeding:
            instruction_text = _generate_proactive_instructions(metadata_snapshot, w)
            if instruction_text:
                _set_general_instructions(parsed, instruction_text)
                try:
                    patch_space_config(w, space_id, parsed)
                    result["instructions_seeded"] = True
                    result["seed_outcome"] = "wrote"
                    result["instruction_chars"] = len(instruction_text)
                    canonical_secs, _, _ = parse_canonical_sections(instruction_text)
                    result["seeded_sections"] = [
                        h for h in CANONICAL_SECTION_HEADERS if h in canonical_secs
                    ]
                    current_instructions = instruction_text
                    write_patch(
                        spark, run_id, 0, 0, 0,
                        {
                            "patch_type": "proactive_instruction_seeding",
                            "scope": "genie_config",
                            "risk_level": "low",
                            "target_object": "instructions.text_instructions",
                            "patch": {"instructions": instruction_text[:200] + "..."},
                            "command": None,
                            "rollback": None,
                            "proposal_id": "proactive_instruction_seeding",
                        },
                        catalog, schema,
                    )
                except Exception:
                    logger.warning(
                        "Proactive instruction seeding: PATCH failed",
                        exc_info=True,
                    )
                    result["seed_outcome"] = "patch_failed"
            else:
                # LLM returned "" — either validation failed after repair
                # or the LLM call raised. Specific reason was logged by
                # _generate_proactive_instructions.
                result["seed_outcome"] = "declined_llm_or_validation"
        else:
            # Seed is only for empty-or-thin prose; nothing to do otherwise.
            result["seed_outcome"] = "skipped_existing_prose"

        # ── Expand phase ────────────────────────────────────────────
        # Always runs after seed. Works on whatever prose exists after
        # the seed phase (if any). Declines rather than writes malformed
        # prose — Fix Agent contract parity.
        canonical_secs, _legacy_secs, _preamble = parse_canonical_sections(
            current_instructions or "",
        )
        present_headers = {h for h in CANONICAL_SECTION_HEADERS if h in canonical_secs}
        missing = [h for h in CANONICAL_SECTION_HEADERS if h not in present_headers]

        # Budget for Layer 1 pre-render trim — re-derived to match what
        # _expand_instructions used when calling the LLM.
        existing_length = len(current_instructions or "")
        remaining_budget = max(MAX_TEXT_INSTRUCTIONS_CHARS - existing_length, 0)
        per_section_budget = (
            remaining_budget // len(missing) if missing else remaining_budget
        )

        if not missing:
            result["expand_outcome"] = "skipped_all_sections_present"
        elif not current_instructions:
            result["expand_outcome"] = "skipped_no_existing_prose"
        else:
            try:
                new_sections = _expand_instructions(
                    metadata_snapshot, current_instructions, missing, w=w,
                )
            except Exception:
                logger.warning("Expand instructions call failed", exc_info=True)
                new_sections = {}
                result["expand_outcome"] = "llm_error"

            # ``__skip_reason__`` sentinel — LLM wasn't called because the
            # remaining char budget was below MIN_EXPAND_BUDGET.
            _expand_skip_reason = new_sections.pop("__skip_reason__", None)
            if _expand_skip_reason:
                logger.info(
                    "Expand no-op: skip_reason=%s", _expand_skip_reason,
                )
                result["expand_outcome"] = f"skipped_{_expand_skip_reason}"

            if new_sections:
                # ── Layer 1: pre-render per-section clip ────────────
                # Hard-enforce per_section_budget so the sum of section
                # bodies can never exceed remaining_budget.
                merged = dict(canonical_secs)
                for header, body in new_sections.items():
                    if header in merged:
                        continue  # never overwrite existing content
                    clipped = _trim_bullets_to_budget(body, per_section_budget)
                    if not clipped.strip():
                        continue
                    merged[header] = [
                        ln for ln in clipped.splitlines() if ln.strip()
                    ]

                rendered = render_canonical_sections(merged)

                # ── Layer 2: post-render global clip ────────────────
                # Handles rendering overhead (headers + blank lines add
                # ~15 chars per section) that Layer 1 can't see. This
                # makes over-cap merge structurally impossible.
                rendered = _trim_rendered_to_cap(
                    rendered, MAX_TEXT_INSTRUCTIONS_CHARS,
                )
                new_text = "".join(rendered).rstrip() + "\n"

                ok, errs = validate_instruction_text(new_text, strict=True)
                if not ok:
                    # Categorise the decline for the summary line.
                    err_codes: list[str] = []
                    for e in errs:
                        le = e.lower()
                        if "length" in le:
                            err_codes.append("length")
                        elif "sql detected" in le:
                            err_codes.append("sql_in_prose")
                        elif "verbatim" in le or "non-canonical" in le:
                            err_codes.append("header")
                        elif "order" in le:
                            err_codes.append("order")
                        else:
                            err_codes.append("other")
                    result["expand_outcome"] = (
                        "declined_" + "+".join(sorted(set(err_codes)))
                    )
                    logger.warning(
                        "Expand instructions: strict validation failed "
                        "(outcome=%s) — keeping existing prose. errors=%s",
                        result["expand_outcome"], errs,
                    )
                else:
                    _set_general_instructions(parsed, new_text)
                    try:
                        patch_space_config(w, space_id, parsed)
                        result["instructions_expanded"] = True
                        result["expand_outcome"] = "wrote"
                        result["expanded_sections"] = list(new_sections.keys())
                        result["instruction_chars"] = len(new_text)
                        write_patch(
                            spark, run_id, 0, 0, 1,
                            {
                                "patch_type": "proactive_instruction_expand",
                                "scope": "genie_config",
                                "risk_level": "low",
                                "target_object": "instructions.text_instructions",
                                "patch": {
                                    "added_sections": list(new_sections.keys()),
                                    "chars": len(new_text),
                                },
                                "command": None,
                                "rollback": None,
                                "proposal_id": "proactive_instruction_expand",
                            },
                            catalog, schema,
                        )
                    except Exception:
                        logger.warning(
                            "Proactive instruction expand: PATCH failed",
                            exc_info=True,
                        )
                        result["expand_outcome"] = "patch_failed"
            elif result["expand_outcome"] == "not_attempted":
                # No sections generated AND no earlier outcome set.
                result["expand_outcome"] = "llm_returned_empty"

        if not result["instructions_seeded"] and not result["instructions_expanded"]:
            result["skipped_reason"] = (
                "already_comprehensive"
                if not needs_seeding and not missing
                else "llm_failed_or_validation"
            )

        # ── Print summary with explicit outcomes (C3) ───────────────
        # Surfaces the real disposition of each phase. "declined_*" lines
        # tell operators exactly what failed validation without having to
        # correlate with an earlier WARNING.
        _lines = [_section("PROACTIVE INSTRUCTION SEEDING", "-")]
        seed_outcome = result.get("seed_outcome", "?")
        expand_outcome = result.get("expand_outcome", "?")
        if result["instructions_seeded"]:
            _lines.append(_kv("Seed", f"WROTE ({result['instruction_chars']} chars)"))
            _lines.append(_kv("  Sections", ", ".join(result["seeded_sections"]) or "(none)"))
        elif seed_outcome == "skipped_existing_prose":
            _lines.append(_kv("Seed", f"SKIPPED (existing {len(current_instructions)} chars, threshold={_INSTRUCTION_SEED_THRESHOLD})"))
        elif seed_outcome.startswith("declined"):
            _lines.append(_kv("Seed", f"DECLINED — {seed_outcome.removeprefix('declined_')}"))
        elif seed_outcome == "patch_failed":
            _lines.append(_kv("Seed", "PATCH_FAILED"))
        else:
            _lines.append(_kv("Seed", seed_outcome.upper()))
        if result["instructions_expanded"]:
            _lines.append(_kv(
                "Expand",
                f"WROTE ({len(result['expanded_sections'])} sections, {result['instruction_chars']} chars)",
            ))
            _lines.append(_kv("  Sections", ", ".join(result["expanded_sections"])))
        elif expand_outcome.startswith("declined"):
            _lines.append(_kv(
                "Expand",
                f"DECLINED — {expand_outcome.removeprefix('declined_')}",
            ))
            if missing:
                _lines.append(_kv("  Unfilled", ", ".join(missing)))
        elif expand_outcome.startswith("skipped"):
            _lines.append(_kv(
                "Expand",
                f"SKIPPED — {expand_outcome.removeprefix('skipped_')}",
            ))
        else:
            _lines.append(_kv("Expand", expand_outcome.upper()))
            if missing:
                _lines.append(_kv("  Unfilled", ", ".join(missing)))
        _lines.append(_bar("-"))
        print("\n".join(_lines))

        write_stage(
            spark, run_id, "PROACTIVE_INSTRUCTION_SEEDING", "COMPLETE",
            task_key="instruction_seeding",
            detail=result, catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("PROACTIVE_INSTRUCTION_SEEDING FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "PROACTIVE_INSTRUCTION_SEEDING", "FAILED",
            task_key="instruction_seeding",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Stage 2.96: INSTRUCTION-TO-SQL-EXPRESSION CONVERSION ─────────────


def _apply_instruction_sql_expressions(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> int:
    """Apply validated instruction-derived SQL expression candidates to the Genie Space.

    Returns the count of expressions successfully applied.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.common.genie_schema import generate_genie_id

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_SQL_EXPRESSION_CONVERSION", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    instr = metadata_snapshot.get("instructions", {})
    if not isinstance(instr, dict):
        instr = {}
    original_snippets = instr.get("sql_snippets", {})
    working_snippets = copy.deepcopy(original_snippets) if original_snippets else {}

    applied = 0
    for c in candidates:
        stype = c["snippet_type"]
        category = f"{stype}s"
        if category not in working_snippets:
            working_snippets[category] = []

        _sql_val = c["sql"]
        entry: dict = {
            "id": generate_genie_id(),
            "sql": [_sql_val] if isinstance(_sql_val, str) else _sql_val,
        }
        if c.get("display_name"):
            entry["display_name"] = c["display_name"]
        if c.get("alias") and stype != "filter":
            entry["alias"] = c["alias"]
        if c.get("synonyms"):
            entry["synonyms"] = c["synonyms"]

        # Persist the LLM-supplied "when to use this" hint. The prose
        # miner emits it as ``description``; ``instruction`` wins if
        # both are present (e.g. when the deterministic qualifier
        # backfilled instruction text upstream). Genie expects
        # ``instruction`` as a list of strings.
        instruction_value = c.get("instruction") or c.get("description") or ""
        if instruction_value:
            if isinstance(instruction_value, str):
                entry["instruction"] = [instruction_value]
            else:
                entry["instruction"] = list(instruction_value)

        working_snippets[category].append(entry)
        applied += 1

    if applied:
        patch_copy = copy.deepcopy(metadata_snapshot)
        patch_copy.setdefault("instructions", {})["sql_snippets"] = working_snippets
        try:
            patch_space_config(w, space_id, patch_copy)
            metadata_snapshot.setdefault("instructions", {})["sql_snippets"] = working_snippets
            logger.info(
                "Applied %d instruction-derived SQL expressions to space %s",
                applied, space_id,
            )
        except Exception:
            logger.warning(
                "Failed to PATCH instruction-derived SQL expressions",
                exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_SQL_EXPRESSION_CONVERSION", "COMPLETE",
        task_key="enrichment",
        detail={
            "candidates_total": len(candidates),
            "applied": applied,
        },
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → SQL EXPRESSION CONVERSION", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    for c in candidates[:5]:
        _lines.append(f"  |  {c['snippet_type']}: {c['sql'][:60]}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


# ── Stage 2.96b: INSTRUCTION-DERIVED JOIN SPECS / TABLE DESC / SYNONYMS ─


def _explain_join_candidate(
    w: WorkspaceClient,
    spark: SparkSession,
    left_identifier: str,
    right_identifier: str,
    join_sql_cond: str,
    *,
    catalog: str,
    gold_schema: str,
    warehouse_id: str = "",
) -> tuple[bool, str]:
    """Run ``EXPLAIN SELECT 1 FROM <l> JOIN <r> ON <cond> LIMIT 1``.

    Returns ``(is_valid, error_message)``. When no execution backend is
    available (both ``spark`` and ``warehouse_id`` are absent/empty), this
    short-circuits to ``(True, "")`` so unit-test runs without a Spark
    session still succeed — production runs always have one.

    The EXPLAIN guard catches re-wrap failures the arbiter-approval pre-
    filter cannot: wrong FQ prefix, type-incompatible join columns, and
    references to columns that don't exist on the named table.
    """
    from genie_space_optimizer.optimization.benchmarks import (
        _resolve_primary_table_fqn,
    )

    if not (left_identifier and right_identifier and join_sql_cond):
        return False, "missing identifier or join condition"

    left_fq = _resolve_primary_table_fqn(
        left_identifier, catalog=catalog, gold_schema=gold_schema,
    )
    right_fq = _resolve_primary_table_fqn(
        right_identifier, catalog=catalog, gold_schema=gold_schema,
    )
    if not (left_fq and right_fq):
        return False, "could not resolve FQ identifiers"

    explain_sql = (
        f"EXPLAIN SELECT 1 FROM {left_fq} JOIN {right_fq} "
        f"ON {join_sql_cond} LIMIT 1"
    )
    try:
        if w is not None and warehouse_id:
            from genie_space_optimizer.optimization.evaluation import (
                _execute_sql_via_warehouse,
            )
            _execute_sql_via_warehouse(
                w, warehouse_id, explain_sql,
                catalog=catalog, schema=gold_schema,
            )
            return True, ""
        if spark is not None:
            try:
                spark.sql(f"USE CATALOG `{catalog}`") if catalog else None
                spark.sql(f"USE SCHEMA `{gold_schema}`") if gold_schema else None
            except Exception:
                pass  # best-effort context setup; EXPLAIN error surfaces below
            spark.sql(explain_sql)
            return True, ""
        # No backend — cannot validate. Accept optimistically; the Genie
        # API PATCH will reject malformed join specs at persist time.
        return True, ""
    except Exception as exc:
        return False, f"EXPLAIN failed: {str(exc)[:200]}"


def _apply_instruction_join_specs(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> int:
    """Append instruction-derived join_spec candidates to the Genie Space.

    Mirrors the patch pattern used by :func:`_mine_and_apply_proven_joins`:
    mutate ``metadata_snapshot["instructions"]["join_specs"]`` in place and
    PATCH the whole config. Dedups against existing pairs by sorted
    (left_identifier, right_identifier).

    Runs ``EXPLAIN`` validation (via :func:`_explain_join_candidate`) on
    each candidate before persisting. The prose miner's source is user-
    asserted text (not arbiter-backed), so EXPLAIN is the critical
    persistence gate — it catches re-wrap failures such as FQ mismatch,
    type-incompatible join columns, and references to columns that don't
    exist. Candidates that fail EXPLAIN are dropped with an INFO log.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_JOIN_SPECS", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    instr = metadata_snapshot.setdefault("instructions", {})
    specs = instr.setdefault("join_specs", [])

    existing_pairs: set[tuple[str, str]] = set()
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        left = spec.get("left", {}) if isinstance(spec.get("left"), dict) else {}
        right = spec.get("right", {}) if isinstance(spec.get("right"), dict) else {}
        lt, rt = left.get("identifier", ""), right.get("identifier", "")
        if lt and rt:
            existing_pairs.add(tuple(sorted((lt, rt))))

    applied = 0
    explain_rejected = 0
    for c in candidates:
        left = c.get("left", {})
        right = c.get("right", {})
        pair = tuple(sorted((left.get("identifier", ""), right.get("identifier", ""))))
        if not all(pair) or pair in existing_pairs:
            continue

        # EXPLAIN-based exec validation — see _explain_join_candidate.
        sql_field = c.get("sql", [])
        join_cond = (
            sql_field[0] if isinstance(sql_field, list) and sql_field
            else str(sql_field or "")
        )
        is_valid, err = _explain_join_candidate(
            w, spark,
            left.get("identifier", ""), right.get("identifier", ""),
            join_cond,
            catalog=catalog, gold_schema=schema,
            warehouse_id=warehouse_id,
        )
        if not is_valid:
            logger.info(
                "Instruction join_spec rejected by EXPLAIN: %s <-> %s ON %s — %s",
                left.get("identifier", "?"), right.get("identifier", "?"),
                (join_cond or "")[:80], err,
            )
            explain_rejected += 1
            continue

        spec_entry = {
            "left": left,
            "right": right,
            "sql": c.get("sql", []),
        }
        if c.get("instruction"):
            spec_entry["instruction"] = c["instruction"]
        specs.append(spec_entry)
        existing_pairs.add(pair)
        applied += 1

    if applied:
        try:
            patch_space_config(w, space_id, metadata_snapshot)
        except Exception:
            logger.warning(
                "Instruction-derived join_specs PATCH failed", exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_JOIN_SPECS", "COMPLETE",
        task_key="enrichment",
        detail={
            "candidates_total": len(candidates),
            "applied": applied,
            "explain_rejected": explain_rejected,
        },
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → JOIN SPECS", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    if explain_rejected:
        _lines.append(_kv("Rejected (EXPLAIN)", explain_rejected))
    for c in candidates[:5]:
        left_id = c.get("left", {}).get("identifier", "?").split(".")[-1]
        right_id = c.get("right", {}).get("identifier", "?").split(".")[-1]
        _lines.append(f"  |  {left_id} ↔ {right_id}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


def _apply_instruction_table_descriptions(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> int:
    """Append instruction-derived descriptions to matching tables / metric views.

    Idempotent: if ``description_append`` already appears in the target
    description (case-insensitive), the append is skipped.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_TABLE_DESCRIPTIONS", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    ds = metadata_snapshot.setdefault("data_sources", {})
    all_sources: list[dict] = []
    if isinstance(ds, dict):
        all_sources.extend(ds.get("tables", []) or [])
        all_sources.extend(ds.get("metric_views", []) or [])

    # Index by lower(identifier) AND lower(short name) so candidates that
    # reference a table by short name still resolve.
    by_full: dict[str, dict] = {}
    by_short: dict[str, dict] = {}
    for t in all_sources:
        if not isinstance(t, dict):
            continue
        ident = (t.get("identifier") or t.get("name") or "").strip().lower()
        if ident:
            by_full[ident] = t
            by_short.setdefault(ident.split(".")[-1], t)

    def _desc_as_text(value: Any) -> str:
        # The Genie API stores ``description`` as ``list[str]`` for tables
        # and metric views, but some legacy snapshots / proactive seeds
        # write a plain string. Normalise both shapes to one comparison
        # string so dedupe / contains checks work uniformly. Mirrors the
        # ``_section_text`` helper in :mod:`applier`.
        if isinstance(value, list):
            return "\n".join(str(x) for x in value)
        if value is None:
            return ""
        return str(value)

    applied = 0
    updated_ids: list[str] = []
    for c in candidates:
        tid_lower = str(c.get("table_identifier", "")).strip().lower()
        desc_append = str(c.get("description_append", "")).strip()
        if not tid_lower or not desc_append:
            continue
        tbl = by_full.get(tid_lower) or by_short.get(tid_lower.split(".")[-1])
        if not tbl:
            continue
        existing_value = tbl.get("description", "") or ""
        existing_text = _desc_as_text(existing_value)
        if desc_append.lower() in existing_text.lower():
            continue  # idempotent — already present
        # Preserve the input shape: list-shaped descriptions stay
        # ``list[str]`` (the Genie API contract), string-shaped stay str.
        # Writing back a Python ``repr`` of a list (the prior bug) caused
        # PATCH to reject with "Expected an array for description".
        if isinstance(existing_value, list):
            tbl["description"] = list(existing_value) + [desc_append]
        else:
            new_desc = (
                existing_text
                + ("\n" if existing_text and not existing_text.endswith("\n") else "")
                + desc_append
            ).strip()
            tbl["description"] = new_desc
        updated_ids.append(tbl.get("identifier", tbl.get("name", "?")))
        applied += 1

    if applied:
        try:
            patch_space_config(w, space_id, metadata_snapshot)
        except Exception:
            logger.warning(
                "Instruction-derived table descriptions PATCH failed",
                exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_TABLE_DESCRIPTIONS", "COMPLETE",
        task_key="enrichment",
        detail={"candidates_total": len(candidates), "applied": applied},
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → TABLE DESCRIPTIONS", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    for tid in updated_ids[:5]:
        _lines.append(f"  |  {tid}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


def _apply_instruction_column_synonyms(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    candidates: list[dict],
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> int:
    """Merge instruction-derived column synonyms into ``column_configs``.

    Dedup is case-insensitive on synonym strings. Missing column_configs
    rows are created on the fly so the synonym survives the PATCH round-trip.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config

    if not candidates:
        return 0

    write_stage(
        spark, run_id, "INSTRUCTION_COLUMN_SYNONYMS", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    ds = metadata_snapshot.setdefault("data_sources", {})
    all_sources: list[dict] = []
    if isinstance(ds, dict):
        all_sources.extend(ds.get("tables", []) or [])
        all_sources.extend(ds.get("metric_views", []) or [])

    by_full: dict[str, dict] = {}
    by_short: dict[str, dict] = {}
    for t in all_sources:
        if not isinstance(t, dict):
            continue
        ident = (t.get("identifier") or t.get("name") or "").strip().lower()
        if ident:
            by_full[ident] = t
            by_short.setdefault(ident.split(".")[-1], t)

    applied = 0
    for c in candidates:
        tid_lower = str(c.get("table_identifier", "")).strip().lower()
        col_name = str(c.get("column_name", "")).strip()
        synonyms = c.get("synonyms", []) or []
        if not tid_lower or not col_name or not synonyms:
            continue
        tbl = by_full.get(tid_lower) or by_short.get(tid_lower.split(".")[-1])
        if not tbl:
            continue
        cc_list = tbl.setdefault("column_configs", [])
        if not isinstance(cc_list, list):
            cc_list = []
            tbl["column_configs"] = cc_list
        col_entry: dict | None = None
        for cc in cc_list:
            if isinstance(cc, dict) and (cc.get("column_name", "") or "").strip().lower() == col_name.lower():
                col_entry = cc
                break
        if col_entry is None:
            col_entry = {"column_name": col_name, "synonyms": []}
            cc_list.append(col_entry)
        existing_syns = col_entry.setdefault("synonyms", []) or []
        if not isinstance(existing_syns, list):
            existing_syns = list(existing_syns)
        existing_lower = {str(s).strip().lower() for s in existing_syns}
        changed = False
        for syn in synonyms:
            s = str(syn).strip()
            if s and s.lower() not in existing_lower:
                existing_syns.append(s)
                existing_lower.add(s.lower())
                changed = True
        col_entry["synonyms"] = existing_syns
        if changed:
            applied += 1

    if applied:
        try:
            patch_space_config(w, space_id, metadata_snapshot)
        except Exception:
            logger.warning(
                "Instruction-derived column synonyms PATCH failed",
                exc_info=True,
            )
            applied = 0

    write_stage(
        spark, run_id, "INSTRUCTION_COLUMN_SYNONYMS", "COMPLETE",
        task_key="enrichment",
        detail={"candidates_total": len(candidates), "applied": applied},
        catalog=catalog, schema=schema,
    )

    _lines = [_section("INSTRUCTION → COLUMN SYNONYMS", "-")]
    _lines.append(_kv("Candidates", len(candidates)))
    _lines.append(_kv("Applied", applied))
    for c in candidates[:5]:
        _lines.append(f"  |  {c.get('table_identifier', '?').split('.')[-1]}.{c.get('column_name')}: {c.get('synonyms', [])[:3]}")
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    return applied


# ── Stage 2.96c: Instruction prose mining orchestrator ─────────────


def _run_instruction_prose_mining(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
) -> dict:
    """Stage 5a (Task C.5): multi-target prose miner + rewrite pipeline.

    Runs BEFORE the proactive seed/expand phase so legacy ALL-CAPS prose is
    promoted into structured config and normalised into the canonical 5-
    section form before seed/expand run. Idempotent: a second pass on
    already-normalised prose finds nothing to promote and the rewrite
    step returns ``SKIP_NO_CHANGE``.

    Source-specific invariant:
        This path persists sql_snippets / join_specs / example_qsqls from
        USER-ASSERTED prose (``text_instructions``). User prose is its
        own authority — no arbiter gate is available (or possible) at
        the source. Persistence gates:

        - sql_snippet: ``validate_sql_snippet`` EXPLAIN+execute.
        - join_spec: ``_explain_join_candidate`` EXPLAIN-only.
        - example_qsql: benchmark-leakage firewall (answer-shape path)
          + example-SQL ground-truth validator.
        - table_desc / column_synonym: shape + dedup only.
        - keep_in_prose: schema-validator only (stays in prose).

        The Bug #4 firewall is intentionally not applied to sql_snippet
        or join_spec outputs (see scoping comment in
        ``leakage._PATCH_TEXT_FIELDS``). Structural primitives are not
        answers, so firewall fingerprint-matching is inappropriate here.

    Pipeline:

    1. ``_convert_instructions_to_sql_expressions`` (multi-target miner).
    2. Per-target appliers (``_apply_instruction_*``) for every non-empty
       bucket.
    3. ``rewrite_instructions_from_miner_output`` — span-based removal +
       canonical regrouping + strict validation; on ``DECLINE_MALFORMED``
       the original prose is preserved (Fix Agent contract parity).
    4. Emit ``set_text_instructions`` via ``_set_general_instructions``
       + ``patch_space_config`` when ``RewriteResult.WRITE``.

    Returns a dict with per-target applied counts + the raw miner stats so
    the caller can decide whether to refetch config. Never raises — any
    downstream failure is logged and treated as a soft miss.
    """
    from genie_space_optimizer.common.genie_client import (
        patch_space_config as _patch,
    )
    from genie_space_optimizer.optimization.applier import (
        RewriteResult, _get_general_instructions,
        _set_general_instructions,
        rewrite_instructions_from_miner_output,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _convert_instructions_to_sql_expressions,
    )

    miner_result = _convert_instructions_to_sql_expressions(
        metadata_snapshot, w=w,
        spark=spark, catalog=catalog, gold_schema=schema,
        warehouse_id=warehouse_id,
    )
    miner_stats = miner_result.get("stats", {})

    sql_applied = join_applied = example_applied = desc_applied = synonym_applied = 0
    applied_spans: list[str] = []

    def _collect_spans(bucket_name: str) -> None:
        applied_spans.extend(
            c.get("source_span", "")
            for c in miner_result.get(bucket_name, [])
            if c.get("source_span")
        )

    if miner_result.get("sql_snippet"):
        sql_applied = _apply_instruction_sql_expressions(
            w, spark, run_id, space_id, miner_result["sql_snippet"],
            metadata_snapshot, catalog, schema,
        )
        if sql_applied:
            _collect_spans("sql_snippet")

    if miner_result.get("join_spec"):
        join_applied = _apply_instruction_join_specs(
            w, spark, run_id, space_id, miner_result["join_spec"],
            metadata_snapshot, catalog, schema,
            warehouse_id=warehouse_id,
        )
        if join_applied:
            _collect_spans("join_spec")

    if miner_result.get("example_qsql"):
        # Reuse the existing proactive-example-sql applier shape.
        _example_proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": c["question"],
                "example_sql": c["sql"],
                "usage_guidance": c.get("usage_guidance", ""),
                "risk_level": "low",
            }
            for c in miner_result["example_qsql"]
        ]
        if _example_proposals:
            try:
                _apply_proactive_example_sqls(
                    w, spark, run_id, space_id, _example_proposals,
                    metadata_snapshot, config, catalog, schema,
                    benchmarks=benchmarks,
                )
                example_applied = len(_example_proposals)
                _collect_spans("example_qsql")
            except Exception:
                logger.warning(
                    "Instruction-derived example SQLs: apply failed",
                    exc_info=True,
                )

    if miner_result.get("table_desc"):
        desc_applied = _apply_instruction_table_descriptions(
            w, spark, run_id, space_id, miner_result["table_desc"],
            metadata_snapshot, catalog, schema,
        )
        if desc_applied:
            _collect_spans("table_desc")

    if miner_result.get("column_synonym"):
        synonym_applied = _apply_instruction_column_synonyms(
            w, spark, run_id, space_id, miner_result["column_synonym"],
            metadata_snapshot, catalog, schema,
        )
        if synonym_applied:
            _collect_spans("column_synonym")

    # Span-based prose rewrite: remove promoted spans, regroup keep_in_prose
    # spans under canonical headers, validate strictly, emit the op or decline.
    total_applied = (
        sql_applied + join_applied + example_applied
        + desc_applied + synonym_applied
    )
    keep_in_prose_spans = miner_result.get("keep_in_prose", []) or []
    rewrite_outcome = "not_run"
    if total_applied or keep_in_prose_spans:
        _original_instr = _get_general_instructions(metadata_snapshot)
        outcome, new_instr, rewrite_errors = rewrite_instructions_from_miner_output(
            _original_instr, applied_spans, keep_in_prose_spans,
        )
        rewrite_outcome = outcome
        if outcome == RewriteResult.WRITE and new_instr:
            try:
                _set_general_instructions(metadata_snapshot, new_instr)
                _patch(w, space_id, metadata_snapshot)
                logger.info(
                    "miner.rewrite.applied chars_before=%d chars_after=%d space_id=%s",
                    len(_original_instr), len(new_instr), space_id,
                )
            except Exception:
                logger.warning("miner.rewrite.patch_failed", exc_info=True)
                rewrite_outcome = "patch_failed"
        elif outcome == RewriteResult.DECLINE_MALFORMED:
            # Fix Agent decline contract: keep original prose, emit a
            # decline-shaped log line for triage.
            logger.warning(
                "miner.rewrite.declined reason=malformed errors=%s space_id=%s",
                rewrite_errors[:5], space_id,
            )
        else:  # SKIP_NO_CHANGE
            logger.info(
                "miner.rewrite.skipped reason=no_change space_id=%s",
                space_id,
            )

    _miner_lines = [_section("INSTRUCTION PROSE MINING & PROMOTION", "-")]
    _miner_lines.append(_kv("Candidates total", miner_stats.get("candidates_total", 0)))
    _miner_lines.append(_kv("SQL snippets", sql_applied))
    _miner_lines.append(_kv("Join specs", join_applied))
    _miner_lines.append(_kv("Example SQLs", example_applied))
    _miner_lines.append(_kv("Table descriptions", desc_applied))
    _miner_lines.append(_kv("Column synonyms", synonym_applied))
    _miner_lines.append(_kv("Kept in prose", len(keep_in_prose_spans)))
    _miner_lines.append(_kv("Rewrite", rewrite_outcome))
    if miner_stats.get("rejected_by_reason"):
        _miner_lines.append(_kv(
            "Rejected",
            ", ".join(
                f"{k}={v}" for k, v in miner_stats["rejected_by_reason"].items()
            ),
        ))
    _miner_lines.append(_bar("-"))
    print("\n".join(_miner_lines))

    return {
        "sql_applied": sql_applied,
        "join_applied": join_applied,
        "example_applied": example_applied,
        "desc_applied": desc_applied,
        "synonym_applied": synonym_applied,
        "total_applied": total_applied,
        "keep_in_prose_count": len(keep_in_prose_spans),
        "rewrite_outcome": rewrite_outcome,
        "stats": miner_stats,
    }


# ── Stage 2.97: PROACTIVE SQL EXPRESSION SEEDING ─────────────────────


_SNIPPET_TYPE_FROM_KEY = {
    "measures": "measure",
    "filters": "filter",
    "expressions": "expression",
}


def _repair_existing_sql_snippets(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> dict:
    """Normalize every existing SQL snippet to fully-qualified form.

    Runs unconditionally — unlike :func:`_seed_new_sql_snippets`, which is
    gated by the remaining SQL-snippet headroom (see that function). This
    is the remediation
    path for spaces whose stored snippets were produced by an older
    GSO version (or by the lever loop before A.1) and still use
    short-form prefixes. The Genie serving path rejects such snippets; the
    failing run in the bug report is exactly this scenario.

    Algorithm:

    - Iterate ``instructions.sql_snippets.{measures,filters,expressions}``.
    - For each snippet, call :func:`normalize_sql_snippet` (no execution
      check; EXPLAIN only). If the normalized form differs from the
      stored form, replace the snippet's ``sql`` field in-place.
    - PATCH the space config once at the end if anything changed.

    Returns ``{"scanned": N, "rewritten": M, "rejected": K}``.
    """
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.optimization.benchmarks import normalize_sql_snippet

    result: dict = {"scanned": 0, "rewritten": 0, "rejected": 0}

    parsed = config.get("_parsed_space", config)
    existing_snippets = parsed.get("instructions", {}).get("sql_snippets", {})
    if not isinstance(existing_snippets, dict):
        existing_snippets = {}

    if not any(existing_snippets.get(k) for k in _SNIPPET_TYPE_FROM_KEY):
        # Nothing to repair — print a terse line so the run log stays tidy.
        _lines = [_section("SQL EXPRESSION REPAIR", "-")]
        _lines.append(_kv("Scanned", 0))
        _lines.append(_kv("Status", "No existing snippets"))
        _lines.append(_bar("-"))
        print("\n".join(_lines))
        return result

    write_stage(
        spark, run_id, "SQL_EXPRESSION_REPAIR", "STARTED",
        task_key="sql_expression_repair", catalog=catalog, schema=schema,
    )

    working = copy.deepcopy(existing_snippets)
    any_change = False
    sample_warnings: list[str] = []

    for type_key, snippet_type in _SNIPPET_TYPE_FROM_KEY.items():
        for snippet in (working.get(type_key) or []):
            sql_val = snippet.get("sql", [])
            if isinstance(sql_val, list) and sql_val:
                original = str(sql_val[0])
            else:
                original = str(sql_val) if sql_val else ""
            if not original:
                continue
            result["scanned"] += 1
            try:
                normalized, warnings = normalize_sql_snippet(
                    original, snippet_type, metadata_snapshot,
                    catalog=catalog, gold_schema=schema,
                    spark=spark, w=w, warehouse_id=warehouse_id,
                )
            except Exception as exc:
                logger.warning(
                    "SQL expression repair: normalize failed for %s: %s",
                    original[:80], exc,
                )
                result["rejected"] += 1
                continue
            explain_failed = any(
                warn.startswith("EXPLAIN failed:") for warn in warnings
            )
            if explain_failed:
                # Don't rewrite one invalid form into another — leave it
                # alone so the lever loop can later propose a real fix.
                result["rejected"] += 1
                if warnings and len(sample_warnings) < 5:
                    sample_warnings.extend(warnings[:2])
                continue
            if normalized != original:
                snippet["sql"] = [normalized]
                any_change = True
                result["rewritten"] += 1
            elif warnings and len(sample_warnings) < 5:
                # Informational warnings (ambiguity, unknown table) — keep
                # a small sample for debugging in the run log.
                sample_warnings.extend(warnings[:1])

    if any_change:
        patch_copy = copy.deepcopy(parsed)
        patch_copy.setdefault("instructions", {})["sql_snippets"] = working
        try:
            patch_space_config(w, space_id, patch_copy)
            parsed.setdefault("instructions", {})["sql_snippets"] = working
        except Exception:
            logger.warning("SQL expression repair: PATCH failed", exc_info=True)
            result["rewritten"] = 0
            write_stage(
                spark, run_id, "SQL_EXPRESSION_REPAIR", "FAILED",
                task_key="sql_expression_repair",
                error_message="PATCH failed", catalog=catalog, schema=schema,
            )
            return result

    _lines = [_section("SQL EXPRESSION REPAIR", "-")]
    _lines.append(_kv("Scanned", result["scanned"]))
    _lines.append(_kv("Rewritten", result["rewritten"]))
    _lines.append(_kv("Rejected", result["rejected"]))
    for warning in sample_warnings[:5]:
        _lines.append(_kv("  warn", warning[:120]))
    _lines.append(_bar("-"))
    print("\n".join(_lines))

    write_stage(
        spark, run_id, "SQL_EXPRESSION_REPAIR", "COMPLETE",
        task_key="sql_expression_repair",
        detail=result, catalog=catalog, schema=schema,
    )
    return result


def _format_sql_expression_seeding_summary(result: dict) -> list[str]:
    """Render the SQL EXPRESSION SEEDING summary block as printable lines.

    Extracted so MV-validation sub-bucket rendering has a unit-testable
    surface; the in-flight seeding loop calls this with the final result
    dict.
    """
    lines = [_section("SQL EXPRESSION SEEDING", "-")]
    lines.append(_kv("Candidates evaluated", result["total_candidates"]))
    rebind_dropped = result.get("rebind_dropped", 0) or 0
    if rebind_dropped:
        lines.append(_kv("  Alias-rebind dropped", rebind_dropped, indent=4))
        for ex in (result.get("rebind_dropped_examples") or [])[:3]:
            lines.append(_kv(f"    e.g. {ex}", "", indent=6))
    lines.append(_kv("Seeded", result["total_seeded"]))
    lines.append(_kv("  Measures", result["measures_seeded"]))
    lines.append(_kv("  Filters", result["filters_seeded"]))
    lines.append(_kv("  Expressions", result["expressions_seeded"]))
    lines.append(_kv("Rejected", result["total_rejected"]))
    lines.append(_kv("  Firewall (leakage)", result["firewall_rejected"]))
    lines.append(_kv("  Validation (EXPLAIN)", result["validation_rejected"]))
    subbuckets = result.get("validation_subbuckets") or {}
    examples = result.get("validation_subbucket_examples") or {}
    if isinstance(subbuckets, dict) and subbuckets:
        for reason, count in sorted(subbuckets.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(_kv(f"    {reason}", count, indent=6))
            for ex in (examples.get(reason) or [])[:3]:
                if not isinstance(ex, dict):
                    continue
                sql_prefix = str(ex.get("sql_prefix") or "")[:120]
                error = str(ex.get("error") or "")[:120]
                lines.append(f"|       [{reason}] {sql_prefix} — {error}")
    lines.append(_kv("  Ngram duplicate", result["ngram_rejected"]))
    for ex in result.get("rejected_examples", [])[:3]:
        lines.append(
            f"|   [{ex.get('gate')}] {ex.get('snippet_type')} "
            f"{ex.get('sql_prefix')} — {ex.get('reason')}"
        )
    lines.append(_bar("-"))
    return lines


def _seed_new_sql_snippets(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    benchmarks: list[dict],
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> dict:
    """Mine and apply new SQL Expressions (measures, filters, dimensions).

    Gated by the remaining per-space SQL-snippet headroom:

        headroom = MAX_SQL_SNIPPETS - existing_count - LEVER_RESERVE

    When ``headroom == 0`` this step skips entirely (repair still runs,
    see :func:`_repair_existing_sql_snippets`). ``LEVER_RESERVE`` holds
    back ~25% of the 200-snippet budget for the lever loop.

    Source-specific invariant:
        This path persists sql_snippets from ARBITER-APPROVED benchmark
        rows only (verdict == ``both_correct``, pre-filtered by the
        caller via ``_extract_arbiter_approved_benchmarks``), plus
        schema-discovery heuristics. Every candidate is EXPLAIN+execute
        validated via ``validate_sql_snippet`` before being applied.

        The Bug #4 benchmark-leakage firewall is NOT applied here —
        the arbiter pre-filter is the source gate; adding the firewall
        would double-gate and reject legitimate structural primitives
        whose fingerprints happen to match the benchmark corpus (see
        scoping comment above ``_PATCH_TEXT_FIELDS`` in leakage.py).
    """
    import json as _json

    from genie_space_optimizer.common.config import (
        SQL_EXPRESSION_SEEDING_LEVER_RESERVE,
        SQL_EXPRESSION_SEEDING_MAX_CANDIDATES,
    )
    from genie_space_optimizer.common.genie_client import patch_space_config
    from genie_space_optimizer.common.genie_schema import (
        MAX_SQL_SNIPPETS,
        count_sql_snippets,
        generate_genie_id,
    )
    from genie_space_optimizer.optimization.benchmarks import validate_sql_snippet
    from genie_space_optimizer.optimization.optimizer import (
        _discover_schema_sql_expressions,
        _enrich_candidates_with_llm,
        _format_existing_sql_snippets,
        _mine_sql_expression_candidates,
        _ngram_similarity,
    )

    # ── Source-specific invariant ───────────────────────────────────────
    # This path persists sql_snippets from arbiter-approved benchmarks
    # only (``both_correct`` verdict, filtered by the caller via
    # ``_extract_arbiter_approved_benchmarks``) OR from schema-discovery
    # heuristics. Every candidate still goes through EXPLAIN+execute via
    # ``validate_sql_snippet`` below.
    #
    # The Bug #4 benchmark-leakage firewall is deliberately NOT run here
    # (see leakage.py ``_PATCH_TEXT_FIELDS`` scoping comment). The pre-
    # mining arbiter filter is the source gate; the firewall would
    # double-gate and reject legitimate structural primitives like
    # ``SUM(revenue)`` whose fingerprint happens to match a benchmark.

    result: dict = {
        "total_candidates": 0,
        "total_seeded": 0,
        "total_rejected": 0,
        # ``firewall_rejected`` retained as a zero for back-compat with
        # observability consumers reading this shape; the firewall no
        # longer runs in this path.
        "firewall_rejected": 0,
        "validation_rejected": 0,         # EXPLAIN / execution validator
        "validation_subbuckets": {},
        "validation_subbucket_examples": {},
        "ngram_rejected": 0,              # duplicate of an already-seeded snippet
        "measures_seeded": 0,
        "filters_seeded": 0,
        "expressions_seeded": 0,
        "skipped_reason": None,
        # Per-candidate rejection diagnostics — bounded list so the
        # pretty summary block can explain WHY candidates died without
        # operators having to grep INFO logs. Shape: list of
        # ``{sql_prefix, snippet_type, gate, reason}``.
        "rejected_examples": [],
    }

    # Bounded so a pathological pool doesn't balloon the result dict /
    # write_stage payload. 10 is plenty for diagnostics; the summary
    # renders at most 3.
    _MAX_REJECTED_EXAMPLES = 10

    def _record_rejection(
        sql_raw: str, snippet_type: str, gate: str, reason: str,
    ) -> None:
        if len(result["rejected_examples"]) >= _MAX_REJECTED_EXAMPLES:
            return
        result["rejected_examples"].append({
            "sql_prefix": (sql_raw or "")[:120],
            "snippet_type": snippet_type,
            "gate": gate,
            "reason": (reason or "")[:200],
        })

    def _record_validation_subbucket(sql_raw: str, err: str) -> None:
        try:
            from genie_space_optimizer.optimization.evaluation import (
                _classify_sql_validation_error,
            )
            reason = _classify_sql_validation_error(err or "")
        except Exception:
            reason = "sql_compile_error"
        sub = result.setdefault("validation_subbuckets", {})
        sub[reason] = sub.get(reason, 0) + 1
        examples = result.setdefault("validation_subbucket_examples", {})
        ex_list = examples.setdefault(reason, [])
        if len(ex_list) < 3:
            ex_list.append({
                "sql_prefix": (sql_raw or "")[:120],
                "error": (err or "")[:200],
            })

    parsed = config.get("_parsed_space", config)
    existing_snippets = parsed.get("instructions", {}).get("sql_snippets", {})
    if not isinstance(existing_snippets, dict):
        existing_snippets = {}

    # Headroom-based gate (replaces the old 5-snippet skip threshold).
    #
    #   headroom = MAX_SQL_SNIPPETS (200) - existing_sql_snippets - LEVER_RESERVE
    #
    # The ``LEVER_RESERVE`` (default 50) holds back budget for the lever
    # loop's iterative additions later in the optimisation run. Seeding
    # only contributes up to ``headroom``; when headroom hits zero we
    # skip seeding entirely so the lever loop retains its runway.
    #
    # ``count_sql_snippets`` intentionally counts only the three
    # sql_snippet buckets (measures + filters + expressions). The
    # Databricks docs' 200-limit also covers join_specs + table
    # descriptions, but the code's validator enforces a different
    # split today; reconciling that is tracked as a separate issue.
    existing_count = count_sql_snippets(parsed)
    headroom = max(
        0,
        MAX_SQL_SNIPPETS - existing_count - SQL_EXPRESSION_SEEDING_LEVER_RESERVE,
    )
    remaining_snippet_budget = headroom  # loop-level cap (was: MAX - existing)

    if headroom == 0:
        _lines = [_section("SQL EXPRESSION SEEDING", "-")]
        _lines.append(_kv("Existing snippets", existing_count))
        _lines.append(_kv(
            "Status",
            f"Skipped (insufficient headroom: "
            f"existing={existing_count}, "
            f"reserve={SQL_EXPRESSION_SEEDING_LEVER_RESERVE}, "
            f"cap={MAX_SQL_SNIPPETS})",
        ))
        _lines.append(_bar("-"))
        print("\n".join(_lines))
        result["skipped_reason"] = "insufficient_headroom_for_seeding"
        return result

    write_stage(
        spark, run_id, "SQL_EXPRESSION_SEEDING", "STARTED",
        task_key="sql_expression_seeding", catalog=catalog, schema=schema,
    )

    try:
        benchmark_candidates = _mine_sql_expression_candidates(benchmarks, metadata_snapshot)
        # Phase 3.R7: expose the alias-rebind drop counts on ``result``
        # so the pretty summary can attribute why candidate N dropped to
        # N-k (undeclared alias in the source benchmark query).
        result["rebind_dropped"] = getattr(
            _mine_sql_expression_candidates, "last_rebind_dropped", 0,
        )
        result["rebind_dropped_examples"] = getattr(
            _mine_sql_expression_candidates, "last_rebind_dropped_examples", [],
        )
        schema_candidates = _discover_schema_sql_expressions(metadata_snapshot)

        all_candidates = benchmark_candidates + schema_candidates

        seen_sqls: set[str] = set()
        deduped: list[dict] = []
        for c in all_candidates:
            key = c["sql"].lower()
            if any(_ngram_similarity(key, s) > 0.85 for s in seen_sqls):
                continue
            seen_sqls.add(key)
            deduped.append(c)

        deduped = deduped[:SQL_EXPRESSION_SEEDING_MAX_CANDIDATES]
        result["total_candidates"] = len(deduped)

        if not deduped:
            _lines = [_section("SQL EXPRESSION SEEDING", "-")]
            _lines.append(_kv("Candidates found", 0))
            _lines.append(_kv("Status", "No candidates"))
            _lines.append(_bar("-"))
            print("\n".join(_lines))
            write_stage(
                spark, run_id, "SQL_EXPRESSION_SEEDING", "COMPLETE",
                task_key="sql_expression_seeding",
                detail=result, catalog=catalog, schema=schema,
            )
            return result

        deduped = _enrich_candidates_with_llm(deduped, metadata_snapshot, w=w)

        existing_sql_set: set[str] = set()
        for snippet_type in ("measures", "filters", "expressions"):
            for item in (existing_snippets.get(snippet_type, []) or []):
                sql_val = item.get("sql", [])
                sql_str = sql_val[0] if isinstance(sql_val, list) and sql_val else str(sql_val)
                existing_sql_set.add(sql_str.lower())

        applied_snippets: list[dict] = []
        type_key_map = {"measure": "measures", "filter": "filters", "expression": "expressions"}
        working_snippets_seed = copy.deepcopy(existing_snippets) if existing_snippets else {}

        for candidate in deduped:
            sql_raw = candidate["sql"]
            snippet_type = candidate["snippet_type"]

            if len(applied_snippets) >= remaining_snippet_budget:
                logger.info(
                    "SQL expression seeding: stopping — snippet budget exhausted (%d/%d)",
                    existing_count + len(applied_snippets), MAX_SQL_SNIPPETS,
                )
                break

            if any(_ngram_similarity(sql_raw.lower(), e) > 0.85 for e in existing_sql_set):
                result["ngram_rejected"] += 1
                result["total_rejected"] += 1
                _record_rejection(
                    sql_raw, snippet_type, "ngram",
                    "duplicate of an already-seeded snippet (>0.85 similarity)",
                )
                continue

            # Bug #4 firewall REMOVED at this call site — the mining source
            # has already been filtered to arbiter-approved baseline rows
            # (verdict == ``both_correct``, see
            # ``_extract_arbiter_approved_benchmarks``) which guarantees
            # the benchmark's ``expected_sql`` is gold-standard, and
            # ``validate_sql_snippet`` below still runs EXPLAIN+execute
            # against the warehouse. Adding the firewall here would
            # double-gate and reject legitimate structural patterns
            # (``SUM(revenue)`` is STRUCTURE, not an ANSWER — see
            # docstring above ``_PATCH_TEXT_FIELDS`` in leakage.py).
            _valid_result = validate_sql_snippet(
                sql_raw, snippet_type, metadata_snapshot,
                spark=spark, catalog=catalog, gold_schema=schema,
                w=w, warehouse_id=warehouse_id,
            )
            is_valid = _valid_result[0]
            err = _valid_result[1]
            prefixed_sql = _valid_result[2] if len(_valid_result) > 2 else sql_raw
            if not is_valid:
                logger.info("SQL expression candidate rejected: %s — %s", sql_raw[:80], err)
                result["validation_rejected"] += 1
                _record_validation_subbucket(sql_raw, err or "")
                result["total_rejected"] += 1
                _record_rejection(sql_raw, snippet_type, "validation", err or "")
                continue

            snippet_entry = {
                "id": generate_genie_id(),
                "sql": [prefixed_sql],
                "display_name": candidate.get("display_name", ""),
                "synonyms": candidate.get("synonyms", []),
                "instruction": [candidate.get("instruction", "")] if candidate.get("instruction") else [],
            }
            if candidate.get("alias") and snippet_type != "filter":
                snippet_entry["alias"] = candidate["alias"]

            type_key = type_key_map[snippet_type]
            items = working_snippets_seed.setdefault(type_key, [])
            items.append(snippet_entry)

            applied_snippets.append({
                "snippet_type": snippet_type,
                "type_key": type_key,
                "sql": sql_raw,
                "display_name": candidate.get("display_name", ""),
                "target_table": candidate.get("target_table", "sql_snippet"),
                "snippet_id": snippet_entry["id"],
            })

            count_key = f"{type_key}_seeded"
            result[count_key] = result.get(count_key, 0) + 1
            existing_sql_set.add(sql_raw.lower())

        if applied_snippets:
            patch_copy = copy.deepcopy(parsed)
            patch_copy.setdefault("instructions", {})["sql_snippets"] = working_snippets_seed
            try:
                patch_space_config(w, space_id, patch_copy)
                parsed.setdefault("instructions", {})["sql_snippets"] = working_snippets_seed
            except Exception:
                logger.warning("SQL expression seeding: PATCH failed", exc_info=True)
                result["total_seeded"] = 0
                result["measures_seeded"] = 0
                result["filters_seeded"] = 0
                result["expressions_seeded"] = 0
                write_stage(
                    spark, run_id, "SQL_EXPRESSION_SEEDING", "FAILED",
                    task_key="sql_expression_seeding",
                    error_message="PATCH failed",
                    catalog=catalog, schema=schema,
                )
                return result

            for idx, snippet_entry in enumerate(applied_snippets):
                write_patch(
                    spark, run_id, 0, 0, idx,
                    {
                        "patch_type": "proactive_sql_expression",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": snippet_entry.get("target_table", "sql_snippet"),
                        "patch": {
                            "snippet_type": snippet_entry["snippet_type"],
                            "sql": snippet_entry["sql"],
                            "display_name": snippet_entry["display_name"],
                        },
                        "command": None,
                        "rollback": None,
                        "proposal_id": f"proactive_sql_expr_{idx}",
                    },
                    catalog, schema,
                )

        result["total_seeded"] = len(applied_snippets)

        print("\n".join(_format_sql_expression_seeding_summary(result)))

        write_stage(
            spark, run_id, "SQL_EXPRESSION_SEEDING", "COMPLETE",
            task_key="sql_expression_seeding",
            detail=result, catalog=catalog, schema=schema,
        )
        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("SQL_EXPRESSION_SEEDING FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "SQL_EXPRESSION_SEEDING", "FAILED",
            task_key="sql_expression_seeding",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


def _run_sql_expression_seeding(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    benchmarks: list[dict],
    catalog: str,
    schema: str,
    *,
    warehouse_id: str = "",
) -> dict:
    """Decoupled repair + seed: always repair existing, seed only when thin.

    This function intentionally keeps its name and call signature to stay
    compatible with all existing callers. The two sub-steps:

    1. :func:`_repair_existing_sql_snippets` runs unconditionally and
       normalizes every stored snippet to fully-qualified form. This
       fixes the bug that caused the failing run — snippets stored with
       short-form prefixes were rejected by Genie's serving path.
    2. :func:`_seed_new_sql_snippets` runs the legacy threshold-gated
       seeding path. No behaviour change.

    Returns a backward-compatible dict that carries both old flat keys
    (``total_candidates`` / ``total_seeded`` / …, used by callers that
    branch on whether new snippets were added) **and** a nested
    ``"repair"`` / ``"seed"`` structure for observability.
    """
    repair_result = _repair_existing_sql_snippets(
        w, spark, run_id, space_id, config=config,
        metadata_snapshot=metadata_snapshot,
        catalog=catalog, schema=schema,
        warehouse_id=warehouse_id,
    )
    # If repair rewrote any snippets, downstream stages need the fresh
    # snapshot. The harness-level caller already refetches after this
    # whole stage when new snippets are seeded; repair feeds the same
    # path because we mutated ``parsed`` in place inside the repair.
    seed_result = _seed_new_sql_snippets(
        w, spark, run_id, space_id, config=config,
        metadata_snapshot=metadata_snapshot,
        benchmarks=benchmarks,
        catalog=catalog, schema=schema,
        warehouse_id=warehouse_id,
    )
    # Preserve legacy flat shape for callers that look at total_seeded /
    # total_candidates; attach the structured summary alongside.
    return {
        **seed_result,
        "repair": repair_result,
        "seed": seed_result,
    }


# ── Stage 2.5b: PREPARE LEVER LOOP ──────────────────────────────────


def _prepare_lever_loop(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    *,
    benchmarks: list[dict] | None = None,
) -> dict:
    """Load Genie Space config, enrich UC metadata, run Stage 2.5 prompt matching.

    Consolidates all config preparation needed before the lever loop into a
    single function used by both the DAG notebook and the convenience wrapper.

    Steps:
      1. Load config from Delta snapshot (API fetch fallback).
      2. Fetch UC column metadata via REST API (for prompt matching + type
         enrichment in the lever loop).
      3. Print detailed inventory diagnostic (tables, cols, FA/VD stats).
      4. Run Stage 2.5 prompt matching auto-config (if enabled).
      5. If changes applied: entity-matching-aware propagation wait + diagnostic.
      6. Post-wait config refresh from API.
      7. If no changes or prompt matching disabled: skip wait entirely.

    Non-fatal: exceptions in UC fetch or prompt matching are logged and
    swallowed so the lever loop can still proceed.

    Returns the fully prepared config dict with ``_uc_columns`` populated.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.common.uc_metadata import (
        extract_genie_space_table_refs,
        get_columns_for_tables_rest,
    )

    # ── 1. Load config from Delta snapshot (API fetch fallback) ──────
    run_data = load_run(spark, run_id, catalog, schema) or {}
    snapshot = run_data.get("config_snapshot", {})
    config: dict
    if isinstance(snapshot, dict) and snapshot:
        config = snapshot
        logger.info("Lever loop: using config snapshot from run row for %s", run_id)
    else:
        logger.warning(
            "No config snapshot found in run row for %s — fetching from API.",
            run_id,
        )
        config = fetch_space_config(w, space_id)
        logger.info("Lever loop: fetched config for space %s", space_id)

    logger.info(
        "Lever loop config loaded: keys=%s",
        sorted(list(config.keys()))[:20] if isinstance(config, dict) else [],
    )

    # ── 2. Fetch UC column metadata via REST API ─────────────────────
    table_refs: list = []
    try:
        table_refs = extract_genie_space_table_refs(config)
        uc_columns = get_columns_for_tables_rest(w, table_refs) if table_refs else []
        config["_uc_columns"] = uc_columns
        logger.info(
            "Lever loop: fetched %d UC columns across %d tables",
            len(uc_columns), len(table_refs),
        )
    except Exception as exc:
        logger.warning(
            "UC column metadata fetch failed for %s (non-fatal): %s",
            run_id, exc,
        )
        uc_columns = config.get("_uc_columns", [])

    # ── 2b. Catalog-level metric-view detection (PR 19) ──────────────
    # Genie's serialized space sometimes omits the ``column_type='measure'``
    # / ``is_measure`` flags on metric-view columns and lists MVs under
    # ``data_sources.tables`` with no ``data_sources.metric_views`` entry,
    # which makes the entire MV machinery (effective identifier set,
    # ``build_metric_view_measures`` map, ``has_metric_view`` trait,
    # MEASURE() auto-wrap) blind. Probing the catalog with
    # ``DESCRIBE TABLE EXTENDED ... AS JSON`` is the only reliable way
    # to recover; we cache the parsed YAML on the config so every
    # downstream stage sees the same answer without re-running DESCRIBE.
    # Idempotent: skipped when the cache is already populated (e.g. the
    # snapshot was warmed by a prior preflight run).
    _yamls: dict[str, dict] = {}
    _outcomes: dict[str, str] = {}
    _diag_samples: dict[str, str] = {}
    if not (
        isinstance(config.get("_metric_view_yaml"), dict)
        and config["_metric_view_yaml"]
    ):
        try:
            from genie_space_optimizer.common.metric_view_catalog import (
                detect_metric_views_via_catalog_with_outcomes,
                summarize_outcomes,
            )
            _warehouse_id = resolve_warehouse_id("")
            # Task 9: surface the warehouse id resolution result up
            # front so a missing warehouse is immediately
            # distinguishable from a DESCRIBE that returned no MV
            # signals — the two outcomes look identical in the
            # downstream "MVs detected: 0" line otherwise.
            print(
                f"  [SEMANTICS] SQL warehouse for MV detection: "
                f"{_warehouse_id or '(missing)'}",
                flush=True,
            )
            _, _yamls, _outcomes = detect_metric_views_via_catalog_with_outcomes(
                spark,
                table_refs,
                w=w,
                warehouse_id=_warehouse_id,
                catalog=catalog,
                schema=schema,
                diagnostic_samples=_diag_samples,
            )
        except Exception as exc:
            logger.warning(
                "Catalog metric-view detection failed for %s "
                "(non-fatal, MV-aware features may be degraded): %s: %s",
                run_id, type(exc).__name__, exc,
            )
        if _yamls:
            config["_metric_view_yaml"] = _yamls
            _ps = config.get("_parsed_space")
            if isinstance(_ps, dict):
                _ps["_metric_view_yaml"] = _yamls

        # PR 23 — Always emit a one-line outcome summary, even when
        # zero MVs were detected, so log readers can distinguish
        # "no MVs in this space" from "DESCRIBE silently failed on
        # every ref". Counts are stable column-by-column so an alert
        # rule keyed on ``describe_error`` rising will fire correctly.
        if table_refs:
            try:
                _counts = summarize_outcomes(_outcomes)
                logger.info(
                    "Catalog metric-view detection summary for %s: "
                    "refs=%d, detected=%d, describe_error=%d, empty_result=%d, "
                    "no_envelope=%d, no_view_text=%d, yaml_parse_error=%d, "
                    "not_mv_shape=%d, no_warehouse=%d",
                    run_id, len(table_refs),
                    _counts["detected"],
                    _counts["describe_error"],
                    _counts["empty_result"],
                    _counts["no_envelope"],
                    _counts["no_view_text"],
                    _counts["yaml_parse_error"],
                    _counts["not_mv_shape"],
                    _counts.get("no_warehouse", 0),
                )
            except Exception:
                logger.debug(
                    "MV detection summary aggregation failed", exc_info=True,
                )

    # PR 27 — Stamp the unified asset-semantics contract right after
    # catalog detection so the lever loop's downstream stages
    # (join discovery, unified synthesis, validation/repair) all
    # read the same answer about each ref's kind. Use ``print()``
    # in addition to logger.info so the block stays visible when
    # package INFO logs are suppressed in the run host. Run
    # unconditionally — even when the catalog cache was pre-populated
    # from the snapshot we still want a freshly-built semantics map so
    # the stamp survives across runs that pre-date this PR.
    try:
        from genie_space_optimizer.common.asset_semantics import (
            build_and_stamp_from_run,
            format_semantics_block,
        )
        _yaml_cache = config.get("_metric_view_yaml") or {}
        if not isinstance(_yaml_cache, dict):
            _yaml_cache = {}
        _semantics = build_and_stamp_from_run(
            config,
            table_refs=list(table_refs) if table_refs else [],
            catalog_yamls=_yaml_cache,
            catalog_outcomes=_outcomes if isinstance(_outcomes, dict) else {},
            catalog_diagnostic_samples=_diag_samples,
            uc_columns=uc_columns if isinstance(uc_columns, list) else None,
        )
        for _line in format_semantics_block(_semantics):
            print(f"  [SEMANTICS] {_line}")
    except Exception:
        logger.debug(
            "Asset semantics stamping failed for %s (non-fatal)",
            run_id, exc_info=True,
        )

    # ── 3. Print detailed inventory diagnostic ───────────────────────
    _parsed = config.get("_parsed_space", {})
    _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
    _tables = _ds.get("tables", []) + _ds.get("metric_views", [])
    _total_cols = sum(len(t.get("column_configs", [])) for t in _tables)
    _visible_cols = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if not c.get("hidden")
    )
    _hidden_cols = _total_cols - _visible_cols
    _string_cols = sum(
        1 for c in uc_columns
        if str(c.get("data_type", "")).upper() == "STRING"
    )
    _fa_existing = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if c.get("enable_format_assistance")
    )
    _vd_existing = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if c.get("enable_entity_matching")
    )
    print(
        f"\n{'=' * 62}\n"
        f"  PREPARE LEVER LOOP — GENIE SPACE INVENTORY\n"
        f"{'=' * 62}\n"
        f"\n-- GENIE SPACE INVENTORY " + "-" * 27 + "\n"
        f"  Tables: {len(_tables)}"
        f" ({', '.join(t.get('name', t.get('identifier', '?')) for t in _tables[:10])})\n"
        f"  Total columns: {_total_cols}"
        f" (visible: {_visible_cols}, hidden: {_hidden_cols})\n"
        f"  UC column metadata: {len(uc_columns)} columns"
        f" fetched across {len(table_refs)} tables\n"
        f"  STRING columns eligible for entity matching: {_string_cols}\n"
        f"  Columns already with format assistance: {_fa_existing}\n"
        f"  Columns already with value dictionary: {_vd_existing} of 120 max slots\n"
        + "-" * 52
    )

    # ── 3b. RLS audit via information_schema (best-effort) ───────────
    # Populate config["_rls_audit"] so auto_apply_prompt_matching can use
    # the view-aware RLS verdict (inherited RLS, dynamic views) that the
    # serialized_space field check alone can't see. Fail-open: any
    # probe/query failure logs a WARNING and leaves verdicts as
    # "unknown", which the scorer treats as clean by default
    # (STRICT_RLS_MODE flips this to tainted).
    try:
        from genie_space_optimizer.iq_scan import collect_rls_audit
        space_tables = (_ds.get("tables") or []) + (_ds.get("metric_views") or [])
        config["_rls_audit"] = collect_rls_audit(
            space_tables,
            spark=spark, w=w,
            warehouse_id=resolve_warehouse_id(""),
        )
        _tainted_count = sum(
            1 for v in config["_rls_audit"].values()
            if v.get("verdict") == "tainted"
        )
        _unknown_count = sum(
            1 for v in config["_rls_audit"].values()
            if v.get("verdict") == "unknown"
        )
        print(
            f"  [RLS AUDIT] {len(config['_rls_audit'])} tables scanned; "
            f"{_tainted_count} tainted, {_unknown_count} unknown"
        )
    except Exception as exc:
        logger.warning(
            "RLS audit failed (non-fatal, proceeding without view-aware "
            "RLS detection): %s: %s",
            type(exc).__name__, exc,
        )
        config["_rls_audit"] = {}

    # ── 4–7. Stage 2.5 prompt matching + propagation wait ────────────
    if ENABLE_PROMPT_MATCHING_AUTO_APPLY:
        try:
            pm_result = _run_prompt_matching_setup(
                w, spark, run_id, space_id, config, catalog, schema,
                benchmarks=benchmarks,
            )
            logger.info(
                "Prompt matching complete: FA=%d, EM=%d, total=%d",
                pm_result.get("format_assistance_count", 0),
                pm_result.get("entity_matching_count", 0),
                pm_result.get("total_changes", 0),
            )

            if pm_result.get("total_changes", 0) > 0:
                em_enabled = pm_result.get("entity_matching_count", 0)
                em_disabled = pm_result.get("entity_matching_disabled_count", 0)
                has_entity_matching = (em_enabled + em_disabled) > 0
                wait_time = (
                    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS if has_entity_matching
                    else PROPAGATION_WAIT_SECONDS
                )
                print(
                    f"\n-- PROPAGATION WAIT " + "-" * 32 + "\n"
                    f"  Changes applied: {pm_result.get('total_changes', 0)}\n"
                    f"  Entity matching changes:"
                    f" +{em_enabled} / -{em_disabled}\n"
                    f"  Wait time: {wait_time}s"
                    + (
                        " (extended for value dictionary rebuild)"
                        if has_entity_matching else ""
                    )
                    + "\n" + "-" * 52
                )
                time.sleep(wait_time)
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                logger.info("Config refreshed after prompt matching propagation wait")
            else:
                logger.info("Prompt matching: no changes applied, skipping wait")
        except Exception as exc:
            logger.warning(
                "Stage 2.5 prompt matching failed (non-fatal, continuing): %s: %s",
                type(exc).__name__, exc,
            )

    return config


def _collect_uc_foreign_keys_for_enrichment(
    *,
    w: "WorkspaceClient",
    spark: "SparkSession",
    table_refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Best-effort REST-first / Spark-fallback FK collection for enrichment.

    Returns ``[]`` when both paths fail. Errors are warning-level; the
    caller continues without FK metadata rather than aborting enrichment.
    """
    if not table_refs:
        return []
    from genie_space_optimizer.common.uc_metadata import (
        get_foreign_keys_for_tables,
        get_foreign_keys_for_tables_rest,
    )

    try:
        rows = get_foreign_keys_for_tables_rest(w, table_refs)
        if isinstance(rows, list) and rows:
            return rows
    except Exception as exc:
        logger.warning(
            "Lever loop: REST FK collection failed, falling back to Spark: %s",
            str(exc)[:200],
        )

    try:
        rows = get_foreign_keys_for_tables(spark, table_refs)
        return rows if isinstance(rows, list) else []
    except Exception as exc:
        logger.warning(
            "Lever loop: Spark FK fallback failed: %s",
            str(exc)[:200],
        )
        return []


def _refresh_config_preserving_mv_state(
    w: "WorkspaceClient",
    space_id: str,
    *,
    uc_columns: list,
    data_profile: dict,
    yaml_cache: dict,
    table_refs: list,
    uc_foreign_keys: list | None = None,
) -> tuple[dict, dict]:
    """Re-fetch the Genie space config without losing MV detection state.

    ``fetch_space_config`` rebuilds the config from the Genie REST
    response and has no awareness of catalog-detection caches. Eight
    refresh sites in :func:`_run_enrichment` previously dropped
    ``_metric_view_yaml`` and ``_asset_semantics`` mid-run, which is
    the root cause of the recurring ``MVs detected: 0`` /
    ``METRIC_VIEW_JOIN_NOT_SUPPORTED`` /
    ``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` cluster.

    Returns ``(config, metadata_snapshot)`` so callers can replace
    both in one expression and stay in lock-step with the existing
    refresh-block contract (``config["_uc_columns"]`` populated,
    ``metadata_snapshot["_data_profile"]`` populated, UC types enriched).

    The caller is responsible for capturing ``yaml_cache`` and
    ``table_refs`` once after the initial ``_prepare_lever_loop`` —
    they are immutable for the lifetime of an enrichment run because
    catalog-level MV detection only runs once.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config

    config = fetch_space_config(w, space_id)
    config["_uc_columns"] = uc_columns
    if uc_foreign_keys is not None:
        config["_uc_foreign_keys"] = list(uc_foreign_keys)
    if isinstance(yaml_cache, dict) and yaml_cache:
        config["_metric_view_yaml"] = dict(yaml_cache)
    metadata_snapshot = config.get("_parsed_space", config)
    if isinstance(metadata_snapshot, dict):
        metadata_snapshot["_data_profile"] = data_profile
        if uc_foreign_keys is not None:
            metadata_snapshot["_uc_foreign_keys"] = list(uc_foreign_keys)
        if isinstance(yaml_cache, dict) and yaml_cache:
            metadata_snapshot["_metric_view_yaml"] = dict(yaml_cache)
    if uc_columns:
        enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

    # Re-stamp ``_asset_semantics`` from the preserved YAML cache so
    # the post-refresh config matches the pre-refresh contract. We do
    # not round-trip ``catalog_outcomes`` / ``catalog_diagnostic_samples``
    # here — they are debug-only and not consulted on the hot path.
    try:
        from genie_space_optimizer.common.asset_semantics import (
            build_and_stamp_from_run,
        )

        build_and_stamp_from_run(
            config,
            table_refs=list(table_refs or []),
            catalog_yamls=yaml_cache if isinstance(yaml_cache, dict) else {},
            catalog_outcomes={},
            catalog_diagnostic_samples={},
            uc_columns=uc_columns if isinstance(uc_columns, list) else None,
        )
    except Exception:
        logger.debug(
            "asset semantics re-stamp during refresh failed (non-fatal)",
            exc_info=True,
        )

    return config, metadata_snapshot


def _run_enrichment(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    benchmarks: list[dict],
    exp_name: str,
    catalog: str,
    schema: str,
    baseline_model_id: str = "",
    optimization_run_id: str = "",
    *,
    held_out_benchmarks: list[dict] | None = None,
    baseline_both_correct_rows: list[dict] | None = None,
) -> dict:
    """Stage 2.5: Config preparation + proactive enrichment + LoggedModel snapshot.

    Combines ``_prepare_lever_loop()`` (config loading, UC metadata, prompt
    matching) with the Phase 1 proactive enrichment steps previously embedded
    in ``_run_lever_loop()`` (descriptions, joins, metadata, instructions,
    example SQLs).

    Side effects:
      - Patches the Genie Space with enrichments via the API
      - Creates an MLflow LoggedModel snapshot of the enriched state
      - Writes Delta stage records (ENRICHMENT_STARTED, ENRICHMENT_COMPLETE)

    Returns dict with:
      - enrichment_model_id: str
      - enrichment_skipped: bool
      - config: dict (enriched config)
      - summary: dict (counts of each enrichment type)
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config

    write_stage(
        spark, run_id, "ENRICHMENT_STARTED", "STARTED",
        task_key="enrichment", catalog=catalog, schema=schema,
    )

    try:
        # ── 1. Load config (delegates to _prepare_lever_loop) ─────────────
        config = _prepare_lever_loop(
            w, spark, run_id, space_id, catalog, schema,
            benchmarks=benchmarks,
        )

        uc_columns = config.get("_uc_columns", [])
        metadata_snapshot = config.get("_parsed_space", config)
        data_profile = metadata_snapshot.get("_data_profile", {})
        if uc_columns:
            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

        # PR 33 — Capture the MV-state caches once. ``_metric_view_yaml``
        # is populated by catalog-level detection inside
        # ``_prepare_lever_loop`` and is immutable for the lifetime of an
        # enrichment run. Subsequent ``fetch_space_config`` calls would
        # otherwise drop these caches and silently break every MV-aware
        # downstream stage. ``table_refs`` is captured here too so the
        # asset-semantics re-stamp inside the helper sees every ref the
        # run knows about, matching the contract that
        # ``_prepare_lever_loop`` first established.
        from genie_space_optimizer.common.uc_metadata import (
            extract_genie_space_table_refs,
        )
        _yaml_cache_for_refresh: dict = dict(
            config.get("_metric_view_yaml") or {},
        )
        _table_refs_for_refresh = extract_genie_space_table_refs(config) or []

        # ── 2. Proactive Enrichment sub-steps ─────────────────────────────
        import mlflow as _mlflow_enr

        uc_schema = f"{catalog}.{schema}"
        # Tier 4: MLflow run naming v2 — ``<run_short>/enrichment/snapshot``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags,
            enrichment_run_name,
        )
        _effective_run_id = optimization_run_id or run_id
        _enr_run_name = enrichment_run_name(_effective_run_id, detail="snapshot")
        with _mlflow_enr.start_run(run_name=_enr_run_name):
            _mlflow_enr.set_tags({
                **_v2_tags(
                    _effective_run_id,
                    space_id=space_id,
                    stage="enrichment_snapshot",
                ),
                "genie.space_id": space_id,
                "genie.domain": domain,
                "genie.optimization_run_id": optimization_run_id or run_id,
                "genie.run_type": "enrichment_snapshot",
            })

            _pe_lines = [_section("ENRICHMENT — PROACTIVE ENRICHMENT", "-")]
            _pe_lines.append(_kv("Space ID", space_id))
            _pe_lines.append(_kv("UC columns", len(uc_columns)))
            _pe_lines.append(_bar("-"))
            print("\n".join(_pe_lines))

            enrichment_result = _run_description_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if enrichment_result.get("total_enriched", 0) > 0 or enrichment_result.get("tables_enriched", 0) > 0:
                config, metadata_snapshot = _refresh_config_preserving_mv_state(
                    w, space_id,
                    uc_columns=uc_columns, data_profile=data_profile,
                    yaml_cache=_yaml_cache_for_refresh,
                    table_refs=_table_refs_for_refresh,
                )

            join_result = _run_proactive_join_discovery(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if join_result.get("total_applied", 0) > 0:
                config, metadata_snapshot = _refresh_config_preserving_mv_state(
                    w, space_id,
                    uc_columns=uc_columns, data_profile=data_profile,
                    yaml_cache=_yaml_cache_for_refresh,
                    table_refs=_table_refs_for_refresh,
                )

            meta_result = _run_space_metadata_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if meta_result.get("description_generated") or meta_result.get("questions_generated"):
                config, metadata_snapshot = _refresh_config_preserving_mv_state(
                    w, space_id,
                    uc_columns=uc_columns, data_profile=data_profile,
                    yaml_cache=_yaml_cache_for_refresh,
                    table_refs=_table_refs_for_refresh,
                )

            # ── 5a. Instruction prose mining & promotion (miner-first) ────
            # Runs BEFORE proactive seed/expand (per Task C.5 ordering) so
            # legacy ALL-CAPS prose is normalised into the canonical 5-
            # section form before seed/expand see it.
            #
            # ``benchmarks`` here is used ONLY as the firewall corpus for
            # the prose miner's example_qsql target (it does not mine
            # from benchmarks). Pass the full train+held_out corpus so
            # the example-SQL firewall can catch held-out leakage.
            _miner_out = _run_instruction_prose_mining(
                w, spark, run_id, space_id, config, metadata_snapshot,
                catalog, schema,
                warehouse_id=resolve_warehouse_id(""),
                benchmarks=list(benchmarks) + list(held_out_benchmarks or []),
            )
            if _miner_out["total_applied"] or _miner_out["keep_in_prose_count"]:
                config, metadata_snapshot = _refresh_config_preserving_mv_state(
                    w, space_id,
                    uc_columns=uc_columns, data_profile=data_profile,
                    yaml_cache=_yaml_cache_for_refresh,
                    table_refs=_table_refs_for_refresh,
                )

            # ── 5b. Proactive instruction seeding + expand ────────────────
            instruction_result = _run_proactive_instruction_seeding(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if (
                instruction_result.get("instructions_seeded")
                or instruction_result.get("instructions_expanded")
            ):
                config, metadata_snapshot = _refresh_config_preserving_mv_state(
                    w, space_id,
                    uc_columns=uc_columns, data_profile=data_profile,
                    yaml_cache=_yaml_cache_for_refresh,
                    table_refs=_table_refs_for_refresh,
                )

            # ── 5c. Pre-flight example_sql synthesis (fills to 20) ────────
            # Two paths behind GSO_UNIFIED_EXAMPLE_SQL_GENERATION (default ON):
            #
            #   UNIFIED (Phase 4.R4): calls ``generate_example_sqls``, which
            #   uses the mature benchmark engine with all its quality
            #   features (MEASURE auto-wrap, field-drift correction,
            #   correction LLM retry, arbiter approval, leakage firewall).
            #   Option A fallback: if unified yields < GSO_UNIFIED_MIN_SURVIVORS,
            #   the archetype-templated preflight path runs to fill the gap.
            #
            #   LEGACY: archetype-templated per-candidate synthesis. Retained
            #   as a rollback lever; flip GSO_UNIFIED_EXAMPLE_SQL_GENERATION
            #   to false to restore the pre-unification behaviour.
            #
            # Both paths share the same last-mile firewall at
            # ``_apply_proactive_example_sqls``, which MUST see the full
            # benchmark corpus (train + held_out) so the SQL fingerprint
            # check catches held-out leakage. See
            # ``docs/example-sql-isolation.md``.
            _full_firewall_corpus = list(benchmarks) + list(held_out_benchmarks or [])
            preflight_example_result: dict = {}
            unified_example_result: dict = {}
            if ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS:
                _unified_enabled = os.environ.get(
                    "GSO_UNIFIED_EXAMPLE_SQL_GENERATION", "true",
                ).lower() in {"1", "true", "yes", "on"}
                _unified_min_survivors = int(
                    os.environ.get("GSO_UNIFIED_MIN_SURVIVORS", "5") or "5",
                )
                if _unified_enabled:
                    unified_example_result = _run_unified_example_sql_generation(
                        w=w, spark=spark, run_id=run_id, space_id=space_id,
                        config=config, metadata_snapshot=metadata_snapshot,
                        uc_columns=uc_columns, domain=domain,
                        catalog=catalog, schema=schema,
                        full_firewall_corpus=_full_firewall_corpus,
                        data_profile=data_profile,
                        baseline_both_correct_rows=baseline_both_correct_rows,
                    )
                    if unified_example_result.get("applied", 0) > 0:
                        config, metadata_snapshot = _refresh_config_preserving_mv_state(
                            w, space_id,
                            uc_columns=uc_columns, data_profile=data_profile,
                            yaml_cache=_yaml_cache_for_refresh,
                            table_refs=_table_refs_for_refresh,
                        )

                # Archetype fallback (Option A) OR legacy path.
                _need_fallback = (
                    not _unified_enabled
                    or unified_example_result.get("applied", 0) < _unified_min_survivors
                )
                if _need_fallback:
                    try:
                        from genie_space_optimizer.optimization.preflight_synthesis import (
                            run_preflight_example_synthesis,
                        )
                        if _unified_enabled and unified_example_result.get("applied", 0) > 0:
                            logger.info(
                                "unified generator yielded %d < %d — running "
                                "archetype fallback to fill the gap",
                                unified_example_result.get("applied", 0),
                                _unified_min_survivors,
                            )
                        preflight_example_result = run_preflight_example_synthesis(
                            w, spark, run_id, space_id, config, metadata_snapshot,
                            benchmarks=_full_firewall_corpus,
                            catalog=catalog, schema=schema,
                            warehouse_id=resolve_warehouse_id(""),
                        )
                        if preflight_example_result.get("applied", 0) > 0:
                            config, metadata_snapshot = _refresh_config_preserving_mv_state(
                                w, space_id,
                                uc_columns=uc_columns, data_profile=data_profile,
                                yaml_cache=_yaml_cache_for_refresh,
                                table_refs=_table_refs_for_refresh,
                            )
                    except Exception:
                        logger.warning(
                            "preflight example synthesis (fallback) raised; "
                            "continuing without it",
                            exc_info=True,
                        )

            example_join_result: dict = {}
            try:
                _example_join_inputs = _collect_examples_for_join_mining(
                    unified_example_result=unified_example_result,
                    preflight_example_result=preflight_example_result,
                )
                if _example_join_inputs:
                    example_join_result = _mine_and_apply_joins_from_example_sqls(
                        w=w,
                        spark=spark,
                        run_id=run_id,
                        space_id=space_id,
                        metadata_snapshot=metadata_snapshot,
                        examples=_example_join_inputs,
                        catalog=catalog,
                        schema=schema,
                        baseline_both_correct_rows=list(
                            baseline_both_correct_rows or []
                        ),
                    )
                    if example_join_result.get("total_applied", 0) > 0:
                        config, metadata_snapshot = _refresh_config_preserving_mv_state(
                            w, space_id,
                            uc_columns=uc_columns, data_profile=data_profile,
                            yaml_cache=_yaml_cache_for_refresh,
                            table_refs=_table_refs_for_refresh,
                        )
            except Exception:
                logger.warning(
                    "example-SQL join mining raised; continuing without it",
                    exc_info=True,
                )

            # ── 5d. SQL Expression REPAIR + SEEDING ───────────────────────
            # Repair runs unconditionally; seeding is headroom-gated.
            #
            # Mining source gate: only baseline benchmarks whose arbiter
            # verdict is ``both_correct`` qualify — the ``expected_sql``
            # on those rows is proven correct (Genie and GT agreed).
            # ``genie_correct`` rows are explicitly EXCLUDED because
            # their ``expected_sql`` may be wrong (that verdict triggers
            # the separate ground-truth-repair path). First run / no
            # baseline -> empty subset -> only schema-discovery proposes.
            approved_benchmarks, _arbiter_verdict_counts = (
                _extract_arbiter_approved_benchmarks(
                    spark, run_id, catalog, schema, benchmarks,
                )
            )
            logger.info(
                "miner.arbiter_filter total=%d approved=%d verdicts=%s",
                len(benchmarks), len(approved_benchmarks),
                _arbiter_verdict_counts,
            )
            sql_expr_result = _run_sql_expression_seeding(
                w, spark, run_id, space_id, config=config,
                metadata_snapshot=metadata_snapshot,
                benchmarks=approved_benchmarks,
                catalog=catalog, schema=schema,
                warehouse_id=resolve_warehouse_id(""),
            )
            if (
                sql_expr_result.get("total_candidates", 0) > 0
                or sql_expr_result.get("repair", {}).get("rewritten", 0) > 0
            ):
                config, metadata_snapshot = _refresh_config_preserving_mv_state(
                    w, space_id,
                    uc_columns=uc_columns, data_profile=data_profile,
                    yaml_cache=_yaml_cache_for_refresh,
                    table_refs=_table_refs_for_refresh,
                )

            # Bug #4 — benchmark verbatim mining removed. Proposals for
            # example_sqls now come exclusively from AFS-gated structural
            # synthesis (Phase 3), never from copying benchmark expected_sql.
            mined_example_proposals: list = []

            # ── Summary ───────────────────────────────────────────────────
            # _miner_out carries per-target applied counts from the prose
            # mining & promotion step (see _run_instruction_prose_mining).
            # The earlier single `_instr_sql_applied` local was removed when
            # the miner grew multi-target support; sum every promoted target
            # so the summary reflects reality.
            total_enrichments = (
                enrichment_result.get("total_enriched", 0)
                + join_result.get("total_applied", 0)
                + example_join_result.get("total_applied", 0)
                + (1 if meta_result.get("description_generated") else 0)
                + (1 if meta_result.get("questions_generated") else 0)
                + (1 if instruction_result.get("instructions_seeded") else 0)
                + _miner_out.get("sql_applied", 0)
                + _miner_out.get("join_applied", 0)
                + _miner_out.get("example_applied", 0)
                + _miner_out.get("desc_applied", 0)
                + _miner_out.get("synonym_applied", 0)
                + sql_expr_result.get("total_seeded", 0)
                + len(mined_example_proposals)
            )

            _enr_summary = [_section("PROACTIVE ENRICHMENT — SUMMARY", "-")]
            _enr_summary.append(_kv("Descriptions enriched", enrichment_result.get("total_enriched", 0)))
            _enr_summary.append(_kv("Joins discovered", join_result.get("total_applied", 0)))
            _enr_summary.append(_kv(
                "Joins from accepted example SQLs",
                example_join_result.get("total_applied", 0),
            ))
            _enr_summary.append(_kv("Space metadata", "description=%s, questions=%s" % (
                "generated" if meta_result.get("description_generated") else "unchanged",
                "generated" if meta_result.get("questions_generated") else "unchanged",
            )))
            _enr_summary.append(_kv("Instructions seeded", "yes" if instruction_result.get("instructions_seeded") else "no"))
            _enr_summary.append(_kv("Instruction-derived SQL expressions", _miner_out.get("sql_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived join specs", _miner_out.get("join_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived example SQLs", _miner_out.get("example_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived table descriptions", _miner_out.get("desc_applied", 0)))
            _enr_summary.append(_kv("Instruction-derived column synonyms", _miner_out.get("synonym_applied", 0)))
            _enr_summary.append(_kv("SQL expressions seeded", sql_expr_result.get("total_seeded", 0)))
            _enr_summary.append(_kv("Example SQLs mined", len(mined_example_proposals)))
            _enr_summary.append(_kv("Total enrichments", total_enrichments))
            _enr_summary.append(_bar("-"))
            print("\n".join(_enr_summary))

            enrichment_skipped = total_enrichments == 0

            # ── Log enrichment metrics ────────────────────────────────────
            _mlflow_enr.log_metrics({
                "enrichment.columns_enriched": enrichment_result.get("total_enriched", 0),
                "enrichment.tables_enriched": enrichment_result.get("tables_enriched", 0),
                "enrichment.joins_discovered": join_result.get("total_applied", 0),
                "enrichment.example_sql_joins_discovered": example_join_result.get("total_applied", 0),
                "enrichment.sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
                "enrichment.examples_mined": len(mined_example_proposals),
                "enrichment.total": total_enrichments,
            })

            # ── 3. LoggedModel snapshot of enriched state ─────────────────
            enrichment_model_id = create_genie_model_version(
                w,
                space_id,
                config,
                iteration=-1,
                domain=domain,
                experiment_name=exp_name,
                uc_schema=uc_schema,
                uc_columns=uc_columns,
                parent_model_id=baseline_model_id or None,
                optimization_run_id=optimization_run_id or run_id,
            )

        # ── 4. Post-enrichment evaluation (Tier 1.3) ──────────────────────
        # Enrichment mutates the Genie Space (descriptions, joins,
        # instructions, example SQLs). Without a fresh eval, Task 4 (lever
        # loop) would gate against the stale baseline accuracy while its
        # clustering reads post-enrichment rows — those two realities can
        # disagree arbitrarily. We run a single-pass eval (no confirmation)
        # and publish the result so Task 4 can consume it. Skipped when
        # nothing changed.
        post_enrichment_accuracy: float | None = None
        post_enrichment_scores: dict[str, float] = {}
        post_enrichment_evaluated_count: int | None = None
        post_enrichment_thresholds_met: bool = False
        if not enrichment_skipped and enrichment_model_id:
            # Own stage lifecycle here under POST_ENRICHMENT_EVAL_* so we
            # don't reuse BASELINE_EVAL_STARTED (which would leak an
            # unclosed STARTED row and pin Step 2 to "Running" until the
            # whole run goes terminal).
            write_stage(
                spark, run_id, "POST_ENRICHMENT_EVAL_STARTED", "STARTED",
                task_key="enrichment", catalog=catalog, schema=schema,
            )
            try:
                _pe_setup = _build_predict_and_scorers(
                    w, spark, space_id, catalog, schema, exp_name,
                    enrichment_model_id, domain,
                    banner_title="ENRICHMENT — POST-ENRICHMENT EVAL SETUP",
                )
                # Tier 4: v2 name — ``<run_short>/enrichment/post_eval``.
                from genie_space_optimizer.common.mlflow_names import (
                    default_tags as _v2_tags_pe,
                    enrichment_run_name as _enrichment_run_name_pe,
                )
                _effective_run_id_pe = optimization_run_id or run_id
                _pe_eval = run_evaluation(
                    space_id, exp_name, 0, benchmarks,
                    domain, enrichment_model_id, "full",
                    _pe_setup["predict_fn"], _pe_setup["scorers"],
                    spark=spark, w=w,
                    catalog=catalog, gold_schema=schema,
                    uc_schema=f"{catalog}.{schema}",
                    run_name=_enrichment_run_name_pe(
                        _effective_run_id_pe, detail="post_eval",
                    ),
                    extra_tags=_v2_tags_pe(
                        _effective_run_id_pe,
                        space_id=space_id, stage="enrichment_post_eval",
                    ),
                )
                post_enrichment_accuracy = float(
                    _pe_eval.get("overall_accuracy", 0.0) or 0.0
                )
                _raw_scores = _pe_eval.get("scores", {})
                if isinstance(_raw_scores, dict):
                    post_enrichment_scores = {
                        str(k): float(v) for k, v in _raw_scores.items()
                        if v is not None
                    }
                _ec = _pe_eval.get("evaluated_count")
                if isinstance(_ec, (int, float)):
                    post_enrichment_evaluated_count = int(_ec)
                post_enrichment_thresholds_met = bool(
                    _pe_eval.get("thresholds_met", False)
                )

                # Persist the post-enrichment eval as iter 0 with
                # ``eval_scope="enrichment"`` so the UI's
                # ``compute_run_scores`` can surface it as the optimized
                # candidate when the lever loop is short-circuited (the
                # "baseline 91.7 → optimized 96.2 driven by enrichment"
                # headline). Best-effort: a write failure must not
                # prevent the existing task-value flow downstream. We
                # deliberately skip ``_merge_bug4_counters`` here so the
                # first lever-loop full write keeps absorbing leakage
                # counters as today.
                try:
                    write_iteration(
                        spark, run_id, 0, _pe_eval,
                        catalog=catalog, schema=schema,
                        eval_scope="enrichment",
                        model_id=enrichment_model_id,
                    )
                    _pe_both_correct_rate = _pe_eval.get("both_correct_rate")
                    if _pe_both_correct_rate is not None:
                        _pe_anchored_best = min(
                            post_enrichment_accuracy,
                            float(_pe_both_correct_rate),
                        )
                    else:
                        _pe_anchored_best = post_enrichment_accuracy
                    update_run_status(
                        spark, run_id, catalog, schema,
                        best_iteration=0,
                        best_accuracy=_pe_anchored_best,
                        best_model_id=enrichment_model_id,
                    )
                except Exception:
                    logger.warning(
                        "Failed to persist post-enrichment iteration row "
                        "for run %s — UI headline will fall back to "
                        "baseline only",
                        run_id,
                        exc_info=True,
                    )

                _pe_lines = [_section("ENRICHMENT — POST-ENRICHMENT EVAL", "-")]
                _pe_lines.append(
                    _kv("Accuracy", f"{post_enrichment_accuracy:.1f}%")
                )
                _pe_lines.append(
                    _kv("Thresholds met", post_enrichment_thresholds_met)
                )
                _pe_lines.append(_bar("-"))
                print("\n".join(_pe_lines))
                try:
                    write_stage(
                        spark, run_id, "POST_ENRICHMENT_EVAL_STARTED", "COMPLETE",
                        task_key="enrichment", catalog=catalog, schema=schema,
                        detail={
                            "accuracy": post_enrichment_accuracy,
                            "evaluated_count": post_enrichment_evaluated_count,
                            "thresholds_met": post_enrichment_thresholds_met,
                        },
                    )
                except Exception:
                    logger.warning(
                        "Failed to write POST_ENRICHMENT_EVAL_STARTED COMPLETE "
                        "for run %s",
                        run_id,
                        exc_info=True,
                    )
            except Exception as _pe_exc:
                logger.warning(
                    "Post-enrichment eval failed — Task 4 will fall back to "
                    "baseline accuracy",
                    exc_info=True,
                )
                try:
                    write_stage(
                        spark, run_id, "POST_ENRICHMENT_EVAL_STARTED", "FAILED",
                        task_key="enrichment", catalog=catalog, schema=schema,
                        error_message=f"{type(_pe_exc).__name__}: {_pe_exc}"[:500],
                    )
                except Exception:
                    logger.warning(
                        "Failed to write POST_ENRICHMENT_EVAL_STARTED FAILED "
                        "for run %s",
                        run_id,
                        exc_info=True,
                    )

        write_stage(
            spark, run_id, "ENRICHMENT_COMPLETE", "COMPLETE",
            task_key="enrichment", catalog=catalog, schema=schema,
            detail={
                "enrichment_model_id": enrichment_model_id,
                "total_enrichments": total_enrichments,
                "enrichment_skipped": enrichment_skipped,
                "descriptions_enriched": enrichment_result.get("total_enriched", 0),
                "joins_discovered": join_result.get("total_applied", 0),
                "instructions_seeded": bool(instruction_result.get("instructions_seeded")),
                "sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
                "examples_mined": len(mined_example_proposals),
                "post_enrichment_accuracy": post_enrichment_accuracy,
                "post_enrichment_thresholds_met": post_enrichment_thresholds_met,
            },
        )

        return {
            "enrichment_model_id": enrichment_model_id,
            "enrichment_skipped": enrichment_skipped,
            "config": config,
            "post_enrichment_accuracy": post_enrichment_accuracy,
            "post_enrichment_scores": post_enrichment_scores,
            "post_enrichment_model_id": enrichment_model_id,
            "post_enrichment_evaluated_count": post_enrichment_evaluated_count,
            "post_enrichment_thresholds_met": post_enrichment_thresholds_met,
            "summary": {
                "descriptions_enriched": enrichment_result.get("total_enriched", 0),
                "joins_discovered": join_result.get("total_applied", 0),
                "description_generated": bool(meta_result.get("description_generated")),
                "questions_generated": bool(meta_result.get("questions_generated")),
                "instructions_seeded": bool(instruction_result.get("instructions_seeded")),
                "sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
                "examples_mined": len(mined_example_proposals),
                "total_enrichments": total_enrichments,
                "post_enrichment_accuracy": post_enrichment_accuracy,
            },
        }

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("ENRICHMENT FAILED for run %s", run_id)
        try:
            write_stage(
                spark, run_id, "ENRICHMENT_STARTED", "FAILED",
                task_key="enrichment",
                error_message=err_msg[:500],
                catalog=catalog, schema=schema,
            )
        except Exception:
            logger.warning("Failed to write ENRICHMENT FAILED stage", exc_info=True)
        raise


# ── Stage 3: LEVER LOOP ─────────────────────────────────────────────

# ── Adaptive loop helpers ───────────────────────────────────────────


def _next_grounding_action_payload(
    *,
    rollback_reason: str,
    grounding_failure_category: str = "",
    repeated_count: int = 1,
) -> dict:
    """Build a serializable next-action payload for reflection extras."""
    try:
        from genie_space_optimizer.optimization.rca_next_action import (
            next_action_for_rejection,
        )

        decision = next_action_for_rejection(
            rollback_reason=rollback_reason,
            grounding_failure_category=grounding_failure_category,
            repeated_count=repeated_count,
        )
        return {
            "action": decision.action.value,
            "forced_levers": list(decision.forced_levers),
            "terminal_status": decision.terminal_status,
            "reason": decision.reason,
        }
    except Exception:
        logger.debug("RCA next-action mapping failed", exc_info=True)
        return {
            "action": "none",
            "forced_levers": [],
            "terminal_status": "",
            "reason": str(rollback_reason or ""),
        }


def _format_rollback_reflection(
    *,
    rollback_reason: str,
    control_plane_reason: str,
    any_target_improved: bool,
    regressions: list,
    patch_types: list[str],
    root_cause_summary: str,
    accuracy_delta_pp: float,
) -> str:
    """Return a strategist-facing reflection sentence for a rolled-back AG.

    The label prefix names the actual cause (so the next strategist call
    does not repeat the same approach). Mapping:

      rejected_no_gain                 -> Rollback (no_overall_improvement)
      rejected_unbounded_collateral    -> Rollback (unbounded_collateral)
      target_qids_not_improved         -> Rollback (target_not_fixed)
      rejected_missing_causal_target   -> Rollback (no_causal_target)
      missing_pre_rows                 -> Rollback (gate_baseline_missing)
      stale_or_candidate_pre_rows      -> Rollback (gate_baseline_stale)
      else                             -> Rollback (<rollback_reason short>)
    """
    label_map = {
        "post_arbiter_not_improved": "no_overall_improvement",
        "rejected_no_gain": "no_overall_improvement",
        "out_of_target_hard_regression": "collateral_regression",
        "rejected_unbounded_collateral": "unbounded_collateral",
        "target_qids_not_improved": "target_not_fixed",
        "missing_target_qids": "no_causal_target",
        "rejected_missing_causal_target": "no_causal_target",
        "missing_pre_rows": "gate_baseline_missing",
        "stale_or_candidate_pre_rows": "gate_baseline_stale",
    }
    if control_plane_reason in label_map:
        label = label_map[control_plane_reason]
    else:
        label = (rollback_reason or "unknown").split(":", 1)[0].strip() or "unknown"

    patch_text = ", ".join(patch_types) if patch_types else "(no patches)"

    if any_target_improved and regressions:
        return (
            f"Rollback ({label}): patches ({patch_text}) improved some target "
            f"questions but caused regressions on {len(regressions)} other(s) "
            f"(qids={[str(r.get('qid', '?')) for r in regressions[:3]]}). "
            f"Narrower scope on the same lever may help."
        )
    return (
        f"Rollback ({label}): {root_cause_summary or 'unknown root cause'} "
        f"was not resolved by {patch_text} "
        f"(accuracy delta {accuracy_delta_pp:+.1f}%). "
        f"Real reason: {control_plane_reason or rollback_reason}. "
        f"A different lever or escalation is needed."
    )


def _build_reflection_entry(
    iteration: int,
    ag_id: str,
    accepted: bool,
    levers: list[int],
    target_objects: list[str],
    prev_scores: dict[str, float],
    new_scores: dict[str, float],
    rollback_reason: str | None,
    patches: list[dict],
    *,
    affected_question_ids: list[str] | None = None,
    prev_failure_qids: set[str] | None = None,
    new_failure_qids: set[str] | None = None,
    reflection_text: str = "",
    refinement_mode: str = "",
    escalation_handled: bool = False,
    root_cause: str = "",
    blame_set: Any = None,
    source_cluster_ids: list[str] | None = None,
    source_cluster_signatures: list[str] | None = None,
    acceptance_delta_pp: float | None = None,
    extra: dict | None = None,
) -> dict:
    """Build a structured reflection dict for the adaptive loop memory.

    *reflection_text* is a 2-3 sentence verbal explanation of why the
    iteration succeeded or failed (Reflexion-style semantic gradient).

    *refinement_mode* is ``"in_plan"`` when the lever direction was correct
    but caused collateral regressions, or ``"out_of_plan"`` when the
    approach fundamentally did not work (AdaPlanner-style classification).

    Phase C2 added the identity fields (``root_cause``, ``blame_set``,
    ``source_cluster_ids``, ``lever_set``, ``rollback_class``) that
    Phases D1-D3 use to build the DO-NOT-RETRY forbidden set without
    parsing the free-form ``action`` string. Callers that didn't supply
    identity fields still produce a valid entry; the fields are simply
    empty and won't match anything in the collision guard.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        classify_rollback_reason,
    )

    score_deltas = {
        k: new_scores.get(k, 0.0) - prev_scores.get(k, 0.0)
        for k in set(prev_scores) | set(new_scores)
    }
    prev_acc = sum(prev_scores.values()) / max(len(prev_scores), 1)
    new_acc = sum(new_scores.values()) / max(len(new_scores), 1)
    _mean_accuracy_delta = new_acc - prev_acc
    _acceptance_delta_pp = (
        float(acceptance_delta_pp)
        if acceptance_delta_pp is not None
        else _mean_accuracy_delta
    )

    patch_summary_parts: list[str] = []
    do_not_retry: list[str] = []
    # Task 18 — keep a copy of the rolled-back patch dicts so the
    # reflection-as-validator gate can compute precise retry
    # signatures (column / instruction-section level) instead of the
    # coarse ``(patch_type, target)`` tuples.
    do_not_retry_patches: list[dict] = []
    # T3.1: minimum-viable leave-one-out attribution. Without running
    # actual re-evals (expensive, would need full harness wiring), we
    # heuristically rank patch suspicion by the T2.4 collateral-risk
    # flag. Patches that touched assets depended on by many passing
    # questions are the prime suspects when a rollback is needed;
    # low-risk patches are kept out of the DO-NOT-RETRY set so the
    # strategist can re-propose them in a different combination.
    #
    # When every patch is low-risk, fall back to blanket DO-NOT-RETRY
    # (the historical behaviour) — at least one of them regressed.
    _high_risk = [p for p in patches if p.get("high_collateral_risk")]
    _suspicious = _high_risk if _high_risk else patches
    _suspicious_keys = {
        (
            p.get("type", p.get("patch_type", "?")),
            p.get("target", p.get("target_object", "?")),
        )
        for p in _suspicious
    }
    for p in patches:
        ptype = p.get("type", p.get("patch_type", "?"))
        target = p.get("target", p.get("target_object", "?"))
        patch_summary_parts.append(f"{ptype} on {target}")
        if not accepted and (ptype, target) in _suspicious_keys:
            do_not_retry.append(f"{ptype} on {target}")
            # Task 18 — capture the patch dict for precise signature
            # computation in the reflection gate.
            do_not_retry_patches.append(dict(p))

    action = ", ".join(patch_summary_parts[:8])
    if len(patch_summary_parts) > 8:
        action += f" (+{len(patch_summary_parts) - 8} more)"

    new_failure_parts: list[str] = []
    for k, delta in score_deltas.items():
        if delta < -1.0:
            new_failure_parts.append(f"{k} {delta:+.1f}%")
    new_failures = ", ".join(new_failure_parts) if new_failure_parts else None

    _prev = prev_failure_qids or set()
    _new = new_failure_qids or set()

    # Normalise blame_set so reflection entries share the same shape as
    # :func:`_filter_tried_clusters` expects — this keeps the DO-NOT-RETRY
    # forbidden set symmetric with the tried-clusters bookkeeping.
    _blame_norm = _normalise_blame(blame_set)

    _lever_set = sorted({int(l) for l in levers}) if levers else []

    entry = {
        "iteration": iteration,
        "ag_id": ag_id,
        "accepted": accepted,
        "action": action,
        "levers": levers,
        "target_objects": target_objects[:15],
        "score_deltas": score_deltas,
        "accuracy_delta": _mean_accuracy_delta,
        "acceptance_delta_pp": _acceptance_delta_pp,
        "new_failures": new_failures,
        "rollback_reason": rollback_reason,
        "rollback_class": classify_rollback_reason(rollback_reason).value,
        "do_not_retry": do_not_retry,
        # Task 18 — precise retry signatures over the rolled-back patches.
        "do_not_retry_patches": do_not_retry_patches,
        "affected_question_ids": affected_question_ids or [],
        "fixed_questions": sorted(_prev - _new),
        "still_failing": sorted(_prev & _new),
        "new_regressions": sorted(_new - _prev),
        "reflection_text": reflection_text,
        "refinement_mode": refinement_mode,
        "escalation_handled": escalation_handled,
        # Phase C2 identity fields.
        "root_cause": root_cause or "",
        "blame_set": _blame_norm,
        "source_cluster_ids": list(source_cluster_ids or []),
        "lever_set": _lever_set,
        # T2.1: iteration-independent cluster identity. Unlike the
        # pretty ``source_cluster_ids`` (which churn as H001→H001
        # between iterations for unrelated clusters), each signature
        # is a sha1 of ``base_question_ids + root_cause + blame`` so
        # "the same cluster" joins across iterations.
        "source_cluster_signatures": list(source_cluster_signatures or []),
    }
    if extra:
        entry.update(extra)
    return entry


def _log_target_fixed_disagreement(
    *,
    ag_id: str,
    baseline_source: str,
    pre_hard_qids: list[str],
    post_hard_qids: list[str],
    target_qids: tuple[str, ...],
    target_fixed_qids: tuple[str, ...],
) -> None:
    """Log when eval row deltas imply a fix but the control plane reports none."""
    pre_hard = set(pre_hard_qids)
    post_hard = set(post_hard_qids)
    target_set = set(str(q) for q in target_qids if str(q))
    fixed_by_delta = tuple(sorted(target_set & (pre_hard - post_hard)))
    if not fixed_by_delta or target_fixed_qids:
        return
    logger.warning(
        "CONTROL PLANE TARGET-FIXED DISAGREEMENT: AG=%s source=%s "
        "pre_hard=%s post_hard=%s target_qids=%s fixed_by_delta=%s "
        "target_fixed_qids=%s",
        ag_id,
        baseline_source,
        pre_hard_qids,
        post_hard_qids,
        list(target_qids),
        list(fixed_by_delta),
        list(target_fixed_qids),
    )
    print(
        _section("CONTROL PLANE TARGET-FIXED DISAGREEMENT", "!") + "\n"
        + _kv("AG", ag_id) + "\n"
        + _kv("Baseline source", baseline_source) + "\n"
        + _kv("Target QIDs", list(target_qids)) + "\n"
        + _kv("Fixed by hard-row delta", list(fixed_by_delta)) + "\n"
        + _kv("Control-plane target_fixed_qids", list(target_fixed_qids)) + "\n"
        + _bar("!")
    )


def _log_strategist_coverage_gap(
    *,
    iteration: int,
    uncovered_cluster_ids: list[str],
    cluster_question_counts: dict[str, int],
    rca_cards_present: dict[str, bool],
    strategist_action_groups: int,
    strategist_input_token_estimate: int | None,
    strategist_output_truncated: bool,
) -> None:
    """Log why the strategist did not cover one or more patchable hard clusters."""
    if not uncovered_cluster_ids:
        return
    logger.warning(
        "STRATEGIST COVERAGE GAP: iter=%s uncovered_cluster_ids=%s "
        "cluster_question_counts=%s rca_cards_present=%s "
        "strategist_action_groups=%s strategist_input_token_estimate=%s "
        "strategist_output_truncated=%s",
        iteration,
        uncovered_cluster_ids,
        {cid: cluster_question_counts.get(cid) for cid in uncovered_cluster_ids},
        {cid: rca_cards_present.get(cid, False) for cid in uncovered_cluster_ids},
        strategist_action_groups,
        strategist_input_token_estimate,
        strategist_output_truncated,
    )
    print(
        _section("STRATEGIST COVERAGE GAP", "!") + "\n"
        + _kv("Iteration", iteration) + "\n"
        + _kv("Uncovered cluster ids", uncovered_cluster_ids) + "\n"
        + _kv(
            "Cluster question counts",
            {cid: cluster_question_counts.get(cid) for cid in uncovered_cluster_ids},
        ) + "\n"
        + _kv(
            "RCA cards present",
            {cid: rca_cards_present.get(cid, False) for cid in uncovered_cluster_ids},
        ) + "\n"
        + _kv("Strategist action groups returned", strategist_action_groups) + "\n"
        + _kv("Strategist input token estimate", strategist_input_token_estimate) + "\n"
        + _kv("Strategist output truncated", strategist_output_truncated) + "\n"
        + _bar("!")
    )


def _qid_values(raw: object) -> list[str]:
    values: list[str] = []
    for item in raw or []:
        if isinstance(item, dict):
            value = item.get("question_id") or item.get("id")
        else:
            value = item
        if value:
            values.append(str(value))
    return list(dict.fromkeys(values))


def _cluster_qids_for_ids(
    source_clusters: list[dict],
    cluster_ids: list[str],
) -> list[str]:
    wanted = {str(cid) for cid in cluster_ids or [] if str(cid)}
    qids: list[str] = []
    for cluster in source_clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        if cid in wanted:
            qids.extend(_qid_values(cluster.get("question_ids") or []))
    return list(dict.fromkeys(qids))


def _backfill_patch_causal_metadata(
    *,
    patches: list[dict],
    action_group: dict,
    source_clusters: list[dict],
) -> list[dict]:
    """Attach AG/cluster causal metadata to broad strategist proposals.

    Explicit RCA metadata always wins. This helper fills only missing
    ``target_qids`` / ``_grounding_target_qids`` / source-cluster fields so
    the patch cap can distinguish broad AG proposals from precise RCA
    proposals.
    """
    ag_id = str(
        action_group.get("id")
        or action_group.get("action_group_id")
        or action_group.get("ag_id")
        or ""
    )
    source_cluster_ids = [
        str(cid) for cid in (action_group.get("source_cluster_ids") or []) if str(cid)
    ]
    primary_cluster_id = str(action_group.get("primary_cluster_id") or "")
    ag_qids = _qid_values(action_group.get("affected_questions") or [])
    if not ag_qids:
        ag_qids = _cluster_qids_for_ids(source_clusters, source_cluster_ids)

    enriched: list[dict] = []
    for patch in patches or []:
        item = dict(patch)
        if ag_id and not item.get("action_group_id"):
            item["action_group_id"] = ag_id
        if primary_cluster_id and not item.get("primary_cluster_id"):
            item["primary_cluster_id"] = primary_cluster_id
        if source_cluster_ids and not item.get("source_cluster_ids"):
            item["source_cluster_ids"] = list(source_cluster_ids)

        explicit_targets = _qid_values(item.get("target_qids") or [])
        grounding_targets = _qid_values(item.get("_grounding_target_qids") or [])
        target_qids = explicit_targets or grounding_targets or ag_qids
        if target_qids:
            item["target_qids"] = list(target_qids)
            item["_grounding_target_qids"] = list(target_qids)
        enriched.append(item)
    return enriched


def _attach_rca_theme_attribution(
    *,
    spark: Any,
    run_id: str,
    iteration_counter: int,
    ag_id: str,
    metadata_snapshot: dict,
    reflection: dict,
    prev_failure_qids: set[str] | None,
    new_failure_qids: set[str] | None,
    catalog: str,
    schema: str,
) -> None:
    """Attach and audit per-theme before/after eval outcome attribution."""
    try:
        from genie_space_optimizer.optimization.rca import (
            attribute_theme_outcomes,
        )

        themes = metadata_snapshot.get("_rca_themes") or []
        if not themes:
            return
        attributions = attribute_theme_outcomes(
            themes,
            prev_failure_qids=prev_failure_qids or set(),
            new_failure_qids=new_failure_qids or set(),
        )
        serialized = [
            {
                "rca_id": a.rca_id,
                "target_qids": list(a.target_qids),
                "fixed_qids": list(a.fixed_qids),
                "still_failing_qids": list(a.still_failing_qids),
                "target_regressed_qids": list(a.target_regressed_qids),
                "global_regressed_qids": list(a.global_regressed_qids),
                "regressed_qids": list(a.regressed_qids),
            }
            for a in attributions
        ]
        reflection["rca_theme_attribution"] = serialized
        if not serialized:
            return
        from genie_space_optimizer.optimization.state import (
            write_lever_loop_decisions,
        )

        write_lever_loop_decisions(
            spark,
            [
                {
                    "run_id": run_id,
                    "iteration": iteration_counter,
                    "ag_id": ag_id,
                    "decision_order": idx,
                    "stage_letter": "R",
                    "gate_name": "rca_theme_attribution",
                    "decision": "attributed",
                    "reason_code": None,
                    "metrics": row,
                }
                for idx, row in enumerate(serialized, start=1)
            ],
            catalog=catalog,
            schema=schema,
        )
    except Exception:
        logger.debug(
            "RCA theme attribution failed for iter %d",
            iteration_counter,
            exc_info=True,
        )


def _collect_regression_mining_iteration_context(
    reflection_buffer: list[dict],
    *,
    enable_rca_ledger: bool,
    enable_strategist_hints: bool,
    min_confidence: float,
) -> dict:
    """Collect rollback-mining context once for RCA ledger and hints."""
    if not (enable_rca_ledger or enable_strategist_hints):
        return {
            "all_insights": [],
            "visible_insights": [],
            "rca_findings": [],
            "strategist_hints": "",
        }

    from genie_space_optimizer.optimization.rca import (
        rca_findings_from_regression_insights,
    )
    from genie_space_optimizer.optimization.regression_mining import (
        collect_insights_from_reflection_buffer,
        render_strategist_hint_block,
        select_strategist_visible_insights,
    )

    all_insights = collect_insights_from_reflection_buffer(reflection_buffer)
    visible = select_strategist_visible_insights(
        all_insights,
        min_confidence=min_confidence,
        enabled=True,
    )
    return {
        "all_insights": all_insights,
        "visible_insights": visible,
        "rca_findings": (
            rca_findings_from_regression_insights(visible)
            if enable_rca_ledger else []
        ),
        "strategist_hints": (
            render_strategist_hint_block(visible)
            if enable_strategist_hints else ""
        ),
    }


# Phase C1 retired the earlier ``_is_schema_fatal_patch_error`` shim in
# favour of :class:`RollbackClass` and :func:`classify_rollback_reason`
# from ``optimization/rollback_class``. A deterministic schema rejection
# now surfaces as ``RollbackClass.SCHEMA_FAILURE`` so all gating code
# (infra retry budget, diminishing-returns filter, DO-NOT-RETRY guard)
# reads one source of truth.


def _is_schema_fatal_patch_error(error: Any) -> bool:
    """Backwards-compatible alias for tests and any legacy call sites.

    Returns True when ``error`` classifies as
    :attr:`RollbackClass.SCHEMA_FAILURE`. New code should call
    :func:`classify_rollback_reason` directly.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
        classify_rollback_reason,
    )

    if not error:
        return False
    return classify_rollback_reason(str(error)) == RollbackClass.SCHEMA_FAILURE


@dataclass(frozen=True)
class PatchBundleEvalSkip:
    skip: bool
    reason_code: str = ""
    reason_detail: str = ""


def _should_skip_eval_for_patch_bundle(
    *,
    patches: list[dict],
    apply_log: dict | None,
    stage: str,
) -> PatchBundleEvalSkip:
    """Return whether a patch bundle is ineligible for acceptance eval.

    The lever loop must not run a full eval for acceptance when the
    candidate made no space mutation. A zero-patch evaluation measures
    Genie/judge variance, not optimizer progress.
    """
    if stage == "post_grounding" and not patches:
        return PatchBundleEvalSkip(
            True,
            "no_grounded_patches",
            "Proposal grounding dropped every patch; no candidate state exists.",
        )
    if stage == "post_apply":
        applied = (apply_log or {}).get("applied") or []
        if not applied:
            return PatchBundleEvalSkip(
                True,
                "no_applied_patches",
                "Patch application produced no applied entries; no candidate state exists.",
            )
    return PatchBundleEvalSkip(False)


def _diminishing_returns(
    reflection_buffer: list[dict],
    epsilon: float | None = None,
    lookback: int | None = None,
) -> bool:
    """Return True if none of the last *lookback* actionable iterations
    achieved a mean accuracy improvement >= *epsilon*.

    Rolled-back iterations count as zero improvement, which correctly
    signals the optimizer is stuck. Phase C3: only entries whose
    ``rollback_class`` is ``CONTENT_REGRESSION`` (or which were
    accepted — those carry an ``accuracy_delta``) participate. Infra,
    schema, escalation, and other non-content rollbacks carry no
    content signal and are skipped so they don't artificially trip
    diminishing-returns before the loop has had a real chance to try
    a strategy.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )

    if epsilon is None:
        epsilon = DIMINISHING_RETURNS_EPSILON
    if lookback is None:
        lookback = DIMINISHING_RETURNS_LOOKBACK

    def _is_content_signal(r: dict) -> bool:
        if r.get("escalation_handled"):
            return False
        if r.get("accepted"):
            return True
        return r.get("rollback_class") == RollbackClass.CONTENT_REGRESSION.value

    content_signal = [r for r in reflection_buffer if _is_content_signal(r)]
    recent = content_signal[-lookback:]
    if len(recent) < lookback:
        return False

    for r in recent:
        progress_delta = r.get(
            "acceptance_delta_pp",
            r.get("accuracy_delta", 0.0),
        )
        if r.get("accepted") and progress_delta >= epsilon:
            return False
    return True


def _detect_divergence(
    reflection_buffer: list[dict],
    lookback: int = 4,
    min_sign_flips: int = 2,
) -> tuple[bool, str]:
    """T4.2: Detect accuracy *divergence* (thrashing).

    A loop that alternates accept → rollback → accept → rollback is
    making negative progress per unit cost — every accepted iteration
    is followed by another that takes it back. Stop the loop rather
    than burn more budget.

    Returns ``(diverging, rationale)``. ``diverging=True`` when the
    sign of ``accuracy_delta`` flips at least ``min_sign_flips`` times
    in the last ``lookback`` content-signal iterations. Rationale is a
    human-readable explanation for logs.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )

    def _is_content_signal(r: dict) -> bool:
        if r.get("escalation_handled"):
            return False
        if r.get("accepted"):
            return True
        return r.get("rollback_class") == RollbackClass.CONTENT_REGRESSION.value

    content_signal = [r for r in reflection_buffer if _is_content_signal(r)]
    recent = content_signal[-lookback:]
    if len(recent) < lookback:
        return False, ""

    # Compute sign series on accuracy_delta. Rolled-back iterations
    # don't carry a real accuracy_delta (they revert the space), but
    # the recorded delta is what the eval produced and is the right
    # signal here — we're measuring "did the space keep trying both
    # directions?" not "what did we end up at?".
    deltas = [r.get("accuracy_delta", 0.0) for r in recent]
    signs = [1 if d > 0.5 else (-1 if d < -0.5 else 0) for d in deltas]
    # Count sign flips (ignore zeros as non-signal).
    prev_sign = 0
    flips = 0
    for s in signs:
        if s == 0:
            continue
        if prev_sign != 0 and s != prev_sign:
            flips += 1
        prev_sign = s
    if flips >= min_sign_flips:
        return (
            True,
            f"{flips} accuracy-sign flips across last {lookback} "
            f"content-signal iterations (deltas={[round(d, 1) for d in deltas]})",
        )
    return False, ""


# Phase D1: which lever sets is the router allowed to try for a given
# root cause? If every one of them has been tried and rolled back, the
# cluster is truly dead and we can drop it. Keep this conservative —
# narrower than the strategist's "also Lever N" hints so a single
# lever-set miss doesn't suppress the whole cluster prematurely.
_FEASIBLE_LEVER_SETS_BY_ROOT_CAUSE: dict[str, tuple[frozenset[int], ...]] = {
    # SQL-shape causes: primary Lever 6 (sql_snippet), secondary Lever 5
    # (example_sql). These are the only two ways to actually install a
    # missing filter / aggregation / measure without touching benchmarks.
    "missing_filter":           (frozenset({6}), frozenset({5})),
    "missing_scd_filter":       (frozenset({6}), frozenset({5})),
    "missing_temporal_filter":  (frozenset({6}), frozenset({5})),
    "wrong_filter_condition":   (frozenset({6}), frozenset({5})),
    "wrong_aggregation":        (frozenset({6}), frozenset({5})),
    "wrong_measure":            (frozenset({6}), frozenset({5})),
    "missing_aggregation":      (frozenset({6}), frozenset({5})),
    "missing_dimension":        (frozenset({6}), frozenset({5})),
    "wrong_grouping":           (frozenset({6}), frozenset({5})),
    # Descriptive causes: Lever 1 is primary; Lever 5 instructions also fit.
    "wrong_column":             (frozenset({1}), frozenset({5})),
    "wrong_table":              (frozenset({1}), frozenset({5})),
    "description_mismatch":     (frozenset({1}),),
    "missing_synonym":          (frozenset({1}),),
    # Join causes: Lever 4 primary; Lever 5 also suggested by the prompt.
    "wrong_join":               (frozenset({4}), frozenset({5})),
    "wrong_join_spec":          (frozenset({4}), frozenset({5})),
    "missing_join_spec":        (frozenset({4}), frozenset({5})),
    "wrong_join_type":          (frozenset({5}),),
    # TVF causes: Lever 3.
    "tvf_parameter_error":      (frozenset({3}), frozenset({5})),
    # Routing / instruction causes: Lever 5 only.
    "asset_routing_error":      (frozenset({5}),),
    "missing_instruction":      (frozenset({5}),),
    "ambiguous_question":       (frozenset({5}),),
}


def _feasible_lever_sets(ft: str) -> tuple[frozenset[int], ...]:
    """Return the tuple of lever sets the router could plausibly try for
    root cause *ft*. Unknown root causes fall back to an empty tuple,
    which means the 3-tuple suppression in :func:`_filter_tried_clusters`
    can never fire — safe default (only the explicit legacy 2-tuple
    will suppress the cluster).
    """
    return _FEASIBLE_LEVER_SETS_BY_ROOT_CAUSE.get(ft, ())


def _compute_forbidden_ag_set(
    reflection_buffer: list[dict],
) -> set[tuple[str, Any, frozenset[int]]]:
    """Build the DO-NOT-RETRY forbidden set from the reflection buffer.

    Only CONTENT_REGRESSION rollbacks contribute — infra / schema / other
    classes don't count as evidence that the strategy was wrong. Returns
    a set of ``(root_cause, blame_set_norm, frozenset(lever_set))`` tuples.
    """
    from genie_space_optimizer.optimization.rollback_class import (
        RollbackClass,
    )

    forbidden: set[tuple[str, Any, frozenset[int]]] = set()
    for r in reflection_buffer:
        if r.get("accepted"):
            continue
        if r.get("escalation_handled"):
            continue
        if r.get("rollback_class") != RollbackClass.CONTENT_REGRESSION.value:
            continue
        rc = r.get("root_cause") or ""
        if not rc:
            continue
        # Defense in depth: persisted reflections JSON-decode tuples back to
        # lists. Normalize on read so resumed entries produce the same
        # forbidden tuple as _ag_collision_key produces on the live path.
        blame = _normalise_blame(r.get("blame_set"))
        lever_set = r.get("lever_set") or []
        if not lever_set:
            continue
        forbidden.add((rc, blame, frozenset(int(l) for l in lever_set)))
    return forbidden


def _ag_collision_key(
    ag: dict,
    ag_root_cause: str,
    ag_blame_set: Any,
    lever_keys: list[str],
) -> tuple[str, Any, frozenset[int]] | None:
    """Build the collision key for an action group.

    Returns ``None`` when the AG lacks enough identity to meaningfully
    collide (no root cause or no lever set). Normalises blame the same
    way :func:`_build_reflection_entry` does so keys can be compared.
    """
    if not ag_root_cause:
        return None
    if not lever_keys:
        return None
    return (
        ag_root_cause,
        _normalise_blame(ag_blame_set),
        frozenset(int(lk) for lk in lever_keys),
    )


def _filter_tried_clusters(
    clusters: list[dict],
    tried_root_causes: set[tuple],
) -> list[dict]:
    """Remove clusters whose root cause was already tried and rolled back.

    *tried_root_causes* stores tuples recorded at rollback time. Supports
    both legacy 2-tuples ``(ft, blame)`` and new 3-tuples
    ``(ft, blame, frozenset_of_levers)``. A cluster is suppressed when
    EITHER:

    * the legacy ``(ft, blame)`` 2-tuple is present (truly-dead cluster
      — rolled back across multiple distinct lever sets per Phase D3), OR
    * every feasible lever set for the cluster's root cause has a matching
      3-tuple entry (lever-aware suppression — we've exhausted the router's
      options for this cluster).

    Phase D1 is what makes the 3-tuple bookkeeping actually effective;
    before D1 the function silently ignored 3-tuples entirely.
    """
    if not tried_root_causes:
        return clusters
    legacy_keys: set[tuple[str, Any]] = set()
    lever_keys: set[tuple[str, Any, frozenset]] = set()
    for entry in tried_root_causes:
        if len(entry) == 2:
            legacy_keys.add(entry)
        elif len(entry) >= 3:
            lever_keys.add((entry[0], entry[1], entry[2]))

    filtered: list[dict] = []
    for c in clusters:
        ft = c.get("asi_failure_type") or c.get("root_cause", "other")
        blame = _normalise_blame(c.get("asi_blame_set"))
        if (ft, blame) in legacy_keys:
            continue
        feasible = _feasible_lever_sets(ft)
        if feasible and all(
            (ft, blame, frozenset(ls)) in lever_keys for ls in feasible
        ):
            continue
        filtered.append(c)
    return filtered


def _normalise_blame(blame_raw: Any) -> tuple[str, ...] | str:
    """Canonical blame representation used by reflection and tried-cluster keys."""
    from genie_space_optimizer.optimization.blame_normalization import (
        normalize_blame_key,
    )

    return normalize_blame_key(blame_raw)


def _extract_arbiter_actions_from_baseline(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Extract genie_correct arbiter actions from baseline iteration rows."""
    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not baseline_iter:
        return []

    rows_json = baseline_iter.get("rows_json")
    if isinstance(rows_json, str):
        try:
            rows_json = json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(rows_json, list):
        return []

    actions: list[dict] = []
    for row in rows_json:
        av = str(
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
            or "skipped"
        ).lower()
        if av != "genie_correct":
            continue
        genie_sql = (
            row.get("outputs/response")
            or (row.get("outputs") or {}).get("response", "")
        )
        question = (
            row.get("inputs/question")
            or (row.get("inputs") or {}).get("question", "")
        )
        if genie_sql and question:
            actions.append({
                "question": str(question),
                "new_expected_sql": str(genie_sql),
                "verdict": "genie_correct",
            })
    return actions


def _extract_arbiter_approved_benchmarks(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
    benchmarks: list[dict],
) -> tuple[list[dict], dict[str, int]]:
    """Return the subset of ``benchmarks`` whose baseline arbiter verdict
    was ``both_correct`` ONLY.

    Per docs/gsl-instruction-schema.md design decision:

    * ``both_correct`` — Genie and the benchmark's ``expected_sql`` both
      produce the same correct result. The ``expected_sql`` is gold
      standard and its patterns (aggregations, filters, derived
      expressions) are safe to mine into structured sql_snippets or
      join_specs.

    * ``genie_correct`` — Genie is right and the benchmark's
      ``expected_sql`` is WRONG (hence the separate repair path at
      :func:`_extract_arbiter_actions_from_baseline`). Mining from
      ``expected_sql`` here would ingest bad SQL. **EXCLUDED.**

    * ``ground_truth_correct``, ``neither_correct``, ``skipped``,
      anything else — EXCLUDED (conservative; only ``both_correct``
      qualifies as unambiguously safe source content).

    Empty-baseline fallback: if no baseline iteration is persisted yet
    (first GSO run on a fresh space, or baseline crashed), returns
    ``([], {})`` — caller falls back to schema-discovery-only mining.

    Returns
    -------
    (approved_benchmarks, verdict_counts)
        ``approved_benchmarks`` is the filtered subset.
        ``verdict_counts`` is a per-verdict tally across all matched
        rows (for the single-line diagnostic log).
    """
    approved: list[dict] = []
    verdict_counts: dict[str, int] = {}

    if not benchmarks:
        return approved, verdict_counts

    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not baseline_iter:
        return approved, verdict_counts

    rows_json = baseline_iter.get("rows_json")
    if isinstance(rows_json, str):
        try:
            rows_json = json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return approved, verdict_counts
    if not isinstance(rows_json, list):
        return approved, verdict_counts

    # Build {question_id: verdict} from baseline rows.
    verdict_by_qid: dict[str, str] = {}
    for row in rows_json:
        av = _get_arbiter_verdict(row)
        qid = _get_question_id(row)
        verdict_counts[av] = verdict_counts.get(av, 0) + 1
        if qid:
            verdict_by_qid[qid] = av

    for b in benchmarks:
        qid = str(b.get("id") or b.get("question_id") or "").strip()
        if not qid:
            continue
        if verdict_by_qid.get(qid) == "both_correct":
            approved.append(b)

    return approved, verdict_counts


# ── Cross-Iteration Verdict History ────────────────────────────────────


def _get_arbiter_verdict(row: dict) -> str:
    """Extract the arbiter verdict string from an evaluation row."""
    return str(
        row.get("arbiter/value")
        or row.get("feedback/arbiter/value")
        or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
        or "skipped"
    ).lower()


def _get_question_id(row: dict) -> str:
    """Extract the question ID from an evaluation row."""
    _rq = row.get("request") or {}
    if isinstance(_rq, str):
        try:
            _rq = json.loads(_rq)
        except (json.JSONDecodeError, TypeError):
            _rq = {}
    _rqk = _rq.get("kwargs", {}) if isinstance(_rq, dict) else {}
    return str(
        row.get("inputs/question_id")
        or (row.get("inputs") or {}).get("question_id", "")
        or row.get("question_id")
        or _rqk.get("question_id")
        or (_rq.get("question_id") if isinstance(_rq, dict) else None)
        or "?"
    )


def _is_quarantined_qid(qid: str, quarantined: set[str]) -> bool:
    """Return True when *qid* (or its base form) is in the quarantine set.

    The benchmark-suffix scheme produces qids like ``retail_..._002:v2``
    and ``retail_..._002:v3`` whose base qid is ``retail_..._002``.
    Exact-equality match misses these, leaking quarantined questions
    through the row filter and the cluster prune. Strip the ``:vN``
    suffix on either side so the base qid is the canonical key.

    Defensive on both directions: a quarantined entry may itself be
    a base qid (the common case) or an already-suffixed string (when
    a previous iteration recorded the suffixed variant directly).
    """
    if not qid or not quarantined:
        return False
    if qid in quarantined:
        return True
    if ":" in qid:
        base = qid.split(":", 1)[0]
        if base in quarantined:
            return True
    for q in quarantined:
        if ":" in q and q.split(":", 1)[0] == qid:
            return True
    return False


def _get_question_text(row: dict) -> str:
    """Extract the question text from an evaluation row."""
    return str(
        row.get("inputs/question")
        or (row.get("inputs") or {}).get("question", "")
    )


def _get_genie_sql(row: dict) -> str:
    """Extract Genie's generated SQL from an evaluation row."""
    return str(
        row.get("outputs/response")
        or (row.get("outputs") or {}).get("response", "")
    )


def _get_expected_sql(row: dict) -> str:
    """Extract the expected (ground truth) SQL from an evaluation row."""
    return str(
        row.get("inputs/expected_response")
        or (row.get("inputs") or {}).get("expected_response", "")
        or row.get("expected_response/value")
    )


def _get_arbiter_rationale(row: dict) -> str:
    """Extract the arbiter rationale from an evaluation row."""
    return str(
        row.get("arbiter/rationale")
        or row.get("feedback/arbiter/rationale")
        or ""
    )


@dataclass
class VerdictEntry:
    """A single arbiter observation for a question in one evaluation."""
    iteration: int
    verdict: str
    genie_sql: str
    expected_sql: str
    question_text: str
    rationale: str


def _build_verdict_history(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
) -> dict[str, list[VerdictEntry]]:
    """Build per-question verdict history across all full-scope evaluations.

    Returns ``{question_id: [VerdictEntry, ...]}``, ordered by iteration.

    T1.10: duplicate benchmark rows (same question_id appearing multiple
    times in the corpus) previously inflated the per-iteration entry count
    — e.g. `_003` appearing 3× produced 3 VerdictEntry rows with
    ``iteration=1`` and the persistence counter then reported
    "Failed 3/3 evals (3 consecutive)" after a single iteration. Group by
    ``(base_qid, iteration)`` and roll up the verdict: passing iff ALL
    trials in that iteration passed, otherwise the most common
    non-passing verdict wins. This ensures per-iteration count caps at 1.
    """
    _PASSING = {"both_correct"}
    all_iters = load_all_full_iterations(spark, run_id, catalog, schema)
    history: dict[str, list[VerdictEntry]] = {}

    for iteration_row in all_iters:
        iteration_num = int(iteration_row.get("iteration", 0))
        rows_json = iteration_row.get("rows_json")
        if isinstance(rows_json, str):
            try:
                rows_json = json.loads(rows_json)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(rows_json, list):
            continue

        trials_by_qid: dict[str, list[VerdictEntry]] = {}
        for row in rows_json:
            qid = _get_question_id(row)
            if qid == "?":
                continue
            base_qid = str(qid).split(":v")[0]
            trials_by_qid.setdefault(base_qid, []).append(
                VerdictEntry(
                    iteration=iteration_num,
                    verdict=_get_arbiter_verdict(row),
                    genie_sql=_get_genie_sql(row),
                    expected_sql=_get_expected_sql(row),
                    question_text=_get_question_text(row),
                    rationale=_get_arbiter_rationale(row),
                )
            )

        for base_qid, trial_entries in trials_by_qid.items():
            if all(t.verdict in _PASSING for t in trial_entries):
                rolled = trial_entries[0]
            else:
                from collections import Counter as _Counter
                _non_pass = [t for t in trial_entries if t.verdict not in _PASSING]
                _dominant_verdict = _Counter(t.verdict for t in _non_pass).most_common(1)[0][0]
                rolled = next(t for t in _non_pass if t.verdict == _dominant_verdict)
            history.setdefault(base_qid, []).append(rolled)

    return history


def _compute_convergence_state(
    entries: list,
    *,
    lookback: int = 4,
) -> tuple[str, str]:
    """T4.1: Return ``(state, rationale)`` for a question's recent history.

    States (in priority order):
      - ``fixed``:       currently passing and passed in the last 2+ iters
      - ``worsening``:   was passing in early window, failing in recent
      - ``improving``:   was failing in early window, passing in recent
      - ``oscillating``: alternating pass/fail in the lookback window
      - ``stuck``:       failing in all ``lookback`` most recent iterations
      - ``intermittent``: some failures but not stuck/oscillating/trending
      - ``new``:         too few evals to classify

    *entries* must be the list of ``VerdictEntry`` for the question in
    chronological order. Only the last ``lookback`` entries are
    considered. A verdict is treated as passing when its arbiter value
    is ``both_correct``; anything else (including ``genie_correct``,
    ``ground_truth_correct``, ``neither_correct``) counts as failing —
    same rule the existing persistence summary applies.
    """
    _PASSING = {"both_correct"}
    if not entries:
        return "new", "no evaluations"
    window = entries[-lookback:]
    if len(window) < 2:
        return "new", f"only {len(window)} eval(s) in history"

    passes = [e.verdict in _PASSING for e in window]
    last = passes[-1]
    n = len(passes)
    n_pass = sum(passes)
    n_fail = n - n_pass

    if last and n_pass >= 2 and all(passes[-2:]):
        return (
            "fixed",
            f"passed last 2+ iters ({n_pass}/{n} in window)",
        )
    if n_fail == n:
        return (
            "stuck",
            f"failed all {n} iterations in window",
        )
    flips = sum(1 for i in range(1, n) if passes[i] != passes[i - 1])
    # Require *more than half* of adjacent-pair transitions to be flips
    # so that [P,P,F,P] (2 flips in 4 items, half) reads as
    # ``intermittent``, while [P,F,P,F] (3 flips in 4 items) reads as
    # ``oscillating``. Minimum of 2 flips keeps short windows honest.
    if flips >= max(2, n // 2 + 1):
        return (
            "oscillating",
            f"{flips} pass/fail flips across {n} iters",
        )
    half = max(1, n // 2)
    early_pass = sum(passes[:half])
    recent_pass = sum(passes[half:])
    if recent_pass > early_pass:
        return (
            "improving",
            f"{early_pass}/{half} -> {recent_pass}/{n - half} passes",
        )
    if early_pass > recent_pass:
        return (
            "worsening",
            f"{early_pass}/{half} -> {recent_pass}/{n - half} passes",
        )
    return (
        "intermittent",
        f"{n_pass}/{n} passes, no clear trend",
    )


def _build_question_persistence_summary(
    verdict_history: dict[str, list["VerdictEntry"]],
    reflection_buffer: list[dict],
    *,
    min_failures: int | None = None,
) -> tuple[str, dict[str, dict]]:
    """Render a per-question failure persistence summary for the strategist.

    Only includes questions that failed in >= *min_failures* iterations.
    For each, shows the question text, consecutive failure count, verdict
    breakdown, patches previously tried for that question, and an
    exhaustion classification.

    Returns ``(text, structured)`` where *text* feeds the strategist prompt
    and *structured* is ``{qid: {question_text, fail_count, max_consecutive,
    classification, patches_tried, fail_iterations}}`` for trace enrichment
    and hard-quarantine decisions.
    """
    from genie_space_optimizer.common.config import PERSISTENCE_MIN_FAILURES

    if min_failures is None:
        min_failures = PERSISTENCE_MIN_FAILURES
    if not verdict_history:
        return "(No cross-iteration verdict data available yet.)", {}

    _PASSING = {"both_correct"}
    _ADDITIVE_PATCH_TYPES = {"add_instruction", "add_example_sql"}

    q_patches: dict[str, list[tuple[int, str]]] = {}
    for entry in reflection_buffer:
        iter_n = entry.get("iteration", 0)
        affected = entry.get("affected_question_ids", [])
        patch_types = set()
        for part in entry.get("action", "").split(", "):
            pt = part.split(" on ")[0].strip() if " on " in part else ""
            if pt:
                patch_types.add(pt)
        for qid in affected:
            for pt in patch_types:
                q_patches.setdefault(qid, []).append((iter_n, pt))

    persistent: list[dict] = []
    for qid, entries in verdict_history.items():
        non_passing = [e for e in entries if e.verdict not in _PASSING]
        if len(non_passing) < min_failures:
            continue

        consecutive = 0
        max_consecutive = 0
        for e in entries:
            if e.verdict not in _PASSING:
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0

        verdict_counts: dict[str, int] = {}
        for e in non_passing:
            verdict_counts[e.verdict] = verdict_counts.get(e.verdict, 0) + 1

        q_text = non_passing[-1].question_text if non_passing else "?"
        fail_iters = sorted({e.iteration for e in non_passing})

        tried = q_patches.get(qid, [])
        additive_counts: dict[str, int] = {}
        for _, pt in tried:
            if pt in _ADDITIVE_PATCH_TYPES:
                additive_counts[pt] = additive_counts.get(pt, 0) + 1

        exhausted = all(
            additive_counts.get(pt, 0) >= 2 for pt in _ADDITIVE_PATCH_TYPES
        )

        if max_consecutive < 2:
            classification = "INTERMITTENT"
        elif exhausted:
            classification = "ADDITIVE_LEVERS_EXHAUSTED"
        else:
            classification = "PERSISTENT"

        # T4.1: compute a second-axis trajectory label — convergence
        # state. Unlike ``classification`` (which is about exhaustion),
        # this describes the *direction*: fixed / improving /
        # oscillating / stuck / worsening / intermittent. The strategist
        # sees both so "oscillating" signals different action than
        # "worsening" even if classification is the same.
        _conv_state, _conv_rationale = _compute_convergence_state(entries)

        persistent.append({
            "qid": qid,
            "question_text": q_text,
            "fail_count": len(non_passing),
            "total_evals": len(entries),
            "max_consecutive": max_consecutive,
            "verdict_counts": verdict_counts,
            "fail_iterations": fail_iters,
            "patches_tried": tried,
            "classification": classification,
            "convergence_state": _conv_state,
            "convergence_rationale": _conv_rationale,
        })

    structured: dict[str, dict] = {}
    for p in persistent:
        structured[p["qid"]] = {
            "question_text": p["question_text"],
            "fail_count": p["fail_count"],
            "total_evals": p["total_evals"],
            "max_consecutive": p["max_consecutive"],
            "classification": p["classification"],
            "patches_tried": p["patches_tried"],
            "fail_iterations": p["fail_iterations"],
            "verdict_counts": p["verdict_counts"],
            # T4.1: carry trajectory state so the strategist and any
            # temporary-quarantine logic (T4.3) can differentiate
            # "oscillating" from "worsening" from plain "stuck".
            "convergence_state": p["convergence_state"],
            "convergence_rationale": p["convergence_rationale"],
        }

    if not persistent:
        return "(No persistent failures detected across iterations.)", structured

    persistent.sort(key=lambda p: (-p["max_consecutive"], -p["fail_count"]))

    lines: list[str] = [
        "Questions failing across multiple iterations despite fix attempts:",
        "",
    ]
    for p in persistent:
        lines.append(f"### {p['qid']}: \"{p['question_text'][:120]}\"")
        lines.append(
            f"  Failed {p['fail_count']}/{p['total_evals']} evals "
            f"({p['max_consecutive']} consecutive)"
        )
        vstr = ", ".join(f"{v}={c}" for v, c in sorted(p["verdict_counts"].items()))
        lines.append(f"  Verdicts: {vstr}")
        lines.append(f"  Failed in iterations: {p['fail_iterations']}")
        if p["patches_tried"]:
            patch_lines = []
            for it, pt in p["patches_tried"]:
                patch_lines.append(f"iter{it}: {pt}")
            lines.append(f"  Patches tried: {'; '.join(patch_lines)}")
        lines.append(f"  ASSESSMENT: {p['classification']}")
        lines.append(
            f"  CONVERGENCE: {p['convergence_state']} "
            f"({p['convergence_rationale']})"
        )
        lines.append("")

    return "\n".join(lines), structured


def _validate_tvf_removal_coverage(
    tvf_identifier: str,
    benchmarks: list[dict],
    schema_overlap: dict,
    metadata_snapshot: dict,
) -> dict:
    """Hard gate: verify alternative assets exist before allowing TVF removal.

    Only TVFs can be removed through escalation — tables and MVs are rejected.
    If the TVF's output columns are not sufficiently covered by other assets
    in the Genie Space, the removal is rejected.

    Returns ``{"valid": bool, "reason": str, ...}``.
    """
    tvf_lower = tvf_identifier.lower()

    is_tvf = any(
        isinstance(fn, dict) and fn.get("identifier", "").lower() == tvf_lower
        for fn in (metadata_snapshot.get("instructions") or {}).get("sql_functions", [])
    )

    if not is_tvf:
        ds = metadata_snapshot.get("data_sources", {})
        if not isinstance(ds, dict):
            ds = {}
        is_table = any(
            (t.get("identifier", "").lower() == tvf_lower or t.get("name", "").lower() == tvf_lower)
            for t in ds.get("tables", [])
        )
        is_mv = any(
            (m.get("identifier", "").lower() == tvf_lower or m.get("name", "").lower() == tvf_lower)
            for m in ds.get("metric_views", [])
        )
        if is_table:
            return {
                "valid": False,
                "reason": f"Cannot remove table '{tvf_identifier}' — only TVFs may be removed via escalation",
                "affected_questions": [],
                "coverage_ratio": 0.0,
                "uncovered_columns": [],
            }
        if is_mv:
            return {
                "valid": False,
                "reason": f"Cannot remove metric view '{tvf_identifier}' — only TVFs may be removed via escalation",
                "affected_questions": [],
                "coverage_ratio": 0.0,
                "uncovered_columns": [],
            }
        return {
            "valid": False,
            "reason": f"Asset '{tvf_identifier}' not found as a TVF in the Genie Space",
            "affected_questions": [],
            "coverage_ratio": 0.0,
            "uncovered_columns": [],
        }

    affected_questions = [
        b.get("question_id", b.get("id", ""))
        for b in benchmarks
        if tvf_lower in (b.get("expected_asset") or "").lower()
    ]

    coverage_ratio = schema_overlap.get("coverage_ratio", 0.0)
    uncovered = schema_overlap.get("uncovered_columns", [])
    full_coverage = schema_overlap.get("full_coverage", False)

    if affected_questions and not full_coverage and coverage_ratio < 0.5:
        return {
            "valid": False,
            "reason": (
                f"Insufficient alternative coverage for TVF '{tvf_identifier}': "
                f"{coverage_ratio:.0%} of columns covered, {len(uncovered)} uncovered "
                f"({', '.join(uncovered[:5])}). {len(affected_questions)} benchmark "
                f"question(s) reference this TVF."
            ),
            "affected_questions": affected_questions,
            "coverage_ratio": coverage_ratio,
            "uncovered_columns": uncovered,
        }

    return {
        "valid": True,
        "reason": (
            f"TVF '{tvf_identifier}' coverage OK: {coverage_ratio:.0%} columns covered"
            + (f", {len(affected_questions)} benchmark question(s) affected" if affected_questions else "")
        ),
        "affected_questions": affected_questions,
        "coverage_ratio": coverage_ratio,
        "uncovered_columns": uncovered,
    }


def _score_tvf_removal_confidence(
    tvf_identifier: str,
    benchmarks: list[dict],
    verdict_history: dict[str, list["VerdictEntry"]],
    reflection_buffer: list[dict],
    schema_overlap: dict,
    asi_provenance: list[dict],
    *,
    min_iterations: int | None = None,
) -> str | None:
    """Tiered confidence model for TVF removal.

    Returns ``"high"``, ``"medium"``, ``"low"``, or ``None`` if the
    iteration gate has not been met yet (too early to consider removal).
    """
    from genie_space_optimizer.common.config import (
        TVF_REMOVAL_BLAME_THRESHOLD,
        TVF_REMOVAL_MIN_ITERATIONS,
    )

    if min_iterations is None:
        min_iterations = TVF_REMOVAL_MIN_ITERATIONS

    _PASSING = {"both_correct"}
    tvf_lower = tvf_identifier.lower()

    blamed_qids: set[str] = set()
    for prov in asi_provenance:
        blame = prov.get("blame_set")
        if isinstance(blame, str):
            try:
                blame = json.loads(blame)
            except (json.JSONDecodeError, TypeError):
                blame = [blame]
        if not isinstance(blame, list):
            continue
        for b in blame:
            if tvf_lower in str(b).lower():
                qid = prov.get("question_id", "")
                if qid:
                    blamed_qids.add(qid)

    max_consecutive = 0
    for qid in blamed_qids:
        entries = verdict_history.get(qid, [])
        consec = 0
        for e in entries:
            if e.verdict not in _PASSING:
                consec += 1
                max_consecutive = max(max_consecutive, consec)
            else:
                consec = 0

    if max_consecutive < min_iterations:
        return None

    gt_refs = sum(
        1 for b in benchmarks
        if tvf_lower in (b.get("expected_asset") or "").lower()
        or tvf_lower in (b.get("expected_sql") or "").lower()
    )

    if gt_refs > 0:
        return "low"

    blame_iterations: set[int] = set()
    for prov in asi_provenance:
        blame = prov.get("blame_set")
        if isinstance(blame, str):
            try:
                blame = json.loads(blame)
            except (json.JSONDecodeError, TypeError):
                blame = [blame]
        if not isinstance(blame, list):
            continue
        for b in blame:
            if tvf_lower in str(b).lower():
                it = prov.get("iteration")
                if it is not None:
                    blame_iterations.add(int(it))

    full_coverage = schema_overlap.get("full_coverage", False)

    if full_coverage and len(blame_iterations) >= TVF_REMOVAL_BLAME_THRESHOLD:
        return "high"

    uncovered = schema_overlap.get("uncovered_columns", [])
    if schema_overlap.get("coverage_ratio", 0) > 0:
        uncov_in_benchmarks = False
        for col in uncovered:
            for b in benchmarks:
                if col.lower() in (b.get("expected_sql") or "").lower():
                    uncov_in_benchmarks = True
                    break
            if uncov_in_benchmarks:
                break
        if not uncov_in_benchmarks:
            return "medium"

    return "low"


def _handle_escalation(
    escalation: str,
    ag: dict,
    *,
    w: Any,
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    domain: str,
    iteration: int,
    benchmarks: list[dict],
    verdict_history: dict[str, list["VerdictEntry"]],
    reflection_buffer: list[dict],
    metadata_snapshot: dict,
) -> dict:
    """Dispatch an escalation action from the strategist.

    Returns ``{"handled": True/False, "action": "...", "detail": {...}}``.
    """
    from genie_space_optimizer.common.uc_metadata import check_tvf_schema_overlap
    from genie_space_optimizer.optimization.labeling import flag_for_human_review
    from genie_space_optimizer.optimization.state import (
        load_provenance,
        write_queued_patch,
    )

    affected = ag.get("affected_questions", [])
    result: dict[str, Any] = {"handled": False, "action": escalation, "detail": {}}

    if escalation == "remove_tvf":
        lever3 = ag.get("lever_directives", {}).get("3", {})
        funcs = lever3.get("functions", [])
        tvf_id = ""
        for f in funcs:
            tvf_id = f.get("identifier") or f.get("function") or ""
            if tvf_id:
                break
        if not tvf_id:
            logger.warning("Escalation remove_tvf but no TVF identifier in lever 3")
            result["detail"] = {"error": "no_tvf_identifier"}
            return result

        schema_overlap = check_tvf_schema_overlap(spark, tvf_id, metadata_snapshot)

        coverage_validation = _validate_tvf_removal_coverage(
            tvf_id, benchmarks, schema_overlap, metadata_snapshot,
        )
        if not coverage_validation["valid"]:
            logger.warning(
                "TVF removal coverage check failed for %s: %s",
                tvf_id, coverage_validation["reason"],
            )
            result["detail"] = {
                "error": "coverage_check_failed",
                "reason": coverage_validation["reason"],
                "affected_questions": coverage_validation["affected_questions"],
                "coverage_ratio": coverage_validation["coverage_ratio"],
            }
            return result

        prov_df = load_provenance(spark, run_id, catalog, schema)
        prov_list = prov_df.to_dict("records") if not prov_df.empty else []

        confidence = _score_tvf_removal_confidence(
            tvf_id, benchmarks, verdict_history, reflection_buffer,
            schema_overlap, prov_list,
        )
        previous_tvf_asset: dict = {}
        for fn in (metadata_snapshot.get("instructions") or {}).get("sql_functions", []):
            if isinstance(fn, dict) and fn.get("identifier") == tvf_id:
                previous_tvf_asset = dict(fn)
                break

        result["detail"]["confidence"] = confidence
        result["detail"]["tvf_identifier"] = tvf_id
        result["detail"]["tvf_id"] = tvf_id
        result["detail"]["previous_tvf_asset"] = previous_tvf_asset
        result["detail"]["schema_overlap"] = schema_overlap

        if confidence is None:
            logger.info(
                "TVF removal of %s: iteration gate not met — deferring",
                tvf_id,
            )
            result["detail"]["deferred"] = True
            return result

        result["handled"] = True

        if confidence == "low":
            write_queued_patch(
                spark, run_id, iteration, "remove_tvf", tvf_id,
                catalog, schema,
                confidence_tier="low",
                coverage_analysis=schema_overlap,
                blame_iterations=0,
            )
            flag_for_human_review(
                spark, run_id, catalog, schema, domain,
                [{
                    "question_id": q,
                    "question_text": "",
                    "reason": f"Low-confidence TVF removal recommended: {tvf_id}",
                    "iterations_failed": 0,
                    "patches_tried": "remove_tvf",
                } for q in (affected or [tvf_id])],
            )
            result["detail"]["tier_action"] = "flagged_only"

        elif confidence == "medium":
            result["detail"]["tier_action"] = "apply_and_flag"
            flag_for_human_review(
                spark, run_id, catalog, schema, domain,
                [{
                    "question_id": q,
                    "question_text": "",
                    "reason": f"Medium-confidence TVF removal applied: {tvf_id} — please verify",
                    "iterations_failed": 0,
                    "patches_tried": "remove_tvf",
                } for q in (affected or [tvf_id])],
            )

        else:
            result["detail"]["tier_action"] = "auto_apply"

    elif escalation == "gt_repair":
        logger.info(
            "Escalation gt_repair for questions %s — running inline arbiter corrections",
            affected,
        )
        _corr_result = _run_arbiter_corrections(
            w, spark, run_id, catalog, schema, domain,
            force_adopt_qids=set(affected) if affected else None,
            data_profile=metadata_snapshot.get("_data_profile"),
        )
        _total_corrections = (
            _corr_result.get("gc_applied", 0)
            + _corr_result.get("nc_repaired", 0)
        )
        result["handled"] = True
        result["detail"]["corrections_applied"] = _total_corrections
        result["detail"]["gc_applied"] = _corr_result.get("gc_applied", 0)
        result["detail"]["nc_repaired"] = _corr_result.get("nc_repaired", 0)
        result["detail"]["corrected_qids"] = sorted(_corr_result.get("corrected_qids", set()))
        result["detail"]["quarantined_qids"] = sorted(_corr_result.get("quarantined_qids", set()))

        if _total_corrections == 0 and affected:
            _unfixed_qids = [
                q for q in affected
                if q not in _corr_result.get("corrected_qids", set())
                and q not in _corr_result.get("quarantined_qids", set())
            ]
            if _unfixed_qids:
                _root_cause = ag.get("root_cause_summary", "Ground truth SQL may be incorrect")
                flag_for_human_review(
                    spark, run_id, catalog, schema, domain,
                    [{
                        "question_id": q,
                        "question_text": "",
                        "reason": f"GT_REPAIR_UNRESOLVED: {_root_cause[:200]}",
                        "iterations_failed": 0,
                        "patches_tried": "gt_repair (arbiter corrections failed)",
                    } for q in _unfixed_qids],
                )
                result["detail"]["flagged_for_review"] = len(_unfixed_qids)
                logger.info(
                    "gt_repair: flagged %d questions for human review "
                    "(arbiter could not fix)",
                    len(_unfixed_qids),
                )

    elif escalation == "flag_for_review":
        flag_for_human_review(
            spark, run_id, catalog, schema, domain,
            [{
                "question_id": q,
                "question_text": "",
                "reason": ag.get("root_cause_summary", "Strategist flagged for review"),
                "iterations_failed": 0,
                "patches_tried": "",
            } for q in affected],
        )
        result["handled"] = True
        result["detail"]["flagged_count"] = len(affected)
    else:
        logger.warning("Unknown escalation type: %s", escalation)

    return result


def _extract_confirmed_corrections(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    already_corrected: set[str] | None = None,
    force_adopt_qids: set[str] | None = None,
) -> list[dict]:
    """Return benchmark corrections for questions with cross-iteration ``genie_correct`` confirmation.

    A question qualifies when it received ``genie_correct`` in at least
    ``GENIE_CORRECT_CONFIRMATION_THRESHOLD`` independent evaluations.
    Questions in *force_adopt_qids* bypass this threshold and are adopted
    with a single ``genie_correct`` evaluation.

    Uses the most recent Genie SQL as the corrected expected SQL.
    """
    history = _build_verdict_history(spark, run_id, catalog, schema)
    corrected = already_corrected or set()
    force = force_adopt_qids or set()
    actions: list[dict] = []

    for qid, entries in history.items():
        if qid in corrected:
            continue
        gc_entries = [e for e in entries if e.verdict == "genie_correct"]
        gc_iterations = {e.iteration for e in gc_entries}
        threshold = 1 if qid in force else GENIE_CORRECT_CONFIRMATION_THRESHOLD
        if len(gc_iterations) < threshold:
            continue
        latest = max(gc_entries, key=lambda e: e.iteration)
        if latest.genie_sql:
            actions.append({
                "question": latest.question_text,
                "question_id": qid,
                "new_expected_sql": latest.genie_sql,
                "verdict": "genie_correct",
                "confirmation_count": len(gc_iterations),
            })
    return actions


def _extract_neither_correct_repair_candidates(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    *,
    already_repaired: set[str] | None = None,
) -> list[dict]:
    """Return questions that need GT repair due to repeated ``neither_correct`` verdicts.

    A question qualifies when it received ``neither_correct`` in at least
    ``NEITHER_CORRECT_REPAIR_THRESHOLD`` independent evaluations.
    """
    history = _build_verdict_history(spark, run_id, catalog, schema)
    repaired = already_repaired or set()
    candidates: list[dict] = []

    for qid, entries in history.items():
        if qid in repaired:
            continue
        nc_entries = [e for e in entries if e.verdict == "neither_correct"]
        nc_iterations = {e.iteration for e in nc_entries}
        if len(nc_iterations) < NEITHER_CORRECT_REPAIR_THRESHOLD:
            continue

        consecutive_nc = 0
        for e in reversed(entries):
            if e.verdict == "neither_correct":
                consecutive_nc += 1
            else:
                break

        latest = max(nc_entries, key=lambda e: e.iteration)
        rationales = [e.rationale for e in nc_entries if e.rationale]

        candidates.append({
            "question": latest.question_text,
            "question_id": qid,
            "genie_sql": latest.genie_sql,
            "expected_sql": latest.expected_sql,
            "rationale": " | ".join(rationales[-3:]),
            "nc_count": len(nc_iterations),
            "consecutive_nc": consecutive_nc,
        })
    return candidates


def _should_quarantine(candidate: dict) -> bool:
    """Decide whether a ``neither_correct`` question should be quarantined."""
    return candidate.get("consecutive_nc", 0) >= NEITHER_CORRECT_QUARANTINE_THRESHOLD




def _attempt_gt_repair(
    w: WorkspaceClient,
    candidate: dict,
    spark: Any,
    *,
    warehouse_id: str = "",
) -> str | None:
    """Use LLM to produce a corrected ground-truth SQL for a ``neither_correct`` question.

    Returns the validated corrected SQL string, or ``None`` if repair fails.
    """
    from genie_space_optimizer.optimization.evaluation import _call_llm_for_scoring
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    prompt = format_mlflow_template(
        GT_REPAIR_PROMPT,
        question=candidate["question"],
        expected_sql=candidate.get("expected_sql", ""),
        genie_sql=candidate.get("genie_sql", ""),
        rationale=candidate.get("rationale", "No rationale available"),
    )

    try:
        result = _call_llm_for_scoring(w, prompt)
        repaired_sql = ""
        if isinstance(result, str):
            repaired_sql = result.strip()
        elif isinstance(result, dict):
            repaired_sql = (
                result.get("sql", "")
                or result.get("corrected_sql", "")
                or result.get("query", "")
            ).strip()

        if not repaired_sql:
            logger.warning("GT repair returned empty SQL for: %s", candidate["question"][:60])
            return None

        is_valid, val_err = validate_ground_truth_sql(
            repaired_sql, spark, execute=True,
            w=w, warehouse_id=warehouse_id,
        )
        if not is_valid:
            logger.warning(
                "GT repair SQL failed validation for '%s': %s",
                candidate["question"][:60], val_err[:200],
            )
            return None

        return repaired_sql
    except Exception:
        logger.warning("GT repair LLM call failed for: %s", candidate["question"][:60], exc_info=True)
        return None


def _run_arbiter_corrections(
    w: WorkspaceClient,
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    domain: str,
    *,
    already_corrected: set[str] | None = None,
    already_repaired: set[str] | None = None,
    quarantined_qids: set[str] | None = None,
    force_adopt_qids: set[str] | None = None,
    data_profile: dict | None = None,
) -> dict:
    """Run the full cross-iteration arbiter correction pipeline.

    1. ``genie_correct`` confirmations → benchmark corrections
    2. ``neither_correct`` repairs → LLM-assisted GT repair or quarantine

    When *force_adopt_qids* is provided, questions in that set bypass the
    normal ``GENIE_CORRECT_CONFIRMATION_THRESHOLD`` and are adopted with a
    single ``genie_correct`` evaluation.  This is used when the strategist
    explicitly escalates ``gt_repair`` for specific questions.

    Returns ``{gc_applied, gc_skipped, nc_repaired, nc_quarantined, corrected_qids, quarantined_qids}``.
    """
    from genie_space_optimizer.optimization.benchmarks import (
        apply_benchmark_corrections,
        quarantine_benchmark_question,
    )

    uc_schema = f"{catalog}.{schema}"
    corrected = set(already_corrected or set())
    repaired = set(already_repaired or set())
    quarantined = set(quarantined_qids or set())

    gc_applied = 0
    gc_skipped = 0
    nc_repaired = 0
    nc_quarantined = 0

    # ── Phase 1: genie_correct confirmations ──────────────────────────
    gc_actions = _extract_confirmed_corrections(
        spark, run_id, catalog, schema,
        already_corrected=corrected,
        force_adopt_qids=force_adopt_qids,
    )

    if gc_actions:
        print(
            f"\n-- PER-QUESTION ARBITER CORRECTIONS " + "-" * 16 + "\n"
            f"  Confirmed genie_correct questions: {len(gc_actions)}"
        )
        for ac in gc_actions:
            print(
                f"    - [{ac.get('question_id', '?')}] "
                f"\"{ac['question'][:60]}\" "
                f"(confirmed in {ac.get('confirmation_count', '?')} evals)"
            )

        result = apply_benchmark_corrections(
            gc_actions, spark, uc_schema, domain,
            data_profile=data_profile,
        )
        gc_applied = result["applied"]
        gc_skipped = result["skipped"]
        print(
            f"  Applied: {gc_applied}, Skipped: {gc_skipped}"
        )
        if result["errors"]:
            print(f"  Errors: {result['errors'][:3]}")
        print("-" * 52)

        for ac in gc_actions:
            qid = ac.get("question_id")
            if qid:
                corrected.add(qid)

    # ── Phase 2: neither_correct repair / quarantine ──────────────────
    nc_candidates = _extract_neither_correct_repair_candidates(
        spark, run_id, catalog, schema,
        already_repaired=repaired | quarantined,
    )

    if nc_candidates:
        print(
            f"\n-- NEITHER_CORRECT GT REPAIR " + "-" * 24 + "\n"
            f"  Candidates: {len(nc_candidates)}"
        )

        for cand in nc_candidates:
            qid = cand.get("question_id", "?")
            if _should_quarantine(cand):
                print(
                    f"    - [{qid}] QUARANTINE: \"{cand['question'][:60]}\" "
                    f"({cand['consecutive_nc']} consecutive neither_correct)"
                )
                try:
                    quarantine_benchmark_question(
                        spark, uc_schema, domain, cand["question"],
                        reason=(
                            f"Quarantined after {cand['consecutive_nc']} consecutive "
                            f"neither_correct verdicts across {cand['nc_count']} evaluations"
                        ),
                    )
                    quarantined.add(qid)
                    nc_quarantined += 1
                except Exception:
                    logger.warning("Failed to quarantine %s", qid, exc_info=True)
            else:
                print(
                    f"    - [{qid}] REPAIR ATTEMPT: \"{cand['question'][:60]}\" "
                    f"(neither_correct in {cand['nc_count']} evals)"
                )
                repaired_sql = _attempt_gt_repair(
                    w, cand, spark,
                    warehouse_id=resolve_warehouse_id(""),
                )
                if repaired_sql:
                    repair_actions = [{
                        "question": cand["question"],
                        "question_id": qid,
                        "new_expected_sql": repaired_sql,
                        "verdict": "arbiter_repair",
                    }]
                    repair_result = apply_benchmark_corrections(
                        repair_actions, spark, uc_schema, domain,
                        data_profile=data_profile,
                    )
                    if repair_result["applied"] > 0:
                        print(f"      -> Repair succeeded: {repaired_sql[:80]}")
                        repaired.add(qid)
                        nc_repaired += 1
                    else:
                        print(f"      -> Repair SQL rejected: {repair_result['errors'][:2]}")
                else:
                    print(f"      -> Repair failed (LLM returned no valid SQL)")

        print("-" * 52)

    return {
        "gc_applied": gc_applied,
        "gc_skipped": gc_skipped,
        "nc_repaired": nc_repaired,
        "nc_quarantined": nc_quarantined,
        "corrected_qids": corrected,
        "quarantined_qids": quarantined,
    }


def _analyze_and_distribute(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    metadata_snapshot: dict,
    iteration_counter: int,
    lever_label: int,
    *,
    verbose: bool = True,
    quarantined_qids: set[str] | None = None,
    exclude_qids: set[str] | None = None,
) -> dict:
    """Analyze failures once, cluster, and distribute clusters to levers.

    Returns a dict with:
      - ``lever_assignments``: ``{lever_int: [clusters]}``
      - ``all_clusters``: all hard-failure clusters (flat list)
      - ``soft_signal_clusters``: soft-signal clusters
      - ``summary``: printable summary lines
      - ``asi_rows``: ASI rows for Delta
      - ``prov_rows``: provenance rows for Delta
    """
    from genie_space_optimizer.optimization.evaluation import row_is_hard_failure
    from genie_space_optimizer.optimization.ground_truth_corrections import (
        build_gt_correction_candidate,
        is_gt_correction_candidate,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _map_to_lever,
        cluster_failures,
    )

    failure_rows = _get_failure_rows(spark, run_id, catalog, schema)
    _quarantined = quarantined_qids or set()
    _exclude = exclude_qids or set()

    # Tier 1.4: ``row_is_hard_failure`` is the single predicate shared by
    # accuracy and clustering. A row is a hard failure only when rc=no AND
    # arbiter didn't override. Previously we used arbiter alone, so any row
    # where Genie's SQL was semantically wrong (arbiter=ground_truth_correct)
    # but happened to return a matching result set (rc=yes) went into the
    # hard cluster — producing phantom clusters even when the accept gate
    # saw 100% accuracy.
    arbiter_counts: dict[str, int] = {}
    arbiter_excluded: list[str] = []
    quarantine_excluded: list[str] = []
    soft_signal_qids: list[str] = []
    filtered_failure_rows: list[dict] = []
    soft_signal_rows: list[dict] = []
    gt_correction_candidates: list[dict] = []
    for row in failure_rows:
        av = _get_arbiter_verdict(row)
        qid = _get_question_id(row)
        arbiter_counts[av] = arbiter_counts.get(av, 0) + 1

        # B3.2 — match base qid AND ``:vN`` suffix variants so a
        # quarantined ``_002`` excludes ``_002:v2`` and ``_002:v3``
        # rows at the source. Without this, the soft-signal cluster
        # below picks them up and the strategist re-targets them.
        if _is_quarantined_qid(qid, _quarantined):
            quarantine_excluded.append(qid)
            continue

        if qid in _exclude:
            continue

        # Task 1: divert ``arbiter=genie_correct`` rows to the corpus-
        # review queue BEFORE either clustering branch. These are
        # corpus-quality signals (the GT itself is wrong or under-
        # specified), not Genie failures, and must not drive patch
        # generation. The queue persists them for reviewer triage; the
        # state machine then governs whether the qid re-enters the loop
        # (rejected_keep_gt) or feeds Task 9's proactive mining
        # (accepted_corpus_fix).
        if is_gt_correction_candidate(row):
            # Task 10: build_gt_correction_candidate now raises
            # ``ValueError`` when the row carries no extractable
            # question_id (the previous silent-empty behavior produced
            # un-reviewable rows). Skip-and-warn rather than abort the
            # whole analysis pass — a single malformed row should not
            # tear down the corpus-review queue for the rest.
            try:
                gt_correction_candidates.append(
                    build_gt_correction_candidate(
                        row, run_id=run_id, iteration=iteration_counter
                    )
                )
            except ValueError as exc:
                logger.warning(
                    "Skipping unidentifiable GT correction candidate: %s", exc,
                )
            continue

        if row_is_hard_failure(row):
            # Task 6: stamp typed SQL feature diff on confirmed-failure
            # rows so downstream stages can route off ``_feature_diff``
            # instead of pattern-detector heuristics. ``compute_diff``
            # is pure and ``mine_sql_features`` returns empty features
            # on parse errors, so this stamping is safe to fail open.
            try:
                from genie_space_optimizer.optimization.feature_mining import (
                    compute_diff as _t6_compute_diff,
                    mine_sql_features as _t6_mine_features,
                )
                _genie_sql = (
                    row.get("outputs.predictions.sql")
                    or row.get("generated_sql")
                    or row.get("genie_sql")
                    or ""
                )
                _expected_sql = (
                    row.get("inputs.expected_sql")
                    or row.get("expected_sql")
                    or ""
                )
                if _genie_sql and _expected_sql:
                    row["_feature_diff"] = _t6_compute_diff(
                        genie=_t6_mine_features(_genie_sql),
                        ground_truth=_t6_mine_features(_expected_sql),
                    )
                    # Task 6: thread the SQL pair so afs._structural_diff
                    # can finally invoke the existing compute_ast_diff
                    # path (was dead code without a populator).
                    row["_sql_pairs_for_ast_diff"] = (
                        _genie_sql, _expected_sql,
                    )
            except Exception:
                logger.debug(
                    "Task 6 feature mining failed (non-fatal) for qid=%s",
                    qid, exc_info=True,
                )
            filtered_failure_rows.append(row)
        elif _has_individual_judge_failure(row):
            soft_signal_rows.append(row)
            soft_signal_qids.append(qid)
        else:
            arbiter_excluded.append(qid)

    # ── Print failure analysis summary ─────────────────────────────
    # Tier 3.5: labels count ROWS not questions (each benchmark trial is a
    # separate row). Show row counts plus unique-question counts so
    # operators don't misread "5 question(s)" when it's really 5 rows from
    # 3 distinct questions (some run twice).
    _arbiter_summary = "  ".join(f"{k}={v}" for k, v in sorted(arbiter_counts.items()))
    _arbiter_unique_qids = len({
        _get_question_id(row) for row in failure_rows if _get_question_id(row)
    })
    _fa_lines = [
        _section("Failure Analysis", "-"),
        _kv(
            "Total rows loaded",
            f"{len(failure_rows)} row(s) across {_arbiter_unique_qids} unique question(s)",
        ),
        _kv("Arbiter verdicts (row counts)", _arbiter_summary),
    ]
    if quarantine_excluded:
        _qu_unique = len(set(quarantine_excluded))
        _fa_lines.append(_kv(
            "Quarantined (excluded)",
            f"{len(quarantine_excluded)} row(s) across {_qu_unique} unique "
            f"question(s): {', '.join(list(dict.fromkeys(quarantine_excluded))[:5])}",
        ))
    if gt_correction_candidates:
        _gtc_qids = [c.get("question_id", "") for c in gt_correction_candidates]
        _gtc_unique = len({q for q in _gtc_qids if q})
        _fa_lines.append(_kv(
            "GT correction candidates (genie_correct → corpus review)",
            f"{len(gt_correction_candidates)} row(s) across {_gtc_unique} unique "
            f"question(s): {', '.join([q for q in _gtc_qids if q][:5])}",
        ))
    if arbiter_excluded:
        _ae_unique = len(set(arbiter_excluded))
        _fa_lines.append(_kv(
            "Excluded (fully correct)",
            f"{len(arbiter_excluded)} row(s) across {_ae_unique} unique question(s)",
        ))
    if soft_signal_rows:
        _ss_unique = len(set(soft_signal_qids))
        _fa_lines.append(_kv(
            "Soft signals (correct but judges failed)",
            f"{len(soft_signal_rows)} row(s) across {_ss_unique} unique question(s)",
        ))
        # Tier 3.4: show "+N more" suffix when truncated so operators know
        # the list is incomplete.
        # T3.15: when soft_signal_qids contains duplicates (two rows with
        # the same base qid), the display was printing ``_004, _004`` —
        # visually identical tokens that readers can't tell apart.
        # Annotate duplicates inline: the first occurrence carries a
        # ``(base)`` tag, subsequent occurrences get ``:v2``, ``:v3`` so
        # the display mirrors the suffix the dedup stage will apply in
        # cluster_failures.
        def _annotate_dups(qids: list[str]) -> list[str]:
            seen: dict[str, int] = {}
            _has_dups = len(set(qids)) != len(qids)
            out: list[str] = []
            for q in qids:
                n = seen.get(q, 0) + 1
                seen[q] = n
                if n == 1:
                    out.append(f"{q} (base)" if _has_dups and qids.count(q) > 1 else q)
                else:
                    out.append(f"{q}:v{n}")
            return out

        _preview_qids = _annotate_dups(soft_signal_qids[:10])
        _suffix = (
            "" if len(soft_signal_qids) <= 10
            else f" (+{len(soft_signal_qids) - 10} more)"
        )
        _fa_lines.append(_kv(
            "  Soft signal question IDs",
            ", ".join(_preview_qids) + _suffix,
        ))
        for _ss_row, (_ss_qid, _ss_label) in zip(
            soft_signal_rows[:10],
            zip(soft_signal_qids[:10], _preview_qids),
        ):
            _failed_judges = _get_failed_judges(_ss_row)
            _fa_lines.append(f"  |    {_ss_label}: failed judges = {', '.join(_failed_judges) if _failed_judges else '(none detected)'}")
    _hf_unique = len({
        _get_question_id(row) for row in filtered_failure_rows if _get_question_id(row)
    })
    _fa_lines.append(_kv(
        "Hard failure rows for clustering",
        f"{len(filtered_failure_rows)} row(s) across {_hf_unique} unique question(s)",
    ))
    _fa_lines.append(_bar("-"))
    print("\n".join(_fa_lines))

    # Tier 2.11: share qid dedup state across hard+soft clustering so a
    # physical row that appears in both pathways gets a single stable qid
    # (possibly with :vN suffix) and therefore a single dominant root
    # cause. Without this, Q001 would be ``missing_aggregation`` in the
    # hard cluster and ``wrong_filter_condition`` in the soft cluster —
    # the DO-NOT-RETRY bookkeeping cannot reconcile those.
    _shared_qid_state: dict = {}

    # ── Cluster hard + soft failures via F3 stage ──────────────────
    # Phase F+H Commit A1: F3 clustering — true replacement of the
    # optimizer.cluster_failures hard+soft pair. ``stages.clustering.
    # form()`` calls ``cluster_failures`` internally for both branches.
    #
    # Production-mode caveat: ``form()`` passes ``spark=None``; replay-
    # fixture mode is unaffected because spark is None everywhere there,
    # but real-Genie runs SKIP the spark-conditional ``read_asi_from_uc``
    # UC enrichment at ``optimizer.py:1913-1915``. Verify in the next
    # production pilot.
    #
    # Demoted-cluster invariant: ``cluster_failures`` does not stamp
    # ``demoted_reason`` on its returns (verified at audit time by
    # ``grep "demoted_reason" optimizer.py`` returning zero hits inside
    # the function body), so ``form()``'s ``_split_by_demoted`` always
    # yields ``rejected_cluster_alternatives=()``. If a future change
    # makes ``cluster_failures`` inline demoted entries, this adapter
    # must combine ``_cluster_findings.clusters +
    # filter(_cluster_findings.rejected_cluster_alternatives, signal_type)``
    # to preserve byte-stability.
    #
    # T1.9: ``namespace="H"`` / ``"S"`` is set internally by ``form()``;
    # hard clusters mint H001..., soft clusters mint S001....
    #
    # Verified against: stages/clustering.py:32-46 (Input), 86-127 (form).
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture as _wrap_with_io_capture,
    )
    from genie_space_optimizer.optimization.stages import StageContext as _StageCtx
    from genie_space_optimizer.optimization.stages import clustering as _clust_stage

    eval_result_for_clustering = {"rows": filtered_failure_rows}
    _stage_ctx_clustering = _StageCtx(
        run_id=str(run_id),
        iteration=int(iteration_counter),
        space_id="",      # form() does not read; see clustering.py:96-103
        domain="",        # form() does not read
        catalog=str(catalog),
        schema=str(schema),
        apply_mode="",    # form() does not read
        journey_emit=lambda *a, **k: None,  # form() does not emit
        decision_emit=lambda r: None,        # form() does not emit
        mlflow_anchor_run_id=None,
        feature_flags={},
    )
    _clust_inp = _clust_stage.ClusteringInput(
        eval_result_for_clustering=eval_result_for_clustering,
        metadata_snapshot=metadata_snapshot,
        soft_eval_result={"rows": soft_signal_rows} if soft_signal_rows else None,
        qid_state=_shared_qid_state,
    )
    # Phase F+H Commit B10: wrap F3 with stage_io_capture decorator.
    # Replay-byte-stable because the wrapper returns out unchanged and
    # the MLflow log_text calls are no-ops while mlflow_anchor_run_id
    # is None (Phase C Commit 17 wires the anchor).
    _clust_wrapped = _wrap_with_io_capture(
        execute=_clust_stage.execute,
        stage_key="cluster_formation",
    )
    _cluster_findings = _clust_wrapped(_stage_ctx_clustering, _clust_inp)
    clusters = list(_cluster_findings.clusters)
    soft_clusters: list[dict] = list(_cluster_findings.soft_clusters)

    if soft_signal_rows:
        # Preserve the harness's defensive signal_type=soft setdefault
        # that guarded against cluster_failures variants returning soft
        # clusters without the field stamped. ``cluster_failures`` sets
        # it natively per ``optimizer.py:9277, 9292``; this loop is
        # defense-in-depth.
        for sc in soft_clusters:
            sc.setdefault("signal_type", "soft")
        _soft_qids_total = sum(len(sc.get("question_ids", [])) for sc in soft_clusters)
        _soft_lines = [
            _section("Soft Signal Clusters (correct-but-suboptimal)", "-"),
            _kv("Soft signal rows", len(soft_signal_rows)),
            _kv("Soft clusters formed", len(soft_clusters)),
            _kv("Soft cluster questions", _soft_qids_total),
            "|",
        ]
        # Phase E1: annotate each soft cluster with its aggregate signal
        # class so operators can see at a glance whether a cluster is
        # driven by NL-text judges (response_quality) or SQL-shape judges.
        from genie_space_optimizer.optimization.judge_classes import (
            aggregate_cluster_signal_class,
        )

        for si, sc in enumerate(soft_clusters, 1):
            sc_judge = sc.get("affected_judge", "?")
            sc_cause = sc.get("root_cause", "?")
            sc_asi = sc.get("asi_failure_type", "n/a")
            sc_qids = sc.get("question_ids", [])
            sc_blame = sc.get("asi_blame_set", sc.get("blame_set", []))
            blame_str = ", ".join(sc_blame) if isinstance(sc_blame, list) and sc_blame else str(sc_blame) if sc_blame else "(none)"
            sc_signal = aggregate_cluster_signal_class(sc.get("affected_judges", []))
            _soft_lines.append(f"|  Soft cluster {si} / {len(soft_clusters)}")
            _soft_lines.append(f"|    {'Judge:':<24s} {sc_judge}")
            _soft_lines.append(f"|    {'Signal class:':<24s} {sc_signal}")
            _soft_lines.append(f"|    {'Root cause:':<24s} {sc_cause}")
            _soft_lines.append(f"|    {'ASI failure type:':<24s} {sc_asi}")
            _soft_lines.append(f"|    {'Blame:':<24s} {blame_str}")
            _soft_lines.append(f"|    Questions ({len(sc_qids)}):")
            for qid in sc_qids:
                _soft_lines.append(f"|      {qid}")
            _soft_lines.append("|")
        _soft_lines.append(_bar("-"))
        print("\n".join(_soft_lines))

    # ── Map clusters to levers ─────────────────────────────────────
    lever_assignments: dict[int, list[dict]] = {}
    cluster_lines = [_section(f"Failure Clusters ({len(clusters)} total)", "-"), "|"]
    _root_cause_counter: Counter[str] = Counter()
    _lever_counter: Counter[int] = Counter()
    _all_cluster_qids: set[str] = set()
    _clusters_with_asi = 0
    _clusters_with_blame = 0
    from genie_space_optimizer.optimization.judge_classes import (
        aggregate_cluster_signal_class,
    )
    for ci, c in enumerate(clusters, 1):
        mapped = _map_to_lever(
            c["root_cause"],
            asi_failure_type=c.get("asi_failure_type"),
            blame_set=c.get("asi_blame_set"),
            judge=c.get("affected_judge"),
        )
        c["_mapped_lever"] = mapped
        lever_assignments.setdefault(mapped, []).append(c)
        blame = c.get("asi_blame_set", c.get("blame_set", []))
        qids = c["question_ids"]
        asi_ft = c.get("asi_failure_type", "n/a")
        c_signal = aggregate_cluster_signal_class(c.get("affected_judges", []))
        cluster_lines.append(f"|  Cluster {ci} / {len(clusters)}")
        cluster_lines.append(f"|    {'Judge:':<24s} {c['affected_judge']}")
        cluster_lines.append(f"|    {'Signal class:':<24s} {c_signal}")
        cluster_lines.append(f"|    {'Root cause:':<24s} {c['root_cause']}")
        cluster_lines.append(f"|    {'ASI failure type:':<24s} {asi_ft}")
        cluster_lines.append(f"|    {'Mapped lever:':<24s} {mapped}")
        blame_str = ", ".join(blame) if isinstance(blame, list) and blame else str(blame) if blame else "(none)"
        cluster_lines.append(f"|    {'Blame:':<24s} {blame_str}")
        cluster_lines.append(f"|    Questions ({len(qids)}):")
        for qid in qids:
            cluster_lines.append(f"|      {qid}")
        cluster_lines.append("|")
        _root_cause_counter[c["root_cause"]] += 1
        _lever_counter[mapped] += 1
        _all_cluster_qids.update(qids)
        if asi_ft and asi_ft != "n/a":
            _clusters_with_asi += 1
        if blame and blame != []:
            _clusters_with_blame += 1

    _lever_summary = ", ".join(f"lever {k} = {v}" for k, v in sorted(_lever_counter.items()))
    _top_causes = ", ".join(f"{k} ({v})" for k, v in _root_cause_counter.most_common(5))
    cluster_lines.append("|  --- Summary ---")
    cluster_lines.append(f"|    {'Clusters by lever:':<24s} {_lever_summary}")
    cluster_lines.append(f"|    {'Unique questions:':<24s} {len(_all_cluster_qids)}")
    cluster_lines.append(f"|    {'Top root causes:':<24s} {_top_causes}")
    cluster_lines.append(f"|    {'Clusters with ASI:':<24s} {_clusters_with_asi} of {len(clusters)}")
    cluster_lines.append(f"|    {'Clusters with blame:':<24s} {_clusters_with_blame} of {len(clusters)}")
    cluster_lines.append(_bar("-"))
    print("\n".join(cluster_lines))

    # ── ASI / provenance rows for Delta ────────────────────────────
    all_clusters_for_asi = clusters + soft_clusters
    _asi_rows: list[dict] = []
    _prov_rows: list[dict] = []
    for c in all_clusters_for_asi:
        sig_type = c.get("signal_type", "hard")
        for qt in c.get("question_traces", []):
            qid = qt.get("question_id", "")
            for jt in qt.get("failed_judges", []):
                _asi_rows.append({
                    "question_id": qid,
                    "judge": jt.get("judge", ""),
                    "value": "no",
                    "failure_type": jt.get("asi_failure_type_raw"),
                    "blame_set": jt.get("blame_set"),
                    "counterfactual_fix": jt.get("counterfactual_fix"),
                    "wrong_clause": jt.get("wrong_clause"),
                })
                _prov_rows.append({
                    "question_id": qid,
                    "signal_type": sig_type,
                    "judge": jt.get("judge", ""),
                    "judge_verdict": jt.get("verdict", "FAIL"),
                    "asi_failure_type_raw": jt.get("asi_failure_type_raw"),
                    "resolved_root_cause": jt.get("resolved_root_cause", "other"),
                    "resolution_method": jt.get("resolution_method", "unknown"),
                    "blame_set": jt.get("blame_set"),
                    "counterfactual_fix": jt.get("counterfactual_fix"),
                    "wrong_clause": jt.get("wrong_clause"),
                    "rationale_snippet": jt.get("rationale_snippet"),
                    "cluster_id": c.get("cluster_id", ""),
                    "mapped_lever": c.get("_mapped_lever"),
                })

    # ── Pipeline lineage summary ───────────────────────────────────
    _lineage_lines = ["\n== PIPELINE LINEAGE ==========================================================", "|"]
    for c in clusters:
        mapped = c.get("_mapped_lever", _map_to_lever(
            c["root_cause"],
            asi_failure_type=c.get("asi_failure_type"),
            blame_set=c.get("asi_blame_set"),
            judge=c.get("affected_judge"),
        ))
        for qt in c.get("question_traces", []):
            qid = qt.get("question_id", "")
            judges_info = ", ".join(
                f"{jt['judge']} ({jt.get('resolved_root_cause', '?')})"
                for jt in qt.get("failed_judges", [])
            )
            blame = c.get("asi_blame_set") or "(none)"
            cfix_list = c.get("asi_counterfactual_fixes", [])
            cfix = str(cfix_list[0])[:120] if cfix_list else "(none)"
            _lineage_lines.append(f"|  Q: {qid}")
            _lineage_lines.append(f"|    Failed judges:         {judges_info}")
            _lineage_lines.append(f"|    Dominant root cause:   {c['root_cause']}")
            _lineage_lines.append(f"|    Blame:                 {blame}")
            _lineage_lines.append(f"|    Counterfactual:        \"{cfix}\"")
            _lineage_lines.append(f"|    -> Cluster {c['cluster_id']} -> Lever {mapped} ({LEVER_NAMES.get(mapped, '?')})")
            _lineage_lines.append("|")
    _lineage_lines.append("=" * 78)
    print("\n".join(_lineage_lines))

    # Task 6: aggregate the per-row ``_feature_diff`` stamps into a
    # DiffKind histogram so the lever loop can carry typed-evidence
    # telemetry into the Task 3 audit (stage_letter "D",
    # gate_name "feature_mining"). Pure aggregation; safe to fail open.
    _feature_diff_histogram: dict[str, int] = {}
    _feature_diff_count = 0
    try:
        for _row in filtered_failure_rows:
            _fd = _row.get("_feature_diff")
            _kind = getattr(getattr(_fd, "primary_kind", None), "value", None)
            if _kind:
                _feature_diff_histogram[_kind] = (
                    _feature_diff_histogram.get(_kind, 0) + 1
                )
                _feature_diff_count += 1
    except Exception:
        logger.debug(
            "Task 6 feature_diff histogram aggregation failed (non-fatal)",
            exc_info=True,
        )

    _rca_ledger: dict = {
        "findings": [],
        "themes": [],
        "conflicts": [],
        "finding_count": 0,
        "theme_count": 0,
        "conflict_count": 0,
    }
    if ENABLE_RCA_LEDGER:
        try:
            from genie_space_optimizer.optimization.rca import (
                build_rca_ledger,
                rca_findings_from_clusters,
            )

            _cluster_rca_findings = rca_findings_from_clusters(clusters)
            _rca_ledger = build_rca_ledger(
                filtered_failure_rows,
                metadata_snapshot=metadata_snapshot,
                extra_findings=[
                    *(_cluster_rca_findings or []),
                    *(metadata_snapshot.get("_regression_rca_findings") or []),
                ],
            )
        except Exception:
            logger.debug("RCA ledger construction failed (non-fatal)", exc_info=True)

    try:
        from genie_space_optimizer.optimization.rca_failure_context import (
            failure_contexts_by_qid,
        )

        metadata_snapshot["_rca_failure_contexts_by_qid"] = failure_contexts_by_qid(
            filtered_failure_rows,
        )
    except Exception:
        logger.debug(
            "RCA failure context extraction failed; continuing without prompt evidence",
            exc_info=True,
        )
        metadata_snapshot["_rca_failure_contexts_by_qid"] = {}

    return {
        "lever_assignments": lever_assignments,
        "all_clusters": clusters,
        "soft_signal_clusters": soft_clusters,
        "asi_rows": _asi_rows,
        "prov_rows": _prov_rows,
        "lever_counter": dict(_lever_counter),
        # Task 1: corpus-review queue payloads (Delta-shaped). The caller
        # is responsible for persisting via state.write_gt_correction_candidates.
        "gt_correction_candidates": gt_correction_candidates,
        # Task 6: typed-evidence telemetry. Carries a DiffKind ->
        # count map plus the total count of confirmed-failure rows
        # that received a feature diff stamp.
        "feature_diff_histogram": _feature_diff_histogram,
        "feature_diff_count": _feature_diff_count,
        "rca_ledger": _rca_ledger,
        # Track H — expose the same row source the soft pile was built
        # from so the caller can ground ``assert_soft_cluster_currency``
        # against the *exact* rows the clusterer saw, not a re-read of
        # Delta. Re-reads risk source skew if anything writes between
        # ``_analyze_and_distribute`` and the assertion.
        "failure_rows": failure_rows,
    }


# Optimizer Control-Plane Hardening Plan — Task B helper.
def _filter_to_causal_applyable_proposals(
    *,
    ag: dict,
    proposals: list[dict],
) -> tuple[list[dict], bool]:
    """Return ``(matching_proposals, had_any_rca_matched)``.

    When the parent AG declares an ``rca_id``, retain only proposals
    whose ``rca_id`` equals the AG's. When the AG carries no ``rca_id``
    (diagnostic AGs that did not yet inherit cluster RCA — see Task F),
    retain all proposals to preserve legacy behaviour. Callers gate
    the halt-on-empty behaviour behind
    ``no_causal_applyable_halt_enabled()``.
    """
    ag_rca = str(ag.get("rca_id") or "").strip()
    if not ag_rca:
        return list(proposals), False
    matched = [
        p for p in proposals
        if str(p.get("rca_id") or "").strip() == ag_rca
    ]
    return matched, bool(matched)


def _run_gate_checks(
    *,
    spark: Any,
    w: WorkspaceClient,
    run_id: str,
    space_id: str,
    exp_name: str,
    domain: str,
    iteration_counter: int,
    ag_id: str,
    benchmarks: list[dict],
    proposals: list[dict],
    patches: list[dict],
    apply_log: dict,
    clusters: list[dict],
    metadata_snapshot: dict,
    predict_fn: Any,
    scorers: list,
    prev_model_id: str,
    best_scores: dict[str, float],
    best_accuracy: float,
    catalog: str,
    schema: str,
    reference_sqls: dict[str, str],
    noise_floor: float,
    affected_question_ids: set[str] | None = None,
    lever_keys: list[str] | None = None,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
    prev_failure_qids: set[str] | None = None,
    prev_iter_pre_accept_baseline: float | None = None,
    accepted_baseline_rows_for_control_plane: list[dict] | None = None,
) -> dict:
    """Run slice → P0 → full eval gate sequence for an action group.

    ``prev_iter_pre_accept_baseline`` is the carried post-arbiter
    baseline at the start of the *previous* iteration (before that
    iteration's gate ran). Used by the post-hoc baseline-drift
    diagnostic — when the current iteration's post-arbiter has fallen
    below this snapshot by ``BASELINE_DRIFT_DIAGNOSTIC_PP`` or more,
    the gate logs a ``suspected_stale_baseline`` decision-audit row.
    Diagnostic only — does not auto-roll back. ``None`` on iteration 0.

    Returns a dict with:
      - ``passed``: bool
      - ``full_scores``: dict (only if full eval ran)
      - ``full_accuracy``: float (only if full eval ran)
      - ``new_model_id``: str (only if full eval ran)
      - ``full_result``: dict (only if full eval ran)
      - ``rollback_reason``: str (only if failed)
    """
    import mlflow

    uc_schema = f"{catalog}.{schema}"
    _primary_lever = int(lever_keys[0]) if lever_keys else 0

    # Task 3: collect decision audit rows as the gates run, persist them
    # in one shot before every return so the audit trail survives even
    # an early rollback. ``_audit_emit`` accepts plain Python lists/dicts;
    # the state writer JSON-serializes.
    _decision_rows: list[dict] = []
    _decision_order = [0]

    def _audit_emit(
        *,
        gate_name: str,
        decision: str,
        stage_letter: str | None = None,
        reason_code: str | None = None,
        reason_detail: str | None = None,
        affected_qids: list[str] | None = None,
        source_cluster_ids: list[str] | None = None,
        proposal_ids: list[str] | None = None,
        proposal_to_patch_map: dict[str, str] | None = None,
        metrics: dict | None = None,
    ) -> None:
        _decision_order[0] += 1
        _decision_rows.append({
            "run_id": run_id,
            "iteration": iteration_counter,
            "ag_id": ag_id,
            "decision_order": _decision_order[0],
            "stage_letter": stage_letter,
            "gate_name": gate_name,
            "decision": decision,
            "reason_code": reason_code,
            "reason_detail": reason_detail,
            "affected_qids": affected_qids,
            "source_cluster_ids": source_cluster_ids,
            "proposal_ids": proposal_ids,
            "proposal_to_patch_map": proposal_to_patch_map,
            "metrics": metrics,
        })

    def _audit_persist() -> None:
        if not _decision_rows:
            return
        try:
            from genie_space_optimizer.optimization.state import (
                write_lever_loop_decisions,
            )
            write_lever_loop_decisions(
                spark, list(_decision_rows), catalog=catalog, schema=schema,
            )
        except Exception:
            logger.debug(
                "Failed to persist lever-loop decision rows", exc_info=True,
            )

    has_dict_changes = any(
        (entry.get("patch", {}) or {}).get("enable_entity_matching")
        or (entry.get("action", {}) or {}).get("type") in (
            "enable_value_dictionary", "enable_entity_matching",
        )
        for entry in apply_log.get("applied", [])
    )
    wait_time = (
        PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS if has_dict_changes
        else PROPAGATION_WAIT_SECONDS
    )
    _wait_note = " (extended for value dictionary rebuild)" if has_dict_changes else ""
    patched_objects = apply_log.get("patched_objects", [])

    # Tier 3.11: bounded polling against fetch_space_config instead of a
    # fixed sleep. Terminates on the first fetch where the applied
    # instruction text / section is visible, falling back to the full
    # wait only when the fetch can't confirm propagation. Median wall-
    # clock drops; slow-API runs still get the full budget.
    _poll_interval = 2.0
    _elapsed = 0.0
    _propagated = False
    _applied_entries = apply_log.get("applied") or []
    _instruction_snippets = []
    for _entry in _applied_entries:
        _patch = _entry.get("patch", {}) if isinstance(_entry, dict) else {}
        for _field in ("new_text", "proposed_value", "text_instructions"):
            _v = _patch.get(_field)
            if isinstance(_v, str) and _v.strip():
                _instruction_snippets.append(_v.strip()[:80])
                break
    _expected_snippets = {s for s in _instruction_snippets if s}

    print(
        _section("Propagation Wait", "-") + "\n"
        + _kv("AG", f"{ag_id}: {len(_applied_entries)} patches applied") + "\n"
        + _kv("Patched objects", ", ".join(str(o) for o in patched_objects) if patched_objects else "(none)") + "\n"
        + _kv("Max wait", f"{wait_time}s{_wait_note}") + "\n"
        + _kv("Mode", "polling fetch_space_config (Tier 3.11)") + "\n"
        + _bar("-")
    )

    from genie_space_optimizer.common.genie_client import fetch_space_config as _fetch_cfg

    while _elapsed < float(wait_time):
        time.sleep(_poll_interval)
        _elapsed += _poll_interval
        if not _expected_snippets:
            # No verifiable instruction-text snippet; fall back to the
            # full budget (this is the case for non-instruction-only
            # patches like snippet / join_spec changes).
            continue
        try:
            _cfg_probe = _fetch_cfg(w, space_id)
        except Exception:
            continue
        _parsed_probe = _cfg_probe.get("_parsed_space", _cfg_probe) if isinstance(_cfg_probe, dict) else {}
        _instr_probe = _parsed_probe.get("instructions", {}) if isinstance(_parsed_probe, dict) else {}
        _txt_probe = _instr_probe.get("text_instructions", "") if isinstance(_instr_probe, dict) else ""
        if not isinstance(_txt_probe, str) or not _txt_probe:
            continue
        if any(_snip in _txt_probe for _snip in _expected_snippets):
            _propagated = True
            break

    if _expected_snippets and _propagated:
        logger.info(
            "Propagation confirmed after %.1fs (< max %ds) for AG %s",
            _elapsed, wait_time, ag_id,
        )
        _audit_emit(
            stage_letter="K",
            gate_name="propagation_wait",
            decision="confirmed",
            metrics={
                "elapsed_seconds": round(_elapsed, 1),
                "max_wait_seconds": int(wait_time),
                "patches_applied": len(_applied_entries),
            },
        )
    else:
        remaining = max(0.0, float(wait_time) - _elapsed)
        if remaining > 0:
            time.sleep(remaining)
            logger.info(
                "Propagation not confirmed for AG %s — waited full %ds budget",
                ag_id, wait_time,
            )
        _audit_emit(
            stage_letter="K",
            gate_name="propagation_wait",
            decision="waited_full_budget",
            reason_code=(
                "no_verifiable_snippet" if not _expected_snippets
                else "snippet_not_observed"
            ),
            metrics={
                "elapsed_seconds": round(_elapsed, 1),
                "max_wait_seconds": int(wait_time),
                "patches_applied": len(_applied_entries),
            },
        )

    # ── Slice gate ────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass

    _run_slice = False
    # Task 2: slice gate is a legacy approval gate. By default, the new
    # strict full-eval acceptance policy supersedes it. Operators who
    # want the old behaviour set ``GSO_ENABLE_LEGACY_SLICE_P0_GATES=true``.
    from genie_space_optimizer.common.config import ENABLE_LEGACY_SLICE_P0_GATES
    if not ENABLE_LEGACY_SLICE_P0_GATES:
        print(
            _section(f"SLICE GATE [{ag_id}]: SKIPPED (Task 2)", "-") + "\n"
            + _kv(
                "Reason",
                "ENABLE_LEGACY_SLICE_P0_GATES=False — strict full-eval "
                "acceptance is the only gate; opt back in via "
                "GSO_ENABLE_LEGACY_SLICE_P0_GATES=true",
            ) + "\n"
            + _bar("-")
        )
    elif ENABLE_SLICE_GATE:
        affected_qids: set[str] = affected_question_ids or set()
        # Phase 5.1: stratified slice. Compute the set of
        # baseline-passing qids (benchmarks minus prior-iteration
        # failures) so the slice can include a regression-detector
        # subset alongside the targeted rows. When ``prev_failure_qids``
        # is unavailable (iteration 0 + no resume), fall back to the
        # legacy targeted-only slice.
        _all_qids = {b.get("id") for b in benchmarks if b.get("id")}
        _baseline_passing = (
            _all_qids - set(prev_failure_qids or set())
            if prev_failure_qids is not None else None
        )
        slice_benchmarks = filter_benchmarks_by_scope(
            benchmarks, "slice", patched_objects,
            affected_question_ids=affected_qids,
            baseline_passing_qids=_baseline_passing,
        )
        _total = len(benchmarks)
        _sliced = len(slice_benchmarks) if slice_benchmarks else 0
        # Tier 3.12: small-corpus bypass. For suites with ≤ 30 benchmarks,
        # SLICE_GATE_MIN_REDUCTION=0.5 usually rejects the slice as "too
        # broad" because affected_qids covers most of the set. Relax the
        # threshold in that regime so the slice gate still catches the
        # early-warning regressions (e.g. the Q4 infra flake) before
        # the full eval spends 15+ minutes.
        _small_corpus = _total <= 30
        _broadness_ratio = _sliced / _total if _total else 1.0
        _slice_threshold = 0.9 if _small_corpus else (1.0 - SLICE_GATE_MIN_REDUCTION)
        if slice_benchmarks and _broadness_ratio <= _slice_threshold:
            _run_slice = True
        else:
            print(
                _section(f"SLICE GATE [{ag_id}]: SKIPPED", "-") + "\n"
                + _kv(
                    "Reason",
                    f"slice too broad ({_sliced}/{_total} benchmarks, "
                    f"ratio {_broadness_ratio:.2f} > {_slice_threshold:.2f})",
                ) + "\n"
                + _bar("-")
            )
    else:
        print(
            _section(f"SLICE GATE [{ag_id}]: DISABLED", "-") + "\n"
            + _kv(
                "Reason",
                "ENABLE_SLICE_GATE=False in common/config.py — set True to enable",
            ) + "\n"
            + _bar("-")
        )

    if _run_slice:
        _ensure_sql_context(spark, catalog, schema)
        write_stage(
            spark, run_id, f"AG_{ag_id}_SLICE_EVAL", "STARTED",
            task_key="lever_loop", iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )
        # Tier 4: v2 run name — ``<run_short>/iter_NN_slice_eval``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_slice,
            slice_eval_run_name,
        )
        slice_result = run_evaluation(
            space_id, exp_name, iteration_counter, slice_benchmarks,
            domain, prev_model_id, "slice",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            patched_objects=patched_objects,
            reference_sqls=reference_sqls if reference_sqls else None,
            max_benchmark_count=max_benchmark_count,
            run_name=slice_eval_run_name(run_id, iteration_counter),
            extra_tags=_v2_tags_slice(
                run_id, space_id=space_id, stage="slice_eval",
                iteration=iteration_counter, ag_id=ag_id,
            ),
        )
        slice_scores = slice_result.get("scores", {})
        slice_accuracy = slice_result.get("overall_accuracy", 0.0)
        _slice_qw = 100.0 / max(len(benchmarks), 1)
        # T2.15: use wider small-corpus tolerance when the full-scope
        # corpus is below ``SLICE_GATE_SMALL_CORPUS_ROWS`` so a single-row
        # swing doesn't spuriously fail the gate (e.g. 22-row retail
        # corpus where one flip is ~4.5%). Emit an INFO line with the
        # effective tolerance and corpus size so operators can see which
        # branch ran.
        _full_corpus = len(benchmarks)
        _is_small_corpus = _full_corpus < SLICE_GATE_SMALL_CORPUS_ROWS
        if _is_small_corpus:
            _base_tol = SLICE_GATE_TOLERANCE_SMALL_CORPUS
            _tol_source = "small_corpus"
        else:
            _base_tol = SLICE_GATE_TOLERANCE
            _tol_source = "standard"
        effective_slice_tol = max(_base_tol, noise_floor + 2.0, _slice_qw + 0.5)
        logger.info(
            "SLICE GATE [%s]: tolerance=%.1f%% (source=%s, base=%.1f, "
            "noise_floor+2=%.1f, qw+0.5=%.1f, corpus=%d)",
            ag_id, effective_slice_tol, _tol_source, _base_tol,
            noise_floor + 2.0, _slice_qw + 0.5, _full_corpus,
        )
        _informational_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
        if slice_accuracy >= best_accuracy - 2 * noise_floor:
            _informational_judges.add("asset_routing")
        slice_drops = detect_regressions(
            slice_scores, best_scores, threshold=effective_slice_tol,
            skip_judges=_informational_judges,
        )

        try:
            write_iteration(
                spark, run_id, iteration_counter, slice_result,
                catalog=catalog, schema=schema,
                lever=int(lever_keys[0]) if lever_keys else 0,
                eval_scope="slice", model_id=prev_model_id,
            )
        except Exception:
            logger.debug("Failed to write slice iteration", exc_info=True)

        if slice_drops:
            _score_changes = ", ".join(
                f"{d['judge']} {best_scores.get(d['judge'], 0):.1f}->{slice_scores.get(d['judge'], 0):.1f} ({d['drop']:+.1f})"
                for d in slice_drops
            )
            print(
                _section(
                    f"SLICE GATE [{ag_id}]: FAILED "
                    f"(tolerance={effective_slice_tol:.1f}%, corpus={_full_corpus})",
                    "-",
                ) + "\n"
                + _kv("Regressions", _score_changes) + "\n"
                + _kv("Action", "ROLLBACK") + "\n"
                + _bar("-")
            )
            try:
                update_provenance_gate(
                    spark, run_id, iteration_counter - 1, _primary_lever,
                    "slice", "rollback",
                    {"regressions": [{"judge": d["judge"], "drop": d["drop"]} for d in slice_drops]},
                    catalog, schema,
                )
            except Exception:
                logger.debug("Failed to update provenance gate", exc_info=True)
            try:
                log_gate_feedback_on_traces(
                    slice_result, "slice", "rollback",
                    regressions=slice_drops, lever=_primary_lever, iteration=iteration_counter,
                )
            except Exception:
                logger.debug("Failed to log gate feedback", exc_info=True)
            _audit_emit(
                stage_letter="K",
                gate_name="slice_gate",
                decision="rolled_back",
                reason_detail=f"slice_gate: {slice_drops[0]['judge']}",
                metrics={"regressions": len(slice_drops)},
            )
            _audit_persist()
            return {"passed": False, "rollback_reason": f"slice_gate: {slice_drops[0]['judge']}", "failed_eval_result": slice_result}
        else:
            _sc = ", ".join(
                f"{j} {best_scores.get(j, 0):.1f}->{slice_scores.get(j, 0):.1f}"
                for j in sorted(slice_scores)
            )
            print(
                _section(
                    f"SLICE GATE [{ag_id}]: PASSED "
                    f"(tolerance={effective_slice_tol:.1f}%, corpus={_full_corpus})",
                    "-",
                ) + "\n"
                + _kv("Score changes", _sc) + "\n"
                + _bar("-")
            )

    # ── P0 gate ───────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    # Task 2: P0 gate is a legacy approval gate alongside the slice gate.
    # The new strict full-eval acceptance policy supersedes both. P0
    # only runs when ENABLE_LEGACY_SLICE_P0_GATES=True.
    p0_benchmarks = (
        filter_benchmarks_by_scope(benchmarks, "p0")
        if ENABLE_LEGACY_SLICE_P0_GATES
        else []
    )
    if not ENABLE_LEGACY_SLICE_P0_GATES:
        print(
            _section(f"P0 GATE [{ag_id}]: SKIPPED (Task 2)", "-") + "\n"
            + _kv(
                "Reason",
                "ENABLE_LEGACY_SLICE_P0_GATES=False — full-eval acceptance "
                "is the only gate",
            ) + "\n"
            + _bar("-")
        )
    if p0_benchmarks:
        _ensure_sql_context(spark, catalog, schema)
        # Tier 4: v2 run name — ``<run_short>/iter_NN_p0_eval``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_p0,
            p0_eval_run_name,
        )
        p0_result = run_evaluation(
            space_id, exp_name, iteration_counter, p0_benchmarks,
            domain, prev_model_id, "p0",
            predict_fn, scorers,
            spark=spark, w=w, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            reference_sqls=reference_sqls if reference_sqls else None,
            max_benchmark_count=max_benchmark_count,
            run_name=p0_eval_run_name(run_id, iteration_counter),
            extra_tags=_v2_tags_p0(
                run_id, space_id=space_id, stage="p0_eval",
                iteration=iteration_counter, ag_id=ag_id,
            ),
        )
        try:
            write_iteration(
                spark, run_id, iteration_counter, p0_result,
                catalog=catalog, schema=schema,
                lever=int(lever_keys[0]) if lever_keys else 0,
                eval_scope="p0", model_id=prev_model_id,
            )
        except Exception:
            logger.debug("Failed to write P0 iteration", exc_info=True)

        p0_failures = p0_result.get("failures", [])
        if p0_failures:
            print(
                _section(f"P0 GATE [{ag_id}]: FAIL", "-") + "\n"
                + _kv("P0 questions failing", len(p0_failures)) + "\n"
                + _kv("Action", "ROLLBACK") + "\n"
                + _bar("-")
            )
            _audit_emit(
                stage_letter="K",
                gate_name="p0_gate",
                decision="rolled_back",
                reason_detail=f"p0_gate: {len(p0_failures)} failures",
                metrics={"p0_failures": len(p0_failures)},
            )
            _audit_persist()
            return {"passed": False, "rollback_reason": f"p0_gate: {len(p0_failures)} failures", "failed_eval_result": p0_result}
        else:
            print(
                _section(f"P0 GATE [{ag_id}]: PASS", "-") + "\n"
                + _kv("P0 benchmarks", len(p0_benchmarks)) + "\n"
                + _bar("-")
            )

    # ── Full evaluation ───────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    write_stage(
        spark, run_id, f"AG_{ag_id}_FULL_EVAL", "STARTED",
        task_key="lever_loop", iteration=iteration_counter,
        catalog=catalog, schema=schema,
    )

    _model_kwargs = {
        "w": w, "space_id": space_id, "config": metadata_snapshot,
        "iteration": iteration_counter, "domain": domain,
        "experiment_name": exp_name, "uc_schema": uc_schema,
        "patch_set": patches, "parent_model_id": prev_model_id,
    }

    _ensure_sql_context(spark, catalog, schema)
    # Tier 4: v2 run name — ``<run_short>/iter_NN_full_eval/run_1``.
    from genie_space_optimizer.common.mlflow_names import (
        default_tags as _v2_tags_full,
        full_eval_run_name,
    )
    # Phase F1 — route the per-iteration full eval through stages.evaluation.
    # The journey-emit + EVAL_CLASSIFIED records are owned by the
    # _run_lever_loop block (lines 12097, 12218) which fires off cluster
    # analysis upstream of this call; here we pass no-op emit hooks so
    # the wrapper does NOT double-emit. Subsequent F-plans absorb the
    # surrounding orchestration into stages and let the wrapper own
    # journey/decision emission directly.
    from genie_space_optimizer.optimization.stages import (
        RunEvaluationKwargs,
        StageContext as _StageCtx,
        evaluation as _eval_stage,
    )

    _stage_ctx_full_eval = _StageCtx(
        run_id=run_id,
        iteration=int(iteration_counter),
        space_id=space_id,
        domain=domain,
        catalog=catalog,
        schema=schema,
        apply_mode="real",
        journey_emit=lambda *a, **k: None,
        decision_emit=lambda record: None,
        mlflow_anchor_run_id=None,
        feature_flags={},
    )
    _eval_kwargs_full: RunEvaluationKwargs = {
        "space_id": space_id,
        "experiment_name": exp_name,
        "iteration": iteration_counter,
        "benchmarks": benchmarks,
        "domain": domain,
        "model_id": None,
        "eval_scope": "full",
        "predict_fn": predict_fn,
        "scorers": scorers,
        "spark": spark,
        "w": w,
        "catalog": catalog,
        "gold_schema": schema,
        "uc_schema": uc_schema,
        "reference_sqls": reference_sqls if reference_sqls else None,
        "model_creation_kwargs": _model_kwargs,
        "max_benchmark_count": max_benchmark_count,
        "run_name": full_eval_run_name(run_id, iteration_counter, pass_index=1),
        "extra_tags": _v2_tags_full(
            run_id, space_id=space_id, stage="full_eval",
            iteration=iteration_counter, ag_id=ag_id,
        ),
    }
    _eval_inp_full = _eval_stage.EvaluationInput(
        space_state={"id": space_id},
        eval_qids=tuple(
            str(b.get("question_id") or "")
            for b in (benchmarks or [])
            if b.get("question_id")
        ),
        run_role="iteration_eval",
        iteration_label=_iteration_label(iteration_counter),
        scope="full",
    )
    # Phase H Completion Task 1: wrap F1 with stage_io_capture via a
    # closure-bound adapter. The decorator demands a 2-arg (ctx, inp)
    # callable; F1's evaluate_post_patch takes eval_kwargs as a kwarg-
    # only argument, so the adapter captures _eval_kwargs_full from
    # local scope and forwards it. Replay-byte-stable — the wrapper
    # returns the EvaluationResult unchanged; MLflow log_text calls
    # are no-ops while mlflow_anchor_run_id is None.
    from genie_space_optimizer.optimization.stage_io_capture import (
        wrap_with_io_capture as _wrap_capture_f1,
    )

    def _f1_capture_adapter(ctx, inp):
        return _eval_stage.evaluate_post_patch(
            ctx, inp, eval_kwargs=_eval_kwargs_full,
        )

    _f1_wrapped = _wrap_capture_f1(
        execute=_f1_capture_adapter, stage_key="evaluation_state",
    )
    _eval_result_full = _f1_wrapped(_stage_ctx_full_eval, _eval_inp_full)
    full_result_1 = _eval_result_full.raw
    new_model_id = full_result_1.get("model_id", "")

    # Phase H Completion Task 2: F2 rca_evidence — additive
    # observability immediately after F1 completes. Sources eval_rows,
    # hard_failure_qids, soft_signal_qids, per_qid_judge, and
    # asi_metadata directly from F1's typed EvaluationResult so F2 has
    # the typed surface it needs without re-deriving from rows.
    # Path C-prime per the F2 follow-up plan's Decision Log.
    # Replay-byte-stable — wrap_with_io_capture returns the bundle
    # unchanged; F2.collect emits zero DecisionRecords; MLflow log_text
    # calls are no-ops while mlflow_anchor_run_id is None.
    try:
        from genie_space_optimizer.optimization.stages import (
            rca_evidence as _rca_stage_f2,
        )
        from genie_space_optimizer.optimization.stage_io_capture import (
            wrap_with_io_capture as _wrap_capture_f2,
        )

        _f2_inp = _rca_stage_f2.RcaEvidenceInput(
            eval_rows=tuple(_eval_result_full.eval_rows),
            hard_failure_qids=tuple(_eval_result_full.hard_failure_qids),
            soft_signal_qids=tuple(_eval_result_full.soft_signal_qids),
            per_qid_judge=dict(_eval_result_full.per_qid_judge),
            asi_metadata=dict(_eval_result_full.asi_metadata),
        )
        _f2_wrapped = _wrap_capture_f2(
            execute=_rca_stage_f2.collect, stage_key="rca_evidence",
        )
        _rca_evidence_bundle = _f2_wrapped(
            _stage_ctx_full_eval, _f2_inp,
        )
    except Exception:
        logger.debug(
            "Phase H Task 2: F2 rca_evidence stage failed (non-fatal)",
            exc_info=True,
        )

    # Task 0 → Task 3: forward the ASI extraction audit row that
    # ``run_evaluation`` stamped on the result dict. This makes a
    # zero-trace eval visible in the lever-loop decision audit instead
    # of silent.
    _asi_audit_1 = full_result_1.get("asi_extraction_audit")
    if isinstance(_asi_audit_1, dict):
        _asi_metrics = _asi_audit_1.get("metrics_json")
        if isinstance(_asi_metrics, str):
            try:
                _asi_metrics = json.loads(_asi_metrics)
            except (TypeError, ValueError):
                _asi_metrics = None
        _audit_emit(
            stage_letter=_asi_audit_1.get("stage_letter") or "C",
            gate_name=_asi_audit_1.get("gate_name") or "asi_extraction",
            decision=_asi_audit_1.get("decision") or "ok",
            reason_code=_asi_audit_1.get("reason_code"),
            metrics=_asi_metrics if isinstance(_asi_metrics, dict) else None,
        )

    scores_1 = dict(full_result_1.get("scores", {}))
    accuracy_1 = full_result_1.get("overall_accuracy", 0.0)
    pre_arbiter_accuracy_1 = float(
        full_result_1.get("pre_arbiter_accuracy", accuracy_1)
    )
    # Tier 1.8: thread both_correct_rate into the full_scores dict so
    # detect_regressions + the arbiter-override suppression at the top of
    # this block can reference it via ``full_scores['_both_correct_rate']``
    # without an extra RPC.
    _bcr_1 = full_result_1.get("both_correct_rate")
    if _bcr_1 is not None:
        scores_1["_both_correct_rate"] = float(_bcr_1)

    # ── Single-criterion acceptance ──
    # The lever loop now decides accept / reject on a single number:
    # post-arbiter delta against the carried baseline. Confirmation
    # eval, run-to-run variance widening, K-of-N composition, and the
    # ``OPTIMIZATION_OBJECTIVE`` pre/post/blended switch are all gone.
    # See acceptance_policy.decide_acceptance and the
    # simplify-acceptance-and-ui-labels plan for rationale.
    full_scores = scores_1
    full_accuracy = accuracy_1
    full_result = full_result_1
    full_pre_arbiter_accuracy = pre_arbiter_accuracy_1

    full_result = _merge_bug4_counters(full_result)
    # Stamp pre-arbiter accuracy on ``full_scores`` so downstream
    # diagnostics (audit rows, MLflow) can read it via the standard
    # scores dict without parameter plumbing. It is no longer a
    # gating signal.
    full_scores["_pre_arbiter/overall_accuracy"] = float(full_pre_arbiter_accuracy)

    # Task 5 — Compare the candidate against the last accepted/live baseline
    # rather than whatever Delta last persisted. Rejected candidate full-eval
    # rows can otherwise become the gate baseline and produce empty
    # target_fixed_qids / out_of_target_regressed_qids.
    _baseline_rows_for_control_plane = list(
        accepted_baseline_rows_for_control_plane or []
    )
    _baseline_source_for_control_plane = "accepted_baseline_memory"
    if not _baseline_rows_for_control_plane:
        _fallback_iter_for_control_plane = load_latest_full_iteration(
            spark, run_id, catalog, schema,
            before_iteration=iteration_counter,
        )
        _baseline_rows_for_control_plane = _rows_from_iteration_payload(
            _fallback_iter_for_control_plane
        )
        _baseline_source_for_control_plane = "delta_latest_full_fallback"
    if not _baseline_rows_for_control_plane:
        logger.warning(
            "Control-plane gate has empty accepted baseline rows at "
            "iteration_counter=%s. Gate will reject with missing_pre_rows.",
            iteration_counter,
        )

    write_iteration(
        spark, run_id, iteration_counter, full_result,
        catalog=catalog, schema=schema,
        lever=int(lever_keys[0]) if lever_keys else 0,
        eval_scope="full", model_id=new_model_id,
    )

    # Soft baseline-drift diagnostic. Compares the current iteration's
    # post-arbiter against the *pre-acceptance* carried baseline at
    # the start of the previous iteration; if it has slipped by
    # ``BASELINE_DRIFT_DIAGNOSTIC_PP`` or more, we suspect the prior
    # accept may have been a noise-driven outlier. Logged + audited;
    # does NOT auto-roll back. None on the first iteration.
    from genie_space_optimizer.common.config import (
        BASELINE_DRIFT_DIAGNOSTIC_PP,
        MIN_POST_ARBITER_GAIN_PP,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        decide_baseline_drift,
    )
    _drift = decide_baseline_drift(
        post_arbiter_current=float(full_accuracy),
        prev_iter_pre_accept_baseline=prev_iter_pre_accept_baseline,
        threshold_pp=float(BASELINE_DRIFT_DIAGNOSTIC_PP),
    )
    if _drift.triggered:
        logger.info(
            "BASELINE DRIFT [%s]: iter %d post-arbiter %.1f%% is %.1fpp "
            "below the previous iteration's pre-acceptance baseline "
            "(%.1f%%). Logging suspected_stale_baseline diagnostic; "
            "iteration continues normally.",
            ag_id, iteration_counter, full_accuracy, _drift.delta_pp,
            float(_drift.prev_iter_pre_accept_baseline or 0.0),
        )
        _audit_emit(
            stage_letter="N",
            gate_name="baseline_drift_diagnostic",
            decision="diagnostic",
            reason_code=_drift.reason_code,
            metrics={
                "post_arbiter_candidate": _drift.post_arbiter_current,
                "prev_iter_pre_accept_baseline": _drift.prev_iter_pre_accept_baseline,
                "delta_pp": _drift.delta_pp,
                "threshold_pp": _drift.threshold_pp,
            },
        )

    # Per-judge regression detection — DIAGNOSTIC only. Logged into
    # the decision_audit row for transparency, never rolls back.
    # Acceptance is decided solely by ``decide_acceptance`` below.
    _best_pre_arbiter = float(
        best_scores.get("_pre_arbiter/overall_accuracy", best_accuracy)
    )
    _diagnostic_regression_tol = max(REGRESSION_THRESHOLD, noise_floor)
    _informational_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
    if full_accuracy >= best_accuracy - 2 * noise_floor:
        _informational_judges.add("asset_routing")
    _diagnostic_regressions = detect_regressions(
        full_scores, best_scores, threshold=_diagnostic_regression_tol,
        skip_judges=_informational_judges,
    )
    if _diagnostic_regressions:
        logger.info(
            "PER-JUDGE REGRESSIONS (diagnostic, non-blocking) [%s]: %s",
            ag_id,
            ", ".join(
                f"{r.get('judge')}: {r.get('previous'):.1f} → "
                f"{r.get('current'):.1f} (Δ-{r.get('drop'):.1f}pp)"
                for r in _diagnostic_regressions
            ),
        )

    # Variables retained for the print blocks further down — under the
    # single-criterion model, "primary" == post-arbiter, "secondary"
    # == pre-arbiter (diagnostic). _objective is pinned for log
    # compatibility but is no longer read for decisions.
    _objective = "post_arbiter"
    _primary_prev = best_accuracy
    _primary_cur = full_accuracy
    _primary_label = "post-arbiter overall accuracy"
    _secondary_prev = _best_pre_arbiter
    _secondary_cur = full_pre_arbiter_accuracy
    _secondary_label = "pre-arbiter result_correctness (diagnostic)"

    logger.info(
        "GATE [%s]: post-arbiter %.1f%% -> %.1f%% (Δ=%+.1fpp)  "
        "pre-arbiter (diagnostic) %.1f%% -> %.1f%% (Δ=%+.1fpp)",
        ag_id, _primary_prev, _primary_cur,
        _primary_cur - _primary_prev,
        _secondary_prev, _secondary_cur,
        _secondary_cur - _secondary_prev,
    )

    # ``regressions`` is the rollback driver — populated only by:
    #   1. decide_acceptance rejection (synthetic entry below), and
    #   2. Task 4 per-question pass→fail blocking_qids (further down).
    # Per-judge diagnostics live in ``_diagnostic_regressions`` and
    # are not appended here.
    regressions: list[dict] = []

    from genie_space_optimizer.optimization.acceptance_policy import (
        decide_acceptance,
    )

    _strict_decision = decide_acceptance(
        post_arbiter_candidate=float(full_accuracy),
        post_arbiter_baseline=float(best_accuracy),
        min_gain_pp=float(MIN_POST_ARBITER_GAIN_PP),
    )
    logger.info(
        "ACCEPTANCE [%s]: accepted=%s reason=%s post-arbiter Δ=%+.1fpp "
        "(candidate=%.1f%%, baseline=%.1f%%, min_gain=%.1fpp)",
        ag_id, _strict_decision.accepted, _strict_decision.reason_code,
        _strict_decision.delta_pp,
        _strict_decision.post_arbiter_candidate,
        _strict_decision.post_arbiter_baseline,
        _strict_decision.min_gain_pp,
    )
    # Emit a typed audit row for the single-criterion acceptance verdict
    # regardless of pass/fail. ``reason_code`` lets a single SQL query
    # answer "did we accept, regress, or fall short of the gain floor?"
    # without parsing logs.
    _audit_emit(
        stage_letter="N",
        gate_name="full_eval_acceptance",
        decision=("pass" if _strict_decision.accepted else "fail"),
        reason_code=_strict_decision.reason_code,
        metrics={
            "delta_pp": _strict_decision.delta_pp,
            "min_gain_pp": _strict_decision.min_gain_pp,
            "post_arbiter_candidate": _strict_decision.post_arbiter_candidate,
            "post_arbiter_baseline": _strict_decision.post_arbiter_baseline,
            "previous_pre_arbiter": _best_pre_arbiter,
            "previous_post_arbiter": best_accuracy,
        },
    )

    # Task 4: per-question pass/fail transition tracker. Aggregate
    # averages on the retail run hid AG2 flipping previously-passing
    # qids to failing. Compute pre/post pass maps, classify each qid,
    # and roll back when any non-suppressed qid went pass_to_fail.
    from genie_space_optimizer.optimization.evaluation import (
        row_is_hard_failure as _row_is_hard_failure,
    )
    from genie_space_optimizer.optimization.per_question_regression import (
        build_question_regression_rows,
        compute_question_transitions,
    )

    def _build_pass_map(rows_iter: list[dict]) -> dict[str, bool]:
        m: dict[str, bool] = {}
        for _row in rows_iter or []:
            _qid = (
                _row.get("inputs.question_id")
                or _row.get("inputs/question_id")
                or _row.get("question_id")
                or (_row.get("inputs") or {}).get("question_id", "")
            )
            if not _qid:
                continue
            m[str(_qid)] = not _row_is_hard_failure(_row)
        return m

    # Build "after" pass map from the single full-eval pass. The
    # confirmation run is gone, so there's nothing to "worst-by-
    # accuracy" pick across.
    _after_rows = full_result_1.get("rows") or []
    _pass_after = _build_pass_map(_after_rows)

    # Build "before" pass map from the pre-candidate baseline rows captured
    # above (loaded with ``before_iteration=iteration_counter`` so the
    # candidate row we just persisted cannot serve as its own baseline).
    _pass_before: dict[str, bool] = {}
    try:
        _pass_before = _build_pass_map(_baseline_rows_for_control_plane)
    except Exception:
        logger.debug("Failed to build prior pass map for Task 4 check", exc_info=True)

    # Control-plane acceptance: causal hard-failure improvement check.
    # Aggregates target_qids from the patches that were applied this
    # iteration (set by AG-scoped grounding in the proposal stage) and
    # runs the pure helper to decide whether the iteration actually
    # fixed what it claimed to fix without out-of-target regressions.
    from genie_space_optimizer.optimization.control_plane import (
        assert_regression_debt_partition_complete,
        decide_control_plane_acceptance,
        format_control_plane_acceptance_detail,
    )

    _target_qids: tuple[str, ...] = ()
    for _patch in patches or []:
        for _q in _patch.get("_grounding_target_qids", []) or []:
            if _q:
                _target_qids += (str(_q),)
    if not _target_qids:
        for _patch in patches or []:
            for _q in _patch.get("target_qids", []) or []:
                if _q:
                    _target_qids += (str(_q),)
    _target_qids = tuple(dict.fromkeys(_target_qids))

    # Task 5 — gate input visibility. Operators must be able to tell at a
    # glance whether pre_rows came from accepted-baseline memory, was empty,
    # or matched post_rows (smoking-gun stale-baseline shape).
    try:
        from genie_space_optimizer.optimization.control_plane import (
            hard_failure_qids as _hard_failure_qids_for_log,
        )

        _pre_hard_for_log = sorted(
            set(_hard_failure_qids_for_log(_baseline_rows_for_control_plane))
        )
        _post_hard_for_log = sorted(
            set(_hard_failure_qids_for_log(_after_rows))
        )
        logger.info(
            "Control-plane gate inputs (AG=%s, source=%s): pre_rows=%d "
            "(hard=%s), post_rows=%d (hard=%s), target_qids=%s",
            ag_id,
            _baseline_source_for_control_plane,
            len(_baseline_rows_for_control_plane),
            _pre_hard_for_log[:10],
            len(_after_rows),
            _post_hard_for_log[:10],
            list(_target_qids),
        )
        if (
            _baseline_rows_for_control_plane
            and _after_rows
            and set(_pre_hard_for_log) == set(_post_hard_for_log)
        ):
            logger.error(
                "AG %s: pre_hard == post_hard while the score moved — "
                "the gate baseline may be stale/candidate-like. Verify "
                "the gate is using _accepted_baseline_rows_for_control_plane "
                "and not a rejected full-eval row from "
                "load_latest_full_iteration.",
                ag_id,
            )
    except Exception:
        logger.debug("Gate input log failed (non-fatal)", exc_info=True)

    _max_new_hard_regressions = 1
    _protected_qids: tuple[str, ...] = ()
    try:
        if isinstance(config, dict):
            _max_new_hard_regressions = int(config.get("max_new_hard_regressions", 1))
            _protected_qids = tuple(
                str(q) for q in (config.get("protected_benchmark_qids") or []) if str(q)
            )
    except Exception:
        pass
    # PR-E Task 2: compute pre-arbiter pcts BEFORE the control-plane gate
    # so the new accepted_pre_arbiter_improvement branch can fire when
    # post-arbiter is saturated but Genie's raw output improved.
    def _pre_arbiter_correct_count(rows: list[dict]) -> int:
        count = 0
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            v = (
                r.get("feedback/pre_arbiter_correctness/value")
                or r.get("feedback/result_correctness/value")
                or r.get("result_correctness/value")
                or ""
            )
            if str(v).strip().lower() in {"yes", "true", "1", "1.0"}:
                count += 1
        return count

    _baseline_pre_arbiter_pct = (
        100.0
        * _pre_arbiter_correct_count(_baseline_rows_for_control_plane)
        / max(1, len(_baseline_rows_for_control_plane or []))
    )
    _candidate_pre_arbiter_pct = (
        100.0
        * _pre_arbiter_correct_count(_after_rows)
        / max(1, len(_after_rows or []))
    )

    # Optimizer Control-Plane Hardening Plan — Task A: when the
    # GSO_TARGET_AWARE_ACCEPTANCE flag is on, derive thresholds_met
    # from the candidate accuracy against the project's overall-
    # accuracy bar (95.0% matches the airline pilot's exit
    # criterion). When the flag is off, pass thresholds_met=True so
    # the gate's behaviour is identical to today.
    from genie_space_optimizer.common.config import (
        target_aware_acceptance_enabled as _target_aware_acceptance_enabled,
    )

    _GATE_OVERALL_ACCURACY_BAR_PCT: float = 95.0
    _gate_thresholds_met = (
        float(full_accuracy) >= _GATE_OVERALL_ACCURACY_BAR_PCT
        if _target_aware_acceptance_enabled()
        else True
    )

    _control_plane_decision = decide_control_plane_acceptance(
        baseline_accuracy=float(best_accuracy),
        candidate_accuracy=float(full_accuracy),
        target_qids=_target_qids,
        pre_rows=_baseline_rows_for_control_plane,
        post_rows=_after_rows,
        min_gain_pp=float(MIN_POST_ARBITER_GAIN_PP),
        max_new_hard_regressions=_max_new_hard_regressions,
        protected_qids=_protected_qids,
        baseline_pre_arbiter_accuracy=_baseline_pre_arbiter_pct,
        candidate_pre_arbiter_accuracy=_candidate_pre_arbiter_pct,
        thresholds_met=_gate_thresholds_met,
    )
    # P1 invariant — partition every out-of-target regression into
    # exactly one of soft/passing/unknown. Raises AssertionError when
    # GSO_REGRESSION_DEBT_INVARIANT is on (default) and the partition
    # is incomplete or non-disjoint, so unattributed regression debt
    # fails loud at runtime instead of silently in the marker.
    assert_regression_debt_partition_complete(_control_plane_decision)

    # v2 Task 2 — Pre-arbiter regression guardrail. A candidate that drops
    # broad pre-arbiter accuracy without flipping any declared target qid
    # is the symptom of a wide instruction edit trading healthy questions
    # for nothing — the Q011 silent-regression pattern. Block at acceptance
    # so the AG cannot be carried forward as a "no-op" win.
    from genie_space_optimizer.optimization.control_plane import (
        decide_pre_arbiter_regression_guardrail,
    )

    _target_fixed_qids_for_guardrail = tuple(
        sorted(set(_control_plane_decision.target_fixed_qids or ()))
    )
    _pre_arbiter_decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=_baseline_pre_arbiter_pct,
        candidate_pre_arbiter_accuracy=_candidate_pre_arbiter_pct,
        target_fixed_qids=_target_fixed_qids_for_guardrail,
        max_pre_arbiter_regression_pp=5.0,
    )
    print(
        format_evaluation_summary_block(
            iteration=iteration_counter,
            ag_id=ag_id,
            baseline_pre_arbiter=_baseline_pre_arbiter_pct,
            candidate_pre_arbiter=_candidate_pre_arbiter_pct,
            baseline_post_arbiter=float(best_accuracy),
            candidate_post_arbiter=float(full_accuracy),
            target_fixed_qids=_target_fixed_qids_for_guardrail,
        )
    )
    if not _pre_arbiter_decision.accepted:
        logger.warning(
            "[%s] pre_arbiter_regression_blocked: baseline=%.1f%% "
            "candidate=%.1f%% delta=%.1fpp reason=%s",
            ag_id,
            _baseline_pre_arbiter_pct,
            _candidate_pre_arbiter_pct,
            _pre_arbiter_decision.delta_pp,
            _pre_arbiter_decision.reason_code,
        )
        try:
            _audit_emit(
                stage_letter="M",
                gate_name="pre_arbiter_regression_guardrail",
                decision="fail",
                reason_code="pre_arbiter_regression_blocked",
                reason_detail=(
                    f"baseline_pre_arbiter={_baseline_pre_arbiter_pct:.1f}% "
                    f"candidate_pre_arbiter={_candidate_pre_arbiter_pct:.1f}% "
                    f"delta_pp={_pre_arbiter_decision.delta_pp:+.1f} "
                    f"reason={_pre_arbiter_decision.reason_code}"
                ),
                metrics={
                    "baseline_pre_arbiter_pct": _baseline_pre_arbiter_pct,
                    "candidate_pre_arbiter_pct": _candidate_pre_arbiter_pct,
                    "delta_pp": _pre_arbiter_decision.delta_pp,
                },
            )
        except Exception:
            logger.debug(
                "Failed to emit pre_arbiter_regression_blocked audit row",
                exc_info=True,
            )
        # Force the control-plane acceptance decision to reject so the
        # downstream regression block rolls the AG back. We synthesize a
        # new ControlPlaneAcceptance using ``replace`` — the existing
        # rejection block reads ``.accepted`` and the reason fields.
        from dataclasses import replace as _dc_replace
        _control_plane_decision = _dc_replace(
            _control_plane_decision,
            accepted=False,
            reason_code="pre_arbiter_regression_blocked",
        )

    # Task 6 — instrument the case where eval-row deltas imply a fix on
    # a target QID but the control-plane gate reports none.
    try:
        _log_target_fixed_disagreement(
            ag_id=ag_id,
            baseline_source=_baseline_source_for_control_plane,
            pre_hard_qids=_pre_hard_for_log,
            post_hard_qids=_post_hard_for_log,
            target_qids=tuple(_target_qids),
            target_fixed_qids=tuple(_control_plane_decision.target_fixed_qids),
        )
    except Exception:
        logger.debug(
            "Failed to log target-fixed disagreement diagnostic",
            exc_info=True,
        )

    # Suppressed qids: quarantine + GT correction queue. Those qids
    # legitimately can flip pass_to_fail without rolling back the AG.
    _suppressed_qids: set[str] = set()
    try:
        _suppressed_qids |= set(full_result_1.get("quarantined_benchmarks_qids") or [])
    except Exception:
        pass
    try:
        for _r in full_result_1.get("rows") or []:
            _av = (_r.get("feedback/arbiter/value") or _r.get("arbiter/value") or "")
            _rc = (_r.get("feedback/result_correctness/value") or _r.get("result_correctness/value") or "")
            if str(_av).strip().lower() == "genie_correct" and str(_rc).strip().lower() in {"no", "false", "0", "0.0"}:
                _qid = (
                    _r.get("inputs.question_id")
                    or _r.get("inputs/question_id")
                    or _r.get("question_id")
                    or (_r.get("inputs") or {}).get("question_id", "")
                )
                if _qid:
                    _suppressed_qids.add(str(_qid))
    except Exception:
        pass

    _t4_verdict = compute_question_transitions(
        pass_map_before=_pass_before,
        pass_map_after=_pass_after,
        suppressed_qids=_suppressed_qids,
    )

    # v2 Task 21 — persistence of per-question regression rows (with
    # cluster/proposal/applied-patch attribution) is now owned by
    # ``_run_lever_loop``, which has direct access to ``strategy``,
    # ``all_proposals`` and the apply log. Surface the verdict and
    # suppressed-qid set in the gate result so the caller can build the
    # attribution dicts and call ``build_question_regression_rows`` once
    # with full provenance.

    # Task 5B — diagnostic only. The control-plane decision below now owns
    # acceptance with a tiered policy (bounded debt vs unbounded collateral).
    # Appending these qids as ``regressions`` here would pre-empt that
    # policy and force a rollback even when the gate intends to accept the
    # AG with regression debt.
    _per_question_blocking_qids = list(_t4_verdict.blocking_qids or [])
    if _per_question_blocking_qids:
        logger.info(
            "Per-question regression check detected %d candidate debt qid(s) for AG %s: %s",
            len(_per_question_blocking_qids),
            ag_id,
            ", ".join(_per_question_blocking_qids[:10]),
        )

    if not _strict_decision.accepted:
        # Translate the typed acceptance reason into a regression entry
        # so the existing rejection block downstream rolls back the AG
        # without needing a parallel code path.
        regressions.append({
            "judge": f"acceptance_gate ({_strict_decision.reason_code})",
            "previous": _strict_decision.post_arbiter_baseline,
            "current": _strict_decision.post_arbiter_candidate,
            "drop": -_strict_decision.delta_pp,
        })

    if (
        ENABLE_CONTROL_PLANE_ACCEPTANCE
        and not _control_plane_decision.accepted
    ):
        regressions.append({
            "judge": "control_plane_acceptance",
            "previous": _control_plane_decision.baseline_accuracy,
            "current": _control_plane_decision.candidate_accuracy,
            "delta": _control_plane_decision.delta_pp,
            "severity": "critical",
            "reason": _control_plane_decision.reason_code,
            "detail": format_control_plane_acceptance_detail(_control_plane_decision),
            "target_qids": list(_control_plane_decision.target_qids),
            "target_fixed_qids": list(_control_plane_decision.target_fixed_qids),
            "target_still_hard_qids": list(
                _control_plane_decision.target_still_hard_qids
            ),
            "out_of_target_regressed_qids": list(
                _control_plane_decision.out_of_target_regressed_qids
            ),
        })

    # Under the single-criterion model the legacy noise filter and
    # hard accuracy guard are no longer needed: ``regressions`` is
    # only populated by (1) ``decide_acceptance`` rejection or (2)
    # Task 4 per-question pass→fail blocking. Both are real
    # rejection signals; there's nothing to filter or guard against.

    if regressions:
        # Tier 3.2: prefer r['previous']/r['current'] (populated by
        # detect_regressions and by the overall_accuracy synthetic entry)
        # so synthetic judges like ``overall_accuracy_guard`` show real
        # numbers instead of ``0.0->0.0``. Render the delta as a signed
        # pp value (negative = regression) so operators don't misread
        # ``+15.0`` as an improvement.
        def _fmt_reg(r: dict) -> str:
            _prev = r.get("previous")
            if _prev is None:
                _prev = best_scores.get(r.get("judge", ""), 0.0)
            _cur = r.get("current")
            if _cur is None:
                _cur = full_scores.get(r.get("judge", ""), 0.0)
            _delta = float(_cur) - float(_prev)
            base = (
                f"{r.get('judge', '?')} {float(_prev):.1f}->{float(_cur):.1f} "
                f"({_delta:+.1f}pp)"
            )
            if r.get("judge") == "control_plane_acceptance":
                return f"{base} [{r.get('detail', '')}]"
            return base

        _reg_details = ", ".join(_fmt_reg(r) for r in regressions)
        print(
            _section(f"FULL EVAL [{ag_id}]: FAIL (REGRESSION)", "-") + "\n"
            + _kv(
                "Objective",
                f"{_objective}  (primary={_primary_label})",
            ) + "\n"
            + _kv(
                "Primary accuracy",
                f"{_primary_prev:.1f}% -> {_primary_cur:.1f}% "
                f"({_primary_cur - _primary_prev:+.1f}pp)",
            ) + "\n"
            + _kv(
                "Secondary accuracy",
                f"{_secondary_prev:.1f}% -> {_secondary_cur:.1f}% "
                f"({_secondary_cur - _secondary_prev:+.1f}pp)",
            ) + "\n"
            + _kv("Regressions", _reg_details) + "\n"
            + _kv("Action", "ROLLBACK") + "\n"
            + _bar("-")
        )
        try:
            update_provenance_gate(
                spark, run_id, iteration_counter - 1, _primary_lever,
                "full", "rollback",
                {"regressions": [{"judge": r["judge"], "drop": r["drop"]} for r in regressions]},
                catalog, schema,
            )
        except Exception:
            logger.debug("Failed to update provenance gate (full rollback)", exc_info=True)
        try:
            log_gate_feedback_on_traces(
                full_result, "full", "rollback",
                regressions=regressions, lever=_primary_lever, iteration=iteration_counter,
            )
        except Exception:
            logger.debug("Failed to log full eval gate feedback", exc_info=True)
        _audit_emit(
            stage_letter="N",
            gate_name="full_eval_acceptance",
            decision="rolled_back",
            reason_code=_strict_decision.reason_code,
            reason_detail=f"full_eval: {regressions[0]['judge']}",
            metrics={
                "regression_count": len(regressions),
                "post_arbiter_candidate": _strict_decision.post_arbiter_candidate,
                "post_arbiter_baseline": _strict_decision.post_arbiter_baseline,
                "delta_pp": _strict_decision.delta_pp,
                "min_gain_pp": _strict_decision.min_gain_pp,
                "pre_arbiter_candidate": float(full_pre_arbiter_accuracy),
                "pre_arbiter_baseline": float(_best_pre_arbiter),
                "diagnostic_regressions": [
                    r.get("judge") for r in _diagnostic_regressions
                ],
            },
        )
        _audit_persist()
        return {
            "passed": False,
            "rollback_reason": f"full_eval: {regressions[0]['judge']}",
            "failed_eval_result": full_result,
            "regressions": regressions,
            "_t4_verdict": _t4_verdict,
            "_suppressed_qids": _suppressed_qids,
        }

    # ── PASSED ────────────────────────────────────────────────────────
    _score_delta = ", ".join(
        f"{j} {best_scores.get(j, 0):.1f}->{full_scores.get(j, 0):.1f}"
        for j in sorted(full_scores)
    )
    if _control_plane_decision.reason_code == "accepted_with_regression_debt":
        _accept_label = "PASS -- ACCEPT WITH REGRESSION DEBT"
    else:
        _accept_label = "PASS -- ACCEPTED"
    print(
        _section(f"FULL EVAL [{ag_id}]: {_accept_label}", "=") + "\n"
        + _kv(
            "Objective",
            f"{_objective}  (primary={_primary_label})",
        ) + "\n"
        + _kv(
            "Primary accuracy",
            f"{_primary_prev:.1f}% -> {_primary_cur:.1f}% "
            f"({_primary_cur - _primary_prev:+.1f}pp)",
        ) + "\n"
        + _kv(
            "Secondary accuracy",
            f"{_secondary_prev:.1f}% -> {_secondary_cur:.1f}% "
            f"({_secondary_cur - _secondary_prev:+.1f}pp)",
        ) + "\n"
        + _kv("Score changes", _score_delta) + "\n"
        + _bar("=")
    )
    try:
        update_provenance_gate(
            spark, run_id, iteration_counter - 1, _primary_lever,
            "full", "pass", None, catalog, schema,
        )
    except Exception:
        logger.debug("Failed to update provenance gate (full pass)", exc_info=True)
    try:
        log_gate_feedback_on_traces(
            full_result, "full", "pass",
            lever=_primary_lever, iteration=iteration_counter,
        )
    except Exception:
        logger.debug("Failed to log full eval gate feedback", exc_info=True)

    # Track F — when the candidate is accepted via the attribution-drift
    # branch, the declared target qids did not move but other qids
    # flipped. Surface the actual flipped qids so operators can see why
    # the candidate is a real win even though the named target is still
    # hard.
    if _control_plane_decision.reason_code == "accepted_with_attribution_drift":
        from genie_space_optimizer.optimization.control_plane import (
            hard_failure_qids,
        )

        _pre_hard_set = set(hard_failure_qids(_baseline_rows_for_control_plane))
        _post_hard_set = set(hard_failure_qids(_after_rows))
        _actual_fixed = tuple(sorted(_pre_hard_set - _post_hard_set))
        logger.info(
            "[Track F] ACCEPTED with attribution drift: "
            "declared_target_qids=%s actual_fixed_qids=%s",
            tuple(_control_plane_decision.target_qids),
            _actual_fixed,
        )

    _audit_emit(
        stage_letter="N",
        gate_name="full_eval_acceptance",
        decision="accepted",
        reason_code=_strict_decision.reason_code,
        metrics={
            "post_arbiter_candidate": _strict_decision.post_arbiter_candidate,
            "post_arbiter_baseline": _strict_decision.post_arbiter_baseline,
            "delta_pp": _strict_decision.delta_pp,
            "min_gain_pp": _strict_decision.min_gain_pp,
            "pre_arbiter_candidate": float(full_pre_arbiter_accuracy),
            "pre_arbiter_baseline": float(_best_pre_arbiter),
            "diagnostic_regressions": [
                r.get("judge") for r in _diagnostic_regressions
            ],
        },
    )
    _audit_persist()
    return {
        "passed": True,
        "full_scores": full_scores,
        "full_accuracy": full_accuracy,
        "acceptance_delta_pp": _strict_decision.delta_pp,
        "new_model_id": new_model_id,
        "full_result": full_result,
        "_t4_verdict": _t4_verdict,
        "_suppressed_qids": _suppressed_qids,
        # Task 9 — surface acceptance tiering so the loop can carry debt
        # forward without re-running ``decide_control_plane_acceptance``.
        "acceptance_decision": {
            "reason": _control_plane_decision.reason_code,
            "target_qids": list(_control_plane_decision.target_qids),
            "target_fixed_qids": list(_control_plane_decision.target_fixed_qids),
            "target_still_hard_qids": list(
                _control_plane_decision.target_still_hard_qids
            ),
            "out_of_target_regressed_qids": list(
                _control_plane_decision.out_of_target_regressed_qids
            ),
            "regression_debt_qids": list(
                _control_plane_decision.regression_debt_qids
            ),
            "soft_to_hard_regressed_qids": list(
                _control_plane_decision.soft_to_hard_regressed_qids
            ),
            "passing_to_hard_regressed_qids": list(
                _control_plane_decision.passing_to_hard_regressed_qids
            ),
        },
    }


def compute_iteration_budget(
    *,
    hard_cluster_count: int,
    requested_max_iterations: int,
) -> int:
    """Return the iteration budget, scaled by initial hard cluster count.

    Each hard cluster gets at least ``MAX_ITERATIONS_PER_CLUSTER`` attempts,
    floored at ``MAX_ITERATIONS`` and capped at ``MAX_ITERATIONS_HARD_CEILING``.
    An explicit caller request wins when greater than the scaled value.
    """
    from genie_space_optimizer.common.config import (
        MAX_ITERATIONS as _MAX_ITERATIONS,
        MAX_ITERATIONS_HARD_CEILING as _CEILING,
        MAX_ITERATIONS_PER_CLUSTER as _PER_CLUSTER,
    )
    requested = int(requested_max_iterations or _MAX_ITERATIONS)
    scaled = max(int(_MAX_ITERATIONS), int(hard_cluster_count or 0) * int(_PER_CLUSTER))
    capped_scaled = min(scaled, int(_CEILING))
    # An explicit caller request wins when greater than the capped scaled value.
    return max(capped_scaled, requested) if requested > int(_MAX_ITERATIONS) else capped_scaled


def _run_lever_loop(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    benchmarks: list[dict],
    exp_name: str,
    prev_scores: dict[str, float],
    prev_accuracy: float,
    prev_model_id: str,
    config: dict,
    catalog: str,
    schema: str,
    levers: list[int] | None = None,
    max_iterations: int = MAX_ITERATIONS,
    thresholds: dict[str, float] | None = None,
    apply_mode: str = APPLY_MODE,
    triggered_by: str = "",
    human_corrections: list[dict] | None = None,
    enrichment_done: bool = False,
    enrichment_model_id: str = "",
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
    iq_scan_recommended_levers: list[int] | None = None,
    iq_scan_summary: dict | None = None,
) -> dict:
    """Stage 3: Iterate levers with convergence checking.

    Internal Python loop over levers. Supports resume on task retry.

    When *enrichment_done* is True, Phase 1 (proactive enrichment) is skipped
    because it was already executed by the standalone enrichment task. The
    enriched config is loaded from the Genie Space API instead.

    Returns dict with best scores, model_id, iteration_counter, levers lists.

    Source-specific invariant (lever-loop-proposed sql_snippets / join_specs):
        The lever loop proposes structural SQL via the LLM (Lever 6 for
        sql_snippets; join-lever for join_specs). Persistence gates:

        - Exec-validation at propose time via ``validate_sql_snippet`` /
          ``_explain_join_candidate``.
        - Post-iteration full-eval arbiter gate (``detect_regressions``
          + accuracy-drop check below). Iterations whose patches cause
          arbiter-detected regression roll back, removing the offending
          snippet.

        This is the functional equivalent of proactive enrichment's
        pre-mining arbiter filter: both mechanisms guarantee that
        persisted sql_snippets / join_specs are arbiter-backed. The
        Bug #4 firewall is therefore no longer wired for these patch
        types — see scoping comment above ``_PATCH_TEXT_FIELDS`` in
        leakage.py.
    """
    levers = levers or DEFAULT_LEVER_ORDER
    thresholds = thresholds or DEFAULT_THRESHOLDS

    from genie_space_optimizer.optimization.run_analysis_contract import (
        convergence_marker,
        iteration_summary_marker,
        phase_b_marker,
        run_manifest_marker,
    )

    write_stage(
        spark, run_id, "LEVER_LOOP_STARTED", "STARTED",
        task_key="lever_loop", catalog=catalog, schema=schema,
    )
    _ensure_sql_context(spark, catalog, schema)

    # GSO run analysis: emit machine-readable start manifest and
    # set Databricks job/run tags on the active MLflow run so the
    # ``gso-lever-loop-run-analysis`` skill can correlate evidence.
    _db_job_id = ""
    _db_parent_run_id = ""
    _db_task_run_id = ""
    try:
        import os as _os_run_analysis

        _db_job_id = str(_os_run_analysis.environ.get("DATABRICKS_JOB_ID") or "")
        _db_parent_run_id = str(
            _os_run_analysis.environ.get("DATABRICKS_RUN_ID")
            or _os_run_analysis.environ.get("DATABRICKS_JOB_RUN_ID")
            or ""
        )
        _db_task_run_id = str(
            _os_run_analysis.environ.get("DATABRICKS_TASK_RUN_ID")
            or _db_parent_run_id
        )
        print(run_manifest_marker(
            optimization_run_id=run_id,
            databricks_job_id=_db_job_id,
            databricks_parent_run_id=_db_parent_run_id,
            lever_loop_task_run_id=_db_task_run_id,
            mlflow_experiment_id=str(os.environ.get("MLFLOW_EXPERIMENT_ID") or ""),
            space_id=space_id,
            event="start",
        ))
    except Exception:
        logger.debug("GSO run manifest start marker skipped", exc_info=True)
    try:
        import mlflow as _mlflow_run_analysis  # type: ignore[import-not-found]
        if _mlflow_run_analysis.active_run() is not None:
            _mlflow_run_analysis.set_tags({
                "genie.databricks.job_id": _db_job_id,
                "genie.databricks.parent_run_id": _db_parent_run_id,
                "genie.databricks.lever_loop_task_run_id": _db_task_run_id,
                "genie.phase_b.partial": "false",
            })
    except Exception:
        logger.debug("GSO run analysis MLflow tags skipped", exc_info=True)

    # Phase F+H C17 (v2, Phase-H reliability fix): resolve a stable
    # parent MLflow run id for Phase H artifacts without relying on
    # ``mlflow.active_run()``. The lever-loop notebook does not open an
    # explicit parent run before calling the harness, so the legacy
    # ``active_run()`` path produced ``None`` in production and left
    # the postmortem bundle unassembled. ``resolve_or_create_phase_h_anchor``
    # searches the experiment for an existing parent tagged
    # ``genie.run_role=lever_loop``/``genie.run_type=lever_loop`` for
    # this optimization_run_id and creates one when missing. It returns
    # ``None`` on any MLflow/client failure so observability never
    # breaks the optimizer. As a final compatibility fallback, if the
    # resolver returns ``None`` we still honor an existing
    # ``active_run()`` anchor when one happens to be present.
    _phase_h_anchor_run_id: str | None = None
    try:
        from genie_space_optimizer.optimization.phase_h_anchor import (
            resolve_or_create_phase_h_anchor as _resolve_phase_h_anchor,
        )
        _phase_h_anchor_run_id = _resolve_phase_h_anchor(
            experiment_name=exp_name,
            optimization_run_id=str(run_id),
            databricks_job_id=_db_job_id,
            databricks_parent_run_id=_db_parent_run_id,
            lever_loop_task_run_id=_db_task_run_id,
        )
    except Exception:
        logger.debug(
            "Phase H anchor: resolve_or_create_phase_h_anchor failed (non-fatal)",
            exc_info=True,
        )
    if not _phase_h_anchor_run_id:
        try:
            import mlflow as _mlflow_phase_h  # type: ignore[import-not-found]
            _active_phase_h = _mlflow_phase_h.active_run()
            if _active_phase_h is not None:
                _phase_h_anchor_run_id = _active_phase_h.info.run_id
        except Exception:
            logger.debug(
                "Phase F+H C17 v2: parent run id capture failed (non-fatal)",
                exc_info=True,
            )

    # Phase F+H C17 (v2): bundle-input accumulators for C18's
    # termination-time bundle assembly. Populated minimally on this
    # commit; per-iteration trace population is deferred (C18 will
    # render whatever is available — empty iteration transcripts are
    # acceptable for the MVP bundle).
    from typing import Any as _AnyPhaseH
    _baseline_for_summary: dict[str, _AnyPhaseH] = _build_baseline_overview_dict(
        prev_accuracy_percent=float(prev_accuracy),
        prev_scores=prev_scores,
        hard_failure_count=0,
        soft_signal_count=0,
    )
    _iter_traces: dict[int, _AnyPhaseH] = {}
    _iter_summaries: dict[int, dict[str, _AnyPhaseH]] = {}
    _hard_failures_for_overview: list[tuple[str, str, str]] = []
    # ── Modular spine carry-over state (cross-iteration) ─────────────
    # The lever loop's modular spine is:
    #   1. Evaluation State
    #   2. RCA Evidence
    #   3. Cluster Formation
    #   4. Action Group Selection
    #   5. Proposal Generation
    #   6. Safety Gates
    #   7. Applied Patches
    #   8. Post-Patch Evaluation
    #   9. Acceptance / Rollback
    #   10. Learning / Next Action
    # Two pieces of cross-iteration state ride here at function scope:
    #
    # (P3 task 4) ``_structural_synthesis_buffer`` — buffer for
    # cluster-driven synthesis proposals queued at the lever-5
    # structural-gate drop site (spine stage 6 fallback into stage 5).
    # Drained at the start of each iteration via
    # _consume_structural_synthesis_buffer. With same-iteration
    # injection at the drop site preferred, this buffer typically
    # remains empty; the drain runs each iteration for hygiene and
    # as a future hook for cross-iteration carry-over.
    _structural_synthesis_buffer: list[dict] = []
    # (Control-plane Task C) ``_prior_buckets_by_qid`` — qid →
    # FailureBucket map produced by spine stage 10 (Learning) at the
    # END of each iteration and consumed by spine stage 4 (Action
    # Group Selection) at the START of the next iteration. Empty on
    # iter 1 so the slate is unfiltered; populated post-trace
    # classification. Active when GSO_BUCKET_DRIVEN_AG_SELECTION is
    # on (production-locked: always on).
    _prior_buckets_by_qid: dict[str, Any] = {}
    # (Cycle 5 T2) ``_prior_iteration_dropped_causal_patches`` —
    # gate-drops carrying a causal-target patch from the prior
    # iteration. Empty on iter 1; populated at the END of each
    # iteration from the per-iteration ``_iter_dropped_causal``
    # accumulator and consumed by spine stage 4 (Action Group
    # Selection) at the START of the next iteration via
    # ``ActionGroupsInput.prior_iteration_dropped_causal_patches``.
    # The strategist prompt-builder consumer surfaces these to the
    # LLM when ``GSO_CAUSAL_DROP_FEEDBACK_TO_STRATEGIST`` is on so
    # the next call can propose a narrower variant or shift levers
    # instead of re-emitting the same dropped pattern.
    _prior_iteration_dropped_causal_patches: list = []

    resume_state = _resume_lever_loop(spark, run_id, catalog, schema)
    # S10 — ``start_lever`` is informational only: the loop below always
    # begins at Lever 1 and iterates the full ``levers`` sequence per
    # iteration. ``None`` means "no completed lever in Delta", i.e. a
    # fresh run; an int means "the last COMPLETE lever_stage from a
    # prior task attempt" and is surfaced in the setup block so on-call
    # can distinguish a retried task from a cold start.
    start_lever = resume_state.get("resume_from_lever")
    iteration_counter = resume_state.get("iteration_counter", 0)
    if resume_state.get("prev_scores"):
        prev_scores = resume_state["prev_scores"]
    if resume_state.get("prev_model_id"):
        prev_model_id = resume_state["prev_model_id"]
    if resume_state.get("prev_accuracy"):
        prev_accuracy = resume_state["prev_accuracy"]

    baseline_accuracy = prev_accuracy
    best_scores = dict(prev_scores)
    # B0.2 — defensive fallback for resumed/older runs that pre-date
    # B0.1's ``_pre_arbiter/overall_accuracy`` stamping. When the prior
    # eval emitted only ``_pre_arbiter/result_correctness`` (the
    # canonical primary signal), promote it so the gate's
    # baseline-pre-arbiter lookup returns a real pre-arbiter number
    # rather than silently falling back to post-arbiter accuracy.
    if "_pre_arbiter/overall_accuracy" not in best_scores:
        if "_pre_arbiter/result_correctness" in best_scores:
            best_scores["_pre_arbiter/overall_accuracy"] = best_scores[
                "_pre_arbiter/result_correctness"
            ]
    best_accuracy = prev_accuracy
    best_model_id = prev_model_id
    best_iteration = iteration_counter
    # Task 5 — Accepted/live baseline rows for the control-plane gate.
    # This is updated only after an AG is accepted, never after a
    # rejected candidate full eval. Falling back to Delta latest-full
    # makes the source visible in logs as a diagnostic risk.
    #
    # v2 Task 22 — clustering reads ``load_latest_state_iteration``
    # (eval_scope ∈ {full, enrichment}). The control-plane guardrail
    # must use the same source so a candidate is not flagged a
    # regression against a stale pre-enrichment baseline.
    _accepted_baseline_rows_for_control_plane: list[dict] = []
    _accepted_baseline_eval_scope: str = "unknown"
    try:
        from genie_space_optimizer.optimization.control_plane import (
            select_control_plane_baseline_rows,
        )
        _state_iter_baseline = load_latest_state_iteration(
            spark, run_id, catalog, schema,
        )
        _full_iter_baseline = load_latest_full_iteration(
            spark, run_id, catalog, schema,
        )
        _accepted_baseline_rows_for_control_plane, _accepted_baseline_eval_scope = (
            select_control_plane_baseline_rows(
                latest_state_iteration=_state_iter_baseline,
                latest_full_iteration=_full_iter_baseline,
            )
        )
        logger.info(
            "Control-plane baseline seeded: rows=%d eval_scope=%s",
            len(_accepted_baseline_rows_for_control_plane),
            _accepted_baseline_eval_scope,
        )
    except Exception:
        logger.warning("Failed to initialize accepted baseline rows", exc_info=True)
    # Task 8 — accumulator for regression-debt qids carried into the next
    # strategist call. Updated only when an AG is accepted with debt.
    _regression_debt_qids_for_next_iteration: tuple[str, ...] = ()

    # Task 6 — track whether live Genie state is trusted enough to
    # mutate quarantine. Set to False if rollback verification ever
    # reports a real mismatch; restored on the next accepted AG. Task 2
    # makes a real mismatch terminal, so this is defense-in-depth.
    _rollback_state_trusted_for_quarantine = True

    # Task 2 — surface the trigger-time baseline snapshot ownership
    # contract. The run-level ``config_snapshot`` belongs in the
    # ``genie_opt_runs`` row, written by the app backend before the
    # lever loop starts. If it is missing here we degrade to a
    # one-shot bounded API fallback so the next operator reading the
    # log can tell the contract was violated.
    try:
        _run_row = load_run(spark, run_id, catalog, schema) or {}
    except Exception:
        _run_row = {}
    if not _run_row.get("config_snapshot"):
        logger.warning(
            "RUN-LEVEL CONFIG SNAPSHOT MISSING for run_id=%s. The run-level "
            "config snapshot should have been captured at trigger time by the app "
            "backend before the lever loop started. Falling back to a one-shot "
            "API fetch; this will fail on serverless if the runtime identity "
            "lacks Genie Space 'Can Edit' permission.",
            run_id,
        )
        try:
            from genie_space_optimizer.optimization.snapshot_contract import (
                capture_pre_ag_snapshot,
            )
            _fallback_baseline = capture_pre_ag_snapshot(
                w=w, space_id=space_id, ag_id="run_baseline"
            )
        except Exception:
            logger.exception(
                "Run-level baseline snapshot fallback fetch failed; downstream "
                "rollback verification will fail terminally."
            )
            _fallback_baseline = None
    # Tracks ``best_accuracy`` as carried into the *previous* iteration
    # (i.e. before that iteration's gate ran). Used by the post-hoc
    # baseline-drift diagnostic in ``_run_gate_checks`` to detect
    # iterations where the accepted baseline now appears to have been
    # an outlier. ``None`` means no previous iteration has run yet.
    _prev_iter_pre_accept_baseline: float | None = None

    levers_attempted: list[int] = []
    levers_accepted: list[int] = []
    levers_rolled_back: list[int] = []
    lever_changes: list[dict] = []
    all_failure_trace_ids: list[str] = []
    all_regression_trace_ids: list[str] = []
    all_eval_mlflow_run_ids: list[str] = []
    all_failure_question_ids: list[str] = []
    question_trace_map: dict[str, list[str]] = {}

    # Phase B observability follow-up — function-scope state used to
    # build ``loop_out["phase_b"]`` manifest at lever-loop terminate.
    # See `docs/2026-05-02-unified-trace-and-operator-transcript-plan.md`
    # postmortem follow-up. ``_phase_b_iter_record_counts`` /
    # ``_phase_b_iter_violation_counts`` /
    # ``_phase_b_no_records_iterations`` / ``_phase_b_artifact_paths``
    # accumulate one entry per iteration. ``_phase_b_producer_exceptions``
    # is a roll-up across iterations (per-iteration counter is built
    # adjacent to ``_current_iter_inputs``).
    from genie_space_optimizer.optimization.decision_emitters import (
        PHASE_B_CONTRACT_VERSION as _PHASE_B_CONTRACT_VERSION,
    )
    _phase_b_iter_record_counts: list[int] = []
    _phase_b_iter_violation_counts: list[int] = []
    _phase_b_no_records_iterations: list[int] = []
    _phase_b_artifact_paths: list[str] = []
    _phase_b_producer_exceptions: dict[str, int] = {}
    _phase_b_target_qids_missing_count: int = 0
    _phase_b_total_violations: int = 0

    # Cycle 9 T5: cross-iteration AG constraints (forbid_tables on
    # blast-radius drops). The strategist's prompt-renderer will surface
    # these in a future task; for now, the structured dict on
    # ``metadata_snapshot["_strategist_constraints"]`` is observable in
    # the replay fixture and MLflow tags.
    from genie_space_optimizer.optimization.strategist_constraints import (
        StrategistConstraints,
    )
    _strategist_constraints: StrategistConstraints = StrategistConstraints()

    # Stamp the contract version on the MLflow run so the postmortem
    # analyzer can tell "deploy is stale" (no tag) from "deploy is
    # current but produced 0 records" (tag present, manifest shows zero).
    try:
        import mlflow as _mlflow_phase_b_init  # type: ignore[import-not-found]
        if _mlflow_phase_b_init.active_run() is not None:
            _mlflow_phase_b_init.set_tag(
                "phase_b_contract_version", _PHASE_B_CONTRACT_VERSION
            )
    except Exception:
        logger.debug(
            "Phase B contract version tag set skipped (non-fatal)",
            exc_info=True,
        )

    _human_sql_fixes = [
        {"question": c.get("question", ""), "new_expected_sql": c["corrected_sql"], "verdict": "genie_correct"}
        for c in (human_corrections or [])
        if c.get("type") == "benchmark_correction" and c.get("corrected_sql")
    ]

    _judge_overrides = [c for c in (human_corrections or []) if c.get("type") == "judge_override"]
    for ov in _judge_overrides:
        try:
            feedback = ov.get("feedback", "")
            if "Genie answer is actually fine" in feedback or "Correct" in feedback:
                genie_sql = _extract_genie_sql_from_trace(ov.get("trace_id", ""))
                if genie_sql:
                    _human_sql_fixes.append({
                        "question": ov.get("question", ""),
                        "new_expected_sql": genie_sql,
                        "verdict": "genie_correct",
                    })
            elif "both answers are wrong" in feedback or "Both Wrong" in feedback:
                from genie_space_optimizer.optimization.benchmarks import quarantine_benchmark_question
                quarantine_benchmark_question(
                    spark, f"{catalog}.{schema}", domain,
                    ov.get("question", "") or ov.get("question_id", ""),
                    reason="both_wrong",
                )
            elif "Ambiguous" in feedback:
                from genie_space_optimizer.optimization.benchmarks import quarantine_benchmark_question
                quarantine_benchmark_question(
                    spark, f"{catalog}.{schema}", domain,
                    ov.get("question", "") or ov.get("question_id", ""),
                    reason="ambiguous",
                )
        except Exception:
            logger.warning("Failed to process judge_override feedback", exc_info=True)

    if _human_sql_fixes:
        try:
            from genie_space_optimizer.optimization.benchmarks import apply_benchmark_corrections
            _hfix = apply_benchmark_corrections(_human_sql_fixes, spark, f"{catalog}.{schema}", domain)
            print(
                f"\n[Human Feedback] Applied {_hfix['applied']} benchmark corrections "
                f"from prior review (skipped {_hfix['skipped']})"
            )
        except Exception:
            logger.warning("Failed to apply human benchmark corrections", exc_info=True)

    _human_suggestions = [c for c in (human_corrections or []) if c.get("type") == "improvement"]

    _ensure_sql_context(spark, catalog, schema)

    # ── SQL snippet normalize safety net ──────────────────────────────
    # Defense-in-depth: the enrichment stage already ran the repair path,
    # but any intermediate step (instruction prose miner, benchmark miner,
    # manual edits between runs) could have added a snippet with a short-
    # form prefix. This pass is cheap (no-op when everything is already
    # fully-qualified) and guarantees the lever loop only sees
    # ``catalog.schema.table.col`` references — Genie's serving path
    # rejects any other form. If repair rewrote anything, refetch config
    # so the lever loop works against the post-repair state.
    try:
        _parsed_pre_repair = config.get("_parsed_space", config) or {}
        _snippet_repair_result = _repair_existing_sql_snippets(
            w, spark, run_id, space_id, config=config,
            metadata_snapshot=_parsed_pre_repair,
            catalog=catalog, schema=schema,
            warehouse_id=resolve_warehouse_id(""),
        )
        if _snippet_repair_result.get("rewritten", 0) > 0:
            from genie_space_optimizer.common.genie_client import fetch_space_config
            config = fetch_space_config(w, space_id)
    except Exception:
        # Non-fatal: the lever loop will still function against the
        # current config; EXPLAIN errors on individual snippets surface
        # later via the lever-specific validators.
        logger.warning("SQL snippet normalize safety net failed", exc_info=True)

    from genie_space_optimizer.optimization.evaluation import build_metric_view_measures
    _mv_measures = build_metric_view_measures(config)
    _instr_prompt = format_mlflow_template(
        INSTRUCTION_PROMPT_NAME_TEMPLATE,
        uc_schema=f"{catalog}.{schema}", space_id=space_id,
    )
    predict_fn = make_predict_fn(
        w, space_id, spark, catalog, schema,
        metric_view_measures=_mv_measures,
        warehouse_id=resolve_warehouse_id(""),
        optimization_run_id=run_id,
        triggered_by=triggered_by,
        instruction_prompt_name=_instr_prompt,
    )
    _parsed_space = config.get("_parsed_space", config)
    _instr_section = _parsed_space.get("instructions", {}) if isinstance(_parsed_space, dict) else {}
    _instr_text_for_scorers = _instr_section.get("text_instructions", "") if isinstance(_instr_section, dict) else ""
    scorers = make_all_scorers(w, spark, catalog, schema, instruction_context=_instr_text_for_scorers)
    uc_schema = f"{catalog}.{schema}"
    metadata_snapshot = _parsed_space
    data_profile = (
        metadata_snapshot.get("_data_profile", {})
        or config.get("_data_profile", {})
    )

    uc_columns = config.get("_uc_columns", [])
    if uc_columns:
        enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

    enrichment_result: dict = {}
    join_result: dict = {}
    meta_result: dict = {}
    instruction_result: dict = {}

    if not enrichment_done:
        # ── Phase 1: Proactive Enrichment (inline, legacy path) ──
        import mlflow as _mlflow_legacy_enr

        # Tier 4: v2 naming — ``<run_short>/enrichment/inline``.
        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_legacy,
            enrichment_run_name as _enrichment_run_name_legacy,
        )
        _legacy_enr_run_name = _enrichment_run_name_legacy(run_id, detail="inline")
        with _mlflow_legacy_enr.start_run(run_name=_legacy_enr_run_name, nested=True):
            _mlflow_legacy_enr.set_tags({
                **_v2_tags_legacy(run_id, space_id=space_id, stage="enrichment_inline"),
                "genie.space_id": space_id,
                "genie.run_type": "inline_enrichment",
            })

            _pe_lines = [_section("LEVER LOOP — PROACTIVE ENRICHMENT", "-")]
            _pe_lines.append(_kv("Space ID", space_id))
            _pe_lines.append(_kv("UC columns", len(uc_columns)))
            _pe_lines.append(_bar("-"))
            print("\n".join(_pe_lines))

            enrichment_result = _run_description_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if enrichment_result.get("total_enriched", 0) > 0 or enrichment_result.get("tables_enriched", 0) > 0:
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            join_result = _run_proactive_join_discovery(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if join_result.get("total_applied", 0) > 0:
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            meta_result = _run_space_metadata_enrichment(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if meta_result.get("description_generated") or meta_result.get("questions_generated"):
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            # ── Instruction prose mining & promotion (miner-first) ───────
            # Mirrors the primary ``_run_enrichment`` path (Task C.5) so
            # direct-lever-loop invocations with ``enrichment_done=False``
            # see the same prose-normalisation behaviour as the job-level
            # entry point. The miner runs BEFORE proactive seed/expand.
            _legacy_miner_out = _run_instruction_prose_mining(
                w, spark, run_id, space_id, config, metadata_snapshot,
                catalog, schema,
                warehouse_id=resolve_warehouse_id(""),
                benchmarks=benchmarks,
            )
            if _legacy_miner_out["total_applied"] or _legacy_miner_out["keep_in_prose_count"]:
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            instruction_result = _run_proactive_instruction_seeding(
                w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
            )
            if (
                instruction_result.get("instructions_seeded")
                or instruction_result.get("instructions_expanded")
            ):
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            # ── Pre-flight example_sql synthesis (legacy path) ───────────
            # Fills example_question_sqls to target. Idempotent on re-runs.
            if ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS:
                try:
                    from genie_space_optimizer.optimization.preflight_synthesis import (
                        run_preflight_example_synthesis,
                    )
                    legacy_preflight_result = run_preflight_example_synthesis(
                        w, spark, run_id, space_id, config, metadata_snapshot,
                        benchmarks=benchmarks,
                        catalog=catalog, schema=schema,
                        warehouse_id=resolve_warehouse_id(""),
                    )
                    if legacy_preflight_result.get("applied", 0) > 0:
                        from genie_space_optimizer.common.genie_client import fetch_space_config
                        config = fetch_space_config(w, space_id)
                        config["_uc_columns"] = uc_columns
                        metadata_snapshot = config.get("_parsed_space", config)
                        metadata_snapshot["_data_profile"] = data_profile
                        if uc_columns:
                            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)
                except Exception:
                    logger.warning(
                        "preflight example synthesis (legacy path) raised; continuing",
                        exc_info=True,
                    )

            # ── SQL Expression REPAIR + SEEDING (legacy path) ────────────
            # Repair runs unconditionally; seeding is headroom-gated.
            # Mining source gate mirrors the primary path
            # (_run_enrichment): only arbiter-approved (``both_correct``)
            # baseline rows contribute. See _extract_arbiter_approved_benchmarks
            # for the verdict-scoping rationale.
            _legacy_approved, _legacy_verdicts = _extract_arbiter_approved_benchmarks(
                spark, run_id, catalog, schema, benchmarks,
            )
            logger.info(
                "miner.arbiter_filter path=legacy total=%d approved=%d verdicts=%s",
                len(benchmarks), len(_legacy_approved), _legacy_verdicts,
            )
            sql_expr_result = _run_sql_expression_seeding(
                w, spark, run_id, space_id, config=config,
                metadata_snapshot=metadata_snapshot,
                benchmarks=_legacy_approved,
                catalog=catalog, schema=schema,
                warehouse_id=resolve_warehouse_id(""),
            )
            if (
                sql_expr_result.get("total_seeded", 0) > 0
                or sql_expr_result.get("repair", {}).get("rewritten", 0) > 0
            ):
                from genie_space_optimizer.common.genie_client import fetch_space_config
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                metadata_snapshot = config.get("_parsed_space", config)
                metadata_snapshot["_data_profile"] = data_profile
                if uc_columns:
                    enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

            _enr_summary = [_section("PROACTIVE ENRICHMENT — SUMMARY", "-")]
            _enr_summary.append(_kv("Descriptions enriched", enrichment_result.get("total_enriched", 0)))
            _enr_summary.append(_kv("Joins discovered", join_result.get("total_applied", 0)))
            _enr_summary.append(_kv("Space metadata", "description=%s, questions=%s" % (
                "generated" if meta_result.get("description_generated") else "unchanged",
                "generated" if meta_result.get("questions_generated") else "unchanged",
            )))
            _enr_summary.append(_kv("Instructions seeded", "yes" if instruction_result.get("instructions_seeded") else "no"))
            _enr_summary.append(_kv("SQL expressions seeded", sql_expr_result.get("total_seeded", 0)))
            _enr_summary.append(_bar("-"))
            print("\n".join(_enr_summary))

            _mlflow_legacy_enr.log_metrics({
                "enrichment.columns_enriched": enrichment_result.get("total_enriched", 0),
                "enrichment.tables_enriched": enrichment_result.get("tables_enriched", 0),
                "enrichment.joins_discovered": join_result.get("total_applied", 0),
                "enrichment.sql_expressions_seeded": sql_expr_result.get("total_seeded", 0),
            })
    else:
        # Enrichment already handled by the enrichment task -- reload fresh
        # config from API (enrichment patches are already applied).
        from genie_space_optimizer.common.genie_client import fetch_space_config
        from genie_space_optimizer.common.uc_metadata import (
            extract_genie_space_table_refs,
            get_columns_for_tables_rest,
        )
        config = fetch_space_config(w, space_id)
        table_refs = extract_genie_space_table_refs(config)
        uc_columns = get_columns_for_tables_rest(w, table_refs) if table_refs else []
        config["_uc_columns"] = uc_columns
        metadata_snapshot = config.get("_parsed_space", config)
        metadata_snapshot["_data_profile"] = data_profile
        if uc_columns:
            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)
        if enrichment_model_id:
            prev_model_id = enrichment_model_id
        print("\n".join([
            _section("LEVER LOOP — ENRICHMENT ALREADY DONE", "-"),
            _kv("Enrichment model", enrichment_model_id or "(none)"),
            _kv("Config loaded from", "Genie Space API (post-enrichment)"),
            _bar("-"),
        ]))

    # ── Phase 1.5: Restructure unstructured instructions ──────────
    # If existing instructions lack ALL-CAPS section headers, classify
    # them into canonical sections via LLM so downstream section-level
    # merges are safe.  The restructured text is persisted to the Genie
    # Space so all lever iterations operate on structured input.
    try:
        from genie_space_optimizer.optimization.applier import (
            _get_general_instructions,
            _set_general_instructions,
        )
        from genie_space_optimizer.optimization.optimizer import (
            _is_unstructured,
            _pre_structure_instructions,
            normalize_instructions,
        )

        _current_instr = _get_general_instructions(metadata_snapshot)
        if _current_instr and _current_instr.strip() and _is_unstructured(_current_instr):
            logger.info(
                "Existing instructions are unstructured (%d chars) "
                "— restructuring into canonical sections",
                len(_current_instr),
            )
            _restructured_secs = _pre_structure_instructions(
                _current_instr, metadata_snapshot, w=w,
            )
            if _restructured_secs:
                parts: list[str] = []
                from genie_space_optimizer.common.config import INSTRUCTION_SECTION_ORDER
                for _sec in INSTRUCTION_SECTION_ORDER:
                    _lines_list = _restructured_secs.get(_sec, [])
                    if not _lines_list:
                        continue
                    parts.append(f"{_sec}:")
                    for _ln in _lines_list:
                        _s = _ln.strip()
                        if not _s:
                            continue
                        if not _s.startswith("- "):
                            _s = f"- {_s}"
                        parts.append(_s)
                    parts.append("")
                _restructured_text = normalize_instructions("\n".join(parts).strip())

                if len(_restructured_text.strip()) >= len(_current_instr.strip()) * 0.5:
                    _set_general_instructions(metadata_snapshot, _restructured_text)
                    try:
                        from genie_space_optimizer.common.genie_client import (
                            fetch_space_config,
                            patch_space_config,
                        )
                        patch_space_config(w, space_id, metadata_snapshot)
                        # Re-fetch to get server-canonical state after PATCH.
                        # Without this, metadata_snapshot diverges from the API's
                        # version and all subsequent lever PATCHes fail with
                        # "Space configuration has been modified since this export".
                        config = fetch_space_config(w, space_id)
                        config["_uc_columns"] = uc_columns
                        metadata_snapshot = config.get("_parsed_space", config)
                        metadata_snapshot["_data_profile"] = data_profile
                        if uc_columns:
                            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)
                        logger.info(
                            "Persisted restructured instructions (%d chars, %d sections)",
                            len(_restructured_text), len(_restructured_secs),
                        )
                        write_patch(
                            spark, run_id, 0, 0, 0,
                            {
                                "patch_type": "instruction_restructure",
                                "scope": "genie_config",
                                "risk_level": "low",
                                "target_object": "instructions.text_instructions",
                                "patch": {"instructions": _restructured_text[:200] + "..."},
                                "command": None,
                                "rollback": None,
                                "proposal_id": "instruction_restructure",
                            },
                            catalog, schema,
                        )
                    except Exception:
                        logger.warning(
                            "Instruction restructure: PATCH to Genie Space failed",
                            exc_info=True,
                        )
                else:
                    logger.warning(
                        "Restructured text too short (%d chars vs %d original) "
                        "— skipping persist",
                        len(_restructured_text), len(_current_instr),
                    )

            print("\n".join([
                _section("INSTRUCTION RESTRUCTURING", "-"),
                _kv("Original format", "unstructured"),
                _kv("Sections detected", ", ".join(_restructured_secs.keys()) if _restructured_secs else "none"),
                _bar("-"),
            ]))
        else:
            logger.debug("Instructions already structured or empty — no restructuring needed")
    except Exception:
        logger.warning("Instruction restructuring failed — continuing with existing format", exc_info=True)

    # ── Phase 1.6: Snapshot user-authored instruction sections ────────
    # Capture the instruction sections AFTER restructuring but BEFORE any
    # lever patches.  This snapshot is the user's ground truth — the
    # optimizer must never generate content that contradicts it.
    _original_instruction_sections: dict[str, list[str]] = {}
    try:
        from genie_space_optimizer.optimization.applier import _get_general_instructions
        from genie_space_optimizer.optimization.optimizer import _ensure_structured
        _pre_loop_instr = _get_general_instructions(metadata_snapshot)
        if _pre_loop_instr and _pre_loop_instr.strip():
            _original_instruction_sections = _ensure_structured(
                _pre_loop_instr, metadata_snapshot, w=w,
            )
            metadata_snapshot["_original_instruction_sections"] = _original_instruction_sections
            logger.info(
                "Snapshotted %d user-authored instruction section(s): %s",
                len(_original_instruction_sections),
                list(_original_instruction_sections.keys()),
            )
    except Exception:
        logger.warning("Could not snapshot original instruction sections", exc_info=True)

    # ── Phase 2: Pre-Loop Setup ──
    _pls_lines = [_section("LEVER LOOP — PRE-LOOP SETUP", "-")]
    print("\n".join(_pls_lines))

    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    reference_sqls: dict[str, str] = {}
    reference_result_hashes: dict[str, str] = {}
    if baseline_iter:
        rows_json = baseline_iter.get("rows_json")
        if isinstance(rows_json, list):
            _rows_payload = {"rows": rows_json}
            reference_sqls = extract_reference_sqls(_rows_payload)
            reference_result_hashes = extract_reference_result_hashes(_rows_payload)
        elif isinstance(rows_json, str):
            try:
                _rows_payload = {"rows": json.loads(rows_json)}
                reference_sqls = extract_reference_sqls(_rows_payload)
                reference_result_hashes = extract_reference_result_hashes(_rows_payload)
            except (json.JSONDecodeError, TypeError):
                pass
    logger.info(
        "Lever loop: %d reference SQLs, %d result hashes from baseline",
        len(reference_sqls),
        len(reference_result_hashes),
    )

    # ── Per-question cross-iteration arbiter corrections ─────────────
    _correction_state: dict[str, set[str]] = {
        "corrected_qids": set(),
        "repaired_qids": set(),
        "quarantined_qids": set(),
    }
    _pre_loop_corr = _run_arbiter_corrections(
        w, spark, run_id, catalog, schema, domain,
        already_corrected=_correction_state["corrected_qids"],
        already_repaired=_correction_state["repaired_qids"],
        quarantined_qids=_correction_state["quarantined_qids"],
        data_profile=metadata_snapshot.get("_data_profile"),
    )
    _correction_state["corrected_qids"] = _pre_loop_corr["corrected_qids"]
    _correction_state["quarantined_qids"] = _pre_loop_corr["quarantined_qids"]

    # Bug #4 — benchmark verbatim mining removed from pre-loop setup.
    # Example SQLs are now proposed only via AFS-gated structural synthesis
    # during the lever loop, never by copying benchmark expected_sql.
    mined_example_proposals: list = []

    _setup_lines = [_section("PRE-LOOP SETUP — COMPLETE", "-")]
    _setup_lines.append(_kv("Reference SQLs", len(reference_sqls)))
    _setup_lines.append(_kv("Reference hashes", len(reference_result_hashes)))
    _setup_lines.append(_kv("Arbiter corrections", len(_pre_loop_corr.get("corrected_qids", set()))))
    _setup_lines.append(_kv("Mined examples", len(mined_example_proposals)))
    # S10 — the old ``Starting lever: 0`` label implied the loop would
    # skip to some lever, which is false (see comment at start_lever
    # initialisation). Replace with a human resume state.
    _resume_display = (
        f"Resuming after lever {start_lever}" if start_lever else "Starting fresh"
    )
    _setup_lines.append(_kv("Resume state", _resume_display))
    _setup_lines.append(_kv("Iteration counter", _iteration_label(iteration_counter)))
    _setup_lines.append(_kv("Baseline accuracy", f"{baseline_accuracy:.1f}%"))
    _setup_lines.append(_bar("-"))
    print("\n".join(_setup_lines))

    # ── Phase 3: Adaptive Lever Loop ──
    _loop_lines = [_section("LEVER LOOP — ADAPTIVE ITERATION", "-")]
    _loop_lines.append(_kv("Max iterations", max_iterations))
    _loop_lines.append(_kv("Lever order", levers))
    _loop_lines.append(_bar("-"))
    print("\n".join(_loop_lines))

    # ═══════════════════════════════════════════════════════════════════
    # ADAPTIVE LEVER LOOP
    # Re-cluster → priority score → strategist (1 AG) → apply → gate
    # → accept/rollback → reflect … repeat
    # ═══════════════════════════════════════════════════════════════════
    ags_attempted: list[str] = []
    ags_accepted: list[str] = []
    ags_rolled_back: list[str] = []
    escalated_gt_repair_qids: set[str] = set()
    noise_floor = min(100.0 / max(len(benchmarks), 1), MAX_NOISE_FLOOR)

    reflection_buffer: list[dict] = resume_state.get("reflection_buffer", [])
    skill_exemplars: list[dict] = resume_state.get("skill_exemplars", [])
    tried_patches: set[tuple[str, str]] = resume_state.get("tried_patches", set())
    tried_root_causes: set[tuple[str, str]] = resume_state.get("tried_root_causes", set())
    # Task 8: cluster signatures escalated to human review within this
    # run. Accumulated across iterations and excluded from clustering
    # so the strategist never sees a signature it has already failed
    # to address twice.
    human_required_signatures: set[str] = set(
        resume_state.get("human_required_signatures", set()) or set()
    )
    prev_failure_qids: set[str] = set()
    # Phase A — track the prior iteration's passing qid set so the
    # post_eval emit can stamp was/is/transition. Initialised to the
    # baseline's passing set so iteration 1's ``was`` reflects pre-loop
    # state.
    _prev_passing_qids: set[str] = set()
    # Phase A — per-iteration input snapshots for the replay-fixture
    # exporter. End-of-run, this list is unconditionally serialized via
    # ``serialize_replay_fixture`` and emitted to stderr (between
    # PHASE_A_REPLAY_FIXTURE_JSON_BEGIN/END markers) plus MLflow when an
    # active run exists.
    _replay_fixture_iterations: list[dict] = []

    # Phase A — deterministic carrier for the most recent full-eval
    # result. Replaces opportunistic ``locals().get("full_result")``
    # reads in the eval-entry / post-eval / validator blocks below.
    # The eval-entry block at iteration N uses iteration N-1's eval
    # result (or this baseline-derived seed for N=1) so the replay
    # fixture has real ``eval_rows`` even on the first iteration.
    # Inside ``_run_lever_loop`` the only outer-loop ``full_result =``
    # assignment is the post-acceptance line below; the carrier is
    # refreshed there. Rolled-back iterations deliberately do NOT
    # update the carrier because the live Genie state reverts to the
    # last accepted (or seeded) baseline.
    _latest_eval_result: dict[str, Any] = {}
    try:
        # Centralised baseline-seed helper: extracts rows_json, dedups by
        # qid, and returns a carrier-shaped dict. Returns {} for any
        # unusable input so this branch falls through to the next-gate
        # refresh path. Same helper is reused at iteration start as a
        # snapshot fallback, so iterations that short-circuit before
        # `_run_gate_checks` (applier-skip-eval, dead-on-arrival retry)
        # still capture the baseline state in the replay fixture.
        _baseline_rows_seed = _rows_from_iteration_payload(baseline_iter)
        if not _baseline_rows_seed:
            logger.warning(
                "Phase A: baseline payload yielded 0 extractable eval rows "
                "(_latest_eval_result will rely on first gate result instead "
                "of baseline seed). baseline_iter keys=%s",
                sorted((baseline_iter or {}).keys()) if isinstance(baseline_iter, dict) else type(baseline_iter).__name__,
            )
        _seeded = _seed_eval_result_from_baseline_iter(baseline_iter)
        if _seeded:
            _latest_eval_result = _seeded
            logger.info(
                "Phase A: seeded _latest_eval_result from baseline "
                "(%d qids, %d failures)",
                len(_seeded.get("question_ids") or []),
                len(_seeded.get("failure_question_ids") or []),
            )
        elif _baseline_rows_seed:
            # Rows extracted but none had a question_id/id key — the seed
            # call walked them and produced 0 qids. Without this branch the
            # absence of "Phase A: seeded ..." is silent and operators
            # cannot tell the difference between "no rows" and "rows but
            # no identifiers". Both states leave the carrier empty.
            logger.warning(
                "Phase A: baseline payload had %d rows but 0 carried any of "
                "the recognised qid keys (question_id, id, client_request_id, "
                "request_id, inputs/question_id) — _latest_eval_result stays "
                "empty. Add the actual qid key to _baseline_row_qid. Sample "
                "row keys=%s",
                len(_baseline_rows_seed),
                sorted((_baseline_rows_seed[0] or {}).keys())[:20]
                if isinstance(_baseline_rows_seed[0], dict)
                else type(_baseline_rows_seed[0]).__name__,
            )
    except Exception:
        # Promoted from debug → warning: a silent seed failure here was the
        # root cause of three wasted real-Genie cycles (see Phase A burn-down
        # log). The carrier is now also refreshed at every gate-checks site
        # so a failed seed is no longer fatal for the replay fixture, but we
        # still want operators to SEE the failure rather than discover it via
        # an empty fixture two hours later.
        logger.warning(
            "Phase A: failed to seed _latest_eval_result from baseline "
            "(non-fatal — carrier will be populated on first gate result)",
            exc_info=True,
        )
    _verdict_history: dict[str, list] = {}
    _last_full_mlflow_run_id: str = baseline_iter.get("mlflow_run_id", "") if baseline_iter else ""

    # Phase 1.3: pending action groups buffer.  When the strategist
    # returns multiple action_groups, only AG[0] is consumed in the
    # current iteration; remaining AGs are buffered for the next
    # iteration so we don't need a fresh (and expensive) strategist
    # round-trip to address the next-priority cluster.  Each buffered
    # AG is re-validated against the current cluster set before reuse;
    # if its source clusters have been resolved or the schema state has
    # diverged, the buffer is drained and the strategist is re-called.
    # Toggle via ``GSO_PROCESS_ALL_AGS=0`` to fall back to the legacy
    # AG[0]-only behavior.
    _process_all_ags = os.getenv(
        "GSO_PROCESS_ALL_AGS", "1",
    ).strip().lower() not in ("0", "false", "no", "off")
    _MAX_AGS_PER_STRATEGIST_CALL = int(
        os.getenv("GSO_MAX_AGS_PER_STRATEGIST_CALL", "3"),
    )
    pending_action_groups: list[dict] = []
    pending_strategy: dict | None = None
    diagnostic_action_queue: list[dict] = []

    _dead_on_arrival_patch_signatures: set[tuple[str, ...]] = set()
    _dead_on_arrival_ag_ids: set[str] = set()
    # Cycle 2 Task 3: ledger keyed on selected-proposal-ID signatures.
    # Closes the iter-3/iter-5 same-AG replay loop in run
    # 2afb0be2-88b6-4832-99aa-c7e78fbc90f7 where blast-radius drops
    # every patch and the applied-patch signature collapses to ``()``.
    _doa_selected_proposal_signatures: dict[str, set[tuple[str, ...]]] = {}

    # Track exclusion counts for the objective-complete check. These start
    # from the current baseline iteration and update each accepted candidate.
    from genie_space_optimizer.optimization.evaluation import (
        objective_blocking_exclusion_count as _objective_blocking_exclusion_count,
    )
    _best_total_questions = len(benchmarks) if benchmarks else 0
    _best_evaluated_count = int(
        (baseline_iter or {}).get("evaluated_count")
        or _best_total_questions
    )
    _best_blocking_excluded_count = 0
    if baseline_iter:
        try:
            _best_blocking_excluded_count = _objective_blocking_exclusion_count(
                _rows_from_iteration_payload(baseline_iter)
            )
        except Exception:
            _best_blocking_excluded_count = 0

    strategist_memo_cache: dict[str, dict[str, Any]] = {}

    # PR-B2: bookkeeping for the convergence-marker exit reason. Each
    # break site overwrites one of these; the marker emit at the bottom
    # of the function reads them through _resolve_lever_loop_exit_reason
    # so the marker `reason` matches the human-readable termination print.
    _lever_loop_plateau_decision = None
    _lever_loop_divergence_label: str | None = None
    _lever_loop_retired_ags: list[tuple[str, tuple[str, ...]]] = []

    for _iter_num in range(1, max_iterations + 1):
        # ── Exit checks ──────────────────────────────────────────────
        from genie_space_optimizer.optimization.acceptance_policy import (
            arbiter_objective_complete_from_counts,
        )

        if arbiter_objective_complete_from_counts(
            post_arbiter_accuracy=float(best_accuracy),
            total_questions=_best_total_questions,
            evaluated_count=_best_evaluated_count,
            blocking_excluded_count=_best_blocking_excluded_count,
        ):
            logger.info(
                "Post-arbiter objective reached: %.1f%% over %d/%d scored rows with no blocking exclusions. Stopping lever loop.",
                float(best_accuracy),
                int(_best_evaluated_count),
                int(_best_total_questions),
            )
            break
        if all_thresholds_met(best_scores, thresholds):
            logger.info(
                "Thresholds met before iteration %d, but lever-loop objective is not complete; continuing toward 100%% post-arbiter accuracy.",
                _iter_num,
            )
        from genie_space_optimizer.optimization.rca_terminal import (
            RcaTerminalDecision as _RcaTerminalDecision,
            RcaTerminalStatus as _RcaTerminalStatus,
            legacy_plateau_allows_stop,
        )

        _plateau_detected = _diminishing_returns(reflection_buffer)
        _prev_terminal_state = metadata_snapshot.get("_rca_terminal_state") or {}
        _prev_terminal_decision: _RcaTerminalDecision | None
        if _prev_terminal_state:
            try:
                _prev_terminal_decision = _RcaTerminalDecision(
                    status=_RcaTerminalStatus(
                        _prev_terminal_state.get("status")
                        or _RcaTerminalStatus.PATCHABLE_IN_PROGRESS.value
                    ),
                    should_continue=bool(
                        _prev_terminal_state.get("should_continue", True)
                    ),
                    reason=str(_prev_terminal_state.get("reason") or ""),
                )
            except Exception:
                _prev_terminal_decision = None
        else:
            _prev_terminal_decision = None

        if legacy_plateau_allows_stop(
            plateau_detected=_plateau_detected,
            terminal_decision=_prev_terminal_decision,
        ):
            logger.info(
                "Diminishing returns confirmed by RCA terminal state at iteration %d",
                _iter_num,
            )
            # Task 6 — replace the legacy unknown plateau label with a
            # typed status that distinguishes "hard failures still
            # quarantined", "open regression debt", and "clean plateau".
            from genie_space_optimizer.optimization.rca_terminal import (
                resolve_terminal_on_plateau,
            )
            from genie_space_optimizer.optimization.control_plane import (
                hard_failure_qids as _hard_failure_qids_for_plateau,
            )

            # Task 20 — read the latest committed state iteration (fully
            # accepts both 'full' and 'enrichment' eval scopes) so the
            # plateau resolver sees the live row set rather than a stale
            # ``full_result`` snapshot from before recent rollbacks.
            _state_iter = load_latest_state_iteration(
                spark, run_id, catalog, schema,
            ) or {}
            _plateau_rows: list[dict] = []
            try:
                _plateau_rows = list(_state_iter.get("rows") or [])
            except Exception:
                _plateau_rows = []
            _current_hard_qids = set(
                _hard_failure_qids_for_plateau(_plateau_rows)
            )
            _regression_debt_qids = set(
                _correction_state.get("regression_debt_qids", set()) or set()
            )
            _quarantined_qids = set(
                _correction_state.get("quarantined_qids", set()) or set()
            )
            # Task 20 — collect target qids for which any rejected AG
            # produced an SQL-shape delta. The resolver routes these to
            # UNRESOLVED_HARD_FAILURE_WITH_UNTRIED_SQL_DELTA so the loop
            # keeps iterating instead of declaring a clean plateau.
            _sql_delta_qids: set[str] = set()
            for _rb in reflection_buffer:
                for _delta in _rb.get("sql_shape_deltas", []) or []:
                    _qid = str(_delta.get("target_qid") or "")
                    if _qid and (_delta.get("remaining") or _delta.get("improved")):
                        _sql_delta_qids.add(_qid)

            # Track G — combine the buffered + diagnostic queues so the
            # plateau resolver can refuse to terminate while either
            # queue still covers a live hard qid.
            _pending_diag_ags_for_plateau = (
                list(pending_action_groups) + list(diagnostic_action_queue)
            )
            _resolved = resolve_terminal_on_plateau(
                quarantined_qids=_quarantined_qids,
                current_hard_qids=_current_hard_qids,
                regression_debt_qids=_regression_debt_qids,
                sql_delta_qids=_sql_delta_qids,
                pending_diagnostic_ags=_pending_diag_ags_for_plateau,
            )
            logger.info(
                "Plateau terminal at iter %d: status=%s reason=%s "
                "(hard=%d quarantined=%d debt=%d sql_delta=%d)",
                _iter_num, _resolved.status.value, _resolved.reason,
                len(_current_hard_qids), len(_quarantined_qids),
                len(_regression_debt_qids), len(_sql_delta_qids),
            )
            if _resolved.should_continue:
                logger.info(
                    "Plateau suppressed because RCA terminal status is %s "
                    "(reason=%s)",
                    _resolved.status.value,
                    _resolved.reason,
                )
                continue
            # PR-B2: capture the resolver decision so the convergence marker
            # at the bottom of the function reports the same typed status
            # the human-readable print below shows. Also accumulate retired
            # AGs (Task 5 emits one DecisionRecord per entry).
            _lever_loop_plateau_decision = _resolved
            _lever_loop_retired_ags.extend(_resolved.retired_ags)
            print(
                _section("LEVER LOOP — TERMINATION: plateau", "!") + "\n"
                + _kv("Reason", _resolved.reason) + "\n"
                + _kv("RCA terminal status", _resolved.status.value) + "\n"
                + _kv("Iteration", _iteration_label(_iter_num)) + "\n"
                + _kv("Retired AGs", str(len(_resolved.retired_ags))) + "\n"
                + _bar("!")
            )
            # Phase F+H A6 (v2): F9 learning — post-stage observability
            # with atomic dedup. resolve_terminal_on_plateau at
            # harness.py:11856 STAYS inline; this stage call emits
            # AG_RETIRED records via _emit_ag_retired_records
            # (stages/learning.py:114-142), replacing the inline
            # AG_RETIRED list-comp + extend block (formerly here).
            #
            # F9.update() also re-calls resolve_terminal_on_plateau
            # internally. PURE — verified by grep of rca_terminal.py
            # for mlflow./spark./global; zero matches. Per-iteration
            # re-call is byte-stable.
            #
            # The stage emits AG_RETIRED records with the same field
            # shape as the deleted inline block (run_id, iteration,
            # decision_type=AG_RETIRED, outcome=RETIRED, reason_code=
            # AG_TARGET_NO_LONGER_HARD, ag_id, target_qids,
            # affected_qids, reason_detail) — see stages/learning.py:
            # 114-142 for the producer body.
            #
            # iteration: the inline emit used _iter_num (the outer
            # loop variable). v2 uses _iter_num too (NOT
            # iteration_counter — they align at this site but
            # _iter_num is the literal source-of-truth the inline
            # emit referenced).
            #
            # Verified against: stages/learning.py:38-51 (Input),
            # 142-217 (update body), 54-62 (LearningUpdate),
            # 114-142 (_emit_ag_retired_records).
            try:
                from genie_space_optimizer.optimization.stages import (
                    StageContext as _StageCtx,
                )
                from genie_space_optimizer.optimization.stages import (
                    learning as _lrn_stage,
                )

                _stage_ctx_a6 = _StageCtx(
                    run_id=str(run_id),
                    iteration=int(_iter_num),
                    space_id=str(space_id),
                    domain=str(domain),
                    catalog=str(catalog),
                    schema=str(schema),
                    apply_mode=str(apply_mode),
                    journey_emit=_journey_emit,
                    decision_emit=_decision_emit,
                    mlflow_anchor_run_id=_phase_h_anchor_run_id,  # C17 v2 — activates Phase B capture
                    feature_flags={},
                )

                # Source do_not_retry / rolled_back from
                # metadata_snapshot per the harness's existing
                # thread-through pattern.
                _prior_do_not_retry = set(
                    metadata_snapshot.get("_do_not_retry_signatures") or set()
                )
                _prior_rolled_back = set(
                    metadata_snapshot.get(
                        "_rolled_back_content_fingerprints"
                    ) or set()
                )

                # Build AG-outcomes-by-id from F8's _ag_outcome (A5).
                # If A5 hasn't landed, _ag_outcome is undefined here;
                # fall back to {} so F9.update() emits AG_RETIRED
                # records based purely on _resolved.retired_ags
                # (which matches the deleted inline block's behavior).
                try:
                    _f9_ag_outcomes_by_id = {
                        ag_id: {
                            "outcome": rec.outcome,
                            "reason_code": rec.reason_code,
                            "content_fingerprint": ";".join(
                                rec.content_fingerprints
                            ),
                            "target_qids": list(rec.target_qids),
                        }
                        for ag_id, rec in _ag_outcome.outcomes_by_ag.items()
                    }
                except NameError:
                    _f9_ag_outcomes_by_id = {}

                _lrn_inp = _lrn_stage.LearningInput(
                    prior_reflection_buffer=tuple(reflection_buffer),
                    prior_do_not_retry=_prior_do_not_retry,
                    prior_rolled_back_content_fingerprints=_prior_rolled_back,
                    ag_outcomes_by_id=_f9_ag_outcomes_by_id,
                    applied_signature="",
                    accuracy_delta=float(full_accuracy - best_accuracy),
                    # ✅ FIX (audit Section 4 A6 #2): current_hard_
                    # failure_qids is the LIVE hard qid set, sourced
                    # from harness.py:11830-11832, NOT
                    # _resolved.retired_ags (different concept).
                    current_hard_failure_qids=tuple(_current_hard_qids),
                    regression_debt_qids=set(_regression_debt_qids),
                    quarantined_qids=set(_quarantined_qids),
                    sql_delta_qids=set(_sql_delta_qids),
                    pending_buffered_ags=tuple(pending_action_groups),
                    diagnostic_action_queue=tuple(diagnostic_action_queue),
                )
                # Phase F+H Commit B16: wrap F9 with stage_io_capture
                # decorator. Replay-byte-stable — wrap_with_io_capture
                # returns the stage output unchanged; MLflow log_text
                # calls are no-ops while mlflow_anchor_run_id is None
                # (C17 wires the anchor on real runs).
                from genie_space_optimizer.optimization.stage_io_capture import (
                    wrap_with_io_capture as _wrap_with_io_capture_a6,
                )
                _lrn_wrapped = _wrap_with_io_capture_a6(
                    execute=_lrn_stage.execute,
                    stage_key="learning_next_action",
                )
                _lrn_update = _lrn_wrapped(_stage_ctx_a6, _lrn_inp)
                # _lrn_update.retired_ags / .ag_retired_records are
                # observability surfaces; the AG_RETIRED records were
                # emitted inside update() via ctx.decision_emit which
                # routes through _decision_emit to
                # _current_iter_inputs["decision_records"].
                #
                # _lrn_update.new_reflection_buffer /
                # new_do_not_retry / new_rolled_back_content_
                # fingerprints are observability-only here: the
                # harness's existing reflection-buffer / do-not-retry
                # update path is unchanged and remains the canonical
                # mutation point.
            except Exception:
                _iter_producer_exceptions.setdefault("ag_retired", 0)
                _iter_producer_exceptions["ag_retired"] += 1
                _phase_b_producer_exceptions["ag_retired"] = (
                    _phase_b_producer_exceptions.get("ag_retired", 0) + 1
                )
                logger.debug(
                    "Phase F+H A6 v2: learning stage failed (non-fatal)",
                    exc_info=True,
                )
            break
        if (
            _plateau_detected
            and _prev_terminal_decision is not None
            and _prev_terminal_decision.should_continue
        ):
            logger.info(
                "Legacy plateau suppressed at iteration %d because RCA terminal state is %s",
                _iter_num,
                _prev_terminal_decision.status.value,
            )
            print(
                _section("LEGACY PLATEAU SUPPRESSED", "-") + "\n"
                + _kv("RCA terminal status", _prev_terminal_decision.status.value) + "\n"
                + _kv("Reason", _prev_terminal_decision.reason) + "\n"
                + _kv("Contract", "continue until 100% post-arbiter accuracy or max_iterations") + "\n"
                + _bar("-")
            )
        _diverging, _div_rationale = _detect_divergence(reflection_buffer)
        if _diverging:
            logger.info(
                "Divergence detected at iteration %d: %s",
                _iter_num, _div_rationale,
            )
            # PR-B2: capture the divergence label so the convergence marker
            # at the bottom of the function reports the typed reason.
            _lever_loop_divergence_label = f"divergence_{_div_rationale}"
            print(
                _section("LEVER LOOP — TERMINATION: divergence", "!") + "\n"
                + _kv("Reason", _div_rationale) + "\n"
                + _kv("Iteration", _iteration_label(_iter_num)) + "\n"
                + _bar("!")
            )
            break
        # Phase C3: only CONTENT_REGRESSION rollbacks count toward the
        # consecutive-rollback limit. INFRA / SCHEMA / escalation / OTHER
        # rollbacks carry no content signal and have their own handling
        # (infra retry budget, schema fatal exit, escalation_handled).
        from genie_space_optimizer.optimization.rollback_class import (
            RollbackClass as _RC,
        )
        _consecutive_rb = 0
        for _rb_entry in reversed(reflection_buffer):
            if _rb_entry.get("escalation_handled"):
                continue
            if _rb_entry.get("accepted"):
                break
            if _rb_entry.get("rollback_class") == _RC.CONTENT_REGRESSION.value:
                _consecutive_rb += 1
            else:
                # Non-content rollback (infra/schema/other). Skip without
                # counting; don't break, so we can still see a
                # CONTENT_REGRESSION further back in the buffer.
                continue
        if _consecutive_rb >= CONSECUTIVE_ROLLBACK_LIMIT:
            logger.info(
                "Consecutive content-rollback limit (%d) reached — stopping at iteration %d",
                CONSECUTIVE_ROLLBACK_LIMIT, _iter_num,
            )
            break

        _consecutive_esc = 0
        _last_esc_type: str | None = None
        for _esc_entry in reversed(reflection_buffer):
            if not _esc_entry.get("escalation_handled"):
                break
            _esc_reason = _esc_entry.get("rollback_reason", "")
            if _last_esc_type is None:
                _last_esc_type = _esc_reason
            if _esc_reason == _last_esc_type:
                _consecutive_esc += 1
            else:
                break
        if _consecutive_esc >= CONSECUTIVE_ESCALATION_LIMIT:
            logger.info(
                "Consecutive escalation limit (%d) reached for '%s' — "
                "stopping at iteration %d",
                CONSECUTIVE_ESCALATION_LIMIT, _last_esc_type, _iter_num,
            )
            write_stage(
                spark, run_id, "LEVER_LOOP_ESCALATION_EXIT", "COMPLETE",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "consecutive_escalations": _consecutive_esc,
                    "escalation_type": _last_esc_type,
                },
                catalog=catalog, schema=schema,
            )
            break

        iteration_counter += 1

        # Phase A — append-on-begin: allocate the per-iteration snapshot
        # AND register it in ``_replay_fixture_iterations`` immediately,
        # so any subsequent ``continue`` / ``break`` (rollback paths,
        # cap drops, diagnostic-AG paths, plateau exits, etc.) cannot
        # silently drop this iteration from the replay fixture.
        # Subsequent code mutates this dict in place; the list entry is
        # the same reference, so mutations are reflected automatically.
        from genie_space_optimizer.optimization.journey_fixture_exporter import (
            begin_iteration_capture as _begin_iteration_capture,
        )
        _current_iter_inputs: dict = _begin_iteration_capture(
            iterations_data=_replay_fixture_iterations,
            iteration=iteration_counter,
        )

        # Cycle 5 T1 — productive-iteration budget accounting locals.
        # All three are populated unconditionally because their cost is
        # negligible; they are READ only when
        # ``productive_iteration_budget_enabled()`` is true (Option A:
        # gate emission, not capture). With the flag off the locals are
        # set but never consumed → zero behaviour change, zero new
        # decision records, byte-stable replay. The most-recent typed
        # P4 reason at the SKIPPING / NO_APPLIED sites is sourced from
        # ``_current_iter_inputs["decision_records"]`` which the harness
        # already accumulates per iteration.
        _iter_consumed: bool = True
        _iter_no_op_cause: str = ""
        _iter_applied_count: int = 0

        # Cycle 5 T2 — per-iteration accumulator for gate-drops carrying
        # a causal-target patch. Populated unconditionally at the
        # blast-radius drop site (capture is cheap memory; no behaviour
        # change). Consumed at iteration end to refresh the outer-scope
        # ``_prior_iteration_dropped_causal_patches`` list, which the
        # next iteration's ``ActionGroupsInput`` reads — gated by
        # ``GSO_CAUSAL_DROP_FEEDBACK_TO_STRATEGIST`` so byte-stability
        # holds with the flag off.
        _iter_dropped_causal: list = []

        # Cycle 6 F-1 — per-iteration emit-dedup set for Cycle 5 records
        # (iteration_budget_decision, soft_cluster_drift_recovered,
        # rca_regeneration_*). Each Cycle 5 emit site checks
        # ``_emit_idempotency_key(rec)`` against this set before
        # emitting and skips on hit. Reset at every iteration body
        # entry so cross-iteration repeats (intentional) still flow.
        _iter_emitted_keys: set[tuple] = set()

        # P3 task 4 — drain any structural-synthesis proposals queued
        # at the prior iteration's lever-5 drop site. Same-iteration
        # injection (below at the drop site) is the active path, so
        # this list is empty in steady state; the drain runs each
        # iteration for hygiene and to surface anything carried over
        # for replay observability.
        _forced_synthesis_proposals_carryover = (
            _consume_structural_synthesis_buffer(_structural_synthesis_buffer)
        )
        if _forced_synthesis_proposals_carryover:
            logger.info(
                "P3: drained %d carry-over structural-synthesis proposal(s) "
                "from prior iteration",
                len(_forced_synthesis_proposals_carryover),
            )
            _current_iter_inputs.setdefault(
                "carryover_structural_proposals", []
            ).extend(_forced_synthesis_proposals_carryover)

        # ── Per-question journey ledger accumulator (Task 13) ────────
        # Stamp every stage that touches a question so the end-of-
        # iteration ledger can reconstruct each qid's full timeline.
        from genie_space_optimizer.optimization.question_journey import (
            QuestionJourneyEvent as _JourneyEvent,
            build_question_journey_ledger as _build_journey_ledger,
            render_question_journey_once as _render_journey_once,
        )
        _journey_events: list[_JourneyEvent] = []
        _journey_render_state: dict[str, bool] = {"rendered": False}

        def _journey_emit(stage: str, **fields):
            """Append journey event(s); fail-safe for any caller."""
            try:
                qids = fields.pop("question_ids", None)
                qid = fields.pop("question_id", None)
                target_qids = list(qids) if qids else (
                    [qid] if qid else []
                )
                for q in target_qids:
                    qstr = str(q).strip()
                    if not qstr:
                        continue
                    _journey_events.append(_JourneyEvent(
                        question_id=qstr, stage=stage, **fields,
                    ))
            except Exception:
                logger.debug(
                    "journey_emit failed (non-fatal) stage=%s", stage,
                    exc_info=True,
                )

        def _decision_emit(record):
            """v2 Pre-Task 0.5: pin the iteration-body decision-emit closure.

            The harness historically appends decision records via direct
            calls to ``_current_iter_inputs.setdefault("decision_records",
            []).append(record.to_dict())`` (~17 sites in this iteration
            body). v2 introduces this closure with the SAME shape so every
            Phase A wire-up routes ``ctx.decision_emit(record)`` through
            one place.

            Contract: ``record`` is a typed DecisionRecord (has
            .to_dict()) OR a plain dict. On exception: log debug + swallow
            (matches _journey_emit shape).
            """
            try:
                rec_dict = (
                    record.to_dict() if hasattr(record, "to_dict")
                    else dict(record)
                )
                _current_iter_inputs.setdefault(
                    "decision_records", []
                ).append(rec_dict)
            except Exception:
                logger.debug(
                    "decision_emit failed (non-fatal)", exc_info=True,
                )

        def _render_current_journey() -> None:
            """Render this AG iteration's journey ledger exactly once."""
            try:
                _render_journey_once(
                    events=_journey_events,
                    iteration=iteration_counter,
                    render_state=_journey_render_state,
                )
            except Exception:
                logger.debug(
                    "Task 13: journey ledger render failed (non-fatal)",
                    exc_info=True,
                )

        # ── 3B.1b: Per-iteration arbiter corrections ─────────────────
        _iter_corr = _run_arbiter_corrections(
            w, spark, run_id, catalog, schema, domain,
            already_corrected=_correction_state["corrected_qids"],
            already_repaired=_correction_state["repaired_qids"],
            quarantined_qids=_correction_state["quarantined_qids"],
            data_profile=metadata_snapshot.get("_data_profile"),
        )
        _correction_state["corrected_qids"] = _iter_corr["corrected_qids"]
        _correction_state["quarantined_qids"] = _iter_corr["quarantined_qids"]

        metadata_snapshot["_regression_rca_findings"] = []
        metadata_snapshot["_regression_mining_hints"] = ""
        try:
            if reflection_buffer:
                _mining_context = _collect_regression_mining_iteration_context(
                    reflection_buffer,
                    enable_rca_ledger=ENABLE_REGRESSION_MINING_RCA_LEDGER,
                    enable_strategist_hints=ENABLE_REGRESSION_MINING_STRATEGIST,
                    min_confidence=REGRESSION_MINING_STRATEGIST_MIN_CONFIDENCE,
                )
                metadata_snapshot["_regression_rca_findings"] = (
                    _mining_context["rca_findings"]
                )
                metadata_snapshot["_regression_mining_hints"] = (
                    _mining_context["strategist_hints"]
                )
        except Exception:
            logger.debug(
                "Failed to convert regression-mining insights to RCA findings",
                exc_info=True,
            )

        # ── 3B.2: Re-cluster from latest eval ────────────────────────
        _analysis = _analyze_and_distribute(
            spark, run_id, catalog, schema, metadata_snapshot,
            iteration_counter - 1, lever_label=0,
            quarantined_qids=_correction_state["quarantined_qids"],
            exclude_qids=escalated_gt_repair_qids,
        )
        clusters = _analysis["all_clusters"]
        soft_signal_clusters = _analysis["soft_signal_clusters"]
        rca_ledger = _analysis.get("rca_ledger") or {}
        # Track H — same row source the soft pile was built from. Pinned
        # to the analyze-distribute return so the soft-cluster currency
        # check sees the exact rows the clusterer saw.
        _analysis_failure_rows = _analysis.get("failure_rows") or []

        # Phase A — Defensive carrier seed at iteration start. The
        # primary seed (lever-loop pre-loop block) populates
        # `_latest_eval_result` from `baseline_iter`, and the per-gate
        # refresh (`_extract_eval_result_from_gate`) keeps it fresh on
        # every accept/rollback. But two skip-eval `continue` paths
        # bypass the gate entirely:
        #   * applier blast-radius gate dropped all patches
        #     ("deterministic_no_applied_patches" → SKIP EVAL: NO
        #     APPLIED PATCHES)
        #   * dead-on-arrival AG retry blocked (same selected patch
        #     IDs already produced no applied patches)
        # When every iteration in a run takes one of those paths, the
        # carrier never refreshes and — if the seed silently produced
        # 0 qids — the replay fixture's `eval_rows` end up empty for
        # every iteration. Lazy-seed here as a last line of defence so
        # the snapshot below always has at least the baseline state.
        if not (_latest_eval_result or {}).get("question_ids"):
            try:
                _lazy_seed = _seed_eval_result_from_baseline_iter(
                    baseline_iter
                )
                if _lazy_seed:
                    _latest_eval_result = _lazy_seed
                    logger.warning(
                        "Phase A: lazy-seeded _latest_eval_result from "
                        "baseline at iteration_counter=%d (carrier was "
                        "empty — primary seed produced 0 qids and no "
                        "gate result has refreshed it yet). %d qids, "
                        "%d failures.",
                        iteration_counter,
                        len(_lazy_seed.get("question_ids") or []),
                        len(_lazy_seed.get("failure_question_ids") or []),
                    )
            except Exception:
                logger.debug(
                    "Phase A: lazy baseline seed failed (non-fatal)",
                    exc_info=True,
                )

        # Phase A — Lossless contract: stamp the eval-entry events for
        # every qid that entered this iteration's eval. This eliminates
        # the validator's missing_qid violations (one per qid) and the
        # illegal-transition violations that would otherwise arise from
        # qids that begin their journey at 'clustered' or 'soft_signal'
        # without a preceding 'evaluated' event.
        try:
            _eval_qids_for_entry = list(
                (_latest_eval_result or {}).get("question_ids") or []
            )
            _hard_qid_set = {
                str(q)
                for c in (clusters or [])
                for q in (c.get("question_ids") or [])
                if q
            }
            _soft_qid_set = {
                str(q)
                for c in (soft_signal_clusters or [])
                for q in (c.get("question_ids") or [])
                if q
            } - _hard_qid_set
            _gt_corr_qid_set = {
                str(c.get("question_id") or "")
                for c in (_analysis.get("gt_correction_candidates") or [])
                if c.get("question_id")
            }
            _all_classified = _hard_qid_set | _soft_qid_set | _gt_corr_qid_set
            _already_passing_set = (
                {str(q) for q in _eval_qids_for_entry if q}
                - _all_classified
            )
            _emit_eval_entry_journey(
                emit=_journey_emit,
                eval_qids=_eval_qids_for_entry,
                already_passing_qids=sorted(_already_passing_set),
                hard_qids=sorted(_hard_qid_set),
                soft_qids=sorted(_soft_qid_set),
                gt_correction_qids=sorted(_gt_corr_qid_set),
            )
        except Exception:
            logger.debug(
                "Phase A: eval-entry journey emit failed (non-fatal)",
                exc_info=True,
            )

        # Phase B observability follow-up — initialize per-iteration
        # producer-exception counter and shared lookup maps used by the
        # 5 typed-record producers wired below.
        _iter_producer_exceptions: dict[str, int] = {
            "eval_classification": 0,
            "cluster": 0,
            "rca_formed": 0,
            "strategist_ag": 0,
            "ag_outcome": 0,
            "post_eval_resolution": 0,
            "proposal_generated": 0,
            "patch_applied": 0,
        }
        _iter_classification: dict[str, str] = {}
        for _q in _already_passing_set:
            _iter_classification[str(_q)] = "already_passing"
        for _q in _hard_qid_set:
            _iter_classification[str(_q)] = "hard"
        for _q in _soft_qid_set:
            _iter_classification[str(_q)] = "soft"
        for _q in _gt_corr_qid_set:
            _iter_classification[str(_q)] = "gt_correction"
        _iter_cluster_by_qid: dict[str, str] = {}
        _iter_source_clusters_by_id: dict[str, dict] = {}
        for _c in (clusters or []):
            _cid = str(_c.get("cluster_id") or "")
            if _cid:
                _iter_source_clusters_by_id[_cid] = _c
            for _q in (_c.get("question_ids") or []):
                _qstr = str(_q)
                if _qstr and _cid:
                    _iter_cluster_by_qid[_qstr] = _cid
        # Phase B delta Task 1: derive {cluster_id: rca_id} from the
        # iteration's RCA findings so every producer (cluster_records,
        # strategist_ag_records, ag_outcome_decision_record,
        # post_eval_resolution_records, blast_radius_decision_records,
        # dead_on_arrival_decision_records) stamps a real rca_id on
        # every record. Empty when no findings overlap a cluster's
        # question_ids; the validator's per-decision_type rules treat
        # missing rca_id as a violation, which is the desired loud
        # signal for that case.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                rca_id_by_cluster_from_findings,
            )
            from genie_space_optimizer.optimization.rca import (
                rca_findings_from_clusters,
            )
            _iter_rca_id_by_cluster: dict[str, str] = (
                rca_id_by_cluster_from_findings(
                    clusters=clusters or [],
                    findings=rca_findings_from_clusters(clusters or []),
                )
            )
        except Exception:
            logger.debug(
                "Phase B delta Task 1: rca_id_by_cluster derivation failed (non-fatal)",
                exc_info=True,
            )
            _iter_rca_id_by_cluster = {}

        # Phase B observability follow-up — closure that builds an
        # ACCEPTANCE_DECIDED record from an AG and outcome string and
        # stashes it on the iteration snapshot. Captures the iteration
        # scope (clusters, lookups, exception counter) so the 5 outcome
        # sites in the harness can call it inline with one line each.
        def _phase_b_emit_ag_outcome_record(_ag_obj, _outcome_str):
            try:
                from genie_space_optimizer.optimization.decision_emitters import (
                    ag_outcome_decision_record as _ag_outcome_decision_record,
                    is_strict_mode as _phase_b_strict_mode_inner,
                )

                _ag_rec = _ag_outcome_decision_record(
                    run_id=run_id,
                    iteration=iteration_counter,
                    ag=_ag_obj,
                    outcome=str(_outcome_str or ""),
                    source_clusters_by_id=_iter_source_clusters_by_id,
                    rca_id_by_cluster=_iter_rca_id_by_cluster,
                )
                if _ag_rec is not None:
                    _current_iter_inputs.setdefault(
                        "decision_records", []
                    ).append(_ag_rec.to_dict())
            except Exception:
                _iter_producer_exceptions["ag_outcome"] += 1
                _phase_b_producer_exceptions["ag_outcome"] = (
                    _phase_b_producer_exceptions.get("ag_outcome", 0) + 1
                )
                logger.debug(
                    "Phase B: ag_outcome_decision_record failed (non-fatal)",
                    exc_info=True,
                )
                if _phase_b_strict_mode_inner():
                    raise

        # Phase B observability follow-up — emit EVAL_CLASSIFIED records
        # (one per qid). Even when no patches reach the cap this
        # iteration, this gives the analyzer 24+ records per iter so
        # ``decision_records_total > 0`` and the trace is observable.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                eval_classification_records as _eval_classification_records,
                is_strict_mode as _phase_b_strict_mode,
            )

            _eval_records = _eval_classification_records(
                run_id=run_id,
                iteration=iteration_counter,
                eval_qids=_eval_qids_for_entry,
                classification=_iter_classification,
                cluster_by_qid=_iter_cluster_by_qid,
            )
            _current_iter_inputs.setdefault("decision_records", []).extend(
                [r.to_dict() for r in _eval_records]
            )
        except Exception as _exc_eval:
            _iter_producer_exceptions["eval_classification"] += 1
            _phase_b_producer_exceptions["eval_classification"] = (
                _phase_b_producer_exceptions.get("eval_classification", 0) + 1
            )
            logger.debug(
                "Phase B: eval_classification_records failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Phase B observability follow-up — emit CLUSTER_SELECTED
        # records (one per hard cluster). Note: clusters list at this
        # site is the hard-cluster list; soft clusters are captured
        # separately via the journey ``soft_signal`` events.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                cluster_records as _cluster_records,
            )

            # Phase D.5 Task 5: capture cluster alternatives.
            # ``clusters`` is the hard-only list at this site; without a
            # local ``candidate_clusters`` collection, the fallback below
            # produces empty alternatives (byte-stable). When a future
            # cycle wires the candidate list (demoted + hard) into a
            # local, pass it as ``candidate_clusters`` here.
            _candidate_clusters_for_alts = (
                _candidate_clusters_for_decision_trace
                if "_candidate_clusters_for_decision_trace" in locals()
                else (clusters or [])
            )
            _cluster_alts_by_id = _build_cluster_alternatives_by_id(
                candidate_clusters=_candidate_clusters_for_alts,
                promoted_cluster_ids=[
                    str(c.get("cluster_id") or "")
                    for c in (clusters or [])
                ],
            )
            _hard_cluster_records = _cluster_records(
                run_id=run_id,
                iteration=iteration_counter,
                clusters=clusters or [],
                rca_id_by_cluster=_iter_rca_id_by_cluster,
                cluster_alternatives_by_id=_cluster_alts_by_id,
            )
            _current_iter_inputs.setdefault("decision_records", []).extend(
                [r.to_dict() for r in _hard_cluster_records]
            )
        except Exception:
            _iter_producer_exceptions["cluster"] += 1
            _phase_b_producer_exceptions["cluster"] = (
                _phase_b_producer_exceptions.get("cluster", 0) + 1
            )
            logger.debug(
                "Phase B: cluster_records failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Phase B delta Task 3 — emit RCA_FORMED records (one per
        # cluster routed to an RCA card). Closes the gap between
        # CLUSTER_SELECTED and STRATEGIST_AG_EMITTED in the decision
        # trace.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                rca_formed_records as _rca_formed_records,
            )

            _rca_formed = _rca_formed_records(
                run_id=run_id,
                iteration=iteration_counter,
                clusters=clusters or [],
                rca_id_by_cluster=_iter_rca_id_by_cluster,
            )
            _current_iter_inputs.setdefault("decision_records", []).extend(
                [r.to_dict() for r in _rca_formed]
            )
        except Exception:
            _iter_producer_exceptions["rca_formed"] += 1
            _phase_b_producer_exceptions["rca_formed"] = (
                _phase_b_producer_exceptions.get("rca_formed", 0) + 1
            )
            logger.debug(
                "Phase B: rca_formed_records failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Phase C Task 7 — emit RCA_FORMED+UNRESOLVED+RCA_UNGROUNDED
        # for clusters with hard failures but no matching RCA finding.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                unresolved_rca_records as _unresolved_rca_records,
            )

            _unresolved_records = _unresolved_rca_records(
                run_id=run_id,
                iteration=iteration_counter,
                clusters=clusters or [],
                rca_id_by_cluster=_iter_rca_id_by_cluster,
            )
            _current_iter_inputs.setdefault("decision_records", []).extend(
                [r.to_dict() for r in _unresolved_records]
            )
        except Exception:
            _phase_b_producer_exceptions["unresolved_rca"] = (
                _phase_b_producer_exceptions.get("unresolved_rca", 0) + 1
            )
            logger.debug(
                "Phase C: unresolved_rca_records failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Phase A — populate replay-fixture iteration snapshot fields
        # eval_rows / clusters / soft_clusters from the analysis result.
        try:
            _fr = _latest_eval_result or {}
            _scores = _fr.get("scores") or {}
            _arbiter_map = _fr.get("arbiter_verdicts") or {}
            _failure_set = {str(q) for q in (_fr.get("failure_question_ids") or [])}
            _fixture_eval_rows: list[dict] = []
            for _qid in (_eval_qids_for_entry or []):
                _qstr = str(_qid)
                _correctness: str
                if isinstance(_scores, dict) and _qstr in _scores:
                    _v = _scores[_qstr]
                    _correctness = "yes" if str(_v).lower() in ("yes", "true", "1", "pass") else "no"
                else:
                    _correctness = "no" if _qstr in _failure_set else "yes"
                _row: dict = {"question_id": _qstr, "result_correctness": _correctness}
                if isinstance(_arbiter_map, dict) and _qstr in _arbiter_map:
                    _row["arbiter"] = str(_arbiter_map[_qstr])
                _fixture_eval_rows.append(_row)
            _current_iter_inputs["eval_rows"] = _fixture_eval_rows
            _current_iter_inputs["clusters"] = [
                {
                    "cluster_id": str(c.get("cluster_id") or ""),
                    "root_cause": str(c.get("root_cause") or ""),
                    "question_ids": [str(q) for q in (c.get("question_ids") or []) if q],
                }
                for c in (clusters or [])
            ]
            _current_iter_inputs["soft_clusters"] = [
                {
                    "cluster_id": str(c.get("cluster_id") or ""),
                    "root_cause": str(c.get("root_cause") or ""),
                    "question_ids": [str(q) for q in (c.get("question_ids") or []) if q],
                }
                for c in (soft_signal_clusters or [])
            ]
        except Exception:
            logger.debug(
                "Phase A: replay-fixture iteration capture failed (non-fatal)",
                exc_info=True,
            )

        # Task 16 — scale max_iterations by initial hard cluster count.
        # Computed once on the first iteration (when ``clusters`` first
        # binds) and held for the rest of the run.
        if _iter_num == 1:
            _scaled_max_iterations = compute_iteration_budget(
                hard_cluster_count=len(clusters or []),
                requested_max_iterations=max_iterations or MAX_ITERATIONS,
            )
            if _scaled_max_iterations != max_iterations:
                logger.info(
                    "Iteration budget set to %d (hard_clusters=%d, requested=%d)",
                    _scaled_max_iterations,
                    len(clusters or []),
                    int(max_iterations or MAX_ITERATIONS),
                )
            else:
                logger.info(
                    "Iteration budget set to %d (hard_clusters=%d)",
                    _scaled_max_iterations,
                    len(clusters or []),
                )
            max_iterations = _scaled_max_iterations

        # Task 13 — emit ``clustered`` events per qid in each hard cluster
        # and ``soft_signal`` events for soft clusters.
        # Plan N1 Task 2 — delegate to ``emit_cluster_membership_events``
        # so a qid that appears in multiple clusters produces exactly
        # one event per stage. Multi-cluster membership is preserved on
        # ``extra.additional_cluster_ids``. Closes the trunk-repeat
        # ``soft_signal -> soft_signal`` defect on 2afb0be2 retry
        # attempt 993610879088298.
        try:
            from genie_space_optimizer.optimization.question_journey import (
                emit_cluster_membership_events,
            )
            emit_cluster_membership_events(
                journey_emit=_journey_emit,
                hard_clusters=list(clusters or []),
                soft_clusters=list(soft_signal_clusters or []),
            )
        except Exception:
            logger.debug(
                "Task 13: cluster journey emit failed (non-fatal)",
                exc_info=True,
            )
        metadata_snapshot["_rca_ledger"] = rca_ledger
        try:
            from genie_space_optimizer.optimization.rca import (
                themes_for_strategy_context,
            )

            metadata_snapshot["_rca_themes"] = themes_for_strategy_context(
                list(rca_ledger.get("themes") or []),
                enable_selection=ENABLE_RCA_THEME_SELECTION,
                max_themes=RCA_MAX_THEMES_PER_ITERATION,
                max_patches=RCA_MAX_THEME_PATCHES_PER_ITERATION,
            )
        except Exception:
            logger.debug(
                "RCA theme selection failed; falling back to all themes",
                exc_info=True,
            )
            metadata_snapshot["_rca_themes"] = rca_ledger.get("themes") or []
        metadata_snapshot["_rca_theme_conflicts"] = (
            rca_ledger.get("conflicts") or []
        )

        try:
            from genie_space_optimizer.optimization.rca_execution import (
                build_rca_execution_plans,
            )

            metadata_snapshot["_rca_execution_plans"] = build_rca_execution_plans(
                metadata_snapshot.get("_rca_themes") or []
            )
        except Exception:
            logger.debug(
                "RCA execution plan construction failed; continuing without forced RCA levers",
                exc_info=True,
            )
            metadata_snapshot["_rca_execution_plans"] = []

        try:
            from genie_space_optimizer.optimization.rca_terminal import (
                classify_terminal_state,
            )

            _terminal_decision = classify_terminal_state(
                post_arbiter_accuracy=float(best_accuracy),
                max_iterations=int(max_iterations),
                iteration_counter=int(iteration_counter),
                actionable_plan_count=len(
                    metadata_snapshot.get("_rca_execution_plans") or []
                ),
                repeated_failure_count=sum(
                    1 for r in reflection_buffer
                    if not r.get("accepted")
                ),
                judge_failure_count=sum(
                    1 for r in reflection_buffer
                    if r.get("rollback_reason") == "judge_unreliable"
                ),
                benchmark_issue_count=sum(
                    1 for r in reflection_buffer
                    if r.get("rollback_reason") == "benchmark_broken"
                ),
                unpatchable_count=sum(
                    1 for r in reflection_buffer
                    if r.get("rollback_reason") == "unpatchable_with_six_levers"
                ),
            )
            metadata_snapshot["_rca_terminal_state"] = {
                "status": _terminal_decision.status.value,
                "should_continue": _terminal_decision.should_continue,
                "reason": _terminal_decision.reason,
            }
        except Exception:
            logger.debug("RCA terminal-state classification failed", exc_info=True)

        try:
            write_asi_results(spark, run_id, iteration_counter - 1, _analysis["asi_rows"], catalog, schema, mlflow_run_id=_last_full_mlflow_run_id)
        except Exception:
            logger.debug("Failed to write ASI results", exc_info=True)
        try:
            write_provenance(spark, run_id, iteration_counter - 1, 0, _analysis["prov_rows"], catalog, schema)
        except Exception:
            logger.debug("Failed to write provenance rows", exc_info=True)
        try:
            # Task 1: persist GT correction queue payloads. Empty list
            # is a no-op inside the helper.
            write_gt_correction_candidates(
                spark,
                _analysis.get("gt_correction_candidates") or [],
                catalog=catalog,
                schema=schema,
            )
        except Exception:
            logger.debug("Failed to write GT correction candidates", exc_info=True)

        # Task 8: detect cluster signatures that have hit the
        # persistent-failure threshold across this run's reflection
        # buffer, persist them, and exclude them from the strategist's
        # input. Pure helper — fail-open on any error so escalation
        # bookkeeping never blocks the loop.
        try:
            from genie_space_optimizer.optimization.persistent_failure_escalation import (
                case_to_delta_row as _t8_to_row,
                compute_human_required_escalations as _t8_compute,
            )

            _t8_cases, _t8_new_sigs = _t8_compute(
                reflection_buffer,
                run_id=run_id,
                already_escalated_signatures=human_required_signatures,
            )
            if _t8_cases:
                # Persist + emit a per-case audit row (Task 3 stage O).
                try:
                    from genie_space_optimizer.optimization.state import (
                        write_human_required_escalations as _t8_write,
                        write_lever_loop_decisions as _t8_audit,
                    )
                    _t8_write(
                        spark,
                        [_t8_to_row(c) for c in _t8_cases],
                        catalog=catalog,
                        schema=schema,
                    )
                    _t8_audit_rows = []
                    for _idx, _c in enumerate(_t8_cases, start=1):
                        _t8_audit_rows.append({
                            "run_id": run_id,
                            "iteration": iteration_counter,
                            "ag_id": None,
                            "decision_order": _idx,
                            "stage_letter": "O",
                            "gate_name": "persistent_failure_escalation",
                            "decision": "escalated",
                            "reason_code": _c.reason_code,
                            "reason_detail": (
                                f"signature={_c.cluster_signature} "
                                f"attempts={_c.attempt_count} "
                                f"qid={_c.question_id or '(sentinel)'}"
                            )[:2000],
                            "affected_qids": (
                                [_c.question_id] if _c.question_id else []
                            ),
                            "source_cluster_ids": [_c.cluster_signature],
                            "metrics": {
                                "attempt_count": _c.attempt_count,
                                "last_iteration": _c.last_iteration,
                                "root_cause": _c.root_cause,
                                **(_c.evidence or {}),
                            },
                        })
                    if _t8_audit_rows:
                        _t8_audit(
                            spark, _t8_audit_rows,
                            catalog=catalog, schema=schema,
                        )
                except Exception:
                    logger.debug(
                        "Task 8 persistence failed (non-fatal)",
                        exc_info=True,
                    )
                logger.info(
                    "Task 8: escalated %d cluster signature(s) to "
                    "human review: %s",
                    len(_t8_new_sigs),
                    ", ".join(sorted(_t8_new_sigs)),
                )
            human_required_signatures |= _t8_new_sigs
        except Exception:
            logger.debug(
                "Task 8 escalation computation failed (non-fatal)",
                exc_info=True,
            )

        # Drop clusters whose signature is in the human-required set
        # so the strategist does not see them. Symmetric to the
        # ``_filter_tried_clusters`` exclusion.
        if human_required_signatures:
            _pre_hard = len(clusters)
            _pre_soft = len(soft_signal_clusters)
            clusters = [
                c for c in clusters
                if not (
                    c.get("cluster_signature")
                    and c["cluster_signature"] in human_required_signatures
                )
            ]
            soft_signal_clusters = [
                c for c in soft_signal_clusters
                if not (
                    c.get("cluster_signature")
                    and c["cluster_signature"] in human_required_signatures
                )
            ]
            _dropped_hard = _pre_hard - len(clusters)
            _dropped_soft = _pre_soft - len(soft_signal_clusters)
            if _dropped_hard or _dropped_soft:
                logger.info(
                    "Task 8: dropped %d hard + %d soft cluster(s) whose "
                    "signature is in the human-required set",
                    _dropped_hard, _dropped_soft,
                )

        clusters = _filter_tried_clusters(clusters, tried_root_causes)
        if not clusters and not soft_signal_clusters:
            logger.info("No actionable clusters remain — stopping at iteration %d", _iter_num)
            break

        # Track H — quarantine attribution audit. The strategist must
        # never receive a quarantine that includes a currently-passing
        # qid (attribution drift) or a singleton-hard qid (the only
        # remaining target). Both invariants raise on violation so the
        # loop stops with a clear traceback rather than silently
        # soft-skipping the wrong qid.
        from genie_space_optimizer.optimization.control_plane import (
            assert_quarantine_attribution_sound,
        )

        _quarantined_for_audit = {
            str(q)
            for q in _correction_state.get("quarantined_qids", set()) or set()
            if str(q)
        }
        _all_eval_qids_for_audit = {
            str(q)
            for q in (_latest_eval_result or {}).get("question_ids") or []
            if str(q)
        }
        # Hard clusters list every currently-failing qid; the complement
        # against the universe is the currently-passing set.
        _live_hard_for_audit = {
            str(q)
            for cluster in (clusters or [])
            for q in cluster.get("question_ids") or []
            if str(q)
        }
        _live_passing_for_audit = _all_eval_qids_for_audit - _live_hard_for_audit

        assert_quarantine_attribution_sound(
            quarantined_qids=_quarantined_for_audit,
            currently_passing_qids=_live_passing_for_audit,
            currently_hard_qids=_live_hard_for_audit,
        )

        # ── Cluster-driven synthesis iteration-scoped state ──────────
        # Stamp clusters on the snapshot so
        # ``_resolve_source_cluster_for_ag`` (optimizer.py) can look up
        # source clusters by id for Lever 5 intercept. Reset the shared
        # per-iteration budget counter + stamp the active space_id so
        # the P2 arbiter gate can call Genie (both per Bug #4 Phase 3
        # Invariants B and C).
        metadata_snapshot["_failure_clusters"] = clusters
        metadata_snapshot["_cluster_synthesis_count"] = 0
        metadata_snapshot["_space_id"] = space_id

        if metadata_snapshot.get("_regression_mining_hints"):
            logger.info(
                "Regression-mining strategist hints active for iter %d",
                iteration_counter,
            )

        # ── 3B.3: Priority scoring ───────────────────────────────────
        _scan_levers = (
            set(iq_scan_recommended_levers)
            if iq_scan_recommended_levers and _iq_scan_strategist_enabled()
            else None
        )
        # Tier 2.3: include soft clusters in ranking. cluster_impact applies a
        # 0.5 dampen for signal_type=="soft" so hard clusters still win at
        # equal q_count, while large soft clusters (the response_quality=63%
        # case) can out-rank tiny hard clusters and earn strategist attention.
        for _sc in soft_signal_clusters or []:
            if isinstance(_sc, dict):
                _sc.setdefault("signal_type", "soft")
        from genie_space_optimizer.optimization.control_plane import (
            clusters_for_strategy,
        )

        _strategy_hard_clusters, _strategy_soft_clusters = clusters_for_strategy(
            list(clusters or []),
            list(soft_signal_clusters or []),
        )

        # Track H — soft-cluster currency invariant. Every qid emitted in
        # any soft cluster must, on the *same* rows the clusterer saw,
        # exhibit at least one row where ``has_individual_judge_failure``
        # returns ``True``. If a soft-cluster qid has no such row, the
        # clusterer is reading stale ASI / cached rows that no longer
        # reflect the latest eval. Grounded against
        # ``_analysis["failure_rows"]`` so the assertion sees the exact
        # rows the soft pile was built from (no Delta re-read skew).
        #
        # Cycle 5 T5 — survival fix: instead of raising and aborting
        # the run on drift, drop the drifted qids (or the entire
        # cluster if every qid drifted) and emit a typed
        # SOFT_CLUSTER_DRIFT_RECOVERED decision record. The recovery
        # helper is pure; it returns the cleaned slate plus an audit
        # trail. Closes the run-aborting AssertionError that hit two
        # early task attempts of run 2423b960-16e8-41d4-a0cb-74c563378e05.
        from genie_space_optimizer.optimization.cluster_formation_recovery import (
            recover_from_soft_cluster_drift,
        )
        from genie_space_optimizer.optimization.control_plane import (
            has_individual_judge_failure as _t5_has_jf,
        )

        try:
            _t5_judge_failing = {
                str(_row.get("question_id") or "")
                for _row in (_analysis_failure_rows or [])
                if isinstance(_row, dict)
                and _row.get("question_id")
                and _t5_has_jf(_row)
            }
            _t5_recovery = recover_from_soft_cluster_drift(
                soft_clusters=_strategy_soft_clusters or [],
                judge_failing_qids=_t5_judge_failing,
            )
            if (
                _t5_recovery.drifted_qids_by_cluster
                or _t5_recovery.dropped_cluster_ids
            ):
                # Refresh the soft slate with the cleaned clusters.
                _strategy_soft_clusters = _t5_recovery.recovered_clusters
                # Emit one decision record per affected cluster.
                from genie_space_optimizer.optimization.decision_emitters import (
                    soft_cluster_drift_recovered_record,
                )
                _t5_dropped = set(_t5_recovery.dropped_cluster_ids)
                for _cid, _drifted in (
                    _t5_recovery.drifted_qids_by_cluster.items()
                ):
                    _t5_rec = soft_cluster_drift_recovered_record(
                        run_id=str(run_id),
                        iteration=int(iteration_counter),
                        cluster_id=str(_cid),
                        drifted_qids=_drifted,
                        cluster_dropped=(_cid in _t5_dropped),
                    )
                    # Cycle 6 F-1 — skip duplicate emits within an iteration.
                    _t5_key = _emit_idempotency_key(_t5_rec.to_dict())
                    if _t5_key in _iter_emitted_keys:
                        continue
                    _iter_emitted_keys.add(_t5_key)
                    _decision_emit(_t5_rec)
                    _current_iter_inputs.setdefault(
                        "decision_records", []
                    ).append(_t5_rec.to_dict())
        except Exception:
            logger.debug(
                "Cycle 5 T5: soft-cluster drift recovery failed "
                "(non-fatal); proceeding with original soft slate",
                exc_info=True,
            )

        ranked = rank_clusters(
            list(_strategy_hard_clusters) + list(_strategy_soft_clusters),
            recommended_levers=_scan_levers,
            # T2.1: pass reflection buffer so each cluster gains a
            # ``history`` block with prior attempts against its
            # iteration-independent ``cluster_signature``. The
            # strategist can then reason about "we've tried this
            # cluster twice and rolled back both times; consider
            # escalating or picking a different lever".
            reflection_buffer=reflection_buffer,
        )

        # Cycle 2 Task 4 closeout — stamp per-cluster recommended_levers
        # so the strategist's ranking_text surfaces the per-cluster
        # lever hint in the LLM prompt. The IQ-scan space-wide override
        # (``_scan_levers`` passed to ``rank_clusters``) remains the
        # authoritative tiebreaker; this stamp is the per-cluster
        # baseline recommendation single-question shape RCAs depend on.
        from genie_space_optimizer.optimization.stages.action_groups import (
            stamp_recommended_levers_on_clusters,
        )
        ranked = stamp_recommended_levers_on_clusters(ranked)

        # ── 3B.4: Adaptive strategist (1 LLM call → 1 AG) ───────────
        print(_section(f"ADAPTIVE STRATEGIST — Iteration ({_iteration_label(iteration_counter)})", "="))

        _verdict_history = _build_verdict_history(spark, run_id, catalog, schema)

        # ── 3B.3b: Hard-quarantine exhausted questions ────────────────
        if reflection_buffer:
            _, _persist_data = _build_question_persistence_summary(
                _verdict_history, reflection_buffer,
            )
            if not _rollback_state_trusted_for_quarantine:
                logger.warning(
                    "Skipping convergence quarantine because live state is untrusted; "
                    "hard failures must remain visible until rollback verification passes."
                )
                _quarantine_qids: set[str] = set()
                _soft_skip_qids: set[str] = set()
                _persist_data = {}
            _quarantine_qids: set[str] = set()
            # T4.3: temporary quarantine for stuck/worsening questions
            # that haven't hit the hard-quarantine threshold yet. These
            # are excluded from cluster formation for the *current*
            # iteration only — they re-enter next iteration automatically.
            # Prevents a couple of stuck questions from dominating every
            # cluster and blinding the loop to patchable failures.
            _soft_skip_qids: set[str] = set()
            for _pq_id, _pq_info in _persist_data.items():
                _pq_class = _pq_info.get("classification", "")
                _pq_consec = _pq_info.get("max_consecutive", 0)
                _pq_conv = _pq_info.get("convergence_state", "")
                if _pq_class == "ADDITIVE_LEVERS_EXHAUSTED" or (
                    _pq_class == "PERSISTENT" and _pq_consec >= 3
                ):
                    _quarantine_qids.add(_pq_id)
                elif _pq_conv in ("stuck", "worsening") and _pq_consec >= 2:
                    # Not bad enough for hard-quarantine, but bad enough
                    # to not dominate cluster-formation this pass.
                    _soft_skip_qids.add(_pq_id)

            # Iteration-local skip set used ONLY for this pass's
            # cluster-formation call. Hard quarantine persists via
            # ``_quarantine_qids``; soft-skip qids re-enter next iteration.
            _iter_local_skip_qids = _quarantine_qids | _soft_skip_qids
            if _soft_skip_qids:
                logger.info(
                    "T4.3: soft-skipping %d stuck/worsening question(s) "
                    "from this iteration's cluster formation: %s",
                    len(_soft_skip_qids), sorted(_soft_skip_qids),
                )
                print(
                    _section(
                        f"T4.3 CONVERGENCE QUARANTINE — "
                        f"{len(_soft_skip_qids)} qid(s) soft-skipped",
                        "-",
                    ) + "\n"
                    + _kv(
                        "Questions",
                        ", ".join(sorted(_soft_skip_qids))[:200],
                    ) + "\n"
                    + _kv(
                        "Rationale",
                        "iteration-local; not persisted into hard quarantine",
                    ) + "\n"
                    + _bar("-")
                )
                # Task 1 — iteration-local soft skip is consumed by the
                # next ``cluster_failures(...)`` call only. The hard
                # quarantine store ``_correction_state["quarantined_qids"]``
                # below MUST NOT see soft-skip qids: a transiently stuck
                # question that was already fixed by a prior accepted AG
                # would otherwise become permanently quarantined and
                # invisible to the next iteration's failure analysis.
                # Bug observed in production: an AG fixing a question
                # with several percentage-points of accuracy gain was
                # masked when the next iteration soft-skipped the same
                # question and then hard-quarantined it, hiding the
                # fact that the acceptance gate had already taken
                # credit for the fix.
                logger.debug(
                    "Soft-skip qids %s are iteration-local; only "
                    "_quarantine_qids %s flow into the persistent hard "
                    "quarantine state below.",
                    sorted(_soft_skip_qids),
                    sorted(_quarantine_qids),
                )
            if _quarantine_qids:
                _newly_quarantined = _quarantine_qids - _correction_state["quarantined_qids"]
                if _newly_quarantined:
                    logger.info(
                        "Hard-quarantining %d exhausted question(s): %s",
                        len(_newly_quarantined), _newly_quarantined,
                    )
                    _correction_state["quarantined_qids"] |= _newly_quarantined
                    try:
                        from genie_space_optimizer.optimization.labeling import flag_for_human_review
                        _flag_items = []
                        for _hq_id in sorted(_newly_quarantined):
                            _hq_info = _persist_data[_hq_id]
                            _tried_str = "; ".join(
                                f"iter{it}: {pt}" for it, pt in _hq_info.get("patches_tried", [])
                            )
                            _flag_items.append({
                                "question_id": _hq_id,
                                "question_text": _hq_info.get("question_text", ""),
                                "reason": (
                                    f"{_hq_info['classification']}: "
                                    f"failed {_hq_info['fail_count']}/{_hq_info['total_evals']} evals, "
                                    f"{_hq_info['max_consecutive']} consecutive"
                                ),
                                "iterations_failed": _hq_info.get("fail_count", 0),
                                "patches_tried": _tried_str,
                            })
                        if _flag_items:
                            _flagged = flag_for_human_review(
                                spark, run_id, catalog, schema, domain, _flag_items,
                            )
                            print(
                                _section("PERSISTENCE QUARANTINE", "!") + "\n"
                                + _kv("Questions quarantined", len(_newly_quarantined)) + "\n"
                                + _kv("Flagged for human review", _flagged) + "\n"
                                + _bar("!")
                            )
                    except Exception:
                        logger.warning("Failed to flag quarantined questions for human review", exc_info=True)
                # B3.3 — prune both hard and soft clusters using the
                # shared base-qid helper so a quarantined ``_002``
                # excludes ``_002:v2`` / ``_002:v3`` from S001 too.
                # Soft clusters were previously left untouched, which
                # is what let iter-2's S001 still contain the suffixed
                # variants of quarantined base qids.
                _pre_prune_hard_clusters = list(clusters or [])
                for c in list(clusters) + list(soft_signal_clusters or []):
                    c_qids = c.get("question_ids", [])
                    c["question_ids"] = [
                        q for q in c_qids
                        if not _is_quarantined_qid(q, _quarantine_qids)
                    ]
                clusters = [c for c in clusters if c.get("question_ids")]
                soft_signal_clusters = [
                    c for c in (soft_signal_clusters or [])
                    if c.get("question_ids")
                ]
                # Task 5A — quarantine must not silently remove unresolved
                # patchable hard failures and let the loop pivot to soft
                # clusters. Stop for human review when no hard clusters
                # remain; otherwise carry the qids in a diagnostic lane.
                try:
                    from genie_space_optimizer.optimization.control_plane import (
                        decide_quarantine_continuation,
                    )

                    _pre_prune_hard_qids = {
                        str(q)
                        for _c in _pre_prune_hard_clusters
                        for q in (_c.get("question_ids", []) or [])
                        if str(q)
                    }
                    _q_decision = decide_quarantine_continuation(
                        quarantined_qids=set(_quarantine_qids),
                        unresolved_patchable_qids=_pre_prune_hard_qids,
                        hard_cluster_count_after_prune=len(clusters),
                        soft_cluster_count_after_prune=len(soft_signal_clusters or []),
                    )
                    if _q_decision["action"] == "stop_for_human_review":
                        print(
                            _section("QUARANTINE STOP — PATCHABLE HARD FAILURES", "!") + "\n"
                            + _kv("Blocking QIDs", ", ".join(_q_decision["blocking_qids"])) + "\n"
                            + "|  Quarantine removed unresolved hard failures. Stopping instead of pivoting to soft signals.\n"
                            + _bar("!")
                        )
                        logger.warning(
                            "Stopping lever loop because quarantine removed unresolved patchable hard failures: %s",
                            _q_decision["blocking_qids"],
                        )
                        break
                    if _q_decision["action"] == "diagnostic_lane":
                        logger.warning(
                            "Quarantined patchable hard qids remain in diagnostic lane: %s",
                            _q_decision["blocking_qids"],
                        )
                except Exception:
                    logger.debug(
                        "decide_quarantine_continuation failed (non-fatal)",
                        exc_info=True,
                    )
                if not clusters and not soft_signal_clusters:
                    logger.info("All clusters emptied after quarantine — stopping at iteration %d", _iter_num)
                    break

        _total_q = len(benchmarks)
        # Tier 2.4: include soft-cluster questions in the "failing" set when
        # computing passing_q. Previously this only subtracted hard-cluster
        # qids, so the success-summary line fed to the strategist read
        # "N of M pass all judges" while the prompt's own soft_signal_clusters
        # block showed judge failures — the two statements contradicted each
        # other and the strategist would under-prioritise soft-cluster work.
        _hard_qids = {
            q for c in clusters for q in c.get("question_ids", []) if q
        }
        _soft_qids = {
            q for c in (soft_signal_clusters or [])
            for q in c.get("question_ids", []) if q
        }
        _passing_q = _total_q - len(_hard_qids | _soft_qids)

        # ── Open a strategy MLflow run for this iteration ──────────
        # Tier 4: v2 naming — ``<run_short>/iter_NN_strategy/<pending>``.
        # We don't know the AG id yet (strategist is called next); use a
        # ``pending`` detail until the strategist returns, then update
        # tags with the concrete ag_id once known.
        import mlflow as _mlflow

        from genie_space_optimizer.common.mlflow_names import (
            default_tags as _v2_tags_strat,
            strategy_run_name,
        )

        try:
            _mlflow.end_run()
        except Exception:
            pass
        _mlflow.start_run(
            run_name=strategy_run_name(run_id, iteration_counter, "pending"),
        )
        try:
            _mlflow.set_tags({
                **_v2_tags_strat(
                    run_id,
                    space_id=space_id,
                    stage="strategy",
                    iteration=iteration_counter,
                ),
                "genie.domain": domain,
                "genie.optimization_run_id": run_id,
                "genie.run_type": "strategy",
            })

            # Phase 1.3: try buffered AG first.  We re-validate against
            # the current cluster set so a buffered AG whose source
            # clusters have been resolved (or split) by a prior
            # iteration is dropped and the strategist is re-called.
            ag = None
            strategy = pending_strategy if _process_all_ags else None
            if _process_all_ags and pending_action_groups:
                # Track D — revalidate buffered AGs by stable signature
                # rather than by the unstable H00N cluster_id label.
                # An AG's signature stays constant across iterations;
                # cluster_id re-numbers. Drop AGs whose signature no
                # longer overlaps the current iteration's cluster set
                # with an explicit audit row.
                _live_cluster_signatures = {
                    str(c.get("cluster_signature") or "")
                    for c in clusters + (soft_signal_clusters or [])
                    if c.get("cluster_signature")
                }
                _src_ids: set[str] = set()
                _dropped_for_drift: list[dict] = []
                while pending_action_groups:
                    _candidate = pending_action_groups.pop(0)
                    _candidate_sig = _candidate.get("_stable_signature")
                    _candidate_sig_set = (
                        set(_candidate_sig[0]) if _candidate_sig else set()
                    )
                    if not _candidate_sig_set:
                        # Backwards-compatible fallback: AGs created
                        # before Track D's stamping landed do not have
                        # a signature; fall through to the legacy
                        # cluster-id check so the loop does not stall
                        # on in-flight buffers.
                        _src_ids = set(
                            _candidate.get("source_cluster_ids", []) or []
                        )
                        _live_cluster_ids = {
                            c.get("cluster_id", "")
                            for c in clusters + (soft_signal_clusters or [])
                        }
                        if not _src_ids or (_src_ids & _live_cluster_ids):
                            ag = _candidate
                            break
                        continue
                    if _candidate_sig_set & _live_cluster_signatures:
                        _src_ids = set(
                            _candidate.get("source_cluster_ids", []) or []
                        )
                        ag = _candidate
                        break
                    # Signature drift — drop and audit.
                    _dropped_for_drift.append(_candidate)
                if _dropped_for_drift:
                    for _drop in _dropped_for_drift:
                        print(
                            _section(
                                "DROPPING BUFFERED AG (signature drift)", "-"
                            ) + "\n"
                            + _kv("AG id", _drop.get("id", "?")) + "\n"
                            + _kv(
                                "Stale signatures",
                                sorted(
                                    set((_drop.get("_stable_signature") or ((),))[0])
                                ),
                            ) + "\n"
                            + _kv(
                                "Live signatures",
                                sorted(_live_cluster_signatures),
                            ) + "\n"
                            + _bar("-")
                        )
                if ag is not None:
                    print(
                        _section(
                            f"REUSING BUFFERED AG (skipping strategist call) — "
                            f"{len(pending_action_groups)} more queued",
                            "-",
                        ) + "\n"
                        + _kv("AG id", ag.get("id", "?")) + "\n"
                        + _kv("Source clusters", sorted(_src_ids)) + "\n"
                        + _bar("-")
                    )
                    # Task 8 — if regression debt is outstanding, drop any
                    # buffered AG that does not target debt qids and force a
                    # fresh strategist call instead.
                    if _regression_debt_qids_for_next_iteration:
                        _debt_set = set(_regression_debt_qids_for_next_iteration)
                        _ag_qids = {
                            str(q)
                            for q in (ag.get("affected_questions", []) or [])
                            if str(q)
                        }
                        if not (_debt_set & _ag_qids):
                            ag = None
                            pending_action_groups.clear()
                            pending_strategy = None
                            strategy = None
                else:
                    pending_strategy = None
                    strategy = None

            if ag is None:
                # Task 8 — pass debt qids into the strategist context and
                # promote any live hard cluster covering them to the front.
                if _regression_debt_qids_for_next_iteration:
                    metadata_snapshot["_mandatory_regression_debt_qids"] = list(
                        _regression_debt_qids_for_next_iteration
                    )
                    _debt_set = set(_regression_debt_qids_for_next_iteration)
                    _debt_clusters = [
                        c for c in _strategy_hard_clusters
                        if _debt_set & {
                            str(q) for q in (c.get("question_ids", []) or [])
                            if str(q)
                        }
                    ]
                    _debt_cluster_ids = {
                        str(c.get("cluster_id") or "") for c in _debt_clusters
                    }
                    _strategy_hard_clusters = _debt_clusters + [
                        c for c in _strategy_hard_clusters
                        if str(c.get("cluster_id") or "") not in _debt_cluster_ids
                    ]
                _live_cluster_ids = {
                    str(c.get("cluster_id") or "")
                    for c in _strategy_hard_clusters + list(_strategy_soft_clusters or [])
                    if c.get("cluster_id")
                }
                _live_diag_signatures = {
                    str(c.get("cluster_signature") or "")
                    for c in _strategy_hard_clusters + list(_strategy_soft_clusters or [])
                    if c.get("cluster_signature")
                }
                _diag_preempt: dict | None = None
                while diagnostic_action_queue and _diag_preempt is None:
                    _candidate = diagnostic_action_queue.pop(0)
                    _candidate_sig = _candidate.get("_stable_signature")
                    _candidate_sig_set = (
                        set(_candidate_sig[0]) if _candidate_sig else set()
                    )
                    # Derive _src_ids once per candidate so the audit
                    # print and the "USING DIAGNOSTIC AG" print can
                    # reference it regardless of which match path
                    # (signature vs id-fallback) was taken.
                    _src_ids = {
                        str(cid)
                        for cid in (_candidate.get("source_cluster_ids") or [])
                        if str(cid)
                    }
                    # Track D — prefer signature match; fall back to
                    # cluster-id only when the AG predates this PR.
                    if _candidate_sig_set:
                        _matches_live = bool(
                            _candidate_sig_set & _live_diag_signatures
                        )
                    else:
                        _matches_live = bool(_src_ids & _live_cluster_ids)
                    if not _matches_live:
                        print(
                            _section(
                                "SKIPPING DIAGNOSTIC AG BECAUSE CLUSTER RESOLVED", "-"
                            ) + "\n"
                            + _kv("AG id", _candidate.get("id", "?")) + "\n"
                            + _kv(
                                "Stale signatures",
                                sorted(_candidate_sig_set) if _candidate_sig_set
                                else sorted(_src_ids),
                            ) + "\n"
                            + _bar("-")
                        )
                        continue
                    _diag_preempt = _candidate
                    print(
                        _section("USING DIAGNOSTIC AG FROM COVERAGE GAP", "-")
                        + "\n"
                        + _kv("AG id", _diag_preempt.get("id", "?"))
                        + "\n"
                        + _kv("Source clusters", sorted(_src_ids))
                        + "\n"
                        + _bar("-")
                    )

                # v2 Task 23 — fingerprint sql_shape_deltas accumulated in
                # reflection_buffer so rollbacks invalidate the memo cache.
                _memo_sql_deltas = [
                    _delta
                    for _rb in reflection_buffer
                    for _delta in (_rb.get("sql_shape_deltas") or [])
                ]
                _memo_key = _strategist_memo_key(
                    list(_strategy_hard_clusters), metadata_snapshot,
                    sql_shape_deltas=_memo_sql_deltas,
                )
                from genie_space_optimizer.optimization.intent_disambiguation import (
                    detect_intent_collisions,
                )

                _intent_collisions = detect_intent_collisions(_strategy_hard_clusters)
                if _intent_collisions:
                    logger.warning(
                        "Detected %d intent collision(s) across active clusters: %s",
                        len(_intent_collisions),
                        [
                            {
                                "term": c["term"],
                                "columns": sorted(c["column_choices"]),
                            }
                            for c in _intent_collisions
                        ],
                    )
                    # Task 13 — record collision touches for every qid
                    # implicated in any column branch of the collision.
                    try:
                        for _coll in _intent_collisions:
                            _term = str(_coll.get("term") or "")
                            _qbycol = _coll.get("questions_by_column") or {}
                            _all_qids: list[str] = []
                            for _qids_list in _qbycol.values():
                                _all_qids.extend(
                                    str(q) for q in (_qids_list or []) if q
                                )
                            if _all_qids:
                                _journey_emit(
                                    "intent_collision_detected",
                                    question_ids=list(dict.fromkeys(_all_qids)),
                                    reason=f"term={_term}",
                                )
                    except Exception:
                        logger.debug(
                            "Task 13: intent collision journey emit failed",
                            exc_info=True,
                        )
                if _diag_preempt is not None:
                    strategy = {
                        "action_groups": [_diag_preempt],
                        "_memoized": False,
                        "_diagnostic_preempt": True,
                    }
                elif _memo_key in strategist_memo_cache:
                    strategy = copy.deepcopy(strategist_memo_cache[_memo_key])
                    strategy["_memoized"] = True
                else:
                    # Cycle 9 T5: surface accumulated forbid_tables
                    # constraints to the strategist via metadata_snapshot.
                    # The prompt-renderer pickup is a future task; the
                    # data is already observable in the replay fixture.
                    if _strategist_constraints.to_strategist_context():
                        metadata_snapshot["_strategist_constraints"] = (
                            _strategist_constraints.to_strategist_context()
                        )
                    # Cycle 5 T2 closeout — when the flag is on, surface
                    # the prior iteration's gate-drops of causal-target
                    # patches to the strategist's prompt context so the
                    # LLM can propose a narrower variant or shift levers
                    # instead of re-emitting the same dropped pattern.
                    # With the flag off, pass None — the strategist
                    # prompt block is omitted (byte-stable).
                    from genie_space_optimizer.common.config import (
                        causal_drop_feedback_to_strategist_enabled,
                    )
                    _t2_drops_for_strategist = (
                        list(_prior_iteration_dropped_causal_patches)
                        if causal_drop_feedback_to_strategist_enabled()
                        else None
                    )
                    strategy = _call_llm_for_adaptive_strategy(
                        clusters=_strategy_hard_clusters,
                        soft_signal_clusters=_strategy_soft_clusters,
                        metadata_snapshot=metadata_snapshot,
                        reflection_buffer=reflection_buffer,
                        priority_ranking=ranked,
                        tried_patches=tried_patches,
                        w=w,
                        total_benchmarks=_total_q,
                        passing_benchmarks=max(0, _passing_q),
                        verdict_history=_verdict_history,
                        skill_exemplars=skill_exemplars or None,
                        human_suggestions=_human_suggestions or None,
                        iq_scan_summary=(
                            iq_scan_summary if _iq_scan_strategist_enabled() else None
                        ),
                        max_ag_patches=MAX_AG_PATCHES,
                        intent_collisions=_intent_collisions,
                        prior_iteration_dropped_causal_patches=(
                            _t2_drops_for_strategist
                        ),
                    )
                    strategist_memo_cache[_memo_key] = copy.deepcopy(strategy)
                    strategy["_memoized"] = False
                logger.info(
                    "Strategist memoization: key=%s hit=%s",
                    _memo_key[:120],
                    strategy.get("_memoized"),
                )
                strategy["_source_clusters"] = (
                    list(_strategy_hard_clusters) + list(_strategy_soft_clusters)
                )
                _l3_diagnostics = _diagnose_lever3_directive_emission(
                    list(_strategy_hard_clusters), strategy,
                )
                if _l3_diagnostics:
                    logger.warning(
                        "Lever 3 directive diagnostics: %s",
                        json.dumps(_l3_diagnostics, default=str),
                    )
                    strategy["lever3_directive_diagnostics"] = _l3_diagnostics
                action_groups = strategy.get("action_groups", [])
                # Task 8 — strategist coverage enforcement. Any patchable
                # hard cluster the LLM dropped gets a deterministic
                # diagnostic AG so the loop attempts it before declaring
                # "exhausted".
                try:
                    from genie_space_optimizer.optimization.control_plane import (
                        diagnostic_action_group_for_cluster,
                        uncovered_patchable_clusters,
                    )

                    _uncovered = uncovered_patchable_clusters(
                        clusters,
                        action_groups,
                    )
                    if _uncovered:
                        # Task 6 — log a structured diagnostic so the next
                        # operator can tell the difference between "no RCA
                        # card", "RCA card present but strategist returned
                        # nothing", and "output truncated".
                        try:
                            _uncovered_ids = [
                                str(c.get("cluster_id"))
                                for c in _uncovered
                                if c.get("cluster_id")
                            ]
                            _log_strategist_coverage_gap(
                                iteration=iteration_counter,
                                uncovered_cluster_ids=_uncovered_ids,
                                cluster_question_counts={
                                    str(c.get("cluster_id")): len(
                                        c.get("question_ids") or []
                                    )
                                    for c in clusters or []
                                    if c.get("cluster_id")
                                },
                                rca_cards_present={
                                    str(c.get("cluster_id")): bool(c.get("rca_card"))
                                    for c in clusters or []
                                    if c.get("cluster_id")
                                },
                                strategist_action_groups=len(
                                    (strategy or {}).get("action_groups") or []
                                ),
                                strategist_input_token_estimate=(strategy or {}).get(
                                    "_input_token_estimate"
                                ),
                                strategist_output_truncated=bool(
                                    (strategy or {}).get("_output_truncated")
                                ),
                            )
                        except Exception:
                            logger.debug(
                                "Failed to log strategist coverage gap diagnostic",
                                exc_info=True,
                            )
                        logger.warning(
                            "Strategist did not cover %d patchable hard cluster(s); "
                            "appending diagnostic AGs: %s",
                            len(_uncovered),
                            [c.get("cluster_id") for c in _uncovered],
                        )
                        from genie_space_optimizer.optimization.control_plane import (
                            compute_ag_stable_signature,
                        )

                        for _c in _uncovered:
                            _diag_ag = diagnostic_action_group_for_cluster(_c)
                            # Track D — stamp a stable signature derived
                            # from cluster_signature, qid set, and root
                            # cause so revalidation in later iterations
                            # does not depend on the unstable H00N
                            # cluster_id label.
                            _diag_ag["_stable_signature"] = compute_ag_stable_signature(
                                _diag_ag, [_c]
                            )
                            # Cycle 5 T3 — when the cluster has no parent
                            # RCA AND ``GSO_DIAGNOSTIC_AG_RCA_REGEN`` is
                            # on, the AG enters the regeneration branch:
                            # emit ``RCA_REGENERATION_TRIGGERED``,
                            # attempt regen (no-op until the regen
                            # helper lands), then emit
                            # ``RCA_REGENERATION_EXHAUSTED`` and skip the
                            # AG so we don't generate empty proposals.
                            # With the flag off, every AG flows through
                            # as before — byte-stable.
                            try:
                                from genie_space_optimizer.common.config import (
                                    diagnostic_ag_rca_regen_enabled,
                                )
                                if (
                                    diagnostic_ag_rca_regen_enabled()
                                    and _diag_ag.get("needs_rca_regeneration")
                                ):
                                    from genie_space_optimizer.optimization.decision_emitters import (
                                        rca_regeneration_triggered_record,
                                        rca_regeneration_exhausted_record,
                                    )
                                    _t3_cluster_id = str(
                                        _diag_ag.get("primary_cluster_id")
                                        or _c.get("cluster_id") or ""
                                    )
                                    _t3_target_qids = tuple(
                                        str(q)
                                        for q in (_c.get("question_ids") or [])
                                        if q
                                    )
                                    # Cycle 6 F-7 — emit diagnostic_ag
                                    # trunk events so the journey
                                    # classifier picks
                                    # HARD_FAILURE_UNRESOLVED rather
                                    # than TERMINAL_UNACTIONABLE for
                                    # T3-regen-exhausted hard qids.
                                    _emit_diagnostic_ag_trunk_events(
                                        journey_emit=_journey_emit,
                                        cluster_qids=_t3_target_qids,
                                        cluster_id=_t3_cluster_id,
                                    )
                                    _t3_trig = rca_regeneration_triggered_record(
                                        run_id=str(run_id),
                                        iteration=int(iteration_counter),
                                        cluster_id=_t3_cluster_id,
                                        target_qids=_t3_target_qids,
                                    )
                                    # Cycle 6 F-1 — gate duplicate emits.
                                    _t3_trig_key = _emit_idempotency_key(
                                        _t3_trig.to_dict()
                                    )
                                    if _t3_trig_key not in _iter_emitted_keys:
                                        _iter_emitted_keys.add(_t3_trig_key)
                                        _decision_emit(_t3_trig)
                                        _current_iter_inputs.setdefault(
                                            "decision_records", []
                                        ).append(_t3_trig.to_dict())
                                    # Regen helper is a follow-up; for
                                    # now every regen attempt fails so
                                    # the AG retires here.
                                    _t3_exh = rca_regeneration_exhausted_record(
                                        run_id=str(run_id),
                                        iteration=int(iteration_counter),
                                        cluster_id=_t3_cluster_id,
                                        attempted_evidence_sources=(),
                                    )
                                    _t3_exh_key = _emit_idempotency_key(
                                        _t3_exh.to_dict()
                                    )
                                    if _t3_exh_key not in _iter_emitted_keys:
                                        _iter_emitted_keys.add(_t3_exh_key)
                                        _decision_emit(_t3_exh)
                                        _current_iter_inputs.setdefault(
                                            "decision_records", []
                                        ).append(_t3_exh.to_dict())
                                        # Cycle 6 F-7 — also emit the
                                        # rca_exhausted trunk event so
                                        # the classifier can
                                        # distinguish tried-and-
                                        # exhausted from never-tried.
                                        for _q in _t3_target_qids:
                                            try:
                                                _journey_emit(
                                                    "rca_exhausted",
                                                    question_id=str(_q),
                                                    cluster_id=_t3_cluster_id,
                                                )
                                            except Exception:
                                                logger.debug(
                                                    "F-7: rca_exhausted "
                                                    "trunk emit failed "
                                                    "(non-fatal)",
                                                    exc_info=True,
                                                )
                                    # Skip this AG entirely — do not
                                    # append to action_groups or the
                                    # diagnostic queue.
                                    continue
                            except Exception:
                                logger.debug(
                                    "Cycle 5 T3: RCA regen branch failed "
                                    "(non-fatal); proceeding with the "
                                    "original diagnostic AG",
                                    exc_info=True,
                                )
                            action_groups.append(_diag_ag)
                            diagnostic_action_queue.append(_diag_ag)
                            # Task 13 — diagnostic AG covers all qids in
                            # the uncovered cluster.
                            try:
                                _diag_qids = [
                                    str(q)
                                    for q in (_c.get("question_ids") or [])
                                    if q
                                ]
                                _diag_ag_id = str(
                                    _diag_ag.get("id")
                                    or _diag_ag.get("ag_id")
                                    or ""
                                )
                                if _diag_qids:
                                    _journey_emit(
                                        "diagnostic_ag",
                                        question_ids=_diag_qids,
                                        ag_id=_diag_ag_id,
                                        cluster_id=str(
                                            _c.get("cluster_id") or ""
                                        ),
                                        root_cause=str(
                                            _c.get("root_cause")
                                            or _c.get("asi_failure_type")
                                            or ""
                                        ),
                                    )
                            except Exception:
                                logger.debug(
                                    "Task 13: diagnostic_ag journey emit failed",
                                    exc_info=True,
                                )
                except Exception:
                    logger.debug(
                        "Strategist coverage enforcement raised (non-fatal)",
                        exc_info=True,
                    )
                # Sort by priority (lower = higher priority); fall back
                # to source-cluster impact_score when priority is
                # missing or tied.
                _impact_by_cid = {
                    c.get("cluster_id", ""): float(c.get("impact_score", 0.0))
                    for c in clusters + (soft_signal_clusters or [])
                }

                def _ag_sort_key(_ag: dict) -> tuple:
                    _pri = _ag.get("priority")
                    _pri_v = float(_pri) if isinstance(_pri, (int, float)) else 999.0
                    _src = _ag.get("source_cluster_ids", []) or []
                    _impact = max(
                        (_impact_by_cid.get(_cid, 0.0) for _cid in _src),
                        default=0.0,
                    )
                    return (_pri_v, -_impact)

                # Track 4 (Phase A burn-down) — decompose any
                # heterogeneous AG spanning multiple root-cause
                # families or tables when the bundle lacks a shared
                # direct fix. Per-cluster diagnostic AGs replace the
                # parent so cap budget can preserve a direct fix per
                # cluster.
                from genie_space_optimizer.optimization.control_plane import (
                    decompose_overbroad_ag,
                )
                _all_clusters_for_decomposition = list(clusters or []) + list(
                    soft_signal_clusters or []
                )
                _decomposed_action_groups: list[dict] = []
                for _ag_in in action_groups:
                    _decomposed_action_groups.extend(
                        decompose_overbroad_ag(
                            _ag_in, _all_clusters_for_decomposition
                        )
                    )
                if len(_decomposed_action_groups) != len(action_groups):
                    print(
                        _section(
                            f"AG DECOMPOSITION GUARDRAIL — {len(action_groups)} -> "
                            f"{len(_decomposed_action_groups)} AGs",
                            "-",
                        ) + "\n"
                        + _kv(
                            "Original AG ids",
                            ", ".join(_a.get("id", "?") for _a in action_groups),
                        ) + "\n"
                        + _kv(
                            "Decomposed AG ids",
                            ", ".join(
                                _a.get("id", "?") for _a in _decomposed_action_groups
                            ),
                        ) + "\n"
                        + _bar("-")
                    )
                action_groups = _decomposed_action_groups
                action_groups = sorted(action_groups, key=_ag_sort_key)
                ag = action_groups[0] if action_groups else None
                if _process_all_ags and len(action_groups) > 1:
                    pending_action_groups = list(
                        action_groups[1:_MAX_AGS_PER_STRATEGIST_CALL]
                    )
                    pending_strategy = strategy
                    # Track D — stamp the stable signature on every
                    # buffered AG before queueing. The signature is
                    # computed against the clusters present at
                    # buffering time so revalidation in later
                    # iterations checks "does this AG's signature
                    # still appear in the live cluster set" rather
                    # than "does the H00N label still match".
                    from genie_space_optimizer.optimization.control_plane import (
                        compute_ag_stable_signature,
                    )

                    _all_clusters_for_signature = list(clusters or []) + list(
                        soft_signal_clusters or []
                    )
                    for _buffered_ag in pending_action_groups:
                        _buffered_ag["_stable_signature"] = compute_ag_stable_signature(
                            _buffered_ag, _all_clusters_for_signature
                        )
                    print(
                        _section(
                            f"BUFFERING {len(pending_action_groups)} ADDITIONAL AG(S) "
                            f"FOR LATER ITERATION(S)",
                            "-",
                        ) + "\n"
                        + _kv(
                            "Buffered AGs",
                            ", ".join(
                                _a.get("id", "?") for _a in pending_action_groups
                            ),
                        ) + "\n"
                        + _bar("-")
                    )
                else:
                    pending_action_groups = []
                    pending_strategy = None

            _global_rewrite = strategy.get("global_instruction_rewrite")
            if isinstance(_global_rewrite, dict):
                non_empty = {k: v for k, v in _global_rewrite.items() if v is not None}
                if non_empty and ag is not None:
                    ld = ag.setdefault("lever_directives", {})
                    l5 = ld.setdefault("5", {})
                    l5["instruction_sections"] = non_empty
            elif isinstance(_global_rewrite, str) and _global_rewrite.strip():
                if ag is not None:
                    ld = ag.setdefault("lever_directives", {})
                    l5 = ld.setdefault("5", {})
                    l5["instruction_guidance"] = _global_rewrite.strip()

            if ag is None and _iter_num == 1:
                logger.info("Adaptive strategist returned 0 AGs on iter 1 — trying holistic fallback")
                fallback_strategy = _generate_holistic_strategy(
                    clusters=clusters,
                    soft_signal_clusters=soft_signal_clusters,
                    metadata_snapshot=metadata_snapshot,
                    w=w,
                )
                _fb_ags = fallback_strategy.get("action_groups", [])
                _fb_ags.sort(key=lambda a: a.get("priority", 999))
                if _fb_ags:
                    ag = _fb_ags[0]
                    strategy = fallback_strategy
        finally:
            _mlflow.end_run()

        if ag is None and clusters:
            _remaining_qids = set()
            for c in clusters:
                _remaining_qids.update(c.get("question_ids", []))
            if _remaining_qids and _iter_num <= max_iterations - 1:
                logger.info(
                    "Strategist returned 0 AGs but %d clusters with %d questions remain — "
                    "constructing diagnostic fallback AG",
                    len(clusters), len(_remaining_qids),
                )
                _top_cluster = ranked[0] if ranked else clusters[0]
                ag = {
                    "id": f"AG{iteration_counter}_fallback",
                    "root_cause_summary": _top_cluster.get("root_cause", "unresolved_failures"),
                    "affected_questions": _top_cluster.get("question_ids", []),
                    "source_cluster_ids": [_top_cluster.get("cluster_id", "")],
                    "lever_directives": {
                        "5": {"instruction_guidance": "Add example SQLs and routing instructions for remaining failure patterns"},
                        "6": {"generate_expressions": True},
                    },
                    "rationale": (
                        f"Diagnostic fallback: {len(_remaining_qids)} question(s) still failing. "
                        f"Trying Lever 5 (instructions/examples) + Lever 6 (SQL expressions) "
                        f"as a broad-spectrum fix."
                    ),
                    "coordination_notes": "Fallback AG — strategist returned empty, applying broad-spectrum lever 5+6",
                }
                strategy = strategy or {}
                strategy["action_groups"] = [ag]
                print(
                    _section(f"DIAGNOSTIC FALLBACK AG — {len(_remaining_qids)} questions remain", "!") + "\n"
                    + _kv("Cluster", _top_cluster.get("cluster_id", "?")) + "\n"
                    + _kv("Root cause", _top_cluster.get("root_cause", "?")) + "\n"
                    + _kv("Questions", len(_top_cluster.get("question_ids", []))) + "\n"
                    + _bar("!")
                )

        if ag is None:
            logger.info("Strategist produced 0 action groups — ending lever loop")
            print(
                _section("Strategy produced 0 action groups — nothing to do", "-") + "\n"
                + _bar("-")
            )
            break

        ag_id = ag.get("id", f"AG{iteration_counter}")
        ags_attempted.append(ag_id)
        lever_keys = sorted(ag.get("lever_directives", {}).keys())

        # Phase A — capture strategist AG snapshot for replay-fixture export.
        try:
            _current_iter_inputs["strategist_response"]["action_groups"].append({
                "id": str(ag_id),
                "affected_questions": [
                    str(q) for q in (ag.get("affected_questions") or []) if q
                ],
                "patches": [],
            })
        except Exception:
            logger.debug(
                "Phase A: strategist AG capture failed (non-fatal)",
                exc_info=True,
            )

        # Tier 3.3: relabel header so the scorecard is clearly identified
        # as "best (post last accepted iter)" rather than conflated with
        # the latest eval. After Tier 1.3, ``prev_accuracy`` is refreshed
        # by the post-enrichment eval, so the two blocks now represent
        # the same reality (current space state), but the label clarifies
        # intent for operators reading stale runs.
        print(
            _section(f"ACTION GROUP {ag_id} — Iteration ({_iteration_label(iteration_counter)})") + "\n"
            + _kv("Root cause", ag.get("root_cause_summary", "?")[:120]) + "\n"
            + _kv("Levers", ", ".join(lever_keys)) + "\n"
            + _kv("Affected questions", len(ag.get("affected_questions", []))) + "\n"
            + _kv("Best accuracy (post last accepted)", f"{best_accuracy:.1f}%") + "\n"
            + _scorecard(best_scores) + "\n"
            + _kv(
                "Failure analysis source",
                f"iter {iteration_counter - 1} full eval (current space state)",
            ) + "\n"
            + _bar("=")
        )

        _ag_source_cids = list(ag.get("source_cluster_ids", []))
        _ag_cluster_info: dict = {}
        # Phase C2: derive identity fields used by DO-NOT-RETRY (D1-D3)
        # from the first source cluster. Action groups can span multiple
        # clusters but in practice they share a root cause (the strategist
        # is instructed to merge clusters with the same blame set). The
        # first cluster's root_cause / blame_set is representative enough
        # for collision detection.
        _ag_root_cause: str = ""
        _ag_blame_set: Any = None
        # T2.1: collect iteration-independent cluster signatures for
        # every source cluster this AG targets. Reflection buffer stamps
        # them so the next iteration can detect "this signature has
        # been tried before" even if the pretty cluster_id changed.
        _ag_source_signatures: list[str] = []
        for _rc_idx, _rc in enumerate(ranked):
            _rc_cid = _rc.get("cluster_id", "")
            if _ag_source_cids and _rc_cid not in set(_ag_source_cids):
                continue
            _rc_sig = _rc.get("cluster_signature")
            if _rc_sig and _rc_sig not in _ag_source_signatures:
                _ag_source_signatures.append(_rc_sig)
            if not _ag_cluster_info:
                _ag_cluster_info = {
                    "cluster_id": _rc_cid,
                    "impact_score": _rc.get("impact_score"),
                    "rank": _rc_idx + 1,
                    "question_count": len(_rc.get("question_ids", [])),
                    "root_cause": _rc.get("root_cause") or _rc.get("asi_failure_type"),
                    "affected_questions": _rc.get("question_ids", [])[:20],
                    "cluster_signature": _rc_sig,
                }
                _ag_root_cause = (
                    _rc.get("asi_failure_type")
                    or _rc.get("root_cause")
                    or ""
                )
                _ag_blame_set = _rc.get("asi_blame_set")

        # Phase C2: reusable identity kwargs for every _build_reflection_entry
        # call in this AG iteration. Keeps call sites DRY while guaranteeing
        # the forbidden-set / tried-cluster bookkeeping downstream always
        # sees the same root_cause / blame_set / source_cluster_ids.
        _ag_identity_kwargs = {
            "root_cause": _ag_root_cause,
            "blame_set": _ag_blame_set,
            "source_cluster_ids": list(_ag_source_cids),
            "source_cluster_signatures": list(_ag_source_signatures),
        }

        # Phase D2: collision guard. The strategist occasionally re-proposes
        # a previously-rejected (root_cause, blame_set, lever_set) tuple
        # despite the DO NOT RETRY hint in its prompt (see Q004 regression).
        # When that happens, skip this AG rather than deploying the same
        # patch again. The reflection entry is logged with rollback_class
        # OTHER so this skip doesn't count against any budget — it's
        # purely a routing correction.
        _forbidden = _compute_forbidden_ag_set(reflection_buffer)
        _collision_key = _ag_collision_key(
            ag, _ag_root_cause, _ag_blame_set, lever_keys,
        )
        if _collision_key is not None and _collision_key in _forbidden:
            _rc_k, _blame_k, _lever_k = _collision_key
            print(
                _section(f"[{ag_id}] AG COLLISION — skipping", "!") + "\n"
                + _kv("Root cause", _rc_k) + "\n"
                + _kv("Blame", _blame_k) + "\n"
                + _kv("Lever set", sorted(_lever_k)) + "\n"
                + _kv(
                    "Reason",
                    "strategist re-proposed a (root_cause, blame, lever_set) "
                    "tuple previously rolled back for content regression",
                ) + "\n"
                + _bar("!")
            )
            write_stage(
                spark, run_id, f"AG_{ag_id}_COLLISION_SKIPPED", "SKIPPED",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "root_cause": _rc_k,
                    "blame_set": list(_blame_k) if isinstance(_blame_k, tuple) else _blame_k,
                    "lever_set": sorted(_lever_k),
                },
                catalog=catalog, schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[int(lk) for lk in lever_keys], target_objects=[],
                prev_scores=best_scores, new_scores=best_scores,
                rollback_reason="ag_collision_with_forbidden_set",
                patches=[],
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
                **_ag_identity_kwargs,
            ))
            _render_current_journey()
            continue

        _ag_cluster_info["rationale"] = ag.get("rationale", strategy.get("rationale", "") if strategy else "")
        _ag_cluster_info["escalation"] = ag.get("escalation") or None
        _global_rewrite = strategy.get("global_instruction_rewrite", "") if strategy else ""
        _ag_cluster_info["instruction_rewrite_preview"] = str(_global_rewrite)[:500] if _global_rewrite else ""

        write_stage(
            spark, run_id, f"AG_{ag_id}_STARTED", "STARTED",
            task_key="lever_loop", iteration=iteration_counter,
            detail=_ag_cluster_info if _ag_cluster_info else None,
            catalog=catalog, schema=schema,
        )

        # ── 3B.4a+: Persist improvement proposals from strategist ───
        _ag_proposals = ag.get("proposals", [])
        if _ag_proposals and isinstance(_ag_proposals, list):
            for _prop in _ag_proposals:
                if not isinstance(_prop, dict):
                    continue
                try:
                    write_suggestion(spark, catalog, schema, {
                        "run_id": run_id,
                        "space_id": space_id,
                        "iteration": iteration_counter,
                        "lever": None,
                        "type": _prop.get("type", "METRIC_VIEW"),
                        "title": _prop.get("title", "Untitled proposal"),
                        "rationale": _prop.get("rationale"),
                        "definition": _prop.get("definition"),
                        "affected_questions": _prop.get("affected_questions", []),
                        "estimated_impact": _prop.get("estimated_impact"),
                    })
                except Exception:
                    logger.debug("Failed to write suggestion from AG %s", ag_id, exc_info=True)
            if _ag_proposals:
                logger.info("Wrote %d improvement proposals from AG %s", len(_ag_proposals), ag_id)

        # ── 3B.4b: Handle escalation if present ─────────────────────
        _escalation = ag.get("escalation", "")
        if _escalation:
            print(
                _section(f"ESCALATION: {_escalation}", "!") + "\n"
                + _kv("Type", _escalation) + "\n"
                + _kv("Affected questions", ag.get("affected_questions", [])) + "\n"
                + _bar("!")
            )
            _esc_result = _handle_escalation(
                _escalation, ag,
                w=w, spark=spark, run_id=run_id,
                catalog=catalog, schema=schema, domain=domain,
                iteration=iteration_counter,
                benchmarks=benchmarks,
                verdict_history=_verdict_history,
                reflection_buffer=reflection_buffer,
                metadata_snapshot=metadata_snapshot,
            )
            logger.info("Escalation result: %s", _esc_result)

            write_stage(
                spark, run_id, f"AG_{ag_id}_ESCALATION", "COMPLETE",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "escalation_type": _escalation,
                    "handled": _esc_result.get("handled", False),
                    "detail": _esc_result.get("detail", {}),
                    "affected_questions": ag.get("affected_questions", [])[:20],
                },
                catalog=catalog, schema=schema,
            )

            _esc_tier = _esc_result.get("detail", {}).get("tier_action", "")

            if _escalation == "flag_for_review" or (
                _escalation == "remove_tvf" and _esc_tier == "flagged_only"
            ):
                reflection_buffer.append(_build_reflection_entry(
                    iteration=iteration_counter, ag_id=ag_id, accepted=False,
                    levers=[], target_objects=ag.get("affected_questions", []),
                    prev_scores=best_scores, new_scores=best_scores,
                    rollback_reason=f"escalation:{_escalation}",
                    patches=[],
                    affected_question_ids=ag.get("affected_questions", []),
                    prev_failure_qids=prev_failure_qids,
                    new_failure_qids=prev_failure_qids,
                    escalation_handled=True,
                    **_ag_identity_kwargs,
                ))
                continue

            if _escalation == "gt_repair":
                _gt_repair_corrections = _esc_result.get("detail", {}).get("corrections_applied", 0)
                if _gt_repair_corrections > 0:
                    reflection_buffer.append(_build_reflection_entry(
                        iteration=iteration_counter, ag_id=ag_id, accepted=True,
                        levers=[], target_objects=ag.get("affected_questions", []),
                        prev_scores=best_scores, new_scores=best_scores,
                        rollback_reason=None,
                        patches=[],
                        affected_question_ids=ag.get("affected_questions", []),
                        prev_failure_qids=prev_failure_qids,
                        new_failure_qids=prev_failure_qids,
                        reflection_text=f"GT repair applied {_gt_repair_corrections} benchmark correction(s)",
                        escalation_handled=True,
                        **_ag_identity_kwargs,
                    ))
                else:
                    _unfixed = set(ag.get("affected_questions", [])) - set(
                        _esc_result.get("detail", {}).get("corrected_qids", [])
                    ) - set(
                        _esc_result.get("detail", {}).get("quarantined_qids", [])
                    )
                    escalated_gt_repair_qids.update(_unfixed)
                    reflection_buffer.append(_build_reflection_entry(
                        iteration=iteration_counter, ag_id=ag_id, accepted=False,
                        levers=[], target_objects=ag.get("affected_questions", []),
                        prev_scores=best_scores, new_scores=best_scores,
                        rollback_reason="escalation:gt_repair (delegated to arbiter)",
                        patches=[],
                        affected_question_ids=ag.get("affected_questions", []),
                        prev_failure_qids=prev_failure_qids,
                        new_failure_qids=prev_failure_qids,
                        escalation_handled=True,
                        **_ag_identity_kwargs,
                    ))
                continue

            if _escalation == "remove_tvf" and _esc_tier in ("auto_apply", "apply_and_flag"):
                _tvf_id = _esc_result.get("detail", {}).get("tvf_id", "")
                _prev_asset = _esc_result.get("detail", {}).get("previous_tvf_asset", {})
                if _tvf_id:
                    _synthetic_patch = {
                        "type": "remove_tvf",
                        "target": _tvf_id,
                        "new_text": "",
                        "old_text": "",
                        "previous_tvf_asset": _prev_asset,
                        "lever": 3,
                        "risk_level": "high",
                        "predicted_affected_questions": len(ag.get("affected_questions", [])),
                        "rationale": (
                            f"TVF {_tvf_id} auto-removed by tiered confidence model "
                            f"(tier={_esc_tier})"
                        ),
                    }
                    _tvf_conf = _esc_result.get("detail", {}).get("confidence", "?")
                    print(
                        _section(f"[{ag_id}] SYNTHETIC remove_tvf PATCH", "!") + "\n"
                        + _kv("TVF", _tvf_id) + "\n"
                        + _kv("Confidence", _tvf_conf) + "\n"
                        + _kv("Tier action", _esc_tier) + "\n"
                        + _bar("!")
                    )
                    _tvf_apply_log = apply_patch_set(
                        w, space_id, [_synthetic_patch], metadata_snapshot,
                        apply_mode=apply_mode,
                        force_apply=True,
                    )
                    _tvf_lever = 3
                    for idx, entry in enumerate(_tvf_apply_log.get("applied", [])):
                        write_patch(
                            spark, run_id, iteration_counter, _tvf_lever, idx,
                            _build_patch_record(entry, _tvf_lever, apply_mode),
                            catalog, schema,
                        )
                    if _tvf_apply_log.get("patch_deployed", False):
                        logger.info("TVF %s removed successfully (tier=%s)", _tvf_id, _esc_tier)
                        metadata_snapshot = _tvf_apply_log.get("post_snapshot", metadata_snapshot)
                        if _original_instruction_sections:
                            metadata_snapshot["_original_instruction_sections"] = _original_instruction_sections
                    else:
                        logger.warning(
                            "TVF removal patch deploy failed: %s",
                            _tvf_apply_log.get("patch_error", "unknown"),
                        )
                else:
                    logger.warning(
                        "remove_tvf escalation with tier %s but no tvf_id — skipping",
                        _esc_tier,
                    )

        try:
            from genie_space_optimizer.optimization.rca_execution import (
                forced_levers_from_reflections,
                next_grounding_remediation,
                plans_for_action_group,
                required_levers_for_action_group,
                union_execution_levers,
            )

            _source_clusters_for_execution = strategy.get("_source_clusters", [])
            _rca_plans_for_ag = plans_for_action_group(
                ag,
                metadata_snapshot.get("_rca_execution_plans") or [],
                source_clusters=_source_clusters_for_execution,
            )
            _rca_required_levers = required_levers_for_action_group(
                ag,
                metadata_snapshot.get("_rca_execution_plans") or [],
                source_clusters=_source_clusters_for_execution,
            )
            _forced_from_reflections = forced_levers_from_reflections(
                reflection_buffer,
                target_rca_ids=tuple(p.rca_id for p in _rca_plans_for_ag),
                min_repeats=2,
            )
            _grounding_remediation = next_grounding_remediation(
                reflection_buffer,
                target_rca_ids=tuple(p.rca_id for p in _rca_plans_for_ag),
            )
            _forced_from_grounding = tuple(
                int(x)
                for x in (_grounding_remediation.get("forced_levers") or ())
            )
            _all_required_rca_levers = tuple(dict.fromkeys(
                list(_rca_required_levers)
                + list(_forced_from_reflections)
                + list(_forced_from_grounding)
            ))
            if _all_required_rca_levers:
                ag["_rca_execution"] = {
                    "rca_ids": [p.rca_id for p in _rca_plans_for_ag],
                    "required_levers": list(_all_required_rca_levers),
                    "defect_keys": [p.defect_key for p in _rca_plans_for_ag],
                    "grounding_terms": sorted({
                        term for p in _rca_plans_for_ag for term in p.grounding_terms
                    }),
                    "forced_from_reflections": list(_forced_from_reflections),
                    "grounding_remediation": _grounding_remediation.get("action", "none"),
                }
                lever_keys = union_execution_levers(
                    lever_keys,
                    _all_required_rca_levers,
                )
                logger.info(
                    "[%s] RCA execution required levers=%s final_levers=%s rca_ids=%s",
                    ag_id,
                    list(_all_required_rca_levers),
                    lever_keys,
                    ag["_rca_execution"]["rca_ids"],
                )
        except Exception:
            logger.debug("Failed to union RCA-required levers", exc_info=True)

        if "6" in lever_keys:
            try:
                from genie_space_optimizer.optimization.control_plane import (
                    rows_for_qids,
                    target_qids_from_action_group,
                )
                from genie_space_optimizer.optimization.feature_mining import (
                    extract_failed_row_sql_expression_candidates,
                )

                _all_rows_for_structural_learning = _get_failure_rows(
                    spark, run_id, catalog, schema,
                )
                _structural_target_qids = target_qids_from_action_group(
                    ag,
                    strategy.get("_source_clusters", []),
                )
                _structural_rows = rows_for_qids(
                    _all_rows_for_structural_learning,
                    _structural_target_qids,
                )
                _structural_candidates: list[dict] = []
                for _row in _structural_rows:
                    for _candidate in extract_failed_row_sql_expression_candidates(_row):
                        _structural_candidates.append(_candidate.as_dict())
                if _structural_candidates:
                    ag["_lever6_structural_candidates"] = _structural_candidates
                    logger.info(
                        "[%s] Lever 6 structural candidates from failed GT SQL: %d",
                        ag_id,
                        len(_structural_candidates),
                    )
                    print(
                        _section(
                            f"LEVER 6 STRUCTURAL SQL LEARNING [{ag_id}]",
                            "-",
                        )
                        + "\n"
                        + _kv("Scoped rows", len(_structural_rows))
                        + "\n"
                        + _kv("Candidates", len(_structural_candidates))
                        + "\n"
                        + _kv(
                            "Source",
                            "arbiter-approved failed question expected_sql",
                        )
                        + "\n"
                        + _bar("-")
                    )
            except Exception:
                logger.debug(
                    "Failed to attach Lever 6 structural candidates",
                    exc_info=True,
                )

        # ── 3B.5: Generate proposals + apply patches ─────────────────
        # Task 4 — initialize per-AG patch-survival snapshots. They get
        # filled in at each handoff gate (proposed → normalized →
        # applyable → capped → applied) and printed after the applier so
        # operators can see exactly where patches were dropped.
        _survival_proposed: list[dict] = []
        _survival_normalized: list[dict] = []
        _survival_applyable: list[dict] = []
        _survival_capped: list[dict] = []
        all_proposals: list[dict] = []
        for lever_key in lever_keys:
            lever_int = int(lever_key)
            levers_attempted.append(lever_int)
            lever_proposals = generate_proposals_from_strategy(
                strategy=strategy,
                action_group=ag,
                metadata_snapshot=metadata_snapshot,
                target_lever=lever_int,
                apply_mode=apply_mode,
                w=w,
                spark=spark,
                catalog=catalog,
                gold_schema=schema,
                warehouse_id=resolve_warehouse_id(""),
                benchmarks=benchmarks,
            )
            all_proposals.extend(lever_proposals)

        # P4 task 5 — stdout marker when an AG produced zero proposals.
        # Distinct from STRUCTURAL_GATE_DROPPED (proposal existed but
        # was dropped) and NO_STRUCTURAL_CANDIDATE (synthesis attempted
        # but no archetype matched). Emitted before the L5-drop block
        # so the empty-proposals signal fires regardless of L5 state.
        if not all_proposals:
            try:
                from genie_space_optimizer.optimization.run_analysis_contract import (
                    proposal_generation_empty_marker,
                )
                print(proposal_generation_empty_marker(
                    ag_id=str(ag_id),
                    iteration=iteration_counter,
                    target_qids=tuple(
                        str(q) for q in (ag.get("affected_questions") or [])
                        if str(q)
                    ),
                ), flush=True)
            except Exception:
                logger.debug(
                    "P4: proposal_generation_empty_marker emit failed (non-fatal)",
                    exc_info=True,
                )

        # Cycle 8 Bug 1 Phase 3b Task B — drain Lever 5 structural-gate
        # drops for this AG into the iteration's decision_records. The
        # gate fires inside generate_proposals_from_strategy and stashes
        # one record per drop on optimizer._LEVER5_GATE_DROPS; we snapshot
        # here, filter to this AG, and build a typed GATE_DECISION
        # DecisionRecord. The full ledger is reset at the end of the
        # iteration alongside the Bug-4 counters.
        try:
            from genie_space_optimizer.optimization.optimizer import (
                get_lever5_gate_drops as _get_lever5_gate_drops,
            )
            from genie_space_optimizer.optimization.decision_emitters import (
                lever5_structural_gate_records as _lever5_structural_gate_records,
            )

            _l5_all_drops = _get_lever5_gate_drops()
            _l5_ag_drops = [
                d for d in _l5_all_drops
                if str(d.get("ag_id") or "") == str(ag_id)
            ]
            if _l5_ag_drops:
                _l5_ag_root_cause = ""
                _l5_ag_rca_id = ""
                for _cid in (ag.get("source_cluster_ids") or []):
                    _l5_ag_rca_id = str(
                        _iter_rca_id_by_cluster.get(str(_cid)) or ""
                    )
                    _src_cluster = _iter_source_clusters_by_id.get(str(_cid))
                    if isinstance(_src_cluster, dict) and not _l5_ag_root_cause:
                        _l5_ag_root_cause = str(
                            _src_cluster.get("root_cause") or ""
                        )
                    if _l5_ag_rca_id and _l5_ag_root_cause:
                        break
                _l5_records = _lever5_structural_gate_records(
                    run_id=run_id,
                    iteration=iteration_counter,
                    ag_id=str(ag_id),
                    rca_id=_l5_ag_rca_id,
                    root_cause=_l5_ag_root_cause,
                    target_qids=tuple(
                        str(q) for q in (ag.get("affected_questions") or [])
                        if str(q)
                    ),
                    drops=_l5_ag_drops,
                )
                _current_iter_inputs.setdefault(
                    "decision_records", []
                ).extend([r.to_dict() for r in _l5_records])

                # P4 task 5 — stdout marker for the L5 structural-gate
                # drop. Aggregates root_causes across the AG's drops.
                try:
                    from genie_space_optimizer.optimization.run_analysis_contract import (
                        structural_gate_dropped_marker,
                    )
                    _l5_marker_root_causes: list[str] = []
                    for _md in _l5_ag_drops:
                        for _rc in (_md.get("root_causes") or ()):
                            _rc_s = str(_rc)
                            if _rc_s and _rc_s not in _l5_marker_root_causes:
                                _l5_marker_root_causes.append(_rc_s)
                    print(structural_gate_dropped_marker(
                        ag_id=str(ag_id),
                        iteration=iteration_counter,
                        root_causes=_l5_marker_root_causes,
                        target_qids=tuple(
                            str(q) for q in (ag.get("affected_questions") or [])
                            if str(q)
                        ),
                    ), flush=True)
                except Exception:
                    logger.debug(
                        "P4: structural_gate_dropped_marker emit failed (non-fatal)",
                        exc_info=True,
                    )

                # P3 task 3+4 wiring — force structural synthesis when
                # the lever-5 structural gate drops an instruction-only
                # proposal for a SQL-shape root cause. Closes the iter-2
                # / iter-5 silent-skip path in run
                # 2423b960-16e8-41d4-a0cb-74c563378e05. Same-iteration
                # injection: a synthesized add_example_sql is appended
                # to ``all_proposals`` so it flows through the existing
                # normalization / applyability / applier pipeline. On
                # failure, a NO_STRUCTURAL_CANDIDATE record is emitted
                # so the transcript shows synthesis was attempted.
                try:
                    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
                        run_cluster_driven_synthesis_for_single_cluster,
                    )
                    from genie_space_optimizer.optimization.decision_emitters import (
                        no_structural_candidate_record,
                    )

                    for _drop in _l5_ag_drops:
                        _drop_cluster: dict | None = None
                        _drop_root_cause = ""
                        for _rc in (_drop.get("root_causes") or ()):
                            if not _should_force_structural_synthesis(
                                gate_drop_reason=(
                                    "lever5_structural_sql_shape_no_example_sql"
                                ),
                                cluster_root_cause=str(_rc),
                            ):
                                continue
                            for _cid in (_drop.get("source_clusters") or ()):
                                _cand = _iter_source_clusters_by_id.get(str(_cid))
                                if isinstance(_cand, dict) and str(
                                    _cand.get("root_cause") or ""
                                ) == str(_rc):
                                    _drop_cluster = _cand
                                    _drop_root_cause = str(_rc)
                                    break
                            if _drop_cluster is not None:
                                break
                        if _drop_cluster is None:
                            continue
                        _synth_result = run_cluster_driven_synthesis_for_single_cluster(
                            _drop_cluster,
                            metadata_snapshot,
                            benchmarks=benchmarks,
                            catalog=catalog,
                            gold_schema=schema,
                            warehouse_id=resolve_warehouse_id(""),
                            w=w,
                            spark=spark,
                        )
                        if _synth_result.proposal is not None:
                            _sp = _synth_result.proposal
                            _forced_proposal = {
                                "proposal_id": f"P{len(all_proposals) + 1:03d}",
                                "cluster_id": f"{ag_id}_FORCED_SYN",
                                "lever": 5,
                                "scope": "genie_config",
                                "patch_type": "add_example_sql",
                                "change_description": (
                                    f"[{ag_id}] Forced structural synthesis: "
                                    f"{str(_sp.get('example_question', ''))[:80]}"
                                ),
                                "proposed_value": _sp.get("example_question", ""),
                                "example_question": _sp.get("example_question", ""),
                                "example_sql": _sp.get("example_sql", ""),
                                "parameters": _sp.get("parameters", []) or [],
                                "usage_guidance": _sp.get("usage_guidance", ""),
                                "rationale": (
                                    f"Forced structural synthesis at L5 gate "
                                    f"drop (archetype="
                                    f"{_sp.get('_archetype_name', '?')}). "
                                    f"Root cause: {_drop_root_cause}."
                                ),
                                "confidence": 0.85,
                                "questions_fixed": 1,
                                "questions_at_risk": 0,
                                "net_impact": 0.85,
                                "kit_id": _sp.get("kit_id", ""),
                                "target_qids": _sp.get("target_qids", []),
                                "rca_id": _sp.get("rca_id", ""),
                                "_archetype_name": _sp.get("_archetype_name", ""),
                                "_cluster_id": _sp.get("_cluster_id", ""),
                                "provenance": {
                                    "synthesis_source": "forced_lever5_drop",
                                    "drop_root_cause": _drop_root_cause,
                                    "kit_id": _sp.get("kit_id", ""),
                                    "target_qids": _sp.get("target_qids", []),
                                },
                            }
                            all_proposals.append(_forced_proposal)
                            logger.info(
                                "P3: forced structural synthesis succeeded "
                                "for AG=%s root_cause=%s archetype=%s",
                                ag_id, _drop_root_cause,
                                _sp.get("_archetype_name", "?"),
                            )
                        else:
                            _nsc = no_structural_candidate_record(
                                run_id=run_id,
                                iteration=iteration_counter,
                                ag_id=str(ag_id),
                                cluster_id=str(
                                    _drop_cluster.get("cluster_id") or ""
                                ),
                                rca_id=_l5_ag_rca_id,
                                root_cause=_drop_root_cause,
                                target_qids=tuple(
                                    str(q) for q in (
                                        ag.get("affected_questions") or []
                                    )
                                    if str(q)
                                ),
                                attempted_archetypes=(
                                    _synth_result.attempted_archetypes
                                ),
                            )
                            _current_iter_inputs.setdefault(
                                "decision_records", []
                            ).append(_nsc.to_dict())
                            try:
                                from genie_space_optimizer.optimization.run_analysis_contract import (
                                    no_structural_candidate_marker,
                                )
                                print(no_structural_candidate_marker(
                                    ag_id=str(ag_id),
                                    iteration=iteration_counter,
                                    attempted_archetypes=(
                                        _synth_result.attempted_archetypes
                                    ),
                                ), flush=True)
                            except Exception:
                                logger.debug(
                                    "P4: no_structural_candidate_marker emit "
                                    "failed (non-fatal)",
                                    exc_info=True,
                                )
                            logger.info(
                                "P3: forced structural synthesis produced no "
                                "candidate for AG=%s root_cause=%s "
                                "skipped=%s archetypes=%s",
                                ag_id, _drop_root_cause,
                                _synth_result.skipped_reason,
                                _synth_result.attempted_archetypes,
                            )
                except Exception:
                    _phase_b_producer_exceptions[
                        "forced_structural_synthesis"
                    ] = (
                        _phase_b_producer_exceptions.get(
                            "forced_structural_synthesis", 0
                        ) + 1
                    )
                    logger.debug(
                        "P3: forced-structural-synthesis at L5 drop "
                        "site failed (non-fatal)",
                        exc_info=True,
                    )
                    if _phase_b_strict_mode():
                        raise
        except Exception:
            _phase_b_producer_exceptions["lever5_structural_gate"] = (
                _phase_b_producer_exceptions.get("lever5_structural_gate", 0) + 1
            )
            logger.debug(
                "Cycle 8 3b: lever5_structural_gate_records failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Task 4 — patch-survival snapshot: proposed gate.
        _survival_proposed = list(all_proposals)

        # ── T2.2: Reflection-as-validator ────────────────────────────
        # Build a per-patch forbidden set from prior rolled-back iterations
        # and drop any proposal whose (patch_type, target) signature was
        # already rolled back. Without this the strategist routinely
        # re-proposes the same patch type against the same table after a
        # content regression (observed: iter-1 + iter-3 of a real run
        # both patched ``mv_<domain>_fact_<entity>.description`` →
        # rolled back → iter-3 re-proposed ``update_description`` on
        # the same table).
        #
        # Escape hatch: a proposal carrying
        # ``escalation_justification: <non-empty>`` bypasses the filter,
        # and every rejection is logged so operators can see what was
        # dropped and why. The existing cluster-level DO-NOT-RETRY
        # (_compute_forbidden_ag_set) covers lever/root-cause combos;
        # this new per-patch guard covers patch-type/target combos.
        # Task 18 — precise reflection retry. Build the forbidden set
        # using ``patch_retry_signature`` (column-/section-level) so a
        # rolled-back patch on column ``A`` of table ``T`` does not
        # block a fresh patch on column ``B`` of the same table. The
        # coarse ``(ptype, target)`` set is kept in parallel for the
        # rewrite-bypass emission below.
        from genie_space_optimizer.optimization.reflection_retry import (
            patch_retry_signature,
            retry_allowed_after_rollback,
        )

        _patch_forbidden: set[tuple[str, str]] = set()
        _patch_forbidden_signatures: set[tuple] = set()
        _rolled_back_patches_for_retry: list[dict] = []
        _content_rollback_cause: str = ""
        for _rb in reflection_buffer:
            if _rb.get("accepted"):
                continue
            # CONTENT_REGRESSION rollbacks are the ones that signal "the
            # patch made things worse". Other classes (infra, schema)
            # don't indicate the patch was wrong, only that applying it
            # blew up.
            from genie_space_optimizer.optimization.rollback_class import (
                RollbackClass as _RC,
            )
            if _rb.get("rollback_class") != _RC.CONTENT_REGRESSION.value:
                continue
            _content_rollback_cause = str(_rb.get("rollback_class") or "")
            for _dnr in _rb.get("do_not_retry", []):
                _s = str(_dnr).strip()
                if " on " not in _s:
                    continue
                _ptype, _target = _s.split(" on ", 1)
                _patch_forbidden.add((_ptype.strip(), _target.strip()))
            # Task 18 — precise patch signatures for the rolled-back
            # patches stored on the reflection entry.
            for _rb_patch in _rb.get("do_not_retry_patches", []) or []:
                if isinstance(_rb_patch, dict):
                    _rolled_back_patches_for_retry.append(_rb_patch)
                    _patch_forbidden_signatures.add(
                        patch_retry_signature(_rb_patch)
                    )

        # B1.3 — diagnostics so an empty ``_patch_forbidden`` is
        # debuggable: distinguish (a) no rollbacks yet, (b) all
        # rollbacks classified non-CONTENT_REGRESSION, (c)
        # CONTENT_REGRESSION rollbacks but empty ``do_not_retry``.
        from genie_space_optimizer.optimization.rollback_class import (
            RollbackClass as _RC_DIAG,
        )
        _total_rb = sum(
            1 for r in reflection_buffer if not r.get("accepted")
        )
        _content_rb = sum(
            1 for r in reflection_buffer
            if not r.get("accepted")
            and r.get("rollback_class") == _RC_DIAG.CONTENT_REGRESSION.value
        )
        _content_rb_with_dnr = sum(
            1 for r in reflection_buffer
            if not r.get("accepted")
            and r.get("rollback_class") == _RC_DIAG.CONTENT_REGRESSION.value
            and r.get("do_not_retry")
        )
        logger.info(
            "[%s] T2.2 forbidden set: size=%d  rollbacks_total=%d  "
            "content_rollbacks=%d  with_do_not_retry=%d",
            ag_id, len(_patch_forbidden), _total_rb, _content_rb,
            _content_rb_with_dnr,
        )

        # PR-E Task 3 — content-fingerprint dedup runs irrespective of
        # rollback_class so byte-identical re-proposals get blocked even
        # when the rollback was infra/insufficient-gain (which leaves
        # ``do_not_retry`` empty and lets the existing _patch_forbidden
        # match miss the duplicate). Build the all-rollbacks list inline
        # because _rolled_back_patches_for_retry filters to
        # CONTENT_REGRESSION only.
        _all_rolled_back_patches_for_dedup: list[dict] = []
        for _rb in reflection_buffer:
            if _rb.get("accepted"):
                continue
            for _rb_patch in _rb.get("do_not_retry_patches", []) or []:
                if isinstance(_rb_patch, dict):
                    _all_rolled_back_patches_for_dedup.append(_rb_patch)
        all_proposals, _content_dedup_dropped = (
            _drop_proposals_matching_rolled_back_content_fingerprints(
                proposals=all_proposals,
                rolled_back_patches=_all_rolled_back_patches_for_dedup,
            )
        )
        if _content_dedup_dropped:
            logger.info(
                "[%s] PR-E content-fingerprint dedup dropped %d proposals",
                ag_id, len(_content_dedup_dropped),
            )

        if _patch_forbidden:
            from genie_space_optimizer.common.config import (
                ENFORCE_REFLECTION_REVALIDATION,
            )
            _kept: list[dict] = []
            _dropped: list[tuple[str, str, str]] = []  # (ptype, target, reason)
            _reflection_rewrites: list[dict] = []  # for audit emission below
            # Map ``(ptype, target)`` → previous proposal_id so we can
            # link parent_proposal_id when a rewrite passes the bypass.
            _prev_proposal_ids: dict[tuple[str, str], str] = {}
            for _rb in reflection_buffer:
                if _rb.get("accepted"):
                    continue
                for _entry in _rb.get("do_not_retry", []) or []:
                    _es = str(_entry).strip()
                    if " on " in _es:
                        _ept, _etgt = _es.split(" on ", 1)
                        _prev_proposal_ids.setdefault(
                            (_ept.strip(), _etgt.strip()),
                            str(_rb.get("ag_id") or ""),
                        )
            for _p in all_proposals:
                _ptype = str(_p.get("type") or _p.get("patch_type") or "")
                # B1.1 — raw proposals carry ``table`` (column-level
                # patches) before ``proposals_to_patches`` populates
                # ``target``. Without reading ``table`` here, T2.2
                # extracts ``"?"`` and never matches the forbidden
                # set entries (which use the FQN target string).
                _target = str(
                    _p.get("target") or _p.get("target_object")
                    or _p.get("target_table")
                    or _p.get("table")
                    or "?"
                )
                _key = (_ptype, _target)
                _justification = str(_p.get("escalation_justification") or "").strip()
                # Task 18 — precise signature short-circuit. If the
                # patch's column-/section-level ``patch_retry_signature``
                # is NOT in the rolled-back set, ``retry_allowed_after_rollback``
                # returns ``allowed=True`` and we keep the proposal even if
                # the coarse ``(ptype, target)`` key matches.
                _precise_sig = patch_retry_signature(_p)
                if (
                    _key in _patch_forbidden
                    and _precise_sig not in _patch_forbidden_signatures
                ):
                    _retry_decision = retry_allowed_after_rollback(
                        current_patch=_p,
                        rolled_back_patches=_rolled_back_patches_for_retry,
                        rollback_cause=_content_rollback_cause,
                    )
                    if _retry_decision.allowed:
                        logger.info(
                            "[%s] T2.2 precise retry allowed: ptype=%s target=%s "
                            "reason=%s",
                            ag_id, _ptype, _target, _retry_decision.reason,
                        )
                        _kept.append(_p)
                        continue
                if _key in _patch_forbidden:
                    if not _justification:
                        _dropped.append((_ptype, _target,
                                         "rolled back previously (no escalation_justification)"))
                        continue
                    if (
                        ENFORCE_REFLECTION_REVALIDATION
                        and len(_justification) < 16
                    ):
                        # Task 10: a one-word justification is not
                        # enough evidence. Require concrete reasoning
                        # so the bypass is auditable, not free.
                        _dropped.append((_ptype, _target,
                                         "escalation_justification too short to be concrete"))
                        continue
                    # Task 10: this is a reflection rewrite. Treat as
                    # a brand-new proposal: stamp fresh proposal_id,
                    # link parent_proposal_id for attribution, mark
                    # the rewrite flag so downstream gates and the
                    # audit trail can recognise it.
                    _orig_pid = str(_p.get("proposal_id") or "")
                    _parent_pid = (
                        _prev_proposal_ids.get(_key) or _orig_pid or ""
                    )
                    _new_pid = f"{_orig_pid or 'rewrite'}:rev{iteration_counter}"
                    _p["parent_proposal_id"] = _parent_pid
                    _p["proposal_id"] = _new_pid
                    _p["is_reflection_rewrite"] = True
                    _p["requires_full_revalidation"] = True
                    _reflection_rewrites.append({
                        "ptype": _ptype,
                        "target": _target,
                        "parent_proposal_id": _parent_pid,
                        "proposal_id": _new_pid,
                        "justification": _justification[:240],
                        "cluster_id": _p.get("cluster_id"),
                    })
                    _kept.append(_p)
                else:
                    _kept.append(_p)
            if _dropped:
                logger.warning(
                    "[%s] T2.2 reflection-as-validator dropped %d proposal(s) "
                    "that were rolled back in prior iterations without an "
                    "escalation_justification:",
                    ag_id, len(_dropped),
                )
                for _ptype, _target, _reason in _dropped:
                    logger.warning("  - %s on %s (%s)", _ptype, _target, _reason)
                print(
                    _section(
                        f"[{ag_id}] T2.2 Reflection validator: dropped "
                        f"{len(_dropped)} re-proposal(s)",
                        "-",
                    )
                )
                for _ptype, _target, _reason in _dropped:
                    print(f"|  - {_ptype} on {_target} ({_reason})")
                print(_bar("-"))
            # Task 10: emit a ``reflection_rewrite`` decision audit
            # row for every rewrite that survived the bypass. Lets
            # operators query "show me every AG where the strategist
            # re-tried a previously-rolled-back patch with a fresh
            # justification" without log scraping.
            if _reflection_rewrites:
                try:
                    from genie_space_optimizer.optimization.state import (
                        write_lever_loop_decisions as _t10_audit,
                    )
                    _t10_rows: list[dict] = []
                    for _idx, _rw in enumerate(_reflection_rewrites, start=1):
                        _t10_rows.append({
                            "run_id": run_id,
                            "iteration": iteration_counter,
                            "ag_id": ag_id,
                            "decision_order": _idx,
                            "stage_letter": "G",
                            "gate_name": "reflection_rewrite",
                            "decision": "accepted",
                            "reason_code": "escalation_justification_supplied",
                            "reason_detail": (
                                f"{_rw['ptype']} on {_rw['target']}: "
                                f"{_rw['justification']}"
                            )[:2000],
                            "proposal_ids": [_rw["proposal_id"]],
                            "source_cluster_ids": (
                                [_rw["cluster_id"]] if _rw.get("cluster_id") else []
                            ),
                            "metrics": {
                                "patch_type": _rw["ptype"],
                                "target": _rw["target"],
                                "parent_proposal_id": _rw["parent_proposal_id"],
                            },
                        })
                    if _t10_rows:
                        _t10_audit(
                            spark, _t10_rows,
                            catalog=catalog, schema=schema,
                        )
                except Exception:
                    logger.debug(
                        "Task 10 reflection_rewrite audit emission "
                        "failed (non-fatal)",
                        exc_info=True,
                    )
                logger.info(
                    "[%s] T2.2 reflection-as-validator accepted %d "
                    "rewrite(s) with escalation_justification; each will "
                    "re-run grounding (Task 5) + counterfactual + apply "
                    "gates as a brand-new proposal.",
                    ag_id, len(_reflection_rewrites),
                )
            all_proposals = _kept

            # Phase A — Lossless contract: stamp dropped_at_reflection for
            # patches the reflection retry signature filtered out. Per-qid
            # attribution falls back to the AG's affected_questions because
            # the reflection drop tuples carry only (ptype, target, reason).
            try:
                if _dropped:
                    _ag_affected = [
                        str(q) for q in (ag.get("affected_questions") or []) if q
                    ]
                    _emit_gate_drop_journey(
                        emit=_journey_emit,
                        gate="reflection",
                        dropped=[
                            {
                                "proposal_id": "",
                                "patch_type": str(_ptype or ""),
                                "cluster_id": "",
                                "target_qids": list(_ag_affected),
                                "_drop_reason": str(_reason or ""),
                            }
                            for (_ptype, _target, _reason) in _dropped
                        ],
                    )
            except Exception:
                logger.debug(
                    "Phase A: reflection-gate journey emit failed (non-fatal)",
                    exc_info=True,
                )

        # ── T2.4: Counterfactual asset-impact scan ───────────────────
        # Before applying, identify passing benchmarks that reference
        # each patch's target asset. If a patch touches an asset that
        # many passing questions depend on, we're rolling the dice on
        # those questions — stamp ``high_collateral_risk`` on the
        # proposal and warn prominently. Downstream the slice-gate
        # (once T3.1 lands) should prioritise the at-risk set.
        _passing_qids = set(b.get("id") for b in benchmarks if b.get("id")) - (prev_failure_qids or set())
        _affected_qids = set(ag.get("affected_questions", []) or [])
        _affected_n = max(len(_affected_qids), 1)
        # Phase 3c Task A: scan body extracted into _t24_counterfactual_scan
        # so the per-proposal stamp logic can be exercised in isolation.
        # The helper stamps ``passing_dependents`` on every visited
        # proposal (including instruction rewrites with no target table)
        # and returns the legacy ``_collateral_details`` shape used by
        # the summary print below.
        _collateral_details = _t24_counterfactual_scan(
            all_proposals=all_proposals,
            benchmarks=benchmarks,
            ag=ag,
            prev_failure_qids=prev_failure_qids or set(),
        )
        # B2 — ALWAYS print a summary so operators can distinguish
        # "scan ran, nothing flagged" from "scan didn't run".
        _total_with_metadata = sum(
            1 for _b in benchmarks if _b.get("required_tables")
        )
        logger.info(
            "[%s] T2.4 counterfactual scan: %d/%d proposal(s) high-risk; "
            "benchmarks_with_required_tables=%d/%d (SQL-text fallback used otherwise)",
            ag_id, len(_collateral_details), len(all_proposals),
            _total_with_metadata, len(benchmarks),
        )
        print(
            _section(
                f"[{ag_id}] T2.4 Counterfactual scan: "
                f"{len(_collateral_details)} high-risk proposal(s)",
                "-",
            )
        )
        print(
            _kv(
                "Benchmarks with required_tables",
                f"{_total_with_metadata}/{len(benchmarks)}",
            )
        )
        if _collateral_details:
            for _ptype, _target, _deps in _collateral_details:
                print(
                    f"|  - {_ptype} on {_target}  "
                    f"(passing dependents: {len(_deps)}+, "
                    f"affected: {_affected_n})"
                )
                print(f"|    sample deps: {', '.join(_deps[:5])}")
        print(_bar("-"))

        # ── Log proposals ────────────────────────────────────────────
        _n_valid = 0
        _n_failed = 0
        proposal_lines = [_section(f"[{ag_id}] Proposals ({len(all_proposals)} total)", "-"), "|"]
        for pi, p in enumerate(all_proposals, 1):
            cluster_id = p.get("cluster_id", "?")
            ptype = p.get("type", p.get("patch_type", "?"))
            rationale = str(p.get("rationale", ""))
            proposed_value = str(p.get("proposed_value", ""))
            table = p.get("table", "")
            column = p.get("column", "")
            status = _classify_proposal_log_status(p)
            if status == "FAILED (non-JSON)":
                _n_failed += 1
            elif status == "INVALID_TARGET":
                _n_failed += 1
            else:
                _n_valid += 1

            proposal_lines.append(f"|  Proposal {pi} / {len(all_proposals)}  [{cluster_id}]")
            proposal_lines.append(f"|    {'Type:':<24s} {ptype}")
            proposal_lines.append(f"|    {'Lever:':<24s} {p.get('lever', '?')}")
            if table:
                proposal_lines.append(f"|    {'Table:':<24s} {table}")
            if column:
                proposal_lines.append(f"|    {'Column:':<24s} {column}")
            proposal_lines.append(f"|    {'Rationale:':<24s} {rationale[:200]}")
            _p_col_sect = p.get("column_sections")
            _p_tbl_sect = p.get("table_sections")
            if isinstance(_p_col_sect, dict) and _p_col_sect:
                proposal_lines.append(f"|    Sections proposed:")
                for _sk, _sv in _p_col_sect.items():
                    _sv_str = str(_sv).replace("\n", " ")
                    proposal_lines.append(f"|      {_sk}: \"{_sv_str[:100]}\"")
            elif isinstance(_p_tbl_sect, dict) and _p_tbl_sect:
                proposal_lines.append(f"|    Table sections proposed:")
                for _sk, _sv in _p_tbl_sect.items():
                    _sv_str = str(_sv).replace("\n", " ")
                    proposal_lines.append(f"|      {_sk}: \"{_sv_str[:100]}\"")
            elif proposed_value:
                _val_preview = proposed_value.replace("\n", "\\n")
                proposal_lines.append(f"|    {'Value (preview):':<24s} {_val_preview[:300]}")
            proposal_lines.append(f"|    {'Status:':<24s} {status}")
            proposal_lines.append("|")

        proposal_lines.append("|  --- Summary ---")
        proposal_lines.append(f"|    {'Valid proposals:':<24s} {_n_valid} of {len(all_proposals)}")
        if _n_failed:
            proposal_lines.append(f"|    {'Failed (non-JSON):':<24s} {_n_failed}")
        proposal_lines.append(f"|    Proceeding with {_n_valid} patch(es)")
        proposal_lines.append(_bar("-"))
        print("\n".join(proposal_lines))

        # ── Provenance log ───────────────────────────────────────────
        _prov_patch_lines = ["\n-- Patch Provenance " + "-" * 58]
        for pi, p in enumerate(all_proposals, 1):
            prov = p.get("provenance", {})
            if not prov:
                continue
            cid = prov.get("cluster_id", "?")
            rc = prov.get("root_cause", "?")
            lv = prov.get("lever", "?")
            ln = prov.get("lever_name", "?")
            pt = prov.get("patch_type", "?")
            _prov_patch_lines.append(f"|  P{pi:03d} [{cid}] lever={lv} ({ln}) type={pt} root_cause={rc}")
        _prov_patch_lines.append("-" * 78)
        print("\n".join(_prov_patch_lines))

        _prop_mappings = [
            {"cluster_id": p.get("cluster_id"), "proposal_id": p.get("proposal_id"), "patch_type": p.get("patch_type"), "lever": p.get("lever")}
            for p in all_proposals if p.get("cluster_id")
        ]
        try:
            update_provenance_proposals(spark, run_id, iteration_counter - 1, _prop_mappings, catalog, schema)
        except Exception:
            logger.debug("Failed to update provenance proposals", exc_info=True)

        if not all_proposals:
            print(_section(f"[{ag_id}] No proposals — SKIPPING iteration", "-"))
            write_stage(
                spark, run_id, f"AG_{ag_id}_STARTED", "SKIPPED",
                task_key="lever_loop", iteration=iteration_counter,
                detail={"reason": "no_proposals", "levers": lever_keys},
                catalog=catalog, schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[], target_objects=[], prev_scores=best_scores,
                new_scores=best_scores, rollback_reason="no_proposals", patches=[],
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
                **_ag_identity_kwargs,
            ))
            _render_current_journey()
            continue

        # Task 6A — RCA/patch-type compatibility gate. Drop proposals
        # whose patch type cannot fix the cluster's RCA defect (e.g. a
        # measure patch for a missing-filter defect).
        try:
            from genie_space_optimizer.optimization.proposal_grounding import (
                proposal_is_defect_compatible,
            )

            _compatible_proposals: list[dict] = []
            _incompatible_proposals: list[dict] = []
            for _p in all_proposals:
                _decision = proposal_is_defect_compatible(_p)
                if _decision["compatible"]:
                    _compatible_proposals.append(_p)
                else:
                    _incompatible_proposals.append({
                        "proposal_id": str(_p.get("proposal_id") or _p.get("id") or "?"),
                        "patch_type": str(_p.get("patch_type") or _p.get("type") or "?"),
                        "rca_kind": _decision.get("rca_kind"),
                        "reason": _decision["reason"],
                    })
            if _incompatible_proposals:
                print(
                    _section(f"[{ag_id}] DEFECT-COMPATIBILITY GATE", "-") + "\n"
                    + _kv("Proposals dropped", len(_incompatible_proposals)) + "\n"
                    + "\n".join(
                        f"|  - {d['proposal_id']} ({d['patch_type']}): "
                        f"rca={d['rca_kind']} reason={d['reason']}"
                        for d in _incompatible_proposals
                    ) + "\n"
                    + _bar("-")
                )
                logger.warning(
                    "AG %s defect-compatibility gate dropped %d proposal(s)",
                    ag_id,
                    len(_incompatible_proposals),
                )
            all_proposals = _compatible_proposals
        except Exception:
            logger.debug(
                "Defect-compatibility gate failed (non-fatal)",
                exc_info=True,
            )

        # ── Apply coordinated patch set ──────────────────────────────
        try:
            from genie_space_optimizer.optimization.proposal_shape import (
                normalize_column_proposals,
            )

            _uc_columns_for_shape = (
                metadata_snapshot.get("_uc_columns", [])
                if isinstance(metadata_snapshot, dict)
                else []
            )
            # Phase A — capture pre-normalize proposals so the journey
            # emit can map dropped proposal_ids back to their target qids.
            _pre_normalize_proposals = list(all_proposals)
            all_proposals, _shape_decisions = normalize_column_proposals(
                all_proposals,
                uc_columns=_uc_columns_for_shape,
            )
            if _shape_decisions:
                print(
                    _section(f"[{ag_id}] RCA COLUMN SHAPE NORMALIZATION", "-") + "\n"
                    + _kv("Decisions", len(_shape_decisions)) + "\n"
                    + "\n".join(
                        f"|  - {d['proposal_id']} ({d['patch_type']}): "
                        f"{d['decision']} reason={d['reason']} outputs={d['output_count']}"
                        for d in _shape_decisions[:12]
                    ) + "\n"
                    + _bar("-")
                )
                _rca_shape_drop_reasons = {
                    "missing_table_for_column",
                    "missing_column",
                    "invalid_column_target",
                    "ambiguous_table_for_column",
                }
                _rca_shape_drops = [
                    d for d in _shape_decisions
                    if d.get("decision") == "dropped"
                    and d.get("reason") in _rca_shape_drop_reasons
                ]
                if _rca_shape_drops:
                    logger.warning(
                        "[%s] rca_theme_shape_dropped: %d malformed column "
                        "proposal(s) dropped during normalization; reasons=%s",
                        ag_id,
                        len(_rca_shape_drops),
                        sorted({str(d.get("reason")) for d in _rca_shape_drops}),
                    )
                    for _drop in _rca_shape_drops:
                        _audit_emit(
                            stage_letter="G",
                            gate_name="rca_column_shape_normalization",
                            decision="reject",
                            reason_code="rca_theme_shape_dropped",
                            reason_detail=(
                                f"proposal_id={_drop.get('proposal_id')} "
                                f"reason={_drop.get('reason')}"
                            ),
                            affected_qids=[],
                            metrics={
                                "proposal_id": _drop.get("proposal_id"),
                                "patch_type": _drop.get("patch_type"),
                                "reason": _drop.get("reason"),
                                "output_count": _drop.get("output_count"),
                            },
                        )
        except Exception:
            logger.debug(
                "RCA column proposal normalization failed (non-fatal)",
                exc_info=True,
            )

        # Phase A — Lossless contract: stamp dropped_at_normalize for
        # every proposal that was filtered by normalize_column_proposals.
        try:
            _shape_dropped_ids = {
                str(d.get("proposal_id") or "")
                for d in (locals().get("_shape_decisions") or [])
                if d.get("decision") == "dropped" and d.get("proposal_id")
            }
            if _shape_dropped_ids:
                _proposals_by_id = {
                    str(p.get("proposal_id") or p.get("id") or ""): p
                    for p in (locals().get("_pre_normalize_proposals") or [])
                }
                _dropped_normalize_proposals = [
                    {
                        "proposal_id": pid,
                        "patch_type": str(
                            (_proposals_by_id.get(pid) or {}).get("patch_type")
                            or (_proposals_by_id.get(pid) or {}).get("type")
                            or ""
                        ),
                        "cluster_id": str(
                            (_proposals_by_id.get(pid) or {}).get("cluster_id") or ""
                        ),
                        "target_qids": list(
                            (_proposals_by_id.get(pid) or {}).get("target_qids") or []
                        ),
                        "_grounding_target_qids": list(
                            (_proposals_by_id.get(pid) or {}).get("_grounding_target_qids") or []
                        ),
                        "_drop_reason": next(
                            (
                                str(d.get("reason") or "")
                                for d in (_shape_decisions or [])
                                if str(d.get("proposal_id") or "") == pid
                                and d.get("decision") == "dropped"
                            ),
                            "",
                        ),
                    }
                    for pid in _shape_dropped_ids
                ]
                _emit_gate_drop_journey(
                    emit=_journey_emit,
                    gate="normalize",
                    dropped=_dropped_normalize_proposals,
                )
        except Exception:
            logger.debug(
                "Phase A: normalize-gate journey emit failed (non-fatal)",
                exc_info=True,
            )

        patches = proposals_to_patches(all_proposals)

        # Phase A — populate ``patches`` on the matching AG snapshot for
        # replay-fixture export. Match by ag_id; use the most recent
        # snapshot (this iteration's append).
        try:
            _ag_snapshots = (
                _current_iter_inputs["strategist_response"]["action_groups"]
            )
            for _snap in reversed(_ag_snapshots):
                if str(_snap.get("id")) == str(ag_id):
                    _ag_affected_qids = [
                        str(q) for q in (ag.get("affected_questions") or []) if q
                    ]
                    _snap["patches"] = [
                        {
                            "proposal_id": str(_p.get("proposal_id") or _p.get("id") or ""),
                            "patch_type": str(_p.get("patch_type") or _p.get("type") or ""),
                            "target_qids": _patch_snapshot_target_qids(
                                _p, _ag_affected_qids,
                            ),
                            "cluster_id": str(_p.get("cluster_id") or ""),
                        }
                        for _p in (all_proposals or [])
                    ]
                    break
        except Exception:
            logger.debug(
                "Phase A: patch capture for replay fixture failed (non-fatal)",
                exc_info=True,
            )

        # Phase A — Lossless contract: stamp ag_assigned for every qid
        # this AG targets, before any 'proposed' event fires. The
        # contract requires AG_ASSIGNED between CLUSTERED and PROPOSED.
        try:
            _ag_assigned_qids = sorted({
                str(q)
                for _p in (all_proposals or [])
                for q in (
                    _p.get("_grounding_target_qids")
                    or _p.get("target_qids")
                    or []
                )
                if q
            })
            if not _ag_assigned_qids:
                _ag_assigned_qids = [
                    str(q) for q in (ag.get("affected_questions") or []) if q
                ]
            _emit_ag_assignment_journey(
                emit=_journey_emit,
                ag_id=str(ag_id),
                affected_qids=_ag_assigned_qids,
            )
        except Exception:
            logger.debug(
                "Phase A: ag_assigned journey emit failed (non-fatal)",
                exc_info=True,
            )

        # Phase F+H A2 (v2): F4 action_groups — additive observability
        # with atomic dedup. Replaces inline _strategist_ag_records with
        # the stage call which emits the same STRATEGIST_AG_EMITTED
        # records via ctx.decision_emit per stages/action_groups.py:
        # 83-84.
        #
        # Verified against: stages/action_groups.py:32-51 (Input), 68-89
        # (select body), harness.py:12280-12291 (_iter_rca_id_by_cluster),
        # harness:567 (_build_ag_alternatives_by_id helper).
        #
        # Phase D.5 Task 6 alternatives builder hoisted out of the inline
        # producer try-block (v1 stayed inside; v2 needs the result for
        # F4's ag_alternatives_by_id input).
        _ag_alts_by_id = _build_ag_alternatives_by_id(
            strategist_returned_ags=(
                list(strategist_returned_ags)
                if "strategist_returned_ags" in locals()
                else [ag]
            ),
            emitted_ag_ids=[str(ag.get("id") or ag.get("ag_id") or "")],
        )

        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                is_strict_mode as _phase_b_strict_mode,
            )
            from genie_space_optimizer.optimization.stages import (
                StageContext as _StageCtx,
            )
            from genie_space_optimizer.optimization.stages import (
                action_groups as _ags_stage,
            )

            _stage_ctx_a2 = _StageCtx(
                run_id=str(run_id),
                iteration=int(iteration_counter),
                space_id=str(space_id),
                domain=str(domain),
                catalog=str(catalog),
                schema=str(schema),
                apply_mode=str(apply_mode),
                journey_emit=_journey_emit,
                decision_emit=_decision_emit,
                mlflow_anchor_run_id=_phase_h_anchor_run_id,  # C17 v2 — activates Phase B capture
                feature_flags={},
            )
            # Spine stage 4 — Action Group Selection. Consumes the
            # carry-over from the prior iteration's spine stage 10
            # (Learning) via ``prior_buckets_by_qid``. When the bucket
            # map is non-empty, ``select`` drops MODEL_CEILING qids
            # from each AG's target set and tags AGs whose remaining
            # qids are all EVIDENCE_GAP with ``ag_kind=
            # "evidence_gathering"``. Iter 1 sees an empty map (no
            # carry-over yet), so the slate is unfiltered.
            _ags_inp = _ags_stage.ActionGroupsInput(
                action_groups=tuple([ag]),
                source_clusters_by_id={
                    str(_c.get("cluster_id") or ""): _c
                    for _c in (clusters or [])
                    if _c.get("cluster_id")
                },
                rca_id_by_cluster=dict(_iter_rca_id_by_cluster),
                ag_alternatives_by_id={
                    k: tuple(v) for k, v in (_ag_alts_by_id or {}).items()
                },
                prior_buckets_by_qid=dict(_prior_buckets_by_qid),
                # Cycle 5 T2 — gate-drops carrying a causal-target
                # patch from the prior iteration. Threaded
                # unconditionally (the dataclass field accepts an empty
                # tuple as default); the strategist prompt-builder
                # consumer surfaces these only when
                # GSO_CAUSAL_DROP_FEEDBACK_TO_STRATEGIST is on, so
                # passing the tuple here with the flag off is byte-
                # stable. Iter 1 sees ().
                prior_iteration_dropped_causal_patches=tuple(
                    _prior_iteration_dropped_causal_patches
                ),
            )
            # Phase F+H Commit B11: wrap F4 with stage_io_capture
            # decorator. Replay-byte-stable — wrap_with_io_capture
            # returns the stage output unchanged; MLflow log_text
            # calls are no-ops while mlflow_anchor_run_id is None
            # (C17 wires the anchor on real runs).
            from genie_space_optimizer.optimization.stage_io_capture import (
                wrap_with_io_capture as _wrap_with_io_capture_a2,
            )
            _ags_wrapped = _wrap_with_io_capture_a2(
                execute=_ags_stage.execute,
                stage_key="action_group_selection",
            )
            _ag_slate = _ags_wrapped(_stage_ctx_a2, _ags_inp)
            # NOTE: F4 stage emits the same records the inline producer
            # did, but ActionGroupSlate does NOT expose them as a tuple.
            # The pre-A2 harness incremented _phase_b_target_qids_missing_
            # count from _ag_records here. After A2, the counter stays at
            # 0 for this iteration (records still flow into Optimization
            # Trace via _decision_emit; only the counter aggregation is
            # lost). TODO follow-up: extend ActionGroupSlate with a
            # records tuple OR re-derive from _current_iter_inputs[
            # "decision_records"] tail.
        except Exception:
            _iter_producer_exceptions["strategist_ag"] += 1
            _phase_b_producer_exceptions["strategist_ag"] = (
                _phase_b_producer_exceptions.get("strategist_ag", 0) + 1
            )
            logger.debug(
                "Phase F+H A2 v2: action_groups stage failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Phase C Task 5 — RCA-groundedness gate at AG level.
        # Observability-first: emits one GATE_DECISION/DROPPED record
        # per AG that fails the groundedness predicate, but does NOT
        # remove the AG from the iteration's pipeline. Existing patch-
        # cap and blast-radius gates remain authoritative.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                groundedness_gate_records as _groundedness_gate_records,
            )
            from genie_space_optimizer.optimization.rca import (
                rca_findings_from_clusters as _rca_findings_from_clusters_c5,
            )
            from genie_space_optimizer.optimization.rca_groundedness import (
                is_rca_grounded as _is_rca_grounded_c5,
            )

            _phase_c_findings_ag = _rca_findings_from_clusters_c5(clusters or [])
            _ag_verdict = _is_rca_grounded_c5(
                ag, _phase_c_findings_ag, target_kind="ag",
            )
            if not _ag_verdict.accepted:
                _ag_root_cause_c5 = ""
                _ag_rca_id_c5 = ""
                for _cid in (ag.get("source_cluster_ids") or []):
                    _ag_rca_id_c5 = str(
                        _iter_rca_id_by_cluster.get(str(_cid)) or ""
                    )
                    if _ag_rca_id_c5:
                        break
                _gate_records_ag = _groundedness_gate_records(
                    run_id=run_id,
                    iteration=iteration_counter,
                    drops=[{
                        "ag_id": str(ag.get("id") or ag.get("ag_id") or ""),
                        "proposal_id": "",
                        "target_qids": list(ag.get("affected_questions") or []),
                        "rca_id": _ag_rca_id_c5,
                        "root_cause": _ag_root_cause_c5,
                        "target_kind": "ag",
                        "verdict": _ag_verdict,
                    }],
                )
                _current_iter_inputs.setdefault(
                    "decision_records", []
                ).extend([r.to_dict() for r in _gate_records_ag])
        except Exception:
            _phase_b_producer_exceptions["groundedness_ag"] = (
                _phase_b_producer_exceptions.get("groundedness_ag", 0) + 1
            )
            logger.debug(
                "Phase C: AG groundedness gate failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Task 13 — emit ``proposed`` events for every proposal that
        # survived to ``proposals_to_patches``. Use both
        # ``_grounding_target_qids`` and ``target_qids`` so we capture
        # the full causal target set even when one is empty.
        try:
            for _p in (all_proposals or []):
                _ptids = list(_p.get("_grounding_target_qids") or [])
                if not _ptids:
                    _ptids = list(_p.get("target_qids") or [])
                _ptids = [str(q) for q in _ptids if q]
                if not _ptids:
                    continue
                # Plan N1 Task 4 — stamp parent_proposal_id so the
                # validator's lane-key collapses this ``proposed``
                # event into the same lane as the matching ``applied_*``
                # / ``dropped_at_cap`` events keyed on the expanded
                # child id. ``_p`` is at the proposal level so its
                # ``proposal_id`` is the parent today.
                _proposed_pid = str(
                    _p.get("proposal_id") or _p.get("id") or ""
                )
                _journey_emit(
                    "proposed",
                    question_ids=_ptids,
                    proposal_id=_proposed_pid,
                    parent_proposal_id=str(
                        _p.get("parent_proposal_id")
                        or _p.get("source_proposal_id")
                        or _proposed_pid
                    ),
                    patch_type=str(
                        _p.get("patch_type") or _p.get("type") or ""
                    ),
                    cluster_id=str(_p.get("cluster_id") or ""),
                )
        except Exception:
            logger.debug(
                "Task 13: proposed journey emit failed (non-fatal)",
                exc_info=True,
            )

        # Phase F+H A3 (v2): F5 proposals — additive observability with
        # atomic dedup. Replaces inline _proposal_generated_records (in
        # the deleted block) with the stage call which emits
        # PROPOSAL_GENERATED via ctx.decision_emit per
        # stages/proposals.py:128-129 and stamps content_fingerprint per
        # :108-115 using the SAME reflection_retry.patch_retry_signature
        # function the harness already used at :14311 (algorithm parity
        # confirmed — NO all_proposals replacement needed).
        #
        # Verified against: stages/proposals.py:39-55 (Input), 95-135
        # (generate body), harness.py:14311 (PR-E content_fingerprint
        # stamping site, same algorithm), harness.py:12280-12291
        # (_iter_rca_id_by_cluster).
        #
        # Phase D.5 Task 7 alternatives builder hoisted out of the
        # deleted inline producer try-block (v1 stayed inside; v2 needs
        # the result for F5's proposal_alternatives_by_ag input).
        _proposal_alts = _build_proposal_alternatives_for_ag(
            raw_proposals=(
                list(_raw_proposals_for_ag)
                if "_raw_proposals_for_ag" in locals()
                else (all_proposals or [])
            ),
            surviving_proposal_ids=[
                str(p.get("proposal_id") or p.get("id") or "")
                for p in (all_proposals or [])
            ],
        )

        try:
            from genie_space_optimizer.optimization.stages import (
                StageContext as _StageCtx,
            )
            from genie_space_optimizer.optimization.stages import (
                proposals as _prop_stage,
            )

            _stage_ctx_a3 = _StageCtx(
                run_id=str(run_id),
                iteration=int(iteration_counter),
                space_id=str(space_id),
                domain=str(domain),
                catalog=str(catalog),
                schema=str(schema),
                apply_mode=str(apply_mode),
                journey_emit=_journey_emit,
                decision_emit=_decision_emit,
                mlflow_anchor_run_id=_phase_h_anchor_run_id,  # C17 v2 — activates Phase B capture
                feature_flags={},
            )
            _cluster_root_cause_by_id = {
                str(_c.get("cluster_id") or ""): str(_c.get("root_cause") or "")
                for _c in (clusters or [])
                if _c.get("cluster_id")
            }
            _prop_inp = _prop_stage.ProposalsInput(
                proposals_by_ag={
                    str(ag_id): tuple(all_proposals or []),
                },
                rca_id_by_cluster=dict(_iter_rca_id_by_cluster),
                cluster_root_cause_by_id=_cluster_root_cause_by_id,
                proposal_alternatives_by_ag={
                    str(ag_id): tuple(_proposal_alts or []),
                },
            )
            # Phase F+H Commit B12: wrap F5 with stage_io_capture
            # decorator. Replay-byte-stable — wrap_with_io_capture
            # returns the stage output unchanged; MLflow log_text
            # calls are no-ops while mlflow_anchor_run_id is None
            # (C17 wires the anchor on real runs).
            from genie_space_optimizer.optimization.stage_io_capture import (
                wrap_with_io_capture as _wrap_with_io_capture_a3,
            )
            _prop_wrapped = _wrap_with_io_capture_a3(
                execute=_prop_stage.execute,
                stage_key="proposal_generation",
            )
            _prop_slate = _prop_wrapped(_stage_ctx_a3, _prop_inp)
            # _prop_slate is observability-only: F6 (deferred) would
            # consume _prop_slate.proposals_by_ag (fingerprint-stamped)
            # when wired. Until F6 lands, the harness's all_proposals
            # (already fingerprinted by :14311) is the canonical input
            # to downstream gates — DO NOT replace.
        except Exception:
            _iter_producer_exceptions["proposal_generated"] += 1
            _phase_b_producer_exceptions["proposal_generated"] = (
                _phase_b_producer_exceptions.get("proposal_generated", 0) + 1
            )
            logger.debug(
                "Phase F+H A3 v2: proposals stage failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Phase C Task 5 — RCA-groundedness gate at proposal level.
        # Same observability-first contract as the AG-level gate above.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                groundedness_gate_records as _groundedness_gate_records_p,
            )
            from genie_space_optimizer.optimization.rca import (
                rca_findings_from_clusters as _rca_findings_from_clusters_c5p,
            )
            from genie_space_optimizer.optimization.rca_groundedness import (
                is_rca_grounded as _is_rca_grounded_c5p,
            )

            _phase_c_findings_p = _rca_findings_from_clusters_c5p(clusters or [])
            _proposal_drops_c5: list[dict] = []
            for _prop in (all_proposals or []):
                _prop_id = str(_prop.get("proposal_id") or "")
                if not _prop_id:
                    continue
                _verdict_p = _is_rca_grounded_c5p(
                    _prop, _phase_c_findings_p, target_kind="proposal",
                )
                if _verdict_p.accepted:
                    continue
                _proposal_drops_c5.append({
                    "ag_id": str(ag.get("id") or ""),
                    "proposal_id": _prop_id,
                    "target_qids": list(_prop.get("target_qids") or []),
                    "rca_id": str(_prop.get("rca_id") or ""),
                    "root_cause": str(_prop.get("root_cause") or ""),
                    "target_kind": "proposal",
                    "verdict": _verdict_p,
                })
            if _proposal_drops_c5:
                _gate_records_p = _groundedness_gate_records_p(
                    run_id=run_id,
                    iteration=iteration_counter,
                    drops=_proposal_drops_c5,
                )
                _current_iter_inputs.setdefault(
                    "decision_records", []
                ).extend([r.to_dict() for r in _gate_records_p])
        except Exception:
            _phase_b_producer_exceptions["groundedness_proposal"] = (
                _phase_b_producer_exceptions.get("groundedness_proposal", 0) + 1
            )
            logger.debug(
                "Phase C: proposal groundedness gate failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Task 4 — patch-survival snapshot: normalized gate.
        _survival_normalized = list(patches)

        # Phase 4.3: expand ``rewrite_instruction`` proposals into
        # ``update_instruction_section`` children BEFORE the cap so a
        # single rewrite_instruction proposal does not balloon past the
        # cap inside ``apply_patch_set``. Idempotent — applier will not
        # re-split children that already carry ``_split_from``.
        try:
            from genie_space_optimizer.optimization.applier import (
                _expand_rewrite_splits as _harness_expand_splits,
            )
            _pre_split_count = len(patches)
            patches = _harness_expand_splits(patches)
            if len(patches) > _pre_split_count:
                logger.info(
                    "Phase 4.3: rewrite splits expanded patch list "
                    "%d -> %d before diversity-aware cap",
                    _pre_split_count, len(patches),
                )
        except Exception:
            logger.debug(
                "Phase 4.3: pre-cap rewrite split expansion failed (non-fatal)",
                exc_info=True,
            )

        # Task 5: ground proposals against failing-question surfaces.
        # AG2 in the retail run shipped 8-patch bundles whose targets
        # (e.g. ``zone_combination``) did not appear in the failing
        # questions' SQL or NL surface. Drop patches that cannot
        # plausibly affect any failing question before the diversity
        # cap so the resulting bundle is causally auditable.
        _dominant_grounding_category = ""
        try:
            from genie_space_optimizer.common.config import (
                MIN_PROPOSAL_RELEVANCE,
            )
            from genie_space_optimizer.optimization.control_plane import (
                rows_for_qids,
                target_qids_from_action_group,
            )
            from genie_space_optimizer.optimization.proposal_grounding import (
                causal_relevance_score as _patch_relevance,
                explain_causal_relevance as _explain_patch_relevance,
            )

            _all_rows_for_grounding = _get_failure_rows(
                spark, run_id, catalog, schema,
            )
            _ag_target_qids = target_qids_from_action_group(
                ag,
                strategy.get("_source_clusters", []),
            )
            _rows_for_grounding = (
                rows_for_qids(_all_rows_for_grounding, _ag_target_qids)
                if _ag_target_qids else list(_all_rows_for_grounding)
            )
            _audit_decisions_grounding: list[tuple[dict, float, str]] = []
            _grounded: list[dict] = []
            _grounding_debug_by_idx: list[dict] = []
            _grounding_debug_rows: list[dict] = []
            for _patch in patches:
                try:
                    _rca_exec = ag.get("_rca_execution") or {}
                    if isinstance(_rca_exec, dict):
                        _patch["_rca_grounding_terms"] = sorted(set(
                            list(_patch.get("_rca_grounding_terms") or [])
                            + list(_rca_exec.get("grounding_terms") or [])
                        ))
                except Exception:
                    logger.debug("Failed to stamp RCA grounding terms", exc_info=True)
                _debug = _explain_patch_relevance(
                    _patch,
                    _rows_for_grounding,
                    target_qids=_ag_target_qids,
                    min_relevance=MIN_PROPOSAL_RELEVANCE,
                )
                _score = float(_debug.get("score", 0.0))
                _patch["_grounding_target_qids"] = list(_ag_target_qids)
                _patch["relevance_score"] = round(_score, 3)
                _patch["_grounding_failure_category"] = _debug.get("failure_category")
                if _score >= MIN_PROPOSAL_RELEVANCE:
                    _grounded.append(_patch)
                    _audit_decisions_grounding.append((_patch, _score, "kept"))
                else:
                    _audit_decisions_grounding.append((_patch, _score, "dropped"))
                _grounding_debug_by_idx.append(_debug)
                _grounding_debug_rows.append({
                    "patch_type": _patch.get("type") or _patch.get("patch_type"),
                    "target": (
                        _patch.get("column")
                        or _patch.get("target")
                        or _patch.get("section_name")
                        or "?"
                    ),
                    "score": round(_score, 3),
                    "category": _debug.get("failure_category"),
                    "scoped_row_count": len(_rows_for_grounding),
                    "surface_size": _debug.get("surface_size"),
                    "overlap": list(_debug.get("overlap") or [])[:8],
                    "rca_overlap": list(_debug.get("rca_overlap") or [])[:8],
                })

            _dropped = [d for d in _audit_decisions_grounding if d[2] == "dropped"]
            _dropped_grounding_patches = [
                patch for patch, _score, decision in _audit_decisions_grounding
                if decision == "dropped"
            ]
            _grounding_categories = [
                str(patch.get("_grounding_failure_category") or "")
                for patch in _dropped_grounding_patches
                if str(patch.get("_grounding_failure_category") or "")
            ]
            _dominant_grounding_category = (
                max(set(_grounding_categories), key=_grounding_categories.count)
                if _grounding_categories else ""
            )
            if _dropped:
                logger.info(
                    "Task 5 grounding [%s]: dropped %d/%d ungrounded patches "
                    "(min_relevance=%.2f)",
                    ag_id, len(_dropped), len(patches), MIN_PROPOSAL_RELEVANCE,
                )
                print(
                    _section(
                        f"PROPOSAL GROUNDING [{ag_id}]: kept {len(_grounded)} of "
                        f"{len(patches)}", "-",
                    ) + "\n"
                    + _kv(
                        "Dropped (ungrounded)",
                        ", ".join(
                            f"{p.get('type', '?')}:{p.get('column', p.get('target', '?'))}"
                            f" (rel={s:.2f})"
                            for p, s, _d in _dropped[:5]
                        ) + (
                            f" (+{len(_dropped) - 5} more)" if len(_dropped) > 5 else ""
                        ),
                    ) + "\n"
                    + _kv(
                        "Grounding debug",
                        "; ".join(
                            f"{d['patch_type']}:{d['target']} cat={d['category']} "
                            f"rows={d['scoped_row_count']} surface={d['surface_size']} "
                            f"overlap={d['overlap']} rca={d['rca_overlap']}"
                            for d in _grounding_debug_rows[:3]
                        ),
                    ) + "\n"
                    + _bar("-")
                )
            patches = _grounded

            # Phase A — Lossless contract: stamp dropped_at_grounding for
            # every dropped patch's target qids so the validator sees a
            # legal proposed → dropped_at_grounding transition.
            try:
                _emit_gate_drop_journey(
                    emit=_journey_emit,
                    gate="grounding",
                    dropped=[
                        {
                            "proposal_id": str(p.get("proposal_id") or p.get("id") or ""),
                            "patch_type": str(p.get("patch_type") or p.get("type") or ""),
                            "cluster_id": str(p.get("cluster_id") or ""),
                            "target_qids": list(p.get("target_qids") or []),
                            "_grounding_target_qids": list(
                                p.get("_grounding_target_qids") or []
                            ),
                            "_drop_reason": "ungrounded",
                        }
                        for p in (_dropped_grounding_patches or [])
                    ],
                )
            except Exception:
                logger.debug(
                    "Phase A: grounding-gate journey emit failed (non-fatal)",
                    exc_info=True,
                )

            try:
                from genie_space_optimizer.optimization.rca_decision_trace import (
                    format_patch_inventory,
                )

                print(
                    _section(f"PROPOSAL INVENTORY [{ag_id}]", "-") + "\n"
                    + _kv("AG target QIDs", list(_ag_target_qids)) + "\n"
                    + _kv("Grounded patch count", len(patches)) + "\n"
                    + _kv("Patches", format_patch_inventory(patches)) + "\n"
                    + _bar("-")
                )
            except Exception:
                logger.debug("Failed to print proposal inventory", exc_info=True)

            # Emit a per-patch ``proposal_grounding`` decision row for
            # each kept/dropped decision so the Task-3 audit chain
            # ``cluster -> proposal -> grounding -> apply -> accept``
            # is queryable end-to-end.
            try:
                from genie_space_optimizer.optimization.state import (
                    write_lever_loop_decisions as _write_decisions,
                )
                _grounding_rows: list[dict] = []
                for _idx, (_patch, _score, _dec) in enumerate(
                    _audit_decisions_grounding, start=1,
                ):
                    _debug_for_row = (
                        _grounding_debug_by_idx[_idx - 1]
                        if _idx - 1 < len(_grounding_debug_by_idx)
                        else _explain_patch_relevance(
                            _patch,
                            _rows_for_grounding,
                            target_qids=_ag_target_qids,
                            min_relevance=MIN_PROPOSAL_RELEVANCE,
                        )
                    )
                    _category_for_row = _debug_for_row.get("failure_category")
                    _grounding_rows.append({
                        "run_id": run_id,
                        "iteration": iteration_counter,
                        "ag_id": ag_id,
                        "decision_order": _idx,
                        "stage_letter": "H",
                        "gate_name": "proposal_grounding",
                        "decision": (
                            "accepted" if _dec == "kept" else "dropped"
                        ),
                        "reason_code": (
                            None if _dec == "kept" else (_category_for_row or "below_min_relevance")
                        ),
                        "metrics": {
                            "relevance_score": round(float(_score), 3),
                            "min_relevance": MIN_PROPOSAL_RELEVANCE,
                            "patch_type": _patch.get("type"),
                            "target": (
                                _patch.get("column")
                                or _patch.get("target")
                                or _patch.get("metric")
                                or _patch.get("instruction_section")
                            ),
                            "lever": _patch.get("lever"),
                            "rca_id": _patch.get("rca_id"),
                            "patch_family": _patch.get("patch_family"),
                            "target_qids": _patch.get("target_qids", []),
                            "ag_target_qids": list(_ag_target_qids),
                            "scoped_row_count": len(_rows_for_grounding),
                            "failure_category": _category_for_row,
                            "debug": _debug_for_row,
                        },
                        "proposal_ids": (
                            [_patch.get("proposal_id")]
                            if _patch.get("proposal_id") else []
                        ),
                        "source_cluster_ids": (
                            [_patch.get("cluster_id")]
                            if _patch.get("cluster_id") else []
                        ),
                    })
                if _grounding_rows:
                    _write_decisions(
                        spark, _grounding_rows, catalog=catalog, schema=schema,
                    )
            except Exception:
                logger.debug(
                    "Failed to persist proposal_grounding decision rows",
                    exc_info=True,
                )
        except Exception:
            logger.debug(
                "Task 5 proposal grounding failed (non-fatal); proceeding with "
                "all patches.",
                exc_info=True,
            )

        _grounding_skip = _should_skip_eval_for_patch_bundle(
            patches=patches,
            apply_log=None,
            stage="post_grounding",
        )
        if _grounding_skip.skip:
            logger.warning(
                "[%s] Skipping acceptance eval: %s",
                ag_id,
                _grounding_skip.reason_detail,
            )
            print(
                _section(f"[{ag_id}] SKIP EVAL: NO GROUNDED PATCHES", "!") + "\n"
                + _kv("Reason", _grounding_skip.reason_detail) + "\n"
                + _bar("!")
            )
            write_stage(
                spark,
                run_id,
                f"AG_{ag_id}_NO_GROUNDED_PATCHES",
                "SKIPPED",
                task_key="lever_loop",
                iteration=iteration_counter,
                detail={"reason_code": _grounding_skip.reason_code},
                catalog=catalog,
                schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter,
                ag_id=ag_id,
                accepted=False,
                levers=[int(lk) for lk in lever_keys],
                target_objects=[],
                prev_scores=best_scores,
                new_scores=best_scores,
                rollback_reason=_grounding_skip.reason_code,
                patches=[],
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
                extra={
                    "rca_execution": ag.get("_rca_execution", {}),
                    "grounding_failure_stage": "post_grounding",
                    "grounding_failure_reason": _grounding_skip.reason_code,
                    "grounding_failure_category": _dominant_grounding_category,
                    "rca_next_action": _next_grounding_action_payload(
                        rollback_reason=_grounding_skip.reason_code,
                        grounding_failure_category=_dominant_grounding_category,
                        repeated_count=1,
                    ),
                },
                **_ag_identity_kwargs,
            ))
            _render_current_journey()
            continue

        # Task 2 — Blast-radius gate. The counterfactual scan above stamps
        # ``passing_dependents`` and ``high_collateral_risk`` on each
        # proposal; turn those informational stamps into a deterministic
        # gate so high-blast-radius patches are dropped before they reach
        # the patch cap and ship.
        try:
            from genie_space_optimizer.optimization.proposal_grounding import (
                instruction_patch_scope_is_safe as _instruction_scope_is_safe,
                patch_blast_radius_is_safe,
            )
            from genie_space_optimizer.optimization.control_plane import (
                target_qids_from_action_group as _target_qids_for_blast,
            )

            _blast_target_qids = _target_qids_for_blast(
                ag,
                strategy.get("_source_clusters", []),
            )
            # Cycle 2 Task 2: collect every currently-hard qid across
            # all clusters so blast-radius can downgrade rejects whose
            # outside-target dependents are themselves hard (shared-
            # cause beneficiaries).
            _live_hard_qids_for_blast = tuple(
                str(q)
                for cluster in (clusters or [])
                for q in (cluster.get("question_ids") or [])
                if str(q)
            )
            _blast_kept: list[dict] = []
            _blast_dropped: list[dict] = []
            for _candidate in patches:
                _decision = patch_blast_radius_is_safe(
                    _candidate,
                    ag_target_qids=_blast_target_qids,
                    max_outside_target=0,
                    live_hard_qids=_live_hard_qids_for_blast,
                )
                if not _decision["safe"]:
                    _blast_dropped.append({
                        "proposal_id": str(
                            _candidate.get("proposal_id")
                            or _candidate.get("id")
                            or "?"
                        ),
                        "patch_type": str(
                            _candidate.get("type")
                            or _candidate.get("patch_type")
                            or "?"
                        ),
                        "reason": _decision["reason"],
                        "passing_dependents_outside_target": _decision.get(
                            "passing_dependents_outside_target", []
                        ),
                        # Cycle 9 T5: surface the patch's target table
                        # so record_blast_radius_drop can capture it
                        # for cross-iteration forbid_tables.
                        "target": str(
                            _candidate.get("target")
                            or _candidate.get("table")
                            or ""
                        ),
                    })
                    continue
                # Task 2A — second classifier for broad instruction rewrites
                # that have no counterfactual dependents.
                _scope_decision = _instruction_scope_is_safe(
                    _candidate,
                    ag_target_qids=_blast_target_qids,
                )
                if not _scope_decision["safe"]:
                    _blast_dropped.append({
                        "proposal_id": str(
                            _candidate.get("proposal_id")
                            or _candidate.get("id")
                            or "?"
                        ),
                        "patch_type": str(
                            _candidate.get("type")
                            or _candidate.get("patch_type")
                            or "?"
                        ),
                        "reason": _scope_decision["reason"],
                        "passing_dependents_outside_target": [],
                        "target": str(
                            _candidate.get("target")
                            or _candidate.get("table")
                            or ""
                        ),
                    })
                    continue
                _blast_kept.append(_candidate)
            if _blast_dropped:
                print(
                    _section(f"[{ag_id}] BLAST-RADIUS GATE", "-") + "\n"
                    + _kv("Patches dropped", len(_blast_dropped)) + "\n"
                    + _kv(
                        "AG target QIDs",
                        list(_blast_target_qids) or "(none)",
                    ) + "\n"
                    + "\n".join(
                        f"|  - {d['proposal_id']} ({d['patch_type']}): "
                        f"reason={d['reason']}, "
                        f"outside_target={d['passing_dependents_outside_target']}"
                        for d in _blast_dropped
                    ) + "\n"
                    + _bar("-")
                )
                logger.warning(
                    "AG %s blast-radius gate dropped %d/%d patches: %s",
                    ag_id,
                    len(_blast_dropped),
                    len(patches),
                    [d["proposal_id"] for d in _blast_dropped[:8]],
                )
                # Cycle 9 T5: record the dropped tables as forbidden for
                # the next strategist call on this AG so it doesn't
                # re-propose the same shape against the same table.
                try:
                    from genie_space_optimizer.optimization.strategist_constraints import (
                        record_blast_radius_drop,
                    )
                    record_blast_radius_drop(
                        constraints=_strategist_constraints,
                        ag_id=str(ag_id),
                        dropped_patches=_blast_dropped,
                    )
                except Exception:
                    logger.debug(
                        "Cycle9 T5: strategist_constraints update skipped "
                        "(non-fatal)",
                        exc_info=True,
                    )
                # Cycle 9 T6: emit one DecisionRecord per blast-radius
                # drop so the iteration's decision_records is non-empty
                # even when no patches survive to the patch-cap.
                try:
                    from genie_space_optimizer.optimization.decision_emitters import (
                        blast_radius_decision_records,
                        is_strict_mode,
                    )
                    _br_root_cause = ""
                    _br_rca_id = ""
                    for _cid in (ag.get("source_cluster_ids") or []):
                        _br_cluster = (
                            _iter_source_clusters_by_id.get(str(_cid)) or {}
                        )
                        if not _br_root_cause:
                            _br_root_cause = str(
                                _br_cluster.get("root_cause") or ""
                            )
                        if not _br_rca_id:
                            _br_rca_id = str(
                                _iter_rca_id_by_cluster.get(str(_cid)) or ""
                            )
                        if _br_root_cause and _br_rca_id:
                            break
                    _br_target_qids = [
                        str(q)
                        for q in (ag.get("affected_questions") or [])
                        if q
                    ]
                    _br_records = blast_radius_decision_records(
                        run_id=run_id,
                        iteration=iteration_counter,
                        ag_id=str(ag_id),
                        rca_id=_br_rca_id,
                        root_cause=_br_root_cause,
                        target_qids=_br_target_qids,
                        dropped=_blast_dropped,
                    )
                    _current_iter_inputs.setdefault(
                        "decision_records", []
                    ).extend([r.to_dict() for r in _br_records])
                except Exception:
                    _phase_b_producer_exceptions["blast_radius"] = (
                        _phase_b_producer_exceptions.get("blast_radius", 0)
                        + 1
                    )
                    logger.debug(
                        "blast-radius DecisionRecord emission failed "
                        "(non-fatal)",
                        exc_info=True,
                    )
                    if is_strict_mode():
                        raise

                # Cycle 5 T2 — capture every blast-radius drop whose
                # target_qids overlap the AG's causal target as a typed
                # DroppedCausalPatch so the next iteration's strategist
                # can see what was rejected. Capture is unconditional
                # (cheap; just appends to ``_iter_dropped_causal``);
                # consumption by the strategist prompt is gated by the
                # flag at the AG construction site, preserving byte-
                # stability with the flag off.
                try:
                    from genie_space_optimizer.optimization.stages.gates import (
                        DroppedCausalPatch as _DroppedCausalPatch,
                    )
                    _t2_target_qids = tuple(
                        str(q) for q in (ag.get("target_qids") or ()) if q
                    )
                    if _t2_target_qids:
                        for _drop in _blast_dropped or ():
                            _t2_dependents = tuple(
                                str(q)
                                for q in (
                                    _drop.get("passing_dependents_outside_target")
                                    or ()
                                )
                            )
                            _iter_dropped_causal.append(_DroppedCausalPatch(
                                gate="blast_radius",
                                reason=str(_drop.get("reason") or ""),
                                proposal_id=str(_drop.get("proposal_id") or ""),
                                patch_type=str(_drop.get("patch_type") or ""),
                                target=str(_drop.get("target") or ""),
                                target_qids=_t2_target_qids,
                                dependents_outside_target=_t2_dependents,
                                rca_id=str(_br_rca_id or ""),
                                root_cause=str(_br_root_cause or ""),
                            ))
                except Exception:
                    logger.debug(
                        "Cycle 5 T2: dropped-causal capture failed "
                        "(non-fatal)",
                        exc_info=True,
                    )
            patches = _blast_kept
        except ImportError:
            # instruction_patch_scope_is_safe not yet implemented (Task 2A
            # lands separately) — proceed with blast-radius gate alone.
            from genie_space_optimizer.optimization.proposal_grounding import (
                patch_blast_radius_is_safe,
            )
            from genie_space_optimizer.optimization.control_plane import (
                target_qids_from_action_group as _target_qids_for_blast,
            )

            _blast_target_qids = _target_qids_for_blast(
                ag,
                strategy.get("_source_clusters", []),
            )
            # Cycle 2 Task 2: shared-cause-aware blast radius — pass
            # the full live-hard set so the gate can downgrade rejects
            # whose outside-target dependents are themselves hard.
            _live_hard_qids_for_blast = tuple(
                str(q)
                for cluster in (clusters or [])
                for q in (cluster.get("question_ids") or [])
                if str(q)
            )
            _blast_kept = []
            _blast_dropped = []
            for _candidate in patches:
                _decision = patch_blast_radius_is_safe(
                    _candidate,
                    ag_target_qids=_blast_target_qids,
                    max_outside_target=0,
                    live_hard_qids=_live_hard_qids_for_blast,
                )
                if _decision["safe"]:
                    _blast_kept.append(_candidate)
                else:
                    _blast_dropped.append({
                        "proposal_id": str(
                            _candidate.get("proposal_id") or "?"
                        ),
                        "patch_type": str(_candidate.get("type") or "?"),
                        "reason": _decision["reason"],
                        "passing_dependents_outside_target": _decision.get(
                            "passing_dependents_outside_target", []
                        ),
                    })
            if _blast_dropped:
                logger.warning(
                    "AG %s blast-radius gate dropped %d/%d patches: %s",
                    ag_id,
                    len(_blast_dropped),
                    len(patches),
                    [d["proposal_id"] for d in _blast_dropped[:8]],
                )
            patches = _blast_kept

        # Phase H Completion Task 4: wire F6 safety_gates as additive
        # observability after the three harness inline gate sites
        # (lever5_structural at the L5 emit site, rca_groundedness via
        # _run_groundedness_gate inside _build_proposals, and
        # blast_radius above). F6 sub-handlers emit zero DecisionRecords
        # (verified at stages/gates.py — zero ctx.decision_emit calls);
        # harness inline records remain authoritative. The wrap is purely
        # additive — wrap_with_io_capture returns the GateOutcome
        # unchanged; MLflow log_text calls are no-ops while
        # mlflow_anchor_run_id is None. Replay-byte-stable.
        try:
            from genie_space_optimizer.optimization.stages import (
                gates as _gates_stage,
                StageContext as _StageCtx_f6,
            )
            from genie_space_optimizer.optimization.stage_io_capture import (
                wrap_with_io_capture as _wrap_capture_f6,
            )

            _f6_inp = _gates_stage.GatesInput(
                proposals_by_ag={str(ag_id): tuple(patches or [])},
                ags=tuple([ag] if isinstance(ag, dict) else []),
                rca_evidence=(
                    dict(_rca_evidence_bundle.per_qid_evidence)
                    if "_rca_evidence_bundle" in dir()
                    and _rca_evidence_bundle is not None
                    else {}
                ),
                applied_history=tuple(),
                rolled_back_content_fingerprints=set(
                    _rolled_back_content_fingerprints
                ) if "_rolled_back_content_fingerprints" in dir() else set(),
                forbidden_signatures=set(),
                space_snapshot={},
            )
            _f6_stage_ctx = _StageCtx_f6(
                run_id=str(run_id),
                iteration=int(iteration_counter),
                space_id=str(space_id),
                domain=str(domain),
                catalog=str(catalog),
                schema=str(schema),
                apply_mode="real",
                journey_emit=lambda *a, **k: None,
                decision_emit=lambda r: None,
                mlflow_anchor_run_id=None,
                feature_flags={},
            )
            _f6_wrapped = _wrap_capture_f6(
                execute=_gates_stage.filter,
                stage_key="safety_gates",
            )
            _gate_outcome = _f6_wrapped(_f6_stage_ctx, _f6_inp)
        except Exception:
            logger.debug(
                "Phase H Task 4: F6 safety_gates stage failed (non-fatal)",
                exc_info=True,
            )

        # Task 5 — backfill AG/cluster causal metadata onto broad
        # strategist proposals so the cap can distinguish RCA-attributed
        # patches (tier 3) from broad AG-fallback patches (tier 1).
        patches = _backfill_patch_causal_metadata(
            patches=patches,
            action_group=ag,
            source_clusters=(
                strategy.get("_source_clusters", [])
                if isinstance(strategy, dict)
                else []
            ),
        )

        try:
            # Dry-run applyability gate. Drops patches with applyable=False
            # before the causal-first cap can rank them.
            from genie_space_optimizer.optimization.patch_applyability import (
                filter_applyable_patches,
            )

            _patches_before_applyability = list(patches)
            patches, _applyability_decisions = filter_applyable_patches(
                patches=_patches_before_applyability,
                metadata_snapshot=metadata_snapshot,
                space_id=space_id,
            )
            _non_applyable_decisions = [
                d for d in _applyability_decisions if not d.applyable
            ]
            if _non_applyable_decisions:
                print(
                    _section(f"[{ag_id}] PATCH APPLYABILITY GATE", "-") + "\n"
                    + _kv("Input patches", len(_patches_before_applyability)) + "\n"
                    + _kv("Applyable patches", len(patches)) + "\n"
                    + _kv("Dropped patches", len(_non_applyable_decisions)) + "\n"
                    + "\n".join(
                        f"|  - {d.expanded_patch_id or d.proposal_id} "
                        f"{d.patch_type} target={d.target or '(none)'} "
                        f"table={d.table or '(none)'} column={d.column or '(none)'} "
                        f"applyable={d.applyable} reason={d.reason}"
                        for d in _non_applyable_decisions[:12]
                    ) + "\n"
                    + _bar("-")
                )
                logger.warning(
                    "AG %s patch applyability gate dropped %d/%d patch(es)",
                    ag_id,
                    len(_non_applyable_decisions),
                    len(_patches_before_applyability),
                )
        except Exception:
            logger.debug(
                "Patch applyability gate failed (non-fatal)",
                exc_info=True,
            )

        # Task 4 — patch-survival snapshot: applyable gate.
        _survival_applyable = list(patches)

        # Phase A — Lossless contract: stamp dropped_at_applyability for
        # every patch the applyability gate dropped.
        try:
            _non_applyable = locals().get("_non_applyable_decisions") or []
            if _non_applyable:
                _emit_gate_drop_journey(
                    emit=_journey_emit,
                    gate="applyability",
                    dropped=[
                        {
                            "proposal_id": str(
                                getattr(d, "expanded_patch_id", None)
                                or getattr(d, "proposal_id", "")
                                or ""
                            ),
                            "patch_type": str(getattr(d, "patch_type", "") or ""),
                            "cluster_id": "",
                            "target_qids": list(getattr(d, "target_qids", []) or []),
                            "_drop_reason": str(getattr(d, "reason", "") or ""),
                        }
                        for d in _non_applyable
                    ],
                )
        except Exception:
            logger.debug(
                "Phase A: applyability-gate journey emit failed (non-fatal)",
                exc_info=True,
            )

        # Task 17 — L5/L6 asset alignment gate. SQL-shape (L5) and
        # sql-snippet (L6) patches must touch an asset that is in the
        # source cluster's lineage; otherwise they are dropped before the
        # cap so non-aligned patches don't burn cap slots.
        from genie_space_optimizer.optimization.proposal_asset_alignment import (
            l5_l6_patch_requires_asset_alignment,
            proposal_aligns_with_cluster,
        )

        _ag_source_cluster_ids = {
            str(cid).strip()
            for cid in (ag.get("source_cluster_ids") or [])
            if str(cid).strip()
        }
        _source_clusters_by_id = {
            str(c.get("cluster_id") or "").strip(): c
            for c in (strategy.get("_source_clusters") or [])
            if str(c.get("cluster_id") or "").strip()
        }
        _aligned_patches: list[dict] = []
        _alignment_drops: list[dict] = []
        for _p in patches:
            if not l5_l6_patch_requires_asset_alignment(_p):
                _aligned_patches.append(_p)
                continue
            _matched_cluster = next(
                (
                    _source_clusters_by_id[c]
                    for c in _ag_source_cluster_ids
                    if c in _source_clusters_by_id
                ),
                None,
            )
            _decision = proposal_aligns_with_cluster(_p, _matched_cluster)
            if _decision.get("aligned"):
                _aligned_patches.append(_p)
                continue
            _alignment_drops.append({
                "proposal_id": str(_p.get("proposal_id") or _p.get("id") or ""),
                "patch_type": str(_p.get("type") or _p.get("patch_type") or ""),
                "reason": _decision.get("reason"),
                "proposal_assets": list(_decision.get("proposal_assets") or ()),
                "cluster_assets": list(_decision.get("cluster_assets") or ()),
            })
        if _alignment_drops:
            logger.info(
                "[%s] asset_alignment_dropped: %d patch(es); reasons=%s",
                ag_id, len(_alignment_drops),
                [d["reason"] for d in _alignment_drops],
            )
        patches = _aligned_patches

        # Phase A — Lossless contract: stamp dropped_at_alignment for
        # every L5/L6 patch dropped by the asset-alignment gate.
        try:
            if _alignment_drops:
                # _alignment_drops carries proposal_id but not target_qids;
                # look them up in the pre-alignment patch list (the union
                # of pre-cap patches captured into _aligned_patches +
                # the dropped set).
                _pre_alignment_patches_by_id = {
                    str(_p.get("proposal_id") or _p.get("id") or ""): _p
                    for _p in (
                        list(_aligned_patches)
                        + [
                            _p
                            for _p in (
                                locals().get("_pre_normalize_proposals") or []
                            )
                            if isinstance(_p, dict)
                        ]
                    )
                }
                _emit_gate_drop_journey(
                    emit=_journey_emit,
                    gate="alignment",
                    dropped=[
                        {
                            "proposal_id": str(d.get("proposal_id") or ""),
                            "patch_type": str(d.get("patch_type") or ""),
                            "cluster_id": str(
                                (
                                    _pre_alignment_patches_by_id.get(
                                        str(d.get("proposal_id") or "")
                                    )
                                    or {}
                                ).get("cluster_id")
                                or ""
                            ),
                            "target_qids": list(
                                (
                                    _pre_alignment_patches_by_id.get(
                                        str(d.get("proposal_id") or "")
                                    )
                                    or {}
                                ).get("target_qids")
                                or []
                            ),
                            "_drop_reason": str(d.get("reason") or ""),
                        }
                        for d in _alignment_drops
                    ],
                )
        except Exception:
            logger.debug(
                "Phase A: alignment-gate journey emit failed (non-fatal)",
                exc_info=True,
            )

        # Tier 2.6: cap AG patch-set size. A single failing patch in a
        # large batch rolls back everything — including the patches that
        # would have helped. If the cap is exceeded, keep the highest-
        # confidence / highest-impact patches first (sorted by the
        # caller); extras are dropped with a clear warning.
        if len(patches) > MAX_AG_PATCHES:
            from genie_space_optimizer.optimization.patch_selection import (
                select_target_aware_causal_patch_cap,
            )
            from genie_space_optimizer.optimization.control_plane import (
                target_qids_from_action_group as _target_qids_for_patch_cap,
            )

            _patch_cap_target_qids = tuple(
                locals().get("_blast_target_qids")
                or _target_qids_for_patch_cap(ag, strategy.get("_source_clusters", []))
            )
            _active_cluster_ids_for_cap = tuple(
                str(cid).strip()
                for cid in (ag.get("source_cluster_ids") or [])
                if str(cid).strip()
            )
            _per_cluster_slot_floor = (
                1 if len(_active_cluster_ids_for_cap) > 1 else 0
            )

            _before_cap = list(patches)

            # Optimizer Control-Plane Hardening Plan — Task B: when
            # GSO_NO_CAUSAL_APPLYABLE_HALT is on and every RCA-matched
            # proposal in the AG has been dropped by upstream gates,
            # halt with reason no_causal_applyable_patch instead of
            # falling back to non-causal proposals. Default-off
            # preserves legacy behaviour.
            from genie_space_optimizer.common.config import (
                no_causal_applyable_halt_enabled as _no_causal_halt,
            )

            if _no_causal_halt():
                _causal_proposals, _had_rca_matched = (
                    _filter_to_causal_applyable_proposals(
                        ag=ag, proposals=_before_cap,
                    )
                )
                if (
                    not _causal_proposals
                    and not _had_rca_matched
                    and ag.get("rca_id")
                ):
                    logger.warning(
                        "[%s] no_causal_applyable_patch: every RCA-"
                        "matched proposal was dropped by upstream "
                        "gates; halting AG before patch_cap",
                        ag.get("id") or ag.get("ag_id"),
                    )
                    _audit_emit(
                        stage_letter="L",
                        gate_name="patch_cap",
                        decision="skipped",
                        reason_code="no_causal_applyable_patch",
                        metrics={"input_count": len(_before_cap)},
                    )
                    patches = []
                    _patch_cap_decisions = []
                else:
                    patches, _patch_cap_decisions = (
                        select_target_aware_causal_patch_cap(
                            _before_cap,
                            target_qids=_patch_cap_target_qids,
                            max_patches=MAX_AG_PATCHES,
                            active_cluster_ids=_active_cluster_ids_for_cap,
                            per_cluster_slot_floor=_per_cluster_slot_floor,
                        )
                    )
            else:
                patches, _patch_cap_decisions = (
                    select_target_aware_causal_patch_cap(
                        _before_cap,
                        target_qids=_patch_cap_target_qids,
                        max_patches=MAX_AG_PATCHES,
                        active_cluster_ids=_active_cluster_ids_for_cap,
                        per_cluster_slot_floor=_per_cluster_slot_floor,
                    )
                )
            _selected_ids = {
                str(p.get("proposal_id") or p.get("id") or "")
                for p in patches
            }
            _dropped_decisions = [
                d for d in _patch_cap_decisions
                if d.get("decision") == "dropped"
            ]
            for _d in _dropped_decisions:
                logger.info(
                    "[%s] cap_dropped pid=%s type=%s cluster=%s "
                    "rel=%.3f cluster_tier=%d direct=%s",
                    ag_id,
                    _d.get("proposal_id"),
                    _d.get("patch_type"),
                    _d.get("cluster_id"),
                    float(_d.get("relevance_score") or 0.0),
                    int(_d.get("active_cluster_match_tier") or 0),
                    _d.get("is_direct_behavior"),
                )

            # Task 13 — emit ``dropped_at_cap`` per drop, looking up the
            # target_qids of the original proposal in ``_before_cap``.
            # The drop reason is derived from the active-cluster match
            # tier (0 = not in active cluster).
            try:
                _by_pid: dict[str, dict] = {}
                for _bp in (_before_cap or []):
                    _bpid = str(
                        _bp.get("proposal_id") or _bp.get("id") or ""
                    )
                    if _bpid:
                        _by_pid[_bpid] = _bp
                for _d in _dropped_decisions:
                    _dpid = str(_d.get("proposal_id") or "")
                    _orig = _by_pid.get(_dpid, {})
                    _dt_qids = list(
                        _orig.get("_grounding_target_qids") or []
                    )
                    if not _dt_qids:
                        _dt_qids = list(_orig.get("target_qids") or [])
                    _dt_qids = [str(q) for q in _dt_qids if q]
                    _tier = int(_d.get("active_cluster_match_tier") or 0)
                    _drop_reason = (
                        "not_in_active_cluster" if _tier == 0
                        else f"cap_overflow_tier={_tier}"
                    )
                    if _dt_qids:
                        # Plan N1 Task 4 — parent_proposal_id collapse.
                        _dpid_parent = str(
                            _d.get("parent_proposal_id")
                            or _d.get("source_proposal_id")
                            or _orig.get("parent_proposal_id")
                            or _orig.get("proposal_id")
                            or _orig.get("id")
                            or _dpid
                        )
                        _journey_emit(
                            "dropped_at_cap",
                            question_ids=_dt_qids,
                            proposal_id=_dpid,
                            parent_proposal_id=_dpid_parent,
                            patch_type=str(_d.get("patch_type") or ""),
                            cluster_id=str(_d.get("cluster_id") or ""),
                            reason=_drop_reason,
                        )
            except Exception:
                logger.debug(
                    "Task 13: dropped_at_cap journey emit failed",
                    exc_info=True,
                )
            logger.warning(
                "AG %s patch cap (causal-first): kept %d of %d. "
                "Dropped proposal_ids=%s.",
                ag_id,
                len(patches),
                len(_before_cap),
                [d.get("proposal_id") for d in _dropped_decisions[:8]],
            )
            print(
                _section(f"[{ag_id}] PATCH CAP APPLIED (causal-first)", "-") + "\n"
                + _kv("Original size", len(_before_cap)) + "\n"
                + _kv("Kept", len(patches)) + "\n"
                + _kv(
                    "Selected proposal_ids",
                    sorted(pid for pid in _selected_ids if pid) or "(none)",
                ) + "\n"
                + _kv("Dropped count", len(_dropped_decisions)) + "\n"
                + _kv("Dropped shown", min(len(_dropped_decisions), 8)) + "\n"
                + _kv("Dropped truncated", len(_dropped_decisions) > 8) + "\n"
                + _kv(
                    "Dropped proposal_ids",
                    [d.get("proposal_id") for d in _dropped_decisions[:8]]
                    if _dropped_decisions else "(none)",
                ) + "\n"
                + _kv(
                    "Reason",
                    "Causal-first cap: relevance_score ranks before lever diversity.",
                ) + "\n"
                + _bar("-")
            )

            try:
                from genie_space_optimizer.optimization.rca_decision_trace import (
                    OptimizationTrace,
                    patch_cap_decision_records,
                    patch_cap_decision_rows,
                    render_operator_transcript,
                )
                from genie_space_optimizer.optimization.state import (
                    write_lever_loop_decisions as _write_decisions,
                )

                # Phase B Trace Plan Task 7 — capture typed records first so
                # the iteration snapshot persists them for replay/fixture.
                # Then convert back to legacy rows for the existing Delta
                # write path (write_lever_loop_decisions consumers haven't
                # migrated yet).
                _patch_cap_records = patch_cap_decision_records(
                    run_id=run_id,
                    iteration=iteration_counter,
                    ag_id=ag_id,
                    decisions=_patch_cap_decisions,
                )
                _current_iter_inputs.setdefault("decision_records", []).extend(
                    [r.to_dict() for r in _patch_cap_records]
                )

                _write_decisions(
                    spark,
                    patch_cap_decision_rows(
                        run_id=run_id,
                        iteration=iteration_counter,
                        ag_id=ag_id,
                        decisions=_patch_cap_decisions,
                    ),
                    catalog=catalog,
                    schema=schema,
                )
            except Exception:
                logger.debug("Failed to persist patch-cap decision rows", exc_info=True)

        # Task 4 — patch-survival snapshot: capped gate. Capture in both
        # branches so the ledger reflects the patch list that actually
        # reaches the applier, regardless of whether the cap fired.
        _survival_capped = list(patches)

        _selected_patch_signature = tuple(sorted(
            str(p.get("expanded_patch_id") or p.get("id") or p.get("proposal_id") or "")
            for p in patches
            if (p.get("expanded_patch_id") or p.get("id") or p.get("proposal_id"))
        ))
        # Cycle 2 Task 3: also compute the selected-proposal-ID
        # signature so DOA replay can be caught even when blast-radius
        # collapses the applied-patch signature to ``()``.
        _doa_selected_proposal_signature = (
            _compute_selected_proposal_signature(
                (ag.get("proposals") or []) if isinstance(ag, dict) else []
            )
        )
        _doa_selected_blocked = _is_doa_selected_signature_blocked(
            seen=_doa_selected_proposal_signatures,
            ag_id=str(ag_id),
            signature=_doa_selected_proposal_signature,
        )
        if (
            _selected_patch_signature in _dead_on_arrival_patch_signatures
            or _doa_selected_blocked
        ):
            _doa_dedup_reason = (
                "same selected proposal IDs already produced no applied patches"
                if _doa_selected_blocked
                else "same selected patch IDs already produced no applied patches"
            )
            logger.warning(
                "Skipping dead-on-arrival AG retry for %s "
                "(applied_sig=%s selected_sig=%s)",
                ag_id,
                _selected_patch_signature,
                _doa_selected_proposal_signature,
            )
            print(
                _section(f"[{ag_id}] DEAD-ON-ARRIVAL RETRY BLOCKED", "!") + "\n"
                + _kv("Patch signature", _selected_patch_signature) + "\n"
                + _kv("Selected proposal signature", _doa_selected_proposal_signature) + "\n"
                + _kv("Reason", _doa_dedup_reason) + "\n"
                + _bar("!")
            )
            # Phase A — Replay-fixture capture: the AG never reached the
            # gate, so no accept/rollback path runs. Without this entry
            # the iteration's ag_outcomes dict stays empty and the
            # fixture loses the signal that an AG was even attempted.
            try:
                _current_iter_inputs["ag_outcomes"][str(ag_id)] = (
                    "skipped_dead_on_arrival"
                )
            except Exception:
                logger.debug(
                    "Phase A: ag_outcome capture (dead-on-arrival) failed (non-fatal)",
                    exc_info=True,
                )
            _phase_b_emit_ag_outcome_record(ag, "skipped_dead_on_arrival")
            # Cycle 9 T1: selective drain — keep buffered AGs whose
            # affected_questions are disjoint from the failed AG's.
            _survivors, _dropped_buffered = _drain_buffered_action_groups(
                failed_ag=ag,
                buffered=pending_action_groups,
                reason="dead_on_arrival",
            )
            pending_action_groups = _survivors
            if not pending_action_groups:
                pending_strategy = None
            continue

        # T3.3: shadow apply. When enabled, the intent is to clone the
        # space, apply patches to the clone, eval, and promote only on
        # pass. The Genie SDK fork/promote primitives aren't wired yet,
        # so for now we log the intent and fall back to in-place apply;
        # the rollback path below still covers us. Leaving this here so
        # operators can see the flag is respected by code and we have a
        # single commit to wire the actual fork when available.
        if SHADOW_APPLY:
            logger.warning(
                "[%s] T3.3 SHADOW_APPLY=True but the Genie fork/promote "
                "API is not yet integrated in this harness; falling back "
                "to in-place apply with rollback-on-regression.",
                ag_id,
            )
            print(
                _section(f"[{ag_id}] SHADOW_APPLY: fallback", "-") + "\n"
                + _kv(
                    "Reason",
                    "Genie space-clone API not yet wired — rollback path "
                    "still covers content regressions.",
                ) + "\n"
                + _bar("-")
            )

        # Task 2 — capture the live parsed Genie config immediately before
        # patch application. This in-memory snapshot becomes the source of
        # truth for both ``rollback`` and ``verify_rollback_restored`` so
        # the rollback contract does not depend on Delta state that may be
        # missing or stale.
        from genie_space_optimizer.optimization.snapshot_contract import (
            capture_pre_ag_snapshot,
        )

        _pre_ag_snapshot_capture = capture_pre_ag_snapshot(
            w=w,
            space_id=space_id,
            ag_id=ag_id,
        )
        if not _pre_ag_snapshot_capture.get("captured"):
            reason = _pre_ag_snapshot_capture.get("reason", "pre_ag_snapshot_failed")
            logger.error(
                "AG %s: could not capture pre-AG snapshot before apply "
                "(reason=%s). Skipping patch application.",
                ag_id,
                reason,
            )
            print(
                _section(f"[{ag_id}] SKIP APPLY: PRE-AG SNAPSHOT FAILED", "!") + "\n"
                + _kv("Reason", reason) + "\n"
                + _bar("!")
            )
            # Phase B observability follow-up — record the AG outcome so
            # the ACCEPTANCE_DECIDED producer (and the cross-checker
            # invariant "every STRATEGIST_AG_EMITTED has a matching
            # ACCEPTANCE_DECIDED") can see this terminal path. Before
            # this fix, the AG was silently discarded with no
            # ag_outcomes write, leaving the trace blind to
            # snapshot-capture failures.
            try:
                _current_iter_inputs["ag_outcomes"][str(ag_id)] = (
                    "skipped_pre_ag_snapshot_failed"
                )
            except Exception:
                logger.debug(
                    "Phase B: ag_outcome capture (pre_ag_snapshot_failed) "
                    "failed (non-fatal)",
                    exc_info=True,
                )
            _phase_b_emit_ag_outcome_record(ag, "skipped_pre_ag_snapshot_failed")
            # Cycle 9 T2: selective drain — keep buffered AGs whose
            # affected_questions are disjoint from the failed AG's.
            _survivors, _dropped_buffered = _drain_buffered_action_groups(
                failed_ag=ag,
                buffered=pending_action_groups,
                reason="pre_ag_snapshot_failed",
            )
            pending_action_groups = _survivors
            if not pending_action_groups:
                pending_strategy = None
            continue

        metadata_snapshot = _pre_ag_snapshot_capture["snapshot"]
        logger.info(
            "pre-AG snapshot captured for AG %s digest=%s",
            ag_id,
            _pre_ag_snapshot_capture.get("digest", ""),
        )

        apply_log = apply_patch_set(
            w, space_id, patches, metadata_snapshot, apply_mode=apply_mode,
        )

        # Cycle 5 T1 — productive-iteration accounting. Accumulate the
        # number of applied patches across ALL AGs in this iteration so
        # the end-of-iteration budget decision can tell a productive
        # iteration (≥1 applied) from a deterministic no-op (0 applied
        # AND a typed P4 reason in the iteration's decision records).
        try:
            _iter_applied_count += len(apply_log.get("applied") or [])
        except Exception:
            logger.debug(
                "Cycle 5 T1: _iter_applied_count update failed (non-fatal)",
                exc_info=True,
            )

        # Task 4 (lever-loop-v2) — per-AG patch-survival ledger. Print a
        # cluster-coverage table across the proposed → normalized →
        # applyable → capped → applied gates so operators can see
        # exactly where each cluster's patches were dropped.
        try:
            from genie_space_optimizer.optimization.patch_survival import (
                PatchSurvivalSnapshot,
                build_patch_survival_table,
            )

            _survival_applied = [
                entry.get("patch", {})
                for entry in (apply_log.get("applied") or [])
                if isinstance(entry, dict) and entry.get("patch")
            ]
            _survival_snapshot = PatchSurvivalSnapshot(
                ag_id=str(ag_id),
                proposed=list(locals().get("_survival_proposed", []) or []),
                normalized=list(locals().get("_survival_normalized", []) or []),
                applyable=list(locals().get("_survival_applyable", []) or []),
                capped=list(locals().get("_survival_capped", []) or []),
                applied=_survival_applied,
            )
            _survival_table = build_patch_survival_table(_survival_snapshot)
            if _survival_table:
                print(_survival_table)
        except Exception:
            logger.debug("Patch-survival ledger failed (non-fatal)", exc_info=True)

        # Task 4 — surface cap-selected vs applier-applied disagreement at
        # WARN level so silent applier drops (an AG2 cap selecting
        # ``['P002#3','P004#1','P005#1']`` but only applying the
        # off-causal filter on a ``mv_<domain>_dim_<entity>`` patch)
        # cannot recur unnoticed.
        try:
            from genie_space_optimizer.optimization.applier_audit import (
                diff_selected_vs_applied,
            )

            _cap_selected_ids = [
                str(p.get("expanded_patch_id") or p.get("id") or p.get("proposal_id") or "")
                for p in (patches or [])
                if (p.get("expanded_patch_id") or p.get("id") or p.get("proposal_id"))
            ]
            _applier_applied_ids = [
                str(
                    entry.get("patch", {}).get("expanded_patch_id")
                    or entry.get("patch", {}).get("id")
                    or entry.get("patch", {}).get("proposal_id")
                    or ""
                )
                for entry in (apply_log.get("applied") or [])
                if entry.get("patch")
            ]
            _recon = diff_selected_vs_applied(
                selected_ids=_cap_selected_ids,
                applied_ids=_applier_applied_ids,
            )
            print(
                _section("CAP-VS-APPLIED RECONCILIATION", "-") + "\n"
                + _kv("Cap selected", ", ".join(_cap_selected_ids)[:200] or "(none)") + "\n"
                + _kv(
                    "Applier applied",
                    ", ".join(_applier_applied_ids)[:200] or "(none)",
                ) + "\n"
                + _kv(
                    "Selected but not applied",
                    ", ".join(_recon.selected_but_not_applied)[:200] or "(none)",
                ) + "\n"
                + _kv(
                    "Applied but not selected",
                    ", ".join(_recon.applied_but_not_selected)[:200] or "(none)",
                ) + "\n"
                + _bar("-")
            )
            if not _recon.in_agreement:
                logger.warning(
                    "CAP-VS-APPLIED RECONCILIATION: selected_but_not_applied=%s "
                    "applied_but_not_selected=%s",
                    _recon.selected_but_not_applied,
                    _recon.applied_but_not_selected,
                )
        except Exception:
            logger.debug(
                "Cap-vs-applied reconciliation failed (non-fatal)",
                exc_info=True,
            )

        _apply_skip = _should_skip_eval_for_patch_bundle(
            patches=patches,
            apply_log=apply_log,
            stage="post_apply",
        )
        if _apply_skip.skip:
            if _apply_skip.reason_code == "no_applied_patches":
                _dead_on_arrival_ag_ids.add(str(ag_id))
                # Cycle 9 T4: never cache an empty signature ``()`` as
                # "already tried" — it represents "every patch was
                # dropped before the applier" (e.g. blast-radius gate),
                # which short-circuits subsequent strategist attempts.
                _record_dead_on_arrival_signature(
                    seen=_dead_on_arrival_patch_signatures,
                    signature=_selected_patch_signature,
                    reason=_apply_skip.reason_code,
                )
                # Cycle 2 Task 3: also record the selected-proposal-ID
                # signature (informative even when blast-radius drops
                # every patch) so a future iteration can detect that
                # the same AG is being retried with the same proposals
                # and avoid wasting iteration budget.
                _doa_selected_proposal_signature = (
                    _compute_selected_proposal_signature(
                        (ag.get("proposals") or [])
                        if isinstance(ag, dict) else []
                    )
                )
                _record_doa_selected_signature(
                    seen=_doa_selected_proposal_signatures,
                    ag_id=str(ag_id),
                    signature=_doa_selected_proposal_signature,
                )
                # Cycle 9 T7: emit one PATCH_SKIPPED DecisionRecord per
                # proposal_id in the dead-on-arrival signature. ACCEPTANCE_DECIDED
                # already covers the AG-level signal; this gives
                # finer-grained per-patch attribution (no-op patch vs
                # applier-rejected patch).
                try:
                    from genie_space_optimizer.optimization.decision_emitters import (
                        dead_on_arrival_decision_records,
                        is_strict_mode,
                    )
                    _doa_root_cause = ""
                    _doa_rca_id = ""
                    for _cid in (ag.get("source_cluster_ids") or []):
                        _doa_cluster = (
                            _iter_source_clusters_by_id.get(str(_cid)) or {}
                        )
                        if not _doa_root_cause:
                            _doa_root_cause = str(
                                _doa_cluster.get("root_cause") or ""
                            )
                        if not _doa_rca_id:
                            _doa_rca_id = str(
                                _iter_rca_id_by_cluster.get(str(_cid)) or ""
                            )
                        if _doa_root_cause and _doa_rca_id:
                            break
                    _doa_target_qids = [
                        str(q)
                        for q in (ag.get("affected_questions") or [])
                        if q
                    ]
                    _doa_records = dead_on_arrival_decision_records(
                        run_id=run_id,
                        iteration=iteration_counter,
                        ag_id=str(ag_id),
                        rca_id=_doa_rca_id,
                        root_cause=_doa_root_cause,
                        target_qids=_doa_target_qids,
                        signature=tuple(_selected_patch_signature or ()),
                        reason=str(_apply_skip.reason_code or ""),
                    )
                    _current_iter_inputs.setdefault(
                        "decision_records", []
                    ).extend([r.to_dict() for r in _doa_records])
                except Exception:
                    _phase_b_producer_exceptions["dead_on_arrival"] = (
                        _phase_b_producer_exceptions.get(
                            "dead_on_arrival", 0
                        )
                        + 1
                    )
                    logger.debug(
                        "dead-on-arrival DecisionRecord emission failed "
                        "(non-fatal)",
                        exc_info=True,
                    )
                    if is_strict_mode():
                        raise
                logger.warning(
                    "AG %s deterministic_no_applied_patches: selected patch "
                    "signature=%s recovery_reason=all_selected_patches_dropped_by_applier",
                    ag_id,
                    _selected_patch_signature,
                )
                print(
                    _section(f"[{ag_id}] DETERMINISTIC REJECTION: NO APPLIED PATCHES", "!") + "\n"
                    + _kv("Reason", "all_selected_patches_dropped_by_applier") + "\n"
                    + _kv("Selected patch signature", _selected_patch_signature) + "\n"
                    + _kv("Action", "discard buffered AG and force strategist recovery") + "\n"
                    + _bar("!")
                )
                # Cycle 9 T3: selective drain — keep buffered AGs whose
                # affected_questions are disjoint from the failed AG's.
                _survivors, _dropped_buffered = _drain_buffered_action_groups(
                    failed_ag=ag,
                    buffered=pending_action_groups,
                    reason="all_selected_patches_dropped_by_applier",
                )
                pending_action_groups = _survivors
                if not pending_action_groups:
                    pending_strategy = None
            logger.warning(
                "[%s] Skipping acceptance eval: %s",
                ag_id,
                _apply_skip.reason_detail,
            )
            print(
                _section(f"[{ag_id}] SKIP EVAL: NO APPLIED PATCHES", "!") + "\n"
                + _kv("Reason", _apply_skip.reason_detail) + "\n"
                + _bar("!")
            )
            try:
                from genie_space_optimizer.optimization.applier_audit import (
                    applier_decision_counts,
                )

                _applier_decisions = apply_log.get("applier_decisions") or []
                _decision_counts = applier_decision_counts(_applier_decisions)
                if _decision_counts:
                    print(
                        _section(f"[{ag_id}] APPLIER DECISIONS", "-") + "\n"
                        + "\n".join(
                            f"|  {key}: {value}"
                            for key, value in sorted(_decision_counts.items())
                        ) + "\n"
                        + _bar("-")
                    )
                # Cycle 8 Bug 1 Phase 3a — persist applier-decision counts to
                # MLflow so future cycle intakes have queryable diagnostic
                # data. Without this, the only way to learn why an AG hit
                # ``skipped_no_applied_patches`` is to dig the per-AG span
                # out of MLflow's full trace tree, or hand-grep the cycle's
                # stderr. Best-effort wrapped.
                try:
                    import mlflow as _mlflow_apl  # type: ignore[import-not-found]
                    if _decision_counts and _mlflow_apl.active_run() is not None:
                        _mlflow_apl.log_dict(
                            {
                                "iteration": iteration_counter,
                                "ag_id": str(ag_id),
                                "decision_counts": dict(_decision_counts),
                                "reason_code": _apply_skip.reason_code,
                                "reason_detail": _apply_skip.reason_detail,
                            },
                            artifact_file=(
                                f"phase_a/applier_decisions/"
                                f"iter_{iteration_counter}_{ag_id}.json"
                            ),
                        )
                        _mlflow_apl.set_tags({
                            (
                                f"applier_decisions.iter_{iteration_counter}."
                                f"{ag_id}.dropped_count"
                            ): str(sum(_decision_counts.values())),
                        })
                except Exception:
                    logger.debug(
                        "Cycle 8 Bug 1 Phase 3a: MLflow applier-decisions "
                        "persistence skipped (non-fatal)",
                        exc_info=True,
                    )
            except Exception:
                logger.debug("Failed to print applier decision counts", exc_info=True)
            write_stage(
                spark,
                run_id,
                f"AG_{ag_id}_NO_APPLIED_PATCHES",
                "SKIPPED",
                task_key="lever_loop",
                iteration=iteration_counter,
                detail={"reason_code": _apply_skip.reason_code},
                catalog=catalog,
                schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter,
                ag_id=ag_id,
                accepted=False,
                levers=[int(lk) for lk in lever_keys],
                target_objects=[],
                prev_scores=best_scores,
                new_scores=best_scores,
                rollback_reason=_apply_skip.reason_code,
                patches=patches,
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
                **_ag_identity_kwargs,
            ))
            # Phase A — Replay-fixture capture: like the dead-on-arrival
            # branch above, the AG short-circuits before the gate and
            # neither the rollback nor the accept paths fire. Stamp the
            # outcome so the fixture surfaces "AG was attempted but the
            # applier produced no applied entries".
            try:
                _current_iter_inputs["ag_outcomes"][str(ag_id)] = (
                    "skipped_no_applied_patches"
                )
            except Exception:
                logger.debug(
                    "Phase A: ag_outcome capture (no_applied_patches) failed (non-fatal)",
                    exc_info=True,
                )
            _phase_b_emit_ag_outcome_record(ag, "skipped_no_applied_patches")
            _render_current_journey()
            continue

        _fallback_lever = int(lever_keys[0]) if lever_keys else 0
        for idx, entry in enumerate(apply_log.get("applied", [])):
            _patch_lever = int(entry.get("patch", {}).get("lever", _fallback_lever))
            write_patch(
                spark, run_id, iteration_counter, _patch_lever, idx,
                _build_patch_record(entry, _patch_lever, apply_mode),
                catalog, schema,
            )
            # Task 13 — emit ``applied`` per applied patch.
            # Track 3/E (Phase A burn-down) — splits into
            # ``applied_targeted`` (qid was in the patch's
            # target_qids) and ``applied_broad_ag_scope`` (qid was
            # in the AG's affected_questions but not specifically
            # targeted by this patch). Phase B's
            # ``causal_patch_survival_pct`` consumes this distinction.
            try:
                _ap = entry.get("patch", {}) or {}
                _ap_pid = str(
                    _ap.get("proposal_id")
                    or _ap.get("expanded_patch_id")
                    or _ap.get("id")
                    or ""
                )
                # Plan N1 Task 4 — parent for lane-key collapse.
                # Patches stamped by ``_stamp_expanded_patch_identity``
                # carry an explicit ``parent_proposal_id``; if absent,
                # fall back to ``source_proposal_id`` or the unqualified
                # id parsed out of the expanded form (``L1:P001#1`` or
                # ``P001#1``).
                _ap_parent_pid = str(
                    _ap.get("parent_proposal_id")
                    or _ap.get("source_proposal_id")
                    or (
                        _ap_pid.split(":", 1)[-1].split("#", 1)[0]
                        if _ap_pid else ""
                    )
                )
                _ap_target_qids = list(_ap.get("_grounding_target_qids") or [])
                if not _ap_target_qids:
                    _ap_target_qids = list(_ap.get("target_qids") or [])
                _ap_target_qid_set = {str(q) for q in _ap_target_qids if q}

                _ap_ag_qids = {
                    str(q)
                    for q in (ag.get("affected_questions", []) or [])
                    if str(q)
                }
                _ap_broad_qid_set = _ap_ag_qids - _ap_target_qid_set

                _ap_patch_type = str(
                    _ap.get("patch_type") or _ap.get("type") or ""
                )

                if _ap_target_qid_set:
                    _journey_emit(
                        "applied_targeted",
                        question_ids=sorted(_ap_target_qid_set),
                        proposal_id=_ap_pid,
                        parent_proposal_id=_ap_parent_pid,
                        patch_type=_ap_patch_type,
                    )
                if _ap_broad_qid_set:
                    _journey_emit(
                        "applied_broad_ag_scope",
                        question_ids=sorted(_ap_broad_qid_set),
                        proposal_id=_ap_pid,
                        parent_proposal_id=_ap_parent_pid,
                        patch_type=_ap_patch_type,
                    )
            except Exception:
                logger.debug(
                    "Track 3/E: applied journey emit failed (non-fatal)",
                    exc_info=True,
                )

        # Phase F+H Commit A4: F7 application — post-stage observability
        # with atomic dedup. apply_patch_set at harness.py:16187 STAYS
        # inline; this stage call consumes the apply_log it produces and
        # emits PATCH_APPLIED records via ctx.decision_emit per
        # stages/application.py:159-171, replacing the inline
        # _patch_applied_records producer (formerly at this site).
        #
        # Dedup is atomic with the stage insertion: the inline producer
        # block (formerly Phase B delta Task 7 — emit PATCH_APPLIED
        # records per applied entry) is removed; the stage call emits
        # the same records via the same producer (decision_emitters.
        # patch_applied_records) wrapped in StageContext.decision_emit
        # which routes back to _current_iter_inputs["decision_records"].
        # Without atomic dedup, both fire and break byte-stability.
        #
        # Verified against: stages/application.py:47-62 (Input dataclass),
        # 137-176 (apply body — does NOT call apply_patch_set; emits
        # PATCH_APPLIED via ctx.decision_emit at :170-171),
        # 65-77 (AppliedPatchSet — fields applied + applied_signature).
        try:
            from genie_space_optimizer.optimization.stages import (
                StageContext as _StageCtx,
            )
            from genie_space_optimizer.optimization.stages import (
                application as _app_stage,
            )

            _cluster_root_cause_by_id = {
                str(_c.get("cluster_id") or ""): str(_c.get("root_cause") or "")
                for _c in (clusters or [])
                if _c.get("cluster_id")
            }
            _stage_ctx_application = _StageCtx(
                run_id=str(run_id),
                iteration=int(iteration_counter),
                space_id=str(space_id),
                domain=str(domain),
                catalog=str(catalog),
                schema=str(schema),
                apply_mode=str(apply_mode),
                journey_emit=_journey_emit,
                decision_emit=(
                    lambda record:
                        _current_iter_inputs.setdefault(
                            "decision_records", []
                        ).append(record.to_dict())
                ),
                mlflow_anchor_run_id=None,  # set by Phase C Commit 17
                feature_flags={},
            )
            _app_inp = _app_stage.ApplicationInput(
                applied_entries_by_ag={
                    str(ag_id): tuple(apply_log.get("applied", []) or [])
                },
                ags=tuple([ag] if isinstance(ag, dict) else []),
                rca_id_by_cluster=_iter_rca_id_by_cluster,
                cluster_root_cause_by_id=_cluster_root_cause_by_id,
            )
            # Phase F+H Commit B14: wrap F7 with stage_io_capture decorator.
            # Replay-byte-stable because wrap_with_io_capture returns out
            # unchanged and the MLflow log_text calls are no-ops while
            # _stage_ctx_application.mlflow_anchor_run_id is None (Phase C
            # Commit 17 wires the anchor).
            from genie_space_optimizer.optimization.stage_io_capture import (
                wrap_with_io_capture as _wrap_with_io_capture_a4,
            )
            _app_wrapped = _wrap_with_io_capture_a4(
                execute=_app_stage.execute,
                stage_key="applied_patches",
            )
            _applied_set = _app_wrapped(_stage_ctx_application, _app_inp)
            # _applied_set.applied is tuple[AppliedPatch, ...]; available
            # for downstream stages (F8 acceptance, F9 learning) when
            # those wire-ups land.
        except Exception:
            _iter_producer_exceptions["patch_applied"] += 1
            _phase_b_producer_exceptions["patch_applied"] = (
                _phase_b_producer_exceptions.get("patch_applied", 0) + 1
            )
            logger.debug(
                "Phase F+H A4: patch_applied stage call failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        _queued = apply_log.get("queued_high", [])
        if _queued:
            from genie_space_optimizer.optimization.state import write_queued_patch
            from genie_space_optimizer.optimization.labeling import flag_for_human_review
            for qentry in _queued:
                _qpatch = qentry.get("patch", {})
                write_queued_patch(
                    spark, run_id, iteration_counter,
                    _qpatch.get("type", ""),
                    _qpatch.get("target", ""),
                    catalog, schema,
                    confidence_tier="queued_high_risk",
                )
            _queued_flag_items = [
                {
                    "question_id": qentry.get("patch", {}).get("target", "unknown"),
                    "question_text": "",
                    "reason": (
                        f"High-risk patch queued for review: "
                        f"{qentry.get('patch', {}).get('type', '')} on "
                        f"{qentry.get('patch', {}).get('target', '')}"
                    ),
                    "iterations_failed": 0,
                    "patches_tried": qentry.get("patch", {}).get("type", ""),
                }
                for qentry in _queued
            ]
            flag_for_human_review(spark, run_id, catalog, schema, domain, _queued_flag_items)
            _qh_lines = [_section(f"[{ag_id}] Queued {len(_queued)} High-Risk Patch(es) for Human Review", "!")]
            for qi, qe in enumerate(_queued, 1):
                _qp = qe.get("patch", {})
                _qh_lines.append(
                    _kv(f"  [{qi}]", f"{_qp.get('type', '?')} \u2192 {_qp.get('target', '?')}")
                )
            _qh_lines.append(_bar("!"))
            print("\n".join(_qh_lines))

        if not apply_log.get("patch_deployed", False) and apply_log.get("applied"):
            _pe = apply_log.get("patch_error", "unknown")
            from genie_space_optimizer.optimization.rollback_class import (
                RollbackClass,
                classify_rollback_reason,
            )
            _pe_class = classify_rollback_reason(f"patch_deploy_failed: {_pe}")
            print(
                _section(f"[{ag_id}] PATCH DEPLOY FAILED", "!") + "\n"
                + _kv("Error", str(_pe)[:300]) + "\n"
                + _kv("Rollback class", _pe_class.value) + "\n"
                + _bar("!")
            )
            write_stage(
                spark, run_id, f"AG_{ag_id}_PATCH_FAILED", "ERROR",
                task_key="lever_loop", iteration=iteration_counter,
                error_message=str(_pe)[:500],
                detail={"rollback_class": _pe_class.value},
                catalog=catalog, schema=schema,
            )
            reflection_buffer.append(_build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[int(lk) for lk in lever_keys], target_objects=[],
                prev_scores=best_scores, new_scores=best_scores,
                rollback_reason=f"patch_deploy_failed: {str(_pe)[:100]}",
                patches=patches,
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=prev_failure_qids,
                **_ag_identity_kwargs,
            ))
            if _pe_class == RollbackClass.SCHEMA_FAILURE:
                print(
                    _section("LEVER LOOP — SCHEMA-FATAL PATCH ERROR", "!") + "\n"
                    + _kv("Error", str(_pe)[:300]) + "\n"
                    + _kv("Rollback class", RollbackClass.SCHEMA_FAILURE.value) + "\n"
                    + _kv("Reason", "Genie API rejected the PATCH payload structure; retrying would deterministically fail.") + "\n"
                    + _bar("!")
                )
                write_stage(
                    spark, run_id, "LEVER_LOOP_SCHEMA_FATAL", "ERROR",
                    task_key="lever_loop", iteration=iteration_counter,
                    error_message=str(_pe)[:500],
                    detail={"rollback_class": RollbackClass.SCHEMA_FAILURE.value},
                    catalog=catalog, schema=schema,
                )
                break
            # Phase C3: INFRA_FAILURE retry budget. Unlike CONTENT_REGRESSION
            # rollbacks, infra failures don't tell us anything about the
            # strategy's quality. We don't want them to count against
            # ``_diminishing_returns`` or the content rollback counter,
            # but an unbounded loop of infra flakes would spin forever —
            # hence the separate budget. Exit cleanly when the budget is
            # exhausted with a dedicated terminal reason.
            if _pe_class == RollbackClass.INFRA_FAILURE:
                _consecutive_infra = 0
                for _rb_entry in reversed(reflection_buffer):
                    if _rb_entry.get("rollback_class") == RollbackClass.INFRA_FAILURE.value:
                        _consecutive_infra += 1
                    else:
                        break
                if _consecutive_infra >= INFRA_RETRY_BUDGET:
                    print(
                        _section("LEVER LOOP — INFRA RETRY BUDGET EXHAUSTED", "!") + "\n"
                        + _kv("Consecutive infra rollbacks", _consecutive_infra) + "\n"
                        + _kv("Budget", INFRA_RETRY_BUDGET) + "\n"
                        + _kv("Last error", str(_pe)[:300]) + "\n"
                        + _bar("!")
                    )
                    write_stage(
                        spark, run_id, "LEVER_LOOP_INFRA_EXHAUSTED", "ERROR",
                        task_key="lever_loop", iteration=iteration_counter,
                        error_message=str(_pe)[:500],
                        detail={
                            "consecutive_infra": _consecutive_infra,
                            "budget": INFRA_RETRY_BUDGET,
                        },
                        catalog=catalog, schema=schema,
                    )
                    break
            _render_current_journey()
            continue

        # ── Applied Patches Detail ───────────────────────────────────
        _applied = apply_log.get("applied", [])
        if _applied:
            _ap_lines = [_section(f"[{ag_id}] Applied {len(_applied)} Patch(es)", "=")]
            for ai, aentry in enumerate(_applied, 1):
                _ap = aentry.get("patch", {})
                _aa = aentry.get("action", {})
                _ap_lines.append(_fmt_patch(ai, _ap, _aa, aentry))
            _ap_lines.append(_bar("="))
            print("\n".join(_ap_lines))

        _dropped = apply_log.get("dropped_patches", [])
        if _dropped:
            _dp_lines = [_section(f"[{ag_id}] Dropped {len(_dropped)} Join Spec Patch(es)", "!")]
            for di, dp in enumerate(_dropped, 1):
                _dp_lines.append(
                    f"|  [{di}] {dp.get('type', '?')}: "
                    f"{dp.get('left_table', '?')} <-> {dp.get('right_table', '?')}"
                )
            _dp_lines.append(
                "|  Reason: join spec PATCH failed; remaining patches deployed successfully"
            )
            _dp_lines.append(_bar("!"))
            print("\n".join(_dp_lines))

        # ── 3B.6: Three-gate eval ───────────────────────────────────
        # Snapshot the carried baseline at the start of *this* iteration
        # so the next iteration's drift diagnostic has something to
        # compare against.
        _baseline_at_start_of_this_iter = float(best_accuracy)
        gate_result = _run_gate_checks(
            spark=spark,
            w=w,
            run_id=run_id,
            space_id=space_id,
            exp_name=exp_name,
            domain=domain,
            iteration_counter=iteration_counter,
            ag_id=ag_id,
            benchmarks=benchmarks,
            proposals=all_proposals,
            patches=patches,
            apply_log=apply_log,
            clusters=clusters,
            metadata_snapshot=metadata_snapshot,
            predict_fn=predict_fn,
            scorers=scorers,
            prev_model_id=prev_model_id,
            best_scores=best_scores,
            best_accuracy=best_accuracy,
            catalog=catalog,
            schema=schema,
            reference_sqls=reference_sqls,
            noise_floor=noise_floor,
            affected_question_ids=set(ag.get("affected_questions", [])),
            lever_keys=lever_keys,
            max_benchmark_count=max_benchmark_count,
            prev_failure_qids=prev_failure_qids,
            prev_iter_pre_accept_baseline=_prev_iter_pre_accept_baseline,
            accepted_baseline_rows_for_control_plane=(
                _accepted_baseline_rows_for_control_plane
            ),
        )

        # Phase A — Lossless contract: refresh the deterministic eval-result
        # carrier IMMEDIATELY after the gate returns, BEFORE the accept/
        # rollback branch below. The previous wiring only refreshed on the
        # accept path (line ~15400 region), so a run where every iteration
        # rolled back would leave the carrier empty for the entire run,
        # producing empty `eval_rows` and `post_eval_passing_qids` in the
        # replay fixture and starving the journey-contract validator of qids.
        # The helper returns {} when neither full_result nor
        # failed_eval_result is populated; in that case we deliberately keep
        # the prior carrier value rather than clobbering with empty.
        _gate_eval = _extract_eval_result_from_gate(gate_result)
        if _gate_eval:
            _latest_eval_result = _gate_eval
            # Defensive backfill — populate the current iteration's snapshot
            # from THIS iteration's gate result so iter 1 has real eval_rows
            # even when the baseline seed at _run_lever_loop start silently
            # failed. The iter-start snapshot block (around line ~11138)
            # reads _latest_eval_result, which on iter 1 only has the
            # baseline seed; if that seed is empty the iter-1 snapshot is
            # empty too. This second write fixes that without depending on
            # the seed.
            try:
                _backfill_rows = _build_fixture_eval_rows(_gate_eval)
                if _backfill_rows and not _current_iter_inputs.get("eval_rows"):
                    _current_iter_inputs["eval_rows"] = _backfill_rows
            except Exception:
                logger.debug(
                    "Phase A: eval_rows backfill from gate_result failed "
                    "(non-fatal)",
                    exc_info=True,
                )

        # v2 Task 21 — Per-question regression rows with full attribution.
        # The gate returns the verdict and suppressed-qid set; the lever
        # loop owns persistence here because ``strategy`` (cluster
        # provenance), ``all_proposals`` (proposal IDs by qid), and
        # ``apply_log`` (which patches actually deployed) are all in
        # scope at this level.
        try:
            _t4_verdict_for_persist = gate_result.get("_t4_verdict")
            _t4_suppressed_for_persist = gate_result.get("_suppressed_qids") or set()
            if _t4_verdict_for_persist is not None:
                from genie_space_optimizer.optimization.per_question_regression import (
                    build_question_regression_rows,
                )
                _cluster_ids_by_qid: dict[str, list[str]] = {}
                for _c in (strategy.get("_source_clusters") or []) if strategy else []:
                    _cid = str(_c.get("cluster_id") or "").strip()
                    if not _cid:
                        continue
                    for _q in _c.get("question_ids") or []:
                        _cluster_ids_by_qid.setdefault(str(_q), []).append(_cid)
                _proposal_ids_by_qid: dict[str, list[str]] = {}
                for _p in (all_proposals or []):
                    _pid = str(_p.get("proposal_id") or _p.get("id") or "").strip()
                    if not _pid:
                        continue
                    for _q in _p.get("target_qids") or []:
                        _proposal_ids_by_qid.setdefault(str(_q), []).append(_pid)
                _applied_patch_entries = apply_log.get("applied", []) or []
                _applied_patch_ids: list[str] = []
                for _entry in _applied_patch_entries:
                    _ap = _entry.get("patch", {}) or {}
                    _ap_pid = str(
                        _ap.get("proposal_id")
                        or _ap.get("expanded_patch_id")
                        or _ap.get("id")
                        or ""
                    )
                    if _ap_pid:
                        _applied_patch_ids.append(_ap_pid)
                _t4_rows = build_question_regression_rows(
                    run_id=run_id,
                    iteration=iteration_counter,
                    ag_id=ag_id,
                    verdict=_t4_verdict_for_persist,
                    suppressed_qids=_t4_suppressed_for_persist,
                    cluster_ids_by_qid=_cluster_ids_by_qid,
                    proposal_ids_by_qid=_proposal_ids_by_qid,
                    applied_patch_ids=_applied_patch_ids,
                )
                if _t4_rows:
                    from genie_space_optimizer.optimization.state import (
                        write_question_regressions,
                    )
                    write_question_regressions(
                        spark, _t4_rows, catalog=catalog, schema=schema,
                    )
        except Exception:
            logger.debug(
                "Failed to persist per-question regression rows", exc_info=True,
            )

        # After the gate finishes, this iteration's pre-acceptance
        # baseline becomes the reference for the next iteration's
        # drift diagnostic.
        _prev_iter_pre_accept_baseline = _baseline_at_start_of_this_iter

        # Phase F+H A5 (v2.1): F8 acceptance — post-stage observability
        # with SELECTIVE atomic dedup at the 2 post-gate callsites + the
        # post-eval block. The closure _phase_b_emit_ag_outcome_record
        # STAYS inline because its 3 pre-gate callsites
        # ("skipped_dead_on_arrival", "skipped_pre_ag_snapshot_failed",
        # "skipped_no_applied_patches") are pre-gate filtering paths that
        # bypass decide_control_plane_acceptance entirely — F8.decide()
        # cannot reproduce them.
        #
        # decide_control_plane_acceptance is called once per AG INSIDE
        # _run_gate_checks (verified PURE — zero mlflow./spark./global
        # hits in control_plane.py). F8.decide() re-calls the same gate
        # per AG with the same inputs, derives the same reason_code, and
        # emits ACCEPTANCE_DECIDED + QID_RESOLUTION via stages/
        # acceptance.py:decide.
        #
        # Why this anchor (best_accuracy drift trap):
        # - At this point best_accuracy is still the pre-acceptance
        #   baseline that _run_gate_checks consumed; best_accuracy is
        #   only mutated later inside the accept branch.
        # - AGs that hit the 3 pre-gate filters have already `continue`'d.
        #   The in-scope `ag` is the SURVIVOR.
        # - inp.ags=(ag,) captures the single-AG slate exactly (one AG
        #   per outer iteration per the strategist invariant).
        #
        # Replaces:
        # - 2 post-gate callsites further below: rolled_back (inside
        #   the gate-failed branch) and accepted (after the accept
        #   branch's _outcome_for_journey computation).
        # - _post_eval_resolution_records block at iteration-end.
        # Does NOT replace:
        # - closure _phase_b_emit_ag_outcome_record (definition + 3
        #   pre-gate callsites STAY inline).
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                is_strict_mode as _phase_b_strict_mode,
            )
            from genie_space_optimizer.optimization.stages import (
                StageContext as _StageCtx,
            )
            from genie_space_optimizer.optimization.stages import (
                acceptance as _accept_stage,
            )

            _stage_ctx_a5 = _StageCtx(
                run_id=str(run_id),
                iteration=int(iteration_counter),
                space_id=str(space_id),
                domain=str(domain),
                catalog=str(catalog),
                schema=str(schema),
                apply_mode=str(apply_mode),
                journey_emit=_journey_emit,
                decision_emit=_decision_emit,
                mlflow_anchor_run_id=_phase_h_anchor_run_id,  # C17 v2 — activates Phase B capture
                feature_flags={},
            )

            # Group apply_log entries by AG. Always source from apply_log
            # (no _applied_set check) — apply_log is the iteration-local
            # bound earlier in this iteration; _applied_set may not be
            # bound if A4 errored upstream.
            _accept_applied_by_ag: dict[str, list[dict]] = {}
            for _entry in (apply_log.get("applied") or []):
                _patch = _entry.get("patch") or {}
                _entry_ag = str(_patch.get("ag_id") or "")
                if _entry_ag:
                    _accept_applied_by_ag.setdefault(_entry_ag, []).append(
                        _entry
                    )
            _accept_applied_by_ag_t: dict[str, tuple] = {
                k: tuple(v) for k, v in _accept_applied_by_ag.items()
            }

            # Source candidate_accuracy + candidate_pre_arbiter_accuracy
            # from gate_result (the values _run_gate_checks consumed
            # internally). Recomputing from full_result_1 is forbidden —
            # it would diverge.
            _accept_candidate_accuracy = float(
                gate_result.get("full_accuracy") or 0.0
            )
            _accept_candidate_pre_arbiter = (
                float(gate_result.get("full_pre_arbiter_accuracy"))
                if gate_result.get("full_pre_arbiter_accuracy") is not None
                else float(full_pre_arbiter_accuracy or 0.0)
            )
            # post_rows: prefer gate_result.full_result.rows (what the
            # gate actually consumed); fall back to full_result_1 only
            # when the gate did not surface it.
            _accept_gate_full_result = (
                gate_result.get("full_result") or {}
            )
            _accept_post_rows = (
                _accept_gate_full_result.get("rows")
                or (full_result_1 or {}).get("rows")
                or []
            )

            _accept_inp = _accept_stage.AcceptanceInput(
                applied_entries_by_ag=_accept_applied_by_ag_t,
                ags=(ag,),  # single-AG slate
                baseline_accuracy=float(best_accuracy),
                candidate_accuracy=_accept_candidate_accuracy,
                baseline_pre_arbiter_accuracy=float(_best_pre_arbiter),
                candidate_pre_arbiter_accuracy=_accept_candidate_pre_arbiter,
                pre_rows=tuple(_baseline_rows_for_control_plane or []),
                post_rows=tuple(_accept_post_rows),
                protected_qids=(),
                min_gain_pp=float(MIN_POST_ARBITER_GAIN_PP),
                min_pre_arbiter_gain_pp=2.0,
                rca_id_by_cluster=dict(_iter_rca_id_by_cluster),
                cluster_by_qid={},
            )
            # Phase F+H Commit B15: wrap F8 with stage_io_capture
            # decorator. Replay-byte-stable — wrap_with_io_capture
            # returns the stage output unchanged; MLflow log_text
            # calls are no-ops while mlflow_anchor_run_id is None
            # (C17 wires the anchor on real runs).
            from genie_space_optimizer.optimization.stage_io_capture import (
                wrap_with_io_capture as _wrap_with_io_capture_a5,
            )
            _accept_wrapped = _wrap_with_io_capture_a5(
                execute=_accept_stage.execute,
                stage_key="acceptance_decision",
            )
            _ag_outcome = _accept_wrapped(_stage_ctx_a5, _accept_inp)
        except Exception:
            _iter_producer_exceptions["ag_outcome"] = (
                _iter_producer_exceptions.get("ag_outcome", 0) + 1
            )
            _phase_b_producer_exceptions["ag_outcome"] = (
                _phase_b_producer_exceptions.get("ag_outcome", 0) + 1
            )
            logger.debug(
                "Phase F+H A5 v2.1: acceptance stage failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # ── 3B.7: Accept or rollback ────────────────────────────────
        _target_objects = [
            p.get("target_object", "") for p in patches if p.get("target_object")
        ]

        if not gate_result.get("passed"):
            reason = gate_result.get("rollback_reason", "unknown")
            # Phase A — Lossless contract: stamp rolled_back for every
            # qid the AG targeted. Required transition from APPLIED.
            try:
                _emit_ag_outcome_journey(
                    emit=_journey_emit,
                    ag_id=str(ag_id),
                    outcome="rolled_back",
                    affected_qids=list(ag.get("affected_questions") or []),
                )
                _current_iter_inputs["ag_outcomes"][str(ag_id)] = "rolled_back"
            except Exception:
                logger.debug(
                    "Phase A: AG-outcome (rolled_back) emit/capture failed (non-fatal)",
                    exc_info=True,
                )
            # A5 v2.1: F8 emits ACCEPTANCE_DECIDED above.
            _render_current_journey()
            rollback(apply_log, w, space_id, metadata_snapshot)
            # Task 7 — verify the Genie Space actually returned to its
            # pre-AG state. If not, halt subsequent AGs because clustering
            # against a still-modified space pollutes downstream RCA.
            try:
                from genie_space_optimizer.optimization.applier import (
                    verify_rollback_restored,
                )

                _restore_decision = verify_rollback_restored(
                    w=w,
                    space_id=space_id,
                    expected_snapshot=metadata_snapshot,
                )
                if not _restore_decision.get("verified", True):
                    logger.error(
                        "AG %s: verify_rollback_restored returned not verified "
                        "(reason=%s, first_diff=%s). Genie Space state may not "
                        "match pre-iteration baseline. Failing run terminally.",
                        ag_id,
                        _restore_decision.get("reason", "unknown"),
                        _restore_decision.get("first_diff_path", "(none)"),
                    )
                    print(
                        _section("ROLLBACK VERIFICATION FAILED", "-") + "\n"
                        + _kv("AG", ag_id) + "\n"
                        + _kv("Reason", _restore_decision.get("reason", "unknown")) + "\n"
                        + _kv("Expected digest", _restore_decision.get("expected_digest", "(none)")) + "\n"
                        + _kv("Live digest", _restore_decision.get("live_digest", "(none)")) + "\n"
                        + _kv("First diff", _restore_decision.get("first_diff_path", "(none)")) + "\n"
                        + "|  Genie Space state did not match pre-iteration snapshot.\n"
                        + "|  Failing the run terminally; subsequent AGs cannot trust live state.\n"
                        + _bar("-")
                    )
                    _rollback_state_trusted_for_quarantine = False
                    update_run_status(
                        spark,
                        run_id,
                        catalog,
                        schema,
                        status="FAILED",
                        convergence_reason="failed_rollback_verification",
                    )
                    raise FailedRollbackVerification(
                        json.dumps(_restore_decision, default=str)[:1000]
                    )
            except FailedRollbackVerification:
                raise
            except Exception:
                logger.warning(
                    "verify_rollback_restored raised — treating as non-fatal "
                    "but flagging for operator review",
                    exc_info=True,
                )
            mark_patches_rolled_back(
                spark, run_id, iteration_counter, reason, catalog, schema,
            )
            ags_rolled_back.append(ag_id)
            # Phase 1.3: drop the AG buffer when an AG rolls back.  The
            # buffered AGs were produced from the same strategist call
            # and tend to share the same flawed root-cause hypothesis;
            # forcing a fresh strategist call gives the next iteration
            # a clean slate informed by the rollback in reflection_buffer.
            if pending_action_groups:
                logger.info(
                    "Dropping %d buffered AG(s) after rollback of %s",
                    len(pending_action_groups), ag_id,
                )
                pending_action_groups = []
                pending_strategy = None
            for lk in lever_keys:
                levers_rolled_back.append(int(lk))
            # Phase E2: include rollback_class in stage detail so the
            # run summary can break rollbacks down by class.
            from genie_space_optimizer.optimization.rollback_class import (
                classify_rollback_reason as _classify_rb,
            )
            _rb_class = _classify_rb(reason).value
            write_stage(
                spark, run_id, f"AG_{ag_id}_STARTED", "ROLLED_BACK",
                task_key="lever_loop", iteration=iteration_counter,
                detail={
                    "reason": reason,
                    "levers": lever_keys,
                    "rollback_class": _rb_class,
                },
                catalog=catalog, schema=schema,
            )
            _failed_eval = gate_result.get("failed_eval_result", {})
            _fail_tmap = _failed_eval.get("trace_map", {})
            _fail_qids = set(_failed_eval.get("failure_question_ids", []))
            all_failure_question_ids.extend(_fail_qids)
            for qid, tid in _fail_tmap.items():
                question_trace_map.setdefault(qid, []).append(tid)
                if qid in _fail_qids:
                    all_failure_trace_ids.append(tid)
                elif "regressions" in gate_result:
                    all_regression_trace_ids.append(tid)
            _fail_run_id = _failed_eval.get("mlflow_run_id") or _failed_eval.get("run_id", "")
            if _fail_run_id:
                all_eval_mlflow_run_ids.append(_fail_run_id)

            _failed_scores = gate_result.get("full_scores", best_scores)
            _rb_fail_qids = set(
                gate_result.get("failed_eval_result", {}).get("failure_question_ids", [])
            )
            _affected_set = set(ag.get("affected_questions", []))
            _any_target_improved = bool(
                _affected_set and prev_failure_qids
                and (_affected_set & prev_failure_qids) - (_rb_fail_qids or prev_failure_qids)
            )
            _rb_refinement = "in_plan" if _any_target_improved else "out_of_plan"
            _rb_acc_delta = (
                sum(_failed_scores.values()) / max(len(_failed_scores), 1)
                - sum(best_scores.values()) / max(len(best_scores), 1)
            )
            _regressions = gate_result.get("regressions", [])
            _rb_patch_types = sorted({p.get("patch_type", "?") for p in patches})
            _control_plane_reason_for_reflection = ""
            for _r in _regressions:
                if _r.get("judge") == "control_plane_acceptance":
                    _control_plane_reason_for_reflection = str(
                        _r.get("reason") or ""
                    )
                    break
            _rb_reflection = _format_rollback_reflection(
                rollback_reason=reason,
                control_plane_reason=_control_plane_reason_for_reflection,
                any_target_improved=_any_target_improved,
                regressions=_regressions,
                patch_types=_rb_patch_types,
                root_cause_summary=str(ag.get("root_cause_summary", "")),
                accuracy_delta_pp=float(_rb_acc_delta),
            )

            # Task 19 — record SQL-shape deltas on the rolled-back AG
            # so the strategist can see what the candidate accomplished
            # vs. ground truth, and where shape work still remains.
            from genie_space_optimizer.optimization.sql_shape_delta import (
                compute_sql_shape_delta,
            )

            def _row_qid(row: dict) -> str:
                return str(
                    row.get("inputs.question_id")
                    or row.get("inputs/question_id")
                    or row.get("question_id")
                    or (row.get("inputs") or {}).get("question_id", "")
                )

            def _row_sql(row: dict) -> str:
                return str(
                    row.get("outputs.predictions.sql")
                    or row.get("outputs/predictions/sql")
                    or row.get("generated_sql")
                    or row.get("genie_sql")
                    or (row.get("outputs") or {}).get("genie_sql", "")
                    or ""
                )

            def _row_count(row: dict) -> int | None:
                for k in ("genie_row_count", "outputs.genie_row_count", "outputs/genie_row_count"):
                    v = row.get(k)
                    if isinstance(v, (int, float)):
                        return int(v)
                v = (row.get("outputs") or {}).get("genie_row_count")
                return int(v) if isinstance(v, (int, float)) else None

            _accepted_by_qid = {
                _row_qid(r): r
                for r in (_accepted_baseline_rows_for_control_plane or [])
                if _row_qid(r)
            }
            _candidate_by_qid_for_delta = {
                _row_qid(r): r
                for r in (_failed_eval.get("rows") or [])
                if _row_qid(r)
            }
            try:
                _gt_by_qid = dict(reference_sqls or {})
            except Exception:
                _gt_by_qid = {}
            _sql_deltas: list[dict] = []
            for _qid, _cand_row in _candidate_by_qid_for_delta.items():
                _gt_sql = str(_gt_by_qid.get(_qid, ""))
                if not _gt_sql:
                    continue
                _acc_row = _accepted_by_qid.get(_qid, {})
                try:
                    _delta = compute_sql_shape_delta(
                        target_qid=_qid,
                        accepted_sql=_row_sql(_acc_row),
                        candidate_sql=_row_sql(_cand_row),
                        ground_truth_sql=_gt_sql,
                        accepted_row_count=_row_count(_acc_row),
                        candidate_row_count=_row_count(_cand_row),
                    )
                except Exception:
                    continue
                if _delta.get("improved") or _delta.get("remaining"):
                    _sql_deltas.append(_delta)

            reflection = _build_reflection_entry(
                iteration=iteration_counter, ag_id=ag_id, accepted=False,
                levers=[int(lk) for lk in lever_keys],
                target_objects=_target_objects,
                prev_scores=best_scores, new_scores=_failed_scores,
                rollback_reason=reason, patches=patches,
                affected_question_ids=ag.get("affected_questions", []),
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=_rb_fail_qids or prev_failure_qids,
                reflection_text=_rb_reflection,
                refinement_mode=_rb_refinement,
                **_ag_identity_kwargs,
            )
            reflection["sql_shape_deltas"] = _sql_deltas
            _attach_rca_theme_attribution(
                spark=spark,
                run_id=run_id,
                iteration_counter=iteration_counter,
                ag_id=ag_id,
                metadata_snapshot=metadata_snapshot,
                reflection=reflection,
                prev_failure_qids=prev_failure_qids,
                new_failure_qids=_rb_fail_qids or set(),
                catalog=catalog,
                schema=schema,
            )
            reflection_buffer.append(reflection)

            # Regression-mining lane (audit-only, soft-fail). Mines
            # ``column_confusion`` insights from failed candidate eval
            # rows for newly-regressed questions. Acceptance, rollback,
            # and state loaders are unchanged; the insights live in
            # the decision-audit table and on the reflection JSON for
            # later inspection. A feature flag controls whether the
            # next strategist call sees them as compact hints.
            _mined_insights: list = []
            try:
                from genie_space_optimizer.optimization.regression_mining import (
                    mine_regression_insights,
                    summarize_insights_for_reflection,
                )
                _regressed_qids: set[str] = set()
                for _r in gate_result.get("regressions") or []:
                    for _q in _r.get("blocking_qids") or []:
                        if _q:
                            _regressed_qids.add(str(_q))
                # Fallback: if no per-question regression fired (e.g.
                # the gate failed for raw acceptance reasons), mine the
                # set of qids that flipped from passing to failing.
                if not _regressed_qids and prev_failure_qids is not None:
                    _flipped = {
                        str(q) for q in (_rb_fail_qids or set())
                        if q not in prev_failure_qids
                    }
                    _regressed_qids = _flipped
                _failed_rows = (
                    gate_result.get("failed_eval_result", {}).get("rows") or []
                )
                _mined_insights = mine_regression_insights(
                    failed_eval_rows=_failed_rows,
                    regressed_qids=_regressed_qids,
                    metadata_snapshot=metadata_snapshot,
                )
                if _mined_insights:
                    reflection["regression_mining"] = (
                        summarize_insights_for_reflection(_mined_insights)
                    )
                    logger.info(
                        "Regression mining produced %d insight(s) for iter %d "
                        "(qids: %s)",
                        len(_mined_insights),
                        iteration_counter,
                        ", ".join(sorted({i.question_id for i in _mined_insights})[:5]),
                    )
            except Exception:
                logger.debug(
                    "Regression mining failed for rollback iter %d",
                    iteration_counter,
                    exc_info=True,
                )

            try:
                update_iteration_reflection(
                    spark, run_id, iteration_counter, reflection,
                    catalog=catalog, schema=schema, eval_scope="full",
                )
            except Exception:
                logger.debug("Failed to persist reflection for rollback iter %d", iteration_counter, exc_info=True)

            # Persist the mined insights as typed decision-audit rows.
            # ``gate_name="regression_mining"`` lets a single SQL query
            # answer "what did we learn from rollbacks on this run?"
            # without parsing reflection JSON. Soft-fail by design —
            # the audit table is non-authoritative.
            if _mined_insights:
                try:
                    from genie_space_optimizer.optimization.regression_mining import (
                        build_decision_audit_rows,
                    )
                    from genie_space_optimizer.optimization.state import (
                        write_lever_loop_decisions as _write_mining_decisions,
                    )
                    _mining_rows = build_decision_audit_rows(
                        _mined_insights,
                        run_id=run_id,
                        iteration=iteration_counter,
                        ag_id=ag_id,
                    )
                    _write_mining_decisions(
                        spark, _mining_rows, catalog=catalog, schema=schema,
                    )
                except Exception:
                    logger.debug(
                        "Failed to persist regression-mining audit rows for iter %d",
                        iteration_counter,
                        exc_info=True,
                    )
            for p in patches:
                # B1.2 — converted patches use ``type`` / ``target``;
                # the legacy fields ``patch_type`` / ``target_object``
                # are absent on the conversion path so the previous
                # extractor returned ``("", "")`` and the guard
                # silently rejected every entry. Read both for
                # backward compatibility with any pre-conversion
                # fixtures.
                ft = str(p.get("type") or p.get("patch_type") or "").strip()
                tgt = str(p.get("target") or p.get("target_object") or "").strip()
                if ft and tgt:
                    tried_patches.add((ft, tgt))
            _lever_frozenset = frozenset(int(lk) for lk in lever_keys)
            # Phase C3 + D3: only CONTENT_REGRESSION rollbacks contribute
            # to the tried-cluster bookkeeping. Phase D3 also lowers the
            # threshold so the (root_cause, blame, lever_set) 3-tuple is
            # marked after the FIRST content rollback, while the legacy
            # (root_cause, blame) 2-tuple (which suppresses across ALL
            # levers) is only written when we've exhausted ``>= 2``
            # distinct lever sets on the same cluster.
            _content_rb_count = sum(
                1 for _rb_entry in reflection_buffer
                if not _rb_entry.get("accepted")
                and not _rb_entry.get("escalation_handled")
                and _rb_entry.get("rollback_class") == _RC.CONTENT_REGRESSION.value
            )
            _should_mark_tried_lever_aware = _content_rb_count >= 1
            source_cids = set(ag.get("source_cluster_ids", []))
            for c in clusters:
                cid = c.get("cluster_id", "")
                if source_cids and cid not in source_cids:
                    continue
                rc_ft = c.get("asi_failure_type") or c.get("root_cause", "other")
                rc_blame = _normalise_blame(c.get("asi_blame_set"))
                if not rc_ft or not _should_mark_tried_lever_aware:
                    continue
                # Always add the 3-tuple (lever-aware) immediately.
                tried_root_causes.add((rc_ft, rc_blame, _lever_frozenset))
                # Legacy 2-tuple: only when the same cluster has failed
                # across >= 2 distinct lever sets (truly-dead cluster).
                _distinct_lever_sets = {
                    frozenset(e.get("lever_set") or [])
                    for e in reflection_buffer
                    if (
                        not e.get("accepted")
                        and not e.get("escalation_handled")
                        and e.get("rollback_class") == _RC.CONTENT_REGRESSION.value
                        and e.get("root_cause") == rc_ft
                        and (e.get("blame_set") or "") == rc_blame
                    )
                }
                # Include the current lever set we're about to add.
                _distinct_lever_sets.add(_lever_frozenset)
                if len(_distinct_lever_sets) >= 2:
                    tried_root_causes.add((rc_ft, rc_blame))
            if not _should_mark_tried_lever_aware:
                logger.info(
                    "No CONTENT_REGRESSION rollbacks yet — keeping cluster "
                    "available for retry (root causes NOT marked as tried)",
                )
            continue

        # ── Accept action group ──────────────────────────────────────
        ags_accepted.append(ag_id)
        for lk in lever_keys:
            levers_accepted.append(int(lk))

        # Phase A — Lossless contract: stamp accepted (or accepted_with_
        # regression_debt) for every qid the AG targeted. Required
        # transition from APPLIED.
        try:
            _outcome_for_journey = (
                "accepted_with_regression_debt"
                if (
                    gate_result.get("acceptance_decision", {}) or {}
                ).get("reason_code") == "accepted_with_regression_debt"
                else "accepted"
            )
            _emit_ag_outcome_journey(
                emit=_journey_emit,
                ag_id=str(ag_id),
                outcome=_outcome_for_journey,
                affected_qids=list(ag.get("affected_questions") or []),
            )
        except Exception:
            logger.debug(
                "Phase A: AG-outcome journey emit (accepted) failed (non-fatal)",
                exc_info=True,
            )
        try:
            _current_iter_inputs["ag_outcomes"][str(ag_id)] = _outcome_for_journey
        except Exception:
            logger.debug(
                "Phase A: ag_outcome capture (accepted) failed (non-fatal)",
                exc_info=True,
            )
        # Phase F+H A5 v2.1: F8.decide() (above, pre-3B.7) emits the
        # ACCEPTANCE_DECIDED record for accepted /
        # accepted_with_regression_debt. The closure callsite formerly
        # here is deleted to prevent double-emission.

        # Phase C Task 3 — ObservedEffect per applied patch in this AG.
        # Best-effort with defensive defaults: empty pre/post passing
        # qid sets and zero deltas if the surrounding scope hasn't yet
        # named the relevant signals. The structured surface is what
        # matters; downstream consumers (next-action mapper) tolerate
        # unset fields, and Phase D / Cycle-8 Bug 1 Phase 3b will plumb
        # richer signals into this site.
        try:
            from genie_space_optimizer.optimization.rca_execution import (
                build_observed_effects,
            )

            _observed = build_observed_effects(
                iteration=iteration_counter,
                ag_id=str(ag_id),
                apply_log=apply_log,
                pre_passing_qids=(),
                post_passing_qids=tuple(
                    _current_iter_inputs.get("post_eval_passing_qids") or ()
                ),
                pre_iq=0.0,
                post_iq=0.0,
                arbiter_verdict_change="",
                pre_judge_failures=0,
                post_judge_failures=0,
            )
            _current_iter_inputs.setdefault("observed_effects", []).extend(
                [
                    {
                        "iteration": e.iteration,
                        "ag_id": e.ag_id,
                        "proposal_id": e.proposal_id,
                        "pre_passing_qids": list(e.pre_passing_qids),
                        "post_passing_qids": list(e.post_passing_qids),
                        "iq_delta": e.iq_delta,
                        "arbiter_verdict_change": e.arbiter_verdict_change,
                        "judge_failure_delta": e.judge_failure_delta,
                    }
                    for e in _observed
                ]
            )
        except Exception:
            _phase_b_producer_exceptions["observed_effect"] = (
                _phase_b_producer_exceptions.get("observed_effect", 0) + 1
            )
            logger.debug(
                "Phase C: build_observed_effects failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        full_scores = gate_result["full_scores"]
        full_accuracy = gate_result["full_accuracy"]
        new_model_id = gate_result["new_model_id"]
        full_result = gate_result["full_result"]
        # Phase A — carrier is now refreshed at the gate-checks site above
        # (right after `_run_gate_checks` returns), so this re-assignment is
        # redundant on the accept path but harmless. Kept as a no-op anchor
        # so downstream reads of the local `full_result` variable stay
        # consistent with the carrier.
        _latest_eval_result = full_result or _latest_eval_result
        _last_full_mlflow_run_id = full_result.get("mlflow_run_id") or full_result.get("run_id", "")

        _full_trace_map = full_result.get("trace_map", {})
        _full_failures = set(full_result.get("failure_question_ids", []))
        all_failure_question_ids.extend(_full_failures)
        for qid, tid in _full_trace_map.items():
            question_trace_map.setdefault(qid, []).append(tid)
            if qid in _full_failures:
                all_failure_trace_ids.append(tid)
        _full_run_id = full_result.get("mlflow_run_id") or full_result.get("run_id", "")
        if _full_run_id:
            all_eval_mlflow_run_ids.append(_full_run_id)

        try:
            from genie_space_optimizer.optimization.evaluation import log_expectations_on_traces
            log_expectations_on_traces(full_result)
        except Exception:
            logger.debug("Failed to log expectations on iter %d traces", iteration_counter, exc_info=True)

        try:
            log_judge_verdicts_on_traces(full_result)
        except Exception:
            logger.debug("Failed to log judge verdicts on iter %d traces", iteration_counter, exc_info=True)

        try:
            _persist_text, _persist_data = _build_question_persistence_summary(
                _verdict_history, reflection_buffer,
            )
            if _persist_data:
                log_persistence_context_on_traces(full_result, _persist_data)
        except Exception:
            logger.debug("Failed to log persistence context on iter %d traces", iteration_counter, exc_info=True)

        lever_changes.append({
            "lever": ag_id,
            "lever_name": f"AG {ag_id}: {ag.get('root_cause_summary', '')[:60]}",
            "patches": [
                {"change": p.get("change_description", ""), "patch_type": p.get("patch_type", "")}
                for p in all_proposals
            ],
            "accuracy_delta": full_accuracy - best_accuracy,
        })

        _accepted_fail_qids = set(full_result.get("failure_question_ids", []))
        _acc_delta = full_accuracy - best_accuracy
        _acc_patch_types = sorted({p.get("patch_type", "?") for p in patches})
        _acc_reflection = (
            f"Accepted: {ag.get('root_cause_summary', 'improvement')} resolved via "
            f"{', '.join(_acc_patch_types)}. "
            f"Accuracy improved by {_acc_delta:+.1f}% "
            f"affecting {len(ag.get('affected_questions', []))} question(s)."
        )
        reflection = _build_reflection_entry(
            iteration=iteration_counter, ag_id=ag_id, accepted=True,
            levers=[int(lk) for lk in lever_keys],
            target_objects=_target_objects,
            prev_scores=best_scores, new_scores=full_scores,
            rollback_reason=None, patches=patches,
            affected_question_ids=ag.get("affected_questions", []),
            prev_failure_qids=prev_failure_qids,
            new_failure_qids=_accepted_fail_qids,
            reflection_text=_acc_reflection,
            acceptance_delta_pp=float(
                gate_result.get("acceptance_delta_pp", _acc_delta)
            ),
            **_ag_identity_kwargs,
        )
        _attach_rca_theme_attribution(
            spark=spark,
            run_id=run_id,
            iteration_counter=iteration_counter,
            ag_id=ag_id,
            metadata_snapshot=metadata_snapshot,
            reflection=reflection,
            prev_failure_qids=prev_failure_qids,
            new_failure_qids=_accepted_fail_qids,
            catalog=catalog,
            schema=schema,
        )
        reflection_buffer.append(reflection)
        try:
            update_iteration_reflection(
                spark, run_id, iteration_counter, reflection,
                catalog=catalog, schema=schema, eval_scope="full",
            )
        except Exception:
            logger.debug("Failed to persist reflection for accepted iter %d", iteration_counter, exc_info=True)
        prev_failure_qids = _accepted_fail_qids

        if _acc_delta >= 1.0:
            skill_exemplars.append({
                "root_cause": ag.get("root_cause_summary", ""),
                "lever_pattern": sorted(ag.get("lever_directives", {}).keys()),
                "patch_types": [p.get("patch_type") for p in patches[:5] if p.get("patch_type") is not None],
                "accuracy_gain": round(_acc_delta, 1),
            })

        best_scores = full_scores
        best_accuracy = full_accuracy
        best_model_id = new_model_id
        best_iteration = iteration_counter
        prev_scores = full_scores
        prev_model_id = new_model_id
        # Task 5 — only update accepted-baseline rows on accept; rollback
        # paths must NOT touch this list. Pull from the gate's
        # ``full_result.rows`` so the snapshot is the same row set the
        # gate just used to decide acceptance, not a stale ``_after_rows``
        # local that may have been narrowed for diagnostics.
        _accepted_full_rows = (
            gate_result.get("full_result", {}).get("rows")
            or []
        )
        _accepted_baseline_rows_for_control_plane = [
            dict(row) for row in _accepted_full_rows
        ]
        # Task 6 — restore quarantine trust now that an AG accepted and the
        # live Genie config matches the gate's accepted-baseline rows.
        _rollback_state_trusted_for_quarantine = True
        # Task 8 — carry accepted regression debt into the next strategist
        # input so the loop targets it before any new soft cluster.
        _acceptance_detail = gate_result.get("acceptance_decision") or {}
        _regression_debt_qids_for_next_iteration = tuple(
            str(q)
            for q in (_acceptance_detail.get("regression_debt_qids") or [])
            if str(q)
        )
        if _regression_debt_qids_for_next_iteration:
            print(
                _section("REGRESSION DEBT CARRIED FORWARD", "-") + "\n"
                + _kv("QIDs", list(_regression_debt_qids_for_next_iteration)) + "\n"
                + _bar("-")
            )

        new_refs = extract_reference_sqls(full_result)
        if new_refs:
            reference_sqls.update(new_refs)
        new_hashes = extract_reference_result_hashes(full_result)
        if new_hashes:
            reference_result_hashes.update(new_hashes)

        update_run_status(
            spark, run_id, catalog, schema,
            best_iteration=best_iteration,
            best_accuracy=best_accuracy,
            best_model_id=best_model_id,
        )

        post_instructions = _get_general_instructions(
            apply_log.get("post_snapshot", metadata_snapshot)
        )
        if post_instructions:
            register_instruction_version(
                uc_schema=f"{catalog}.{schema}",
                space_id=space_id,
                instruction_text=post_instructions,
                run_id=run_id,
                lever=0,
                iteration=iteration_counter,
                accuracy=best_accuracy,
                domain=domain,
            )

        write_stage(
            spark, run_id, f"AG_{ag_id}_STARTED", "COMPLETE",
            task_key="lever_loop", iteration=iteration_counter,
            detail={
                "accuracy": full_accuracy,
                "accepted": True,
                "patches_applied": len(apply_log.get("applied", [])),
                "levers": lever_keys,
            },
            catalog=catalog, schema=schema,
        )

        metadata_snapshot = apply_log.get("post_snapshot", metadata_snapshot)
        if _original_instruction_sections:
            metadata_snapshot["_original_instruction_sections"] = _original_instruction_sections

        # Phase 7: end-of-iteration diagnostics. Single block summarizing
        # ASI source quality, cluster processing, patch capping, and
        # pre-arbiter accuracy so operators can spot degenerate values
        # at a glance. Any field at a danger threshold (ASI=none > 50%,
        # arbiter rescue > 30%) emits an inline SEVERITY:HIGH banner.
        try:
            _diag_lines = [_section("LEVER_LOOP_DIAGNOSTICS", "=")]
            _diag_lines.append(_kv("Iteration", iteration_counter))
            _diag_lines.append(_kv("AG id", ag_id))

            # ASI source histogram from the latest eval rows. The
            # ``_eval_rows`` variable is populated below in the join-
            # mining block but doesn't exist yet at this point in the
            # iteration; pull rows directly from ``full_result``.
            _asi_source_counts: dict[str, int] = {}
            _asi_total = 0
            _diag_rows = full_result.get("rows", []) if isinstance(full_result, dict) else []
            if not _diag_rows and isinstance(full_result, dict):
                _rows_json = full_result.get("rows_json")
                if isinstance(_rows_json, list):
                    _diag_rows = _rows_json
                elif isinstance(_rows_json, str):
                    try:
                        import json as _json_diag
                        _diag_rows = _json_diag.loads(_rows_json)
                    except (ValueError, TypeError):
                        _diag_rows = []
            for _r in _diag_rows:
                if not isinstance(_r, dict):
                    continue
                for _log in (_r.get("_asi_extraction_log") or []):
                    if not isinstance(_log, dict):
                        continue
                    _src = str(_log.get("source") or "none")
                    _asi_source_counts[_src] = _asi_source_counts.get(_src, 0) + 1
                    _asi_total += 1
            if _asi_total:
                _none_pct = (
                    100 * _asi_source_counts.get("none", 0) / _asi_total
                )
                _hist = ", ".join(
                    f"{k}={v}"
                    for k, v in sorted(
                        _asi_source_counts.items(), key=lambda kv: -kv[1],
                    )
                )
                _diag_lines.append(_kv("ASI source histogram", _hist))
                if _none_pct > 50.0:
                    _diag_lines.append(
                        f"|   [SEVERITY:HIGH] ASI source none={_none_pct:.0f}% > 50% "
                        f"— strategist is reasoning blind on heuristic root_causes"
                    )
            else:
                _diag_lines.append(_kv("ASI source histogram", "(no rows)"))

            # Cluster processing.
            _diag_lines.append(_kv(
                "Hard clusters formed", len(clusters or []),
            ))
            _diag_lines.append(_kv(
                "Soft clusters formed", len(soft_signal_clusters or []),
            ))
            _diag_lines.append(_kv("AGs attempted (cumulative)", len(ags_attempted)))
            _diag_lines.append(_kv("AGs accepted (cumulative)", len(ags_accepted)))
            _diag_lines.append(_kv("AGs rolled back (cumulative)", len(ags_rolled_back)))
            _diag_lines.append(_kv("Pending AGs in buffer", len(pending_action_groups)))

            # Patch cap.
            _diag_lines.append(_kv("Patches applied this AG", len(apply_log.get("applied", []))))

            # Pre-arbiter accuracy + arbiter rescue rate.
            _full_scores = (
                full_result.get("scores", {})
                if isinstance(full_result, dict) else {}
            )
            _pre_arb = _full_scores.get(
                "_pre_arbiter/overall_accuracy",
                _full_scores.get("_pre_arbiter/result_correctness"),
            )
            _adj = (
                full_result.get("overall_accuracy")
                if isinstance(full_result, dict) else None
            )
            if _pre_arb is not None:
                _diag_lines.append(
                    _kv("Pre-arbiter accuracy", f"{_pre_arb:.1f}%"),
                )
            if _adj is not None:
                _diag_lines.append(
                    _kv("Arbiter-adjusted accuracy", f"{_adj:.1f}%"),
                )
            _bcr = (
                full_result.get("both_correct_rate")
                if isinstance(full_result, dict) else None
            )
            if isinstance(_bcr, (int, float)) and _bcr is not None:
                _alljudge = float(
                    _full_scores.get("_pre_arbiter/result_correctness", 0.0)
                ) / 100.0
                # Task 2 — ``both_correct_rate`` may arrive as 0-100 or 0-1
                # depending on producer. Normalise to a fraction before
                # subtracting ``_alljudge`` and clamp to the valid
                # probability range so the printed value cannot exceed 100%.
                _bcr_frac = float(_bcr) / 100.0 if float(_bcr) > 1.0 else float(_bcr)
                _rescue = max(0.0, min(1.0, _bcr_frac - _alljudge))
                _diag_lines.append(
                    _kv("Arbiter rescue rate", f"{_rescue*100:.1f}%"),
                )
                if _rescue > 0.30:
                    _diag_lines.append(
                        f"|   [SEVERITY:HIGH] Arbiter rescue rate "
                        f"{_rescue*100:.1f}% > 30% — pre-arbiter is the "
                        f"truthful signal"
                    )

            _diag_lines.append(_bar("="))
            print("\n".join(_diag_lines))
        except Exception:
            logger.debug(
                "Phase 7: LEVER_LOOP_DIAGNOSTICS block failed (non-fatal)",
                exc_info=True,
            )

        # ── Mine execution-proven joins from latest eval rows ────────
        try:
            _eval_rows = full_result.get("rows", [])
            if not _eval_rows:
                _eval_rows_json = full_result.get("rows_json")
                if isinstance(_eval_rows_json, str):
                    import json as _json_mod
                    try:
                        _eval_rows = _json_mod.loads(_eval_rows_json)
                    except (ValueError, TypeError):
                        _eval_rows = []
                elif isinstance(_eval_rows_json, list):
                    _eval_rows = _eval_rows_json
            if _eval_rows:
                _mine_result = _mine_and_apply_proven_joins(
                    w, spark, run_id, space_id, metadata_snapshot, _eval_rows,
                    catalog, schema, iteration=iteration_counter,
                )
                if _mine_result.get("total_applied", 0) > 0:
                    from genie_space_optimizer.common.genie_client import fetch_space_config as _fetch_cfg
                    config = _fetch_cfg(w, space_id)
                    metadata_snapshot = config.get("_parsed_space", config)
        except Exception:
            logger.debug(
                "Iterative join mining failed at iter %d (non-fatal)",
                iteration_counter, exc_info=True,
            )

        # Phase A — Lossless contract: stamp post_eval for every qid
        # that entered eval, with was/is/transition derived from the
        # pre-iteration and post-iteration passing sets. The contract
        # requires every evaluated qid to terminate in POST_EVAL.
        try:
            _post_eval_full_result = _latest_eval_result or {}
            _post_eval_eval_qids = list(
                _post_eval_full_result.get("question_ids") or []
            )
            _post_eval_failure_set = set(
                _post_eval_full_result.get("failure_question_ids") or []
            )
            _post_eval_is_passing = [
                str(q)
                for q in _post_eval_eval_qids
                if str(q) and str(q) not in _post_eval_failure_set
            ]
            _emit_post_eval_journey(
                emit=_journey_emit,
                eval_qids=_post_eval_eval_qids,
                was_passing_qids=list(_prev_passing_qids),
                is_passing_qids=_post_eval_is_passing,
            )
            # Capture the post-iteration passing set as next iteration's
            # was-passing baseline.
            _prev_passing_qids = set(_post_eval_is_passing)
            # Phase A — capture the post-eval passing set in the replay
            # fixture iteration snapshot.
            try:
                _current_iter_inputs["post_eval_passing_qids"] = sorted(
                    _post_eval_is_passing
                )
            except Exception:
                logger.debug(
                    "Phase A: post_eval_passing_qids capture failed (non-fatal)",
                    exc_info=True,
                )
        except Exception:
            logger.debug(
                "Phase A: post_eval journey emit failed (non-fatal)",
                exc_info=True,
            )

        # Phase F+H A5 v2.1: F8.decide() (above, pre-3B.7) emits the
        # QID_RESOLUTION records via stages/acceptance.py:decide →
        # post_eval_resolution_records (acceptance.py:230-240). The
        # harness inline post_eval_resolution_records emit block
        # formerly here is deleted to prevent double-emission.

        # Phase A — note: the iteration snapshot was appended at
        # iteration begin via ``begin_iteration_capture``. No late append
        # is needed; mutations to ``_current_iter_inputs`` above already
        # reach ``_replay_fixture_iterations`` because they share a ref.

        # Lossless contract Task 7 — warn-only journey-contract validator.
        # The hard gate flips this to raise in Phase 4. Defensive wrap so a
        # validator bug never breaks the loop while we burn down warnings.
        # L4a: capture the report and stash it on _current_iter_inputs so the
        # fixture exporter and MLflow per-iteration write (next try-block)
        # both see it.
        _journey_report = None
        try:
            _eval_qids_for_validator = list(
                (_latest_eval_result or {}).get("question_ids") or []
            )
            _journey_report = _validate_journeys_at_iteration_end(
                events=_journey_events,
                eval_qids=_eval_qids_for_validator,
                iteration=iteration_counter,
                raise_on_violation=False,
            )
            if _journey_report is not None:
                _current_iter_inputs["journey_validation"] = (
                    _journey_report.to_dict()
                )
        except Exception:
            logger.debug(
                "iteration-end journey validator failed (non-fatal)",
                exc_info=True,
            )

        # Phase E.0 Task 5 — anchor phase_a/journey_validation/ to the
        # lever_loop sibling instead of mlflow.active_run() (which is
        # whichever stage end_run/start_run last started). Surfaces
        # success/failure via GSO_PHASE_A_ARTIFACT_V1 stdout marker.
        if _journey_report is not None:
            _phase_a_artifact_path = (
                f"phase_a/journey_validation/iter_{iteration_counter}.json"
            )
            _phase_a_result = _persist_phase_a_artifact_to_anchor(
                opt_run_id=run_id,
                iteration=iteration_counter,
                report_dict=_journey_report.to_dict(),
            )
            if not _phase_a_result.success:
                logger.warning(
                    "Phase A: anchored persistence failed: %s",
                    _phase_a_result.exception_class,
                )
            from genie_space_optimizer.common.mlflow_markers import (
                phase_a_artifact_marker,
            )
            print(phase_a_artifact_marker(
                optimization_run_id=run_id,
                iteration=iteration_counter,
                anchor_run_id=_phase_a_result.anchor_run_id,
                artifact_path=_phase_a_artifact_path,
                success=_phase_a_result.success,
                exception_class=_phase_a_result.exception_class,
            ))

        # Phase B Trace Plan Task 7 — render the operator transcript and
        # persist the per-iteration decision trace + transcript to MLflow.
        # All best-effort wrapped: missing mlflow / no active run / no
        # decisions captured this iteration is silently skipped.
        try:
            from genie_space_optimizer.optimization.rca_decision_trace import (
                DecisionRecord,
                OptimizationTrace,
                canonical_decision_json,
                render_operator_transcript,
                validate_decisions_against_journey,
            )

            _decision_records = [
                DecisionRecord.from_dict(r)
                for r in (_current_iter_inputs.get("decision_records") or [])
            ]
            if _decision_records:
                _decision_validation = validate_decisions_against_journey(
                    records=_decision_records,
                    events=_journey_events,
                )
                _trace = OptimizationTrace(
                    journey_events=tuple(_journey_events),
                    decision_records=tuple(_decision_records),
                )
                # Spine stage 10 — Learning / Next Action. Classify
                # each unresolved qid in this iteration's trace into a
                # FailureBucket and refresh ``_prior_buckets_by_qid``
                # so the next iteration's spine stage 4 (Action Group
                # Selection) consumes it via ``ActionGroupsInput.
                # prior_buckets_by_qid``. The classifier is pure (no
                # side effects); the call is wrapped in try/except so
                # any failure preserves the prior map rather than
                # corrupting carry-over state.
                try:
                    from genie_space_optimizer.optimization.failure_bucketing import (
                        classify_unresolved_qid as _classify_unresolved_qid,
                    )
                    from genie_space_optimizer.optimization.rca_decision_trace import (
                        DecisionOutcome as _DecisionOutcome,
                    )
                    _next_buckets: dict[str, Any] = {}
                    _qids_seen: set[str] = set()
                    for _r in _decision_records:
                        _qid = str(getattr(_r, "question_id", "") or "")
                        if not _qid or _qid in _qids_seen:
                            continue
                        if getattr(_r, "outcome", None) != _DecisionOutcome.UNRESOLVED:
                            continue
                        _qids_seen.add(_qid)
                        _classification = _classify_unresolved_qid(
                            _trace, _qid, iteration=iteration_counter,
                        )
                        if _classification.bucket is not None:
                            _next_buckets[_qid] = _classification.bucket
                    _prior_buckets_by_qid = _next_buckets
                except Exception:
                    logger.debug(
                        "Spine stage 10: bucket classification failed "
                        "(non-fatal); prior_buckets_by_qid carried over "
                        "unchanged",
                        exc_info=True,
                    )
                # Cycle 5 T2 — refresh the cross-iteration carry-over
                # for dropped causal patches. The next iteration's
                # ActionGroupsInput threads this list into the
                # strategist's prompt context (gated by the flag).
                try:
                    _prior_iteration_dropped_causal_patches = list(
                        _iter_dropped_causal
                    )
                except Exception:
                    logger.debug(
                        "Cycle 5 T2: dropped-causal carry-over refresh "
                        "failed (non-fatal); prior list unchanged",
                        exc_info=True,
                    )
                _transcript = render_operator_transcript(
                    trace=_trace,
                    iteration=iteration_counter,
                )
                print(_transcript)
                _phase_b_decision_artifact = (
                    f"phase_b/decision_trace/iter_{iteration_counter}.json"
                )
                _phase_b_transcript_artifact = (
                    f"phase_b/operator_transcript/iter_{iteration_counter}.txt"
                )
                print(phase_b_marker(
                    optimization_run_id=run_id,
                    iteration=iteration_counter,
                    decision_record_count=len(_decision_records),
                    decision_validation_count=len(_decision_validation),
                    transcript_chars=len(_transcript),
                    decision_trace_artifact=_phase_b_decision_artifact,
                    operator_transcript_artifact=_phase_b_transcript_artifact,
                    persist_ok=True,
                ))
                # Phase E.0 Task 5 — anchor phase_b/ to the lever_loop
                # sibling. Replaces mlflow.active_run() persistence so
                # decision_trace + operator_transcript land alongside
                # phase_a/ on the same operator-discoverable run.
                _phase_b_result = _persist_phase_b_artifacts_to_anchor(
                    opt_run_id=run_id,
                    iteration=iteration_counter,
                    decision_json=canonical_decision_json(_decision_records),
                    transcript=_transcript,
                    record_count=len(_decision_records),
                    violation_count=len(_decision_validation),
                )
                if not _phase_b_result.success:
                    logger.warning(
                        "Phase B: anchored persistence failed: %s",
                        _phase_b_result.exception_class,
                    )
                from genie_space_optimizer.common.mlflow_markers import (
                    phase_b_artifact_marker,
                )
                print(phase_b_artifact_marker(
                    optimization_run_id=run_id,
                    iteration=iteration_counter,
                    anchor_run_id=_phase_b_result.anchor_run_id,
                    decision_trace_path=_phase_b_decision_artifact,
                    operator_transcript_path=_phase_b_transcript_artifact,
                    success=_phase_b_result.success,
                    exception_class=_phase_b_result.exception_class,
                ))
        except Exception:
            logger.debug(
                "Phase B: decision trace persistence skipped (non-fatal)",
                exc_info=True,
            )
            try:
                import mlflow as _mlflow_phase_b_partial  # type: ignore[import-not-found]
                if _mlflow_phase_b_partial.active_run() is not None:
                    _mlflow_phase_b_partial.set_tag(
                        "genie.phase_b.partial", "true"
                    )
            except Exception:
                logger.debug("Phase B partial tag skipped", exc_info=True)

        # Phase B observability follow-up — per-iteration accounting
        # for the manifest + no-records diagnostic. ALWAYS runs, even
        # when the persistence block above short-circuited (no records,
        # missing mlflow, etc.). This is what makes the postmortem
        # analyzer able to distinguish "Phase B ran but had nothing to
        # record" from "Phase B never ran" or "deploy is stale".
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                NoRecordsReason as _NoRecordsReason,
                classify_no_records_reason as _classify_no_records_reason,
            )
            from genie_space_optimizer.optimization.rca_decision_trace import (
                DecisionRecord as _DecisionRecord_pb,
                validate_decisions_against_journey as _validate_pb,
            )
            from genie_space_optimizer.optimization.run_analysis_contract import (
                phase_b_no_records_marker as _phase_b_no_records_marker,
            )

            _iter_records_dicts = list(
                _current_iter_inputs.get("decision_records") or []
            )
            _iter_record_count = len(_iter_records_dicts)
            _phase_b_iter_record_counts.append(_iter_record_count)

            if _iter_record_count == 0:
                # No-records diagnostic — emit a stable stdout marker +
                # MLflow tag with a reason from the closed
                # NoRecordsReason vocabulary.
                _no_rec_reason = _classify_no_records_reason(
                    iteration_inputs=_current_iter_inputs,
                    producer_exceptions=_iter_producer_exceptions,
                )
                _phase_b_no_records_iterations.append(int(iteration_counter))
                _phase_b_iter_violation_counts.append(0)
                print(_phase_b_no_records_marker(
                    optimization_run_id=run_id,
                    iteration=iteration_counter,
                    reason=_no_rec_reason.value,
                    producer_exceptions=dict(_iter_producer_exceptions),
                    contract_version=_PHASE_B_CONTRACT_VERSION,
                ))
                try:
                    import mlflow as _mlflow_no_rec  # type: ignore[import-not-found]
                    if _mlflow_no_rec.active_run() is not None:
                        _mlflow_no_rec.set_tags({
                            (
                                f"decision_trace.iter_{iteration_counter}."
                                "no_records_reason"
                            ): _no_rec_reason.value,
                            (
                                f"decision_trace.iter_{iteration_counter}.records"
                            ): "0",
                        })
                except Exception:
                    logger.debug(
                        "Phase B no-records MLflow tag skipped (non-fatal)",
                        exc_info=True,
                    )
            else:
                # Records were captured — track artifact path + count
                # violations.
                _phase_b_artifact_paths.append(
                    f"phase_b/decision_trace/iter_{iteration_counter}.json"
                )
                try:
                    _typed_records = [
                        _DecisionRecord_pb.from_dict(r) for r in _iter_records_dicts
                    ]
                    _violations = _validate_pb(
                        records=_typed_records,
                        events=_journey_events,
                    )
                    _phase_b_iter_violation_counts.append(len(_violations))
                    _phase_b_total_violations += len(_violations)
                except Exception:
                    _phase_b_iter_violation_counts.append(0)
        except Exception:
            logger.debug(
                "Phase B: per-iter accounting skipped (non-fatal)",
                exc_info=True,
            )

        # Phase C Task 6 — emit STRATEGIST_AG_EMITTED+UNRESOLVED+
        # RCA_UNGROUNDED for findings whose qids are not covered by
        # any emitted AG. Fires once per iteration after all AGs have
        # been processed so action_groups is fully populated.
        try:
            from genie_space_optimizer.optimization.decision_emitters import (
                orphan_rca_records as _orphan_rca_records,
            )
            from genie_space_optimizer.optimization.rca import (
                rca_findings_from_clusters as _rca_findings_from_clusters_c6,
            )

            _phase_c_findings_orphan = _rca_findings_from_clusters_c6(
                clusters or []
            )
            _strategy_for_orphan = (
                _current_iter_inputs.get("strategist_response") or {}
            )
            _orphan_records = _orphan_rca_records(
                run_id=run_id,
                iteration=iteration_counter,
                findings=_phase_c_findings_orphan,
                action_groups=(_strategy_for_orphan.get("action_groups") or []),
            )
            _current_iter_inputs.setdefault("decision_records", []).extend(
                [r.to_dict() for r in _orphan_records]
            )
        except Exception:
            _phase_b_producer_exceptions["orphan_rca"] = (
                _phase_b_producer_exceptions.get("orphan_rca", 0) + 1
            )
            logger.debug(
                "Phase C: orphan_rca_records failed (non-fatal)",
                exc_info=True,
            )
            if _phase_b_strict_mode():
                raise

        # Cycle 5 T1 — productive-iteration budget accounting end-of-iter
        # decision. Gated by ``GSO_PRODUCTIVE_ITERATION_BUDGET`` (Option
        # A: zero behaviour change with flag off → no record emitted, no
        # marker, no counter decrement). With flag on, an iteration that
        # applied zero patches AND produced a typed P4 reason
        # (``proposal_generation_empty``,
        # ``structural_gate_dropped_instruction_only``,
        # ``no_structural_candidate``) is classified as a deterministic
        # no-op and does not consume the iteration counter; the
        # iteration_budget_decision_record + GSO_ITERATION_BUDGET_V1
        # marker make the accounting auditable.
        try:
            from genie_space_optimizer.common.config import (
                productive_iteration_budget_enabled,
            )
            if productive_iteration_budget_enabled():
                from genie_space_optimizer.optimization.decision_emitters import (
                    iteration_budget_decision_record,
                )
                from genie_space_optimizer.common.mlflow_markers import (
                    iteration_budget_marker,
                )
                if _iter_applied_count == 0:
                    _iter_no_op_cause = _classify_iteration_no_op_cause(
                        _current_iter_inputs.get("decision_records") or []
                    )
                    # Only treat as deterministic no-op when one of the
                    # typed P4 reasons fired this iteration. Untyped /
                    # unexplained no-ops still consume budget so the
                    # loop terminates on plateau rather than spinning.
                    if _iter_no_op_cause:
                        _iter_consumed = False
                _iter_budget_rec = iteration_budget_decision_record(
                    run_id=str(run_id),
                    iteration=int(iteration_counter),
                    consumed=_iter_consumed,
                    no_op_cause=(_iter_no_op_cause or None),
                    applied_patches=int(_iter_applied_count),
                )
                # Cycle 6 F-1 — gate duplicate emits within the iteration.
                # The counter decrement and marker print live inside the
                # gate too, since both are non-idempotent side effects.
                _iter_budget_key = _emit_idempotency_key(
                    _iter_budget_rec.to_dict()
                )
                if _iter_budget_key not in _iter_emitted_keys:
                    _iter_emitted_keys.add(_iter_budget_key)
                    _decision_emit(_iter_budget_rec)
                    _current_iter_inputs.setdefault(
                        "decision_records", []
                    ).append(_iter_budget_rec.to_dict())
                    _counter_after = (
                        iteration_counter
                        if _iter_consumed
                        else iteration_counter - 1
                    )
                    print(iteration_budget_marker(
                        optimization_run_id=str(run_id),
                        iteration=int(iteration_counter),
                        consumed=_iter_consumed,
                        no_op_cause=str(_iter_no_op_cause or ""),
                        applied_patches=int(_iter_applied_count),
                        iteration_counter_after=int(_counter_after),
                    ))
                    if not _iter_consumed:
                        iteration_counter -= 1
        except Exception:
            logger.debug(
                "Cycle 5 T1: iteration_budget emit failed (non-fatal)",
                exc_info=True,
            )

        # GSO run analysis: emit machine-readable per-iteration summary
        # so the analyzer skill can build a postmortem without scraping
        # freeform logs.
        try:
            _iter_ag_outcomes = (_current_iter_inputs.get("ag_outcomes") or {})
            _accepted_count = sum(
                1 for v in _iter_ag_outcomes.values()
                if str(v).startswith("accepted")
            )
            _rolled_back_count = sum(
                1 for v in _iter_ag_outcomes.values()
                if str(v) == "rolled_back"
            )
            _skipped_count = sum(
                1 for v in _iter_ag_outcomes.values()
                if str(v).startswith("skipped")
            )
            _gate_drop_count = sum(
                1
                for r in (_current_iter_inputs.get("decision_records") or [])
                if str(r.get("outcome") or "") == "dropped"
            )
            try:
                _scoreboard_loop_snapshot = {
                    "iteration": int(iteration_counter),
                    "passing_qids": list(
                        _current_iter_inputs.get("post_eval_passing_qids") or []
                    ),
                    "hard_failure_qids": [
                        qid
                        for c in (_current_iter_inputs.get("clusters") or [])
                        for qid in (c.get("question_ids") or [])
                    ],
                    "applied_patch_count": _accepted_count,
                    "rolled_back_patch_count": _rolled_back_count,
                    "trace_id_fallback_count": 0,
                    "trace_id_total": 0,
                }
                print(_format_scoreboard_banner(
                    loop_snapshot=_scoreboard_loop_snapshot,
                ))
            except Exception:
                logger.debug(
                    "scoreboard banner failed (non-fatal)", exc_info=True,
                )
            print(iteration_summary_marker(
                optimization_run_id=run_id,
                iteration=iteration_counter,
                accepted_count=_accepted_count,
                rolled_back_count=_rolled_back_count,
                skipped_count=_skipped_count,
                gate_drop_count=_gate_drop_count,
                decision_record_count=len(
                    _current_iter_inputs.get("decision_records") or []
                ),
                journey_violation_count=(
                    0 if _journey_report is None else len(_journey_report.violations)
                ),
            ))
        except Exception:
            logger.debug("GSO iteration summary marker skipped", exc_info=True)

        # Phase H content population — every iteration that reaches end-of-body
        # MUST appear in the operator transcript, even when no decision records
        # were produced (those iterations still rendered the per-stage "no
        # decisions emitted" placeholder via PROCESS_STAGE_ORDER). Without this
        # block the transcript only renders the run-overview header.
        try:
            from genie_space_optimizer.optimization.rca_decision_trace import (
                DecisionRecord as _PhaseH_DecisionRecord,
                OptimizationTrace as _PhaseH_OptimizationTrace,
            )

            _phase_h_records_raw = list(
                _current_iter_inputs.get("decision_records") or []
            )
            _phase_h_records: list[Any] = []
            for _r in _phase_h_records_raw:
                try:
                    _phase_h_records.append(
                        _PhaseH_DecisionRecord.from_dict(_r)
                    )
                except Exception:
                    continue
            _iter_traces[iteration_counter] = _PhaseH_OptimizationTrace(
                journey_events=tuple(_journey_events or ()),
                decision_records=tuple(_phase_h_records),
            )
            _iter_acc_pct: float | None
            try:
                _iter_acc_pct = (
                    float(best_accuracy)
                    if iteration_counter == best_iteration
                    else None
                )
            except Exception:
                _iter_acc_pct = None
            _iter_summaries[iteration_counter] = _build_iteration_summary_dict(
                iteration=int(iteration_counter),
                accepted_count=int(_accepted_count or 0),
                rolled_back_count=int(_rolled_back_count or 0),
                skipped_count=int(_skipped_count or 0),
                gate_drop_count=int(_gate_drop_count or 0),
                decision_record_count=len(_phase_h_records),
                journey_violation_count=(
                    0 if _journey_report is None
                    else len(_journey_report.violations)
                ),
                iteration_accuracy_percent=_iter_acc_pct,
            )
        except Exception:
            logger.debug(
                "Phase H content population skipped (non-fatal)",
                exc_info=True,
            )

        # Task 13 — render the per-question journey ledger at normal
        # iteration end. Early-exit paths call the same idempotent helper
        # before continuing.
        _render_current_journey()

    write_stage(
        spark, run_id, "LEVER_LOOP_STARTED", "COMPLETE",
        task_key="lever_loop",
        detail={
            "levers_attempted": levers_attempted,
            "levers_accepted": levers_accepted,
            "levers_rolled_back": levers_rolled_back,
            "reflection_buffer": reflection_buffer,
        },
        catalog=catalog, schema=schema,
    )

    # ── End-of-Run Summary ─────────────────────────────────────────
    _summary = [_section("OPTIMIZATION RUN SUMMARY", "=")]
    _summary.append(_kv("Space ID", space_id))
    _summary.append(_kv("Run ID", run_id))
    _summary.append(_kv("Baseline accuracy", f"{baseline_accuracy:.1f}%"))
    _summary.append(_kv("Final accuracy", f"{best_accuracy:.1f}%"))
    _delta = best_accuracy - baseline_accuracy
    _summary.append(_kv("Net improvement", f"{'+' if _delta >= 0 else ''}{_delta:.1f}%"))
    _summary.append(_kv("Iterations", iteration_counter))
    _summary.append(_kv("Best iteration", best_iteration))
    _summary.append("|")

    # Proactive changes
    _summary.append("|  --- Proactive Changes (pre-lever-loop) ---")
    _desc_enriched = enrichment_result.get("total_enriched", 0)
    _tbl_desc_enriched = enrichment_result.get("tables_enriched", 0)
    _joins_applied = join_result.get("total_applied", 0)
    _desc_gen = meta_result.get("description_generated", False)
    _sq_gen = meta_result.get("questions_generated", False)
    _sq_count = meta_result.get("questions_count", 0)
    _summary.append(_kv("Column descriptions added", _desc_enriched))
    _summary.append(_kv("Table descriptions added", _tbl_desc_enriched))
    _summary.append(_kv("Join specs discovered", _joins_applied))
    _summary.append(_kv("Space description", "generated" if _desc_gen else "unchanged"))
    _summary.append(_kv("Sample questions", f"generated ({_sq_count})" if _sq_gen else "unchanged"))
    _instr_seeded = instruction_result.get("instructions_seeded", False)
    _instr_chars = instruction_result.get("instruction_chars", 0)
    _summary.append(_kv("Instructions seeded", f"generated ({_instr_chars} chars)" if _instr_seeded else "unchanged"))
    _mined_count = len(mined_example_proposals) if mined_example_proposals else 0
    _summary.append(_kv("Benchmark examples mined", _mined_count))
    _summary.append("|")

    # Lever loop changes
    _summary.append("|  --- Lever Loop Changes ---")
    _summary.append(_kv("Action groups attempted", len(ags_attempted)))
    _summary.append(_kv("Action groups accepted", len(ags_accepted)))
    # Phase E3: break down rolled-back AGs by rollback_class so operators
    # can tell "3 rolled back (1 content, 2 infra)" from "3 rolled back
    # (3 content)" at a glance.
    _rb_class_counter: Counter[str] = Counter()
    for _rb_entry in reflection_buffer:
        if _rb_entry.get("accepted"):
            continue
        if _rb_entry.get("escalation_handled"):
            continue
        _rb_class_counter[_rb_entry.get("rollback_class", "other")] += 1
    if len(ags_rolled_back) and _rb_class_counter:
        _rb_class_str = ", ".join(
            f"{v} {k}" for k, v in sorted(_rb_class_counter.items())
        )
        _summary.append(_kv(
            "Action groups rolled back",
            f"{len(ags_rolled_back)} ({_rb_class_str})",
        ))
    else:
        _summary.append(_kv("Action groups rolled back", len(ags_rolled_back)))
    _summary.append(_kv("Levers used", sorted(set(levers_attempted)) if levers_attempted else "none"))
    if lever_changes:
        _summary.append("|")
        for lc in lever_changes:
            _delta_str = f"{lc['accuracy_delta']:+.1f}%"
            _summary.append(f"|  {lc['lever_name']}")
            _summary.append(f"|      Accuracy delta: {_delta_str}")
            for p in lc.get("patches", []):
                _ptype = _PATCH_TYPE_LABELS.get(p.get("patch_type", ""), p.get("patch_type", ""))
                _change = p.get("change", "")[:80]
                _summary.append(f"|      - {_ptype}: {_change}")
    elif not ags_accepted:
        _summary.append(_kv("Status", "No lever loop changes were accepted"))

    _summary.append("|")
    _summary.append("|  --- Final Scores ---")
    for sname, sval in sorted(best_scores.items()):
        _summary.append(f"|  {sname + ':':<28s} {sval:.1f}")
    _summary.append(_bar("="))
    print("\n".join(_summary))

    # Phase A — Lossless contract: always emit replay fixture at end-of-run.
    # Two channels:
    #   1. stderr markers (always): user extracts with grep/sed.
    #   2. MLflow artifact (when an active run exists): downloadable from UI.
    # No env-var gating — Databricks Jobs makes that awkward.
    try:
        import sys
        from genie_space_optimizer.optimization.journey_fixture_exporter import (
            serialize_replay_fixture,
            summarize_replay_fixture,
        )

        _replay_fixture_id = f"airline_real_v1_run_{run_id}"
        _replay_fixture_json = serialize_replay_fixture(
            fixture_id=_replay_fixture_id,
            iterations_data=list(_replay_fixture_iterations or []),
        )

        # Operator sanity-check log line: per-iteration counts so a real
        # run can be triaged without parsing the JSON body. If
        # ``iterations`` is 0 or any per-iter ``eval_rows`` is 0,
        # extraction should be paused and the run triaged.
        try:
            import json as _summary_json
            _replay_fixture_summary = summarize_replay_fixture(
                iterations_data=list(_replay_fixture_iterations or []),
            )
            logger.info(
                "Phase A: replay fixture summary %s",
                _summary_json.dumps(
                    _replay_fixture_summary, separators=(",", ":")
                ),
            )
        except Exception:
            logger.debug(
                "Phase A: replay fixture summary log failed (non-fatal)",
                exc_info=True,
            )

        # Channel 1 — stderr markers.
        # Use unique multi-segment markers that cannot collide with normal
        # logger output. The user's extractor script greps on these.
        sys.stderr.write("\n===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n")
        sys.stderr.write(_replay_fixture_json)
        sys.stderr.write("\n===PHASE_A_REPLAY_FIXTURE_JSON_END===\n")
        sys.stderr.flush()
        logger.info(
            "Phase A: emitted replay fixture (%d iterations, %d bytes) to "
            "stderr between markers PHASE_A_REPLAY_FIXTURE_JSON_BEGIN/END",
            len(_replay_fixture_iterations or []),
            len(_replay_fixture_json),
        )

        # Channel 2 — MLflow artifact (best-effort; skips if no active run).
        try:
            import mlflow  # type: ignore[import-not-found]
            import json as _json
            if mlflow.active_run() is not None:
                mlflow.log_dict(
                    _json.loads(_replay_fixture_json),
                    artifact_file="phase_a/airline_real_v1.json",
                )
                logger.info(
                    "Phase A: also logged replay fixture to MLflow "
                    "artifact at phase_a/airline_real_v1.json",
                )
        except Exception:
            logger.debug(
                "Phase A: MLflow artifact log skipped (non-fatal)",
                exc_info=True,
            )
    except Exception:
        logger.warning(
            "Phase A: replay-fixture export failed (non-fatal)",
            exc_info=True,
        )

    # GSO run analysis: emit convergence + end-of-run manifest markers
    # so the analyzer skill knows the loop terminated normally.
    # ``_run_lever_loop`` itself does not own the fine-grained convergence
    # reason — ``_run_finalize`` writes that to ``genie_opt_runs``. The
    # marker here records that the loop body finished and carries the
    # final accuracy and threshold flag.
    try:
        # PR-B2: project plateau / divergence break state onto a typed
        # marker reason so the GSO_CONVERGENCE_V1 reader sees the same
        # vocabulary as the LEVER LOOP — TERMINATION print above.
        _convergence_reason = _resolve_lever_loop_exit_reason(
            _lever_loop_plateau_decision,
            _lever_loop_divergence_label,
        )
        _thresholds_met = bool(all_thresholds_met(best_scores, thresholds))
        print(convergence_marker(
            optimization_run_id=run_id,
            reason=_convergence_reason,
            iteration_counter=iteration_counter,
            best_accuracy=(
                float(best_accuracy) if best_accuracy is not None else None
            ),
            thresholds_met=_thresholds_met,
        ))
        print(run_manifest_marker(
            optimization_run_id=run_id,
            databricks_job_id=_db_job_id,
            databricks_parent_run_id=_db_parent_run_id,
            lever_loop_task_run_id=_db_task_run_id,
            mlflow_experiment_id=str(os.environ.get("MLFLOW_EXPERIMENT_ID") or ""),
            space_id=space_id,
            event="end",
        ))
    except Exception:
        logger.debug("GSO convergence/end marker skipped", exc_info=True)

    # Phase B observability follow-up — emit GSO_PHASE_B_END_V1 marker
    # and build the ``loop_out["phase_b"]`` manifest. The manifest is
    # the CLI-truth surface for the postmortem analyzer because
    # ``databricks jobs get-run-output`` only exposes the
    # ``dbutils.notebook.exit(...)`` JSON for this task; stdout is not
    # surfaced. ``run_lever_loop.py:548-563`` allowlists ``phase_b`` in
    # the debug_info filter so this manifest survives the round trip.
    _phase_b_total_records = sum(_phase_b_iter_record_counts)
    try:
        from genie_space_optimizer.optimization.run_analysis_contract import (
            phase_b_end_marker as _phase_b_end_marker,
        )
        print(_phase_b_end_marker(
            optimization_run_id=run_id,
            total_records=_phase_b_total_records,
            iter_record_counts=list(_phase_b_iter_record_counts),
            iter_violation_counts=list(_phase_b_iter_violation_counts),
            no_records_iterations=list(_phase_b_no_records_iterations),
            contract_version=_PHASE_B_CONTRACT_VERSION,
        ))
    except Exception:
        logger.debug("Phase B end marker emission skipped", exc_info=True)

    # Phase F+H C18 (v2, Phase-H reliability fix): split rendering from
    # upload. Rendering the operator transcript and bundle JSONs runs
    # unconditionally so the notebook can always print the transcript to
    # stdout (recoverable via ``databricks jobs export-run``). The
    # parent-run artifact upload and the GSO_ARTIFACT_INDEX_V1 marker
    # only run when a stable Phase H anchor was resolved/created by
    # ``resolve_or_create_phase_h_anchor``; upload failures are reported
    # via GSO_BUNDLE_ASSEMBLY_FAILED_V1 without dropping the rendered
    # transcript from the loop-out return value.
    _phase_h_artifact_index_path: str | None = None
    _phase_h_iterations_completed: list[int] = []
    _full_transcript: str | None = None
    _phase_h_upload_status: str = "not_attempted"

    try:
        import json as _json_phase_h_c18
        from genie_space_optimizer.optimization.operator_process_transcript import (
            render_full_transcript as _render_full_transcript,
            render_iteration_transcript as _render_iteration_transcript,
            render_run_overview as _render_run_overview,
        )
        from genie_space_optimizer.optimization.run_output_bundle import (
            build_artifact_index as _build_artifact_index,
            build_manifest as _build_manifest,
            build_run_summary as _build_run_summary,
        )
        from genie_space_optimizer.optimization.run_output_contract import (
            bundle_artifact_paths as _bundle_artifact_paths,
        )

        _phase_h_iterations_completed = list(
            range(1, int(iteration_counter) + 1)
        )
        # Cycle 6 F-6 — drain any stage_io_capture failures so the
        # manifest's missing_pieces reflects what the bundle actually
        # contains. Without this drain the manifest would claim
        # missing_pieces=[] even when stage_io_capture caught a
        # RecursionError and skipped the artifact write.
        from genie_space_optimizer.optimization.stage_io_capture import (
            consume_capture_failures,
        )
        _capture_failures = consume_capture_failures()
        _missing_pieces = [
            {
                "kind": "stage_io_capture_failed",
                "stage_key": _f["stage_key"],
                "artifact_path": _f["artifact_path"],
                "error_class": _f["error_class"],
            }
            for _f in _capture_failures
        ]
        _manifest = _build_manifest(
            optimization_run_id=run_id,
            databricks_job_id=_db_job_id,
            databricks_parent_run_id=_db_parent_run_id,
            lever_loop_task_run_id=_db_task_run_id,
            iterations=_phase_h_iterations_completed,
            missing_pieces=_missing_pieces,
        )
        _artifact_index = _build_artifact_index(
            iterations=_phase_h_iterations_completed,
        )
        # Terminal status — _lrn_update may be unbound if the
        # final iteration's F9 stage errored out, so fall back to
        # "max_iterations" (matches the harness's existing
        # convergence-marker default).
        try:
            _terminal_status = (
                _lrn_update.terminal_decision.get("status")  # type: ignore[name-defined]
                or "max_iterations"
            )
        except (NameError, AttributeError):
            _terminal_status = "max_iterations"

        _best_acc_for_delta = (
            float(best_accuracy) if best_accuracy is not None
            else float(prev_accuracy)
        )
        _run_summary = _build_run_summary(
            baseline=_baseline_for_summary,
            terminal_state={
                "status": str(_terminal_status),
                "should_continue": False,
            },
            iteration_count=len(_phase_h_iterations_completed),
            accuracy_delta_pp=round(
                _best_acc_for_delta - float(prev_accuracy), 1
            ),
        )

        _run_overview = _render_run_overview(
            run_id=run_id,
            space_id=space_id,
            domain=domain,
            max_iters=int(max_iterations),
            baseline=_baseline_for_summary,
            hard_failures=_hard_failures_for_overview,
        )
        _iter_transcripts = [
            _render_iteration_transcript(
                iteration=_i,
                trace=_iter_traces.get(_i),
                iteration_summary=_iter_summaries.get(_i, {}),
            )
            for _i in _phase_h_iterations_completed
            if _iter_traces.get(_i) is not None
        ]
        _full_transcript = _render_full_transcript(
            run_overview=_run_overview,
            iteration_transcripts=_iter_transcripts,
        )

        _paths = _bundle_artifact_paths(
            iterations=_phase_h_iterations_completed,
        )
        _phase_h_artifact_index_path = _paths["artifact_index"]

        # Upload path — only runs when a stable anchor is available.
        # On upload failure we emit GSO_BUNDLE_ASSEMBLY_FAILED_V1 but
        # keep the rendered transcript so stdout pretty-print still works.
        if _phase_h_anchor_run_id:
            try:
                from mlflow.tracking import MlflowClient as _MlflowClient
                from genie_space_optimizer.optimization.run_analysis_contract import (
                    artifact_index_marker as _artifact_index_marker,
                )

                _client_phase_h = _MlflowClient()
                _client_phase_h.log_text(
                    run_id=_phase_h_anchor_run_id,
                    text=_json_phase_h_c18.dumps(
                        _manifest, sort_keys=True, indent=2,
                    ),
                    artifact_file=_paths["manifest"],
                )
                _client_phase_h.log_text(
                    run_id=_phase_h_anchor_run_id,
                    text=_json_phase_h_c18.dumps(
                        _artifact_index, sort_keys=True, indent=2,
                    ),
                    artifact_file=_paths["artifact_index"],
                )
                _client_phase_h.log_text(
                    run_id=_phase_h_anchor_run_id,
                    text=_json_phase_h_c18.dumps(
                        _run_summary, sort_keys=True, indent=2,
                    ),
                    artifact_file=_paths["run_summary"],
                )
                _client_phase_h.log_text(
                    run_id=_phase_h_anchor_run_id,
                    text=_full_transcript,
                    artifact_file=_paths["operator_transcript"],
                )
                print(_artifact_index_marker(
                    optimization_run_id=run_id,
                    parent_bundle_run_id=_phase_h_anchor_run_id,
                    artifact_index_path=_phase_h_artifact_index_path,
                    iterations=_phase_h_iterations_completed,
                ))
                _phase_h_upload_status = "uploaded"
            except Exception as _phase_h_upload_exc:
                from genie_space_optimizer.optimization.run_analysis_contract import (
                    bundle_assembly_failed_marker as _bundle_assembly_failed_marker,
                )
                logger.warning(
                    "Phase H bundle upload failed; stdout pretty-print "
                    "still available via loop_out['pretty_print_transcript']",
                    exc_info=True,
                )
                print(_bundle_assembly_failed_marker(
                    optimization_run_id=run_id,
                    parent_bundle_run_id=_phase_h_anchor_run_id,
                    error_type=type(_phase_h_upload_exc).__name__,
                    error_message=str(_phase_h_upload_exc),
                ))
                _phase_h_upload_status = "upload_failed"
        else:
            _phase_h_upload_status = "skipped_no_anchor"
    except Exception as _phase_h_render_exc:
        from genie_space_optimizer.optimization.run_analysis_contract import (
            bundle_assembly_failed_marker as _bundle_assembly_failed_marker,
        )
        logger.warning(
            "Phase H bundle assembly (rendering) failed; stdout "
            "pretty-print will be unavailable for this run",
            exc_info=True,
        )
        print(_bundle_assembly_failed_marker(
            optimization_run_id=run_id,
            parent_bundle_run_id=_phase_h_anchor_run_id,
            error_type=type(_phase_h_render_exc).__name__,
            error_message=str(_phase_h_render_exc),
        ))
        _full_transcript = None
        _phase_h_upload_status = "render_failed"

    _loop_out_base = {
        "scores": best_scores,
        "accuracy": best_accuracy,
        "model_id": best_model_id,
        "iteration_counter": iteration_counter,
        "best_iteration": best_iteration,
        "levers_attempted": levers_attempted,
        "levers_accepted": levers_accepted,
        "levers_rolled_back": levers_rolled_back,
        "question_trace_map": question_trace_map,
        "reflection_buffer": reflection_buffer,
        "all_eval_mlflow_run_ids": list(dict.fromkeys(all_eval_mlflow_run_ids)),
        "all_failure_trace_ids": list(dict.fromkeys(all_failure_trace_ids)),
        "all_regression_trace_ids": list(dict.fromkeys(all_regression_trace_ids)),
        "all_failure_question_ids": list(dict.fromkeys(all_failure_question_ids)),
        "_debug_ref_sqls_count": len(reference_sqls),
        "_debug_failure_rows_loaded": len(_get_failure_rows(spark, run_id, catalog, schema)),
        "phase_b": {
            "contract_version": _PHASE_B_CONTRACT_VERSION,
            "decision_records_total": int(_phase_b_total_records),
            "iter_record_counts": list(_phase_b_iter_record_counts),
            "iter_violation_counts": list(_phase_b_iter_violation_counts),
            "no_records_iterations": list(_phase_b_no_records_iterations),
            "artifact_paths": list(_phase_b_artifact_paths),
            "producer_exceptions": dict(_phase_b_producer_exceptions),
            "target_qids_missing_count": int(_phase_b_target_qids_missing_count),
            "total_violations": int(_phase_b_total_violations),
        },
        # Phase F+H C18 (v2) — Phase H T13: bundle pointers for the
        # exit JSON. None when the parent MLflow run wasn't captured
        # (replay path); run_lever_loop.py's call to
        # lever_loop_exit_manifest() omits the keys in that case.
        "phase_h_anchor_run_id": _phase_h_anchor_run_id,
        "phase_h_artifact_index_path": _phase_h_artifact_index_path,
        "phase_h_iterations_completed": list(_phase_h_iterations_completed),
        # Phase-H reliability fix: surface the upload path outcome so
        # the notebook fallback log can explain what happened
        # (uploaded / skipped_no_anchor / upload_failed / render_failed
        # / not_attempted). ``phase_h_pretty_print_status`` and
        # ``phase_h_pretty_print_reason`` are stamped by
        # ``_build_loop_out_with_pretty_print`` below.
        "phase_h_upload_status": _phase_h_upload_status,
    }
    return _build_loop_out_with_pretty_print(
        loop_out_base=_loop_out_base,
        phase_h_full_transcript=_full_transcript,
        phase_h_anchor_run_id=_phase_h_anchor_run_id,
    )


# ── Stage 4: FINALIZE ───────────────────────────────────────────────


def _run_finalize(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    exp_name: str,
    prev_scores: dict[str, float],
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
    run_repeatability: bool = True,
    benchmarks: list[dict] | None = None,
    thresholds: dict[str, float] | None = None,
    finalize_timeout_seconds: int = FINALIZE_TIMEOUT_SECONDS,
    heartbeat_interval_seconds: int = FINALIZE_HEARTBEAT_SECONDS,
    question_trace_map: dict[str, list[str]] | None = None,
    reflection_buffer: list[dict] | None = None,
    all_eval_mlflow_run_ids: list[str] | None = None,
    all_failure_trace_ids: list[str] | None = None,
    all_regression_trace_ids: list[str] | None = None,
    all_failure_question_ids: list[str] | None = None,
    max_iterations: int | None = None,
    deploy_target: str | None = None,
) -> dict:
    """Stage 4: Repeatability test, promote model, generate report.

    Adds heartbeat events + a soft timeout so long-running finalization
    remains observable and fails with an explicit terminal reason.
    """
    thresholds = thresholds or DEFAULT_THRESHOLDS
    max_iterations = max_iterations or MAX_ITERATIONS
    finalize_timeout_seconds = max(1, int(finalize_timeout_seconds))
    heartbeat_interval_seconds = max(5, int(heartbeat_interval_seconds))
    started_monotonic = time.monotonic()
    last_heartbeat = 0.0
    heartbeat_count = 0
    current_phase = "initializing"

    def _elapsed_seconds() -> float:
        return time.monotonic() - started_monotonic

    def _check_timeout(phase: str) -> None:
        elapsed = _elapsed_seconds()
        if elapsed > finalize_timeout_seconds:
            raise TimeoutError(
                f"Finalize exceeded timeout ({finalize_timeout_seconds}s) "
                f"during {phase} after {elapsed:.1f}s",
            )

    def _emit_heartbeat(
        phase: str,
        *,
        detail: dict[str, Any] | None = None,
        force: bool = False,
    ) -> None:
        nonlocal last_heartbeat, heartbeat_count, current_phase
        current_phase = phase
        now = time.monotonic()
        if not force and (now - last_heartbeat) < heartbeat_interval_seconds:
            return
        last_heartbeat = now
        heartbeat_count += 1

        heartbeat_detail: dict[str, Any] = {
            "phase": phase,
            "elapsed_seconds": round(_elapsed_seconds(), 1),
            "heartbeat_count": heartbeat_count,
            "timeout_seconds": finalize_timeout_seconds,
        }
        if detail:
            heartbeat_detail.update(detail)

        try:
            # Touch run.updated_at so stale-state reconciliation doesn't mark finalize as dead.
            update_run_status(spark, run_id, catalog, schema)
            write_stage(
                spark, run_id, "FINALIZE_HEARTBEAT", "STARTED",
                task_key="finalize",
                detail=heartbeat_detail,
                catalog=catalog, schema=schema,
            )
        except Exception:
            logger.warning(
                "Failed to persist finalize heartbeat for run %s",
                run_id,
                exc_info=True,
            )

    write_stage(
        spark, run_id, "FINALIZE_STARTED", "STARTED",
        task_key="finalize",
        detail={
            "timeout_seconds": finalize_timeout_seconds,
            "heartbeat_interval_seconds": heartbeat_interval_seconds,
        },
        catalog=catalog, schema=schema,
    )
    _emit_heartbeat("finalize_started", force=True)

    question_trace_map = question_trace_map or {}
    reflection_buffer = reflection_buffer or []
    all_eval_mlflow_run_ids = list(all_eval_mlflow_run_ids or [])
    all_failure_trace_ids = list(all_failure_trace_ids or [])
    all_regression_trace_ids = list(all_regression_trace_ids or [])
    all_failure_question_ids = list(all_failure_question_ids or [])

    # Pre-seed question_trace_map from ALL eval runs (incl. baseline) so that
    # persistent-failure questions always have trace IDs for tagging and session
    # population, even when the lever loop never re-evaluated them.
    if all_eval_mlflow_run_ids:
        try:
            import mlflow
            from genie_space_optimizer.optimization.labeling import _extract_question_id
            for _seed_rid in dict.fromkeys(all_eval_mlflow_run_ids):
                try:
                    _seed_traces = mlflow.search_traces(run_id=_seed_rid)
                    if _seed_traces is not None and len(_seed_traces) > 0 and "request" in _seed_traces.columns:
                        for _, row in _seed_traces.iterrows():
                            _qid = _extract_question_id(row.get("request"))
                            _tid = row.get("trace_id", "")
                            if _qid and _tid:
                                question_trace_map.setdefault(_qid, []).append(_tid)
                except Exception as _seed_exc:
                    logger.debug("Failed to seed trace map from eval run %s: %s", _seed_rid, _seed_exc)
            # Deduplicate trace IDs per question
            for _qid in question_trace_map:
                question_trace_map[_qid] = list(dict.fromkeys(question_trace_map[_qid]))
            logger.info(
                "Pre-seeded question_trace_map with %d questions from %d eval runs",
                len(question_trace_map), len(all_eval_mlflow_run_ids),
            )
        except Exception as _seed_err:
            logger.warning("Failed to pre-seed question_trace_map: %s", _seed_err, exc_info=True)

    terminal_reason = ""
    repeatability_pct = 0.0
    try:
        # ── Phase 0: Split benchmarks ──
        train_benchmarks = [b for b in (benchmarks or []) if b.get("split") != "held_out"]
        held_out_benchmarks = [b for b in (benchmarks or []) if b.get("split") == "held_out"]

        # ── Phase 1: Repeatability Testing ──
        _lines = [_section("FINALIZE — REPEATABILITY TESTING", "-")]
        _lines.append(_kv("Run repeatability", run_repeatability))
        _lines.append(_kv("Train benchmarks", len(train_benchmarks)))
        _lines.append(_kv("Held-out benchmarks", len(held_out_benchmarks)))
        _lines.append(_bar("-"))
        print("\n".join(_lines))

        rep_results: list[dict] = []
        held_out_accuracy: float | None = None
        _check_timeout("pre_repeatability")
        if run_repeatability and train_benchmarks:
            write_stage(
                spark, run_id, "REPEATABILITY_TEST", "STARTED",
                task_key="finalize", catalog=catalog, schema=schema,
            )
            _emit_heartbeat(
                "repeatability_test",
                force=True,
                detail={"benchmark_count": len(train_benchmarks), "runs": FINALIZE_REPEATABILITY_PASSES},
            )

            uc_schema = f"{catalog}.{schema}"
            latest_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
            reference_sqls: dict[str, str] = {}
            reference_result_hashes: dict[str, str] = {}
            if latest_iter:
                rows_json = latest_iter.get("rows_json")
                if isinstance(rows_json, list):
                    _rows_payload = {"rows": rows_json}
                    reference_sqls = extract_reference_sqls(_rows_payload)
                    reference_result_hashes = extract_reference_result_hashes(_rows_payload)
                elif isinstance(rows_json, str):
                    try:
                        _rows_payload = {"rows": json.loads(rows_json)}
                        reference_sqls = extract_reference_sqls(_rows_payload)
                        reference_result_hashes = extract_reference_result_hashes(_rows_payload)
                    except (json.JSONDecodeError, TypeError):
                        pass
            if not reference_sqls and train_benchmarks:
                logger.warning("No reference SQLs from iterations — extracting from benchmarks")
                for b in train_benchmarks:
                    qid = b.get("id", "")
                    sql = b.get("expected_sql", "")
                    if qid and sql:
                        reference_sqls[qid] = sql
            logger.info(
                "Repeatability: %d reference SQLs, %d result hashes loaded",
                len(reference_sqls),
                len(reference_result_hashes),
            )
            print(
                f"  Repeatability: {len(reference_sqls)} reference SQLs, "
                f"{len(reference_result_hashes)} result hashes loaded"
            )

            _ensure_sql_context(spark, catalog, schema)
            predict_fn = make_predict_fn(
                w, space_id, spark, catalog, schema,
                warehouse_id=resolve_warehouse_id(""),
            )

            rep_pcts: list[float] = []
            try:
                for rep_run_idx in range(1, FINALIZE_REPEATABILITY_PASSES + 1):
                    _check_timeout(f"repeatability_run_{rep_run_idx}")
                    _emit_heartbeat(
                        f"repeatability_run_{rep_run_idx}",
                        force=True,
                        detail={"run": rep_run_idx, "of": FINALIZE_REPEATABILITY_PASSES},
                    )
                    # Tier 4: v2 name — ``<run_short>/finalize/repeat_pass_{k}``.
                    from genie_space_optimizer.common.mlflow_names import (
                        default_tags as _v2_tags_rep,
                        finalize_run_name as _finalize_run_name_rep,
                    )
                    rep_result = run_repeatability_evaluation(
                        space_id=space_id,
                        experiment_name=exp_name,
                        iteration=iteration_counter,
                        benchmarks=train_benchmarks,
                        domain=domain,
                        reference_sqls=reference_sqls,
                        predict_fn=predict_fn,
                        spark=spark,
                        catalog=catalog,
                        gold_schema=schema,
                        uc_schema=uc_schema,
                        model_id=prev_model_id,
                        run_label=f"final_{rep_run_idx}",
                        reference_result_hashes=reference_result_hashes,
                        run_name=_finalize_run_name_rep(
                            run_id,
                            detail=f"repeat_pass_{rep_run_idx}",
                            iteration=iteration_counter,
                        ),
                        extra_tags=_v2_tags_rep(
                            run_id, space_id=space_id, stage="finalize_repeatability",
                            iteration=iteration_counter,
                        ),
                    )
                    rep_results.append(rep_result)
                    rep_pcts.append(rep_result.get("repeatability_pct", 0.0))
                    logger.info(
                        "Repeatability run %d/%d: %.1f%%",
                        rep_run_idx,
                        FINALIZE_REPEATABILITY_PASSES,
                        rep_pcts[-1],
                    )

                _check_timeout("post_repeatability")
                repeatability_pct = (
                    sum(rep_pcts) / len(rep_pcts) if rep_pcts else 0.0
                )
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "COMPLETE",
                    task_key="finalize",
                    detail={
                        "average_pct": repeatability_pct,
                        "per_run_pcts": rep_pcts,
                        "total_questions": len(train_benchmarks),
                    },
                    catalog=catalog, schema=schema,
                )
            except TimeoutError as exc:
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "FAILED",
                    task_key="finalize",
                    detail={"terminal_reason": "finalize_timeout"},
                    error_message=str(exc)[:500],
                    catalog=catalog, schema=schema,
                )
                raise
            except Exception:
                logger.exception("Repeatability evaluation failed")
                repeatability_pct = (
                    sum(rep_pcts) / len(rep_pcts) if rep_pcts else 0.0
                )
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "FAILED",
                    task_key="finalize",
                    error_message="Repeatability evaluation exception",
                    detail={"partial_pcts": rep_pcts},
                    catalog=catalog, schema=schema,
                )
        else:
            _emit_heartbeat(
                "repeatability_skipped",
                force=True,
                detail={"reason": "disabled_or_no_benchmarks"},
            )

        # ── Phase 2: Human Review Session ──
        _rep_lines = [_section("FINALIZE — REPEATABILITY RESULTS", "-")]
        _rep_lines.append(_kv("Average repeatability", f"{repeatability_pct:.1f}%"))
        _rep_lines.append(_kv("Passes completed", len(rep_results)))
        _rep_lines.append(_bar("-"))
        print("\n".join(_rep_lines))

        # ── Phase 1b: Held-Out Generalization Check ──
        if held_out_benchmarks:
            try:
                write_stage(
                    spark, run_id, "HELD_OUT_EVAL", "STARTED",
                    task_key="finalize", catalog=catalog, schema=schema,
                )
                _emit_heartbeat(
                    "held_out_eval", force=True,
                    detail={"held_out_count": len(held_out_benchmarks)},
                )
                _check_timeout("held_out_eval")

                _ensure_sql_context(spark, catalog, schema)
                ho_predict_fn = make_predict_fn(
                    w, space_id, spark, catalog, schema,
                    warehouse_id=resolve_warehouse_id(""),
                )
                try:
                    from genie_space_optimizer.common.genie_client import fetch_space_config as _ho_fetch
                    _ho_cfg = _ho_fetch(w, space_id)
                    _ho_parsed = _ho_cfg.get("_parsed_space", _ho_cfg)
                    _ho_instr = _ho_parsed.get("instructions", {}) if isinstance(_ho_parsed, dict) else {}
                    _ho_instr_text = _ho_instr.get("text_instructions", "") if isinstance(_ho_instr, dict) else ""
                except Exception:
                    _ho_instr_text = ""
                ho_scorers = make_all_scorers(w, spark, catalog, schema, instruction_context=_ho_instr_text)

                # Tier 4: v2 name — ``<run_short>/finalize/held_out``.
                from genie_space_optimizer.common.mlflow_names import (
                    default_tags as _v2_tags_ho,
                    finalize_run_name as _finalize_run_name_ho,
                )
                held_out_result = run_evaluation(
                    space_id, exp_name, iteration_counter, held_out_benchmarks,
                    domain, prev_model_id, "held_out",
                    ho_predict_fn, ho_scorers,
                    spark=spark, w=w, catalog=catalog, gold_schema=schema,
                    uc_schema=f"{catalog}.{schema}",
                    run_name=_finalize_run_name_ho(
                        run_id, detail="held_out", iteration=iteration_counter,
                    ),
                    extra_tags=_v2_tags_ho(
                        run_id, space_id=space_id, stage="finalize_held_out",
                        iteration=iteration_counter,
                    ),
                )

                write_iteration(
                    spark, run_id, iteration_counter, held_out_result,
                    catalog=catalog, schema=schema,
                    eval_scope="held_out", model_id=prev_model_id,
                )

                held_out_accuracy = held_out_result.get("overall_accuracy", 0.0)
                train_accuracy = prev_scores.get(
                    "genie_correct",
                    prev_scores.get("overall_accuracy", 0.0),
                )
                delta = train_accuracy - held_out_accuracy
                _ho_lines = [_section("FINALIZE — HELD-OUT GENERALIZATION CHECK", "-")]
                _ho_lines.append(_kv("Train accuracy", f"{train_accuracy:.1f}%"))
                _ho_lines.append(_kv("Held-out accuracy", f"{held_out_accuracy:.1f}%"))
                _ho_lines.append(_kv("Delta", f"{delta:+.1f} pp"))
                _ho_lines.append(_kv("Held-out questions", len(held_out_benchmarks)))
                if delta > 15.0:
                    _ho_lines.append(_kv("Warning", "Possible instruction overfitting (>15pp gap)"))
                _ho_lines.append(_bar("-"))
                print("\n".join(_ho_lines))

                write_stage(
                    spark, run_id, "HELD_OUT_EVAL", "COMPLETE",
                    task_key="finalize",
                    detail={
                        "held_out_accuracy": held_out_accuracy,
                        "train_accuracy": train_accuracy,
                        "delta_pp": round(delta, 1),
                        "held_out_count": len(held_out_benchmarks),
                    },
                    catalog=catalog, schema=schema,
                )

                _best_eval_run_id = (latest_iter or {}).get("mlflow_run_id", "")
                if _best_eval_run_id:
                    try:
                        from mlflow.tracking import MlflowClient as _HoMlflowClient
                        _ho_client = _HoMlflowClient()
                        _ho_client.log_metric(_best_eval_run_id, "held_out_accuracy", held_out_accuracy)
                        _ho_client.log_metric(_best_eval_run_id, "held_out_count", float(len(held_out_benchmarks)))
                        logger.info(
                            "Logged held-out metrics to eval run %s: accuracy=%.1f%%, count=%d",
                            _best_eval_run_id, held_out_accuracy, len(held_out_benchmarks),
                        )
                    except Exception:
                        logger.debug("Failed to log held-out metrics to eval run", exc_info=True)
            except TimeoutError:
                raise
            except Exception:
                logger.exception("Held-out evaluation failed — continuing")
                write_stage(
                    spark, run_id, "HELD_OUT_EVAL", "FAILED",
                    task_key="finalize",
                    error_message="Held-out evaluation exception",
                    catalog=catalog, schema=schema,
                )
        else:
            write_stage(
                spark, run_id, "HELD_OUT_EVAL", "SKIPPED",
                task_key="finalize",
                detail={"reason": "no_held_out_benchmarks"},
                catalog=catalog, schema=schema,
            )

        _review_lines = [_section("FINALIZE — HUMAN REVIEW SESSION", "-")]
        print("\n".join(_review_lines))

        _check_timeout("human_review_session")
        _emit_heartbeat("human_review_session", force=True)
        session_info: dict = {}
        try:
            for _rr in (rep_results if run_repeatability and benchmarks else []):
                _rr_tmap = _rr.get("trace_map", {})
                for qid, tid in _rr_tmap.items():
                    question_trace_map.setdefault(qid, []).append(tid)
                _rr_run_id = _rr.get("mlflow_run_id") or _rr.get("run_id", "")
                if _rr_run_id:
                    all_eval_mlflow_run_ids.append(_rr_run_id)

            _verdict_history = _build_verdict_history(spark, run_id, catalog, schema)
            _persist_text, _persist_data = _build_question_persistence_summary(
                _verdict_history, reflection_buffer,
            )

            from genie_space_optimizer.optimization.evaluation import log_patch_history_on_traces

            persistent_question_ids = [
                qid for qid, ctx in _persist_data.items()
                if ctx["classification"] in ("PERSISTENT", "ADDITIVE_LEVERS_EXHAUSTED")
            ]

            if _persist_data:
                log_persistence_context_on_traces(
                    {}, _persist_data, extra_trace_map=question_trace_map,
                )
            if reflection_buffer and question_trace_map:
                log_patch_history_on_traces(
                    question_trace_map, reflection_buffer,
                    persistent_question_ids=set(persistent_question_ids) if persistent_question_ids else None,
                )

            _persistent_items = [
                {
                    "question_id": qid,
                    "question_text": ctx.get("question_text", ""),
                    "reason": ctx["classification"],
                    "iterations_failed": ctx["fail_count"],
                    "patches_tried": str(ctx.get("patches_tried", [])),
                }
                for qid, ctx in _persist_data.items()
                if ctx["classification"] in ("PERSISTENT", "ADDITIVE_LEVERS_EXHAUSTED")
            ]
            if _persistent_items:
                from genie_space_optimizer.optimization.labeling import flag_for_human_review
                flag_for_human_review(spark, run_id, catalog, schema, domain, _persistent_items)
                _persistent_qids = {item["question_id"] for item in _persistent_items}
                all_failure_question_ids.extend(
                    qid for qid in _persistent_qids if qid not in set(all_failure_question_ids)
                )

            # Resolve stale flags for questions that now pass in the latest evaluation
            _PASSING_VERDICTS = {"both_correct", "genie_correct"}
            _now_passing: set[str] = set()
            for _qid, _entries in _verdict_history.items():
                if _entries and _entries[-1].verdict in _PASSING_VERDICTS:
                    _now_passing.add(_qid)
            _still_failing = {item["question_id"] for item in _persistent_items} if _persistent_items else set()
            _resolve_candidates = _now_passing - _still_failing
            if _resolve_candidates:
                try:
                    from genie_space_optimizer.optimization.labeling import resolve_stale_flags
                    _resolved = resolve_stale_flags(spark, catalog, schema, domain, _resolve_candidates)
                    if _resolved:
                        logger.info("Resolved %d stale flag(s) for now-passing questions", _resolved)
                except Exception:
                    logger.debug("Failed to resolve stale flags", exc_info=True)

            _session_trace_ids = [
                question_trace_map[qid][-1]
                for qid in persistent_question_ids
                if qid in question_trace_map
            ] if persistent_question_ids else []
            _session_question_ids = [
                qid for qid in persistent_question_ids if qid in question_trace_map
            ] if persistent_question_ids else []

            if _session_question_ids:
                from genie_space_optimizer.optimization.labeling import create_review_session
                session_info = create_review_session(
                    run_id=run_id,
                    domain=domain,
                    experiment_name=exp_name,
                    uc_schema=f"{catalog}.{schema}",
                    failure_trace_ids=list(dict.fromkeys(_session_trace_ids)),
                    regression_trace_ids=[],
                    eval_mlflow_run_ids=list(dict.fromkeys(all_eval_mlflow_run_ids)),
                    failure_question_ids=list(dict.fromkeys(_session_question_ids)),
                    flagged_trace_ids=list(dict.fromkeys(_session_trace_ids)),
                )
                _sname = session_info.get("session_name", "")
                _srun = session_info.get("session_run_id", "")
                _surl = session_info.get("session_url", "")
                if _sname:
                    print(
                        f"\n[MLflow Review] Labeling session created for human review:\n"
                        f"  Name: {_sname}\n"
                        f"  Traces: {session_info.get('trace_count', 0)}\n"
                        f"  Persistent questions: {len(_session_question_ids)}\n"
                    )
                    if _surl:
                        print(f"  URL: {_surl}\n")
                if _sname or _srun:
                    update_run_status(
                        spark, run_id, catalog, schema,
                        labeling_session_name=_sname,
                        labeling_session_run_id=_srun,
                        labeling_session_url=_surl,
                    )
            else:
                print("\n[MLflow Review] No persistent failures — skipping labeling session creation\n")
        except Exception as exc:
            print(f"[Labeling] Failed to create post-repeatability review session: {exc}")
            logger.warning("Failed to create post-repeatability review session", exc_info=True)

        # ── Phase 3: Model Promotion & Report ──
        _promo_lines = [_section("FINALIZE — MODEL PROMOTION & REPORT", "-")]
        print("\n".join(_promo_lines))

        _check_timeout("promote_best_model")
        _emit_heartbeat("promote_best_model", force=True)
        promoted_model = promote_best_model(spark, run_id, catalog, schema)

        from genie_space_optimizer.optimization.models import register_uc_model
        uc_result = register_uc_model(spark, run_id, catalog, schema, ws=w, deploy_target=deploy_target)

        if uc_result:
            _uc_lines = [_section("FINALIZE — UC MODEL REGISTRATION", "-")]
            _uc_lines.append(_kv("UC Model", uc_result["uc_model_name"]))
            _uc_lines.append(_kv("Version", uc_result["version"]))
            _uc_lines.append(_kv("Champion", "YES" if uc_result["promoted_to_champion"] else "NO (existing champion is better)"))
            if uc_result.get("comparison"):
                for _judge, _cmp in uc_result["comparison"].items():
                    _uc_lines.append(_kv(f"  {_judge}", f"new={_cmp['new']:.1f} vs existing={_cmp['existing']:.1f}"))
            _uc_lines.append(_bar("-"))
            print("\n".join(_uc_lines))

        _check_timeout("generate_report")
        _emit_heartbeat("generate_report", force=True)
        report_path = generate_report(spark, run_id, domain, catalog, schema)

        # Publish benchmarks to the Genie Space's native benchmarks section
        # so they appear in the Genie UI and can be used for UI-based eval runs.
        # Merge-not-overwrite with existing user-authored benchmarks; tagged
        # with [auto-optimize] + source metadata. Opt-out via
        # GSO_PUBLISH_BENCHMARKS_TO_SPACE=0.
        benchmark_publish_count = 0
        _check_timeout("publish_benchmarks")
        _emit_heartbeat("publish_benchmarks", force=True)
        from genie_space_optimizer.common.config import PUBLISH_BENCHMARKS_TO_SPACE
        if benchmarks and PUBLISH_BENCHMARKS_TO_SPACE:
            try:
                from genie_space_optimizer.common.genie_client import (
                    publish_benchmarks_to_genie_space,
                )

                benchmark_publish_count = publish_benchmarks_to_genie_space(
                    w, space_id, benchmarks, run_id=run_id,
                )
                write_stage(
                    spark, run_id, "BENCHMARK_PUBLISH", "COMPLETE",
                    task_key="finalize",
                    detail={"published_count": benchmark_publish_count},
                    catalog=catalog, schema=schema,
                )
            except Exception:
                logger.warning(
                    "Failed to publish benchmarks to Genie space %s — "
                    "optimization results are still valid",
                    space_id,
                    exc_info=True,
                )
                write_stage(
                    spark, run_id, "BENCHMARK_PUBLISH", "FAILED",
                    task_key="finalize",
                    error_message="Benchmark publish failed (non-fatal)",
                    catalog=catalog, schema=schema,
                )
        else:
            write_stage(
                spark, run_id, "BENCHMARK_PUBLISH", "SKIPPED",
                task_key="finalize",
                detail={"reason": "no_benchmarks_available"},
                catalog=catalog, schema=schema,
            )

        _promo_result_lines = [_section("FINALIZE — PROMOTION RESULTS", "-")]
        _promo_result_lines.append(_kv("Promoted model", promoted_model or "(none)"))
        _promo_result_lines.append(_kv("Report path", report_path or "(none)"))
        _promo_result_lines.append(_kv("Benchmarks published", benchmark_publish_count))
        _promo_result_lines.append(_bar("-"))
        print("\n".join(_promo_result_lines))

        # ── Phase 4: Terminal Status Resolution ──
        _term_lines = [_section("FINALIZE — TERMINAL STATUS RESOLUTION", "-")]
        print("\n".join(_term_lines))

        _check_timeout("resolve_terminal_status")
        from genie_space_optimizer.optimization.acceptance_policy import (
            arbiter_objective_complete,
        )

        prev_accuracy = float(prev_scores.get("accuracy", 0.0)) if isinstance(prev_scores, dict) else 0.0
        objective_met = arbiter_objective_complete(float(prev_accuracy))
        thresholds_met = all_thresholds_met(prev_scores, thresholds)
        if objective_met:
            status = "CONVERGED"
            reason = "post_arbiter_objective_met"
        elif thresholds_met:
            status = "CONVERGED"
            reason = "threshold_met"
        elif iteration_counter >= max_iterations:
            status = "MAX_ITERATIONS"
            reason = "max_iterations"
        else:
            status = "STALLED"
            reason = "no_further_improvement"

        terminal_reason = f"finalize_completed:{reason}"

        # Postflight IQ scan (soft-fail). Flag-gated via scan_snapshots; runs
        # before the terminal status write so the phase='postflight' row is
        # committed even if update_run_status subsequently fails.
        try:
            from genie_space_optimizer.optimization.scan_snapshots import (
                run_postflight_scan,
            )
            run_postflight_scan(
                w, spark, run_id, space_id, catalog, schema,
                best_accuracy=prev_scores.get("accuracy") if isinstance(prev_scores, dict) else None,
            )
        except Exception:
            logger.warning(
                "Postflight scan hook raised for run=%s — continuing",
                run_id, exc_info=True,
            )

        update_run_status(
            spark, run_id, catalog, schema,
            status=status,
            convergence_reason=reason,
            best_repeatability=repeatability_pct,
        )

        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "COMPLETE",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "status": status,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "COMPLETE",
            task_key="finalize",
            detail={
                "status": status,
                "report_path": report_path,
                "promoted_model": promoted_model,
                "repeatability_pct": repeatability_pct,
                "terminal_reason": terminal_reason,
                "heartbeat_count": heartbeat_count,
                "uc_model_name": (uc_result or {}).get("uc_model_name", ""),
                "uc_model_version": (uc_result or {}).get("version", ""),
                "uc_champion_promoted": (uc_result or {}).get("promoted_to_champion", False),
            },
            catalog=catalog, schema=schema,
        )

        _term_result_lines = [_section("FINALIZE — FINAL STATUS", "-")]
        _term_result_lines.append(_kv("Status", status))
        _term_result_lines.append(_kv("Convergence reason", reason))
        _term_result_lines.append(_kv("Repeatability", f"{repeatability_pct:.1f}%"))
        _term_result_lines.append(_kv("Promoted model", promoted_model or "(none)"))
        _term_result_lines.append(_kv("Report path", report_path or "(none)"))
        _term_result_lines.append(_kv("Elapsed", f"{_elapsed_seconds():.1f}s"))
        _term_result_lines.append(_kv("Heartbeats", heartbeat_count))
        _term_result_lines.append(_bar("-"))
        print("\n".join(_term_result_lines))

        return {
            "status": status,
            "convergence_reason": reason,
            "repeatability_pct": repeatability_pct,
            "report_path": report_path,
            "promoted_model": promoted_model,
            "terminal_reason": terminal_reason,
            "benchmark_publish_count": benchmark_publish_count,
            "labeling_session": session_info,
            "uc_registration": uc_result,
            "elapsed_seconds": round(_elapsed_seconds(), 1),
            "heartbeat_count": heartbeat_count,
            "held_out_accuracy": held_out_accuracy,
            "held_out_count": len(held_out_benchmarks) if held_out_benchmarks else 0,
        }

    except TimeoutError as exc:
        terminal_reason = "finalize_timeout"
        logger.exception("Finalize timeout for run %s", run_id)
        update_run_status(
            spark, run_id, catalog, schema,
            status="FAILED",
            convergence_reason=terminal_reason,
            best_repeatability=repeatability_pct,
        )
        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "FAILED",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "phase": current_phase,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            error_message=str(exc)[:500],
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "FAILED",
            task_key="finalize",
            detail={"terminal_reason": terminal_reason, "phase": current_phase},
            error_message=str(exc)[:500],
            catalog=catalog, schema=schema,
        )
        raise
    except Exception as exc:
        terminal_reason = "finalize_error"
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("Finalize failure for run %s", run_id)
        update_run_status(
            spark, run_id, catalog, schema,
            status="FAILED",
            convergence_reason=terminal_reason,
            best_repeatability=repeatability_pct,
        )
        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "FAILED",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "phase": current_phase,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "FAILED",
            task_key="finalize",
            detail={"terminal_reason": terminal_reason, "phase": current_phase},
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        raise


# ── Stage 5: DEPLOY ─────────────────────────────────────────────────


def deploy_check(
    deploy_target: str | None,
    prev_model_id: str,
    iteration_counter: int,
) -> dict:
    """Sub-step 5a: Check deploy eligibility and print target info."""
    _lines = [_section("DEPLOY — GATE CHECK", "-")]
    _lines.append(_kv("Deploy target", deploy_target or "(none — will skip)"))
    _lines.append(_kv("Model ID", prev_model_id))
    _lines.append(_kv("Iteration", _iteration_label(iteration_counter)))
    _lines.append(_kv("Decision", "PROCEED" if deploy_target else "SKIP"))
    _lines.append(_bar("-"))
    print("\n".join(_lines))
    return {
        "should_deploy": bool(deploy_target),
        "deploy_target": deploy_target,
        "prev_model_id": prev_model_id,
        "iteration_counter": iteration_counter,
    }


def deploy_execute(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    deploy_target: str | None,
    space_id: str,
    exp_name: str,
    domain: str,
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
) -> dict:
    """Sub-step 5b: Execute deployment or skip. Writes Delta stages."""
    if not deploy_target:
        write_stage(
            spark, run_id, "DEPLOY_SKIPPED", "SKIPPED",
            task_key="deploy",
            detail={"reason": "no_deploy_target"},
            catalog=catalog, schema=schema,
        )
        _lines = [_section("DEPLOY — RESULT", "-")]
        _lines.append(_kv("Status", "SKIPPED"))
        _lines.append(_kv("Reason", "no deploy target configured"))
        _lines.append(_bar("-"))
        print("\n".join(_lines))
        return {"status": "SKIPPED", "reason": "no_deploy_target"}

    write_stage(
        spark, run_id, "DEPLOY_DELEGATED", "COMPLETE",
        task_key="deploy",
        detail={"deploy_target": deploy_target, "mechanism": "mlflow_deployment_job"},
        catalog=catalog, schema=schema,
    )
    _lines = [_section("DEPLOY — RESULT", "-")]
    _lines.append(_kv("Status", "PENDING_APPROVAL"))
    _lines.append(_kv("Target", deploy_target))
    _lines.append(_kv("Model ID", prev_model_id))
    _lines.append(_kv("Mechanism", "MLflow Deployment Job (Approval -> Cross-env Deploy)"))
    _lines.append(_bar("-"))
    print("\n".join(_lines))
    return {"status": "PENDING_APPROVAL", "deploy_target": deploy_target}


def _run_deploy(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    deploy_target: str | None,
    space_id: str,
    exp_name: str,
    domain: str,
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 5: Deploy via DABs, held-out evaluation (optional).

    Wrapper that calls deploy_check() then deploy_execute() in sequence.
    """
    deploy_check(deploy_target, prev_model_id, iteration_counter)
    return deploy_execute(
        w, spark, run_id, deploy_target, space_id, exp_name,
        domain, prev_model_id, iteration_counter, catalog, schema,
    )


# ── Resume Helper ────────────────────────────────────────────────────


def _resume_lever_loop(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> dict:
    """Read Delta to find last completed lever for resume after task retry.

    Returns: resume_from_lever, iteration_counter, prev_scores, prev_model_id,
    reflection_buffer, tried_patches, tried_root_causes, skill_exemplars.

    Uses :func:`load_latest_state_iteration` so cold-start runs that
    have a Task 3 ``eval_scope='enrichment'`` row see the post-
    enrichment scores/accuracy rather than the stale Task 2
    ``eval_scope='full'`` baseline. Without this, the unconditional
    overrides further down in ``_run_lever_loop`` (``if
    resume_state.get("prev_accuracy"): prev_accuracy = ...``) clobber
    the orchestrator-resolved post-enrichment value with the pre-
    enrichment baseline — producing a 81.8% baseline display when
    enrichment had already lifted the space to 86.4%.
    """
    latest_iter = load_latest_state_iteration(spark, run_id, catalog, schema)
    if not latest_iter:
        # S10 — ``None`` signals "no prior lever". Previously we returned
        # ``0`` which collides with a legitimate "loop starts at index 0"
        # reading. The display label ``Starting fresh`` makes the intent
        # explicit; see the _resume_display block in ``run_lever_loop``.
        return {"resume_from_lever": None, "iteration_counter": 0}

    stages_df = load_stages(spark, run_id, catalog, schema)
    last_lever: int | None = None
    if not stages_df.empty:
        lever_stages = stages_df[
            stages_df["stage"].str.startswith("LEVER_")
            & (stages_df["status"] == "COMPLETE")
        ]
        if not lever_stages.empty:
            lever_nums = lever_stages["lever"].dropna().astype(int)
            if not lever_nums.empty:
                last_lever = int(lever_nums.max())

    scores_json = latest_iter.get("scores_json", {})
    if isinstance(scores_json, str):
        try:
            scores_json = json.loads(scores_json)
        except (json.JSONDecodeError, TypeError):
            scores_json = {}

    all_iters = load_all_full_iterations(spark, run_id, catalog, schema)
    restored_reflections: list[dict] = []
    restored_tried_patches: set[tuple[str, str]] = set()
    restored_tried_root_causes: set[tuple] = set()
    restored_skill_exemplars: list[dict] = []
    for it in all_iters:
        rj = it.get("reflection_json")
        if not isinstance(rj, dict):
            continue
        # Normalize blame in-place so downstream readers (forbidden-set
        # computation, resume display) see the same tuple shape the live
        # writer persisted before JSON round-trip flattened it to a list.
        rj = dict(rj)
        rj["blame_set"] = _normalise_blame(rj.get("blame_set"))
        restored_reflections.append(rj)
        for dnr in rj.get("do_not_retry", []):
            parts = dnr.split(" on ", 1)
            if len(parts) == 2:
                restored_tried_patches.add((parts[0], parts[1]))
        if not rj.get("accepted"):
            root_cause = rj.get("root_cause", "")
            blame = rj.get("blame_set", "")
            if root_cause and blame:
                # Always-hashable 2-tuple (legacy contract) for back-compat
                # with consumers that test the 2-tuple form.
                restored_tried_root_causes.add((root_cause, blame))
                # Add the live-path 3-tuple shape too, when the persisted
                # reflection carries the lever set. Mirrors harness.py:14348.
                lever_raw = rj.get("lever_set") or []
                lever_set = frozenset(
                    int(x) for x in lever_raw if str(x).isdigit()
                )
                if lever_set:
                    restored_tried_root_causes.add(
                        (root_cause, blame, lever_set)
                    )
        if rj.get("accepted") and rj.get("accuracy_delta", 0.0) >= 1.0:
            restored_skill_exemplars.append({
                "root_cause": rj.get("root_cause", ""),
                "lever_pattern": rj.get("levers", []),
                "patch_types": [x for x in rj.get("patch_types", []) if x is not None],
                "accuracy_gain": rj.get("accuracy_delta", 0.0),
            })

    if restored_reflections:
        logger.info(
            "Restored %d reflection entries from Delta for resume",
            len(restored_reflections),
        )

    return {
        "resume_from_lever": last_lever,
        "iteration_counter": int(latest_iter.get("iteration", 0)),
        "prev_scores": scores_json if isinstance(scores_json, dict) else {},
        "prev_model_id": latest_iter.get("model_id", ""),
        "prev_accuracy": float(latest_iter.get("overall_accuracy", 0.0)),
        "reflection_buffer": restored_reflections,
        "tried_patches": restored_tried_patches,
        "tried_root_causes": restored_tried_root_causes,
        "skill_exemplars": restored_skill_exemplars,
    }


# ── Evaluation via Job (for multi-task architecture) ─────────────────


def run_evaluation_via_job(
    w: WorkspaceClient,
    space_id: str,
    experiment_name: str,
    iteration: int,
    domain: str,
    model_id: str,
    eval_scope: str,
    **kwargs: Any,
) -> dict:
    """Submit evaluation as a Databricks Job run and poll for results.

    Uses ``w.jobs.submit_run()`` with Serverless compute. Benchmarks
    are loaded from the MLflow evaluation dataset.

    This is the job-based alternative to inline ``run_evaluation()``.
    """
    from genie_space_optimizer.common.config import JOB_MAX_WAIT, JOB_POLL_INTERVAL

    notebook_path = kwargs.get(
        "notebook_path", "/Workspace/genie_space_optimizer/jobs/run_evaluation_only",
    )

    task_params = {
        "space_id": space_id,
        "experiment_name": experiment_name,
        "iteration": str(iteration),
        "domain": domain,
        "model_id": model_id or "",
        "eval_scope": eval_scope,
    }

    try:
        run = w.jobs.submit(
            run_name=f"genie_eval_{space_id}_{iteration}",
            tasks=cast(Any, [
                {
                    "task_key": "evaluation",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": task_params,
                    },
                    "new_cluster": {"spark_version": "auto", "num_workers": 0},
                }
            ]),
        )
        run_id = run.run_id
        logger.info("Submitted evaluation job: run_id=%s", run_id)

        elapsed = 0
        while elapsed < JOB_MAX_WAIT:
            time.sleep(JOB_POLL_INTERVAL)
            elapsed += JOB_POLL_INTERVAL
            status = w.jobs.get_run(run_id)
            state = str(status.state.life_cycle_state) if status.state else "UNKNOWN"
            if state in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                break

        return {
            "job_run_id": str(run_id),
            "status": state,
        }

    except Exception as exc:
        logger.exception("Evaluation job submission failed")
        return {"status": "FAILED", "error": str(exc)}


# ── Convenience Function ─────────────────────────────────────────────


def _resolve_effective_starting_point(
    *,
    baseline_scores: dict[str, float],
    baseline_accuracy: float,
    baseline_thresholds_met: bool,
    baseline_model_id: str,
    enrichment_out: dict | None,
) -> dict[str, Any]:
    """Resolve the *current* starting state for the lever-loop gate.

    Enrichment may mutate the Genie Space and re-evaluate it. When that
    happens the post-enrichment evaluation is the authoritative current
    state of the space — the baseline numbers are stale. This helper
    centralises the choice between baseline and post-enrichment values
    so both the in-process orchestration (``optimize_genie_space``) and
    the Databricks Jobs notebook task (``jobs/run_lever_loop.py``) gate
    on the same data.

    Parameters
    ----------
    baseline_scores, baseline_accuracy, baseline_thresholds_met,
    baseline_model_id
        Values captured immediately after Stage 2 (baseline eval).
    enrichment_out
        Return value of ``_run_enrichment`` (or ``None`` if enrichment
        raised). When ``enrichment_skipped`` is true, or
        ``post_enrichment_accuracy`` is missing, baseline state is kept.

    Returns
    -------
    dict
        ``{"scores", "accuracy", "thresholds_met", "model_id", "source"}``
        where ``source`` is one of:

        * ``"baseline_eval"`` — enrichment skipped or absent.
        * ``"baseline_eval_post_enrichment_missing"`` — enrichment ran
          but did not produce a post-enrichment accuracy (failure /
          early-exit). State stays baseline; the source is logged so
          operators can spot the silent fallback.
        * ``"enrichment.post_enrichment_accuracy"`` — post-enrichment
          evaluation produced a number; that becomes the current state.

    Notes
    -----
    Pure function. No Spark / Workspace dependencies. ``scores`` is
    always returned as an independent dict so callers can mutate it
    without leaking back into ``enrichment_out``.
    """

    resolved: dict[str, Any] = {
        "scores": dict(baseline_scores or {}),
        "accuracy": float(baseline_accuracy),
        "thresholds_met": bool(baseline_thresholds_met),
        "model_id": str(baseline_model_id) if baseline_model_id is not None else "",
        "source": "baseline_eval",
    }

    if not isinstance(enrichment_out, dict):
        return resolved
    if enrichment_out.get("enrichment_skipped"):
        return resolved

    post_acc = enrichment_out.get("post_enrichment_accuracy")
    if post_acc is None:
        # Enrichment ran but did not (re)evaluate — keep baseline state
        # but flag the source so the gate decision is auditable.
        resolved["source"] = "baseline_eval_post_enrichment_missing"
        return resolved

    post_scores = enrichment_out.get("post_enrichment_scores") or {}
    if isinstance(post_scores, dict) and post_scores:
        resolved["scores"] = {
            str(k): float(v) for k, v in post_scores.items() if v is not None
        }

    try:
        resolved["accuracy"] = float(post_acc)
    except (TypeError, ValueError):
        resolved["source"] = "baseline_eval_post_enrichment_missing"
        return resolved

    resolved["thresholds_met"] = bool(
        enrichment_out.get("post_enrichment_thresholds_met", False)
    )
    resolved["model_id"] = str(
        enrichment_out.get("post_enrichment_model_id")
        or enrichment_out.get("enrichment_model_id")
        or baseline_model_id
        or ""
    )
    resolved["source"] = "enrichment.post_enrichment_accuracy"
    return resolved


def optimize_genie_space(
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    *,
    run_id: str | None = None,
    apply_mode: str = APPLY_MODE,
    experiment_name: str | None = None,
    levers: list[int] | None = None,
    max_iterations: int = MAX_ITERATIONS,
    thresholds: dict[str, float] | None = None,
    deploy_target: str | None = None,
    run_repeat: bool = True,
    triggered_by: str | None = None,
    warehouse_id: str = "",
) -> OptimizationResult:
    """Run all 6 stages in a single process.

    When *run_id* is supplied the caller has already created the Delta row
    (e.g. the backend ``start_optimization`` endpoint), so we skip
    ``create_run`` to avoid duplicating the row.
    """
    from genie_space_optimizer._workspace_client import make_workspace_client
    w = make_workspace_client()
    from genie_space_optimizer.common.genie_client import (
        configure_connection_pool,
        configure_mlflow_connection_pool,
    )
    from genie_space_optimizer.common.config import CONNECTION_POOL_SIZE
    configure_connection_pool(w, CONNECTION_POOL_SIZE)
    configure_mlflow_connection_pool(CONNECTION_POOL_SIZE)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    _run_created_here = run_id is None
    if _run_created_here:
        run_id = str(uuid.uuid4())
    assert run_id is not None
    run_id_str = run_id

    ensure_optimization_tables(spark, catalog, schema)

    if _run_created_here:
        create_run(
            spark, run_id_str, space_id, domain, catalog, schema,
            max_iterations=max_iterations,
            levers=levers,
            apply_mode=apply_mode,
            deploy_target=deploy_target,
            triggered_by=triggered_by,
        )

    result = OptimizationResult(
        run_id=run_id_str,
        space_id=space_id,
        domain=domain,
        status="FAILED",
        best_iteration=0,
        best_accuracy=0.0,
        best_repeatability=0.0,
        best_model_id=None,
        convergence_reason=None,
        total_iterations=0,
    )

    try:
        # Stage 1: Preflight
        warehouse_id = resolve_warehouse_id(warehouse_id)
        preflight_out = _run_preflight(
            w, spark, run_id_str, space_id, catalog, schema, domain, experiment_name,
            warehouse_id=warehouse_id,
        )
        config = cast(dict[str, Any], preflight_out["config"])
        benchmarks = cast(list[dict], preflight_out["benchmarks"])
        model_id = str(preflight_out["model_id"])
        human_corrections = cast(list[dict], preflight_out.get("human_corrections", []))
        exp_name = str(preflight_out["experiment_name"])
        result.experiment_name = exp_name
        result.experiment_id = str(preflight_out.get("experiment_id", ""))

        train_benchmarks = [b for b in benchmarks if b.get("split") != "held_out"]
        held_out_benchmarks = [b for b in benchmarks if b.get("split") == "held_out"]
        logger.info(
            "Benchmark split: %d train, %d held_out",
            len(train_benchmarks), len(held_out_benchmarks),
        )

        # Stage 2: Baseline
        baseline_out = _run_baseline(
            w, spark, run_id_str, space_id, train_benchmarks, exp_name,
            model_id, catalog, schema, domain,
        )
        prev_scores = cast(dict[str, float], baseline_out["scores"])
        prev_accuracy = float(baseline_out["overall_accuracy"])
        thresholds_met = bool(baseline_out["thresholds_met"])

        # Extract baseline ``both_correct`` rows for the high-risk lane
        # smoke test in enrichment. Synthetic example SQLs are gated on
        # not regressing this set when staged.
        _baseline_eval_result = baseline_out.get("eval_result") or {}
        _baseline_rows = (
            _baseline_eval_result.get("rows") or []
            if isinstance(_baseline_eval_result, dict) else []
        )
        _baseline_both_correct = [
            r for r in _baseline_rows
            if str(
                ((r.get("arbiter") or {}).get("value")
                 if isinstance(r.get("arbiter"), dict)
                 else r.get("arbiter/value")
                 or r.get("feedback/arbiter/value") or "")
            ) == "both_correct"
        ]

        # Stage 2.5: Proactive Enrichment (always runs)
        _enrichment_out = None
        _effective_model_id = model_id
        try:
            _enrichment_out = _run_enrichment(
                w, spark, run_id_str, space_id, domain, train_benchmarks, exp_name,
                catalog, schema,
                baseline_model_id=model_id,
                held_out_benchmarks=held_out_benchmarks,
                baseline_both_correct_rows=_baseline_both_correct,
            )
            if not _enrichment_out["enrichment_skipped"]:
                _effective_model_id = _enrichment_out["enrichment_model_id"]
        except Exception:
            logger.exception(
                "Enrichment failed for run %s — continuing with baseline model",
                run_id_str,
            )

        # PR 34: gate the lever loop on the *current* evaluated state of
        # the Genie Space, not the stale pre-enrichment baseline. When
        # enrichment mutates the space and post-enrichment eval regresses
        # below thresholds, the run must enter the lever loop instead of
        # silently converging as ``baseline_meets_thresholds``.
        _start = _resolve_effective_starting_point(
            baseline_scores=prev_scores,
            baseline_accuracy=prev_accuracy,
            baseline_thresholds_met=thresholds_met,
            baseline_model_id=model_id,
            enrichment_out=_enrichment_out,
        )
        prev_scores = cast(dict[str, float], _start["scores"])
        prev_accuracy = float(_start["accuracy"])
        thresholds_met = bool(_start["thresholds_met"])
        _accuracy_source = str(_start["source"])
        if _accuracy_source == "enrichment.post_enrichment_accuracy":
            _effective_model_id = str(_start["model_id"]) or _effective_model_id
        logger.info(
            "Lever-loop gate: accuracy_source=%s accuracy=%.2f thresholds_met=%s "
            "effective_model_id=%s",
            _accuracy_source, prev_accuracy, thresholds_met, _effective_model_id,
        )

        if thresholds_met:
            _convergence_reason = (
                "post_enrichment_meets_thresholds"
                if _accuracy_source == "enrichment.post_enrichment_accuracy"
                else "baseline_meets_thresholds"
            )
            result.status = "CONVERGED"
            result.convergence_reason = _convergence_reason
            result.best_accuracy = prev_accuracy
            result.best_model_id = _effective_model_id
            result.final_scores = prev_scores
            try:
                from genie_space_optimizer.optimization.scan_snapshots import (
                    run_postflight_scan,
                )
                run_postflight_scan(
                    w, spark, run_id_str, space_id, catalog, schema,
                    best_accuracy=prev_accuracy,
                )
            except Exception:
                logger.warning(
                    "Postflight scan hook raised for run=%s — continuing",
                    run_id_str, exc_info=True,
                )
            update_run_status(
                spark, run_id_str, catalog, schema,
                status="CONVERGED",
                convergence_reason=_convergence_reason,
            )
        elif prev_accuracy >= 99.0 and not thresholds_met:
            _ao_qids = baseline_out.get("arbiter_overridden_qids", [])
            _ss_qids = baseline_out.get("soft_signal_qids", [])

            # Tier 1.6: if baseline is arbiter-saturated AND the unified
            # hard-failure predicate finds zero hard rows, the loop has
            # literally nothing to cluster on. Previously the loop would
            # still produce ghost clusters (rows where arbiter disagreed
            # despite rc=yes) and attack them, causing the ghost-ceiling
            # rollback cycle. We explicitly tag this as
            # ``arbiter_saturated_no_clusters`` so the convergence reason
            # is diagnosable without log-mining.
            _hard_failure_count = 0
            try:
                from genie_space_optimizer.optimization.evaluation import (
                    row_is_hard_failure,
                )
                _baseline_rows = baseline_out.get("eval_result", {}).get("rows") or []
                _hard_failure_count = sum(
                    1 for r in _baseline_rows if isinstance(r, dict) and row_is_hard_failure(r)
                )
            except Exception:
                logger.debug("Could not compute unified hard-failure count", exc_info=True)

            _saturation_reason = (
                "arbiter_saturated_no_clusters"
                if _hard_failure_count == 0
                else "arbiter_saturated"
            )

            logger.warning(
                "ARBITER-SATURATED (%s): accuracy=%.1f%% but thresholds not met. "
                "%d arbiter-overridden, %d soft-signal questions, %d unified hard failures. "
                "The lever loop cannot improve further — flagging for human review.",
                _saturation_reason, prev_accuracy,
                len(_ao_qids), len(_ss_qids), _hard_failure_count,
            )
            _review_items = []
            for _aq in _ao_qids:
                _review_items.append({
                    "question_id": _aq,
                    "question_text": "",
                    "reason": "ARBITER_SATURATED: arbiter overrode judge failures; "
                              "ground truth may need manual review",
                    "iterations_failed": 0,
                    "patches_tried": "none (arbiter-saturated baseline)",
                })
            if _review_items:
                from genie_space_optimizer.optimization.labeling import flag_for_human_review
                flag_for_human_review(
                    spark, run_id_str, catalog, schema, domain, _review_items,
                )
            result.status = "CONVERGED"
            result.convergence_reason = _saturation_reason
            result.best_accuracy = prev_accuracy
            result.best_model_id = _effective_model_id
            result.final_scores = prev_scores
            write_stage(
                spark, run_id_str, "ARBITER_SATURATED_EXIT", "COMPLETE",
                task_key="lever_loop", catalog=catalog, schema=schema,
                detail={
                    "baseline_accuracy": prev_accuracy,
                    "arbiter_overridden_count": len(_ao_qids),
                    "soft_signal_count": len(_ss_qids),
                    "hard_failure_count": _hard_failure_count,
                    "convergence_reason": _saturation_reason,
                    "thresholds_met": False,
                    "scores": prev_scores,
                },
            )
            try:
                from genie_space_optimizer.optimization.scan_snapshots import (
                    run_postflight_scan,
                )
                run_postflight_scan(
                    w, spark, run_id_str, space_id, catalog, schema,
                    best_accuracy=prev_accuracy,
                )
            except Exception:
                logger.warning(
                    "Postflight scan hook raised for run=%s — continuing",
                    run_id_str, exc_info=True,
                )
            update_run_status(
                spark, run_id_str, catalog, schema,
                status="CONVERGED",
                convergence_reason=_saturation_reason,
            )
        else:
            # Stage 3: Lever Loop (enrichment already done)
            loop_out = _run_lever_loop(
                w, spark, run_id_str, space_id, domain, train_benchmarks, exp_name,
                prev_scores, prev_accuracy, _effective_model_id,
                _enrichment_out["config"] if _enrichment_out else {},
                catalog, schema, levers, max_iterations, thresholds, apply_mode,
                triggered_by=triggered_by or "",
                human_corrections=human_corrections,
                enrichment_done=bool(_enrichment_out),
                enrichment_model_id=_effective_model_id,
                iq_scan_recommended_levers=cast(
                    list[int],
                    preflight_out.get("iq_scan_recommended_levers") or [],
                ),
                iq_scan_summary=cast(
                    dict | None, preflight_out.get("iq_scan_summary"),
                ),
            )
            result.levers_attempted = cast(list[int], loop_out["levers_attempted"])
            result.levers_accepted = cast(list[int], loop_out["levers_accepted"])
            result.levers_rolled_back = cast(list[int], loop_out["levers_rolled_back"])
            result.total_iterations = int(loop_out["iteration_counter"])
            result.best_accuracy = float(loop_out["accuracy"])
            result.best_model_id = str(loop_out["model_id"])
            result.best_iteration = int(loop_out["best_iteration"])
            result.final_scores = cast(dict[str, float], loop_out["scores"])

            prev_scores = cast(dict[str, float], loop_out["scores"])
            prev_model_id = str(loop_out["model_id"])

            # Stage 4: Finalize
            finalize_out = _run_finalize(
                w, spark, run_id_str, space_id, domain, exp_name,
                prev_scores, prev_model_id, int(loop_out["iteration_counter"]),
                catalog, schema, run_repeat, benchmarks, thresholds,
                question_trace_map=loop_out.get("question_trace_map"),
                reflection_buffer=loop_out.get("reflection_buffer"),
                all_eval_mlflow_run_ids=loop_out.get("all_eval_mlflow_run_ids"),
                all_failure_trace_ids=loop_out.get("all_failure_trace_ids"),
                all_regression_trace_ids=loop_out.get("all_regression_trace_ids"),
                all_failure_question_ids=loop_out.get("all_failure_question_ids"),
                max_iterations=max_iterations,
            )
            result.status = str(finalize_out["status"])
            result.convergence_reason = str(finalize_out["convergence_reason"])
            result.best_repeatability = float(finalize_out["repeatability_pct"])
            result.report_path = str(finalize_out["report_path"])

        # Stage 5: Deploy
        _run_deploy(
            w, spark, run_id_str, deploy_target, space_id, exp_name,
            domain, result.best_model_id or "", result.total_iterations,
            catalog, schema,
        )

    except Exception as exc:
        result.status = "FAILED"
        result.error = traceback.format_exc()
        logger.exception("optimize_genie_space failed for run %s", run_id_str)

    return result


# ── Private Helpers ──────────────────────────────────────────────────


def _build_patch_record(entry: dict, lever: int, apply_mode: str) -> dict:
    """Build a patch record dict for Delta from an apply_log entry.

    T2.13: preserves the original proposal-side patch_type in
    ``patch_type`` (so downstream readers that track proposal stats keep
    working) while adding ``applied_patch_type`` / ``applied_patch_detail``
    columns that reflect what the applier actually executed. For a
    downgraded ``rewrite_instruction`` that was split into per-section
    ``update_instruction_section`` children, the original proposal row
    loses its applied record but each child gets its own row with
    ``patch_type=rewrite_instruction, applied_patch_type=update_instruction_section``
    and ``applied_patch_detail="section=ASSET ROUTING; lever=5; split_from=rewrite_instruction"``.
    """
    patch = entry.get("patch", {})
    action = entry.get("action", {})
    applied_type = entry.get("applied_patch_type") or patch.get("type", action.get("action_type", "unknown"))
    proposal_type = entry.get("proposal_patch_type") or patch.get("_proposal_patch_type") or patch.get("type", applied_type)
    return {
        "patch_type": proposal_type,
        "scope": apply_mode if lever <= 3 else "genie_config",
        "risk_level": action.get("risk_level", "medium"),
        "target_object": action.get("target", patch.get("target", "")),
        "patch": patch,
        "command": action.get("command"),
        "rollback": action.get("rollback_command"),
        "proposal_id": patch.get("source_proposal_id", patch.get("proposal_id", "")),
        "applied_patch_type": applied_type,
        "applied_patch_detail": entry.get("applied_patch_detail"),
        "rca_id": patch.get("rca_id"),
        "patch_family": patch.get("patch_family"),
        "target_qids": patch.get("target_qids", []),
    }


# The judge-failure predicates moved to ``evaluation.py`` so the
# ``ground_truth_corrections`` module (Task 1) can import them without
# pulling in ``harness``. The aliases below preserve the legacy
# underscore-prefixed names for existing call sites in this module.
from genie_space_optimizer.optimization.evaluation import (
    get_failed_judges as _get_failed_judges,
    has_individual_judge_failure as _has_individual_judge_failure,
)


def _get_failure_rows(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Load the latest iteration's per-question rows for failure clustering.

    Uses :func:`load_latest_state_iteration` so post-enrichment evals
    (``eval_scope='enrichment'``) are visible to clustering and proposal
    grounding immediately after Task 3 mutates the space — without
    this, callers see the pre-enrichment baseline_eval row and proposal
    grounding can't surface columns referenced only by the post-
    enrichment failures.
    """
    latest = load_latest_state_iteration(spark, run_id, catalog, schema)
    if not latest:
        return []
    rows_json = latest.get("rows_json")
    if isinstance(rows_json, str):
        try:
            return json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(rows_json, list):
        return rows_json
    return []


def _compute_category_performance(
    iteration_rows: list[dict],
    benchmarks: list[dict],
) -> dict[str, dict]:
    """Compute per-category benchmark accuracy from an evaluation iteration.

    Maps each evaluation row to its benchmark's ``category`` and computes
    ``{category: {"total": N, "correct": M}}``.  Used to identify weak
    categories for targeted gap-filling.
    """
    qid_to_category: dict[str, str] = {}
    for b in benchmarks:
        qid = b.get("question_id") or b.get("id") or ""
        cat = b.get("category", "")
        question = b.get("question", "")
        if qid and cat:
            qid_to_category[str(qid)] = cat
        if question and cat:
            qid_to_category[question.lower().strip()] = cat

    category_stats: dict[str, dict] = {}
    for row in iteration_rows:
        req = row.get("request") or row.get("inputs") or {}
        if isinstance(req, dict):
            question = str(req.get("question", "")).lower().strip()
            qid = str(req.get("question_id", ""))
        else:
            question = ""
            qid = ""

        cat = qid_to_category.get(qid) or qid_to_category.get(question) or "unknown"
        if cat not in category_stats:
            category_stats[cat] = {"total": 0, "correct": 0}

        category_stats[cat]["total"] += 1

        arbiter = (
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
            or "skipped"
        ).lower()
        if arbiter in ("both_correct", "genie_correct"):
            category_stats[cat]["correct"] += 1

    return category_stats
