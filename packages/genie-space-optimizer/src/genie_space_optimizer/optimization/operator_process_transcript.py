"""Process-first transcript renderer (Phase H).

Reads an OptimizationTrace and produces a markdown transcript whose
sections mirror PROCESS_STAGE_ORDER. Each iteration block has a fixed
schema:

  ## Iteration <N>
  ### Iteration Summary
  ### 1. Evaluation State
    - What happened
    - Why this stage exists
  ### 2. RCA Evidence
  ... (and so on for all 13 stages)
  ### 13. Contract Health

Schema reference: the predecessor plan
(2026-05-03-gso-run-output-contract-plan.md:497-820) has the full
template. This module implements it.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionRecord,
    DecisionType,
    OptimizationTrace,
)
from genie_space_optimizer.optimization.run_output_contract import (
    PROCESS_STAGE_ORDER,
)


_STAGE_DECISION_TYPE_MAP: dict[str, tuple[DecisionType, ...]] = {
    "evaluation_state":         (DecisionType.EVAL_CLASSIFIED,),
    "rca_evidence":             (DecisionType.RCA_FORMED,),
    "cluster_formation":        (DecisionType.CLUSTER_SELECTED, DecisionType.RCA_FORMED),
    "action_group_selection":   (DecisionType.STRATEGIST_AG_EMITTED,),
    "proposal_generation":      (DecisionType.PROPOSAL_GENERATED,),
    "safety_gates":             (DecisionType.GATE_DECISION,),
    "applied_patches":          (DecisionType.PATCH_APPLIED, DecisionType.PATCH_SKIPPED),
    "post_patch_evaluation":    (DecisionType.EVAL_CLASSIFIED,),
    "acceptance_decision":      (DecisionType.ACCEPTANCE_DECIDED,),
    "learning_next_action":     (
        DecisionType.AG_RETIRED,
        DecisionType.QID_RESOLUTION,
        # Phase H Fidelity Task 4: surface the per-iteration learning
        # / next-action record (proposals_empty, rolled_back, etc.) in
        # Stage 10 so the operator transcript always carries the
        # iteration outcome and operator-facing guidance.
        DecisionType.ITERATION_BUDGET_DECISION,
    ),
    # C15 Phase 1: bundle_assembly and run_manifest are new executable
    # stages (positions 11 and 12 in PROCESS_STAGE_ORDER). They do not
    # emit DecisionRecord events in the current implementation; the empty
    # tuple means the transcript renders the standard placeholder.
    "bundle_assembly":          (),
    "run_manifest":             (),
    # Phase H Fidelity Task 5: Stage 13 was permanently empty because
    # ``contract_health`` mapped to an empty tuple. Surface producer
    # exceptions and invariant violations here so the operator
    # transcript reports whether decision-record persistence and the
    # invariant suite are healthy enough to base a postmortem on.
    "contract_health":          (
        DecisionType.PRODUCER_EXCEPTION,
        DecisionType.INVARIANT_VIOLATION,
    ),
}


def render_run_overview(
    *,
    run_id: str,
    space_id: str,
    domain: str,
    max_iters: int,
    baseline: dict[str, Any],
    hard_failures: list[tuple[str, str, str]],
) -> str:
    """Render the once-per-run overview block at the top of the transcript."""
    lines: list[str] = []
    lines.append("=" * 80)
    lines.append("GSO LEVER LOOP RUN")
    lines.append("=" * 80)
    lines.append(f"Run ID:        {run_id}")
    lines.append(f"Space ID:      {space_id}")
    lines.append(f"Domain:        {domain}")
    lines.append(f"Max iters:     {max_iters}")
    lines.append("")
    lines.append("Baseline:")
    overall = baseline.get("overall_accuracy", 0.0)
    all_pass = baseline.get("all_judge_pass_rate", 0.0)
    lines.append(f"  Overall accuracy:        {overall * 100:.1f}%")
    lines.append(f"  All-judge pass:          {all_pass * 100:.1f}%")
    lines.append(f"  Hard failures:           {baseline.get('hard_failures', 0)}")
    lines.append(f"  Soft signals:            {baseline.get('soft_signals', 0)}")
    lines.append("")
    if hard_failures:
        lines.append("Hard failures:")
        for qid, root_cause, symptom in hard_failures:
            lines.append(f"  - {qid}  root={root_cause:<24} symptom={symptom}")
    lines.append("=" * 80)
    return "\n".join(lines)


def render_iteration_transcript(
    *,
    iteration: int,
    trace: OptimizationTrace,
    iteration_summary: dict[str, Any],
) -> str:
    """Render a single iteration's transcript block."""
    lines: list[str] = []
    lines.append(f"\n## Iteration {iteration}\n")

    lines.append("### Iteration Summary")
    if iteration_summary:
        for k, v in sorted(iteration_summary.items()):
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- (no summary metrics for this iteration)")
    lines.append("")

    for stage_idx, stage in enumerate(PROCESS_STAGE_ORDER, start=1):
        lines.append(f"### {stage_idx}. {stage.title}")
        lines.append("")
        lines.append(f"**Why this stage exists:** {stage.why}")
        lines.append("")
        lines.append("**What happened:**")
        records = _records_for_stage(trace, stage.key)
        if records:
            for rec in records[:5]:
                lines.append(f"- {_format_record(rec)}")
            if len(records) > 5:
                lines.append(f"- (+{len(records) - 5} more records)")
        else:
            lines.append("- (no decisions emitted for this stage in this iteration)")
        lines.append("")

    return "\n".join(lines)


def _records_for_stage(
    trace: OptimizationTrace, stage_key: str,
) -> list[DecisionRecord]:
    decision_types = _STAGE_DECISION_TYPE_MAP.get(stage_key, ())
    return [
        rec for rec in trace.decision_records
        if rec.decision_type in decision_types
    ]


def _format_record(rec: DecisionRecord) -> str:
    target_str = f" target={list(rec.target_qids)}" if rec.target_qids else ""
    reason_str = (
        f" reason={rec.reason_code.value}"
        if rec.reason_code and rec.reason_code.value != "none"
        else ""
    )
    # Phase H Fidelity Task 3: surface the detailed bucket breakdown
    # (e.g. target_qids_not_improved + target_fixed/regressed lists) and
    # any explicit regression qids so Stage 9 reads as the operator
    # source of truth instead of one collapsed line per outcome.
    regression_str = (
        f" regressions={list(rec.regression_qids)}" if rec.regression_qids else ""
    )
    detail_str = f" detail={rec.reason_detail}" if rec.reason_detail else ""
    return (
        f"{rec.decision_type.value} outcome={rec.outcome.value}"
        f"{target_str}{reason_str}{regression_str}{detail_str}"
    )


def render_full_transcript(
    *,
    run_overview: str,
    iteration_transcripts: list[str],
) -> str:
    """Concatenate the run overview + every iteration's transcript."""
    return run_overview + "\n\n" + "\n\n".join(iteration_transcripts)
