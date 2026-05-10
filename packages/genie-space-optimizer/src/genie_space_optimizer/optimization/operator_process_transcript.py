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
from genie_space_optimizer.optimization.stages._json_io import (
    JsonRoundTrip,
    pretty_block,
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
    trace: OptimizationTrace | None,
    iteration_summary: dict[str, Any],
    typed_stage_io: "dict[str, tuple[Any, Any, tuple[str, ...]]] | None" = None,
    # ^ optional. Maps stage_key → (input_obj, output_obj, markers_emitted).
    fixture_anchor: str | None = None,
) -> str:
    """Render a single iteration's transcript block.

    C15 Phase 5: ``typed_stage_io`` and ``fixture_anchor`` are new optional
    parameters.  When ``typed_stage_io`` is provided and a stage key appears
    in it, ``render_typed_stage_block`` appends the typed I/O block after the
    legacy decision-record summary.  ``trace`` may be ``None`` when only the
    typed path is used (e.g. snapshot tests).
    """
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
        if trace is not None:
            records = _records_for_stage(trace, stage.key)
            if records:
                for rec in records[:5]:
                    lines.append(f"- {_format_record(rec)}")
                if len(records) > 5:
                    lines.append(f"- (+{len(records) - 5} more records)")
            else:
                lines.append("- (no decisions emitted for this stage in this iteration)")
        else:
            lines.append("- (trace not available)")
        lines.append("")
        if typed_stage_io and stage.key in typed_stage_io:
            inp_obj, out_obj, markers = typed_stage_io[stage.key]
            lines.append(render_typed_stage_block(
                stage_index=stage_idx,
                stage_key=stage.key,
                inp=inp_obj,
                out=out_obj,
                markers_emitted=markers,
                fixture_anchor=fixture_anchor,
            ))
            lines.append("")

    return "\n".join(lines)


def render_typed_stage_block(
    *,
    stage_index: int,
    stage_key: str,
    inp: JsonRoundTrip,
    out: JsonRoundTrip,
    markers_emitted: tuple[str, ...] = (),
    fixture_anchor: str | None = None,
    width: int = 72,
) -> str:
    """C15 Phase 5: render one stage's I/O as a fixed-format block.

    Produces:

        [STAGE N: stage_key]                     fixture: <anchor>
        ─ Input ──────────────────────────────────────────────────
         <field> : <value>
         ...
        ─ Output ─────────────────────────────────────────────────
         <field> : <value>
         ...
        ─ Markers emitted ───────────────────────────────────────
         <marker_name>            ✓
    """
    lines: list[str] = []
    header_left = f"[STAGE {stage_index}: {stage_key}]"
    header_right = f"fixture: {fixture_anchor}" if fixture_anchor else ""
    pad = max(1, width - len(header_left) - len(header_right))
    lines.append(f"{header_left}{' ' * pad}{header_right}")
    lines.append(pretty_block("Input", inp.to_pretty(width=width), width=width))
    lines.append(pretty_block("Output", out.to_pretty(width=width), width=width))
    if markers_emitted:
        marker_body = "\n".join(f" {m:<28}    ✓" for m in markers_emitted)
        lines.append(pretty_block("Markers emitted", marker_body, width=width))
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
