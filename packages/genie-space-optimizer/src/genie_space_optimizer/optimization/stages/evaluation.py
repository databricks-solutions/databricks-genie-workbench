"""Stage 1 + 8: Evaluation orchestration (Phase F1).

Owns the typed StageInput / StageOutput for the evaluation_state (Stage 1)
and post_patch_evaluation (Stage 8) entries of PROCESS_STAGE_ORDER. The
12k-LOC evaluation.py primitives stay where they are; this module is a
thin orchestrator that the harness calls into and that the Phase H
per-stage I/O capture decorator wraps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization import evaluation as _eval_primitives
from genie_space_optimizer.optimization.control_plane import (
    row_is_actionable_soft,
    row_is_hard_failure,
    row_is_passing,
)
from genie_space_optimizer.optimization.decision_emitters import (
    eval_classification_records,
)
from genie_space_optimizer.optimization.eval_entry import (
    _emit_eval_entry_journey,
)


STAGE_KEY: str = "evaluation_state"
POST_PATCH_STAGE_KEY: str = "post_patch_evaluation"


@dataclass
class EvaluationInput:
    """Input to evaluate_baseline / evaluate_post_patch.

    ``run_role`` distinguishes baseline / iteration_eval / strategy from
    the run_output_contract.RunRole enum. ``scope`` is "full" or
    "enrichment". ``iteration_label`` matches the existing harness
    helper ``_iteration_label(N)`` so journey-emit downstream is
    byte-stable.
    """

    space_state: dict[str, Any]
    eval_qids: tuple[str, ...]
    run_role: str
    iteration_label: str
    scope: str = "full"


@dataclass
class EvaluationResult:
    """Output of evaluate_baseline / evaluate_post_patch.

    Field set is the union of what today's harness locals expose to
    downstream stages. F2 / F3 / F8 read from this dataclass instead
    of from harness locals.
    """

    scoreboard: dict[str, Any]
    hard_failure_qids: tuple[str, ...]
    soft_signal_qids: tuple[str, ...]
    already_passing_qids: tuple[str, ...]
    gt_correction_candidate_qids: tuple[str, ...]
    eval_rows: tuple[dict[str, Any], ...]
    per_qid_judge: dict[str, Any] = field(default_factory=dict)
    asi_metadata: dict[str, Any] = field(default_factory=dict)
    eval_provenance: dict[str, Any] = field(default_factory=dict)


def _row_qid(row: dict[str, Any]) -> str:
    """Resolve a benchmark qid from a row using the canonical priority order."""
    return str(
        row.get("question_id")
        or row.get("inputs.question_id")
        or row.get("qid")
        or ""
    )


def _classify_eval_rows(
    rows: list[dict[str, Any]],
) -> tuple[set[str], set[str], set[str], set[str]]:
    """Partition rows into (already_passing, hard, soft, gt_correction)
    using the **production** control_plane predicates.

    Structurally similar to lever_loop_replay._classify_eval_rows but
    uses control_plane.row_is_* instead of replay-side arbiter-string
    parsing. Partition-parity with the replay-side helper is enforced
    by test_classify_eval_rows_agrees_with_lever_loop_replay_partition.

    gt_correction is determined by an additional arbiter check
    (genie_correct + result_correctness=yes) because
    control_plane.row_is_passing returns True for both already_passing
    and gt_correction rows.
    """
    already_passing: set[str] = set()
    hard: set[str] = set()
    soft: set[str] = set()
    gt_correction: set[str] = set()
    for row in rows or []:
        qid = _row_qid(row)
        if not qid:
            continue
        if row_is_hard_failure(row):
            hard.add(qid)
            continue
        rc = str(row.get("result_correctness") or "").lower()
        arb = str(
            row.get("arbiter") or row.get("feedback/arbiter/value") or ""
        ).lower()
        if rc == "yes" and arb == "genie_correct":
            gt_correction.add(qid)
            continue
        if row_is_passing(row):
            already_passing.add(qid)
            continue
        if row_is_actionable_soft(row):
            soft.add(qid)
            continue
        soft.add(qid)
    return already_passing, hard, soft, gt_correction


def _run_full_evaluation(
    inp: EvaluationInput, eval_kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Thin wrapper around evaluation.run_evaluation.

    Production callers (harness line 9924) pass a long argument list;
    F1 forwards it through ``eval_kwargs`` so the wrapper stays narrow.
    Phase G freezes ``eval_kwargs`` into a typed RunEvaluationKwargs
    dataclass.
    """
    return _eval_primitives.run_evaluation(**eval_kwargs)


def evaluate_baseline(
    ctx,
    inp: EvaluationInput,
    *,
    eval_kwargs: dict[str, Any],
) -> EvaluationResult:
    """Stage 1 entry. Currently a placeholder kept for future migration
    of harness.py:2013 (the once-per-run baseline call). F1 does NOT
    wire this from the harness today; it's exposed so a follow-up plan
    can migrate the baseline orchestrator without changing this stage's
    public contract."""
    return _evaluate(ctx, inp, eval_kwargs=eval_kwargs, run_role="baseline")


def evaluate_post_patch(
    ctx,
    inp: EvaluationInput,
    *,
    eval_kwargs: dict[str, Any],
) -> EvaluationResult:
    """Stage 8 entry. Wraps the per-iteration full eval call site at
    harness.py:9924. The harness still owns the post-eval logic that
    follows the eval call (full_scores extraction, baseline-drift,
    detect_regressions, decide_acceptance) — F1 does not absorb that;
    subsequent F-plans (F8 acceptance) will."""
    return _evaluate(
        ctx, inp,
        eval_kwargs=eval_kwargs,
        run_role=inp.run_role or "iteration_eval",
    )


def _evaluate(
    ctx,
    inp: EvaluationInput,
    *,
    eval_kwargs: dict[str, Any],
    run_role: str,
) -> EvaluationResult:
    raw = _run_full_evaluation(inp, eval_kwargs)
    rows = list(raw.get("rows") or [])
    already, hard, soft, gt = _classify_eval_rows(rows)

    _emit_eval_entry_journey(
        emit=ctx.journey_emit,
        eval_qids=tuple(inp.eval_qids),
        already_passing_qids=tuple(already),
        hard_qids=tuple(hard),
        soft_qids=tuple(soft),
        gt_correction_qids=tuple(gt),
    )

    classification: dict[str, str] = {}
    for qid in already:
        classification[qid] = "already_passing"
    for qid in hard:
        classification[qid] = "hard"
    for qid in soft:
        classification[qid] = "soft"
    for qid in gt:
        classification[qid] = "gt_correction"

    classified_qids = tuple(
        q for q in inp.eval_qids if str(q) in classification
    ) or tuple(sorted(classification.keys()))
    for record in eval_classification_records(
        run_id=ctx.run_id,
        iteration=ctx.iteration,
        eval_qids=classified_qids,
        classification=classification,
    ):
        ctx.decision_emit(record)

    scoreboard = {
        k: raw.get(k)
        for k in (
            "overall_accuracy", "pre_arbiter_accuracy", "scores",
            "both_correct_rate", "thresholds_passed",
        )
        if k in raw
    }

    return EvaluationResult(
        scoreboard=scoreboard,
        hard_failure_qids=tuple(sorted(hard)),
        soft_signal_qids=tuple(sorted(soft)),
        already_passing_qids=tuple(sorted(already)),
        gt_correction_candidate_qids=tuple(sorted(gt)),
        eval_rows=tuple(rows),
        per_qid_judge=dict(raw.get("per_qid_judge") or {}),
        asi_metadata=dict(raw.get("asi_metadata") or {}),
        eval_provenance={
            "run_id": str(raw.get("run_id") or ""),
            "experiment_id": str(raw.get("experiment_id") or ""),
            "model_id": str(raw.get("model_id") or ""),
        },
    )
