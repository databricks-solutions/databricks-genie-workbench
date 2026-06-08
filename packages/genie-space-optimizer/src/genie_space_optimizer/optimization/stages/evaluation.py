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
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip
from genie_space_optimizer.optimization.stages._run_evaluation_kwargs import (
    RunEvaluationKwargs,
)


STAGE_KEY: str = "evaluation_state"
POST_PATCH_STAGE_KEY: str = "post_patch_evaluation"


def _canonical_qid_for_match(qid: str) -> str:
    """Namespace-insensitive canonical form for slice matching.

    Reuses the shared ``canonical_eval_row._split_namespaced_qid``
    canonicaliser (NOT a hand-rolled extractor) so the slice agrees with
    the rest of the codebase on what a benchmark's canonical qid is. A
    namespaced qid like ``airline_..._gs_024`` collapses to ``gs_024``;
    a bare canonical qid is returned unchanged.
    """
    from genie_space_optimizer.optimization.canonical_eval_row import (
        _split_namespaced_qid,
    )

    canonical, _namespaced = _split_namespaced_qid({"question_id": str(qid)})
    return canonical or str(qid)


def slice_benchmarks_to_eval_qids(
    benchmarks: list[dict] | None,
    eval_qids: "list[str] | tuple[str, ...] | None",
) -> list[dict]:
    """Filter ``benchmarks`` to the rows matching ``eval_qids``.

    Trial 30 W30.4(c) — matches namespace-insensitively. The pre-W30.4(c)
    slice compared ``extract_question_id(b)[0]`` (which returns the
    NAMESPACED qid) against the raw requested set, so a
    namespaced-vs-canonical mismatch produced
    ``benchmarks_count=0`` even when the benchmark row was present
    (the ``POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS`` regression on
    ``gs_024``). Here both sides are canonicalised via
    :func:`_canonical_qid_for_match`; an exact-match short-circuit keeps
    the canonical-only path byte-identical to the prior behaviour.

    Empty / missing ``eval_qids`` is the no-scope path (baseline
    once-per-run call) — the full list passes through unchanged.
    """
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    requested = {str(q) for q in (eval_qids or ()) if q}
    if not requested:
        return list(benchmarks or [])
    requested_canon = {_canonical_qid_for_match(q) for q in requested}
    out: list[dict] = []
    for b in benchmarks or []:
        bqid = extract_question_id(dict(b))[0]
        if bqid in requested or _canonical_qid_for_match(bqid) in requested_canon:
            out.append(b)
    return out


@dataclass
class EvaluationInput(JsonRoundTrip):
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
    # Trial 31 W31.4 — whether a missing benchmark row for a requested
    # qid is a genuine ``OPTIMIZER_INVARIANT_VIOLATION``. Default ``True``
    # preserves the Trial 16.1 fail-fast for hard QIDs the iteration is
    # repairing. The SM evaluated-gate sets this ``False`` when the
    # requested qid is NOT a live-hard repair target (already-correct /
    # no-benchmark), so an empty slice for it is a benign skip rather
    # than a violation that W31.3 would fail the whole task on.
    enforce_benchmark_presence: bool = True


@dataclass
class EvaluationResult(JsonRoundTrip):
    """Output of evaluate_baseline / evaluate_post_patch.

    Field set is the union of what today's harness locals expose to
    downstream stages. F2 / F3 / F8 read from this dataclass instead
    of from harness locals.

    ``raw`` carries the full ``evaluation.run_evaluation`` return dict
    unchanged. This is a deliberate backward-compat escape hatch for
    F1's wire-up: the harness's ~250 lines of post-eval logic
    (full_result_1.get('asi_extraction_audit'), .get('scores'),
    .get('quarantined_benchmarks_qids'), etc.) read fields the typed
    surface doesn't yet expose. Subsequent F-plans absorb that logic
    into their own stages and shrink ``raw`` toward removal in Phase
    G.
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
    raw: dict[str, Any] = field(default_factory=dict)


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


class PostApplyEvalEmptySliceError(RuntimeError):
    """Raised when ``_run_full_evaluation`` is asked to scope the
    benchmark slice to a non-empty ``eval_qids`` set but no benchmark
    row carries any of the requested qids.

    The harness loader (UC ``genie_benchmarks_<domain>`` table) or the
    upstream eval-row → hard-qid promotion has produced a qid that the
    benchmark list cannot satisfy. Running ``run_evaluation`` with an
    empty benchmarks list is wasteful and produces the legacy
    ``no_post_apply_row_for_qid`` shape downstream — surfacing this as
    a typed exception lets ``evaluated_gate`` produce a structured
    terminal whose ``forbidden_signature`` teaches the next iteration's
    strategist.
    """


def _run_full_evaluation(
    inp: EvaluationInput, eval_kwargs: RunEvaluationKwargs,
) -> dict[str, Any]:
    """Thin wrapper around evaluation.run_evaluation.

    Production callers (harness line 9924) pass a long argument list;
    F1 forwards it through ``eval_kwargs`` so the wrapper stays narrow.
    Phase G-lite typed ``eval_kwargs`` as ``RunEvaluationKwargs``
    (TypedDict, total=False) — required keys are enforced by
    ``run_evaluation``'s own signature at runtime.

    Trial 16 RC1 — when ``inp.eval_qids`` is non-empty, scope the
    benchmark slice to those qids before splatting. Up to Trial 15 this
    helper ignored ``eval_qids`` and forwarded the full benchmark list
    (production postmortems 575892594490176 and 319530250904653
    timed out at ~120 min because every applied patch triggered a full
    23-question post-apply evaluation). The downstream EvaluationInput
    contract already names this parameter, gates already populate it
    from ``ctx.eval_qids or (state.qid,)`` — we just stopped honouring
    it at the call boundary.

    Empty / missing ``eval_qids`` is the no-scope path (used by the
    baseline once-per-run call); the full list goes through unchanged.
    """
    requested = {str(q) for q in (inp.eval_qids or ()) if q}
    if requested:
        # Trial 16.1 — use the canonical extractor instead of the strict
        # ``b.get("question_id")`` lookup. Production postmortems
        # 127751814861356 and 813949510175466 both showed the slice
        # firing with ``benchmarks_count=0`` even when the harness had
        # loaded 23 benchmark rows — because the rows carried the qid
        # under nested ``inputs.question_id`` (MLflow ``genai.datasets``
        # shape), flat-slash ``inputs/question_id``, ``id`` only, or
        # ``request.kwargs.question_id``. Trial 16 RC2 already taught
        # the OUTPUT row-match path in ``evaluated_gate`` to use
        # ``extract_question_id``; the slice INPUT filter must use the
        # same canonical extractor for the contract to hold end-to-end.
        #
        # Trial 30 W30.4(c) — the INPUT filter additionally matches
        # namespace-insensitively (``slice_benchmarks_to_eval_qids``):
        # ``extract_question_id`` returns the NAMESPACED qid, so when
        # ``eval_qids`` carried the canonical ``gs_024`` (or vice versa)
        # the slice went empty (``POST_APPLY_EVAL_SLICED_ZERO_BENCHMARKS``
        # on ``gs_024``). The helper canonicalises both sides while
        # keeping an exact-match short-circuit for byte stability.
        original_benchmarks = list(eval_kwargs.get("benchmarks") or [])
        benchmarks = slice_benchmarks_to_eval_qids(
            original_benchmarks, inp.eval_qids,
        )
        # Copy to avoid mutating the caller's dict; the harness reuses
        # the same kwargs across baseline + post-apply calls.
        eval_kwargs = {**eval_kwargs, "benchmarks": benchmarks}  # type: ignore[assignment]
        # Postmortem marker — operators grep for this to confirm RC1
        # is in effect after Trial 16 lands.
        print(
            "GSO_POST_APPLY_EVAL_SLICED_V1 "
            f"requested_qids={sorted(requested)!r} "
            f"benchmarks_count={len(benchmarks)}",
            flush=True,
        )
        # Trial 16.1 — defense-in-depth invariant. Both postmortems
        # 127751814861356 and 813949510175466 recommended: "Make
        # hard-QID benchmark presence a pre-apply invariant. Refuse to
        # evaluate when the requested hard QID cannot map to exactly
        # one benchmark row." Raising a typed error here lets
        # ``evaluated_gate`` surface a structured ``OPTIMIZER_INVARIANT_
        # VIOLATION`` terminal with a ``forbidden_signature`` that
        # teaches the next iteration's strategist, rather than silently
        # running an empty eval that produces the legacy
        # ``no_post_apply_row_for_qid`` symptom.
        #
        # Only fires when the harness DID load benchmarks but the
        # requested qids are absent (the production failure shape).
        # If the harness loaded zero benchmarks the invariant is a
        # different class (loader/budget) and is not this site's job.
        if original_benchmarks and not benchmarks:
            # Trial 31 W31.4 — the empty-slice invariant only fires for a
            # benchmark-expected (live-hard) requested qid. When the
            # caller has declared this slice's qids are NOT repair
            # targets (already-correct / no-benchmark), an empty slice is
            # benign: skip the eval (returning the empty-rows shape that
            # ``_evaluate`` handles) instead of raising the violation
            # that W31.3 would fail the whole lever_loop task on. Flag-
            # gated (default ON); when OFF or when benchmark presence is
            # enforced (the default), the legacy fail-fast is preserved
            # byte-for-byte.
            from genie_space_optimizer.optimization.trial31_flags import (
                trial31_empty_slice_excludes_correct_enabled,
            )
            if (
                trial31_empty_slice_excludes_correct_enabled()
                and not inp.enforce_benchmark_presence
            ):
                print(
                    "GSO_TRIAL31_EMPTY_SLICE_BENIGN_SKIP_V1 "
                    f"requested_qids={sorted(requested)!r} "
                    "reason=not_benchmark_expected",
                    flush=True,
                )
                return {"rows": []}
            raise PostApplyEvalEmptySliceError(
                "post_apply_eval_empty_slice_for_requested_qid:"
                f"{sorted(requested)!r}"
            )
    # type: ignore[arg-type] — RunEvaluationKwargs is total=False; required
    # keys are enforced by run_evaluation's own signature at runtime.
    return _eval_primitives.run_evaluation(**eval_kwargs)


def evaluate_baseline(
    ctx,
    inp: EvaluationInput,
    *,
    eval_kwargs: RunEvaluationKwargs,
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
    eval_kwargs: RunEvaluationKwargs,
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
    eval_kwargs: RunEvaluationKwargs,
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
        raw=raw,
    )


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
INPUT_CLASS = EvaluationInput
OUTPUT_CLASS = EvaluationResult


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = evaluate_post_patch
