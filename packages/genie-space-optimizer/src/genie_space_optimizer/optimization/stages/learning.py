"""Stage 9: Learning / Next Action (Phase F9).

Wraps the existing primitives that govern end-of-iteration state:
  - reflection_buffer append (from harness inline pattern).
  - do-not-retry signature accumulation.
  - PR-E content-fingerprint blocklist accumulation.
  - rca_terminal.resolve_terminal_on_plateau invocation.
  - PR-B2 AG_RETIRED DecisionRecord emission.

F9 is observability-only: per the plan's Reality Check, the harness's
end-of-iteration learning logic is intertwined with break/continue
control flow and stdout banner emission. Lifting that under F9's
byte-stability gate is high-risk. F9 stands up the typed surface +
AG_RETIRED emission entry; harness wiring is deferred to a follow-up
plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionRecord,
    DecisionType,
    ReasonCode,
)
from genie_space_optimizer.optimization.rca_terminal import (
    RcaTerminalDecision,
    resolve_terminal_on_plateau,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


STAGE_KEY: str = "learning_next_action"


# ── C15 Phase 1: new typed contracts for LearningInput / LearningOutput ──────


class AcceptanceVerdict(StrEnum):
    """Canonical AG-level verdict, sourced from AgOutcomeRecord.outcome.

    String values are the legacy outcome strings the codebase already
    emits, so JSON round-trip is byte-stable across this rewrite.
    """

    ACCEPTED = "accepted"
    ACCEPTED_WITH_ATTRIBUTION_DRIFT = "accepted_with_attribution_drift"
    ACCEPTED_WITH_REGRESSION_DEBT = "accepted_with_regression_debt"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True, slots=True)
class IterationSummary(JsonRoundTrip):
    """One per attempted iteration. Closes D-7 — the totality marker
    has one summary per attempted iter, not one per accepted iter."""

    iteration: int
    attempted: bool
    verdict: AcceptanceVerdict
    candidate_accuracy: float
    baseline_accuracy: float
    accidentally_improved_qids: tuple[str, ...] = ()
    unresolved_target_debt_qids: tuple[str, ...] = ()
    rolled_back_content_fingerprints: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    @classmethod
    def from_json(cls, payload: dict) -> "IterationSummary":
        return cls(
            iteration=int(payload["iteration"]),
            attempted=bool(payload["attempted"]),
            verdict=AcceptanceVerdict(payload["verdict"]),
            candidate_accuracy=float(payload["candidate_accuracy"]),
            baseline_accuracy=float(payload["baseline_accuracy"]),
            accidentally_improved_qids=tuple(payload.get("accidentally_improved_qids") or ()),
            unresolved_target_debt_qids=tuple(payload.get("unresolved_target_debt_qids") or ()),
            rolled_back_content_fingerprints=tuple(payload.get("rolled_back_content_fingerprints") or ()),
            notes=tuple(payload.get("notes") or ()),
        )


@dataclass(frozen=True, slots=True)
class LearningInput(JsonRoundTrip):
    """C15 Phase 1 typed input to stages.learning.execute.

    Sourced directly from the AcceptanceOutput plus iteration-counter
    bookkeeping. The harness never reshapes the verdict; LearningInput
    accepts the AcceptanceVerdict enum or its string form (from_json).

    NOTE: The legacy LearningInput (below) is preserved for the existing
    update() function. This new contract is what INPUT_CLASS points to
    for the conformance test and fixture replay.
    """

    iteration: int
    attempted: bool
    verdict: AcceptanceVerdict
    candidate_accuracy: float
    baseline_accuracy: float
    accidentally_improved_qids: tuple[str, ...] = ()
    unresolved_target_debt_qids: tuple[str, ...] = ()
    rolled_back_content_fingerprints: tuple[str, ...] = ()
    qid_resolutions: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_json(cls, payload: dict) -> "LearningInput":
        return cls(
            iteration=int(payload["iteration"]),
            attempted=bool(payload["attempted"]),
            verdict=AcceptanceVerdict(payload["verdict"]),
            candidate_accuracy=float(payload["candidate_accuracy"]),
            baseline_accuracy=float(payload["baseline_accuracy"]),
            accidentally_improved_qids=tuple(payload.get("accidentally_improved_qids") or ()),
            unresolved_target_debt_qids=tuple(payload.get("unresolved_target_debt_qids") or ()),
            rolled_back_content_fingerprints=tuple(payload.get("rolled_back_content_fingerprints") or ()),
            qid_resolutions=dict(payload.get("qid_resolutions") or {}),
        )


@dataclass(frozen=True, slots=True)
class LearningOutput(JsonRoundTrip):
    """C15 Phase 1 typed output of stages.learning.execute. Drives:

    * D-7: GSO_ITERATION_SUMMARY_TOTALITY_V1 emits len(iteration_summaries)
      records, exactly one per attempted iter.
    * Stage 10 → Stage 4 arrow: forbidden-AG set is rebuilt from
      iteration_summaries[*].rolled_back_content_fingerprints.
    """

    iteration_summaries: tuple[IterationSummary, ...] = ()
    terminate: bool = False
    terminate_reason: str = ""

    @classmethod
    def from_json(cls, payload: dict) -> "LearningOutput":
        return cls(
            iteration_summaries=tuple(
                IterationSummary.from_json(s)
                for s in (payload.get("iteration_summaries") or ())
            ),
            terminate=bool(payload.get("terminate", False)),
            terminate_reason=str(payload.get("terminate_reason", "")),
        )


@dataclass
class _LearningInputLegacy:
    """Legacy learning input used by update(). Preserved for harness wiring
    that has not yet migrated to the C15 Phase 1 typed LearningInput.
    Not exported as INPUT_CLASS — the new LearningInput (above) is."""

    prior_reflection_buffer: tuple[dict[str, Any], ...]
    prior_do_not_retry: set[str]
    prior_rolled_back_content_fingerprints: set[str]
    ag_outcomes_by_id: dict[str, dict[str, Any]]
    applied_signature: str
    accuracy_delta: float
    current_hard_failure_qids: tuple[str, ...]
    regression_debt_qids: set[str] = field(default_factory=set)
    quarantined_qids: set[str] = field(default_factory=set)
    sql_delta_qids: set[str] = field(default_factory=set)
    pending_buffered_ags: tuple[dict[str, Any], ...] = ()
    diagnostic_action_queue: tuple[dict[str, Any], ...] = ()


@dataclass
class LearningUpdate:
    new_reflection_buffer: tuple[dict[str, Any], ...]
    new_do_not_retry: set[str]
    new_rolled_back_content_fingerprints: set[str]
    terminal_decision: dict[str, Any]
    retired_ags: tuple[tuple[str, tuple[str, ...]], ...] = ()
    ag_retired_records: tuple[DecisionRecord, ...] = ()


def _append_reflection_buffer_entry(
    *,
    prior: tuple[dict[str, Any], ...],
    iteration: int,
    ag_outcomes_by_id: dict[str, dict[str, Any]],
    accuracy_delta: float,
    applied_signature: str,
) -> tuple[dict[str, Any], ...]:
    """Append one summary entry to reflection_buffer."""
    accepted = any(
        rec.get("outcome") in {"accepted", "accepted_with_regression_debt"}
        for rec in ag_outcomes_by_id.values()
    )
    rollback_class = next(
        (rec.get("rollback_class") for rec in ag_outcomes_by_id.values()
         if rec.get("rollback_class")),
        None,
    )
    entry: dict[str, Any] = {
        "iter": int(iteration),
        "accepted": accepted,
        "rollback_class": rollback_class,
        "applied_signature": applied_signature,
        "accuracy_delta": float(accuracy_delta),
    }
    return prior + (entry,)


def _accumulate_rolled_back_fingerprints(
    *,
    prior: set[str],
    ag_outcomes_by_id: dict[str, dict[str, Any]],
) -> set[str]:
    """PR-E groundwork: union prior fingerprints with newly rolled-back ones."""
    new_set = set(prior)
    for rec in ag_outcomes_by_id.values():
        if rec.get("outcome") != "rolled_back":
            continue
        fp_value = rec.get("content_fingerprint")
        if isinstance(fp_value, (list, tuple, set, frozenset)):
            for fp in fp_value:
                if str(fp).strip():
                    new_set.add(str(fp))
        elif fp_value:
            for fp in str(fp_value).split(";"):
                if fp.strip():
                    new_set.add(fp)
    return new_set


def _emit_ag_retired_records(
    *,
    ctx,
    retired_ags: tuple[tuple[str, tuple[str, ...]], ...],
) -> tuple[DecisionRecord, ...]:
    """PR-B2: emit one AG_RETIRED DecisionRecord per retired AG."""
    records: list[DecisionRecord] = []
    for retired_ag_id, retired_qids in retired_ags:
        rec = DecisionRecord(
            run_id=str(ctx.run_id),
            iteration=int(ctx.iteration),
            decision_type=DecisionType.AG_RETIRED,
            outcome=DecisionOutcome.RETIRED,
            reason_code=ReasonCode.AG_TARGET_NO_LONGER_HARD,
            ag_id=str(retired_ag_id),
            target_qids=tuple(retired_qids),
            affected_qids=tuple(retired_qids),
            reason_detail=(
                f"AG {retired_ag_id} retired at plateau because "
                f"target qids {list(retired_qids)} are no longer "
                f"in the live hard-failure set."
            ),
        )
        ctx.decision_emit(rec)
        records.append(rec)
    return tuple(records)


def update(ctx, inp: _LearningInputLegacy) -> LearningUpdate:
    """Stage 9 entry. Builds the next-iteration learning state and
    emits AG_RETIRED records when applicable.

    F9 is observability-only — does NOT modify any harness call site.
    Harness still owns the inline reflection-buffer / plateau /
    AG_RETIRED block; this stage exposes a parallel typed surface
    that Phase G/H will adopt.
    """
    new_reflection_buffer = _append_reflection_buffer_entry(
        prior=inp.prior_reflection_buffer,
        iteration=ctx.iteration,
        ag_outcomes_by_id=inp.ag_outcomes_by_id,
        accuracy_delta=inp.accuracy_delta,
        applied_signature=inp.applied_signature,
    )

    new_do_not_retry = set(inp.prior_do_not_retry)
    for rec in inp.ag_outcomes_by_id.values():
        if rec.get("outcome") == "rolled_back":
            sig = (
                rec.get("apply_signature")
                or rec.get("applied_signature")
                or ""
            )
            if sig:
                new_do_not_retry.add(str(sig))

    new_rolled_back_fps = _accumulate_rolled_back_fingerprints(
        prior=inp.prior_rolled_back_content_fingerprints,
        ag_outcomes_by_id=inp.ag_outcomes_by_id,
    )

    pending_diag = list(inp.pending_buffered_ags) + list(inp.diagnostic_action_queue)
    decision: RcaTerminalDecision = resolve_terminal_on_plateau(
        quarantined_qids=set(inp.quarantined_qids),
        current_hard_qids=set(inp.current_hard_failure_qids),
        regression_debt_qids=set(inp.regression_debt_qids),
        sql_delta_qids=set(inp.sql_delta_qids),
        pending_diagnostic_ags=pending_diag,
    )
    terminal_decision = {
        "status": decision.status.value,
        "should_continue": decision.should_continue,
        "reason": decision.reason,
    }

    ag_retired_records: tuple[DecisionRecord, ...] = ()
    if decision.retired_ags:
        ag_retired_records = _emit_ag_retired_records(
            ctx=ctx,
            retired_ags=tuple(decision.retired_ags),
        )

    return LearningUpdate(
        new_reflection_buffer=new_reflection_buffer,
        new_do_not_retry=new_do_not_retry,
        new_rolled_back_content_fingerprints=new_rolled_back_fps,
        terminal_decision=terminal_decision,
        retired_ags=tuple(decision.retired_ags),
        ag_retired_records=ag_retired_records,
    )


def execute(ctx, inp: LearningInput) -> LearningOutput:
    """Stage 9 entry for the C15 Phase 1 typed contract.

    Builds one IterationSummary for this attempted iteration and
    returns it wrapped in a LearningOutput. The ``terminate`` flag
    and ``terminate_reason`` are computed by the harness caller
    (which still owns plateau resolution) and passed through inp.
    """
    summary = IterationSummary(
        iteration=inp.iteration,
        attempted=inp.attempted,
        verdict=inp.verdict,
        candidate_accuracy=inp.candidate_accuracy,
        baseline_accuracy=inp.baseline_accuracy,
        accidentally_improved_qids=inp.accidentally_improved_qids,
        unresolved_target_debt_qids=inp.unresolved_target_debt_qids,
        rolled_back_content_fingerprints=inp.rolled_back_content_fingerprints,
    )
    return LearningOutput(
        iteration_summaries=(summary,),
        terminate=False,
        terminate_reason="",
    )


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
# C15 Phase 1: INPUT_CLASS → new frozen LearningInput (JsonRoundTrip);
# OUTPUT_CLASS → new frozen LearningOutput (JsonRoundTrip).
# The legacy _LearningInputLegacy / LearningUpdate remain for the
# existing update() function body.
INPUT_CLASS = LearningInput
OUTPUT_CLASS = LearningOutput
