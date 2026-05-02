"""Typed contract for the question-journey event graph.

This module is the source of truth for which stages a Lever Loop iteration may
emit, which terminal states a question may end an iteration in, and which
stage-to-stage transitions are legal. The validator and canonical serializer
also live here.

Producers (mostly harness.py) keep using QuestionJourneyEvent from
question_journey.py; consumers use this module to validate that every
evaluated qid has a complete, legal journey.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


class JourneyStage(str, Enum):
    """Every legal stage a question may pass through in one Lever Loop iteration."""

    EVALUATED = "evaluated"
    CLUSTERED = "clustered"
    SOFT_SIGNAL = "soft_signal"
    GT_CORRECTION_CANDIDATE = "gt_correction_candidate"
    INTENT_COLLISION_DETECTED = "intent_collision_detected"
    ALREADY_PASSING = "already_passing"
    DIAGNOSTIC_AG = "diagnostic_ag"
    AG_ASSIGNED = "ag_assigned"
    PROPOSED = "proposed"
    DROPPED_AT_GROUNDING = "dropped_at_grounding"
    DROPPED_AT_NORMALIZE = "dropped_at_normalize"
    DROPPED_AT_APPLYABILITY = "dropped_at_applyability"
    DROPPED_AT_ALIGNMENT = "dropped_at_alignment"
    DROPPED_AT_REFLECTION = "dropped_at_reflection"
    DROPPED_AT_CAP = "dropped_at_cap"
    APPLIED = "applied"
    # Track 3/E (Phase A burn-down) — apply emit splits into:
    #   APPLIED_TARGETED        — qid in patch.target_qids
    #   APPLIED_BROAD_AG_SCOPE  — qid in AG.affected_questions \ patch.target_qids
    APPLIED_TARGETED = "applied_targeted"
    APPLIED_BROAD_AG_SCOPE = "applied_broad_ag_scope"
    ROLLED_BACK = "rolled_back"
    ACCEPTED = "accepted"
    ACCEPTED_WITH_REGRESSION_DEBT = "accepted_with_regression_debt"
    POST_EVAL = "post_eval"


class JourneyTerminalState(str, Enum):
    """Every legal end-of-iteration outcome for a question."""

    ALREADY_PASSING = "already_passing"
    HARD_FAILURE_RESOLVED = "hard_failure_resolved"
    HARD_FAILURE_UNRESOLVED = "hard_failure_unresolved"
    SOFT_SIGNAL_ONLY = "soft_signal_only"
    GT_CORRECTION_CANDIDATE = "gt_correction_candidate"
    TERMINAL_UNACTIONABLE = "terminal_unactionable"
    ROLLED_BACK_NO_PROGRESS = "rolled_back_no_progress"


_DROP_STAGES: frozenset[JourneyStage] = frozenset({
    JourneyStage.DROPPED_AT_GROUNDING,
    JourneyStage.DROPPED_AT_NORMALIZE,
    JourneyStage.DROPPED_AT_APPLYABILITY,
    JourneyStage.DROPPED_AT_ALIGNMENT,
    JourneyStage.DROPPED_AT_REFLECTION,
    JourneyStage.DROPPED_AT_CAP,
})


_LEGAL_NEXT: dict[JourneyStage, frozenset[JourneyStage]] = {
    JourneyStage.EVALUATED: frozenset({
        JourneyStage.CLUSTERED,
        JourneyStage.SOFT_SIGNAL,
        JourneyStage.GT_CORRECTION_CANDIDATE,
        JourneyStage.ALREADY_PASSING,
    }),
    JourneyStage.CLUSTERED: frozenset({
        JourneyStage.AG_ASSIGNED,
        JourneyStage.DIAGNOSTIC_AG,
        JourneyStage.INTENT_COLLISION_DETECTED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.SOFT_SIGNAL: frozenset({
        JourneyStage.AG_ASSIGNED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.GT_CORRECTION_CANDIDATE: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.INTENT_COLLISION_DETECTED: frozenset({
        JourneyStage.AG_ASSIGNED,
        JourneyStage.DIAGNOSTIC_AG,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.ALREADY_PASSING: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DIAGNOSTIC_AG: frozenset({
        JourneyStage.PROPOSED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.AG_ASSIGNED: frozenset({
        JourneyStage.PROPOSED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.PROPOSED: frozenset(_DROP_STAGES | {JourneyStage.APPLIED}),
    JourneyStage.DROPPED_AT_GROUNDING: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_NORMALIZE: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_APPLYABILITY: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_ALIGNMENT: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_REFLECTION: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_CAP: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.APPLIED: frozenset({
        JourneyStage.ACCEPTED,
        JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT,
        JourneyStage.ROLLED_BACK,
    }),
    JourneyStage.ACCEPTED: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.ROLLED_BACK: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.POST_EVAL: frozenset(),
}


def is_legal_next_stage(*, prev: JourneyStage, nxt: JourneyStage) -> bool:
    """Return True if a question may transition from prev to nxt in one iteration."""
    return nxt in _LEGAL_NEXT.get(prev, frozenset())


@dataclass(frozen=True)
class JourneyContractViolation:
    """One reportable defect found by validate_question_journeys."""

    question_id: str
    kind: str  # "missing_qid" | "unknown_stage" | "illegal_transition" | "no_terminal_state"
    detail: str = ""


@dataclass(frozen=True)
class JourneyValidationReport:
    is_valid: bool
    missing_qids: tuple[str, ...]
    violations: list[JourneyContractViolation]
    terminal_state_by_qid: dict[str, JourneyTerminalState]

    def to_dict(self) -> dict:
        """Return a JSON-safe, stable dict representation.

        Used by the lever-loop harness to persist per-iteration validation
        results to the replay fixture and MLflow without leaking dataclass
        identities or enum reprs. Output is deterministic under
        ``json.dumps(sort_keys=True)``, suitable for byte-stable artifact diffs.
        """
        return {
            "is_valid": bool(self.is_valid),
            "missing_qids": list(self.missing_qids),
            "violations": [
                {
                    "question_id": v.question_id,
                    "kind": v.kind,
                    "detail": v.detail,
                }
                for v in self.violations
            ],
            "terminal_state_by_qid": {
                qid: state.value
                for qid, state in self.terminal_state_by_qid.items()
            },
        }


def _classify_terminal_state(
    *,
    events: list[QuestionJourneyEvent],
) -> JourneyTerminalState:
    """Reduce a qid's event list to one terminal-state classification.

    Order of resolution matches the expected information flow:
      1. already_passing → ALREADY_PASSING
      2. gt_correction_candidate → GT_CORRECTION_CANDIDATE
      3. accepted/accepted_with_regression_debt + post_eval is_passing → HARD_FAILURE_RESOLVED
      4. rolled_back → ROLLED_BACK_NO_PROGRESS
      5. soft_signal only (no clustered) → SOFT_SIGNAL_ONLY
      6. clustered but no ag_assigned/diagnostic_ag → TERMINAL_UNACTIONABLE
      7. otherwise → HARD_FAILURE_UNRESOLVED
    """
    stages = {ev.stage for ev in events}
    if "already_passing" in stages:
        return JourneyTerminalState.ALREADY_PASSING
    if "gt_correction_candidate" in stages:
        return JourneyTerminalState.GT_CORRECTION_CANDIDATE
    is_passing_after = any(
        ev.stage == "post_eval" and ev.is_passing is True for ev in events
    )
    if (
        ("accepted" in stages or "accepted_with_regression_debt" in stages)
        and is_passing_after
    ):
        return JourneyTerminalState.HARD_FAILURE_RESOLVED
    if "rolled_back" in stages:
        return JourneyTerminalState.ROLLED_BACK_NO_PROGRESS
    if "soft_signal" in stages and "clustered" not in stages:
        return JourneyTerminalState.SOFT_SIGNAL_ONLY
    if (
        "clustered" in stages
        and "ag_assigned" not in stages
        and "diagnostic_ag" not in stages
    ):
        return JourneyTerminalState.TERMINAL_UNACTIONABLE
    return JourneyTerminalState.HARD_FAILURE_UNRESOLVED


def _ordered_stages_for_qid(events: list[QuestionJourneyEvent]) -> list[str]:
    """Return a qid's events in the canonical order used by the renderer.

    Mirrors question_journey._STAGE_ORDER ordering with proposal_id as tiebreak.
    """
    from genie_space_optimizer.optimization.question_journey import _stage_rank

    return [
        ev.stage
        for ev in sorted(events, key=lambda e: (_stage_rank(e.stage), e.proposal_id))
    ]


def validate_question_journeys(
    *,
    events: list[QuestionJourneyEvent],
    eval_qids: Iterable[str],
) -> JourneyValidationReport:
    """Assert every evaluated qid has a complete, legal journey.

    A journey is *complete* when:
      - the qid appears in at least one event,
      - every adjacent pair of stages is in the legal-transition map, and
      - the event list resolves to a JourneyTerminalState.
    """
    legal_stages = {s.value for s in JourneyStage}
    eval_qid_set = {str(q) for q in eval_qids if q}

    # Group events per-qid, preserving canonical ordering.
    by_qid: dict[str, list[QuestionJourneyEvent]] = {}
    for ev in events:
        if ev.question_id:
            by_qid.setdefault(ev.question_id, []).append(ev)

    violations: list[JourneyContractViolation] = []
    terminal_state_by_qid: dict[str, JourneyTerminalState] = {}

    missing_qids = tuple(sorted(eval_qid_set - by_qid.keys()))
    for missing in missing_qids:
        violations.append(
            JourneyContractViolation(
                question_id=missing,
                kind="missing_qid",
                detail="qid in eval set has no journey events",
            )
        )

    for qid, qevents in by_qid.items():
        ordered = _ordered_stages_for_qid(qevents)

        # 1. Unknown-stage check.
        for s in ordered:
            if s not in legal_stages:
                violations.append(
                    JourneyContractViolation(
                        question_id=qid,
                        kind="unknown_stage",
                        detail=f"stage={s!r} not in JourneyStage enum",
                    )
                )

        # 2. Adjacent-transition check.
        for prev_s, next_s in zip(ordered, ordered[1:]):
            if prev_s not in legal_stages or next_s not in legal_stages:
                continue  # already reported as unknown_stage
            if not is_legal_next_stage(
                prev=JourneyStage(prev_s),
                nxt=JourneyStage(next_s),
            ):
                violations.append(
                    JourneyContractViolation(
                        question_id=qid,
                        kind="illegal_transition",
                        detail=f"{prev_s} -> {next_s}",
                    )
                )

        # 3. Terminal-state check: classification must succeed AND post_eval must
        # exist for any qid that entered eval.
        if qid in eval_qid_set and "post_eval" not in {ev.stage for ev in qevents}:
            violations.append(
                JourneyContractViolation(
                    question_id=qid,
                    kind="no_terminal_state",
                    detail="qid has no post_eval event",
                )
            )
        else:
            terminal_state_by_qid[qid] = _classify_terminal_state(events=qevents)

    return JourneyValidationReport(
        is_valid=not violations and not missing_qids,
        missing_qids=missing_qids,
        violations=violations,
        terminal_state_by_qid=terminal_state_by_qid,
    )


import json


_CANONICAL_FIELDS: tuple[str, ...] = (
    "question_id",
    "stage",
    "cluster_id",
    "ag_id",
    "proposal_id",
    "patch_type",
    "root_cause",
    "reason",
    "transition",
    "was_passing",
    "is_passing",
)


def canonical_journey_json(*, events: list[QuestionJourneyEvent]) -> str:
    """Return a byte-stable JSON serialization of a journey event list.

    Volatile fields (the ``extra`` dict, any timestamps or durations a producer
    chose to attach) are stripped. Events are sorted by
    (question_id, stage_rank, proposal_id) so insertion order is irrelevant.
    The output is sorted-key JSON with no whitespace, suitable for byte-equal
    fixture diffs.
    """
    from genie_space_optimizer.optimization.question_journey import _stage_rank

    rows: list[dict] = []
    for ev in events:
        row: dict = {}
        for name in _CANONICAL_FIELDS:
            val = getattr(ev, name, None)
            if val in (None, "", False):
                # Skip falsy values to avoid presence-vs-absence noise across
                # producers; keep True booleans because they carry signal.
                if val is False:
                    row[name] = False
                continue
            row[name] = val
        rows.append(row)
    rows.sort(key=lambda r: (
        r.get("question_id", ""),
        _stage_rank(r.get("stage", "")),
        r.get("proposal_id", ""),
    ))
    return json.dumps(rows, sort_keys=True, separators=(",", ":"))


class JourneyContractViolationError(RuntimeError):
    """Raised when a Lever Loop iteration produces journey contract violations.

    Carries the validation report so callers can log specifics. Raised at end
    of iteration when raise_on_violation=True (Phase 4 hard gate).
    """

    def __init__(self, report: JourneyValidationReport) -> None:
        self.report = report
        summary = (
            f"{len(report.violations)} journey contract violations across "
            f"{len(set(v.question_id for v in report.violations))} qid(s); "
            f"missing_qids={list(report.missing_qids)}"
        )
        super().__init__(summary)
