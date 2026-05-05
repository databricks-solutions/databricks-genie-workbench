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
        JourneyStage.ACCEPTED,
        JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT,
        JourneyStage.ROLLED_BACK,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.AG_ASSIGNED: frozenset({
        JourneyStage.PROPOSED,
        JourneyStage.ACCEPTED,
        JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT,
        JourneyStage.ROLLED_BACK,
        JourneyStage.POST_EVAL,
    }),
    # Plan N1 Task 5 — extend PROPOSED to legally transition to the
    # Track 3/E split stages. APPLIED_TARGETED and APPLIED_BROAD_AG_SCOPE
    # were introduced as splits of APPLIED but never wired into
    # _LEGAL_NEXT as either successors of PROPOSED or predecessors of
    # ACCEPTED/ROLLED_BACK. The new edges are the strict semantic
    # equivalent of the legacy APPLIED edge.
    JourneyStage.PROPOSED: frozenset(
        _DROP_STAGES | {
            JourneyStage.APPLIED,
            JourneyStage.APPLIED_TARGETED,
            JourneyStage.APPLIED_BROAD_AG_SCOPE,
        }
    ),
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
    JourneyStage.APPLIED_TARGETED: frozenset({
        JourneyStage.ACCEPTED,
        JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT,
        JourneyStage.ROLLED_BACK,
    }),
    JourneyStage.APPLIED_BROAD_AG_SCOPE: frozenset({
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
      6. clustered + diagnostic_ag + rca_exhausted → HARD_FAILURE_UNRESOLVED  (Cycle 6 F-7)
      7. clustered, no ag_assigned, no diagnostic_ag → TERMINAL_UNACTIONABLE
      8. otherwise → HARD_FAILURE_UNRESOLVED
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
    # Cycle 6 F-7 — diagnostic_ag + rca_exhausted means we tried and
    # exhausted the regen ladder, not "never tried". Distinguish from
    # the case-7 fall-through below so gs_021 (and similar T3-regen-
    # exhausted hard qids) classify correctly. Run 833969815458299
    # misclassified gs_021 as TERMINAL_UNACTIONABLE because the T3
    # path emitted decision records but no diagnostic_ag trunk event;
    # F-7 emits the trunk event and lets the classifier consume it.
    if (
        "clustered" in stages
        and "diagnostic_ag" in stages
        and "rca_exhausted" in stages
    ):
        return JourneyTerminalState.HARD_FAILURE_UNRESOLVED
    if (
        "clustered" in stages
        and "ag_assigned" not in stages
        and "diagnostic_ag" not in stages
    ):
        return JourneyTerminalState.TERMINAL_UNACTIONABLE
    return JourneyTerminalState.HARD_FAILURE_UNRESOLVED


_LANE_STAGES: frozenset[str] = frozenset({
    "proposed",
    "applied",
    "applied_targeted",
    "applied_broad_ag_scope",
    "dropped_at_grounding",
    "dropped_at_normalize",
    "dropped_at_applyability",
    "dropped_at_alignment",
    "dropped_at_reflection",
    "dropped_at_cap",
})


def _trunk_anchor_stage(events: list[QuestionJourneyEvent]) -> str:
    """Return the trunk stage that legally precedes a lane's `proposed`.

    The optimizer routes a qid to a lane via either ``ag_assigned`` (the
    typical case) or ``diagnostic_ag`` (the diagnostic-AG decompose
    path). When both are present, ``diagnostic_ag`` takes priority
    because it is emitted earlier in the trunk.
    """
    stages = {ev.stage for ev in events if not ev.proposal_id}
    if "diagnostic_ag" in stages:
        return "diagnostic_ag"
    if "ag_assigned" in stages:
        return "ag_assigned"
    return ""


def _lane_key(ev: QuestionJourneyEvent) -> str:
    """Plan N1 Task 4 — lane key for ``_split_trunk_and_lanes``.

    Prefers ``parent_proposal_id`` so a ``proposed`` event keyed on
    the parent (``P001``) and an ``applied_targeted`` event keyed on
    the expanded child (``P001#1``) collapse into the same lane.
    Falls back to ``proposal_id`` for legacy producer sites that
    have not yet stamped the parent.
    """
    return ev.parent_proposal_id or ev.proposal_id


def _split_trunk_and_lanes(
    events: list[QuestionJourneyEvent],
) -> tuple[list[str], dict[str, list[str]]]:
    """Return (trunk_stages, lanes_by_lane_key).

    ``trunk_stages`` is the canonical-ordered list of stages for events
    whose ``proposal_id`` is empty. ``lanes_by_lane_key`` is a dict
    keyed by ``parent_proposal_id`` (or ``proposal_id`` when the parent
    is empty), with each value the canonical-ordered list of stages
    for that lane.

    Lane sorting uses the same ``_stage_rank`` ordering as the trunk so
    `proposed -> applied -> dropped_at_*` always validates in causal
    order.

    Plan N1 Task 4 — intra-lane dedup: within a lane, two events that
    share the same (stage, expanded proposal_id, drop reason) collapse
    to one. Multi-cluster routing for the same qid (e.g., emitted once
    under ``cluster_id=H001`` and again under ``cluster_id=rca_*``)
    is metadata, not a state change.
    """
    from genie_space_optimizer.optimization.question_journey import _stage_rank

    trunk_events: list[QuestionJourneyEvent] = []
    lane_events: dict[str, list[QuestionJourneyEvent]] = {}
    for ev in events:
        if (ev.parent_proposal_id or ev.proposal_id) and ev.stage in _LANE_STAGES:
            lane_events.setdefault(_lane_key(ev), []).append(ev)
        else:
            trunk_events.append(ev)

    trunk_stages = [
        ev.stage
        for ev in sorted(trunk_events, key=lambda e: _stage_rank(e.stage))
    ]

    def _dedup_key(ev: QuestionJourneyEvent) -> tuple[str, str, str]:
        # Collapse repeated (stage, expanded_proposal_id, reason) tuples
        # so the same proposal emitted under two different cluster_ids
        # appears once in the lane chain.
        return (ev.stage, ev.proposal_id or "", ev.reason or "")

    lanes_by_pid: dict[str, list[str]] = {}
    for key, lst in lane_events.items():
        ordered = sorted(lst, key=lambda e: _stage_rank(e.stage))
        seen: set[tuple[str, str, str]] = set()
        deduped: list[str] = []
        for ev in ordered:
            dk = _dedup_key(ev)
            if dk in seen:
                continue
            seen.add(dk)
            deduped.append(ev.stage)
        lanes_by_pid[key] = deduped
    return trunk_stages, lanes_by_pid


def _ordered_stages_for_qid(events: list[QuestionJourneyEvent]) -> list[str]:
    """Backwards-compat alias used by ``canonical_journey_json``.

    Returns trunk + every lane concatenated. Used only for byte-stable
    serialization, NOT for transition validation. Validation goes through
    ``_split_trunk_and_lanes`` so each lane is checked independently.
    """
    trunk_stages, lanes_by_pid = _split_trunk_and_lanes(events)
    flattened = list(trunk_stages)
    for pid in sorted(lanes_by_pid):
        flattened.extend(lanes_by_pid[pid])
    return flattened


def validate_question_journeys(
    *,
    events: list[QuestionJourneyEvent],
    eval_qids: Iterable[str],
) -> JourneyValidationReport:
    """Assert every evaluated qid has a complete, legal journey.

    Lane-aware: events are split into a trunk (no proposal_id) and one
    lane per proposal_id. Each lane validates independently with its
    trunk anchor (``ag_assigned`` or ``diagnostic_ag``) prepended so the
    first transition is the lane's anchor -> proposed.
    """
    # Cycle 6 F-5 — collapse consecutive identical trunk events at the
    # validator boundary. The producer-side emit can legitimately call
    # ``_journey_emit`` twice for the same (qid, stage) on adjacent
    # passes (soft-pile classifier + cluster-formation), and validating
    # the raw stream produces noise that obscures real violations.
    from genie_space_optimizer.optimization.question_journey import (
        dedupe_consecutive_trunk_events,
    )
    events = dedupe_consecutive_trunk_events(list(events or ()))

    legal_stages = {s.value for s in JourneyStage}
    eval_qid_set = {str(q) for q in eval_qids if q}

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
        trunk_stages, lanes_by_pid = _split_trunk_and_lanes(qevents)
        anchor = _trunk_anchor_stage(qevents)

        all_chains: list[tuple[str, list[str]]] = [("trunk", trunk_stages)]
        for pid in sorted(lanes_by_pid):
            chain = lanes_by_pid[pid]
            if anchor and chain:
                chain = [anchor, *chain]
            all_chains.append((f"lane[{pid}]", chain))

        for chain_label, ordered in all_chains:
            for s in ordered:
                if s not in legal_stages:
                    violations.append(
                        JourneyContractViolation(
                            question_id=qid,
                            kind="unknown_stage",
                            detail=f"{chain_label}: stage={s!r} not in JourneyStage",
                        )
                    )
            for prev_s, next_s in zip(ordered, ordered[1:]):
                if prev_s not in legal_stages or next_s not in legal_stages:
                    continue
                if not is_legal_next_stage(
                    prev=JourneyStage(prev_s),
                    nxt=JourneyStage(next_s),
                ):
                    violations.append(
                        JourneyContractViolation(
                            question_id=qid,
                            kind="illegal_transition",
                            detail=f"{chain_label}: {prev_s} -> {next_s}",
                        )
                    )

        # Terminal-state check: classification + (for eval qids) post_eval.
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
