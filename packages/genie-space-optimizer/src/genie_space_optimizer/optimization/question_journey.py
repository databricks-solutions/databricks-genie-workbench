"""Per-question journey ledger for end-of-iteration diagnostic output."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


_STAGE_ORDER: list[str] = [
    "evaluated",
    "clustered",
    "soft_signal",
    "gt_correction_candidate",
    "intent_collision_detected",
    "already_passing",
    "diagnostic_ag",
    "ag_assigned",
    "proposed",
    "dropped_at_grounding",
    "dropped_at_normalize",
    "dropped_at_applyability",
    "dropped_at_alignment",
    "dropped_at_reflection",
    "dropped_at_cap",
    # Track 3/E (Phase A burn-down) — apply emit splits into:
    #   applied_targeted        — qid in patch.target_qids
    #   applied_broad_ag_scope  — qid in AG.affected_questions \ patch.target_qids
    # The bare ``applied`` stage is retained for replay compatibility
    # with snapshots written before this PR.
    "applied",
    "applied_targeted",
    "applied_broad_ag_scope",
    "rolled_back",
    "accepted",
    "accepted_with_regression_debt",
    "post_eval",
]


@dataclass(frozen=True)
class QuestionJourneyEvent:
    question_id: str
    stage: str
    cluster_id: str = ""
    ag_id: str = ""
    proposal_id: str = ""
    patch_type: str = ""
    root_cause: str = ""
    reason: str = ""
    was_passing: bool | None = None
    is_passing: bool | None = None
    transition: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def _stage_rank(stage: str) -> int:
    try:
        return _STAGE_ORDER.index(stage)
    except ValueError:
        return len(_STAGE_ORDER)


def _format_event(ev: QuestionJourneyEvent) -> str:
    parts: list[str] = [ev.stage]
    if ev.cluster_id:
        parts.append(f"cluster={ev.cluster_id}")
    if ev.root_cause:
        parts.append(f"root={ev.root_cause}")
    if ev.ag_id:
        parts.append(f"ag={ev.ag_id}")
    if ev.proposal_id:
        parts.append(f"pid={ev.proposal_id}")
    if ev.patch_type:
        parts.append(f"type={ev.patch_type}")
    if ev.reason:
        parts.append(f"reason={ev.reason}")
    if ev.transition:
        parts.append(f"transition={ev.transition}")
    if ev.was_passing is not None or ev.is_passing is not None:
        parts.append(f"was={ev.was_passing} is={ev.is_passing}")
    return "  ".join(parts)


def build_question_journey_ledger(
    *,
    events: list[QuestionJourneyEvent],
    iteration: int,
) -> str:
    """Render a per-qid timeline of every loop stage that touched it."""
    if not events:
        return ""
    by_qid: dict[str, list[QuestionJourneyEvent]] = {}
    for ev in events:
        if not ev.question_id:
            continue
        by_qid.setdefault(ev.question_id, []).append(ev)
    if not by_qid:
        return ""
    bar = "─" * 100
    lines = [
        f"┌{bar}",
        f"│  QUESTION JOURNEY LEDGER  iteration={iteration}",
        f"├{bar}",
    ]
    for qid in sorted(by_qid.keys()):
        lines.append(f"│  {qid}")
        sorted_events = sorted(
            by_qid[qid], key=lambda e: (_stage_rank(e.stage), e.proposal_id),
        )
        for ev in sorted_events:
            lines.append(f"│    └─ {_format_event(ev)}")
    lines.append(f"└{bar}")
    return "\n".join(lines)


def render_question_journey_once(
    *,
    events: list[QuestionJourneyEvent],
    iteration: int,
    render_state: dict[str, bool],
    printer=print,
) -> bool:
    """Render the journey ledger at most once for an AG iteration.

    ``render_state`` is a mutable one-key dict owned by the harness loop.
    Returning ``True`` means the render opportunity was consumed, even when
    there are no events and therefore no stdout text. This prevents duplicate
    ledgers when rollback paths call the renderer and the bottom-of-loop hook
    also executes.
    """
    if render_state.get("rendered"):
        return True
    ledger = build_question_journey_ledger(events=events, iteration=iteration)
    if ledger:
        printer(ledger)
    render_state["rendered"] = True
    return True
