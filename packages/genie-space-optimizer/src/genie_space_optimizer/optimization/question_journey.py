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
    # Plan N1 Task 4 — parent proposal id for lane-key unification.
    # When the producer emits ``proposed`` keyed on the parent
    # (``P001``) and ``applied_targeted`` keyed on the expanded child
    # (``P001#1``), both stamp the same ``parent_proposal_id`` so the
    # validator's ``_split_trunk_and_lanes`` collapses them into the
    # same lane. Defaults to "" so legacy call sites that have not
    # yet stamped the field land in a per-``proposal_id`` lane.
    parent_proposal_id: str = ""
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


def emit_cluster_membership_events(
    *,
    journey_emit,
    hard_clusters: list[dict] | None = None,
    soft_clusters: list[dict] | None = None,
) -> None:
    """Plan N1 Task 2 — emit one trunk event per (qid, stage) pair
    across all clusters.

    Multi-cluster membership for a qid is preserved by stamping the
    primary cluster_id on the event and recording every other cluster
    on ``extra={"additional_cluster_ids": [...]}``. Validators see
    exactly one trunk event per qid; auditors still see the full
    membership.

    Closes the trunk-repeat journey-validation defect observed on
    2afb0be2-88b6-4832-99aa-c7e78fbc90f7 retry attempt
    993610879088298 where 7 + 5 ``soft_signal -> soft_signal``
    self-transitions came from qids appearing in multiple soft
    clusters.
    """
    def _emit_loop(
        clusters: list[dict],
        stage: str,
        seen: set[str],
        primary_cid: dict[str, str],
        extras: dict[str, list[str]],
    ) -> None:
        # First pass: pick a primary cluster per qid (insertion order
        # of clusters wins) and accumulate additional cluster ids.
        primary_qids_by_cluster: dict[str, list[str]] = {}
        primary_root_by_cluster: dict[str, str] = {}
        for c in clusters:
            cid = str(c.get("cluster_id") or "")
            rc = str(c.get("root_cause") or c.get("asi_failure_type") or "")
            primary_root_by_cluster[cid] = rc
            primary_qids_by_cluster.setdefault(cid, [])
            for q in (c.get("question_ids") or []):
                qid = str(q)
                if not qid:
                    continue
                if qid in seen:
                    extras.setdefault(qid, []).append(cid)
                    continue
                seen.add(qid)
                primary_cid[qid] = cid
                primary_qids_by_cluster[cid].append(qid)

        # Second pass: emit one event per (cluster, primary_qids).
        # additional_cluster_ids carries the secondary memberships.
        for cid, qids in primary_qids_by_cluster.items():
            if not qids:
                continue
            for qid in qids:
                journey_emit(
                    stage,
                    question_ids=[qid],
                    cluster_id=cid,
                    root_cause=primary_root_by_cluster.get(cid, ""),
                    extra={
                        "additional_cluster_ids": list(extras.get(qid, []))
                    },
                )

    seen_clustered: set[str] = set()
    seen_soft: set[str] = set()
    primary_clustered: dict[str, str] = {}
    primary_soft: dict[str, str] = {}
    extras_clustered: dict[str, list[str]] = {}
    extras_soft: dict[str, list[str]] = {}

    _emit_loop(
        list(hard_clusters or []),
        "clustered",
        seen_clustered,
        primary_clustered,
        extras_clustered,
    )
    _emit_loop(
        list(soft_clusters or []),
        "soft_signal",
        seen_soft,
        primary_soft,
        extras_soft,
    )


def dedupe_consecutive_trunk_events(
    events: list[QuestionJourneyEvent],
) -> list[QuestionJourneyEvent]:
    """Cycle 6 F-5 — collapse consecutive identical trunk events for
    the same qid. Trunk events are those with empty ``proposal_id``;
    lane events are intentionally per-patch and never deduped here.

    The dedup key is ``(question_id, stage)``; equal-keyed consecutive
    trunk events collapse to a single event (the first occurrence).
    A lane event between two trunk emits resets the tracker so a later
    same-stage trunk re-emit is preserved (legitimate cycle).

    Run 833969815458299 emitted 13 ``soft_signal -> soft_signal`` trunk
    transitions because the soft-pile classifier and the cluster-
    formation pass both append a soft_signal event for the same qid.
    N1's contract validator and lane-keys landed; this is the missing
    producer-side dedup.
    """
    deduped: list[QuestionJourneyEvent] = []
    last_trunk_key_by_qid: dict[str, tuple[str, str]] = {}
    for ev in events or ():
        is_trunk = not (ev.proposal_id or "")
        if is_trunk:
            key = (str(ev.question_id), str(ev.stage))
            if last_trunk_key_by_qid.get(str(ev.question_id)) == key:
                continue
            last_trunk_key_by_qid[str(ev.question_id)] = key
            deduped.append(ev)
        else:
            last_trunk_key_by_qid.pop(str(ev.question_id), None)
            deduped.append(ev)
    return deduped


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
