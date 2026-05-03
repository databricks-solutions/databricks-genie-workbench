"""Structured trace helpers for RCA lever-loop decisions.

Phase B (`docs/2026-05-02-unified-trace-and-operator-transcript-plan.md`)
extends this module into the canonical optimizer-decision trace owner:
``DecisionRecord`` is the source-of-truth row model, ``OptimizationTrace``
is the in-memory container, and the existing legacy Delta rows + scoreboard
+ operator transcript are deterministic projections over the same trace.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class DecisionType(str, Enum):
    EVAL_CLASSIFIED = "eval_classified"
    CLUSTER_SELECTED = "cluster_selected"
    RCA_FORMED = "rca_formed"
    STRATEGIST_AG_EMITTED = "strategist_ag_emitted"
    PROPOSAL_GENERATED = "proposal_generated"
    GATE_DECISION = "gate_decision"
    PATCH_APPLIED = "patch_applied"
    PATCH_SKIPPED = "patch_skipped"
    ACCEPTANCE_DECIDED = "acceptance_decided"
    QID_RESOLUTION = "qid_resolution"


class DecisionOutcome(str, Enum):
    INFO = "info"
    ACCEPTED = "accepted"
    DROPPED = "dropped"
    APPLIED = "applied"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"


class ReasonCode(str, Enum):
    NONE = "none"
    ALREADY_PASSING = "already_passing"
    HARD_FAILURE = "hard_failure"
    SOFT_SIGNAL = "soft_signal"
    GT_CORRECTION = "gt_correction"
    CLUSTERED = "clustered"
    RCA_GROUNDED = "rca_grounded"
    RCA_UNGROUNDED = "rca_ungrounded"
    STRATEGIST_SELECTED = "strategist_selected"
    PROPOSAL_EMITTED = "proposal_emitted"
    NO_CAUSAL_TARGET = "no_causal_target"
    PATCH_CAP_SELECTED = "patch_cap_selected"
    PATCH_CAP_DROPPED = "patch_cap_dropped"
    PATCH_APPLIED = "patch_applied"
    PATCH_SKIPPED = "patch_skipped"
    MISSING_TARGET_QIDS = "missing_target_qids"
    NO_APPLIED_PATCHES = "no_applied_patches"
    POST_EVAL_HOLD_PASS = "post_eval_hold_pass"
    POST_EVAL_FAIL_TO_PASS = "post_eval_fail_to_pass"
    POST_EVAL_HOLD_FAIL = "post_eval_hold_fail"
    POST_EVAL_PASS_TO_FAIL = "post_eval_pass_to_fail"


def _enum_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value or "")


def _clean_str_tuple(values: Sequence[Any] | None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(v) for v in (values or ()) if str(v)))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


@dataclass(frozen=True)
class DecisionRecord:
    run_id: str = ""
    iteration: int = 0
    decision_type: DecisionType = DecisionType.EVAL_CLASSIFIED
    outcome: DecisionOutcome = DecisionOutcome.INFO
    reason_code: ReasonCode = ReasonCode.NONE
    question_id: str = ""
    cluster_id: str = ""
    rca_id: str = ""
    ag_id: str = ""
    proposal_id: str = ""
    patch_id: str = ""
    gate: str = ""
    reason_detail: str = ""
    affected_qids: tuple[str, ...] = ()
    # RCA-grounding contract fields (Phase B): every applicable decision must
    # carry the chain "evidence -> RCA -> causal target qids -> expected effect
    # -> observed effect -> next action" (regression_qids when applicable).
    # See `docs/2026-05-02-unified-trace-and-operator-transcript-plan.md`
    # `## Required Decision Fields`.
    evidence_refs: tuple[str, ...] = ()
    root_cause: str = ""
    target_qids: tuple[str, ...] = ()
    expected_effect: str = ""
    observed_effect: str = ""
    regression_qids: tuple[str, ...] = ()
    next_action: str = ""
    source_cluster_ids: tuple[str, ...] = ()
    proposal_ids: tuple[str, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "run_id": str(self.run_id),
            "iteration": int(self.iteration),
            "decision_type": self.decision_type.value,
            "outcome": self.outcome.value,
            "reason_code": self.reason_code.value,
        }
        optional = {
            "question_id": self.question_id,
            "cluster_id": self.cluster_id,
            "rca_id": self.rca_id,
            "ag_id": self.ag_id,
            "proposal_id": self.proposal_id,
            "patch_id": self.patch_id,
            "gate": self.gate,
            "reason_detail": self.reason_detail,
            "root_cause": self.root_cause,
            "expected_effect": self.expected_effect,
            "observed_effect": self.observed_effect,
            "next_action": self.next_action,
        }
        for key, value in optional.items():
            if value:
                row[key] = str(value)
        if self.affected_qids:
            row["affected_qids"] = list(self.affected_qids)
        if self.evidence_refs:
            row["evidence_refs"] = list(self.evidence_refs)
        if self.target_qids:
            row["target_qids"] = list(self.target_qids)
        if self.regression_qids:
            row["regression_qids"] = list(self.regression_qids)
        if self.source_cluster_ids:
            row["source_cluster_ids"] = list(self.source_cluster_ids)
        if self.proposal_ids:
            row["proposal_ids"] = list(self.proposal_ids)
        if self.metrics:
            row["metrics"] = _json_safe(dict(self.metrics))
        return row

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "DecisionRecord":
        return cls(
            run_id=str(row.get("run_id") or ""),
            iteration=_as_int(row.get("iteration")),
            decision_type=DecisionType(str(row.get("decision_type") or "eval_classified")),
            outcome=DecisionOutcome(str(row.get("outcome") or "info")),
            reason_code=ReasonCode(str(row.get("reason_code") or "none")),
            question_id=str(row.get("question_id") or ""),
            cluster_id=str(row.get("cluster_id") or ""),
            rca_id=str(row.get("rca_id") or ""),
            ag_id=str(row.get("ag_id") or ""),
            proposal_id=str(row.get("proposal_id") or ""),
            patch_id=str(row.get("patch_id") or ""),
            gate=str(row.get("gate") or ""),
            reason_detail=str(row.get("reason_detail") or ""),
            affected_qids=_clean_str_tuple(row.get("affected_qids") or ()),
            evidence_refs=_clean_str_tuple(row.get("evidence_refs") or ()),
            root_cause=str(row.get("root_cause") or ""),
            target_qids=_clean_str_tuple(row.get("target_qids") or ()),
            expected_effect=str(row.get("expected_effect") or ""),
            observed_effect=str(row.get("observed_effect") or ""),
            regression_qids=_clean_str_tuple(row.get("regression_qids") or ()),
            next_action=str(row.get("next_action") or ""),
            source_cluster_ids=_clean_str_tuple(row.get("source_cluster_ids") or ()),
            proposal_ids=_clean_str_tuple(row.get("proposal_ids") or ()),
            metrics=dict(row.get("metrics") or {}),
        )


def _decision_sort_key(rec: DecisionRecord) -> tuple:
    return (
        int(rec.iteration),
        rec.decision_type.value,
        rec.question_id,
        rec.cluster_id,
        rec.ag_id,
        rec.proposal_id,
        rec.patch_id,
        rec.gate,
        rec.reason_code.value,
    )


def canonical_decision_json(records: Sequence[DecisionRecord]) -> str:
    rows = [r.to_dict() for r in sorted(records, key=_decision_sort_key)]
    return json.dumps(rows, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class OptimizationTrace:
    """Canonical in-memory container for the optimizer-decision trace.

    Owns journey events, typed decision records, and per-iteration
    validation reports. Replay, fixtures, persistence, and the operator
    transcript are deterministic projections over this single source.
    """

    journey_events: tuple[Any, ...] = ()
    decision_records: tuple[DecisionRecord, ...] = ()
    validation_by_iteration: Mapping[int, Mapping[str, Any]] = field(default_factory=dict)

    def canonical_decision_json(self) -> str:
        return canonical_decision_json(self.decision_records)

    def render_operator_transcript(self, *, iteration: int) -> str:
        return render_operator_transcript(trace=self, iteration=iteration)


def _record_qids(record: DecisionRecord) -> tuple[str, ...]:
    if record.affected_qids:
        return record.affected_qids
    if record.question_id:
        return (record.question_id,)
    return ()


def _has_event(
    *,
    events: Sequence[Any],
    qid: str,
    stage: str,
    proposal_id: str = "",
) -> bool:
    for ev in events:
        if getattr(ev, "question_id", "") != qid:
            continue
        if getattr(ev, "stage", "") != stage:
            continue
        if proposal_id and getattr(ev, "proposal_id", "") != proposal_id:
            continue
        return True
    return False


def validate_decisions_against_journey(
    *,
    records: Sequence[DecisionRecord],
    events: Sequence[Any],
) -> list[str]:
    """Cross-check decision records against the journey events.

    Two layers of checks:

    1. **RCA-required fields** — every applicable decision_type must
       carry the chain ``evidence_refs -> rca_id -> root_cause ->
       target_qids``. ``EVAL_CLASSIFIED`` is exempt from rca_id /
       root_cause (the event hasn't been routed to an RCA yet).
       Decisions whose reason_code is ``MISSING_TARGET_QIDS`` are
       allowed to omit ``target_qids`` (that's exactly what the reason
       code says).
    2. **Journey-stage cross-check** — decision types that imply a
       particular journey stage must have a matching event for each
       affected qid (proposal_generated -> proposed, patch_applied ->
       applied, qid_resolution -> post_eval).

    Empty list = clean cross-check.
    """
    violations: list[str] = []
    rca_required = {
        DecisionType.CLUSTER_SELECTED,
        DecisionType.RCA_FORMED,
        DecisionType.STRATEGIST_AG_EMITTED,
        DecisionType.PROPOSAL_GENERATED,
        DecisionType.GATE_DECISION,
        DecisionType.PATCH_APPLIED,
        DecisionType.PATCH_SKIPPED,
        DecisionType.ACCEPTANCE_DECIDED,
        DecisionType.QID_RESOLUTION,
    }
    stage_requirements = {
        DecisionType.PROPOSAL_GENERATED: "proposed",
        DecisionType.PATCH_APPLIED: "applied",
        DecisionType.QID_RESOLUTION: "post_eval",
    }
    # ``POST_EVAL_HOLD_PASS`` records describe qids that were passing
    # before AND after this iteration's patches; they were never
    # clustered, so claiming an ``rca_id`` would be a lie. Exempt the
    # held-pass path from rca-required and target_qids checks. See
    # `docs/2026-05-02-unified-trace-and-operator-transcript-plan.md`
    # postmortem follow-up.
    rca_exempt_reason_codes = {ReasonCode.POST_EVAL_HOLD_PASS}
    for record in records:
        if (
            record.decision_type in rca_required
            and record.reason_code not in rca_exempt_reason_codes
        ):
            if not record.evidence_refs:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no evidence_refs"
                )
            if not record.rca_id and record.decision_type not in {DecisionType.EVAL_CLASSIFIED}:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no rca_id"
                )
            if not record.root_cause and record.decision_type not in {DecisionType.EVAL_CLASSIFIED}:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no root_cause"
                )
            if not record.target_qids and record.reason_code != ReasonCode.MISSING_TARGET_QIDS:
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no target_qids"
                )
        required_stage = stage_requirements.get(record.decision_type)
        if not required_stage:
            continue
        for qid in _record_qids(record):
            if _has_event(
                events=events,
                qid=qid,
                stage=required_stage,
                proposal_id=record.proposal_id if required_stage in {"proposed", "applied"} else "",
            ):
                continue
            violations.append(
                "decision "
                f"{record.decision_type.value} qid={qid} "
                f"proposal={record.proposal_id or '-'} "
                f"has no matching journey stage {required_stage}"
            )
    return violations


def render_operator_transcript(
    *,
    trace: OptimizationTrace,
    iteration: int,
) -> str:
    """Render the deterministic stdout projection of OptimizationTrace.

    The transcript follows the fixed nine-section schema defined in the
    plan's `## Observability Contract`. Section headings always appear so
    operators can scan for any section even when empty (which itself is a
    diagnostic signal — e.g., empty 'AG Decisions And Rationale' on an
    iteration that should have produced AGs is a wiring bug).
    """
    records = [
        r for r in trace.decision_records
        if int(r.iteration) == int(iteration)
    ]
    bar = "-" * 100
    lines = [
        f"+{bar}",
        f"|  OPERATOR TRANSCRIPT  iteration={iteration}",
        f"+{bar}",
        "|  Iteration Summary",
        f"|  Decision records: {len(records)}",
        "|",
        "|  Hard Failures And QID State",
        "|",
        "|  RCA Cards With Evidence",
        "|",
        "|  AG Decisions And Rationale",
        "|",
        "|  Proposal Survival And Gate Drops",
        "|",
        "|  Applied Patches And Acceptance",
        "|",
        "|  Observed Results And Regressions",
        "|",
        "|  Unresolved QID Buckets",
        "|",
        "|  Next Suggested Action",
    ]
    by_type: dict[str, list[DecisionRecord]] = {}
    for rec in sorted(records, key=_decision_sort_key):
        by_type.setdefault(rec.decision_type.value, []).append(rec)
    for dtype in sorted(by_type):
        lines.append("|")
        lines.append(f"|  {dtype}")
        for rec in by_type[dtype]:
            qids = list(rec.affected_qids) or ([rec.question_id] if rec.question_id else [])
            target = ",".join(qids) if qids else "-"
            parts = [
                f"outcome={rec.outcome.value}",
                f"reason={rec.reason_code.value}",
                f"qid={target}",
            ]
            if rec.cluster_id:
                parts.append(f"cluster={rec.cluster_id}")
            if rec.ag_id:
                parts.append(f"ag={rec.ag_id}")
            if rec.proposal_id:
                parts.append(f"proposal={rec.proposal_id}")
            if rec.gate:
                parts.append(f"gate={rec.gate}")
            if rec.reason_detail:
                parts.append(f"detail={rec.reason_detail}")
            if rec.root_cause:
                parts.append(f"root={rec.root_cause}")
            if rec.expected_effect:
                parts.append(f"expected={rec.expected_effect}")
            if rec.observed_effect:
                parts.append(f"observed={rec.observed_effect}")
            if rec.next_action:
                parts.append(f"next={rec.next_action}")
            lines.append("|    - " + "  ".join(parts))
    lines.append(f"+{bar}")
    return "\n".join(lines)


def summarize_patch_for_trace(patch: dict[str, Any]) -> dict[str, Any]:
    target = (
        patch.get("section_name")
        or patch.get("section")
        or patch.get("column")
        or patch.get("function")
        or patch.get("target")
        or patch.get("target_object")
        or patch.get("display_name")
        or ""
    )
    return {
        "proposal_id": str(
            patch.get("proposal_id")
            or patch.get("expanded_patch_id")
            or patch.get("source_proposal_id")
            or patch.get("id")
            or ""
        ),
        "parent_proposal_id": str(
            patch.get("parent_proposal_id")
            or patch.get("source_proposal_id")
            or ""
        ),
        "expanded_patch_id": str(patch.get("expanded_patch_id") or ""),
        "lever": _as_int(patch.get("lever"), 5),
        "patch_type": patch.get("patch_type") or patch.get("type"),
        "target": str(target),
        "rca_id": patch.get("rca_id"),
        "patch_family": patch.get("patch_family"),
        "target_qids": list(patch.get("target_qids") or []),
        "relevance_score": _as_float(patch.get("relevance_score")),
    }


def patch_cap_decision_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    decisions: list[dict[str, Any]],
) -> list[DecisionRecord]:
    """Convert per-AG patch-cap audit dicts into typed DecisionRecord rows.

    This is the Phase B source-of-truth conversion. ``patch_cap_decision_rows``
    below is the legacy Delta-row adapter that delegates here.

    Carries the RCA-grounding contract fields when the upstream decision
    dict supplies them (rca_id, root_cause, evidence_refs, target_qids,
    expected_effect, regression_qids). Synthesizes ``observed_effect``
    and ``next_action`` from the decision's selected/dropped state so the
    transcript always carries an operator-actionable next step.
    """
    records: list[DecisionRecord] = []
    for decision in decisions:
        proposal_id = str(decision.get("proposal_id") or "")
        selected = decision.get("decision") == "selected"
        target_qids = _clean_str_tuple(decision.get("target_qids") or ())
        rca_id = str(decision.get("rca_id") or "")
        root_cause = str(decision.get("root_cause") or "")
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.GATE_DECISION,
                outcome=DecisionOutcome.ACCEPTED if selected else DecisionOutcome.DROPPED,
                reason_code=(
                    ReasonCode.PATCH_CAP_SELECTED
                    if selected else ReasonCode.PATCH_CAP_DROPPED
                ),
                question_id=target_qids[0] if len(target_qids) == 1 else "",
                rca_id=rca_id,
                root_cause=root_cause,
                ag_id=ag_id,
                proposal_id=proposal_id,
                gate="patch_cap",
                reason_detail=str(decision.get("selection_reason") or ""),
                evidence_refs=_clean_str_tuple(decision.get("evidence_refs") or ()),
                affected_qids=target_qids,
                target_qids=target_qids,
                expected_effect=str(decision.get("expected_effect") or ""),
                observed_effect=(
                    "Selected for apply" if selected else "Dropped by patch cap"
                ),
                regression_qids=_clean_str_tuple(decision.get("regression_qids") or ()),
                next_action=(
                    "Apply selected patch and evaluate target qids"
                    if selected else "Inspect lower-ranked patch if target remains unresolved"
                ),
                proposal_ids=(proposal_id,) if proposal_id else (),
                metrics={
                    "selection_reason": decision.get("selection_reason"),
                    "rank": decision.get("rank"),
                    "relevance_score": _as_float(decision.get("relevance_score")),
                    "lever": _as_int(decision.get("lever"), 5),
                    "patch_type": decision.get("patch_type"),
                    "rca_id": rca_id,
                    "root_cause": root_cause,
                    "target_qids": list(target_qids),
                    "parent_proposal_id": str(decision.get("parent_proposal_id") or ""),
                    "expanded_patch_id": str(decision.get("expanded_patch_id") or ""),
                },
            )
        )
    return records


def patch_cap_decision_rows(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Legacy Delta-row adapter over ``patch_cap_decision_records``.

    Phase B: typed ``DecisionRecord`` is the source of truth; this row
    shape is preserved for the existing Delta persistence path
    (``write_decisions``) and for callers that read the legacy schema.
    """
    rows: list[dict[str, Any]] = []
    for idx, record in enumerate(
        patch_cap_decision_records(
            run_id=run_id,
            iteration=iteration,
            ag_id=ag_id,
            decisions=decisions,
        ),
        start=1,
    ):
        row = record.to_dict()
        rows.append({
            "run_id": row["run_id"],
            "iteration": row["iteration"],
            "ag_id": row.get("ag_id"),
            "decision_order": idx,
            "stage_letter": "I",
            "gate_name": row.get("gate", ""),
            "decision": (
                "accepted"
                if record.outcome == DecisionOutcome.ACCEPTED else "dropped"
            ),
            "reason_code": (
                None
                if record.outcome == DecisionOutcome.ACCEPTED
                else row.get("reason_detail")
            ),
            "reason_detail": row.get("reason_detail"),
            "affected_qids": row.get("affected_qids", []),
            "source_cluster_ids": row.get("source_cluster_ids", []),
            "proposal_ids": row.get("proposal_ids", []),
            "proposal_to_patch_map": None,
            "metrics": row.get("metrics", {}),
        })
    return rows


def format_patch_inventory(
    patches: list[dict[str, Any]],
    *,
    max_rows: int = 8,
) -> str:
    summaries = [summarize_patch_for_trace(p) for p in patches[:max_rows]]
    parts = [
        (
            f"{s['proposal_id']} L{s['lever']} {s['patch_type']} "
            f"target={s['target']} rel={s['relevance_score']:.2f} "
            f"rca={s['rca_id']} qids={s['target_qids']}"
        )
        for s in summaries
    ]
    if len(patches) > max_rows:
        parts.append(f"+{len(patches) - max_rows} more")
    return "; ".join(parts) if parts else "(none)"
