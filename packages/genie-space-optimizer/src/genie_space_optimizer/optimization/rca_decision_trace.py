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
    AG_RETIRED = "ag_retired"


class DecisionOutcome(str, Enum):
    INFO = "info"
    ACCEPTED = "accepted"
    DROPPED = "dropped"
    APPLIED = "applied"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    RETIRED = "retired"


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
    AG_TARGET_NO_LONGER_HARD = "ag_target_no_longer_hard"


class RejectReason(str, Enum):
    """Why a candidate option was rejected in favor of the chosen one.

    Used by AlternativeOption to record cluster, AG, and proposal
    candidates that were considered but not selected.
    """
    NONE = "none"
    BELOW_HARD_THRESHOLD = "below_hard_threshold"  # cluster: not promoted to hard
    INSUFFICIENT_QIDS = "insufficient_qids"        # cluster: too small
    LOWER_SCORE = "lower_score"                    # ag/proposal: ranked below chosen
    BUFFERED = "buffered"                          # ag: deferred to later iteration
    MISSING_TARGET_QIDS = "missing_target_qids"    # ag/proposal: cycle-8-bug-1 pattern
    MALFORMED = "malformed"                        # proposal: failed shape validation
    PATCH_CAP_DROPPED = "patch_cap_dropped"        # proposal: dropped by N-cap
    RCA_UNGROUNDED = "rca_ungrounded"              # ag/proposal: no RCA backing
    OTHER = "other"


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
class AlternativeOption:
    """A single candidate option that was rejected in favor of the chosen one.

    Stamped on `DecisionRecord.alternatives_considered` for
    CLUSTER_SELECTED, STRATEGIST_AG_EMITTED, and PROPOSAL_GENERATED.
    """
    option_id: str = ""
    kind: str = ""  # one of "cluster" | "ag" | "proposal"
    score: float | None = None
    reject_reason: RejectReason = RejectReason.NONE
    reject_detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "option_id": str(self.option_id),
            "kind": str(self.kind),
            "reject_reason": self.reject_reason.value,
        }
        if self.score is not None:
            row["score"] = float(self.score)
        if self.reject_detail:
            row["reject_detail"] = str(self.reject_detail)
        return row

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "AlternativeOption":
        score_raw = row.get("score")
        return cls(
            option_id=str(row.get("option_id") or ""),
            kind=str(row.get("kind") or ""),
            score=float(score_raw) if score_raw is not None else None,
            reject_reason=RejectReason(
                str(row.get("reject_reason") or "none")
            ),
            reject_detail=str(row.get("reject_detail") or ""),
        )


def _alt_sort_key(opt: AlternativeOption) -> tuple:
    return (str(opt.kind), str(opt.option_id))


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
    alternatives_considered: tuple[AlternativeOption, ...] = ()

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
        if self.alternatives_considered:
            row["alternatives_considered"] = [
                opt.to_dict()
                for opt in sorted(self.alternatives_considered, key=_alt_sort_key)
            ]
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
            alternatives_considered=tuple(
                AlternativeOption.from_dict(o)
                for o in (row.get("alternatives_considered") or ())
            ),
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


# Phase B delta Task 8: ``PATCH_APPLIED`` decision records map to the
# ``applied`` journey stage, but the harness has split that stage into
# ``applied_targeted`` (qid was in patch's target_qids) and
# ``applied_broad_ag_scope`` (qid was in the AG's affected_questions
# but not the patch's narrow target). Both descend from ``applied``,
# so the validator treats them as members of the same stage family.
_STAGE_FAMILIES: Mapping[str, tuple[str, ...]] = {
    "applied": ("applied", "applied_targeted", "applied_broad_ag_scope"),
}


def _stage_matches(event_stage: str, required_stage: str) -> bool:
    family = _STAGE_FAMILIES.get(required_stage)
    if family is not None:
        return event_stage in family
    return event_stage == required_stage


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
        if not _stage_matches(getattr(ev, "stage", ""), stage):
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
    # Phase C Task 7: ``RCA_FORMED + RCA_UNGROUNDED`` is the no-RCA-for-
    # cluster signal. The cluster failed but RCA produced no finding —
    # so claiming an ``rca_id`` would also be a lie. Exempt exactly that
    # pairing. ``rca_id`` and ``root_cause`` exemptions are scoped to
    # specific (decision_type, reason_code) pairs so a forgotten emit
    # site cannot silently route through the exemption.
    rca_exempt_reason_codes = {ReasonCode.POST_EVAL_HOLD_PASS}
    rca_id_exempt_pairs: set[tuple[DecisionType, ReasonCode]] = {
        (DecisionType.RCA_FORMED, ReasonCode.RCA_UNGROUNDED),
    }
    root_cause_exempt_pairs: set[tuple[DecisionType, ReasonCode]] = {
        (DecisionType.RCA_FORMED, ReasonCode.RCA_UNGROUNDED),
    }
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
            if (
                not record.rca_id
                and record.decision_type not in {DecisionType.EVAL_CLASSIFIED}
                and (record.decision_type, record.reason_code) not in rca_id_exempt_pairs
            ):
                violations.append(
                    f"decision {record.decision_type.value} qid={record.question_id or '-'} "
                    "has no rca_id"
                )
            if (
                not record.root_cause
                and record.decision_type not in {DecisionType.EVAL_CLASSIFIED}
                and (record.decision_type, record.reason_code) not in root_cause_exempt_pairs
            ):
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


# Phase B delta Task 9: project each DecisionType into one of the nine
# fixed transcript sections defined by the roadmap's Observability
# Contract. Sections are stable strings (also used as transcript
# headings) so a future DecisionType added without a mapping fails
# `test_every_decision_type_has_an_assigned_section` loud.
SECTION_HARD_FAILURES = "Hard Failures And QID State"
SECTION_RCA_CARDS = "RCA Cards With Evidence"
SECTION_AG_DECISIONS = "AG Decisions And Rationale"
SECTION_PROPOSAL_SURVIVAL = "Proposal Survival And Gate Drops"
SECTION_APPLIED_PATCHES = "Applied Patches And Acceptance"
SECTION_OBSERVED_RESULTS = "Observed Results And Regressions"
SECTION_UNRESOLVED_QIDS = "Unresolved QID Buckets"
SECTION_NEXT_ACTION = "Next Suggested Action"

TYPE_TO_SECTION: Mapping[DecisionType, str] = {
    DecisionType.EVAL_CLASSIFIED: SECTION_HARD_FAILURES,
    DecisionType.CLUSTER_SELECTED: SECTION_RCA_CARDS,
    DecisionType.RCA_FORMED: SECTION_RCA_CARDS,
    DecisionType.STRATEGIST_AG_EMITTED: SECTION_AG_DECISIONS,
    DecisionType.PROPOSAL_GENERATED: SECTION_PROPOSAL_SURVIVAL,
    DecisionType.GATE_DECISION: SECTION_PROPOSAL_SURVIVAL,
    DecisionType.PATCH_APPLIED: SECTION_APPLIED_PATCHES,
    DecisionType.PATCH_SKIPPED: SECTION_APPLIED_PATCHES,
    DecisionType.ACCEPTANCE_DECIDED: SECTION_APPLIED_PATCHES,
    DecisionType.QID_RESOLUTION: SECTION_OBSERVED_RESULTS,
    # PR-B2: AG retirement is an AG-level decision (the resolver retiring
    # buffered AGs at plateau because their target qids reclassified out
    # of hard) — same section as the original AG emission.
    DecisionType.AG_RETIRED: SECTION_AG_DECISIONS,
}


def _format_alternatives_line(rec: "DecisionRecord") -> str | None:
    """Return one indented line summarizing rec.alternatives_considered.

    Format: "    alternatives: <opt_id>(<reason_code>:<detail>), ..."
    Returns None when the record has no alternatives to surface.
    """
    if not rec.alternatives_considered:
        return None
    parts: list[str] = []
    for opt in sorted(rec.alternatives_considered, key=_alt_sort_key):
        chunk = f"{opt.option_id}({opt.reject_reason.value}"
        if opt.reject_detail:
            chunk += f":{opt.reject_detail}"
        chunk += ")"
        parts.append(chunk)
    return "|    alternatives: " + ", ".join(parts)


def _format_record_line(rec: "DecisionRecord") -> str:
    qids = list(rec.affected_qids) or (
        [rec.question_id] if rec.question_id else []
    )
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
    if rec.rca_id:
        parts.append(f"rca={rec.rca_id}")
    if rec.expected_effect:
        parts.append(f"expected={rec.expected_effect}")
    if rec.observed_effect:
        parts.append(f"observed={rec.observed_effect}")
    if rec.next_action:
        parts.append(f"next={rec.next_action}")
    return "|    - " + "  ".join(parts)


def render_operator_transcript(
    *,
    trace: OptimizationTrace,
    iteration: int,
) -> str:
    """Render the deterministic stdout projection of OptimizationTrace.

    Phase B delta Task 9: each ``DecisionType`` is projected into one
    of the nine named sections via ``TYPE_TO_SECTION``. Section
    headings always appear (even when empty) so operators can scan
    for any section, and an empty section is itself a diagnostic
    signal (e.g., empty ``RCA Cards With Evidence`` on an iteration
    that had hard failures means the cluster -> RCA wiring broke).

    QID_RESOLUTION records with ``UNRESOLVED`` outcome additionally
    surface in ``Unresolved QID Buckets``; the ``Next Suggested
    Action`` section is rendered from the dominant un-passed cluster's
    ``next_action`` field if present, falling back to a static prompt.
    """
    records = [
        r for r in trace.decision_records
        if int(r.iteration) == int(iteration)
    ]
    sorted_records = sorted(records, key=_decision_sort_key)
    by_section: dict[str, list[DecisionRecord]] = {}
    for rec in sorted_records:
        section = TYPE_TO_SECTION.get(rec.decision_type)
        if section is None:
            # Defensive: future DecisionType without a mapping. The
            # test_every_decision_type_has_an_assigned_section guard
            # catches this on every CI run, but at runtime we keep the
            # output legible by falling back to Next Action.
            section = SECTION_NEXT_ACTION
        by_section.setdefault(section, []).append(rec)

    bar = "-" * 100
    lines = [
        f"+{bar}",
        f"|  OPERATOR TRANSCRIPT  iteration={iteration}",
        f"+{bar}",
        "|  Iteration Summary",
        f"|  Decision records: {len(records)}",
    ]

    section_order = (
        SECTION_HARD_FAILURES,
        SECTION_RCA_CARDS,
        SECTION_AG_DECISIONS,
        SECTION_PROPOSAL_SURVIVAL,
        SECTION_APPLIED_PATCHES,
        SECTION_OBSERVED_RESULTS,
        SECTION_UNRESOLVED_QIDS,
        SECTION_NEXT_ACTION,
    )
    for section in section_order:
        lines.append("|")
        lines.append(f"|  {section}")
        if section == SECTION_UNRESOLVED_QIDS:
            unresolved = [
                r for r in by_section.get(SECTION_OBSERVED_RESULTS, [])
                if r.outcome == DecisionOutcome.UNRESOLVED
            ]
            # Phase D Failure-Bucketing T5: classify each unresolved qid
            # via the canonical RCA-chain ladder. Render a histogram
            # summary line at the top of the section, then annotate each
            # per-qid line with bucket label + next-action prose.
            try:
                from genie_space_optimizer.optimization.failure_bucketing import (
                    classify_unresolved_qid,
                )
                _classifier_available = True
            except ImportError:
                _classifier_available = False
            if _classifier_available:
                histogram: dict[str, int] = {}
                qid_to_classification: dict[str, Any] = {}
                for rec in unresolved:
                    if not rec.question_id:
                        continue
                    classification = classify_unresolved_qid(
                        trace, rec.question_id, iteration=iteration,
                    )
                    qid_to_classification[rec.question_id] = classification
                    bucket_name = (
                        classification.bucket.name
                        if classification.bucket is not None
                        else "RESOLVED"
                    )
                    histogram[bucket_name] = histogram.get(bucket_name, 0) + 1
                if histogram:
                    counts = " ".join(
                        f"{name}={histogram[name]}"
                        for name in sorted(histogram)
                    )
                    lines.append(f"|    buckets: {counts}")
                else:
                    lines.append("|    buckets: (none)")
                for rec in unresolved:
                    line = _format_record_line(rec)
                    classification = qid_to_classification.get(rec.question_id)
                    if classification and classification.bucket is not None:
                        line = (
                            line
                            + f"  bucket={classification.bucket.name}"
                            + f"  bucket_action={classification.reason}"
                        )
                    lines.append(line)
            else:
                for rec in unresolved:
                    lines.append(_format_record_line(rec))
            continue
        if section == SECTION_NEXT_ACTION:
            next_actions = [
                r.next_action
                for r in sorted_records
                if r.next_action and r.outcome != DecisionOutcome.RESOLVED
            ]
            if next_actions:
                lines.append(f"|    - {next_actions[0]}")
            else:
                lines.append("|    - (no open next action)")
            continue
        # Phase D.5 Task 8: surface alternatives_considered under each
        # chosen record in the three selection-point sections (RCA Cards,
        # AG Decisions, Proposal Survival). Other sections render plain
        # record lines — alternatives are not meaningful for terminal
        # outcomes.
        _ALT_SECTIONS = {
            SECTION_RCA_CARDS,
            SECTION_AG_DECISIONS,
            SECTION_PROPOSAL_SURVIVAL,
        }
        for rec in by_section.get(section, []):
            lines.append(_format_record_line(rec))
            if section in _ALT_SECTIONS:
                alts_line = _format_alternatives_line(rec)
                if alts_line is not None:
                    lines.append(alts_line)

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
