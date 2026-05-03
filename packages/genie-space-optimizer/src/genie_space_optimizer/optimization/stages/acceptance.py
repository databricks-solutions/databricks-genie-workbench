"""Stage 8: Acceptance / Rollback decision (Phase F8).

Wraps the existing ``control_plane.decide_control_plane_acceptance``
gate (with the PR-E ``accepted_pre_arbiter_improvement`` branch) and
the ``decision_emitters.ag_outcome_decision_record`` /
``post_eval_resolution_records`` producers in a typed
``AcceptanceInput`` / ``AgOutcome`` surface so F9 (learning) can read
the slate from a stage-aligned dataclass.

F8 is observability-only: per the plan's Reality Check, the harness's
acceptance loop is intertwined with downstream eval-rerun and journey
emission, and the existing thin ``ag_outcome.py`` / ``post_eval.py``
modules are still imported by harness wiring. Lifting those under F8's
byte-stability gate is high-risk. F8 stands up the typed surface +
ACCEPTANCE_DECIDED + QID_RESOLUTION emission entry; harness wiring +
ag_outcome.py / post_eval.py absorption are deferred to a follow-up
plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.control_plane import (
    decide_control_plane_acceptance,
)
from genie_space_optimizer.optimization.decision_emitters import (
    ag_outcome_decision_record,
    post_eval_resolution_records,
)


STAGE_KEY: str = "acceptance_decision"


@dataclass
class AgOutcomeRecord:
    """Per-AG acceptance outcome record."""

    ag_id: str
    outcome: str  # "accepted" | "accepted_with_regression_debt" | "rolled_back"
    reason_code: str
    target_qids: tuple[str, ...]
    affected_qids: tuple[str, ...]
    content_fingerprints: tuple[str, ...] = ()


@dataclass
class AcceptanceInput:
    """Input to stages.acceptance.decide.

    Field names match the actual ``decide_control_plane_acceptance``
    signature (per the plan's Reality Check appendix).

    ``applied_entries_by_ag`` mirrors F7's apply log: each entry has a
    ``patch`` key carrying the patch dict (with target_qids and
    content_fingerprint).

    ``pre_rows`` / ``post_rows`` are eval-row dicts (with
    ``question_id``, ``result_correctness``, ``arbiter``) — the gate
    derives hard-failure sets via ``hard_failure_qids`` from these.

    ``baseline_pre_arbiter_accuracy`` / ``candidate_pre_arbiter_accuracy``
    enable the PR-E saturation-mode acceptance branch when supplied.
    """

    applied_entries_by_ag: dict[str, tuple[Mapping[str, Any], ...]]
    ags: tuple[Mapping[str, Any], ...]
    baseline_accuracy: float = 0.0
    candidate_accuracy: float = 0.0
    baseline_pre_arbiter_accuracy: float | None = None
    candidate_pre_arbiter_accuracy: float | None = None
    pre_rows: tuple[Mapping[str, Any], ...] = ()
    post_rows: tuple[Mapping[str, Any], ...] = ()
    protected_qids: tuple[str, ...] = ()
    min_gain_pp: float = 0.0
    min_pre_arbiter_gain_pp: float = 2.0
    rca_id_by_cluster: Mapping[str, str] = field(default_factory=dict)
    cluster_by_qid: Mapping[str, str] = field(default_factory=dict)


@dataclass
class AgOutcome:
    """Output of stages.acceptance.decide.

    ``outcomes_by_ag`` maps AG id → AgOutcomeRecord.
    ``qid_resolutions`` maps eval qid → transition string
    (``hold_pass`` / ``fail_to_pass`` / ``hold_fail`` / ``pass_to_fail``).
    ``rolled_back_content_fingerprints`` is the union of every
    rolled-back AG's patch fingerprints — F6's PR-E content-fingerprint
    dedup gate consumes this on the next iteration.
    """

    outcomes_by_ag: dict[str, AgOutcomeRecord]
    qid_resolutions: dict[str, str] = field(default_factory=dict)
    rolled_back_content_fingerprints: set[str] = field(default_factory=set)


def _row_qid(row: Mapping[str, Any]) -> str:
    return str(
        row.get("question_id")
        or row.get("qid")
        or row.get("inputs.question_id")
        or ""
    )


def _passing_qids(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    """Set of qids with result_correctness=yes (passing per the
    benchmark's own ground-truth check; matches control_plane.row_is_passing)."""
    out: set[str] = set()
    for row in rows or ():
        qid = _row_qid(row)
        if not qid:
            continue
        if str(row.get("result_correctness") or "").lower() == "yes":
            out.add(qid)
    return out


def _outcome_string(decision) -> str:
    """Map ControlPlaneAcceptance.accepted + reason_code → journey
    outcome string ("accepted" | "accepted_with_regression_debt" |
    "rolled_back")."""
    if not decision.accepted:
        return "rolled_back"
    if decision.reason_code == "accepted_with_regression_debt":
        return "accepted_with_regression_debt"
    return "accepted"


def _decide_for_ag(
    *,
    ag: Mapping[str, Any],
    inp: AcceptanceInput,
):
    target_qids = tuple(
        str(q) for q in (ag.get("target_qids") or ag.get("affected_questions") or [])
        if str(q)
    )
    return decide_control_plane_acceptance(
        baseline_accuracy=inp.baseline_accuracy,
        candidate_accuracy=inp.candidate_accuracy,
        target_qids=target_qids,
        pre_rows=inp.pre_rows,
        post_rows=inp.post_rows,
        min_gain_pp=inp.min_gain_pp,
        protected_qids=inp.protected_qids,
        baseline_pre_arbiter_accuracy=inp.baseline_pre_arbiter_accuracy,
        candidate_pre_arbiter_accuracy=inp.candidate_pre_arbiter_accuracy,
        min_pre_arbiter_gain_pp=inp.min_pre_arbiter_gain_pp,
    )


def decide(ctx, inp: AcceptanceInput) -> AgOutcome:
    """Stage 8 entry. For each AG, run the control-plane gate; emit
    ACCEPTANCE_DECIDED via ``ag_outcome_decision_record``; emit one
    QID_RESOLUTION per eval qid via ``post_eval_resolution_records``;
    return the typed ``AgOutcome`` slate.

    F8 is observability-only — does NOT modify any harness call site.
    Harness wiring is deferred to a follow-up plan.
    """
    outcomes: dict[str, AgOutcomeRecord] = {}
    rolled_back_fps: set[str] = set()

    for ag in inp.ags:
        ag_id = str(ag.get("id") or ag.get("ag_id") or "")
        if not ag_id:
            continue
        applied_entries = inp.applied_entries_by_ag.get(ag_id, ())
        decision = _decide_for_ag(ag=ag, inp=inp)
        outcome_str = _outcome_string(decision)
        target_qids = tuple(
            str(q) for q in
            (ag.get("target_qids") or ag.get("affected_questions") or [])
            if str(q)
        )
        affected_qids = tuple(
            str(q) for q in (ag.get("affected_questions") or []) if str(q)
        ) or target_qids

        fps = tuple(
            str((e.get("patch") or {}).get("content_fingerprint") or "")
            for e in applied_entries
        )
        fps = tuple(fp for fp in fps if fp)

        outcomes[ag_id] = AgOutcomeRecord(
            ag_id=ag_id,
            outcome=outcome_str,
            reason_code=str(decision.reason_code),
            target_qids=target_qids,
            affected_qids=affected_qids,
            content_fingerprints=fps,
        )

        if outcome_str == "rolled_back":
            rolled_back_fps.update(fps)

        record = ag_outcome_decision_record(
            run_id=ctx.run_id,
            iteration=ctx.iteration,
            ag=ag,
            outcome=outcome_str,
            rca_id_by_cluster=inp.rca_id_by_cluster,
            regression_qids=decision.out_of_target_regressed_qids,
        )
        if record is not None:
            ctx.decision_emit(record)

    # QID_RESOLUTION emission per eval qid.
    eval_qids: list[str] = []
    seen: set[str] = set()
    for row in (inp.pre_rows or ()):
        qid = _row_qid(row)
        if qid and qid not in seen:
            seen.add(qid)
            eval_qids.append(qid)
    for row in (inp.post_rows or ()):
        qid = _row_qid(row)
        if qid and qid not in seen:
            seen.add(qid)
            eval_qids.append(qid)

    prior_passing = _passing_qids(inp.pre_rows)
    post_passing = _passing_qids(inp.post_rows)

    records = post_eval_resolution_records(
        run_id=ctx.run_id,
        iteration=ctx.iteration,
        eval_qids=eval_qids,
        prior_passing_qids=prior_passing,
        post_passing_qids=post_passing,
        cluster_by_qid=inp.cluster_by_qid,
        rca_id_by_cluster=inp.rca_id_by_cluster,
    )
    for r in records:
        ctx.decision_emit(r)

    qid_resolutions: dict[str, str] = {}
    for q in eval_qids:
        prior = q in prior_passing
        after = q in post_passing
        if prior and after:
            qid_resolutions[q] = "hold_pass"
        elif not prior and after:
            qid_resolutions[q] = "fail_to_pass"
        elif prior and not after:
            qid_resolutions[q] = "pass_to_fail"
        else:
            qid_resolutions[q] = "hold_fail"

    return AgOutcome(
        outcomes_by_ag=outcomes,
        qid_resolutions=qid_resolutions,
        rolled_back_content_fingerprints=rolled_back_fps,
    )
