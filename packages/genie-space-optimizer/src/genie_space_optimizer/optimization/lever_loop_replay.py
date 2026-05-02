"""Pure deterministic replay driver for the Lever Loop journey contract.

This module exists so a developer can verify the journey contract holds for a
given set of frozen inputs (eval rows, clusters, strategist responses, AG
outcomes) without calling Genie, the LLM, the warehouse, or any Delta writer.
It is the inner-loop verification surface used by the Phase 3 replay test.
"""

from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)
from genie_space_optimizer.optimization.question_journey_contract import (
    JourneyValidationReport,
    canonical_journey_json,
    validate_question_journeys,
)


@dataclass(frozen=True)
class ReplayResult:
    events: list[QuestionJourneyEvent]
    canonical_json: str
    validation: JourneyValidationReport


def _classify_eval_rows(
    rows: list[dict],
) -> tuple[set[str], set[str], set[str], set[str]]:
    """Partition rows into (already_passing, hard, soft, gt_correction)."""
    already_passing: set[str] = set()
    hard: set[str] = set()
    soft: set[str] = set()
    gt_correction: set[str] = set()
    for r in rows:
        qid = str(r.get("question_id") or "")
        if not qid:
            continue
        rc = str(r.get("result_correctness") or "").lower()
        arb = str(r.get("arbiter") or "").lower()
        if rc == "yes" and arb in ("both_correct",):
            already_passing.add(qid)
        elif rc == "yes" and arb == "genie_correct":
            gt_correction.add(qid)
        elif rc == "no" and arb in ("ground_truth_correct", "neither_correct"):
            # neither_correct is treated as hard for journey purposes.
            hard.add(qid)
        else:
            # Conservative default: soft signal.
            soft.add(qid)
    # Suppress soft-only qids that are also classified hard: hard wins.
    soft -= hard
    return already_passing, hard, soft, gt_correction


def _replay_iteration(
    *,
    iteration_plan: dict,
    events: list[QuestionJourneyEvent],
) -> None:
    """Walk one iteration in the fixture and append journey events."""
    rows = iteration_plan.get("eval_rows") or []
    eval_qids = [str(r.get("question_id") or "") for r in rows]
    eval_qids = [q for q in eval_qids if q]

    already_passing, hard, soft, gt_corr = _classify_eval_rows(rows)

    # Fixtures declare soft_clusters explicitly; promote those qids out of
    # every row-level partition so the journey reflects the cluster's
    # intended classification rather than the row-level rc/arbiter heuristic.
    # Demoting from `already_passing` and `gt_corr` (in addition to `hard`)
    # is required because `_classify_eval_rows` returns mutually-exclusive
    # row-level partitions, but a single qid can be `already_passing` at the
    # row level AND listed in `soft_clusters[*].question_ids` at the
    # fixture level (cycle 8 had 9 such qids × 5 iterations = 45 spurious
    # `soft_signal -> already_passing` violations before this demotion).
    fixture_soft_qids: set[str] = set()
    for c in iteration_plan.get("soft_clusters") or []:
        for q in c.get("question_ids") or []:
            qstr = str(q)
            if qstr:
                fixture_soft_qids.add(qstr)
    if fixture_soft_qids:
        soft.update(fixture_soft_qids)
        hard -= fixture_soft_qids
        already_passing -= fixture_soft_qids
        gt_corr -= fixture_soft_qids

    def _emit(stage, **fields):
        qids = fields.pop("question_ids", None) or []
        qid = fields.pop("question_id", None)
        target = list(qids) if qids else ([qid] if qid else [])
        for q in target:
            qstr = str(q)
            if not qstr:
                continue
            events.append(
                QuestionJourneyEvent(question_id=qstr, stage=stage, **fields)
            )

    # Eval entry events.
    if eval_qids:
        _emit("evaluated", question_ids=sorted(eval_qids))
    for q in already_passing:
        _emit("already_passing", question_id=q)
    for q in soft:
        _emit("soft_signal", question_id=q)
    for q in gt_corr:
        _emit("gt_correction_candidate", question_id=q)

    # Hard clusters.
    for cluster in iteration_plan.get("clusters") or []:
        cid = str(cluster.get("cluster_id") or "")
        rc = str(cluster.get("root_cause") or "")
        for q in cluster.get("question_ids") or []:
            _emit("clustered", question_id=str(q), cluster_id=cid, root_cause=rc)

    # AG assignment + proposed + applied + outcome.
    strategy = iteration_plan.get("strategist_response") or {}
    ag_outcomes = iteration_plan.get("ag_outcomes") or {}
    for ag in strategy.get("action_groups") or []:
        ag_id = str(ag.get("id") or "")
        affected = [str(q) for q in (ag.get("affected_questions") or []) if q]
        for q in affected:
            _emit("ag_assigned", question_id=q, ag_id=ag_id)
        applied_qids: list[str] = []
        for prop in ag.get("patches") or []:
            pid = str(prop.get("proposal_id") or "")
            ptype = str(prop.get("patch_type") or "")
            target_qids = [
                str(q) for q in (prop.get("target_qids") or []) if q
            ]
            for q in target_qids:
                _emit(
                    "proposed", question_id=q,
                    proposal_id=pid, patch_type=ptype,
                    cluster_id=str(prop.get("cluster_id") or ""),
                )
            for q in target_qids:
                _emit("applied", question_id=q, proposal_id=pid, patch_type=ptype)
                applied_qids.append(q)
        outcome = ag_outcomes.get(ag_id, "rolled_back")
        if outcome in ("accepted", "accepted_with_regression_debt", "rolled_back"):
            _emit(outcome, question_ids=affected, ag_id=ag_id)
        elif outcome in ("skipped_no_applied_patches", "skipped_dead_on_arrival"):
            # Recognized "AG made no successful changes" outcomes from
            # harness.py:14658 (skipped_dead_on_arrival) and
            # harness.py:14908 (skipped_no_applied_patches).
            #
            # Emit a terminal `rolled_back` event ONLY for qids that
            # reached `applied` in this AG. For qids that never reached
            # `applied` (e.g., when every patch had target_qids:[], the
            # Cycle 8 reality), no terminal AG event is emitted — their
            # journey legally ends `ag_assigned -> post_eval` per
            # _LEGAL_NEXT[AG_ASSIGNED]. For qids that did reach `applied`
            # (the post-strategist-fix world), `applied -> rolled_back ->
            # post_eval` is legal per _LEGAL_NEXT[APPLIED].
            if applied_qids:
                _emit("rolled_back", question_ids=applied_qids, ag_id=ag_id)

    # Post-eval closer.
    is_passing = {
        str(q) for q in (iteration_plan.get("post_eval_passing_qids") or [])
    }
    was_passing = already_passing
    for q in eval_qids:
        prior = q in was_passing
        after = q in is_passing
        if prior and after:
            transition = "hold_pass"
        elif not prior and after:
            transition = "fail_to_pass"
        elif prior and not after:
            transition = "pass_to_fail"
        else:
            transition = "hold_fail"
        _emit(
            "post_eval", question_id=q,
            was_passing=prior, is_passing=after, transition=transition,
        )


def run_replay(fixture: dict) -> ReplayResult:
    """Replay every iteration in the fixture and return events + report.

    Validation is per-iteration: each iteration's events and ``eval_qids``
    are validated independently, then the per-iteration reports are merged
    into a single composite ``JourneyValidationReport``. This mirrors the
    harness production contract at ``harness.py:16039-16056``, where
    ``_validate_journeys_at_iteration_end`` is called once per iteration
    boundary.

    The flat ``events`` list is preserved across iterations for
    ``canonical_journey_json``, whose output is order-insensitive (it sorts
    by ``(question_id, stage_rank, proposal_id)``).
    """
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyContractViolation,
        JourneyTerminalState,
    )

    events: list[QuestionJourneyEvent] = []
    combined_violations: list[JourneyContractViolation] = []
    combined_missing_qids: list[str] = []
    combined_terminals: dict[str, JourneyTerminalState] = {}

    for it in fixture.get("iterations") or []:
        iter_events: list[QuestionJourneyEvent] = []
        _replay_iteration(iteration_plan=it, events=iter_events)
        iter_eval_qids = {
            str(r.get("question_id") or "")
            for r in (it.get("eval_rows") or [])
            if r.get("question_id")
        }
        report = validate_question_journeys(
            events=iter_events, eval_qids=iter_eval_qids,
        )
        combined_violations.extend(report.violations)
        combined_missing_qids.extend(report.missing_qids)
        # Later iterations' terminal states overwrite earlier ones for the
        # same qid, matching how the harness re-classifies a qid at each
        # iteration boundary.
        combined_terminals.update(report.terminal_state_by_qid)
        events.extend(iter_events)

    composite = JourneyValidationReport(
        is_valid=not combined_violations and not combined_missing_qids,
        missing_qids=tuple(combined_missing_qids),
        violations=combined_violations,
        terminal_state_by_qid=combined_terminals,
    )
    return ReplayResult(
        events=events,
        canonical_json=canonical_journey_json(events=events),
        validation=composite,
    )
