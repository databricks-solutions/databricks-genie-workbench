"""Phase B DecisionRecord producer helpers.

Cycle-9 postmortem (run 894992655057610) showed that the only
``DecisionRecord`` producer in the harness — the patch-cap site — fires
at most once per iteration and only when proposals survive to the cap.
For runs where every AG hits ``skipped_no_applied_patches``, the harness
captured zero records and Phase B persistence was a silent no-op.

This module adds five producer helpers that emit typed records at the
journey-emit hook points already wired into ``harness.py``:

* ``eval_classification_records`` — one ``EVAL_CLASSIFIED`` per qid.
* ``cluster_records`` — one ``CLUSTER_SELECTED`` per hard cluster.
* ``strategist_ag_records`` — one ``STRATEGIST_AG_EMITTED`` per AG.
* ``ag_outcome_decision_record`` — one ``ACCEPTANCE_DECIDED`` per AG outcome.
* ``post_eval_resolution_records`` — one ``QID_RESOLUTION`` per qid.

All producers are pure functions (return ``list[DecisionRecord]``;
``ag_outcome_decision_record`` returns a single record). They populate
the RCA-grounding contract fields wherever the upstream input supplies
them and use sensible synthesised defaults for ``observed_effect`` /
``next_action`` so every transcript line carries an operator-actionable
next step.

The harness wraps each call in a ``try/except`` that increments a
per-iteration ``producer_exceptions`` counter; the failure surfaces in
the Phase B manifest via ``loop_out["phase_b"]["producer_exceptions"]``.
Set ``GSO_DECISION_EMITTER_STRICT=1`` in the environment to make
producer wrappers re-raise instead of swallow — used by tests.

Plan: ``docs/2026-05-02-unified-trace-and-operator-transcript-plan.md``
+ postmortem follow-up at ``docs/runid_analysis/1036606061019898_894992655057610_analysis.md``.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionRecord,
    DecisionType,
    ReasonCode,
)


PHASE_B_CONTRACT_VERSION: str = "v1"
"""Bump on incompatible Phase B contract changes.

Sourced from this single constant by both the MLflow tag set at
``_run_lever_loop`` start and the manifest field on
``loop_out["phase_b"]["contract_version"]``. Keeping them in lockstep
prevents drift on a future bump.
"""


class NoRecordsReason(str, Enum):
    """Closed vocabulary for the ``GSO_PHASE_B_NO_RECORDS_V1`` marker.

    Pinned now (rather than ad-hoc strings) so the postmortem analyzer
    can reliably pivot on the reason without parsing free-form text.
    """

    NO_CLUSTERS = "no_clusters"
    NO_AGS_EMITTED = "no_ags_emitted"
    ALL_AGS_DROPPED_AT_GROUNDING = "all_ags_dropped_at_grounding"
    PATCH_CAP_DID_NOT_FIRE = "patch_cap_did_not_fire"
    PRODUCER_EXCEPTION = "producer_exception"
    UNKNOWN = "unknown"


def is_strict_mode() -> bool:
    """Return True when GSO_DECISION_EMITTER_STRICT=1 is set.

    Used by the harness wrappers around producer calls. Strict mode
    re-raises producer exceptions so test failures from wiring bugs are
    obvious; production runs use best-effort logging instead.
    """
    return str(os.environ.get("GSO_DECISION_EMITTER_STRICT", "")).strip() in {
        "1",
        "true",
        "True",
        "TRUE",
    }


# ---------------------------------------------------------------------------
# Eval-time classification — EVAL_CLASSIFIED + CLUSTER_SELECTED
# ---------------------------------------------------------------------------


_EVAL_REASON_BY_PARTITION: Mapping[str, ReasonCode] = {
    "already_passing": ReasonCode.ALREADY_PASSING,
    "hard": ReasonCode.HARD_FAILURE,
    "soft": ReasonCode.SOFT_SIGNAL,
    "gt_correction": ReasonCode.GT_CORRECTION,
}


def eval_classification_records(
    *,
    run_id: str,
    iteration: int,
    eval_qids: Sequence[str],
    classification: Mapping[str, str],
    cluster_by_qid: Mapping[str, str] | None = None,
) -> list[DecisionRecord]:
    """One ``EVAL_CLASSIFIED`` ``DecisionRecord`` per evaluated qid.

    EVAL_CLASSIFIED is the broadest decision type; the cross-checker
    treats it as exempt from ``rca_id`` / ``root_cause`` (the qid hasn't
    been routed to an RCA yet) but still requires ``evidence_refs``.

    Args:
        run_id: Optimizer run id.
        iteration: 1-indexed iteration number.
        eval_qids: All qids that entered the evaluation this iteration.
        classification: ``{qid: "already_passing" | "hard" | "soft" | "gt_correction"}``.
            Qids not in the map are skipped (defensive).
        cluster_by_qid: Optional ``{qid: cluster_id}`` for hard-cluster qids.
            When supplied, the resulting ``EVAL_CLASSIFIED`` carries
            ``cluster_id`` so the analyzer can correlate to the matching
            ``CLUSTER_SELECTED`` record without re-deriving the partition.

    Returns:
        One ``DecisionRecord`` per qid present in ``classification``.
    """
    cluster_lookup = dict(cluster_by_qid or {})
    records: list[DecisionRecord] = []
    for qid in eval_qids:
        qstr = str(qid or "")
        if not qstr:
            continue
        partition = str(classification.get(qstr, "")).lower()
        if not partition:
            continue
        reason = _EVAL_REASON_BY_PARTITION.get(partition, ReasonCode.NONE)
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.EVAL_CLASSIFIED,
                outcome=DecisionOutcome.INFO,
                reason_code=reason,
                question_id=qstr,
                cluster_id=str(cluster_lookup.get(qstr) or ""),
                evidence_refs=(f"eval:{qstr}",),
                target_qids=(qstr,),
                expected_effect=(
                    f"Qid {qstr} classified as {partition}; downstream stages "
                    "decide whether to act on it."
                ),
                affected_qids=(qstr,),
            )
        )
    return records


def cluster_records(
    *,
    run_id: str,
    iteration: int,
    clusters: Sequence[Mapping[str, Any]],
    rca_id_by_cluster: Mapping[str, str] | None = None,
) -> list[DecisionRecord]:
    """One ``CLUSTER_SELECTED`` ``DecisionRecord`` per hard cluster.

    Args:
        clusters: Hard cluster dicts as recorded in the iteration snapshot
            (must carry ``cluster_id``, ``question_ids``, and
            ``root_cause``).
        rca_id_by_cluster: Optional ``{cluster_id: rca_id}`` lookup. When a
            cluster has been routed to an RCA card, that ``rca_id`` is
            stamped on the record. Otherwise empty (the cross-checker
            already requires it for CLUSTER_SELECTED, so a missing
            ``rca_id`` will surface as a wiring violation — desired
            behavior).
    """
    rca_lookup = dict(rca_id_by_cluster or {})
    records: list[DecisionRecord] = []
    for cluster in clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        if not cid:
            continue
        qids = tuple(
            str(q) for q in (cluster.get("question_ids") or []) if str(q)
        )
        root_cause = str(cluster.get("root_cause") or "")
        rca_id = str(rca_lookup.get(cid) or "")
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.CLUSTER_SELECTED,
                outcome=DecisionOutcome.INFO,
                reason_code=ReasonCode.CLUSTERED,
                cluster_id=cid,
                rca_id=rca_id,
                root_cause=root_cause,
                evidence_refs=(f"cluster:{cid}",),
                affected_qids=qids,
                target_qids=qids,
                expected_effect=(
                    f"Strategist should emit an AG that resolves {root_cause} "
                    f"for {len(qids)} qid(s)."
                ),
                next_action=f"Generate proposals for {cid}.",
            )
        )
    return records


# ---------------------------------------------------------------------------
# Strategist AG emission — STRATEGIST_AG_EMITTED
# ---------------------------------------------------------------------------


def strategist_ag_records(
    *,
    run_id: str,
    iteration: int,
    action_groups: Sequence[Mapping[str, Any]],
    source_clusters_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    rca_id_by_cluster: Mapping[str, str] | None = None,
) -> list[DecisionRecord]:
    """One ``STRATEGIST_AG_EMITTED`` per AG returned by the strategist.

    Args:
        action_groups: AG dicts from ``strategy["action_groups"]``.
        source_clusters_by_id: ``{cluster_id: cluster_dict}`` map used to
            recover ``root_cause`` from the AG's source clusters.
        rca_id_by_cluster: ``{cluster_id: rca_id}`` map.
    """
    cluster_lookup = dict(source_clusters_by_id or {})
    rca_lookup = dict(rca_id_by_cluster or {})
    records: list[DecisionRecord] = []
    for ag in action_groups or []:
        ag_id = str(ag.get("id") or ag.get("ag_id") or "")
        if not ag_id:
            continue
        affected_qids = tuple(
            str(q) for q in (ag.get("affected_questions") or []) if str(q)
        )
        # The AG's directive may carry per-lever target_qids; aggregate
        # them as the AG's overall causal target. Fall back to
        # affected_questions when no narrower scope is present.
        directives = ag.get("lever_directives") or {}
        target_qids: list[str] = []
        if isinstance(directives, Mapping):
            for _lev, directive in directives.items():
                if isinstance(directive, Mapping):
                    for q in (directive.get("target_qids") or []):
                        if str(q):
                            target_qids.append(str(q))
        target_qids_tuple = tuple(dict.fromkeys(target_qids)) or affected_qids
        source_cluster_ids = tuple(
            str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)
        )
        # Pull root_cause + rca_id from the first known source cluster
        # (the AG dict itself sometimes carries ``root_cause_summary``).
        root_cause = str(ag.get("root_cause_summary") or "")
        rca_id = ""
        for cid in source_cluster_ids:
            if not root_cause:
                cluster = cluster_lookup.get(cid) or {}
                root_cause = str(cluster.get("root_cause") or "")
            if not rca_id:
                rca_id = str(rca_lookup.get(cid) or "")
            if root_cause and rca_id:
                break
        # MISSING_TARGET_QIDS is the Cycle-8-Bug-1 signal; the
        # cross-checker already exempts it from the target_qids
        # requirement.
        reason_code = (
            ReasonCode.STRATEGIST_SELECTED
            if target_qids_tuple else ReasonCode.MISSING_TARGET_QIDS
        )
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.STRATEGIST_AG_EMITTED,
                outcome=DecisionOutcome.INFO,
                reason_code=reason_code,
                ag_id=ag_id,
                rca_id=rca_id,
                root_cause=root_cause,
                evidence_refs=tuple(
                    f"cluster:{cid}" for cid in source_cluster_ids
                ),
                affected_qids=affected_qids,
                target_qids=target_qids_tuple,
                source_cluster_ids=source_cluster_ids,
                expected_effect=(
                    f"AG {ag_id} should produce proposals that resolve "
                    f"{root_cause or 'failure pattern'} on "
                    f"{len(target_qids_tuple)} target qid(s)."
                ),
                next_action=(
                    "Emit proposals for AG"
                    if target_qids_tuple
                    else "Diagnose missing target_qids upstream"
                ),
            )
        )
    return records


# ---------------------------------------------------------------------------
# AG outcome — ACCEPTANCE_DECIDED
# ---------------------------------------------------------------------------


_OUTCOME_TO_DECISION: Mapping[str, tuple[DecisionOutcome, ReasonCode, str, str]] = {
    "accepted": (
        DecisionOutcome.ACCEPTED,
        ReasonCode.PATCH_APPLIED,
        "Patches applied; eval improved or held.",
        "Keep accepted patch and proceed to next iteration.",
    ),
    "accepted_with_regression_debt": (
        DecisionOutcome.ACCEPTED,
        ReasonCode.PATCH_APPLIED,
        "Patches applied with bounded regression debt.",
        "Monitor regression_qids; consider follow-up patch.",
    ),
    "rolled_back": (
        DecisionOutcome.ROLLED_BACK,
        ReasonCode.PATCH_SKIPPED,
        "Patches applied but eval regressed; reverted.",
        "Triage rollback reason; consider alternative RCA.",
    ),
    "skipped_no_applied_patches": (
        DecisionOutcome.SKIPPED,
        ReasonCode.NO_APPLIED_PATCHES,
        "Selected patches all dropped by applier.",
        "Inspect applier-decision counts for rejection reasons.",
    ),
    "skipped_dead_on_arrival": (
        DecisionOutcome.SKIPPED,
        ReasonCode.NO_APPLIED_PATCHES,
        "Patches signature-equal to a prior dead-on-arrival bundle.",
        "Force strategist to produce a new patch shape.",
    ),
    "skipped_pre_ag_snapshot_failed": (
        DecisionOutcome.SKIPPED,
        ReasonCode.NONE,
        "Pre-AG snapshot capture failed; AG discarded.",
        "Investigate snapshot capture site for regression.",
    ),
}


def ag_outcome_decision_record(
    *,
    run_id: str,
    iteration: int,
    ag: Mapping[str, Any],
    outcome: str,
    source_clusters_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    rca_id_by_cluster: Mapping[str, str] | None = None,
    regression_qids: Sequence[str] | None = None,
) -> DecisionRecord | None:
    """One ``ACCEPTANCE_DECIDED`` ``DecisionRecord`` for one AG outcome.

    Args:
        ag: The AG dict (must carry ``id``, ``affected_questions``).
        outcome: One of ``accepted``, ``accepted_with_regression_debt``,
            ``rolled_back``, ``skipped_no_applied_patches``,
            ``skipped_dead_on_arrival``, ``skipped_pre_ag_snapshot_failed``.
            Returns ``None`` for unknown outcome strings (defensive — the
            harness should never call with one).
    """
    ag_id = str(ag.get("id") or ag.get("ag_id") or "")
    if not ag_id:
        return None
    mapping = _OUTCOME_TO_DECISION.get(str(outcome).strip().lower())
    if not mapping:
        return None
    decision_outcome, reason_code, observed_effect, next_action = mapping

    cluster_lookup = dict(source_clusters_by_id or {})
    rca_lookup = dict(rca_id_by_cluster or {})

    affected_qids = tuple(
        str(q) for q in (ag.get("affected_questions") or []) if str(q)
    )
    source_cluster_ids = tuple(
        str(cid) for cid in (ag.get("source_cluster_ids") or []) if str(cid)
    )
    root_cause = str(ag.get("root_cause_summary") or "")
    rca_id = ""
    for cid in source_cluster_ids:
        if not root_cause:
            cluster = cluster_lookup.get(cid) or {}
            root_cause = str(cluster.get("root_cause") or "")
        if not rca_id:
            rca_id = str(rca_lookup.get(cid) or "")
        if root_cause and rca_id:
            break
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.ACCEPTANCE_DECIDED,
        outcome=decision_outcome,
        reason_code=reason_code,
        ag_id=ag_id,
        rca_id=rca_id,
        root_cause=root_cause,
        evidence_refs=tuple(f"cluster:{cid}" for cid in source_cluster_ids),
        affected_qids=affected_qids,
        target_qids=affected_qids,
        regression_qids=tuple(
            str(q) for q in (regression_qids or ()) if str(q)
        ),
        source_cluster_ids=source_cluster_ids,
        expected_effect=(
            f"AG {ag_id} should land patches that improve "
            f"{len(affected_qids)} target qid(s)."
        ),
        observed_effect=observed_effect,
        next_action=next_action,
    )


# ---------------------------------------------------------------------------
# Post-eval qid resolution — QID_RESOLUTION
# ---------------------------------------------------------------------------


_TRANSITION_TO_REASON: Mapping[str, ReasonCode] = {
    "hold_pass": ReasonCode.POST_EVAL_HOLD_PASS,
    "fail_to_pass": ReasonCode.POST_EVAL_FAIL_TO_PASS,
    "hold_fail": ReasonCode.POST_EVAL_HOLD_FAIL,
    "pass_to_fail": ReasonCode.POST_EVAL_PASS_TO_FAIL,
}


def post_eval_resolution_records(
    *,
    run_id: str,
    iteration: int,
    eval_qids: Sequence[str],
    prior_passing_qids: Sequence[str] | set[str],
    post_passing_qids: Sequence[str] | set[str],
    cluster_by_qid: Mapping[str, str] | None = None,
    rca_id_by_cluster: Mapping[str, str] | None = None,
) -> list[DecisionRecord]:
    """One ``QID_RESOLUTION`` ``DecisionRecord`` per evaluated qid.

    Reason-code semantics:

    * ``POST_EVAL_HOLD_PASS`` (rca-exempt) — qid was passing before AND
      after. It was never clustered, so claiming an ``rca_id`` would be a
      lie. The cross-checker exempts this reason from rca-required.
    * ``POST_EVAL_FAIL_TO_PASS`` — qid was failing, now passes. The
      record carries the rca_id of the cluster the qid belonged to (if
      any), so the post-eval improvement attributes to a specific RCA.
    * ``POST_EVAL_HOLD_FAIL`` — qid was failing, still fails. Carries
      rca_id from its cluster.
    * ``POST_EVAL_PASS_TO_FAIL`` — qid regressed. Carries rca_id from its
      cluster (regressions are usually collateral from a different RCA's
      patch; the rca_id here identifies *this* qid's home cluster, not
      the cause of the regression — the cause requires a separate
      attribution chain).
    """
    prior_set = {str(q) for q in (prior_passing_qids or ()) if str(q)}
    post_set = {str(q) for q in (post_passing_qids or ()) if str(q)}
    cluster_lookup = dict(cluster_by_qid or {})
    rca_lookup = dict(rca_id_by_cluster or {})

    records: list[DecisionRecord] = []
    for qid in eval_qids:
        qstr = str(qid or "")
        if not qstr:
            continue
        was_passing = qstr in prior_set
        is_passing = qstr in post_set
        if was_passing and is_passing:
            transition = "hold_pass"
        elif not was_passing and is_passing:
            transition = "fail_to_pass"
        elif was_passing and not is_passing:
            transition = "pass_to_fail"
        else:
            transition = "hold_fail"
        reason_code = _TRANSITION_TO_REASON.get(transition, ReasonCode.NONE)
        outcome = (
            DecisionOutcome.RESOLVED
            if transition in {"hold_pass", "fail_to_pass"}
            else DecisionOutcome.UNRESOLVED
        )
        cluster_id = str(cluster_lookup.get(qstr) or "")
        # Held-pass qids were never clustered → no rca_id (and the
        # cross-checker exempts POST_EVAL_HOLD_PASS from rca-required).
        # Other transitions carry the cluster's rca_id when known.
        rca_id = ""
        if transition != "hold_pass" and cluster_id:
            rca_id = str(rca_lookup.get(cluster_id) or "")
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.QID_RESOLUTION,
                outcome=outcome,
                reason_code=reason_code,
                question_id=qstr,
                cluster_id=cluster_id,
                rca_id=rca_id,
                root_cause="",
                evidence_refs=(f"post_eval:{qstr}",),
                affected_qids=(qstr,),
                target_qids=(qstr,) if transition != "hold_pass" else (),
                expected_effect=(
                    f"Patch should change {qstr} from "
                    f"{'pass' if was_passing else 'fail'} to "
                    f"{'pass' if is_passing else 'fail'}."
                ),
                observed_effect=(
                    f"Qid {qstr} {transition} (was_passing={was_passing}, "
                    f"is_passing={is_passing})."
                ),
                next_action=(
                    "Continue"
                    if transition in {"hold_pass", "fail_to_pass"}
                    else "Triage why qid did not improve."
                ),
            )
        )
    return records


# ---------------------------------------------------------------------------
# No-records reason classification
# ---------------------------------------------------------------------------


def classify_no_records_reason(
    *,
    iteration_inputs: Mapping[str, Any],
    producer_exceptions: Mapping[str, int],
) -> NoRecordsReason:
    """Pick the closest-fit ``NoRecordsReason`` for an empty iteration.

    Used when ``_decision_records`` is empty after an iteration so the
    Phase B no-records marker carries a stable reason rather than a
    free-form string. Order matters: producer-exception is the most
    specific signal (a producer failed silently), so it wins over
    structural reasons.
    """
    if any(int(v) > 0 for v in (producer_exceptions or {}).values()):
        return NoRecordsReason.PRODUCER_EXCEPTION
    clusters = iteration_inputs.get("clusters") or []
    if not clusters:
        return NoRecordsReason.NO_CLUSTERS
    strategy = iteration_inputs.get("strategist_response") or {}
    action_groups = strategy.get("action_groups") if isinstance(strategy, Mapping) else None
    if not action_groups:
        return NoRecordsReason.NO_AGS_EMITTED
    ag_outcomes = iteration_inputs.get("ag_outcomes") or {}
    # If every AG hit a "skipped" outcome before reaching the cap, we're
    # in the all-AGs-dropped-at-grounding regime (or its cousin,
    # skipped_dead_on_arrival).
    skipped_prefixes = ("skipped_",)
    if ag_outcomes and all(
        str(v).lower().startswith(skipped_prefixes)
        for v in ag_outcomes.values()
    ):
        return NoRecordsReason.ALL_AGS_DROPPED_AT_GROUNDING
    return NoRecordsReason.PATCH_CAP_DID_NOT_FIRE
