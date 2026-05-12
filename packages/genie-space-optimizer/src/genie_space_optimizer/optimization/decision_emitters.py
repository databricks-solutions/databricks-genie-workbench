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
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
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
    cluster_alternatives_by_id: (
        Mapping[str, Sequence[AlternativeOption]] | None
    ) = None,
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
    alt_lookup = dict(cluster_alternatives_by_id or {})
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
                alternatives_considered=tuple(alt_lookup.get(cid) or ()),
            )
        )
    return records


# ---------------------------------------------------------------------------
# RCA formed — RCA_FORMED
# ---------------------------------------------------------------------------


def rca_formed_records(
    *,
    run_id: str,
    iteration: int,
    clusters: Sequence[Mapping[str, Any]],
    rca_id_by_cluster: Mapping[str, str],
) -> list[DecisionRecord]:
    """One ``RCA_FORMED`` ``DecisionRecord`` per cluster routed to an RCA card.

    Phase B delta Task 2: closes the gap between ``CLUSTER_SELECTED``
    (the cluster was identified) and ``STRATEGIST_AG_EMITTED`` (the AG
    was generated). Without this record, postmortem readers can't tell
    which clusters made it through the RCA layer vs which were dropped,
    and the trace skips the link between cluster and AG.

    Skips clusters with no rca_id — those are emitted upstream as
    CLUSTER_SELECTED with empty rca_id, which is the existing
    cross-checker violation that surfaces the gap.
    """
    rca_lookup = dict(rca_id_by_cluster or {})
    records: list[DecisionRecord] = []
    for cluster in clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        if not cid:
            continue
        rca_id = str(rca_lookup.get(cid) or "")
        if not rca_id:
            continue
        qids = tuple(
            str(q) for q in (cluster.get("question_ids") or []) if str(q)
        )
        root_cause = str(cluster.get("root_cause") or "")
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.RCA_FORMED,
                outcome=DecisionOutcome.INFO,
                reason_code=ReasonCode.RCA_GROUNDED,
                cluster_id=cid,
                rca_id=rca_id,
                root_cause=root_cause,
                evidence_refs=(f"cluster:{cid}", f"rca:{rca_id}"),
                affected_qids=qids,
                target_qids=qids,
                source_cluster_ids=(cid,),
                expected_effect=(
                    f"RCA {rca_id} provides causal grounding for "
                    f"{root_cause or 'failure pattern'} on "
                    f"{len(qids)} qid(s)."
                ),
                next_action=(
                    f"Strategist should emit AG targeting {cid}"
                ),
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
    ag_alternatives_by_id: (
        Mapping[str, Sequence[AlternativeOption]] | None
    ) = None,
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
    alt_lookup = dict(ag_alternatives_by_id or {})
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
                alternatives_considered=tuple(alt_lookup.get(ag_id) or ()),
            )
        )
    return records


# ---------------------------------------------------------------------------
# Proposal generated — PROPOSAL_GENERATED
# ---------------------------------------------------------------------------


def proposal_generated_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    proposals: Sequence[Mapping[str, Any]],
    rca_id_by_cluster: Mapping[str, str],
    cluster_root_cause_by_id: Mapping[str, str],
    proposal_alternatives_for_ag: (
        Sequence[AlternativeOption] | None
    ) = None,
) -> list[DecisionRecord]:
    """One ``PROPOSAL_GENERATED`` ``DecisionRecord`` per surviving proposal.

    Phase B delta Task 4: parallels the existing ``_journey_emit
    ("proposed", ...)`` site so the decision trace records every proposal
    that survived to ``proposals_to_patches``. Proposals without
    target_qids (the Cycle-8-Bug-1 pattern) are dropped here — they
    cannot satisfy the cross-checker's RCA-grounding contract, and the
    strategist-AG record's ``MISSING_TARGET_QIDS`` reason already
    surfaces the gap upstream.
    """
    rca_lookup = dict(rca_id_by_cluster or {})
    root_cause_lookup = dict(cluster_root_cause_by_id or {})
    alternatives_tuple = tuple(proposal_alternatives_for_ag or ())
    records: list[DecisionRecord] = []
    for proposal in proposals or []:
        bare_id = str(
            proposal.get("proposal_id")
            or proposal.get("id")
            or ""
        )
        if not bare_id:
            continue
        # Cycle 6 F-4 — prefer expanded_patch_id (lever-qualified, e.g.
        # "L1:P001#1") so cross-lever patches sharing a parent
        # ``proposal_id`` ("P001" under L1, L5, L6) emit distinct
        # canonical ids. The bare id is preserved on
        # ``metrics.parent_proposal_id`` for legitimate parent-grouping
        # (N1 lane semantics intentionally group patches sharing a
        # parent).
        expanded_id = str(proposal.get("expanded_patch_id") or "")
        canonical_id = expanded_id or bare_id
        target_qids = tuple(
            str(q) for q in (proposal.get("_grounding_target_qids") or []) if str(q)
        )
        if not target_qids:
            target_qids = tuple(
                str(q) for q in (proposal.get("target_qids") or []) if str(q)
            )
        if not target_qids:
            continue
        cluster_id = str(proposal.get("cluster_id") or "")
        rca_id = str(rca_lookup.get(cluster_id) or "")
        root_cause = str(root_cause_lookup.get(cluster_id) or "")
        patch_type = str(
            proposal.get("patch_type") or proposal.get("type") or ""
        )
        evidence_refs = tuple(
            v for v in (
                f"ag:{ag_id}" if ag_id else "",
                f"cluster:{cluster_id}" if cluster_id else "",
                f"rca:{rca_id}" if rca_id else "",
            ) if v
        )
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.PROPOSAL_GENERATED,
                outcome=DecisionOutcome.ACCEPTED,
                reason_code=ReasonCode.PROPOSAL_EMITTED,
                ag_id=str(ag_id or ""),
                cluster_id=cluster_id,
                rca_id=rca_id,
                root_cause=root_cause,
                proposal_id=canonical_id,
                proposal_ids=(canonical_id,),
                evidence_refs=evidence_refs,
                affected_qids=target_qids,
                target_qids=target_qids,
                source_cluster_ids=(cluster_id,) if cluster_id else (),
                expected_effect=(
                    f"Proposal {canonical_id} ({patch_type}) should "
                    f"resolve {root_cause or 'failure pattern'} on "
                    f"{len(target_qids)} target qid(s)."
                ),
                next_action="Apply proposal and observe target qid outcome.",
                metrics={
                    "patch_type": patch_type,
                    "parent_proposal_id": bare_id,
                },
                alternatives_considered=alternatives_tuple,
            )
        )
    return records


def proposal_generation_empty_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str = "",
    rca_id: str = "",
    root_cause: str = "",
    target_qids: tuple[str, ...] = (),
) -> DecisionRecord:
    """P4 — emit one PROPOSAL_GENERATED/DROPPED record when the
    proposer returns zero proposals for an AG.

    Different from STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY (proposal
    existed but was dropped) and NO_STRUCTURAL_CANDIDATE (synthesis
    attempted but no fallback). Reproducer: run 2423b960 iter 3 and
    iter 4 each emitted ``Proposals (0 total)`` for AG_COVERAGE_H001
    with no gate-drop reason — the proposer simply returned nothing.
    """
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
            f"rca:{rca_id}" if rca_id else "",
        ) if v
    )
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.DROPPED,
        reason_code=ReasonCode.PROPOSAL_GENERATION_EMPTY,
        ag_id=str(ag_id or ""),
        cluster_id=str(cluster_id or ""),
        rca_id=str(rca_id or ""),
        root_cause=str(root_cause or ""),
        proposal_id="",
        proposal_ids=(),
        evidence_refs=evidence_refs,
        affected_qids=target_qids,
        target_qids=target_qids,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        expected_effect=(
            f"AG {ag_id} produced zero proposals; root cause "
            f"{root_cause or '(none)'} did not match any patch shape."
        ),
        next_action=(
            "Regenerate RCA, broaden lever set, or escalate to "
            "operator review."
        ),
        metrics={"proposals_total": 0},
    )


def no_structural_candidate_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str = "",
    rca_id: str = "",
    root_cause: str = "",
    target_qids: tuple[str, ...] = (),
    attempted_archetypes: tuple[str, ...] = (),
) -> DecisionRecord:
    """P4 — emit one PROPOSAL_GENERATED/DROPPED record when synthesis
    was attempted (lever-5 structural gate fired and a structural
    fallback path was invoked) but no archetype produced a viable
    structural candidate.

    Distinct from PROPOSAL_GENERATION_EMPTY (proposer never tried
    synthesis) and STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY (gate
    dropped a non-structural proposal but no fallback was attempted).
    """
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
            f"rca:{rca_id}" if rca_id else "",
        ) if v
    )
    archetypes_str = (
        ",".join(attempted_archetypes) if attempted_archetypes else "(none)"
    )
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.DROPPED,
        reason_code=ReasonCode.NO_STRUCTURAL_CANDIDATE,
        ag_id=str(ag_id or ""),
        cluster_id=str(cluster_id or ""),
        rca_id=str(rca_id or ""),
        root_cause=str(root_cause or ""),
        proposal_id="",
        proposal_ids=(),
        evidence_refs=evidence_refs,
        affected_qids=target_qids,
        target_qids=target_qids,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        gate="cluster_driven_synthesis",
        reason_detail=f"attempted_archetypes={archetypes_str}",
        expected_effect=(
            f"Synthesis attempted for {root_cause or '(unknown root cause)'}; "
            f"no archetype produced a structural candidate."
        ),
        next_action=(
            "Regenerate RCA with sharper grounding, or escalate to "
            "operator review."
        ),
        metrics={"proposals_total": 0, "synthesis_attempted": True},
    )


def lever6_forced_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str = "",
    rca_id: str = "",
    root_cause: str = "",
    target_qids: tuple[str, ...] = (),
    recommended_levers: tuple[int, ...] = (),
    existing_patch_types: tuple[str, ...] = (),
) -> DecisionRecord:
    """Cycle 7 N3 — emit one PROPOSAL_GENERATED/INFO record when the
    harness forces a Lever-6 candidate onto an AG whose cluster has
    a SQL-shape root cause and the strategist did not propose any
    add_sql_snippet_* patch.

    INFO outcome (not DROPPED, not ACCEPTED): the forced L6 candidate
    is appended to the AG's proposal slate and flows through the
    existing safety gates. This record documents the *force-emit*
    decision; the candidate's ultimate fate is captured by the
    downstream gate / acceptance records.
    """
    levers_str = (
        ",".join(str(int(L)) for L in recommended_levers)
        if recommended_levers
        else "(none)"
    )
    patches_str = (
        ",".join(str(p) for p in existing_patch_types)
        if existing_patch_types
        else "(empty)"
    )
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
            f"rca:{rca_id}" if rca_id else "",
        ) if v
    )
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.LEVER6_FORCED_FOR_SQL_SHAPE_RCA,
        ag_id=str(ag_id or ""),
        cluster_id=str(cluster_id or ""),
        rca_id=str(rca_id or ""),
        root_cause=str(root_cause or ""),
        proposal_id="",
        proposal_ids=(),
        evidence_refs=evidence_refs,
        affected_qids=target_qids,
        target_qids=target_qids,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        gate="proposal_generation",
        reason_detail=(
            f"recommended_levers={levers_str};"
            f" existing_patch_types={patches_str}"
        ),
        expected_effect=(
            "Forced add_sql_snippet_* candidate appended to AG to "
            "close run-to-run variance on SQL-shape hard failures."
        ),
        next_action=(
            "Generated L6 candidate flows through normal blast_radius "
            "and leakage gates."
        ),
        metrics={"forced_lever6_candidates": 1},
    )


# ---------------------------------------------------------------------------
# Patch applied — PATCH_APPLIED
# ---------------------------------------------------------------------------


def patch_applied_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    applied_entries: Sequence[Mapping[str, Any]],
    rca_id_by_cluster: Mapping[str, str],
    cluster_root_cause_by_id: Mapping[str, str],
) -> list[DecisionRecord]:
    """One ``PATCH_APPLIED`` ``DecisionRecord`` per applied entry.

    Phase B delta Task 6: parallels the harness's
    ``_journey_emit("applied_targeted", ...)`` site. Each
    ``applied_entries`` element is the ``apply_log["applied"][i]`` dict
    whose ``"patch"`` key carries the actual patch dictionary.
    """
    rca_lookup = dict(rca_id_by_cluster or {})
    root_cause_lookup = dict(cluster_root_cause_by_id or {})
    records: list[DecisionRecord] = []
    for entry in applied_entries or []:
        patch = entry.get("patch") or {}
        if not isinstance(patch, Mapping):
            continue
        proposal_id = str(
            patch.get("proposal_id")
            or patch.get("expanded_patch_id")
            or patch.get("id")
            or ""
        )
        if not proposal_id:
            continue
        target_qids = tuple(
            str(q) for q in (patch.get("_grounding_target_qids") or []) if str(q)
        )
        if not target_qids:
            target_qids = tuple(
                str(q) for q in (patch.get("target_qids") or []) if str(q)
            )
        if not target_qids:
            continue
        cluster_id = str(patch.get("cluster_id") or "")
        rca_id = str(rca_lookup.get(cluster_id) or "")
        root_cause = str(root_cause_lookup.get(cluster_id) or "")
        patch_type = str(patch.get("patch_type") or patch.get("type") or "")
        evidence_refs = tuple(
            v for v in (
                f"ag:{ag_id}" if ag_id else "",
                f"cluster:{cluster_id}" if cluster_id else "",
                f"rca:{rca_id}" if rca_id else "",
            ) if v
        )
        records.append(
            DecisionRecord(
                run_id=run_id,
                iteration=int(iteration),
                decision_type=DecisionType.PATCH_APPLIED,
                outcome=DecisionOutcome.APPLIED,
                reason_code=ReasonCode.PATCH_APPLIED,
                ag_id=str(ag_id or ""),
                cluster_id=cluster_id,
                rca_id=rca_id,
                root_cause=root_cause,
                proposal_id=proposal_id,
                proposal_ids=(proposal_id,),
                evidence_refs=evidence_refs,
                affected_qids=target_qids,
                target_qids=target_qids,
                source_cluster_ids=(cluster_id,) if cluster_id else (),
                expected_effect=(
                    f"Patch {proposal_id} ({patch_type}) should resolve "
                    f"{root_cause or 'failure pattern'} on "
                    f"{len(target_qids)} target qid(s)."
                ),
                observed_effect="Patch applied successfully.",
                next_action="Run post-eval and observe target qid outcomes.",
                metrics={"patch_type": patch_type},
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


def _resolve_acceptance_reason_code(
    cp_reason_code: str,
    *,
    accepted: bool,
) -> ReasonCode:
    """Map a ``ControlPlaneAcceptance.reason_code`` string to a typed
    ``ReasonCode`` enum value.

    Phase H Fidelity Task 3 — preserves the operator-relevant cause of
    rejection (or acceptance variant) instead of collapsing every
    rolled-back outcome into the generic ``PATCH_SKIPPED``. Unknown
    strings degrade to the legacy default for the outcome class
    (``PATCH_APPLIED`` for accepted, ``PATCH_SKIPPED`` for rejected) so
    the emitter never crashes on a future / unrecognised reason.
    """
    raw = str(cp_reason_code or "").strip().lower()
    if not raw:
        return ReasonCode.PATCH_APPLIED if accepted else ReasonCode.PATCH_SKIPPED
    try:
        return ReasonCode(raw)
    except ValueError:
        return ReasonCode.PATCH_APPLIED if accepted else ReasonCode.PATCH_SKIPPED


def _acceptance_detail_metrics(detail: Any) -> dict[str, Any]:
    """Project the per-bucket counts off a ``ControlPlaneAcceptance``-shaped
    object into a flat metrics dict for the operator transcript."""
    if detail is None:
        return {}
    metrics: dict[str, Any] = {}
    for src_attr, dst_key in (
        ("target_qids", "target_qids_count"),
        ("target_fixed_qids", "target_fixed_count"),
        ("target_still_hard_qids", "target_still_hard_count"),
        ("out_of_target_regressed_qids", "out_of_target_regressed_count"),
        ("regression_debt_qids", "regression_debt_count"),
        ("protected_regressed_qids", "protected_regressed_count"),
        ("soft_to_hard_regressed_qids", "soft_to_hard_regressed_count"),
        ("passing_to_hard_regressed_qids", "passing_to_hard_regressed_count"),
        ("unknown_to_hard_regressed_qids", "unknown_to_hard_regressed_count"),
    ):
        try:
            value = getattr(detail, src_attr, None)
        except Exception:
            value = None
        if isinstance(value, (list, tuple, set)):
            metrics[dst_key] = len(value)
    return metrics


def ag_outcome_decision_record(
    *,
    run_id: str,
    iteration: int,
    ag: Mapping[str, Any],
    outcome: str,
    source_clusters_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    rca_id_by_cluster: Mapping[str, str] | None = None,
    regression_qids: Sequence[str] | None = None,
    acceptance_detail: Any = None,
) -> DecisionRecord | None:
    """One ``ACCEPTANCE_DECIDED`` ``DecisionRecord`` for one AG outcome.

    Args:
        ag: The AG dict (must carry ``id``, ``affected_questions``).
        outcome: One of ``accepted``, ``accepted_with_regression_debt``,
            ``rolled_back``, ``skipped_no_applied_patches``,
            ``skipped_dead_on_arrival``, ``skipped_pre_ag_snapshot_failed``.
            Returns ``None`` for unknown outcome strings (defensive — the
            harness should never call with one).
        acceptance_detail: Optional ``ControlPlaneAcceptance`` (or shape-
            compatible object) carrying the rich rejection cause. When
            supplied, ``reason_code`` is upgraded from the generic
            ``PATCH_SKIPPED`` / ``PATCH_APPLIED`` to the typed control-
            plane reason (e.g. ``TARGET_QIDS_NOT_IMPROVED``),
            ``reason_detail`` is populated with
            ``format_control_plane_acceptance_detail(decision)``,
            ``regression_qids`` is sourced from
            ``out_of_target_regressed_qids``, and ``metrics`` carries
            per-bucket counts. This is Phase H Fidelity Task 3 — without
            it, the operator transcript Stage 9 collapses every rollback
            into a single line lacking the actual rejection cause.
    """
    ag_id = str(ag.get("id") or ag.get("ag_id") or "")
    if not ag_id:
        return None
    mapping = _OUTCOME_TO_DECISION.get(str(outcome).strip().lower())
    if not mapping:
        return None
    decision_outcome, reason_code, observed_effect, next_action = mapping

    reason_detail = ""
    metrics: dict[str, Any] = {}
    detail_regression_qids: tuple[str, ...] | None = None
    if acceptance_detail is not None:
        try:
            cp_reason_code = str(getattr(acceptance_detail, "reason_code", "") or "")
            cp_accepted = bool(getattr(acceptance_detail, "accepted", False))
            reason_code = _resolve_acceptance_reason_code(
                cp_reason_code, accepted=cp_accepted,
            )
            # Cycle 14-T2: prefer the canonical helper. On flag-off
            # fall back to the legacy renderer + legacy metrics
            # projection so pre-T2 fixtures replay byte-stable.
            try:
                from genie_space_optimizer.common.config import (
                    canonical_acceptance_render_enabled,
                )
                use_canonical = canonical_acceptance_render_enabled()
            except Exception:
                use_canonical = False
            if use_canonical:
                from genie_space_optimizer.optimization.control_plane import (
                    format_full_eval_marker_payload,
                )
                _ag_id_for_payload = str(
                    ag.get("id") or ag.get("ag_id") or ""
                )
                payload = format_full_eval_marker_payload(
                    acceptance_detail,
                    ag_id=_ag_id_for_payload,
                    iteration=int(iteration),
                    accepted_label=(
                        "PASS -- ACCEPTED" if cp_accepted else "FAIL (REGRESSION)"
                    ),
                )
                reason_detail = str(payload.get("reason_detail") or "")
                # Match the legacy *_count metrics shape so existing
                # transcript readers are byte-stable on the metrics
                # surface, then layer the canonical bucket lists on top.
                metrics = _acceptance_detail_metrics(acceptance_detail)
                metrics.update({
                    k: payload[k]
                    for k in (
                        "target_fixed_qids",
                        "target_still_hard_qids",
                        "out_of_target_regressed_qids",
                        "regression_debt_qids",
                        "soft_to_hard_regressed_qids",
                        "passing_to_hard_regressed_qids",
                        "unknown_to_hard_regressed_qids",
                        "target_delta_states",
                    )
                    if k in payload
                })
                cp_regress = payload.get("out_of_target_regressed_qids") or ()
                if cp_regress:
                    detail_regression_qids = tuple(
                        str(q) for q in cp_regress if str(q)
                    )
            else:
                from genie_space_optimizer.optimization.control_plane import (
                    format_control_plane_acceptance_detail,
                )
                try:
                    reason_detail = format_control_plane_acceptance_detail(
                        acceptance_detail,
                    )
                except Exception:
                    reason_detail = (
                        f"reason={cp_reason_code}" if cp_reason_code else ""
                    )
                metrics = _acceptance_detail_metrics(acceptance_detail)
                cp_regress = getattr(
                    acceptance_detail, "out_of_target_regressed_qids", None,
                )
                if isinstance(cp_regress, (list, tuple, set)):
                    detail_regression_qids = tuple(
                        str(q) for q in cp_regress if str(q)
                    )
        except Exception:
            # Defensive: never let a malformed detail shape crash the
            # emitter — fall back to the legacy reason mapping.
            reason_detail = ""
            metrics = {}
            detail_regression_qids = None

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
    final_regression_qids = (
        detail_regression_qids
        if detail_regression_qids is not None
        else tuple(str(q) for q in (regression_qids or ()) if str(q))
    )
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.ACCEPTANCE_DECIDED,
        outcome=decision_outcome,
        reason_code=reason_code,
        ag_id=ag_id,
        rca_id=rca_id,
        root_cause=root_cause,
        reason_detail=reason_detail,
        evidence_refs=tuple(f"cluster:{cid}" for cid in source_cluster_ids),
        affected_qids=affected_qids,
        target_qids=affected_qids,
        regression_qids=final_regression_qids,
        source_cluster_ids=source_cluster_ids,
        expected_effect=(
            f"AG {ag_id} should land patches that improve "
            f"{len(affected_qids)} target qid(s)."
        ),
        observed_effect=observed_effect,
        next_action=next_action,
        metrics=metrics,
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


def blast_radius_decision_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    rca_id: str,
    root_cause: str,
    target_qids: Sequence[str],
    dropped: Sequence[Mapping[str, Any]],
) -> list[DecisionRecord]:
    """Emit one ``GATE_DECISION`` / ``DROPPED`` record per blast-radius drop.

    Cycle 9 T6: the blast-radius gate runs *before* the patch-cap; the
    patch-cap is the only producer in the existing pipeline, so AGs
    fully dropped by the gate contributed zero ``DecisionRecord`` rows
    and Phase B's operator transcript rendered nothing for that
    iteration. This producer closes that gap.

    ``reason_code=NO_CAUSAL_TARGET`` is the precise semantic of a
    blast-radius drop: the patch would change rows for passing
    dependents outside the AG's target qids — i.e. the patch has no
    causally-clean target.

    Gate-specific signals (``passing_dependents_outside_target``,
    ``target`` table) live in ``metrics`` so the cross-checker's
    RCA-grounding contract still validates against the canonical
    fields.
    """
    cleaned_target_qids = tuple(
        str(q) for q in (target_qids or ()) if str(q)
    )
    records: list[DecisionRecord] = []
    for d in dropped or []:
        proposal_id = str(d.get("proposal_id") or "")
        outside = [
            str(q)
            for q in (d.get("passing_dependents_outside_target") or [])
            if str(q)
        ]
        records.append(
            DecisionRecord(
                run_id=str(run_id),
                iteration=int(iteration),
                ag_id=str(ag_id),
                rca_id=str(rca_id or ""),
                root_cause=str(root_cause or ""),
                proposal_id=proposal_id,
                proposal_ids=(proposal_id,) if proposal_id else (),
                decision_type=DecisionType.GATE_DECISION,
                outcome=DecisionOutcome.DROPPED,
                reason_code=ReasonCode.NO_CAUSAL_TARGET,
                gate="blast_radius",
                reason_detail=str(d.get("reason") or ""),
                evidence_refs=(f"ag:{ag_id}", "blast_radius_gate"),
                target_qids=cleaned_target_qids,
                affected_qids=cleaned_target_qids,
                expected_effect=(
                    f"Patch would address "
                    f"{root_cause or 'failure pattern'} on "
                    f"{len(cleaned_target_qids)} target qid(s)."
                ),
                observed_effect=(
                    f"Dropped: collateral risk on {len(outside)} passing "
                    f"dependent(s) outside target."
                ),
                next_action=(
                    "Add target table to AG forbid_tables and "
                    "re-strategize"
                ),
                metrics={
                    "patch_type": str(d.get("patch_type") or ""),
                    "passing_dependents_outside_target": outside,
                    "target": str(d.get("target") or ""),
                },
            )
        )
    return records


def lever5_structural_gate_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    rca_id: str,
    root_cause: str,
    target_qids: Sequence[str],
    drops: Sequence[Mapping[str, Any]],
) -> list[DecisionRecord]:
    """Emit one ``GATE_DECISION`` / ``DROPPED`` record per Lever 5
    structural-gate drop.

    Cycle 8 Bug 1 Phase 3b Task B: the gate at
    ``optimizer.py:13961-13971`` silently zeroes ``instruction_sections``
    and ``instruction_guidance`` when the dominant cluster root cause is
    SQL-shape (``wrong_aggregation``, ``missing_filter``, ``wrong_join``,
    etc.) but no ``example_sql`` is attached. Today no ``DecisionRecord``
    is emitted, so the operator transcript's
    ``Proposal Survival And Gate Drops`` section renders nothing for the
    most common drop on ``gs_024``.

    ``reason_code=RCA_UNGROUNDED`` is the precise semantic: the proposal
    has no causal grounding for the SQL-shape root cause it claims to
    address (instructions don't change SQL structure).

    Gate-specific signals (``target_lever``, ``root_causes``,
    ``had_example_sqls``, ``instruction_sections_dropped``,
    ``instruction_guidance_dropped``) live in ``metrics`` so the
    cross-checker's RCA-grounding contract still validates against the
    canonical fields.
    """
    cleaned_target_qids = tuple(
        str(q) for q in (target_qids or ()) if str(q)
    )
    records: list[DecisionRecord] = []
    for d in drops or []:
        root_causes = list(d.get("root_causes") or ())
        records.append(
            DecisionRecord(
                run_id=str(run_id),
                iteration=int(iteration),
                ag_id=str(ag_id),
                rca_id=str(rca_id or ""),
                root_cause=str(root_cause or ""),
                decision_type=DecisionType.GATE_DECISION,
                outcome=DecisionOutcome.DROPPED,
                # P4: specific reason — the lever-5 gate dropped a
                # non-structural (instruction-only) proposal because
                # the dominant cluster root cause was SQL-shape and
                # no example_sql was attached. Distinct from the
                # generic RCA_UNGROUNDED case.
                reason_code=ReasonCode.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY,
                gate="lever5_structural_gate",
                reason_detail=f"sql_shape_without_example_sql:{','.join(root_causes)}",
                evidence_refs=(f"ag:{ag_id}", "lever5_structural_gate"),
                target_qids=cleaned_target_qids,
                affected_qids=cleaned_target_qids,
                expected_effect=(
                    f"Lever 5 instruction would address "
                    f"{root_cause or 'failure pattern'} on "
                    f"{len(cleaned_target_qids)} target qid(s)."
                ),
                observed_effect=(
                    f"Dropped: SQL-shape root cause(s) "
                    f"{root_causes} require structural fix; no "
                    f"example_sql attached."
                ),
                next_action=(
                    "Re-route via Lever 6 (sql_snippet) or attach "
                    "example_sql via cluster-driven synthesis"
                ),
                metrics={
                    "target_lever": int(d.get("target_lever") or 5),
                    "root_causes": [str(r) for r in root_causes],
                    "had_example_sqls": bool(d.get("had_example_sqls") or False),
                    "instruction_sections_dropped": bool(
                        d.get("instruction_sections_dropped") or False
                    ),
                    "instruction_guidance_dropped": bool(
                        d.get("instruction_guidance_dropped") or False
                    ),
                    "source_clusters": [
                        str(s) for s in (d.get("source_clusters") or ())
                    ],
                },
            )
        )
    return records


def dead_on_arrival_decision_records(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    rca_id: str,
    root_cause: str,
    target_qids: Sequence[str],
    signature: tuple[str, ...],
    reason: str,
) -> list[DecisionRecord]:
    """Emit one ``PATCH_SKIPPED`` record per dead-on-arrival patch signature.

    Cycle 9 T7: the existing ``ACCEPTANCE_DECIDED`` producer (wired in
    the postmortem follow-up) emits *one* record per AG when the AG hits
    the dead-on-arrival path. This producer adds *finer-grained
    per-signature* records — one ``PATCH_SKIPPED`` per proposal_id in
    the signature — so the operator can distinguish "AG dropped because
    P001#1 was a no-op" from "AG dropped because P002#1 hit applier
    rejection."

    When ``signature`` is empty (all patches dropped before the
    applier), returns an empty list — there's nothing to attribute at
    the per-patch level, and the AG-level ACCEPTANCE_DECIDED record
    already carries the AG-wide signal.
    """
    if not signature:
        return []
    cleaned_target_qids = tuple(
        str(q) for q in (target_qids or ()) if str(q)
    )
    records: list[DecisionRecord] = []
    for proposal_id in signature:
        if not proposal_id:
            continue
        records.append(
            DecisionRecord(
                run_id=str(run_id),
                iteration=int(iteration),
                ag_id=str(ag_id),
                rca_id=str(rca_id or ""),
                root_cause=str(root_cause or ""),
                proposal_id=str(proposal_id),
                proposal_ids=(str(proposal_id),),
                decision_type=DecisionType.PATCH_SKIPPED,
                outcome=DecisionOutcome.SKIPPED,
                reason_code=ReasonCode.NO_APPLIED_PATCHES,
                reason_detail=str(reason or ""),
                evidence_refs=(f"ag:{ag_id}", f"patch:{proposal_id}"),
                target_qids=cleaned_target_qids,
                affected_qids=cleaned_target_qids,
                expected_effect=(
                    f"Patch {proposal_id} would address "
                    f"{root_cause or 'failure pattern'}."
                ),
                observed_effect=(
                    f"Patch dropped before apply: {reason or 'unknown'}."
                ),
                next_action=(
                    "Force strategist to produce a different patch shape"
                ),
                metrics={
                    "signature": list(signature),
                    "recovery_reason": str(reason or ""),
                },
            )
        )
    return records


def producer_exception_record(
    *,
    run_id: str,
    iteration: int,
    producer: str,
    ag_id: str | None,
    exception: BaseException,
) -> DecisionRecord:
    """Cycle 11 — typed PRODUCER_EXCEPTION record for every harness
    producer try/except site.

    Today every producer try/except only increments
    ``_iter_producer_exceptions[<producer>]`` and debug-logs. The
    exception class, message, and traceback head are nowhere in the
    Phase B trace, so postmortems see ``producer_exceptions={...}``
    with no payload. This helper builds a typed ``DecisionRecord``
    the harness appends to ``_current_iter_inputs["decision_records"]``
    *before* the existing counter increment. The strict-mode re-raise
    is unchanged.

    Pure: no I/O, no clock, no logger.
    """
    repr_text = repr(exception)[:512]
    try:
        tb_lines = traceback.format_exception(
            type(exception), exception, exception.__traceback__,
        )
        tb_head = ("".join(tb_lines))[:2048]
    except Exception:
        tb_head = ""
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.PRODUCER_EXCEPTION,
        outcome=DecisionOutcome.FAILED,
        reason_code=ReasonCode.PRODUCER_EXCEPTION,
        reason_detail=f"{type(exception).__name__}: {repr_text}",
        ag_id=str(ag_id or ""),
        evidence_refs=(f"producer:{producer}",),
        metrics={
            "producer": str(producer),
            "exception_class": type(exception).__name__,
            "exception_repr": repr_text,
            "traceback_head": tb_head,
        },
    )


def invariant_violation_record(
    *,
    run_id: str,
    iteration: int,
    violation: Mapping[str, Any],
) -> DecisionRecord:
    """Cycle 11 — wrap a single invariant violation dict in a
    ``DecisionRecord``. The dict shape is whatever ``invariants.py``
    produced; the relevant fields are ``invariant_id``, ``title``,
    and ``detail``.
    """
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.INVARIANT_VIOLATION,
        outcome=DecisionOutcome.FAILED,
        reason_code=ReasonCode.INVARIANT_VIOLATION,
        reason_detail=str(violation.get("detail") or ""),
        evidence_refs=(f"invariant:{violation.get('invariant_id', '')}",),
        metrics={
            "invariant_id": str(violation.get("invariant_id") or ""),
            "title": str(violation.get("title") or ""),
            **{k: v for k, v in violation.items()
               if k not in {"invariant_id", "title", "detail"}},
        },
    )


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


def rca_id_by_cluster_from_findings(
    *,
    clusters: Sequence[Mapping[str, Any]],
    findings: Sequence[Any],
) -> dict[str, str]:
    """Derive ``{cluster_id: rca_id}`` from clusters and RCA findings.

    Phase B delta Task 1: the harness used to initialize this map to
    ``{}`` because the cluster dicts at the cluster-build site do not
    carry ``rca_id`` directly. This helper is the single source of
    truth for the derivation; ``harness.py`` imports it.

    Strategy: for each cluster, find the first finding whose
    ``target_qids`` intersect the cluster's ``question_ids``. Tolerates
    both dataclass findings (``rca_id``/``target_qids`` attributes) and
    dict findings (``rca_id``/``target_qids`` keys), since callers feed
    both shapes.
    """
    def _attr_or_key(obj: Any, name: str, default: Any = None) -> Any:
        if isinstance(obj, Mapping):
            return obj.get(name, default)
        return getattr(obj, name, default)

    cluster_to_rca: dict[str, str] = {}
    for cluster in clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        if not cid:
            continue
        cluster_qids = {str(q) for q in (cluster.get("question_ids") or []) if str(q)}
        if not cluster_qids:
            continue
        for finding in findings or []:
            rca_id = str(_attr_or_key(finding, "rca_id", "") or "")
            if not rca_id:
                continue
            target_qids = {
                str(q)
                for q in (_attr_or_key(finding, "target_qids", ()) or ())
                if str(q)
            }
            if cluster_qids & target_qids:
                cluster_to_rca[cid] = rca_id
                break
    return cluster_to_rca


# ---------------------------------------------------------------------------
# Unresolved RCA — RCA_FORMED + UNRESOLVED + RCA_UNGROUNDED
# ---------------------------------------------------------------------------


def unresolved_rca_records(
    *,
    run_id: str,
    iteration: int,
    clusters: Sequence[Mapping[str, Any]],
    rca_id_by_cluster: Mapping[str, str],
) -> list[DecisionRecord]:
    """Emit ``RCA_FORMED`` + ``UNRESOLVED`` + ``RCA_UNGROUNDED`` for
    clusters with hard failures but no matching RCA finding.

    Phase C Task 7: ``rca_formed_records`` only emits when an RCA *is*
    formed for the cluster. Clusters whose RCA prompt produced no
    finding silently drop out of the trace — invisible failure. This
    producer closes that gap with the exact same shape as
    ``rca_formed_records`` but with ``UNRESOLVED`` outcome and an
    empty ``rca_id``. The validator's per-(type,reason) exemption
    (``rca_decision_trace`` Task 7) allows the empty ``rca_id`` for
    this combination only.
    """
    rca_lookup = dict(rca_id_by_cluster or {})
    records: list[DecisionRecord] = []
    for cluster in clusters or []:
        cid = str(cluster.get("cluster_id") or "")
        if not cid:
            continue
        if rca_lookup.get(cid):
            # The cluster has an RCA finding; ``rca_formed_records``
            # owns this case.
            continue
        qids = tuple(
            str(q) for q in (cluster.get("question_ids") or []) if str(q)
        )
        if not qids:
            continue
        root_cause = str(cluster.get("root_cause") or "")
        records.append(
            DecisionRecord(
                run_id=str(run_id),
                iteration=int(iteration),
                decision_type=DecisionType.RCA_FORMED,
                outcome=DecisionOutcome.UNRESOLVED,
                reason_code=ReasonCode.RCA_UNGROUNDED,
                cluster_id=cid,
                rca_id="",
                root_cause=root_cause,
                evidence_refs=(f"cluster:{cid}",),
                affected_qids=qids,
                target_qids=qids,
                source_cluster_ids=(cid,),
                expected_effect=(
                    f"Cluster {cid} should ground on a root cause."
                ),
                observed_effect=(
                    f"No RCA finding for cluster {cid} "
                    f"({len(qids)} hard qid(s)); strategist has "
                    f"nothing to ground against."
                ),
                next_action=(
                    "Re-run RCA prompt with broader evidence, or "
                    "promote this cluster to a benchmark-review queue."
                ),
            )
        )
    return records


# ---------------------------------------------------------------------------
# Orphan RCA — STRATEGIST_AG_EMITTED + UNRESOLVED + RCA_UNGROUNDED
# ---------------------------------------------------------------------------


def orphan_rca_records(
    *,
    run_id: str,
    iteration: int,
    findings: Sequence[Any],
    action_groups: Sequence[Mapping[str, Any]],
) -> list[DecisionRecord]:
    """Emit ``STRATEGIST_AG_EMITTED`` + ``UNRESOLVED`` + ``RCA_UNGROUNDED``
    for findings whose qids are not covered by any emitted AG.

    Phase C Task 6: ``strategist_ag_records`` only emits per-AG. A
    finding the strategist did not pick up (the LLM dropped it; or
    the finding's qids fell outside every AG's
    ``affected_questions``) produces no record — the trace silently
    loses signal. This producer closes that gap.

    The record carries the finding's ``rca_id`` (it IS known), so
    the validator's ``rca_id`` requirement passes without any
    exemption.
    """
    def _attr(obj: Any, name: str, default: Any = None) -> Any:
        if isinstance(obj, Mapping):
            return obj.get(name, default)
        return getattr(obj, name, default)

    covered_qids: set[str] = set()
    for ag in action_groups or []:
        for q in (ag.get("affected_questions") or []):
            qstr = str(q or "")
            if qstr:
                covered_qids.add(qstr)

    records: list[DecisionRecord] = []
    for finding in findings or []:
        rca_id = str(_attr(finding, "rca_id", "") or "")
        if not rca_id:
            continue
        f_qids = tuple(
            str(q) for q in (_attr(finding, "target_qids", ()) or ())
            if str(q)
        )
        if not f_qids:
            continue
        # Any qid covered by any AG → not orphaned.
        if any(q in covered_qids for q in f_qids):
            continue
        root_cause = str(_attr(finding, "root_cause", "") or "")
        records.append(
            DecisionRecord(
                run_id=str(run_id),
                iteration=int(iteration),
                decision_type=DecisionType.STRATEGIST_AG_EMITTED,
                outcome=DecisionOutcome.UNRESOLVED,
                reason_code=ReasonCode.RCA_UNGROUNDED,
                ag_id="",
                rca_id=rca_id,
                root_cause=root_cause,
                evidence_refs=(f"rca:{rca_id}",),
                affected_qids=f_qids,
                target_qids=f_qids,
                expected_effect=(
                    f"Strategist should emit an AG for "
                    f"{root_cause or 'failure pattern'}."
                ),
                observed_effect=(
                    f"RCA {rca_id} produced no AG "
                    f"({len(f_qids)} target qid(s) orphaned)."
                ),
                next_action=(
                    "Force strategist to emit an AG covering "
                    f"{','.join(f_qids[:5])}, or quarantine the RCA."
                ),
            )
        )
    return records


# ---------------------------------------------------------------------------
# RCA groundedness gate — GATE_DECISION (rca_groundedness)
# ---------------------------------------------------------------------------


def groundedness_gate_records(
    *,
    run_id: str,
    iteration: int,
    drops: Sequence[Mapping[str, Any]],
) -> list[DecisionRecord]:
    """Emit one ``GATE_DECISION`` / ``DROPPED`` record per groundedness drop.

    Phase C Task 5: the unified RCA-groundedness gate
    (``rca_groundedness.is_rca_grounded``) runs at the AG-emission
    and proposal-emission sites. Targets that fail grounding are
    fed here; one record per drop lands in the operator transcript's
    ``Proposal Survival And Gate Drops`` section.

    Each ``drops`` entry must carry:

    * ``ag_id`` and (optional) ``proposal_id``
    * ``target_qids`` — the AG's ``affected_questions`` or the
      proposal's narrower scope
    * ``rca_id`` and ``root_cause`` — best-known values when the
      gate ran (may be empty for ``MISSING_TARGET_QIDS``)
    * ``target_kind`` — ``"ag"`` or ``"proposal"``
    * ``verdict`` — the :class:`GroundednessVerdict` returned by
      ``is_rca_grounded``

    The producer trusts the verdict's ``reason_code``; it does not
    re-decide.
    """
    records: list[DecisionRecord] = []
    for d in drops or []:
        verdict = d.get("verdict")
        if verdict is None or getattr(verdict, "accepted", False):
            continue
        target_qids = tuple(
            str(q) for q in (d.get("target_qids") or ()) if str(q)
        )
        ag_id = str(d.get("ag_id") or "")
        proposal_id = str(d.get("proposal_id") or "")
        target_kind = str(d.get("target_kind") or "")
        rca_id = str(d.get("rca_id") or "")
        root_cause = str(d.get("root_cause") or "")
        records.append(
            DecisionRecord(
                run_id=str(run_id),
                iteration=int(iteration),
                ag_id=ag_id,
                rca_id=rca_id,
                root_cause=root_cause,
                proposal_id=proposal_id,
                proposal_ids=(proposal_id,) if proposal_id else (),
                decision_type=DecisionType.GATE_DECISION,
                outcome=DecisionOutcome.DROPPED,
                reason_code=verdict.reason_code,
                gate="rca_groundedness",
                reason_detail=f"groundedness:{target_kind}:{verdict.reason_code.value}",
                evidence_refs=(
                    f"ag:{ag_id}" if ag_id else "groundedness_gate",
                ),
                target_qids=target_qids,
                affected_qids=target_qids,
                expected_effect=(
                    f"{target_kind.title()} would address "
                    f"{root_cause or 'failure pattern'} on "
                    f"{len(target_qids)} target qid(s)."
                ),
                observed_effect=(
                    f"Dropped at groundedness gate: "
                    f"{verdict.reason_code.value}."
                ),
                next_action=(
                    "Strategist must re-target an RCA-grounded scope "
                    "or skip this AG."
                ),
                metrics={
                    "target_kind": target_kind,
                    "verdict_finding_id": str(getattr(verdict, "finding_id", "") or ""),
                },
            )
        )
    return records


def iteration_budget_decision_record(
    *,
    run_id: str,
    iteration: int,
    consumed: bool,
    no_op_cause: str | None,
    applied_patches: int,
) -> DecisionRecord:
    """Cycle 5 T1 — emit a typed iteration-budget decision so the
    operator transcript and postmortem skill can audit which
    iterations consumed budget and why.

    ``no_op_cause`` is one of the typed P4 outcomes
    (``proposal_generation_empty``,
    ``structural_gate_dropped_instruction_only``,
    ``no_structural_candidate``) when ``consumed=False``; otherwise
    ``None``.

    Reproducer: run 2423b960-16e8-41d4-a0cb-74c563378e05 burned 4/5
    iterations on deterministic no-ops because every no-op consumed
    budget. With the productive-iteration flag on, the same run
    surfaces a SKIPPED record per no-op and only counts productive
    iterations toward MAX_ITERATIONS.
    """
    if consumed:
        reason = ReasonCode.ITERATION_BUDGET_CONSUMED
        next_action = (
            f"Iteration {iteration} consumed budget; "
            f"{applied_patches} patch(es) applied."
        )
    else:
        reason = ReasonCode.ITERATION_BUDGET_SKIPPED_NO_OP
        next_action = (
            f"Iteration {iteration} did not consume budget "
            f"(no_op_cause={no_op_cause}); strategist re-runs."
        )
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.ITERATION_BUDGET_DECISION,
        outcome=DecisionOutcome.INFO,
        reason_code=reason,
        next_action=next_action,
        metrics={
            "applied_patches": int(applied_patches),
            "no_op_cause": str(no_op_cause or ""),
            "consumed": bool(consumed),
        },
    )


_LEARNING_EXIT_PATH_TO_REASON: Mapping[str, ReasonCode] = {
    "proposals_empty": ReasonCode.PROPOSAL_GENERATION_EMPTY,
    "no_pending_ags_first_pass": ReasonCode.PROPOSAL_GENERATION_EMPTY,
    "no_pending_ags_second_pass": ReasonCode.PROPOSAL_GENERATION_EMPTY,
    "ag_identity_skip": ReasonCode.PROPOSAL_GENERATION_EMPTY,
    "no_actionable_clusters": ReasonCode.PROPOSAL_GENERATION_EMPTY,
    "strategy_zero_ags": ReasonCode.PROPOSAL_GENERATION_EMPTY,
    "skipped_no_applied_patches": ReasonCode.NO_APPLIED_PATCHES,
    "applier_failed": ReasonCode.NO_APPLIED_PATCHES,
    "post_grounding_skip": ReasonCode.RCA_UNGROUNDED,
    "rolled_back": ReasonCode.PATCH_SKIPPED,
    "completed": ReasonCode.PATCH_APPLIED,
}


def _learning_outcome_for_exit_path(
    exit_path: str,
    *,
    accepted_count: int,
    rolled_back_count: int,
) -> DecisionOutcome:
    raw = (exit_path or "").strip().lower()
    if raw == "rolled_back" or rolled_back_count > 0:
        return DecisionOutcome.ROLLED_BACK
    if raw == "completed" and accepted_count > 0:
        return DecisionOutcome.ACCEPTED
    if raw in _LEARNING_EXIT_PATH_TO_REASON and raw != "completed":
        return DecisionOutcome.SKIPPED
    return DecisionOutcome.INFO


def _learning_next_action_for_exit_path(
    exit_path: str,
    *,
    accepted_count: int,
    rolled_back_count: int,
    gate_drop_count: int,
) -> str:
    raw = (exit_path or "").strip().lower()
    if raw == "proposals_empty":
        return (
            "Proposal generation produced 0 candidates; the strategist "
            "should switch approach or the operator should add evidence."
        )
    if raw in (
        "no_pending_ags_first_pass",
        "no_pending_ags_second_pass",
        "no_actionable_clusters",
        "strategy_zero_ags",
        "ag_identity_skip",
    ):
        return (
            "No actionable AG produced this iteration; consider "
            "re-clustering or expanding the strategist's failure scope."
        )
    if raw == "rolled_back":
        return (
            "Iteration rolled back; consult Stage 9 reason_detail for "
            "the control-plane rejection cause and adjust the AG."
        )
    if raw in ("skipped_no_applied_patches", "applier_failed"):
        return (
            "All proposals dropped before apply; inspect Stage 7 "
            "applier-decision counts to identify the rejection reason."
        )
    if raw == "post_grounding_skip":
        return "Skipped post-grounding gate; cluster lacked an RCA-grounded card."
    if raw == "completed" and accepted_count > 0:
        return "Iteration accepted; carry the patch into the next iteration's baseline."
    if gate_drop_count:
        return (
            f"{gate_drop_count} patch(es) dropped by safety gates; "
            f"review Stage 6 for blast-radius / RCA-groundedness issues."
        )
    return "Iteration produced no learning signal; advance to next iteration."


def iteration_learning_record(
    *,
    run_id: str,
    iteration: int,
    exit_path: str,
    accepted_count: int = 0,
    rolled_back_count: int = 0,
    skipped_count: int = 0,
    gate_drop_count: int = 0,
) -> DecisionRecord:
    """Phase H Fidelity Task 4 — emit one ``ITERATION_BUDGET_DECISION``
    per iteration so the operator transcript Stage 10 always has at
    least one record describing what happened and what the operator /
    next iteration should do.

    Run ``3b050ec5-4032-457f-a785-2d1a3942a097`` showed Stage 10 empty
    for every iteration despite the postmortem identifying
    ``proposals_empty`` four times in a row plus a rollback in
    iteration 1. The empty stage hid the most operator-relevant signal
    in the entire transcript.

    Maps the iteration's ``exit_path`` to a typed reason code (see
    ``_LEARNING_EXIT_PATH_TO_REASON``) and a per-bucket metrics block
    so postmortems can tell terminal no-op causes apart without
    reparsing freeform logs. Unknown exit paths degrade to
    ``DecisionOutcome.INFO`` with ``ReasonCode.NONE`` so the helper
    never crashes on a future / unrecognised exit label.
    """
    raw = (exit_path or "").strip().lower()
    reason = _LEARNING_EXIT_PATH_TO_REASON.get(raw, ReasonCode.NONE)
    outcome = _learning_outcome_for_exit_path(
        raw,
        accepted_count=accepted_count,
        rolled_back_count=rolled_back_count,
    )
    next_action = _learning_next_action_for_exit_path(
        raw,
        accepted_count=accepted_count,
        rolled_back_count=rolled_back_count,
        gate_drop_count=gate_drop_count,
    )
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.ITERATION_BUDGET_DECISION,
        outcome=outcome,
        reason_code=reason,
        next_action=next_action,
        observed_effect=(
            f"Iteration {iteration} exit_path={raw or 'unknown'}: "
            f"{accepted_count} accepted, {rolled_back_count} rolled back, "
            f"{skipped_count} skipped, {gate_drop_count} gate drops."
        ),
        metrics={
            "exit_path": str(raw or ""),
            "accepted_count": int(accepted_count or 0),
            "rolled_back_count": int(rolled_back_count or 0),
            "skipped_count": int(skipped_count or 0),
            "gate_drop_count": int(gate_drop_count or 0),
        },
    )


def rca_regeneration_triggered_record(
    *,
    run_id: str,
    iteration: int,
    cluster_id: str,
    target_qids: tuple[str, ...],
) -> DecisionRecord:
    """Cycle 5 T3 — emit when a ``diagnostic_no_parent_rca`` AG triggers
    RCA regeneration before proposal generation. Emitted as
    ``DecisionType.RCA_FORMED`` with outcome ``INFO`` so the operator
    transcript surfaces the regeneration attempt in the RCA Cards
    section."""
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.RCA_REGENERATION_TRIGGERED,
        cluster_id=str(cluster_id),
        target_qids=tuple(target_qids),
        next_action=(
            "Re-run RCA prompt with broader evidence "
            "(failure_buckets, ASI, cluster cohort)."
        ),
    )


def rca_classified_ungrounded_record(
    *,
    run_id: str,
    iteration: int,
    cluster_id: str,
    target_qids: tuple[str, ...],
    ungrounded_reason: Any,
    policy_max_attempts: int,
) -> DecisionRecord:
    """Plan P-D (2026-05-12) — emit once per ungrounded cluster per
    iteration when the recovery policy classifies the reason.
    Surfaced as ``DecisionType.RCA_FORMED + INFO`` so the operator
    transcript records the typed cause before any retry decision is
    visible.

    ``policy_max_attempts == 0`` means the policy refused — the
    next_action surfaces "policy refused" so a transcript reader
    sees immediately that no driver call will follow.
    """
    refused = int(policy_max_attempts) == 0
    return DecisionRecord(
        run_id=str(run_id or ""),
        iteration=int(iteration),
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.RCA_CLASSIFIED_UNGROUNDED,
        cluster_id=str(cluster_id or ""),
        target_qids=tuple(target_qids or ()),
        next_action=(
            "Recovery policy refused regeneration for this typed "
            "ungrounded reason; cluster will fall through to "
            "Defect Plan 1 G1 cluster_blocked_no_rca."
            if refused else
            "Recovery policy permits up to "
            f"{int(policy_max_attempts)} regeneration attempt(s) "
            "for this typed ungrounded reason."
        ),
        metrics={
            "ungrounded_reason": str(
                getattr(ungrounded_reason, "value", ungrounded_reason)
            ),
            "policy_max_attempts": int(policy_max_attempts),
        },
    )


def rca_regeneration_succeeded_record(
    *,
    run_id: str,
    iteration: int,
    cluster_id: str,
    rca_id: str,
    target_qids: tuple[str, ...],
    attempt_number: int,
    attempted_evidence_sources: tuple[str, ...],
    ungrounded_reason: Any,
) -> DecisionRecord:
    """Plan P-D (2026-05-12) — emit when a cluster-level regen
    attempt produced a fit ``rca_card`` and the cluster avoids the
    Defect Plan 1 G1 short-circuit.

    Carries the typed ``ungrounded_reason`` that motivated the
    attempt so the transcript shows the closed loop: classified
    NO_FINDINGS → driver attempted → produced fit card.
    """
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.RCA_REGENERATION_SUCCEEDED,
        cluster_id=str(cluster_id),
        rca_id=str(rca_id),
        target_qids=tuple(target_qids or ()),
        next_action=(
            "Cluster proceeds to AG-emit with regenerated RCA card "
            "(Defect Plan 1 G1 grounding gate will not block)."
        ),
        metrics={
            "attempt_number": int(attempt_number),
            "attempted_evidence_sources": list(attempted_evidence_sources or ()),
            "ungrounded_reason": str(
                getattr(ungrounded_reason, "value", ungrounded_reason)
            ),
        },
    )


def rca_regeneration_exhausted_record(
    *,
    run_id: str,
    iteration: int,
    cluster_id: str,
    attempted_evidence_sources: tuple[str, ...],
) -> DecisionRecord:
    """Cycle 5 T3 — emit when RCA regeneration cannot produce a
    grounded card; the AG is retired with ``UNRESOLVED`` so the
    cluster appears in the operator transcript's Unresolved QID
    Buckets section instead of generating empty proposals."""
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.UNRESOLVED,
        reason_code=ReasonCode.RCA_REGENERATION_EXHAUSTED,
        cluster_id=str(cluster_id),
        next_action="Promote cluster to benchmark-review queue.",
        metrics={
            "attempted_evidence_sources": list(attempted_evidence_sources),
        },
    )


def soft_cluster_drift_recovered_record(
    *,
    run_id: str,
    iteration: int,
    cluster_id: str,
    drifted_qids: tuple[str, ...],
    cluster_dropped: bool,
) -> DecisionRecord:
    """Cycle 5 T5 — emit when the harness recovered from soft-cluster
    drift by dropping drifted qids (or the entire cluster if every
    qid drifted). Surfaced as ``CLUSTER_SELECTED + INFO`` so the
    operator transcript records the recovery in the cluster-formation
    section without flagging it as an unresolved failure."""
    if cluster_dropped:
        next_action = (
            f"Soft cluster {cluster_id} dropped — every qid drifted "
            f"out of judge-failing state. Cluster removed from this "
            f"iteration's slate."
        )
    else:
        next_action = (
            f"Soft cluster {cluster_id} retained with "
            f"{len(drifted_qids)} drifted qid(s) removed: "
            f"{sorted(drifted_qids)}."
        )
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.CLUSTER_SELECTED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.SOFT_CLUSTER_DRIFT_RECOVERED,
        cluster_id=str(cluster_id),
        next_action=next_action,
        metrics={
            "drifted_qids": list(drifted_qids),
            "cluster_dropped": bool(cluster_dropped),
        },
    )


def qid_released_from_quarantine_record(
    *,
    run_id: str,
    iteration: int,
    qids: tuple[str, ...],
    cause: str = "",
) -> DecisionRecord:
    """Plan N4 — emit when the lenient quarantine-attribution
    invariant releases recovered qids back into the live state.

    The quarantine was set by the pre-loop arbiter; in-loop patches
    in unrelated clusters can have positive side-effects that move a
    qid into the passing set. The strict invariant treated that as a
    drift and raised; the lenient policy recognises it as a desirable
    end state and releases the qid so the next iteration starts from
    a consistent state.

    Surfaced as ``QID_RESOLUTION + INFO`` so the operator transcript's
    Observed Results section records the release alongside the
    matching post-eval resolution.
    """
    next_action = (
        f"Released {len(qids)} qid(s) from quarantine: "
        f"{sorted(qids)}. Cause: {cause or 'recovered_post_eval'}."
    )
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.QID_RELEASED_FROM_QUARANTINE,
        target_qids=tuple(sorted(str(q) for q in qids if q)),
        next_action=next_action,
        metrics={
            "released_qids": sorted(str(q) for q in qids if q),
            "cause": str(cause or "recovered_post_eval"),
        },
    )


def regression_debt_partition_incomplete_record(
    *,
    run_id: str,
    iteration: int,
    missing_qids: tuple[str, ...],
) -> DecisionRecord:
    """Plan N4 — emit when the regression-debt partition assertion
    finds out_of_target_regressed_qids that are not the disjoint
    union of the soft/passing/unknown to-hard buckets.

    Bookkeeping-only: the candidate accept/reject outcome is left
    unchanged (the gap is partition completeness, not correctness).
    Surfaced as ``ACCEPTANCE_DECIDED + INFO`` so the operator
    transcript surfaces the bookkeeping gap in the Applied Patches
    And Acceptance section.
    """
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.ACCEPTANCE_DECIDED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.REGRESSION_DEBT_PARTITION_INCOMPLETE,
        target_qids=tuple(sorted(str(q) for q in missing_qids if q)),
        next_action=(
            f"Regression-debt partition is incomplete: "
            f"{len(missing_qids)} qid(s) unbucketed: "
            f"{sorted(missing_qids)}. Acceptance outcome unchanged; "
            f"audit the partition logic."
        ),
        metrics={
            "missing_qids": sorted(str(q) for q in missing_qids if q),
        },
    )


def cap_conservation_repaired_record(
    *,
    run_id: str,
    iteration: int,
    func_name: str,
    decisions_in: int,
    decisions_out: int,
    input_count: int,
) -> DecisionRecord:
    """Plan N4 — emit when ``_assert_cap_conservation`` repairs a
    decision-list count mismatch instead of raising.

    Reconciliation truncates extras and pads missing slots with
    explicit ``decision="dropped"`` entries carrying
    ``reason="cap_conservation_repaired"``, so the survival ledger
    downstream sees a typed dropped-decision rather than a count
    mismatch.

    Surfaced as ``GATE_DECISION + INFO`` so the Proposal Survival
    section surfaces the repair.
    """
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.GATE_DECISION,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.CAP_CONSERVATION_REPAIRED,
        gate=str(func_name),
        next_action=(
            f"{func_name} returned {decisions_in} decision(s) for "
            f"{input_count} input(s); reconciled to {decisions_out}."
        ),
        metrics={
            "func_name": str(func_name),
            "decisions_in": int(decisions_in),
            "decisions_out": int(decisions_out),
            "input_count": int(input_count),
        },
    )


def non_canonical_judge_row_record(
    *,
    run_id: str,
    iteration: int,
    judge: str,
    detail: str = "",
) -> DecisionRecord:
    """Plan N4 — emit when ``_summary_judges_or_raise`` encounters a
    non-canonical judge row under ``GSO_ASSERT_ROW_CANONICAL=1`` in
    lenient mode. The function continues with the existing empty-
    rationale value; this record surfaces the non-canonical row in
    the operator transcript.
    """
    return DecisionRecord(
        run_id=run_id,
        iteration=int(iteration),
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.NON_CANONICAL_JUDGE_ROW,
        gate=str(judge),
        reason_detail=str(detail),
        next_action=(
            f"Non-canonical judge row for {judge}; "
            f"continuing with empty rationale. Audit the row schema."
        ),
        metrics={"judge": str(judge), "detail": str(detail)},
    )


# ---------------------------------------------------------------------------
# Cycle 10 W3 — typed outcomes for the Cycle 7 N3 force-Lever-6 silent path
# ---------------------------------------------------------------------------


def lever6_force_llm_declined_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    target_qids: tuple = (),
) -> DecisionRecord:
    """Cycle 10 W3 — Cycle 7 N3 force-L6 path: ``_generate_lever6_proposal``
    returned ``None`` (LLM declined / no synthesizable archetype).

    Decision type ``PROPOSAL_GENERATED`` with reason_code
    ``lever6_force_llm_declined``.
    """
    qids = tuple(str(q) for q in (target_qids or ()) if str(q))
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
        ) if v
    )
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.UNRESOLVED,
        reason_code=ReasonCode.LEVER6_FORCE_LLM_DECLINED,
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        root_cause=str(root_cause),
        evidence_refs=evidence_refs,
        affected_qids=qids,
        target_qids=qids,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        gate="proposal_generation",
        expected_effect=(
            "Forced Lever-6 candidate would close the SQL-shape "
            "hard failure for the AG."
        ),
        observed_effect=(
            "_generate_lever6_proposal returned no candidate; "
            "the AG retains the strategist's slate (which lacks "
            "an L6 add_sql_snippet_*)."
        ),
        next_action=(
            "Inspect the LLM transcript for this AG to confirm the "
            "decline rationale; consider widening the synthesizer "
            "archetype list."
        ),
    )


def lever6_force_raised_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    exception_repr: str,
) -> DecisionRecord:
    """Cycle 10 W3 — Cycle 7 N3 force-L6 path: ``_generate_lever6_proposal``
    raised. Captures ``repr(exc)[:512]`` as the diagnostic.
    """
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
        ) if v
    )
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.UNRESOLVED,
        reason_code=ReasonCode.LEVER6_FORCE_RAISED,
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        root_cause=str(root_cause),
        evidence_refs=evidence_refs,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        gate="proposal_generation",
        reason_detail=str(exception_repr)[:512],
        expected_effect=(
            "Forced Lever-6 candidate would close the SQL-shape "
            "hard failure for the AG."
        ),
        observed_effect=(
            "_generate_lever6_proposal raised an exception; the AG "
            "retains the strategist's slate."
        ),
        next_action=(
            "Inspect the harness logs for the exception traceback; "
            "if recurring, file a P3 against the synthesizer."
        ),
        metrics={"exception_repr": str(exception_repr)[:512]},
    )


def narrow_not_applicable_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    original_patch_type: str,
    reason: str,
) -> DecisionRecord:
    """Cycle 10 W4 — narrow-L6 replacement does not apply for the
    given patch_type. Decision type ``PROPOSAL_GENERATED`` with
    reason_code ``narrow_not_applicable``.
    """
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
        ) if v
    )
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.UNRESOLVED,
        reason_code=ReasonCode.NARROW_NOT_APPLICABLE,
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        root_cause=str(root_cause),
        evidence_refs=evidence_refs,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        gate="proposal_generation",
        reason_detail=(
            f"original_patch_type={original_patch_type}; reason={reason}"
        ),
        expected_effect=(
            "Narrowed Lever-6 variant would replace a parent dropped "
            "at high_collateral_risk_flagged."
        ),
        observed_effect=(
            "Builder declined to produce a narrowed variant; harness "
            "falls back to L5 example_sql synthesis."
        ),
        next_action=(
            "Surface this AG to the L5 example_sql path; if recurring, "
            "expand the narrow-L6 builder archetype list."
        ),
        metrics={
            "original_patch_type": str(original_patch_type),
            "reason": str(reason),
        },
    )


def narrow_skipped_no_original_patch_type_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
) -> DecisionRecord:
    """P-E1 — narrow-L6 replacement skipped because the original
    patch carries an empty ``patch_type`` (so there is nothing to
    narrow). Decision type ``PROPOSAL_GENERATED`` with reason_code
    ``narrow_skipped_no_original_patch_type``.

    Replaces the misleading ``unrecognized_patch_type`` reason that
    historical fixtures recorded for this exact case (every fixture
    containing ``"original_patch_type=; reason=unrecognized_patch_type"``).
    """
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
        ) if v
    )
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.UNRESOLVED,
        reason_code=ReasonCode.NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE,
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        root_cause=str(root_cause),
        evidence_refs=evidence_refs,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        gate="proposal_generation",
        reason_detail=(
            "original_patch_type=; "
            "reason=narrow_skipped_no_original_patch_type"
        ),
        expected_effect=(
            "Narrow-L6 replacement would substitute a question-scoped "
            "variant for the dropped L6 patch."
        ),
        observed_effect=(
            "Dropped patch has no original patch_type; narrow "
            "replacement is not applicable by construction."
        ),
        next_action=(
            "Inspect why a patch with empty patch_type reached the "
            "narrow-replacement orchestrator; usually indicates an "
            "upstream proposal-shaping bug."
        ),
    )


@dataclass(frozen=True)
class NarrowReplacementSynthesizedRecord:
    decision_type: str
    run_id: str
    iteration: int
    ag_id: str
    cluster_id: str
    root_cause: str
    original_patch_type: str
    original_proposal_id: str
    narrow_proposal_id: str
    narrowing_strategy: str
    target_qids: tuple

    def to_dict(self) -> dict:
        return {
            "decision_type": self.decision_type,
            "run_id": self.run_id,
            "iteration": int(self.iteration),
            "ag_id": self.ag_id,
            "cluster_id": self.cluster_id,
            "root_cause": self.root_cause,
            "original_patch_type": self.original_patch_type,
            "original_proposal_id": self.original_proposal_id,
            "narrow_proposal_id": self.narrow_proposal_id,
            "narrowing_strategy": self.narrowing_strategy,
            "target_qids": list(self.target_qids),
        }


def narrow_replacement_synthesized_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    original_patch_type: str,
    original_proposal_id: str,
    narrow_proposal_id: str,
    narrowing_strategy: str,
    target_qids,
) -> "NarrowReplacementSynthesizedRecord":
    """P0 Task 5A: typed record emitted at the narrow-replacement
    survivor site in `_run_narrow_l6_replacement_loop`. Mirror of
    `narrow_not_applicable_record` for the SUCCESS path so dashboards
    can distinguish "narrow-replacement saved an iteration" from "no
    narrow-replacement was attempted" or "synthesizer declined".
    """
    return NarrowReplacementSynthesizedRecord(
        decision_type="narrow_replacement_synthesized",
        run_id=str(run_id),
        iteration=int(iteration),
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        root_cause=str(root_cause),
        original_patch_type=str(original_patch_type),
        original_proposal_id=str(original_proposal_id),
        narrow_proposal_id=str(narrow_proposal_id),
        narrowing_strategy=str(narrowing_strategy),
        target_qids=tuple(str(q) for q in (target_qids or ()) if str(q)),
    )


@dataclass(frozen=True)
class NarrowReplacementBranchCSynthesizedRecord:
    """Cycle 16 T3 — typed record emitted at every Branch C survivor.

    One record per Lever-5 ``add_example_sql`` patch synthesized from a
    dropped L6 expression / measure. Mirrors
    ``NarrowReplacementSynthesizedRecord`` (Branch A) but additionally
    carries ``branch`` and the per-QID ``target_qid`` so the
    operator transcript can render Branch C survivors one-to-one with
    their target QIDs.
    """
    decision_type: str
    run_id: str
    iteration: int
    ag_id: str
    cluster_id: str
    root_cause: str
    original_patch_type: str
    original_proposal_id: str
    narrow_proposal_id: str
    narrowing_strategy: str
    branch: str
    target_qid: str
    target_qids: tuple

    def to_dict(self) -> dict:
        return {
            "decision_type": self.decision_type,
            "run_id": self.run_id,
            "iteration": int(self.iteration),
            "ag_id": self.ag_id,
            "cluster_id": self.cluster_id,
            "root_cause": self.root_cause,
            "original_patch_type": self.original_patch_type,
            "original_proposal_id": self.original_proposal_id,
            "narrow_proposal_id": self.narrow_proposal_id,
            "narrowing_strategy": self.narrowing_strategy,
            "branch": self.branch,
            "target_qid": self.target_qid,
            "target_qids": list(self.target_qids),
        }


def narrow_replacement_branch_c_synthesized_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    original_patch_type: str,
    original_proposal_id: str,
    narrow_proposal_id: str,
    target_qid: str,
    target_qids,
) -> "NarrowReplacementBranchCSynthesizedRecord":
    """Cycle 16 T3 — typed record for one Branch C L5 survivor."""
    return NarrowReplacementBranchCSynthesizedRecord(
        decision_type="narrow_replacement_branch_c_synthesized",
        run_id=str(run_id),
        iteration=int(iteration),
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        root_cause=str(root_cause),
        original_patch_type=str(original_patch_type),
        original_proposal_id=str(original_proposal_id),
        narrow_proposal_id=str(narrow_proposal_id),
        narrowing_strategy="l5_example_sql_per_qid",
        branch="C",
        target_qid=str(target_qid),
        target_qids=tuple(str(q) for q in (target_qids or ()) if str(q)),
    )


# ---------------------------------------------------------------------------
# Plan P-F (2026-05-12) — Proposal Failure Taxonomy taxonomy record
# ---------------------------------------------------------------------------


def proposal_failure_decided_record(
    *,
    run_id: str,
    iteration: int,
    context: Any,  # ProposalFailureContext (lazy import to avoid cycles)
    decision: Any,  # ProposalFailureDecision
    target_qids: tuple[str, ...] = (),
) -> DecisionRecord:
    """Plan P-F — taxonomy companion to the existing proposal-phase
    failure records (proposal_generation_empty_record,
    lever6_force_llm_declined_record, ag_outcome_decision_record for
    skipped_no_applied_patches, etc.).

    Emits a ``DecisionType.PROPOSAL_FAILURE_DECIDED`` / ``INFO`` record
    whose ``reason_code`` is one of six closed-vocabulary next-action
    labels (see ``optimization.proposal_failure_policy``). Postmortems
    pivot on this label without parsing free-form ``next_action`` text.

    The caller (``harness.py``) builds the ``ProposalFailureContext``
    from observable state at the failure site and invokes
    ``decide_next_action`` to produce the ``ProposalFailureDecision``.
    """
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureNextAction,
    )

    _NEXT_ACTION_TO_REASON_CODE = {
        ProposalFailureNextAction.ROTATE_LEVER_FAMILY: (
            ReasonCode.ROTATE_LEVER_FAMILY
        ),
        ProposalFailureNextAction.NARROW_AG_SCOPE: (
            ReasonCode.NARROW_AG_SCOPE
        ),
        ProposalFailureNextAction.MARK_EVIDENCE_GAP: (
            ReasonCode.MARK_EVIDENCE_GAP
        ),
        ProposalFailureNextAction.BLOCK_AG_RETRY_BY_CLUSTER_SIGNATURE: (
            ReasonCode.BLOCK_AG_RETRY_BY_CLUSTER_SIGNATURE
        ),
        ProposalFailureNextAction.ESCALATE_UNSUPPORTED_REPAIR_SHAPE: (
            ReasonCode.ESCALATE_UNSUPPORTED_REPAIR_SHAPE
        ),
        ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING: (
            ReasonCode.REQUEST_EVIDENCE_GATHERING
        ),
    }

    reason_code = _NEXT_ACTION_TO_REASON_CODE[decision.next_action]
    qids = tuple(str(q) for q in (target_qids or ()) if str(q))
    evidence_refs = tuple(
        v for v in (
            f"ag:{context.ag_id}" if context.ag_id else "",
            f"cluster:{context.cluster_id}" if context.cluster_id else "",
            f"rca:{context.rca_id}" if context.rca_id else "",
            (
                f"signature:{context.cluster_signature}"
                if context.cluster_signature
                else ""
            ),
        ) if v
    )

    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.PROPOSAL_FAILURE_DECIDED,
        outcome=DecisionOutcome.INFO,
        reason_code=reason_code,
        ag_id=str(context.ag_id or ""),
        cluster_id=str(context.cluster_id or ""),
        rca_id=str(context.rca_id or ""),
        root_cause=str(context.root_cause or ""),
        evidence_refs=evidence_refs,
        affected_qids=qids,
        target_qids=qids,
        source_cluster_ids=(
            (context.cluster_id,) if context.cluster_id else ()
        ),
        gate="proposal_failure_policy",
        reason_detail=str(decision.rationale or ""),
        expected_effect=(
            f"Next iteration applies {decision.next_action.value} "
            f"for AG {context.ag_id} on failure mode "
            f"{context.failure_mode}."
        ),
        next_action=str(decision.next_action.value),
        metrics={
            "failure_mode": str(context.failure_mode),
            "cluster_signature": str(context.cluster_signature),
            "lever_set": list(int(L) for L in (context.lever_set or ())),
            "tried_lever_families": list(
                int(L) for L in (context.tried_lever_families or ())
            ),
            "ag_source_cluster_count": int(
                context.ag_source_cluster_count or 0
            ),
            "rca_card_grounded": bool(context.rca_card_grounded),
            "prior_failure_count": int(context.prior_failure_count or 0),
        },
    )


def structural_causal_dropped_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    root_cause: str,
    rca_id: str,
    original_proposal_id: str,
    original_patch_type: str,
    original_target: str,
    drop_reason: str,
    target_qids: tuple,
) -> DecisionRecord:
    """Cycle 16 T4 — typed record for one structural-causal blast-radius
    drop that was not replaced by any narrow survivor.

    Decision type ``GATE_DECISION`` with reason_code
    ``structural_causal_dropped``. Mirrors the existing blast-radius
    drop record convention but adds the causal-continuity dimension
    (the drop crossed an RCA-bound structural patch).
    """
    qids_tuple = tuple(str(q) for q in (target_qids or ()) if str(q))
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.GATE_DECISION,
        outcome=DecisionOutcome.DROPPED,
        reason_code=ReasonCode.STRUCTURAL_CAUSAL_DROPPED,
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        rca_id=str(rca_id),
        proposal_id=str(original_proposal_id),
        gate="blast_radius",
        root_cause=str(root_cause),
        target_qids=qids_tuple,
        affected_qids=qids_tuple,
        reason_detail=(
            f"structural patch (type={original_patch_type}) on "
            f"{original_target} dropped at blast-radius "
            f"({drop_reason}); no narrow survivor replaced it"
        ),
        expected_effect=(
            "Structural patch would have closed the AG's causal RCA."
        ),
        observed_effect=(
            "Structural patch dropped at blast-radius; narrow-"
            "replacement loop produced no survivor for this parent."
        ),
        next_action=(
            "Halt the AG with no_structural_alternative; the "
            "next-iteration strategist must shift the lever family or "
            "narrow the cluster scope."
        ),
        metrics={
            "original_patch_type": str(original_patch_type),
            "original_target": str(original_target),
            "drop_reason": str(drop_reason),
        },
    )


def no_structural_alternative_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    rca_id: str,
    root_cause: str,
    dropped_proposal_ids: tuple,
    target_qids: tuple,
) -> DecisionRecord:
    """Cycle 16 T4 — typed AG-level halt record emitted when at least
    one structural-causal patch was dropped AND Branch C synthesis
    produced no survivor.

    Decision type ``AG_RETIRED`` with reason_code
    ``no_structural_alternative`` and outcome ``RETIRED``. Sibling to
    Cycle 15-T3's ``no_grounded_clusters`` terminal record (different
    failure mode: patch-level vs cluster-level).
    """
    dropped_pid_tuple = tuple(
        str(p) for p in (dropped_proposal_ids or ()) if str(p)
    )
    qids_tuple = tuple(str(q) for q in (target_qids or ()) if str(q))
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.AG_RETIRED,
        outcome=DecisionOutcome.RETIRED,
        reason_code=ReasonCode.NO_STRUCTURAL_ALTERNATIVE,
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        rca_id=str(rca_id),
        root_cause=str(root_cause),
        gate="blast_radius",
        target_qids=qids_tuple,
        affected_qids=qids_tuple,
        proposal_ids=dropped_pid_tuple,
        reason_detail=(
            f"{len(dropped_pid_tuple)} structural-causal patch(es) "
            f"dropped; Branch C synthesis produced no survivor"
        ),
        expected_effect=(
            "AG would have shipped a structural fix for the causal RCA."
        ),
        observed_effect=(
            "No structural alternative survived blast-radius; AG halts "
            "for this iteration."
        ),
        next_action=(
            "C13 forbidden-set admits the NO_ACTION reflection entry "
            "on the next iteration; strategist must shift levers or "
            "narrow scope."
        ),
        metrics={
            "dropped_proposal_count": len(dropped_pid_tuple),
        },
    )


def ag_levers_unioned_record(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    levers_before: tuple,
    levers_after: tuple,
) -> DecisionRecord:
    """Cycle 10 W8 — Cycle 10 W2 union widened the AG's levers.

    Decision type ``STRATEGIST_AG_EMITTED`` with reason_code
    ``ag_levers_unioned``.
    """
    before = tuple(str(l) for l in (levers_before or ()))
    after = tuple(str(l) for l in (levers_after or ()))
    evidence_refs = tuple(
        v for v in (
            f"ag:{ag_id}" if ag_id else "",
            f"cluster:{cluster_id}" if cluster_id else "",
        ) if v
    )
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.STRATEGIST_AG_EMITTED,
        outcome=DecisionOutcome.RESOLVED,
        reason_code=ReasonCode.AG_LEVERS_UNIONED,
        ag_id=str(ag_id),
        cluster_id=str(cluster_id),
        evidence_refs=evidence_refs,
        source_cluster_ids=(cluster_id,) if cluster_id else (),
        reason_detail=(
            f"levers_before={list(before)}; levers_after={list(after)}"
        ),
        expected_effect=(
            "AG carries every lever in cluster.recommended_levers; "
            "the strategist sees the wider lever set on the next "
            "AG-emit pass."
        ),
        observed_effect=(
            "union_ag_levers_with_recommended widened the AG's "
            "lever_directives to satisfy the cluster recommendation."
        ),
        next_action=(
            "If the wider set is consistently rejected at proposal-"
            "generation, audit the cluster.recommended_levers heuristic."
        ),
        metrics={
            "levers_before": list(before),
            "levers_after": list(after),
        },
    )


def cluster_blocked_no_rca_record(
    *,
    run_id: str,
    iteration: int,
    cluster_id: str,
    rca_id: str | None,
    affected_qids: Sequence[str] | None,
    root_cause: str | None,
) -> DecisionRecord:
    """Defect Plan 1 (2026-05-12) — emit a typed record when an open
    hard cluster reaches AG-emit time with no fit RCA card.

    Consumed by:

    1. ``invariants.check_i7_rca_grounding`` — the green branch
       (cluster present in ``blocked_clusters`` set) becomes
       reachable in production once the harness wires this producer
       (Task 4). Before this plan, I7 was detection-only.
    2. ``stages.action_groups.select`` — the runtime gate (Task 6)
       reads ``ActionGroupsInput.blocked_cluster_ids`` (Task 5) and
       drops AGs whose ``source_cluster_ids`` intersect the blocked
       set.

    Pure function. The harness call site (Task 4) wraps the call in
    the producer-exception try/except so any failure becomes a
    typed ``PRODUCER_EXCEPTION`` record rather than a silent mute.
    """
    return DecisionRecord(
        run_id=str(run_id or ""),
        iteration=int(iteration),
        decision_type=DecisionType.CLUSTER_BLOCKED_NO_RCA,
        outcome=DecisionOutcome.SKIPPED,
        reason_code=ReasonCode.RCA_UNGROUNDED,
        cluster_id=str(cluster_id or ""),
        rca_id=str(rca_id or ""),
        ag_id="",
        target_qids=tuple(str(q) for q in (affected_qids or []) if q),
        root_cause=str(root_cause or ""),
        next_action=(
            "regenerate RCA evidence for this cluster or escalate to "
            "diagnostic-AG path before re-attempting AG emission"
        ),
    )


# ---------------------------------------------------------------------------
# Plan P-G — Stage 4 strategist context boundary
# ---------------------------------------------------------------------------


def _canonical_sha256(payload: Any) -> str:
    """SHA-256 of a canonical JSON projection of ``payload``.

    Used by both ASSEMBLED and CONSUMED producers so the two hashes are
    directly comparable for drift detection. Accepts either an object
    with ``.to_json()`` (e.g. ``StrategistContextOutput``) or a plain
    dict / list — falls back to ``json.dumps(sort_keys=True,
    separators=(",", ":"), default=str)`` so a dataclass instance and a
    dict with the same logical content produce identical hashes when
    their fields agree.
    """
    import hashlib
    import json as _json
    if hasattr(payload, "to_json"):
        intermediate = payload.to_json()
    else:
        intermediate = payload
    text = _json.dumps(
        intermediate, sort_keys=True, separators=(",", ":"), default=str,
    )
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def strategist_context_assembled_record(
    *,
    run_id: str,
    iteration: int,
    assembled_output: Any,
) -> DecisionRecord:
    """Emit exactly one ``STRATEGIST_CONTEXT_ASSEMBLED`` record per iter.

    ``assembled_output`` is the typed ``StrategistContextOutput`` returned
    by ``stages.strategist_context.build_strategist_context``. The record
    carries:

    * ``metrics.assembled_hash``: canonical SHA-256 of the typed output
      (used for drift detection vs ``STRATEGIST_CONTEXT_CONSUMED``).
    * ``metrics.rca_cards_grounded_only_count`` / ``*_ungrounded_count``
      so the operator transcript can show how many cards survived the
      Stage 2 → Stage 4 grounded-only filter.
    * ``affected_qids``: the union of hard-failure qids in the
      assembled boundary so postmortem can correlate Stage 4 with the
      iteration's failures.

    This record is iteration-level (not per-qid). It is intentionally
    NOT in ``validate_decisions_against_journey``'s ``rca_required``
    set (see Task 5) because it captures a stage boundary, not an RCA
    routing decision.
    """
    import dataclasses as _dc
    hard_qids = tuple(
        str(q) for q in getattr(assembled_output, "hard_failure_qids", ()) or ()
    )
    grounded = getattr(assembled_output, "rca_cards_grounded_only", ()) or ()
    ungrounded_n = int(
        getattr(assembled_output, "rca_cards_ungrounded_count", 0) or 0
    )
    assembled_hash = _canonical_sha256(assembled_output)
    # Plan P-G: expose the typed-boundary's top-level field names so the
    # CONSUMED producer (Task 4) can compute a structural diff against
    # the dict actually fed to the LLM. Use dataclasses.fields when
    # possible (deterministic, no Python dunder noise); fall back to a
    # filtered ``dir()`` for non-dataclass inputs (test stubs).
    if _dc.is_dataclass(assembled_output) and not isinstance(
        assembled_output, type
    ):
        top_fields: tuple[str, ...] = tuple(
            f.name for f in _dc.fields(assembled_output)
        )
    else:
        top_fields = tuple(
            sorted(
                str(k) for k in dir(assembled_output)
                if not str(k).startswith("_")
            )
        )
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.STRATEGIST_CONTEXT_ASSEMBLED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.CONTEXT_ASSEMBLED,
        evidence_refs=("stage:strategist_context",),
        affected_qids=hard_qids,
        expected_effect=(
            f"Strategist LLM in iter {iteration} should receive "
            f"{len(grounded)} grounded RCA card(s) and "
            f"{len(hard_qids)} hard-failure qid(s)."
        ),
        next_action=(
            "Compare assembled_hash to STRATEGIST_CONTEXT_CONSUMED "
            "metrics.consumed_hash in the same iteration to verify "
            "the Stage 4 boundary reached the LLM unchanged."
        ),
        metrics={
            "assembled_hash": assembled_hash,
            "hard_failure_qid_count": len(hard_qids),
            "rca_cards_grounded_only_count": len(grounded),
            "rca_cards_ungrounded_count": ungrounded_n,
            "top_level_fields": top_fields,
        },
    )


def strategist_context_consumed_record(
    *,
    run_id: str,
    iteration: int,
    consumed_payload: Any,
    assembled_hash: str,
    assembled_top_level_fields: tuple[str, ...] = (),
) -> DecisionRecord:
    """Emit one ``STRATEGIST_CONTEXT_CONSUMED`` record per iter.

    ``consumed_payload`` is the dict that becomes the strategist LLM
    prompt's ``context_json`` (i.e. the output of
    ``optimizer._build_context_data`` after
    ``_truncate_context_to_budget``). ``assembled_hash`` is the value
    stamped on the matching ``STRATEGIST_CONTEXT_ASSEMBLED`` record.
    ``assembled_top_level_fields`` is the corresponding
    ``metrics.top_level_fields`` tuple from that ASSEMBLED record; it
    drives the structural key-set diff (see metrics below).

    The two hashes are not expected to match today — the consumed
    boundary still routes through the legacy kwargs-soup in
    ``_build_context_data``. Drift surfaces in
    ``metrics.drift_detected`` (boolean), in
    ``metrics.keys_only_in_consumed`` / ``metrics.keys_only_in_assembled``
    (structural diff), and ultimately in the persisted
    ``consumed.json`` artifact (Task 8). Subsequent Chunk-A phases will
    narrow the diff; when the strategist prompt finally reads directly
    from ``StrategistContextOutput``, all three signals converge.

    Special case: when ``assembled_hash`` is the empty string (Chunk-A
    flag off, ASSEMBLED record was not emitted), we report
    ``CONTEXT_CONSUMED_MATCHES_ASSEMBLED`` with ``drift_detected=False``
    and leave the structural-diff buckets empty so a missing assembled
    record does not get misclassified as drift.
    """
    consumed_hash = _canonical_sha256(consumed_payload)
    if not assembled_hash:
        drift = False
    else:
        drift = consumed_hash != assembled_hash
    reason = (
        ReasonCode.CONTEXT_CONSUMED_DRIFTED if drift
        else ReasonCode.CONTEXT_CONSUMED_MATCHES_ASSEMBLED
    )
    # Structural key-set diff. Only computed when both sides supply a
    # comparable set: ``assembled_top_level_fields`` from the matching
    # ASSEMBLED record AND a Mapping-typed ``consumed_payload``. When
    # either side is empty/missing (e.g. Chunk-A flag off, no ASSEMBLED
    # record), all three diff buckets remain empty so postmortem can
    # tell "diff not applicable" apart from "all fields drifted".
    keys_only_consumed: tuple[str, ...] = ()
    keys_only_assembled: tuple[str, ...] = ()
    keys_in_both: int = 0
    if assembled_top_level_fields and isinstance(consumed_payload, Mapping):
        consumed_keys = {str(k) for k in consumed_payload.keys()}
        assembled_keys = {str(k) for k in assembled_top_level_fields}
        keys_only_consumed = tuple(sorted(consumed_keys - assembled_keys))
        keys_only_assembled = tuple(sorted(assembled_keys - consumed_keys))
        keys_in_both = len(consumed_keys & assembled_keys)
    return DecisionRecord(
        run_id=str(run_id),
        iteration=int(iteration),
        decision_type=DecisionType.STRATEGIST_CONTEXT_CONSUMED,
        outcome=DecisionOutcome.INFO,
        reason_code=reason,
        evidence_refs=("stage:action_group_selection",),
        expected_effect=(
            "Strategist LLM should consume the exact typed boundary "
            "produced by Stage 4."
        ),
        observed_effect=(
            "consumed_hash differs from assembled_hash"
            if drift else "consumed_hash matches assembled_hash"
        ),
        next_action=(
            "Narrow _build_context_data toward StrategistContextOutput "
            "until the two hashes converge. Diff "
            "stages/04_strategist_context/{output,consumed}.json to "
            "see which fields drifted."
            if drift else
            "No action — Stage 4 boundary reached the LLM unchanged."
        ),
        metrics={
            "assembled_hash": str(assembled_hash or ""),
            "consumed_hash": consumed_hash,
            "drift_detected": bool(drift),
            "keys_only_in_consumed": keys_only_consumed,
            "keys_only_in_assembled": keys_only_assembled,
            "keys_in_both": keys_in_both,
        },
    )
