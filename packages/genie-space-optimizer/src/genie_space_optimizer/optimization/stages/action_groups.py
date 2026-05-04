"""Stage 4: Action Group selection (Phase F4).

Wraps the existing decision_emitters.strategist_ag_records producer in
a typed ActionGroupsInput / ActionGroupSlate surface so F5 (proposals)
can read the slate from a stage-aligned dataclass.

F4 is observability-only: per the plan's Reality Check appendix, the
strategist invocation block in harness.py is a non-contiguous sequence
of inline operations (~300-500 LOC), not a function. Lifting it
inside a single F4 gate is high-risk for byte-stability. F4 stands up
the typed surface and STRATEGIST_AG_EMITTED emission entry; the LLM
invocation, constraint filtering, and buffered-AG draining stay in
harness for now and are deferred to a follow-up plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.decision_emitters import (
    strategist_ag_records,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
)


STAGE_KEY: str = "action_group_selection"


# Cycle 2 Task 4 — root causes that are inherently question-local.
# A single-question cluster with one of these root causes should be
# fixed with a per-question lever, not a space-wide one.
_QUESTION_SHAPE_ROOT_CAUSES: frozenset[str] = frozenset({
    "plural_top_n_collapse",
    "count_vs_distinct",
    "row_ordering_drift",
    "limit_vs_rank",
})

# Cycle 2 Task 4 — default per-question levers. 3 = example_sql
# (benchmark anchor), 5 = instructions narrowed by question_id.
_PER_QUESTION_PREFERRED_LEVERS: tuple[int, ...] = (3, 5)

# Cycle 2 Task 4 — default space-wide levers when no preference fires.
# Includes 6 (SQL expressions) which is appropriate for multi-
# question patterns.
_DEFAULT_RECOMMENDED_LEVERS: tuple[int, ...] = (3, 5, 6)


def recommended_levers_for_cluster(cluster: dict) -> tuple[int, ...]:
    """Cycle 2 Task 4 — return the strategist's preferred lever
    ordering for a cluster.

    When ``GSO_QUESTION_SHAPE_LEVER_PREFERENCE`` is on and the cluster
    has ``q_count == 1`` AND ``root_cause`` is a question-shape root
    cause, returns the per-question lever set (3, 5) WITHOUT lever 6.
    Otherwise returns the default lever set that includes lever 6.
    """
    from genie_space_optimizer.common.config import (
        question_shape_lever_preference_enabled,
    )

    if not question_shape_lever_preference_enabled():
        return _DEFAULT_RECOMMENDED_LEVERS

    qids = cluster.get("question_ids") or []
    q_count = int(cluster.get("q_count") or len(qids) or 0)
    root_cause = str(cluster.get("root_cause") or "")
    if q_count == 1 and root_cause in _QUESTION_SHAPE_ROOT_CAUSES:
        return _PER_QUESTION_PREFERRED_LEVERS
    return _DEFAULT_RECOMMENDED_LEVERS


def stamp_recommended_levers_on_clusters(
    clusters: list[dict],
) -> list[dict]:
    """Cycle 2 Task 4 closeout — stamp ``recommended_levers`` on each
    cluster post-``rank_clusters`` so the strategist's ``ranking_text``
    builder can surface the per-cluster lever hint to the LLM.

    Returns a NEW list of NEW dicts (does not mutate input). Idempotent —
    re-stamping a cluster overwrites the prior ``recommended_levers``
    with the same value.
    """
    out: list[dict] = []
    for cluster in clusters:
        c = dict(cluster)
        c["recommended_levers"] = list(recommended_levers_for_cluster(c))
        out.append(c)
    return out


@dataclass
class ActionGroupsInput:
    """Input to stages.action_groups.select.

    ``action_groups`` is the slate of AGs the strategist returned (after
    filtering and buffered-AG drain — F4 doesn't re-do that work).
    ``source_clusters_by_id`` maps cluster id to cluster dict so each
    AG's root_cause can be recovered. ``rca_id_by_cluster`` maps cluster
    id to its RCA id. ``ag_alternatives_by_id`` carries Phase D.5
    rejected-alternatives stamping.
    """

    action_groups: tuple[Mapping[str, Any], ...]
    source_clusters_by_id: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )
    rca_id_by_cluster: Mapping[str, str] = field(default_factory=dict)
    ag_alternatives_by_id: Mapping[str, Sequence[AlternativeOption]] = field(
        default_factory=dict
    )
    # Optimizer Control-Plane Hardening Plan — Task C. Maps qid -> the
    # bucket the prior iteration assigned. When
    # ``GSO_BUCKET_DRIVEN_AG_SELECTION`` is on, ``select`` drops
    # MODEL_CEILING qids from AG target sets and tags AGs whose targets
    # are all EVIDENCE_GAP with ``ag_kind="evidence_gathering"``.
    prior_buckets_by_qid: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class ActionGroupSlate:
    """Output of stages.action_groups.select.

    ``ags`` is the selected AG tuple (same content as input but normalized
    to a tuple). ``rejected_ag_alternatives`` records AGs the strategist
    proposed but the constraint/buffer pipeline filtered out, for Phase
    D.5 alternatives capture.
    """

    ags: tuple[Mapping[str, Any], ...]
    rejected_ag_alternatives: tuple[Mapping[str, Any], ...] = ()


def _apply_bucket_policy(
    action_groups: tuple[Mapping[str, Any], ...],
    *,
    buckets_by_qid: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Optimizer Control-Plane Hardening Plan — Task C policy.

    Drop ``MODEL_CEILING`` qids from each AG's target set; if the AG
    ends up with no qids, drop the AG entirely. AGs whose remaining
    target qids are all ``EVIDENCE_GAP`` are tagged with
    ``ag_kind="evidence_gathering"`` so the proposal stage emits a
    no-op evidence-gathering proposal rather than mutating the space.
    """
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket,
    )

    out: list[dict[str, Any]] = []
    for ag in action_groups:
        target_qids = tuple(
            str(q) for q in (ag.get("target_qids") or ())
        )
        kept_qids = tuple(
            q for q in target_qids
            if buckets_by_qid.get(q) is not FailureBucket.MODEL_CEILING
        )
        if not kept_qids:
            continue
        new_ag = dict(ag)
        new_ag["target_qids"] = kept_qids
        affected = tuple(
            str(q) for q in (ag.get("affected_questions") or ())
        )
        if affected:
            new_ag["affected_questions"] = tuple(
                q for q in affected
                if buckets_by_qid.get(q) is not FailureBucket.MODEL_CEILING
            ) or kept_qids
        all_evidence_gap = all(
            buckets_by_qid.get(q) is FailureBucket.EVIDENCE_GAP
            for q in kept_qids
        )
        if all_evidence_gap:
            new_ag["ag_kind"] = "evidence_gathering"
        out.append(new_ag)
    return out


def select(ctx, inp: ActionGroupsInput) -> ActionGroupSlate:
    """Stage 4 entry. Emits STRATEGIST_AG_EMITTED records and returns a
    typed slate. F4 is observability-only — does NOT invoke the
    strategist LLM, drain buffered AGs, or apply constraints. Harness
    still owns those steps and feeds the result into ``inp.action_groups``
    when the harness wire-up lands in a follow-up plan.

    Optimizer Control-Plane Hardening Plan — Task C. When
    ``GSO_BUCKET_DRIVEN_AG_SELECTION`` is on AND ``prior_buckets_by_qid``
    is non-empty, the slate is filtered through ``_apply_bucket_policy``
    before STRATEGIST_AG_EMITTED records are produced.
    """
    from genie_space_optimizer.common.config import (
        bucket_driven_ag_selection_enabled,
    )

    if (
        bucket_driven_ag_selection_enabled()
        and inp.prior_buckets_by_qid
    ):
        filtered_ags = tuple(
            _apply_bucket_policy(
                inp.action_groups,
                buckets_by_qid=inp.prior_buckets_by_qid,
            )
        )
    else:
        filtered_ags = tuple(inp.action_groups)

    records = strategist_ag_records(
        run_id=ctx.run_id,
        iteration=ctx.iteration,
        action_groups=filtered_ags,
        source_clusters_by_id=inp.source_clusters_by_id,
        rca_id_by_cluster=inp.rca_id_by_cluster,
        ag_alternatives_by_id=inp.ag_alternatives_by_id,
    )
    for record in records:
        ctx.decision_emit(record)

    return ActionGroupSlate(
        ags=filtered_ags,
        rejected_ag_alternatives=(),
    )


def materialize_diagnostic_ag(
    *,
    cluster: Mapping[str, Any],
    rca_id_by_cluster: Mapping[str, str],
) -> dict[str, Any]:
    """Optimizer Control-Plane Hardening Plan — Task F.

    Build a diagnostic AG for ``cluster`` that inherits its ``rca_id``.

    Used when the strategist did not emit an AG for a hard cluster in
    this iteration but the harness wants to attempt a diagnostic-only
    pass. The inherited ``rca_id`` propagates to every proposal at the
    F5 stage entry (Task D), keeping these proposals out of the
    ``rca_groundedness`` gate's drop set.
    """
    cluster_id = str(cluster.get("id") or "")
    rca_id = str(rca_id_by_cluster.get(cluster_id) or "")
    return {
        "id": f"AG_COVERAGE_{cluster_id}",
        "ag_id": f"AG_COVERAGE_{cluster_id}",
        "ag_kind": "diagnostic",
        "rca_id": rca_id,
        "primary_cluster_id": cluster_id,
        "source_cluster_ids": (cluster_id,),
        "target_qids": tuple(
            str(q) for q in (cluster.get("qids") or ())
        ),
        "affected_questions": tuple(
            str(q) for q in (cluster.get("qids") or ())
        ),
    }


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
INPUT_CLASS = ActionGroupsInput
OUTPUT_CLASS = ActionGroupSlate


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = select
