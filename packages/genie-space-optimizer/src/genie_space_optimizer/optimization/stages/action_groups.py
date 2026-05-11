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

C15 Phase 3: adds JsonRoundTrip to ActionGroupsInput / ActionGroupSlate,
and adds the ForbiddenReason / AdmissionVerdict / ForbiddenAG /
AdmissionTrace admission-trace types. When stage_handlers_chunk_b_enabled()
is on, select() populates ActionGroupSlate.admission_trace from the
forbidden-AG set so the postmortem bundle can surface which AGs were
denied and why (forbidden-AG no-op loop observability).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.decision_emitters import (
    strategist_ag_records,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


STAGE_KEY: str = "action_group_selection"


# ── C15 Phase 3: Admission trace types ────────────────────────────────


class ForbiddenReason(StrEnum):
    """Why an AG was denied admission to the slate.

    Mirrors the rollback_class vocabulary used by
    ``_compute_forbidden_ag_set`` in harness.py so postmortems can
    cross-reference forbidden-AG denials with the reflection buffer
    without re-parsing raw harness logs.
    """
    CONTENT_REGRESSION = "content_regression"
    NO_PROPOSALS = "no_proposals"
    AG_RETIRED = "ag_retired"
    OTHER = "other"


class AdmissionVerdict(StrEnum):
    ADMITTED = "admitted"
    DENIED = "denied"


@dataclass(frozen=True, slots=True)
class ForbiddenAG(JsonRoundTrip):
    """A single forbidden-AG record supplied to ``ActionGroupsInput``.

    ``ag_id`` matches the ``id`` / ``ag_id`` key on candidate AG dicts.
    ``reason`` is the ``ForbiddenReason`` that caused the denial.
    """
    ag_id: str
    reason: ForbiddenReason

    @classmethod
    def from_json(cls, payload: dict) -> "ForbiddenAG":
        return cls(
            ag_id=str(payload["ag_id"]),
            reason=ForbiddenReason(payload["reason"]),
        )


@dataclass(frozen=True, slots=True)
class AdmissionTrace(JsonRoundTrip):
    """Per-candidate admission verdict for a single AG.

    Populated in ``ActionGroupSlate.admission_trace`` when
    ``stage_handlers_chunk_b_enabled()`` is on.  Empty tuple when
    flag is off (byte-stable with legacy behaviour).
    """
    ag_id: str
    verdict: AdmissionVerdict
    denial_reason: str = ""

    @classmethod
    def from_json(cls, payload: dict) -> "AdmissionTrace":
        return cls(
            ag_id=str(payload["ag_id"]),
            verdict=AdmissionVerdict(payload["verdict"]),
            denial_reason=str(payload.get("denial_reason", "")),
        )


# ── C15 Phase 2 Task 4 — root causes that are inherently question-local.
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
class ActionGroupsInput(JsonRoundTrip):
    """Input to stages.action_groups.select.

    ``action_groups`` is the slate of AGs the strategist returned (after
    filtering and buffered-AG drain — F4 doesn't re-do that work).
    ``source_clusters_by_id`` maps cluster id to cluster dict so each
    AG's root_cause can be recovered. ``rca_id_by_cluster`` maps cluster
    id to its RCA id. ``ag_alternatives_by_id`` carries Phase D.5
    rejected-alternatives stamping.

    C15 Phase 3: ``forbidden_ags`` carries the typed forbidden-AG set so
    select() can produce a per-candidate AdmissionTrace when
    ``stage_handlers_chunk_b_enabled()`` is on.
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
    # Cycle 5 T2 — gate-drops carrying a causal-target patch from the
    # prior iteration. Empty unless the prior iteration captured drops.
    # Surfaced to the strategist's prompt context when
    # ``GSO_CAUSAL_DROP_FEEDBACK_TO_STRATEGIST`` is on so the LLM can
    # propose a narrower variant or shift levers instead of re-emitting
    # the same dropped pattern. Typed as ``tuple[Any, ...]`` to avoid a
    # circular import on ``stages.gates.DroppedCausalPatch``.
    prior_iteration_dropped_causal_patches: tuple[Any, ...] = ()
    # C15 Phase 3 — typed forbidden-AG set forwarded from
    # _compute_forbidden_ag_set. Empty unless stage_handlers_chunk_b_enabled().
    # When non-empty, select() records an AdmissionTrace per candidate.
    forbidden_ags: tuple[ForbiddenAG, ...] = ()

    @classmethod
    def from_json(cls, payload: dict) -> "ActionGroupsInput":
        ags = tuple(
            dict(a) for a in (payload.get("action_groups") or [])
        )
        src = {
            str(k): dict(v)
            for k, v in (payload.get("source_clusters_by_id") or {}).items()
        }
        rca_by_cluster = {
            str(k): str(v)
            for k, v in (payload.get("rca_id_by_cluster") or {}).items()
        }
        ag_alts = {
            str(k): tuple(v)
            for k, v in (payload.get("ag_alternatives_by_id") or {}).items()
        }
        buckets = dict(payload.get("prior_buckets_by_qid") or {})
        dropped = tuple(payload.get("prior_iteration_dropped_causal_patches") or [])
        forbidden = tuple(
            ForbiddenAG.from_json(f)
            for f in (payload.get("forbidden_ags") or [])
        )
        return cls(
            action_groups=ags,
            source_clusters_by_id=src,
            rca_id_by_cluster=rca_by_cluster,
            ag_alternatives_by_id=ag_alts,
            prior_buckets_by_qid=buckets,
            prior_iteration_dropped_causal_patches=dropped,
            forbidden_ags=forbidden,
        )


@dataclass
class ActionGroupSlate(JsonRoundTrip):
    """Output of stages.action_groups.select.

    ``ags`` is the selected AG tuple (same content as input but normalized
    to a tuple). ``rejected_ag_alternatives`` records AGs the strategist
    proposed but the constraint/buffer pipeline filtered out, for Phase
    D.5 alternatives capture.

    C15 Phase 3: ``admission_trace`` records per-candidate AdmissionTrace
    entries when stage_handlers_chunk_b_enabled() is on. Empty tuple
    when flag is off (byte-stable with legacy behaviour — zero new fields
    emitted to postmortem bundle unless flag is on).
    """

    ags: tuple[Mapping[str, Any], ...]
    rejected_ag_alternatives: tuple[Mapping[str, Any], ...] = ()
    # C15 Phase 3 — per-candidate admission verdicts. Populated when
    # stage_handlers_chunk_b_enabled() is on; always empty otherwise.
    admission_trace: tuple[AdmissionTrace, ...] = ()

    @classmethod
    def from_json(cls, payload: dict) -> "ActionGroupSlate":
        ags = tuple(
            dict(a) for a in (payload.get("ags") or [])
        )
        rejected = tuple(
            dict(r) for r in (payload.get("rejected_ag_alternatives") or [])
        )
        trace = tuple(
            AdmissionTrace.from_json(t)
            for t in (payload.get("admission_trace") or [])
        )
        return cls(ags=ags, rejected_ag_alternatives=rejected, admission_trace=trace)


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


def normalize_strategist_ags_with_recommended_levers(
    *,
    ags,
    clusters,
):
    """Cycle 11 — union ``cluster.recommended_levers`` into every
    strategist-emit AG's ``lever_directives``. Mirrors the union the
    coverage path performs (control_plane.union_ag_levers_with_recommended)
    so the strategist path stops drifting from cluster RCA.

    Pure. No-op when ``GSO_AG_LEVERS_UNION_STRATEGIST_PATH=0`` or
    ``GSO_AG_LEVERS_UNION_RECOMMENDED=0``.
    """
    from genie_space_optimizer.common.config import (
        ag_levers_union_recommended_enabled,
        ag_levers_union_strategist_path_enabled,
    )
    if not (
        ag_levers_union_recommended_enabled()
        and ag_levers_union_strategist_path_enabled()
    ):
        return list(ags or [])

    from genie_space_optimizer.optimization.control_plane import (
        union_ag_levers_with_recommended,
    )

    cluster_by_id = {
        str(c.get("cluster_id") or ""): c for c in (clusters or [])
        if c.get("cluster_id")
    }
    out = []
    for ag in ags or []:
        src_ids = [
            str(cid) for cid in (ag.get("source_cluster_ids") or [])
            if str(cid)
        ]
        if not src_ids:
            out.append(ag)
            continue
        primary = cluster_by_id.get(src_ids[0]) or {}
        out.append(union_ag_levers_with_recommended(ag=ag, cluster=primary))
    return out


def _build_admission_trace(
    candidates: tuple[Mapping[str, Any], ...],
    forbidden_ags: tuple[ForbiddenAG, ...],
) -> tuple[AdmissionTrace, ...]:
    """C15 Phase 3 — produce per-candidate AdmissionTrace entries.

    Each candidate whose ``id``/``ag_id`` key matches a ``ForbiddenAG``
    gets verdict=DENIED with ``denial_reason`` set to the ForbiddenReason
    value. All other candidates get verdict=ADMITTED.

    Pure; no side effects. Called only when stage_handlers_chunk_b_enabled().
    """
    forbidden_by_id = {f.ag_id: f for f in forbidden_ags}
    trace: list[AdmissionTrace] = []
    for cand in candidates:
        ag_id = str(cand.get("id") or cand.get("ag_id") or "")
        if not ag_id:
            continue
        if ag_id in forbidden_by_id:
            trace.append(AdmissionTrace(
                ag_id=ag_id,
                verdict=AdmissionVerdict.DENIED,
                denial_reason=forbidden_by_id[ag_id].reason.value,
            ))
        else:
            trace.append(AdmissionTrace(ag_id=ag_id, verdict=AdmissionVerdict.ADMITTED))
    return tuple(trace)


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

    C15 Phase 3: when stage_handlers_chunk_b_enabled() is on AND
    ``inp.forbidden_ags`` is non-empty, populates ``ActionGroupSlate.
    admission_trace`` with per-candidate verdicts so the postmortem bundle
    can surface which AGs were denied and why (forbidden-AG no-op loop
    observability). Flag-off behaviour is byte-stable with pre-Phase-3
    runs (admission_trace is always an empty tuple when flag is off).

    RCO-7 Site 2: ``inp.action_groups`` and ``inp.forbidden_ags`` are
    pre-sorted by canonical AG id before any downstream walk so the
    stage's outputs are independent of incidental LLM-output ordering.
    Harness-side sort (Site 1) is the first defense; this is
    defense-in-depth at the stage boundary.
    """
    from genie_space_optimizer.common.config import (
        bucket_driven_ag_selection_enabled,
        stage_handlers_chunk_b_enabled,
    )
    from genie_space_optimizer.optimization.llm_boundary_sort import (
        sort_action_groups_canonically,
    )

    # RCO-7 Site 2 — canonical pre-sort.
    sorted_action_groups = tuple(
        sort_action_groups_canonically(inp.action_groups)
    )
    sorted_forbidden_ags = tuple(
        sorted(inp.forbidden_ags, key=lambda f: f.ag_id)
    )

    if (
        bucket_driven_ag_selection_enabled()
        and inp.prior_buckets_by_qid
    ):
        filtered_ags = tuple(
            _apply_bucket_policy(
                sorted_action_groups,
                buckets_by_qid=inp.prior_buckets_by_qid,
            )
        )
    else:
        filtered_ags = sorted_action_groups

    # Cycle 11 Task 13 — union cluster.recommended_levers into
    # strategist-emit AG lever_directives so the strategist path
    # honours cluster RCA. Closes 7NOW H002 drift.
    filtered_ags = tuple(
        normalize_strategist_ags_with_recommended_levers(
            ags=list(filtered_ags),
            clusters=list(inp.source_clusters_by_id.values())
                if inp.source_clusters_by_id else [],
        )
    )

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

    # C15 Phase 3 — admission trace (chunk_b flag-gated; byte-stable when off).
    # RCO-7 Site 2 — feed the canonically sorted tuples so admission
    # trace order is deterministic.
    admission_trace: tuple[AdmissionTrace, ...] = ()
    if stage_handlers_chunk_b_enabled() and sorted_forbidden_ags:
        admission_trace = _build_admission_trace(
            candidates=sorted_action_groups,
            forbidden_ags=sorted_forbidden_ags,
        )

    return ActionGroupSlate(
        ags=filtered_ags,
        rejected_ag_alternatives=(),
        admission_trace=admission_trace,
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
    has_parent_rca = bool(rca_id)
    return {
        "id": f"AG_COVERAGE_{cluster_id}",
        "ag_id": f"AG_COVERAGE_{cluster_id}",
        # Cycle 5 T3 — split the diagnostic AG kind so the harness can
        # route the no-parent-RCA case to a regeneration step before
        # proposal generation. With parent RCA present (existing AG-1-F
        # path), the AG is ``"diagnostic"`` and proceeds normally;
        # without it, ``"diagnostic_no_parent_rca"`` signals to the
        # harness that ``ag_kind == "diagnostic_no_parent_rca"`` AND
        # ``needs_rca_regeneration is True`` together require an RCA
        # regen attempt before generating proposals.
        "ag_kind": "diagnostic" if has_parent_rca else "diagnostic_no_parent_rca",
        "needs_rca_regeneration": not has_parent_rca,
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
