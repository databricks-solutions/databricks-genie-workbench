"""Plan 3 — three-stage pipeline orchestrator.

Per-iteration flow (when ``GSO_THREE_STAGE_V1=1``):

    Stage 1: _call_llm_for_stage_1_discovery(ag_id, root_cause_summary,
                clusters, metadata_snapshot, w)
              -> {"applicable_skills": [...], "discovery_rationale": str}

    Stage 2: For each pick (after merge_skill_picks collapses dupes):
              build_activation_bundle(pick, ag_id, clusters, metadata_snapshot)
              _stage_2_for_skill(bundle, w)
              -> {"skill_id": str, "ag_id": str, "proposals": list, ...}

    Stitch: project Stage-2 results back into legacy lever_directives
            shape for downstream apply_patch_set.

Fallback: empty Stage-1 picks → caller invokes
``_call_llm_for_adaptive_strategy`` for the AG (logged with marker
``GSO_DISCOVERY_FALLBACK_V1``).

Adapter pattern: each ``_stage_2_<skill>`` is a thin shim that
translates ``ActivationBundle`` → existing per-lever function input,
calls the function, and wraps the output in the canonical
``{skill_id, ag_id, proposals, ...}`` envelope. Adapters do NOT
modify per-lever functions or their prompts; per-skill prompt
improvements live in Plan 4.

Plan 3 wires only ``lever-4-join-discovery``. Tasks 15-18 add the
remaining adapters.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ── Stage-2 adapters ──────────────────────────────────────────────────


def _patch_type_for_target(target: str) -> tuple[str, str]:
    """Return (patch_type, kind) for a target identifier.

    Heuristic: a target with three or more dotted parts is treated as
    ``catalog.schema.table.column`` → column-level. Two parts (or
    schemaless) → table-level.
    """
    parts = (target or "").split(".")
    if len(parts) >= 4:
        return ("add_column_description", "column")
    return ("add_table_description", "table")


def _stage_2_l4(bundle: "ActivationBundle", w: Any) -> dict:
    """Stage-2 adapter for lever-4-join-discovery.

    Translates the bundle's ``target_objects`` into ``hints`` (the
    list-of-dicts shape ``_call_llm_for_join_discovery`` expects) and
    returns the join_specs in the canonical envelope.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _call_llm_for_join_discovery,
    )

    # Build pairwise hints from target_objects. The legacy hint shape
    # accepts a flat list of dicts with optional left/right table
    # identification; for an N-table target list we emit one hint per
    # ordered pair so the LLM can validate any-to-any joins.
    targets = list(bundle.target_objects)
    hints: list[dict] = []
    if len(targets) >= 2:
        for i in range(len(targets)):
            for j in range(i + 1, len(targets)):
                hints.append({
                    "left_table": targets[i],
                    "right_table": targets[j],
                    "source": "stage_1_discovery",
                })
    elif len(targets) == 1:
        hints.append({"table": targets[0], "source": "stage_1_discovery"})

    try:
        proposals = _call_llm_for_join_discovery(
            bundle.metadata_snapshot, hints, w=w,
            raw_evidence=bundle.raw_evidence,
        )
    except Exception:
        logger.warning(
            "Stage-2 L4 (lever-4-join-discovery) failed for AG=%s",
            bundle.ag_id, exc_info=True,
        )
        return {
            "skill_id": bundle.skill_id,
            "ag_id": bundle.ag_id,
            "proposals": [],
            "error": "L4 LLM call failed",
        }
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": proposals or [],
    }


def _stage_2_l1(bundle: "ActivationBundle", w: Any) -> dict:
    """Stage-2 adapter for lever-1-table-column-description.

    Plan 4: forwards ``bundle.raw_evidence`` to
    ``_call_llm_for_proposal`` as a keyword arg so the per-skill
    prompt sees the rendered ``{{ raw_evidence_block }}`` slot.
    Empty tuple is passed when the bundle has no raw evidence
    (Plan 3 default or Plan 4 flag-off) — call signature stays
    consistent.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _call_llm_for_proposal,
    )
    proposals: list[dict] = []
    for target in (bundle.target_objects or ("",)):
        patch_type, _ = _patch_type_for_target(target)
        for cluster_afs in bundle.cluster_afs:
            try:
                p = _call_llm_for_proposal(
                    cluster_afs, bundle.metadata_snapshot,
                    patch_type, lever=1, w=w,
                    raw_evidence=bundle.raw_evidence,
                )
            except Exception:
                logger.warning(
                    "Stage-2 L1 proposal failed for target=%s AG=%s",
                    target, bundle.ag_id, exc_info=True,
                )
                continue
            if p:
                p = {**p, "_target": target, "_patch_type": patch_type}
                proposals.append(p)
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": proposals,
    }


def _stage_2_l2(bundle: "ActivationBundle", w: Any) -> dict:
    """Stage-2 adapter for lever-2-mv-column-refinement.

    Same shape as L1 but lever=2; restricted (by Stage-1 prompt
    instruction) to MV-column targets.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _call_llm_for_proposal,
    )
    proposals: list[dict] = []
    for target in (bundle.target_objects or ("",)):
        for cluster_afs in bundle.cluster_afs:
            try:
                p = _call_llm_for_proposal(
                    cluster_afs, bundle.metadata_snapshot,
                    "add_column_description", lever=2, w=w,
                    raw_evidence=bundle.raw_evidence,
                )
            except Exception:
                logger.warning(
                    "Stage-2 L2 proposal failed for target=%s AG=%s",
                    target, bundle.ag_id, exc_info=True,
                )
                continue
            if p:
                p = {**p, "_target": target}
                proposals.append(p)
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": proposals,
    }


def _stage_2_l3(bundle: "ActivationBundle", w: Any) -> dict:
    """Stage-2 adapter for lever-3-tvf-routing."""
    from genie_space_optimizer.optimization.optimizer import (
        _call_llm_for_proposal,
    )
    proposals: list[dict] = []
    for target in (bundle.target_objects or ("",)):
        for cluster_afs in bundle.cluster_afs:
            try:
                p = _call_llm_for_proposal(
                    cluster_afs, bundle.metadata_snapshot,
                    "add_tvf_description", lever=3, w=w,
                    raw_evidence=bundle.raw_evidence,
                )
            except Exception:
                logger.warning(
                    "Stage-2 L3 proposal failed for target=%s AG=%s",
                    target, bundle.ag_id, exc_info=True,
                )
                continue
            if p:
                p = {**p, "_target": target}
                proposals.append(p)
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": proposals,
    }


def _stage_2_l5a(bundle: "ActivationBundle", w: Any) -> dict:
    """Stage-2 adapter for lever-5a-instructions (Plan 2)."""
    from genie_space_optimizer.optimization.optimizer import (
        _call_llm_for_lever_5a_instructions,
    )
    # 5a is per-AG (one merged instruction document), not per-cluster.
    # The legacy entry takes the AG's clusters as a list.
    raw_clusters = list(bundle.cluster_afs)  # AFS dicts are fine — lever 5a
                                              # uses _format_cluster_briefs_afs internally.
    try:
        result = _call_llm_for_lever_5a_instructions(
            all_clusters=raw_clusters,
            metadata_snapshot=bundle.metadata_snapshot,
            lever_changes=[],
            w=w,
            raw_evidence=bundle.raw_evidence,
        )
    except Exception:
        logger.warning(
            "Stage-2 L5a failed for AG=%s", bundle.ag_id, exc_info=True,
        )
        return {
            "skill_id": bundle.skill_id, "ag_id": bundle.ag_id,
            "proposals": [], "error": "L5a LLM call failed",
        }
    if not (result.get("instruction_text") or "").strip():
        return {
            "skill_id": bundle.skill_id, "ag_id": bundle.ag_id,
            "proposals": [],
        }
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": [{
            "instruction_text": result["instruction_text"],
            "rationale": result.get("rationale", ""),
        }],
    }


def _stage_2_l5b(bundle: "ActivationBundle", w: Any) -> dict:
    """Stage-2 adapter for lever-5b-example-sql (Plan 2 per-cluster)."""
    from genie_space_optimizer.optimization.optimizer import (
        _dispatch_lever_5b_for_cluster,
    )
    # Plan 2's adapter handles benchmark_corpus internally when None;
    # since the bundle does not carry the raw benchmarks list, pass None
    # (firewall degrades gracefully — see Plan 2 Task 10 docstring).
    proposals: list[dict] = []
    for cluster_afs in bundle.cluster_afs:
        # The adapter takes a cluster dict; AFS dicts are accepted because
        # synthesize_example_sqls calls format_afs(cluster) which is
        # idempotent on already-AFS-shaped dicts.
        per_cluster = _dispatch_lever_5b_for_cluster(
            cluster=dict(cluster_afs),
            metadata_snapshot=bundle.metadata_snapshot,
            w=w,
            benchmark_corpus=None,
        )
        proposals.extend(per_cluster or [])
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": proposals,
    }


def _stage_2_l6(
    bundle: "ActivationBundle",
    w: Any,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
) -> dict:
    """Stage-2 adapter for lever-6-sql-expression.

    Per-cluster fan-out — Lever 6 produces one SQL-expression proposal
    per cluster. Drops ``None`` returns (validation failures).

    Extra kwargs (spark/catalog/gold_schema/warehouse_id/benchmarks)
    are harness-side context — orchestrator threads them via
    ``executor_context`` (Task 19).
    """
    from genie_space_optimizer.optimization.optimizer import (
        _generate_lever6_proposal,
    )
    proposals: list[dict] = []
    for cluster_afs in bundle.cluster_afs:
        try:
            p = _generate_lever6_proposal(
                dict(cluster_afs), bundle.metadata_snapshot,
                strategist_hints=None,
                w=w, spark=spark, catalog=catalog,
                gold_schema=gold_schema, warehouse_id=warehouse_id,
                benchmarks=benchmarks,
                raw_evidence=bundle.raw_evidence,
            )
        except Exception:
            logger.warning(
                "Stage-2 L6 failed for AG=%s", bundle.ag_id, exc_info=True,
            )
            continue
        if p is not None:
            proposals.append(p)
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": proposals,
    }


# ── Dispatcher ────────────────────────────────────────────────────────

# Plan 3 starts with L4 only. Tasks 15-18 add the remaining adapters
# to this table.
_STAGE_2_DISPATCH_TABLE: dict[str, Callable[..., dict]] = {
    "lever-1-table-column-description": _stage_2_l1,
    "lever-2-mv-column-refinement": _stage_2_l2,
    "lever-3-tvf-routing": _stage_2_l3,
    "lever-4-join-discovery": _stage_2_l4,
    "lever-5a-instructions": _stage_2_l5a,
    "lever-5b-example-sql": _stage_2_l5b,
    "lever-6-sql-expression": _stage_2_l6,
}


def _stage_2_for_skill(bundle: "ActivationBundle", w: Any) -> dict:
    """Dispatch one ActivationBundle to its skill's executor.

    Plan 4 wraps Plan 3's dispatcher with shadow-mode logic:

      * Default off (no Plan 4 flags) → single call with bundle as-is.
        Byte-stable with Plan 3.
      * Pipeline mode (``GSO_RAW_EVIDENCE_V1=1``) → single call with
        bundle as-is (which now has populated raw_evidence per
        Plan 4 Task 6).
      * Shadow mode (``GSO_RAW_EVIDENCE_SHADOW_V1=1``) → TWO calls.
        The "OFF" call uses a clone of the bundle with raw_evidence=();
        the "ON" call uses the bundle as-is. The OFF result is
        applied (zero production risk); the comparison record is
        emitted via _emit_raw_evidence_shadow_comparison.

    ``lever-5b-example-sql`` always runs once (its bundle's
    raw_evidence is always () by projector design — there's nothing
    to compare). Same for any unknown skill_id.
    """
    from genie_space_optimizer.common.config import (
        _record_three_stage_skill_dispatch,
        raw_evidence_v1_shadow_enabled,
        three_stage_enabled,
        three_stage_shadow_enabled,
    )

    adapter = _STAGE_2_DISPATCH_TABLE.get(bundle.skill_id)
    if adapter is None:
        logger.warning(
            "Stage-2: no adapter registered for skill_id=%s (AG=%s)",
            bundle.skill_id, bundle.ag_id,
        )
        return {
            "skill_id": bundle.skill_id,
            "ag_id": bundle.ag_id,
            "proposals": [],
            "error": f"no adapter registered for skill_id={bundle.skill_id}",
        }

    if three_stage_enabled() or three_stage_shadow_enabled():
        _record_three_stage_skill_dispatch(bundle.skill_id)

    # Plan 4 shadow: only meaningful when bundle has raw evidence to
    # toggle off. Empty raw_evidence (lever-5b, unknown skill,
    # cluster with no failed-judge questions, GSO_RAW_EVIDENCE_N=0)
    # falls through to the single-call path.
    if raw_evidence_v1_shadow_enabled() and bundle.raw_evidence:
        from dataclasses import replace
        # OFF path: clone bundle with empty evidence; this is what gets applied.
        off_bundle = replace(bundle, raw_evidence=())
        off_result = adapter(off_bundle, w)
        # ON path: bundle as-is; observability only.
        try:
            on_result = adapter(bundle, w)
        except Exception:
            logger.warning(
                "Stage-2 raw-evidence ON path failed (AG=%s skill=%s) "
                "— applying OFF result, no comparison emitted",
                bundle.ag_id, bundle.skill_id, exc_info=True,
            )
            return off_result
        _emit_raw_evidence_shadow_comparison(
            ag_id=bundle.ag_id,
            skill_id=bundle.skill_id,
            n_evidence=len(bundle.raw_evidence),
            off_proposals=off_result.get("proposals") or [],
            on_proposals=on_result.get("proposals") or [],
        )
        return off_result

    return adapter(bundle, w)


def _emit_raw_evidence_shadow_comparison(
    ag_id: str,
    skill_id: str,
    n_evidence: int,
    off_proposals: list,
    on_proposals: list,
) -> None:
    """Plan 4 — emit one shadow-comparison record per Stage-2 dispatch.

    No-op when neither raw-evidence flag is on.

    Schema:
      {
        "ag_id": str,
        "skill_id": str,
        "n_evidence": int,
        "off_proposal_count": int,
        "on_proposal_count": int,
        "off_proposal_keys": [...],   # sorted top-level keys, scrubbed
        "on_proposal_keys": [...],
        "structural_diff": "<one of: identical | count_differs | keys_differ | content_differs | both_empty>",
      }
    """
    from genie_space_optimizer.common.config import (
        _record_raw_evidence_shadow_comparison,
        raw_evidence_v1_enabled,
        raw_evidence_v1_shadow_enabled,
    )
    if not (raw_evidence_v1_enabled() or raw_evidence_v1_shadow_enabled()):
        return

    def _proposal_signature(props: list) -> tuple:
        return tuple(
            tuple(sorted((k for k in p.keys() if isinstance(k, str))))
            for p in props if isinstance(p, dict)
        )

    off_sig = _proposal_signature(off_proposals)
    on_sig = _proposal_signature(on_proposals)

    if not off_proposals and not on_proposals:
        diff = "both_empty"
    elif len(off_proposals) != len(on_proposals):
        diff = "count_differs"
    elif off_sig != on_sig:
        diff = "keys_differ"
    elif off_proposals == on_proposals:
        diff = "identical"
    else:
        diff = "content_differs"

    record = {
        "ag_id": ag_id,
        "skill_id": skill_id,
        "n_evidence": n_evidence,
        "off_proposal_count": len(off_proposals),
        "on_proposal_count": len(on_proposals),
        "off_proposal_keys": [list(s) for s in off_sig],
        "on_proposal_keys": [list(s) for s in on_sig],
        "structural_diff": diff,
    }
    _record_raw_evidence_shadow_comparison(record)


# ── Orchestrator ──────────────────────────────────────────────────────


def run_three_stage_pipeline_for_ag(
    ag_id: str,
    root_cause_summary: str,
    clusters: list[dict],
    metadata_snapshot: dict,
    w: Any,
) -> dict:
    """Plan 3 — orchestrator. Stage-1 discovery → bundle build →
    Stage-2 fan-out → result envelope.

    Returns ``{"ag_id": str, "stage_1_picks": list[dict],
    "discovery_rationale": str, "stage_2_results": list[dict],
    "fallback_to_legacy": bool}``.

    ``fallback_to_legacy=True`` means the caller MUST run
    ``_call_llm_for_adaptive_strategy`` for this AG (logged with
    marker ``GSO_DISCOVERY_FALLBACK_V1``). Triggered when:
      * Stage-1 returned zero valid picks (LLM failure, parse error,
        empty list, or all picks rejected as unknown skill_ids).
    """
    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle, merge_skill_picks,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _call_llm_for_stage_1_discovery,
    )

    discovery = _call_llm_for_stage_1_discovery(
        ag_id=ag_id,
        root_cause_summary=root_cause_summary,
        clusters=clusters or [],
        metadata_snapshot=metadata_snapshot,
        w=w,
    )
    raw_picks = discovery.get("applicable_skills") or []
    discovery_rationale = discovery.get("discovery_rationale", "")

    if not raw_picks:
        logger.info(
            "GSO_DISCOVERY_FALLBACK_V1: AG=%s — Stage-1 returned 0 picks; "
            "falling back to legacy strategist. discovery_rationale=%r",
            ag_id, discovery_rationale[:200],
        )
        return {
            "ag_id": ag_id,
            "stage_1_picks": [],
            "discovery_rationale": discovery_rationale,
            "stage_2_results": [],
            "fallback_to_legacy": True,
        }

    merged_picks = merge_skill_picks(raw_picks)
    stage_2_results: list[dict] = []
    for pick in merged_picks:
        bundle = build_activation_bundle(
            pick=pick,
            ag_id=ag_id,
            clusters=clusters or [],
            metadata_snapshot=metadata_snapshot,
        )
        result = _stage_2_for_skill(bundle, w=w)
        stage_2_results.append(result)

    _maybe_emit_lever5_shadow_for_pipeline(
        ag_id=ag_id,
        clusters=clusters or [],
        metadata_snapshot=metadata_snapshot,
        stage_2_results=stage_2_results,
        w=w,
    )

    return {
        "ag_id": ag_id,
        "stage_1_picks": merged_picks,
        "discovery_rationale": discovery_rationale,
        "stage_2_results": stage_2_results,
        "fallback_to_legacy": False,
    }


def _maybe_emit_lever5_shadow_for_pipeline(
    *,
    ag_id: str,
    clusters: list[dict],
    metadata_snapshot: dict,
    stage_2_results: list[dict],
    w: Any,
) -> None:
    """Track B+ — Plan 2 shadow emission for the three-stage pipeline.

    Closes the gap left by Track B: the pipeline's stage-2 adapters
    (:func:`_stage_2_l5a`, :func:`_stage_2_l5b`) call the L5 skill
    functions directly and bypass :func:`_dispatch_lever_5_split`, so
    the dispatcher's shadow tail never fires for AGs routed through
    the pipeline. Trial-3 ran with Plan 3 shadow on, the L5 skill hit
    counters incremented, but ``shadow_comparisons`` stayed at 0.

    Behavior:
      * If ``GSO_LEVER5_SHADOW_V1`` is off → no-op (skip the holistic
        cost). The shared helper enforces this too; we early-out here
        to also skip aggregation work when not needed.
      * If no L5 skill was picked (only L1/L2/L3/L4/L6 in
        ``stage_2_results``) → no-op. Otherwise every AG would pay
        the holistic LLM cost regardless of whether L5 is in scope.
      * Otherwise aggregate the L5a (``instruction_text``) and L5b
        (``example_sql_proposals``) stage-2 outputs into the same
        envelope shape ``_run_lever5_shadow_emission`` expects, and
        delegate.

    Cost trade-off: when an iteration runs BOTH the legacy strategist
    (selector → dispatcher path with split or shadow on) AND the
    three-stage pipeline (shadow mode), each path emits its own
    record for the same AG. We do not deduplicate at the emission
    boundary because:
      1. The shadow_comparisons counter measures emission attempts,
         not unique AGs — useful for diagnosing pipeline coverage.
      2. The fixture exporter downstream rolls up by content hash,
         so duplicate records do not pollute the byte-stability
         fixture set.
    """
    from genie_space_optimizer.common.config import lever5_shadow_enabled
    if not lever5_shadow_enabled():
        return

    l5_skill_ids = ("lever-5a-instructions", "lever-5b-example-sql")
    l5_results = [
        r for r in (stage_2_results or [])
        if r.get("skill_id") in l5_skill_ids
    ]
    if not l5_results:
        return

    instruction_text = ""
    rationale_parts: list[str] = []
    example_sql_proposals: list[dict] = []
    for r in l5_results:
        proposals = r.get("proposals") or []
        if r.get("skill_id") == "lever-5a-instructions":
            for p in proposals:
                txt = (p.get("instruction_text") or "").strip()
                if txt and not instruction_text:
                    instruction_text = txt
                if p.get("rationale"):
                    rationale_parts.append(f"L5a: {p['rationale']}")
        elif r.get("skill_id") == "lever-5b-example-sql":
            example_sql_proposals.extend(
                p for p in proposals if isinstance(p, dict)
            )
            rationale_parts.append(
                f"L5b: {len(example_sql_proposals)} example SQLs"
            )

    new_result = {
        "instruction_text": instruction_text,
        "example_sql_proposals": example_sql_proposals,
        "rationale": " | ".join(rationale_parts),
    }

    from genie_space_optimizer.optimization.optimizer import (
        _run_lever5_shadow_emission,
    )
    _run_lever5_shadow_emission(
        ag_id=ag_id,
        all_clusters=clusters,
        metadata_snapshot=metadata_snapshot,
        lever_changes=None,
        w=w,
        new_result=new_result,
    )


def _select_strategy_path_for_iteration(
    legacy_kwargs: dict,
    clusters_for_pipeline: list[dict],
) -> dict:
    """Plan 3 — flag-aware selector that decides which strategy path
    runs for one iteration.

    Returns ``{"source": str, "legacy_action_groups": list,
    "legacy_strategy_full": dict, "pipeline_result": dict | None}``
    where ``source`` is one of:
      * ``legacy_strategist`` — both flags off; pipeline never ran.
      * ``three_stage_pipeline`` — pipeline-mode succeeded; pipeline
        result authoritative.
      * ``legacy_strategist_after_fallback`` — pipeline-mode ran and
        returned ``fallback_to_legacy=True``; legacy ran second.
      * ``legacy_strategist_shadow`` — shadow mode; both paths ran
        in parallel; legacy applied; comparison emitted.

    Plan 3 implementation divergence: the plan's verbatim selector
    returned only ``legacy_action_groups`` (action_groups slice).
    To preserve byte-stability of the default-off harness path
    (``global_instruction_rewrite`` and ``rationale`` keys carried
    forward from the legacy strategist output), the selector also
    surfaces ``legacy_strategy_full`` — the full legacy strategist
    dict — so the harness can use it directly for the off / fallback
    / shadow modes. For pipeline-only mode, ``legacy_strategy_full``
    is ``{}`` (no legacy call was made).

    The harness call site in ``_run_lever_loop`` consumes
    ``legacy_strategy_full`` directly when ``source`` is anything but
    ``three_stage_pipeline``. When source is ``three_stage_pipeline``,
    the harness uses the projection helper
    ``_project_pipeline_to_action_groups`` (Task 14) to convert
    Stage-2 results back to ``lever_directives`` shape.
    """
    from genie_space_optimizer.common.config import (
        three_stage_enabled, three_stage_shadow_enabled,
    )
    from genie_space_optimizer.optimization import optimizer

    pipeline_on = three_stage_enabled()
    shadow_on = three_stage_shadow_enabled()

    if shadow_on and not pipeline_on:
        legacy = optimizer._call_llm_for_adaptive_strategy(**legacy_kwargs)
        ag_for_pipeline = (legacy.get("action_groups") or [{}])[0]
        ag_id = str(ag_for_pipeline.get("id", ""))
        rcs = str(ag_for_pipeline.get("root_cause_summary", ""))
        pipeline_result = run_three_stage_pipeline_for_ag(
            ag_id=ag_id,
            root_cause_summary=rcs,
            clusters=clusters_for_pipeline,
            metadata_snapshot=legacy_kwargs.get("metadata_snapshot", {}),
            w=legacy_kwargs.get("w"),
        )
        optimizer._emit_three_stage_shadow_comparison(
            ag_id=ag_id,
            stage_1_picks=pipeline_result.get("stage_1_picks", []),
            legacy_action_groups=legacy.get("action_groups", []),
            pipeline_stage_2_results=pipeline_result.get("stage_2_results", []),
        )
        return {
            "source": "legacy_strategist_shadow",
            "legacy_action_groups": legacy.get("action_groups", []),
            "legacy_strategy_full": legacy,
            "pipeline_result": pipeline_result,
        }

    if pipeline_on:
        # Need an AG ID + root_cause_summary for discovery's prompt;
        # in pipeline-mode we synthesize a stub legacy call ONLY to
        # mine the cluster context — the legacy strategist does NOT
        # produce the applied AGs in pipeline-only mode.
        # To keep the LLM call count to one in pipeline-mode, we use
        # the most-impacted cluster's metadata as a proxy AG context.
        proxy_ag_id = "AG_PIPELINE"
        proxy_rcs = ""
        if clusters_for_pipeline:
            proxy_rcs = str(clusters_for_pipeline[0].get("root_cause", ""))
        pipeline_result = run_three_stage_pipeline_for_ag(
            ag_id=proxy_ag_id,
            root_cause_summary=proxy_rcs,
            clusters=clusters_for_pipeline,
            metadata_snapshot=legacy_kwargs.get("metadata_snapshot", {}),
            w=legacy_kwargs.get("w"),
        )
        if pipeline_result.get("fallback_to_legacy"):
            legacy = optimizer._call_llm_for_adaptive_strategy(**legacy_kwargs)
            return {
                "source": "legacy_strategist_after_fallback",
                "legacy_action_groups": legacy.get("action_groups", []),
                "legacy_strategy_full": legacy,
                "pipeline_result": pipeline_result,
            }
        return {
            "source": "three_stage_pipeline",
            "legacy_action_groups": [],
            "legacy_strategy_full": {},
            "pipeline_result": pipeline_result,
        }

    legacy = optimizer._call_llm_for_adaptive_strategy(**legacy_kwargs)
    return {
        "source": "legacy_strategist",
        "legacy_action_groups": legacy.get("action_groups", []),
        "legacy_strategy_full": legacy,
        "pipeline_result": None,
    }


def _project_pipeline_to_action_groups(pipeline_result: dict) -> list[dict]:
    """Plan 3 — convert a pipeline result envelope back to the legacy
    ``action_groups`` shape so the downstream
    ``apply_patch_set`` consumer (unchanged) sees what it expects.

    Mapping rules:
      * One AG per pipeline_result (the orchestrator runs once per AG).
      * Stage-2 results group by canonical legacy lever key:
          - lever-1-table-column-description → key "1"
          - lever-2-mv-column-refinement     → key "2"
          - lever-3-tvf-routing              → key "3"
          - lever-4-join-discovery           → key "4"
          - lever-5a-instructions            → key "5" (instruction_text only)
          - lever-5b-example-sql             → key "5" (example_sqls only)
          - lever-6-sql-expression           → key "6"
      * Multiple Stage-2 results targeting the same legacy key merge
        their proposals lists (the applier already handles
        list-shaped lever_directives values).
    """
    skill_to_legacy_key: dict[str, str] = {
        "lever-1-table-column-description": "1",
        "lever-2-mv-column-refinement": "2",
        "lever-3-tvf-routing": "3",
        "lever-4-join-discovery": "4",
        "lever-5a-instructions": "5",
        "lever-5b-example-sql": "5",
        "lever-6-sql-expression": "6",
    }

    lever_directives: dict[str, dict] = {}
    for r in pipeline_result.get("stage_2_results", []) or []:
        lk = skill_to_legacy_key.get(r.get("skill_id", ""))
        if lk is None:
            continue
        proposals = r.get("proposals") or []
        bucket = lever_directives.setdefault(lk, {"_pipeline_proposals": []})
        bucket["_pipeline_proposals"].extend(proposals)

    return [{
        "id": pipeline_result.get("ag_id", "AG_PIPELINE"),
        "root_cause_summary": pipeline_result.get("discovery_rationale", ""),
        "source_cluster_ids": [],
        "primary_cluster_id": "",
        "affected_questions": [],
        "lever_directives": lever_directives,
        "coordination_notes": "(generated by three-stage pipeline)",
        "_three_stage_pipeline": True,
    }]
