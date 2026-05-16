"""Plan 3 — three-stage pipeline orchestrator.

Per-iteration flow (unconditional as of 2026-05-16 — the historical
rollout flag was retired by the dead-flag cleanup):

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

from genie_space_optimizer.optimization.proposal_canonicalize import (
    canonicalize_stage_2_proposal,
)

logger = logging.getLogger(__name__)


# ── Plan 3 / Stage-1 catalogue rendering ─────────────────────────────


def _format_targets_line(kind: str, min_count: Any) -> str:
    """Render the 'Targets:' bullet for the Stage-1 skill catalogue.

    Combines target_kind and target_min_count into LLM-friendly prose:
      kind='base_table', min_count=2  -> '2+ base_table identifiers'
      kind='base_table', min_count=0  -> 'any base_table (empty = all in cluster)'
      kind='mixed',      min_count=0  -> 'any table or metric_view (AG-wide allowed)'
      kind='function',   min_count=0  -> 'any function (empty = all in cluster)'

    Returns '' when metadata is incomplete — caller falls back to the
    3-line block.
    """
    if not kind:
        return ""
    try:
        n = int(min_count)
    except (TypeError, ValueError):
        return ""
    if kind == "mixed":
        if n == 0:
            return "any table or metric_view (AG-wide allowed)"
        return f"{n}+ tables or metric_views"
    if n == 0:
        return f"any {kind} (empty = all in cluster)"
    return f"{n}+ {kind} identifiers"


def _render_rich_skill_catalogue(
    skill_ids: tuple[str, ...] | None = None,
    loader: Any = None,
) -> str:
    """Render the Stage-1 ``{{ skill_catalogue }}`` slot with rich
    per-skill routing aid.

    Each pickable skill becomes a 3-line block:

        - <skill_id>
            What: <description>
            Pick when: <when_to_pick>

    When a skill's SKILL.md is missing ``description`` or
    ``when_to_pick`` frontmatter (regression safety — a new skill
    might land without updating its metadata), the renderer falls
    back to a bare ``- <skill_id>`` bullet for that skill so the rest
    of the catalogue is unaffected.

    Args:
        skill_ids: tuple of skill_ids to render. Defaults to
            ``_THREE_STAGE_SKILL_NAMES`` (production behavior).
            Tests can pass a synthetic tuple to exercise edge cases
            without touching the real registry.
        loader: ``SkillLoader`` instance. Defaults to the
            module-level ``_SKILL_LOADER`` (production behavior).
            Tests can pass a loader pointed at a ``tmp_path`` root.

    Returns:
        Newline-joined string suitable for substitution into the
        Stage-1 prompt's ``{{ skill_catalogue }}`` variable.
    """
    from genie_space_optimizer.common.config import _THREE_STAGE_SKILL_NAMES
    from genie_space_optimizer.skills._loader import _SKILL_LOADER

    if skill_ids is None:
        skill_ids = tuple(sorted(_THREE_STAGE_SKILL_NAMES))
    if loader is None:
        loader = _SKILL_LOADER

    lines: list[str] = []
    for sid in sorted(skill_ids):
        try:
            meta = loader.load_metadata(sid) or {}
        except Exception:
            logger.warning(
                "rich skill catalogue: metadata load failed for %s — "
                "emitting bare-id bullet", sid, exc_info=True,
            )
            lines.append(f"- {sid}")
            continue
        desc = (meta.get("description") or "").strip()
        when = (meta.get("when_to_pick") or "").strip()
        kind = (meta.get("target_kind") or "").strip()
        min_count = meta.get("target_min_count")
        targets_line = _format_targets_line(kind, min_count)
        if desc and when and targets_line:
            lines.append(
                f"- {sid}\n"
                f"    What: {desc}\n"
                f"    Pick when: {when}\n"
                f"    Targets: {targets_line}"
            )
        elif desc and when:
            # Backward compat — target metadata missing, fall back to 3-line block.
            lines.append(
                f"- {sid}\n"
                f"    What: {desc}\n"
                f"    Pick when: {when}"
            )
        else:
            lines.append(f"- {sid}")
    return "\n".join(lines)


# Lever number -> skill_id(s). Sourced from optimizer.py's legacy
# lever-key mapping (see _project_pipeline_to_action_groups's
# skill_to_legacy_key dict). Lever 5 fans out to both 5a + 5b because
# the legacy lever-5 directive could trigger either an instruction
# patch or an example_sql patch depending on cluster shape.
_LEVER_NUMBER_TO_SKILL_IDS: dict[int, tuple[str, ...]] = {
    1: ("lever-1-table-column-description",),
    2: ("lever-2-mv-column-refinement",),
    3: ("lever-3-tvf-routing",),
    4: ("lever-4-join-discovery",),
    5: ("lever-5a-instructions", "lever-5b-example-sql"),
    6: ("lever-6-sql-expression",),
}


def _render_failure_type_routing_table() -> str:
    """Render the deterministic failure_type -> preferred skill_id(s)
    routing table from _ROOT_CAUSE_LEVER_MAP.

    Output is a Markdown pipe table:

        | failure_type            | preferred skill_id(s)                       |
        |-------------------------|---------------------------------------------|
        | missing_join_spec       | lever-4-join-discovery                      |
        | missing_instruction     | lever-5a-instructions or lever-5b-example-sql|
        | wrong_aggregation       | lever-6-sql-expression                      |
        ...

    Treats the table as a prior, not an override — Stage-1 can still
    propose decomposition into multiple picks for compound failures.

    Lever=0 entries (extra_columns_only, select_star) are omitted
    because they intentionally route to no skill (the legacy strategist
    treats them as advisory-only).
    """
    from genie_space_optimizer.optimization.optimizer import (
        _ROOT_CAUSE_LEVER_MAP,
    )

    rows: list[str] = [
        "| failure_type | preferred skill_id(s) |",
        "|---|---|",
    ]
    for failure_type in sorted(_ROOT_CAUSE_LEVER_MAP.keys()):
        lever = _ROOT_CAUSE_LEVER_MAP[failure_type]
        if lever == 0:
            continue
        skill_ids = _LEVER_NUMBER_TO_SKILL_IDS.get(lever)
        if not skill_ids:
            continue
        if len(skill_ids) == 1:
            skill_col = skill_ids[0]
        else:
            skill_col = " or ".join(skill_ids)
        rows.append(f"| {failure_type} | {skill_col} |")
    return "\n".join(rows)


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
        raw_proposals = _call_llm_for_join_discovery(
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
    proposals = [
        canonicalize_stage_2_proposal(
            p, skill_id=bundle.skill_id, target="",
            patch_type="add_join_spec",
        )
        for p in (raw_proposals or [])
        if isinstance(p, dict)
    ]
    return {
        "skill_id": bundle.skill_id,
        "ag_id": bundle.ag_id,
        "proposals": proposals,
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
                proposals.append(canonicalize_stage_2_proposal(
                    p, skill_id=bundle.skill_id, target=target,
                    patch_type=patch_type,
                ))
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
                proposals.append(canonicalize_stage_2_proposal(
                    p, skill_id=bundle.skill_id, target=target,
                    patch_type="add_column_description",
                ))
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
                proposals.append(canonicalize_stage_2_proposal(
                    p, skill_id=bundle.skill_id, target=target,
                    patch_type="add_tvf_description",
                ))
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
        "proposals": [canonicalize_stage_2_proposal(
            {
                "instruction_text": result["instruction_text"],
                "rationale": result.get("rationale", ""),
            },
            skill_id=bundle.skill_id,
            target="",
            patch_type="add_instruction",
        )],
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
        for sub in (per_cluster or []):
            proposals.append(canonicalize_stage_2_proposal(
                sub,
                skill_id=bundle.skill_id,
                target="",
                patch_type="add_example_sql",
            ))
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
            proposals.append(canonicalize_stage_2_proposal(
                p,
                skill_id=bundle.skill_id,
                target=str(p.get("target") or ""),
                patch_type=str(p.get("patch_type") or ""),
            ))
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

    Plan 4 (raw-evidence projection) is unconditionally on as of the
    2026-05-16 dead-flag cleanup; ``ActivationBundle.raw_evidence`` is
    populated by ``build_activation_bundle`` and the dispatcher runs
    once with the bundle as-is.
    """
    from genie_space_optimizer.common.config import (
        _record_three_stage_skill_dispatch,
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

    _record_three_stage_skill_dispatch(bundle.skill_id)
    return adapter(bundle, w)


def _emit_raw_evidence_shadow_comparison(
    ag_id: str,
    skill_id: str,
    n_evidence: int,
    off_proposals: list,
    on_proposals: list,
) -> None:
    """Plan 4 — emit one shadow-comparison record per Stage-2 dispatch.

    The 2026-05-16 dead-flag cleanup removed the live callers of this
    function (the dispatch-time shadow A/B is gone). The body stays
    callable for the structural-diff classifier unit tests; the
    underlying ``_record_raw_evidence_shadow_comparison`` sink no-ops
    unless ``GSO_RAW_EVIDENCE_CAPTURE_PATH`` is set.

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
    )

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

    return {
        "ag_id": ag_id,
        "stage_1_picks": merged_picks,
        "discovery_rationale": discovery_rationale,
        "stage_2_results": stage_2_results,
        "cluster_briefs": list(clusters or []),
        "fallback_to_legacy": False,
    }


def _select_strategy_path_for_iteration(
    legacy_kwargs: dict,
    clusters_for_pipeline: list[dict],
) -> dict:
    """Plan 3 — strategy-path selector. Always routes through the
    three-stage pipeline as of the 2026-05-16 dead-flag cleanup; the
    historical shadow + flag-off branches are retired.

    Returns ``{"source": str, "legacy_action_groups": list,
    "legacy_strategy_full": dict, "pipeline_result": dict | None}``
    where ``source`` is one of:
      * ``three_stage_pipeline`` — pipeline succeeded; pipeline result
        is authoritative.
      * ``legacy_strategist_after_fallback`` — pipeline returned
        ``fallback_to_legacy=True`` (Stage-1 produced zero valid
        picks); the legacy strategist ran as a runtime fallback and
        its action groups were applied.

    The harness call site in ``_run_lever_loop`` consumes
    ``legacy_strategy_full`` directly when ``source`` is
    ``legacy_strategist_after_fallback``. When source is
    ``three_stage_pipeline`` the harness uses the projection helper
    ``_project_pipeline_to_action_groups`` to convert Stage-2 results
    back to ``lever_directives`` shape.
    """
    from genie_space_optimizer.optimization import optimizer

    # AG_PIPELINE is the proxy AG id Stage-1 uses; the most-impacted
    # cluster's root_cause is the proxy root_cause_summary so the
    # discovery prompt has cluster context even when no legacy call
    # has run.
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

    cluster_briefs = pipeline_result.get("cluster_briefs") or []
    source_cluster_ids = [
        str(cb.get("cluster_id", ""))
        for cb in cluster_briefs
        if cb.get("cluster_id")
    ]
    primary_cluster_id = source_cluster_ids[0] if source_cluster_ids else ""

    affected_qids_set: set[str] = set()
    for pick in pipeline_result.get("stage_1_picks", []) or []:
        for qid in pick.get("expected_impact_qids", []) or []:
            if qid:
                affected_qids_set.add(str(qid))
    affected_questions = sorted(affected_qids_set)

    return [{
        "id": pipeline_result.get("ag_id", "AG_PIPELINE"),
        "root_cause_summary": pipeline_result.get("discovery_rationale", ""),
        "source_cluster_ids": source_cluster_ids,
        "primary_cluster_id": primary_cluster_id,
        "affected_questions": affected_questions,
        "lever_directives": lever_directives,
        "coordination_notes": "(generated by three-stage pipeline)",
        "_three_stage_pipeline": True,
    }]
