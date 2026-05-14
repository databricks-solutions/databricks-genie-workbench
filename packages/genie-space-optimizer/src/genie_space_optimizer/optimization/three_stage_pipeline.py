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


# ── Dispatcher ────────────────────────────────────────────────────────

# Plan 3 starts with L4 only. Tasks 15-18 add the remaining adapters
# to this table.
_STAGE_2_DISPATCH_TABLE: dict[str, Callable[..., dict]] = {
    "lever-4-join-discovery": _stage_2_l4,
}


def _stage_2_for_skill(bundle: "ActivationBundle", w: Any) -> dict:
    """Dispatch one ActivationBundle to its skill's executor.

    Returns the canonical envelope ``{skill_id, ag_id, proposals,
    [error]}``. Unknown skill_id returns empty proposals + error
    string; the orchestrator continues with remaining picks.
    """
    from genie_space_optimizer.common.config import (
        _record_three_stage_skill_dispatch,
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

    return adapter(bundle, w)


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
        "fallback_to_legacy": False,
    }
