"""Plan B — Stage-2 L5b routing through the rich synthesizer.

This module owns the routing decision and the rich-path executor for
``_dispatch_lever_5b_for_cluster``. It is separate from
``forced_synthesis_dispatch.py`` (the Plan A trapdoor) so the two
mechanisms can evolve independently:

  - Plan A's trapdoor: after-the-fact recovery for L5a gate drops and
    "Stage-1 didn't pick L5" safety net.
  - Plan B's primary path: Stage-2 lever-5b for SQL-shape clusters.

The two are orthogonal. Plan A keeps firing for the cases Plan B does
not cover.

Public surface
==============

  - ``should_route_l5b_to_rich_synthesizer(cluster) -> bool`` —
    routing predicate. True iff the flag is on AND the cluster has a
    SQL-shape failure label (per ``cluster_failure_keys`` ∩
    ``_SQL_SHAPE_ROOT_CAUSES``).
  - ``_dispatch_rich_synthesis_for_l5b(cluster, metadata_snapshot, w,
    benchmarks)`` — wraps the rich synthesizer with the L5b output
    contract. Appends declines to ``_L5B_RICH_PATH_DECLINES``.
  - ``_normalize_rich_proposal_to_l5b_shape(proposal)`` — adapts the
    rich synthesizer's proposal dict to the
    ``_dispatch_lever_5b_for_cluster`` return shape.
  - ``drain_l5b_rich_path_declines()`` — pops + returns the current
    ledger (used by the harness after Stage-2 completes).
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Decline ledger (module-level singleton, drained per iteration) ──────
#
# Captures one entry per SQL-shape cluster whose rich-path synthesis
# returned ``proposal=None``. The harness drains this list after Stage-2
# completes and emits one ``NO_STRUCTURAL_CANDIDATE`` decision record
# per entry. We use a module-level list (rather than a context-managed
# ledger) for parity with Plan A's ``_LEVER5_GATE_DROPS``.

_L5B_RICH_PATH_DECLINES: list[dict[str, Any]] = []


def should_route_l5b_to_rich_synthesizer(cluster: Any) -> bool:
    """Return True iff the cluster's L5b synthesis should route to the
    rich synthesizer (Plan B) instead of the lean
    ``synthesize_example_sqls`` adapter.

    Two conditions must both hold:
      1. ``GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE`` is on (per
         ``rich_synthesis_primary_for_sql_shape_enabled``).
      2. The cluster has at least one failure label
         (``asi_failure_type`` or ``root_cause``) in
         ``_SQL_SHAPE_ROOT_CAUSES``.

    Plan A's ``cluster_failure_keys`` returns the union of both labels
    (empties dropped), so the check is a single set membership over
    that tuple.

    Defensive against ``None`` / non-dict input (returns False).
    """
    if not isinstance(cluster, dict):
        return False
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    if not rich_synthesis_primary_for_sql_shape_enabled():
        return False
    # Lazy imports avoid module-import cycles. _SQL_SHAPE_ROOT_CAUSES is
    # defined in optimizer.py because that module owns the structural-
    # gate constants; cluster_failure_keys lives in
    # forced_synthesis_dispatch.py (Plan A's helper).
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )
    from genie_space_optimizer.optimization.optimizer import (
        _SQL_SHAPE_ROOT_CAUSES,
    )
    for key in cluster_failure_keys(cluster):
        if key in _SQL_SHAPE_ROOT_CAUSES:
            return True
    return False


def _normalize_rich_proposal_to_l5b_shape(
    proposal: Any,
) -> dict[str, Any] | None:
    """Adapt the rich synthesizer's proposal dict to the four-field shape
    that ``_dispatch_lever_5b_for_cluster`` currently returns.

    The lean L5b path returns dicts like::

        {
            "example_question": str,
            "example_sql": str,
            "parameters": list,
            "usage_guidance": str,
        }

    The rich synthesizer returns a proposal carrying many more fields
    (patch_type, rationale, _archetype_name, provenance, ...) that the
    downstream Stage-2 canonicalizer adds back. We strip to the four
    canonical fields so the upstream
    ``canonicalize_stage_2_proposal(sub, ..., patch_type="add_example_sql")``
    call in ``_stage_2_l5b`` works unchanged.

    Falls back to ``rationale`` when ``usage_guidance`` is empty, mirroring
    the lean path's behaviour at ``optimizer.py:9538-9539``.

    Returns ``None`` when the proposal is missing required fields
    (``example_question`` or ``example_sql``) — caller treats this as a
    decline and appends to ``_L5B_RICH_PATH_DECLINES``.
    """
    if not isinstance(proposal, dict):
        return None
    example_question = str(proposal.get("example_question") or "").strip()
    example_sql = str(proposal.get("example_sql") or "").strip()
    if not example_question or not example_sql:
        return None
    usage_guidance = (
        str(proposal.get("usage_guidance") or "").strip()
        or str(proposal.get("rationale") or "").strip()
    )
    parameters = proposal.get("parameters") or []
    if not isinstance(parameters, list):
        parameters = []
    normalized: dict[str, Any] = {
        "example_question": example_question,
        "example_sql": example_sql,
        "parameters": parameters,
        "usage_guidance": usage_guidance,
    }
    # Plan 8 Task 7 — preserve _archetype_name on the normalized dict so
    # _dispatch_rich_synthesis_for_l5b can stamp the typed RepairIntent.
    archetype_name = str(proposal.get("_archetype_name") or "")
    if archetype_name:
        normalized["_archetype_name"] = archetype_name
    return normalized


def _dispatch_rich_synthesis_for_l5b(
    *,
    cluster: dict,
    metadata_snapshot: dict,
    w: Any,
    benchmarks: list[dict] | None,
    _synthesize: Any = None,
) -> list[dict]:
    """Plan B — rich-path executor for Stage-2 L5b.

    Wraps ``run_cluster_driven_synthesis_for_single_cluster`` with the
    output contract that ``_dispatch_lever_5b_for_cluster`` enforces:

      - On success: returns ``[normalized_proposal_dict]`` (one entry).
      - On decline (proposal=None OR normalize_returns_none OR
        synthesizer raised): returns ``[]`` AND appends a record to
        ``_L5B_RICH_PATH_DECLINES``.

    The decline ledger captures everything the harness needs to emit
    a typed ``NO_STRUCTURAL_CANDIDATE`` decision record.

    ``_synthesize`` is the rich synthesizer; defaults to the production
    callable resolved lazily to avoid circular imports. Tests pass a
    stub.

    ``benchmarks`` may be ``None`` (Stage-2 L5b's bundle does not carry
    raw benchmarks today). The rich synthesizer accepts ``None`` and
    degrades the leakage gate to a permissive corpus.
    """
    cluster_id = str((cluster or {}).get("cluster_id") or "?")

    if _synthesize is None:
        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            run_cluster_driven_synthesis_for_single_cluster as _default_synth,
        )
        _synthesize = _default_synth

    try:
        result = _synthesize(
            cluster,
            metadata_snapshot,
            benchmarks=benchmarks,
            w=w,
        )
    except Exception:
        logger.exception(
            "L5b rich-path synthesis raised for cluster=%s; recording decline",
            cluster_id,
        )
        _L5B_RICH_PATH_DECLINES.append(
            _build_decline_record(
                cluster=cluster,
                attempted_archetypes=(),
                skipped_reason="exception",
            )
        )
        return []

    attempted = tuple(result.attempted_archetypes or ())

    if result.proposal is None:
        _L5B_RICH_PATH_DECLINES.append(
            _build_decline_record(
                cluster=cluster,
                attempted_archetypes=attempted,
                skipped_reason=str(result.skipped_reason or "no_proposal"),
            )
        )
        logger.info(
            "L5b rich-path declined cluster=%s archetypes=%s reason=%s",
            cluster_id, attempted, result.skipped_reason,
        )
        return []

    normalized = _normalize_rich_proposal_to_l5b_shape(result.proposal)
    if normalized is None:
        _L5B_RICH_PATH_DECLINES.append(
            _build_decline_record(
                cluster=cluster,
                attempted_archetypes=attempted,
                skipped_reason="normalize_returned_none",
            )
        )
        logger.warning(
            "L5b rich-path proposal missing required fields for cluster=%s "
            "(example_question/example_sql); recording decline",
            cluster_id,
        )
        return []

    proposals = [normalized]
    # Plan 8 Task 7 — stamp the typed RepairIntent on every rich fallback
    # proposal so ProposalSlate.repair_intents_by_id is non-empty when
    # the LLM intent short-circuit declines.
    try:
        from genie_space_optimizer.optimization.archetypes import ARCHETYPES
        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            stamp_proposals_from_archetype,
        )
        from genie_space_optimizer.optimization.failure_cluster import (
            FailureCluster,
        )
        fc = FailureCluster.from_legacy(cluster)
        by_arch: dict[str, list[dict[str, Any]]] = {}
        for p in proposals:
            name = str(p.get("_archetype_name") or "")
            if name:
                by_arch.setdefault(name, []).append(p)
        ag_id_value = str(
            getattr(cluster, "ag_id", "")
            or (cluster.get("ag_id") if isinstance(cluster, dict) else "")
            or ""
        )
        for arch_name, batch in by_arch.items():
            arch = next(
                (a for a in ARCHETYPES if a.name == arch_name), None,
            )
            if arch is None:
                continue
            # The normalize step strips patch_type; stamp_proposals_from_archetype
            # requires patch_type to match archetype.patch_type, so set it
            # here from the archetype before stamping.
            for proposal_dict in batch:
                proposal_dict.setdefault(
                    "patch_type", str(arch.patch_type),
                )
            stamp_proposals_from_archetype(
                proposals=batch, archetype=arch, cluster=fc,
                ag_id=ag_id_value,
            )
    except Exception:
        logger.debug(
            "Plan 8 Task 7 — rich fallback stamp failed (non-fatal)",
            exc_info=True,
        )
    return proposals


def _build_decline_record(
    *,
    cluster: dict,
    attempted_archetypes: tuple[str, ...],
    skipped_reason: str,
    intent_decline_reason: str | None = None,
) -> dict[str, Any]:
    """Construct one entry for ``_L5B_RICH_PATH_DECLINES``.

    Plan 5 (2026-05-19) — ``intent_decline_reason`` (optional) records
    the typed Plan-5 LLM decline reason (e.g. ``"ambiguous_failure"``,
    ``"blame_set_not_in_identifier_allowlist"``) so postmortems can
    distinguish Plan-5 declines from no-Archetype declines uniformly.
    Existing callers that don't pass it produce records with the field
    set to ``None`` — byte-stable for downstream consumers that read
    only the original fields.
    """
    qids = cluster.get("question_ids") or []
    if not isinstance(qids, (list, tuple)):
        qids = []
    return {
        "cluster_id": str(cluster.get("cluster_id") or ""),
        "root_cause": str(cluster.get("root_cause") or ""),
        "asi_failure_type": str(cluster.get("asi_failure_type") or ""),
        "attempted_archetypes": tuple(str(a) for a in attempted_archetypes),
        "skipped_reason": str(skipped_reason),
        "question_ids": tuple(str(q) for q in qids if str(q).strip()),
        "intent_decline_reason": intent_decline_reason,
    }


def drain_l5b_rich_path_declines() -> list[dict[str, Any]]:
    """Pop and return the current decline ledger.

    The harness calls this once per iteration AFTER Stage-2 completes,
    converts each entry into a typed ``NO_STRUCTURAL_CANDIDATE``
    decision record, emits the corresponding
    ``GSO_NO_STRUCTURAL_CANDIDATE_V1`` stdout marker, and continues.

    Drain is destructive — once returned, the entries are removed from
    module state. Idempotent for the harness's purposes (calling twice
    just returns an empty list the second time).
    """
    drained = list(_L5B_RICH_PATH_DECLINES)
    _L5B_RICH_PATH_DECLINES.clear()
    return drained
