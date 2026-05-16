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
    return {
        "example_question": example_question,
        "example_sql": example_sql,
        "parameters": parameters,
        "usage_guidance": usage_guidance,
    }
