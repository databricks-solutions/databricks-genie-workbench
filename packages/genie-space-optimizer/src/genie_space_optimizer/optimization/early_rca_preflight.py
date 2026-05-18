"""WU-3 — early RCA preflight + slate decision at lever_per_iter_setup.

The iter body's ``proposal_generation_empty_continue`` (at
``harness.py:24693``) short-circuits before Plan P-D's recovery and
the grounding-gate prelude. Under the airline + 7now anchor failure
shape, the existing recovery wire site is unreachable. This module
provides an early preflight that runs at ``lever_per_iter_setup``
(reachable under both anchors per
``docs/harness/control_flow_sites.md``) so ungrounded clusters get one
regeneration attempt + an authoritative ``SlateAction.SKIP_AG`` verdict
BEFORE lever-6 fires.

Verdict authority: this preflight delegates the verdict to the
**existing** ``decide_slate_action`` from ``slate_consumption.py``.
That function previously had zero callers (dead code); WU-3 makes it
reachable under the failure shape.

Scope note (deliberate): we construct a sentinel ``AdmissionResult``
because the action_groups stage has not run at this site. That
intentionally quiesces ``decide_slate_action``'s precedence-1
(``pivot_signal``) and precedence-2 (``ag_denied_by_admission_trace``)
branches. Only precedence-3 (``cluster_blocked_no_rca``) and the
default ``PROCEED`` fire here. Full slate enforcement —
admission_trace consumption and pivot_signal handling — runs at the
action_groups stage and is out of scope for this WU.

LIVE-REFERENCE contract: the regenerator receives the **live** harness
references to ``cluster`` and ``metadata_snapshot`` — NOT copies —
because ``build_rca_card`` persists the full card object via
``metadata_snapshot["_rca_card_store"]`` and mutates
``cluster["rca_card_id"]`` / ``cluster["rca_card"]`` directly. Copying
either dict drops these mutations and downstream gates lose
``intended_patch_shape``.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, MutableMapping, Sequence

from genie_space_optimizer.optimization.admission_trace_consumer import (
    AdmissionResult,
)
from genie_space_optimizer.optimization.slate_consumption import (
    SlateAction,
    SlateDecision,
    _ag_effective_source_clusters,
    decide_slate_action as _decide_slate_action,
)


def _cluster_id(cluster: Mapping[str, Any]) -> str:
    """Match the cluster-identity fallback used in WU-1."""
    return str(
        cluster.get("primary_cluster_id")
        or cluster.get("cluster_id")
        or ""
    )


def _cluster_has_rca(cluster: Mapping[str, Any]) -> bool:
    """A cluster is grounded iff ``cluster["rca_card"]`` carries a
    non-empty card id. Matches the shape that ``build_rca_card``
    stamps in-place."""
    card = cluster.get("rca_card")
    if not card:
        return False
    if isinstance(card, Mapping):
        return bool(card.get("rca_id") or card.get("card_id"))
    return True


def _sentinel_admission_result(ag: Mapping[str, Any]) -> AdmissionResult:
    """Construct the no-op admission result for the preflight site.

    The action_groups stage has not run yet at lever_per_iter_setup,
    so we cannot have a real admission trace. We pass a sentinel
    with the AG admitted, no denials, no pivot — which makes
    ``decide_slate_action``'s precedence-1 and precedence-2 branches
    quiescent. Only precedence-3 (``cluster_blocked_no_rca``) and the
    default ``PROCEED`` fire on the preflight outcome.
    """
    return AdmissionResult(
        admitted_ags=[dict(ag)],
        denied_ag_ids=(),
        pivot_signal=False,
        first_ag_retired_id="",
    )


def run_early_rca_preflight(
    *,
    ag: Mapping[str, Any],
    clusters: Sequence[MutableMapping[str, Any]],
    spark: Any,
    run_id: str,
    iteration: int,
    metadata_snapshot: MutableMapping[str, Any],
    regenerator: Callable[..., Mapping[str, Any]],
    soft_clusters: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[SlateDecision, tuple[dict, ...]]:
    """Run RCA preflight for ``ag`` against ``clusters``.

    Returns ``(slate_decision, decision_records)``. ``decision_records``
    is a tuple of dict payloads the harness appends to
    ``_current_iter_inputs["decision_records"]`` so the postmortem can
    surface the preflight outcomes.

    Behavior:
      1. Compute the AG's source cluster ids via
         ``_ag_effective_source_clusters`` (shared with the slate
         consumer to avoid drift).
      2. For each ungrounded source cluster, call ``regenerator``
         with the LIVE cluster and LIVE metadata_snapshot. The
         regenerator (``_regenerate_rca_for_cluster``) calls
         ``build_rca_card``, which stamps the card id on
         ``cluster["rca_card_id"]`` and persists the full card on
         ``metadata_snapshot["_rca_card_store"]``.
      3. After the regen pass, build ``blocked_cluster_ids`` from the
         source clusters that remain ungrounded.
      4. Delegate the verdict to ``decide_slate_action`` with a
         sentinel ``AdmissionResult``.

    Mutation contract:
      * ``cluster`` references inside ``clusters`` are mutated in place
        by the regenerator. Callers MUST pass the live harness
        references.
      * ``metadata_snapshot`` is mutated in place by the regenerator
        (the card store stash + any WU-2 SQL corpora carry-through).
    """
    src_ids = _ag_effective_source_clusters(ag)
    if not src_ids:
        return (
            _decide_slate_action(
                ag=ag,
                slate_admitted_ags=(),
                admission_result=_sentinel_admission_result(ag),
                blocked_cluster_ids=(),
            ),
            (),
        )

    cluster_by_id: dict[str, MutableMapping[str, Any]] = {}
    for c in clusters or ():
        if not isinstance(c, MutableMapping):
            continue
        cid = _cluster_id(c)
        if cid:
            cluster_by_id[cid] = c

    records: list[dict] = []
    blocked: list[str] = []

    for cid in sorted(src_ids):  # deterministic order for replay
        cluster = cluster_by_id.get(cid)
        if cluster is None:
            blocked.append(cid)
            records.append({
                "kind": "cluster_blocked_no_rca",
                "iteration": int(iteration),
                "cluster_id": cid,
                "rca_id": "",
                "attempted_sources": ("missing_from_clusters",),
                "reason": "cluster_blocked_no_rca",
            })
            continue
        if _cluster_has_rca(cluster):
            continue

        # Regen attempt. LIVE references — see module docstring.
        try:
            out = regenerator(
                spark=spark,
                run_id=str(run_id or ""),
                cluster=cluster,                       # LIVE.
                metadata_snapshot=metadata_snapshot,   # LIVE.
                soft_clusters=list(soft_clusters) if soft_clusters else None,
            )
        except Exception:
            out = {"rca_id": "", "attempted_sources": ("exception",)}

        rca_id = str((out or {}).get("rca_id") or "")
        attempted = tuple(
            str(s) for s in ((out or {}).get("attempted_sources") or ())
        )

        # Re-read groundedness from the live cluster — build_rca_card
        # mutates in place, so the cluster itself is the source of
        # truth, not the regenerator's return value.
        if rca_id or _cluster_has_rca(cluster):
            records.append({
                "kind": "early_preflight_regen_succeeded",
                "iteration": int(iteration),
                "cluster_id": cid,
                "rca_id": rca_id,
                "attempted_sources": attempted,
                "reason": "rca_regenerated_during_preflight",
            })
        else:
            blocked.append(cid)
            records.append({
                "kind": "cluster_blocked_no_rca",
                "iteration": int(iteration),
                "cluster_id": cid,
                "rca_id": "",
                "attempted_sources": attempted,
                "reason": "cluster_blocked_no_rca",
            })

    decision = _decide_slate_action(
        ag=ag,
        slate_admitted_ags=(),
        admission_result=_sentinel_admission_result(ag),
        blocked_cluster_ids=tuple(blocked),
    )
    return decision, tuple(records)
