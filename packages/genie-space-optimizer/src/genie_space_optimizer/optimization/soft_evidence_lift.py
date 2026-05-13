"""Phase 3 Action 3.3.2 — lift cluster soft evidence into kit-safety lookup.

This module is the data-flow bridge from Phase 1 Addendum (which
mutates ``cluster["rca_card_supporting_soft_evidence"]`` per the
matcher's output) to Phase 2 Section B (which expects a flat
``soft_evidence_matched_qids_by_kit: dict[str, tuple[str, ...]]``
keyed by kit_id).

Pure function — no I/O, no globals.

A previous implementation lived in ``kit_safety.build_soft_evidence_lookup_by_kit``
and joined kits to clusters by ``target_qids``. This Phase 3 helper is
keyed instead off the per-cluster ``_repair_kit`` stamped by
``apply_repair_planner_to_clusters`` — it requires the planner to have
run but it removes the qid-join ambiguity, so a kit shared by multiple
clusters naturally unions soft evidence from all of them.
"""

from __future__ import annotations

from typing import Iterable


def lift_soft_evidence_to_kit_lookup(
    clusters: Iterable[dict],
) -> dict[str, tuple[str, ...]]:
    """Phase 3 Action 3.3.2 — pure lift.

    Walks ``clusters``, extracts the kit_id from each cluster's
    ``_repair_kit`` (if present), unions the cluster's
    ``rca_card_supporting_soft_evidence`` soft qids into the
    per-kit set, and returns a deterministic-sorted lookup.

    Output shape matches the Phase 2 Section B
    ``select_kit_aware_patch_cap(soft_evidence_matched_qids_by_kit=...)``
    parameter exactly: ``{kit_id: tuple(sorted(soft_qids))}``.

    Clusters without a ``_repair_kit`` are silently skipped — their
    soft evidence remains on the cluster object but cannot be tied to
    a kit_id. Clusters with a kit but no soft evidence contribute
    nothing to the lookup.
    """
    by_kit: dict[str, set[str]] = {}
    for cluster in clusters or ():
        if not isinstance(cluster, dict):
            continue
        kit = cluster.get("_repair_kit")
        if not isinstance(kit, dict):
            continue
        kit_id = str(kit.get("kit_id") or "")
        if not kit_id:
            continue
        evidence = cluster.get("rca_card_supporting_soft_evidence") or ()
        for entry in evidence:
            if not isinstance(entry, dict):
                continue
            soft_qid = str(entry.get("soft_qid") or "")
            if soft_qid:
                by_kit.setdefault(kit_id, set()).add(soft_qid)
    return {kit_id: tuple(sorted(qids)) for kit_id, qids in by_kit.items()}
