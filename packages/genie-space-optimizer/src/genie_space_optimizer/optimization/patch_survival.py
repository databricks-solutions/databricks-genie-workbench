"""Per-AG cluster-coverage ledger across proposal-handoff gates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class PatchSurvivalSnapshot:
    ag_id: str
    proposed: list[dict] = field(default_factory=list)
    normalized: list[dict] = field(default_factory=list)
    applyable: list[dict] = field(default_factory=list)
    capped: list[dict] = field(default_factory=list)
    applied: list[dict] = field(default_factory=list)


def _canonical_cluster_id(patch: dict) -> str:
    """Return the canonical cluster id for ledger attribution.

    Priority order matches ``patch_selection._cluster_ids[0]`` so the
    survival ledger and the cap decision rows speak the same lineage:

      1. ``cluster_id`` (scalar)
      2. ``primary_cluster_id`` (scalar)
      3. First of ``source_cluster_ids`` (plural list)
      4. ``source_cluster_id`` (legacy scalar fallback)

    Returns ``""`` when no lineage field is populated. Callers can
    treat that as the "AG-level non-cluster patch" row sentinel.
    """
    cid = str(patch.get("cluster_id") or "").strip()
    if cid:
        return cid
    cid = str(patch.get("primary_cluster_id") or "").strip()
    if cid:
        return cid
    src_ids = patch.get("source_cluster_ids") or []
    if src_ids:
        first = str(src_ids[0] or "").strip()
        if first:
            return first
    return str(patch.get("source_cluster_id") or "").strip()


def _clusters_with_count(patches: Iterable[dict]) -> dict[str, int]:
    """Return per-cluster patch counts using canonical lineage.

    Track 3/E (Phase A burn-down): the reader now matches the cap-side
    cluster identity reader (``patch_selection._cluster_ids``) so a
    patch whose lineage lives in ``source_cluster_ids`` or
    ``primary_cluster_id`` is no longer reported as ``lost_at:normalize``
    in the survival ledger.

    Patches with no cluster lineage at all (AG-level metadata patches
    that do not name a cluster) are bucketed under the empty-string
    key and rendered as a separate ``(ag_level)`` row by Track 3/E
    Task 3E.4.
    """
    counts: dict[str, int] = {}
    for p in patches or []:
        cid = _canonical_cluster_id(p)
        # Note: empty-string cid is allowed and used as the AG-level
        # bucket. Task 3E.4 renders it as ``(ag_level)``.
        counts[cid] = counts.get(cid, 0) + 1
    return counts


# Cycle 15.2 / C12-T5: JSON serialisation of the per-AG survival
# snapshot. Mirrors build_patch_survival_table column-for-column;
# both readers consume the same PatchSurvivalSnapshot so the table
# and the persisted JSON can never disagree.

_GATE_ORDER: tuple[str, ...] = (
    "proposed", "normalized", "applyable", "capped", "applied",
)


def _lost_at_gate(counts: dict[str, int]) -> str:
    """Return the name of the first gate where the cluster's count
    drops to zero. Returns "" if the cluster survives all gates or
    never appeared. The names mirror the ``lost_at:G`` notes in
    ``build_patch_survival_table`` so JSON consumers see the same
    failure point operators saw in stdout.
    """
    prev = 0
    for idx, gate in enumerate(_GATE_ORDER):
        current = int(counts.get(gate, 0))
        if idx == 0:
            prev = current
            continue
        if prev > 0 and current == 0:
            _GATE_LABEL: dict[str, str] = {
                "normalized": "normalize",
                "applyable":  "applyability",
                "applied":    "apply",
            }
            return _GATE_LABEL.get(gate, gate)
        prev = current
    return ""


def build_patch_survival_json_payload(snap: "PatchSurvivalSnapshot") -> dict:
    """Per-AG JSON shape for the bundle contract path.

    Returns ``{"ag_id": str, "clusters": [...]}``. Cluster rows are
    sorted by ``cluster_id``; the empty-string cluster id (AG-level
    metadata patches with no cluster lineage) renders as
    ``"(ag_level)"`` to stay consistent with
    ``build_patch_survival_table``.

    Pure: no I/O, no module-level state, no MLflow imports.
    """
    by_gate: dict[str, dict[str, int]] = {
        "proposed":   _clusters_with_count(snap.proposed),
        "normalized": _clusters_with_count(snap.normalized),
        "applyable":  _clusters_with_count(snap.applyable),
        "capped":     _clusters_with_count(snap.capped),
        "applied":    _clusters_with_count(snap.applied),
    }
    all_cluster_ids = sorted(
        set().union(*(g.keys() for g in by_gate.values()))
    )
    clusters_out: list[dict] = []
    for cid in all_cluster_ids:
        counts = {gate: int(by_gate[gate].get(cid, 0)) for gate in _GATE_ORDER}
        label = cid if cid else "(ag_level)"
        row = {
            "cluster_id": label,
            **counts,
            "lost_at": _lost_at_gate(counts),
        }
        clusters_out.append(row)
    return {"ag_id": str(snap.ag_id), "clusters": clusters_out}


def aggregate_patch_survival_for_iteration(
    *,
    iteration: int,
    per_ag_snapshots: Iterable["PatchSurvivalSnapshot"],
) -> dict:
    """Per-iteration JSON shape for the canonical bundle contract path
    ``gso_postmortem_bundle/iterations/iter_NN/patch_survival.json``.

    Returns ``{"iteration": int, "ags": [<per-AG-payload>, ...]}``.
    AGs are sorted by ``ag_id``; each AG's ``clusters`` list is sorted
    by ``cluster_id`` (see ``build_patch_survival_json_payload``).
    Empty ``per_ag_snapshots`` returns ``{"iteration": N, "ags": []}``
    — a valid, persistable shape for iterations where every AG
    bailed before producing a snapshot.

    Pure: no I/O, no module-level state, no MLflow imports.
    """
    snapshots = list(per_ag_snapshots or [])
    snapshots.sort(key=lambda s: str(s.ag_id))
    return {
        "iteration": int(iteration),
        "ags": [build_patch_survival_json_payload(s) for s in snapshots],
    }


def build_patch_survival_table(snap: PatchSurvivalSnapshot) -> str:
    """Render a fixed-width per-AG patch-survival table.

    Rows: one per cluster_id seen at any gate.
    Columns: proposed, normalized, applyable, capped, applied.
    A cluster that loses all patches at gate G is annotated with
    ``lost_at:G`` so operators can see the failure point at a glance.
    """
    proposed_c = _clusters_with_count(snap.proposed)
    norm_c = _clusters_with_count(snap.normalized)
    appl_c = _clusters_with_count(snap.applyable)
    cap_c = _clusters_with_count(snap.capped)
    applied_c = _clusters_with_count(snap.applied)

    all_clusters = sorted(
        set(proposed_c) | set(norm_c) | set(appl_c) | set(cap_c) | set(applied_c)
    )
    if not all_clusters:
        return ""
    bar = "─" * 88
    lines = [
        f"┌{bar}",
        f"│  PATCH SURVIVAL  ag={snap.ag_id}",
        f"├{bar}",
        f"│  {'cluster':<14}{'proposed':>10}{'normalized':>12}{'applyable':>11}"
        f"{'capped':>9}{'applied':>9}   notes",
    ]
    for cid in all_clusters:
        n_prop = proposed_c.get(cid, 0)
        n_norm = norm_c.get(cid, 0)
        n_appl = appl_c.get(cid, 0)
        n_cap = cap_c.get(cid, 0)
        n_done = applied_c.get(cid, 0)
        notes: list[str] = []
        if n_prop and not n_norm:
            notes.append("lost_at:normalize")
        if n_norm and not n_appl:
            notes.append("lost_at:applyability")
        if n_appl and not n_cap:
            notes.append("lost_at:cap")
        if n_cap and not n_done:
            notes.append("lost_at:apply")
        # Track 3/E — patches with no cluster lineage render under
        # the (ag_level) label so operators can distinguish "lost
        # cluster" from "AG-level metadata patch".
        cid_label = cid if cid else "(ag_level)"
        lines.append(
            f"│  {cid_label:<14}{n_prop:>10}{n_norm:>12}{n_appl:>11}{n_cap:>9}{n_done:>9}   "
            f"{', '.join(notes)}"
        )
    lines.append(f"└{bar}")
    return "\n".join(lines)
