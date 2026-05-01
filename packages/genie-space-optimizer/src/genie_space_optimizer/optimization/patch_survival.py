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


def _clusters_with_count(patches: Iterable[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for p in patches or []:
        cid = str(p.get("cluster_id") or p.get("source_cluster_id") or "").strip()
        if not cid:
            continue
        counts[cid] = counts.get(cid, 0) + 1
    return counts


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
        lines.append(
            f"│  {cid:<14}{n_prop:>10}{n_norm:>12}{n_appl:>11}{n_cap:>9}{n_done:>9}   "
            f"{', '.join(notes)}"
        )
    lines.append(f"└{bar}")
    return "\n".join(lines)
