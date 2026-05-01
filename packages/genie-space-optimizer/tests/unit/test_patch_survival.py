"""Pin the per-AG patch-survival ledger output."""

from __future__ import annotations

from genie_space_optimizer.optimization.patch_survival import (
    PatchSurvivalSnapshot,
    build_patch_survival_table,
)


def test_survival_table_lists_each_gate_in_order() -> None:
    snap = PatchSurvivalSnapshot(
        ag_id="AG1",
        proposed=[{"proposal_id": "P001", "cluster_id": "H001"}, {"proposal_id": "P002", "cluster_id": "H002"}],
        normalized=[{"proposal_id": "P001", "cluster_id": "H001"}],
        applyable=[{"proposal_id": "P001", "cluster_id": "H001"}],
        capped=[{"proposal_id": "P001", "cluster_id": "H001"}],
        applied=[{"proposal_id": "P001", "cluster_id": "H001"}],
    )
    table = build_patch_survival_table(snap)
    for header in ("PATCH SURVIVAL", "proposed", "normalized", "applyable", "capped", "applied"):
        assert header in table


def test_survival_table_marks_dropped_clusters_per_gate() -> None:
    snap = PatchSurvivalSnapshot(
        ag_id="AG1",
        proposed=[
            {"proposal_id": "P001", "cluster_id": "H001"},
            {"proposal_id": "P002", "cluster_id": "H002"},
            {"proposal_id": "P003", "cluster_id": "H003"},
        ],
        normalized=[
            {"proposal_id": "P001", "cluster_id": "H001"},
            {"proposal_id": "P002", "cluster_id": "H002"},
        ],
        applyable=[{"proposal_id": "P001", "cluster_id": "H001"}],
        capped=[{"proposal_id": "P001", "cluster_id": "H001"}],
        applied=[],
    )
    table = build_patch_survival_table(snap)
    assert "H003" in table
    assert "dropped_at_normalize" in table or "lost_at:normalize" in table
    assert "lost_at:apply" in table or "applied=0" in table
