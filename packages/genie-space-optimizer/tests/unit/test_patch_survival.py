"""Pin the per-AG patch-survival ledger output."""

from __future__ import annotations

from genie_space_optimizer.optimization.patch_survival import (
    PatchSurvivalSnapshot,
    _clusters_with_count,
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


# ── Track 3/E ────────────────────────────────────────────────────────


def test_clusters_with_count_reads_source_cluster_ids() -> None:
    """A patch whose only lineage lives in ``source_cluster_ids`` (the
    shape produced by Track B's split-child stamping in the MVP plan)
    must contribute to the count for that cluster.
    """
    patches = [
        {"proposal_id": "P_A", "source_cluster_ids": ["H001"]},
    ]
    counts = _clusters_with_count(patches)
    assert counts == {"H001": 1}, (
        f"_clusters_with_count missed source_cluster_ids; got {counts}"
    )


def test_clusters_with_count_reads_primary_cluster_id() -> None:
    """A patch with lineage only in ``primary_cluster_id`` (used by the
    cap-attribution path's primary-cluster reservation) must also count.
    """
    patches = [
        {"proposal_id": "P_B", "primary_cluster_id": "H002"},
    ]
    counts = _clusters_with_count(patches)
    assert counts == {"H002": 1}, (
        f"_clusters_with_count missed primary_cluster_id; got {counts}"
    )


def test_clusters_with_count_uses_priority_when_multiple_fields_present() -> None:
    """When multiple lineage fields are populated, the canonical id is
    the first non-empty value in the priority order: ``cluster_id`` ->
    ``primary_cluster_id`` -> first of ``source_cluster_ids`` ->
    ``source_cluster_id``. This matches ``_cluster_ids`` in
    ``patch_selection.py``.
    """
    patches = [
        {
            "proposal_id": "P_C",
            "cluster_id": "H_PRIMARY",
            "primary_cluster_id": "H_NOT_USED",
            "source_cluster_ids": ["H_ALSO_NOT_USED"],
        },
    ]
    counts = _clusters_with_count(patches)
    assert counts == {"H_PRIMARY": 1}, (
        f"priority order broken; expected cluster_id wins, got {counts}"
    )


def test_ag_level_patches_with_no_cluster_lineage_render_their_own_row() -> None:
    """Track 3/E — patches with no cluster lineage (AG-level metadata
    patches the strategist proposes against the AG as a whole) must
    appear as their own ``(ag_level)`` row in the survival ledger,
    not silently disappear.
    """
    snap = PatchSurvivalSnapshot(
        ag_id="AG_TEST",
        proposed=[
            {"proposal_id": "P_AG_LEVEL", "type": "update_global_setting"},
            {"proposal_id": "P_H001", "source_cluster_ids": ["H001"]},
        ],
        normalized=[
            {"proposal_id": "P_AG_LEVEL", "type": "update_global_setting"},
            {"proposal_id": "P_H001", "source_cluster_ids": ["H001"]},
        ],
        applyable=[
            {"proposal_id": "P_AG_LEVEL", "type": "update_global_setting"},
            {"proposal_id": "P_H001", "source_cluster_ids": ["H001"]},
        ],
        capped=[
            {"proposal_id": "P_AG_LEVEL", "type": "update_global_setting"},
            {"proposal_id": "P_H001", "source_cluster_ids": ["H001"]},
        ],
        applied=[
            {"proposal_id": "P_AG_LEVEL", "type": "update_global_setting"},
            {"proposal_id": "P_H001", "source_cluster_ids": ["H001"]},
        ],
    )
    table = build_patch_survival_table(snap)

    assert "(ag_level)" in table, (
        "AG-level patches with no cluster lineage must render as "
        "(ag_level) row; ledger output:\n" + table
    )
    assert "H001" in table, "expected H001 cluster row"
    ag_level_line = next(
        (line for line in table.splitlines() if "(ag_level)" in line),
        "",
    )
    assert "lost_at" not in ag_level_line, (
        f"AG-level row incorrectly marked as lost; line: {ag_level_line!r}"
    )


def test_lost_at_normalize_must_not_fire_when_descendant_patches_applied() -> None:
    """Track 3 — when a parent rewrite proposal is normalized into K
    split-children for the same cluster lineage and any of those
    children land in the applied set, the cluster cannot be reported
    as ``lost_at:normalize``.
    """
    snap = PatchSurvivalSnapshot(
        ag_id="AG_REWRITE",
        proposed=[
            {"proposal_id": "P_REWRITE", "source_cluster_ids": ["H001"]},
        ],
        normalized=[
            # Expanded children at normalize gate — same lineage.
            {"proposal_id": "P_REWRITE#1", "source_cluster_ids": ["H001"],
             "_split_from": "rewrite_instruction",
             "parent_proposal_id": "P_REWRITE"},
            {"proposal_id": "P_REWRITE#2", "source_cluster_ids": ["H001"],
             "_split_from": "rewrite_instruction",
             "parent_proposal_id": "P_REWRITE"},
        ],
        applyable=[
            {"proposal_id": "P_REWRITE#1", "source_cluster_ids": ["H001"],
             "_split_from": "rewrite_instruction",
             "parent_proposal_id": "P_REWRITE"},
            {"proposal_id": "P_REWRITE#2", "source_cluster_ids": ["H001"],
             "_split_from": "rewrite_instruction",
             "parent_proposal_id": "P_REWRITE"},
        ],
        capped=[
            {"proposal_id": "P_REWRITE#1", "source_cluster_ids": ["H001"],
             "_split_from": "rewrite_instruction",
             "parent_proposal_id": "P_REWRITE"},
        ],
        applied=[
            {"proposal_id": "P_REWRITE#1", "source_cluster_ids": ["H001"],
             "_split_from": "rewrite_instruction",
             "parent_proposal_id": "P_REWRITE"},
        ],
    )
    table = build_patch_survival_table(snap)
    h001_line = next(
        (line for line in table.splitlines() if "│  H001" in line),
        "",
    )
    assert h001_line, f"missing H001 row in ledger:\n{table}"
    assert "lost_at:normalize" not in h001_line, (
        "H001 row marked lost_at:normalize despite descendant patches "
        f"applied for the same cluster; row: {h001_line!r}"
    )
