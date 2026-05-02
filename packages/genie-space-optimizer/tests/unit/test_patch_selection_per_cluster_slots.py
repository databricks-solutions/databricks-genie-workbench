"""Pin that per-cluster slots fill before relevance ranking."""

from __future__ import annotations

from genie_space_optimizer.optimization.patch_selection import (
    select_target_aware_causal_patch_cap,
)


def _patch(pid: str, cluster_id: str, ptype: str, score: float) -> dict:
    return {
        "proposal_id": pid,
        "cluster_id": cluster_id,
        "type": ptype,
        "target": f"cat.sch.{cluster_id}_tbl",
        "lever": 5 if ptype.startswith("add_sql_snippet") else 1,
        "relevance_score": score,
        "target_qids": [f"q_{cluster_id}"],
    }


def test_per_cluster_slot_floor_reserves_one_per_cluster() -> None:
    """Each active cluster must keep at least one patch when slot floor is set."""
    patches = [
        _patch("P_A1", "H001", "update_column_description", 0.95),
        _patch("P_A2", "H001", "update_column_description", 0.94),
        _patch("P_A3", "H001", "update_column_description", 0.93),
        _patch("P_B1", "H002", "add_sql_snippet_filter", 0.50),
        _patch("P_C1", "H003", "add_sql_snippet_filter", 0.45),
    ]
    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q_H001", "q_H002", "q_H003"),
        max_patches=3,
        active_cluster_ids=("H001", "H002", "H003"),
        per_cluster_slot_floor=1,
    )
    selected_clusters = {p["cluster_id"] for p in selected}
    assert selected_clusters == {"H001", "H002", "H003"}, (
        f"expected one patch per cluster, got {selected_clusters}"
    )


def test_decision_rows_include_score_provenance() -> None:
    """Dropped patches must carry the score vector that determined the drop."""
    patches = [
        _patch("P_A1", "H001", "update_column_description", 0.95),
        _patch("P_B1", "H002", "add_sql_snippet_filter", 0.40),
    ]
    _, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q_H001", "q_H002"),
        max_patches=1,
        active_cluster_ids=("H002",),
        per_cluster_slot_floor=0,
    )
    assert decisions, "decisions must be returned for dropped patches"
    dropped = [d for d in decisions if d.get("decision") == "dropped"]
    assert dropped, "at least one dropped decision expected"
    for d in dropped:
        for key in ("relevance_score", "lever_diversity_tier", "active_cluster_match_tier"):
            assert key in d, f"dropped decision missing {key}"


def test_per_cluster_floor_recognizes_source_cluster_ids_only() -> None:
    """Track 2 — a patch whose only cluster lineage lives in
    ``source_cluster_ids`` (no scalar ``cluster_id``) must still count
    toward its cluster's slot floor. Without this, the cap silently
    drops the higher-relevance direct fix for an active cluster
    because the per-cluster floor cannot see it.

    Test design: with ``max_patches=1`` only Pass 1 (per-cluster slot
    floor) gets to pick. P_GOOD has the higher relevance and is
    attributed to H001 via ``source_cluster_ids`` (this is the shape
    a section-split child gets after Track B propagation). P_BAD has
    the lower relevance but a scalar ``cluster_id``. Without Track 2,
    only P_BAD is visible to the floor and it wins. With Track 2, both
    are visible and P_GOOD wins on relevance.
    """
    patches = [
        # P_BAD has scalar cluster_id but lower relevance.
        {
            "proposal_id": "P_BAD",
            "type": "update_column_description",
            "lever": 1,
            "cluster_id": "H001",
            "target_qids": ["q1"],
            "relevance_score": 0.50,
        },
        # P_GOOD carries cluster lineage only in source_cluster_ids
        # (no scalar cluster_id) — the shape a split-child gets via
        # the MVP plan's PROPOSAL_METADATA_ALLOWLIST. Higher relevance.
        {
            "proposal_id": "P_GOOD",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "source_cluster_ids": ["H001"],
            "target_qids": ["q1"],
            "relevance_score": 0.95,
        },
    ]
    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q_unrelated",),
        max_patches=1,
        active_cluster_ids=("H001",),
        per_cluster_slot_floor=1,
    )

    selected_ids = {p["proposal_id"] for p in selected}
    assert selected_ids == {"P_GOOD"}, (
        f"per-cluster floor failed to see source_cluster_ids lineage; "
        f"got {selected_ids}, expected {{'P_GOOD'}}"
    )


def test_plural_top_n_collapse_qualifies_as_direct_behavior_patch() -> None:
    """Track 2 — SQL-shape failures (plural top-N collapse, missing
    temporal filters) are direct-fix root causes for cap reservation.
    Without this, the only direct fix for a top-N tie collapse never
    earns the global direct-behavior reservation slot.
    """
    from genie_space_optimizer.optimization.patch_selection import (
        _is_direct_behavior_patch,
    )

    sql_shape_fix = {
        "proposal_id": "P_TOPN",
        "type": "add_sql_snippet_calculation",
        "lever": 5,
        "root_cause": "plural_top_n_collapse",
        "target_qids": ["q_top5"],
    }
    assert _is_direct_behavior_patch(sql_shape_fix), (
        "plural_top_n_collapse should be a behavior root cause for "
        "cap reservation; otherwise SQL-shape direct fixes lose to "
        "broad metadata patches at cap"
    )
