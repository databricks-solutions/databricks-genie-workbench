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
