"""Integration test: ``_select_lever_for_cluster`` plus
``_mark_lever_tried`` produce real lever rotation across iterations
for a MEASURE_SWAP cluster."""

from __future__ import annotations


def _measure_swap_cluster() -> dict:
    return {
        "cluster_id": "C-airline-wrong-agg",
        "root_cause": "wrong_aggregation",
        "asi_failure_type": "wrong_aggregation",
        "asi_blame_set": ["measure:total_revenue"],
        "affected_judge": "logical_accuracy",
        "rca_card": {
            "rca_id": "rca-airline-1",
            "rca_kind": "measure_swap",
        },
    }


def test_measure_swap_rotation_6_then_2_then_5():
    """3-iteration rotation walk-through. Each iter the selector picks
    the next untried lever; after the iter's failure emit, the lever
    is marked tried; the next iter picks the next pair."""
    from genie_space_optimizer.optimization.harness import (
        _mark_lever_tried,
        _select_lever_for_cluster,
    )

    holder: dict = {"tried": {}}
    cluster = _measure_swap_cluster()

    # iter 1 — preferred lever
    lever_1 = _select_lever_for_cluster(cluster, holder)
    assert lever_1 == 6, "preferred MEASURE_SWAP lever is 6 (sql_snippet_measure)"
    _mark_lever_tried(holder, cluster_id=cluster["cluster_id"], lever=lever_1)

    # iter 2 — fallback 1
    lever_2 = _select_lever_for_cluster(cluster, holder)
    assert lever_2 == 2, "first fallback is lever 2 (update_column_description)"
    _mark_lever_tried(holder, cluster_id=cluster["cluster_id"], lever=lever_2)

    # iter 3 — fallback 2
    lever_3 = _select_lever_for_cluster(cluster, holder)
    assert lever_3 == 5, "second fallback is lever 5 (add_example_sql)"
    _mark_lever_tried(holder, cluster_id=cluster["cluster_id"], lever=lever_3)

    # iter 4 — exhausted, falls back to legacy _map_to_lever
    lever_4 = _select_lever_for_cluster(cluster, holder)
    # _ROOT_CAUSE_LEVER_MAP["wrong_aggregation"] == 6 — legacy result.
    assert lever_4 == 6, "after exhaustion, legacy mapping returns 6"


def test_unknown_rca_kind_does_not_rotate():
    """A cluster whose RcaKind resolves to UNKNOWN falls through to
    ``_map_to_lever`` every iteration; marking levers as tried for it
    is harmless (the selector ignores the rotation holder when there's
    no matrix entry)."""
    from genie_space_optimizer.optimization.harness import (
        _mark_lever_tried,
        _select_lever_for_cluster,
    )

    holder: dict = {"tried": {}}
    cluster = {
        "cluster_id": "C-unknown",
        "root_cause": "other",
        "asi_failure_type": "other",
        "asi_blame_set": [],
        "affected_judge": "schema_accuracy",
        "rca_card": {},
    }
    lever_1 = _select_lever_for_cluster(cluster, holder)
    _mark_lever_tried(holder, cluster_id=cluster["cluster_id"], lever=lever_1)
    lever_2 = _select_lever_for_cluster(cluster, holder)
    assert lever_2 == lever_1, "UNKNOWN clusters stay on the legacy lever"


def test_two_clusters_rotate_independently():
    """Marking lever 6 as tried for C1 does NOT affect C2's rotation."""
    from genie_space_optimizer.optimization.harness import (
        _mark_lever_tried,
        _select_lever_for_cluster,
    )

    holder: dict = {"tried": {}}
    c1 = _measure_swap_cluster()
    c1["cluster_id"] = "C1"
    c2 = _measure_swap_cluster()
    c2["cluster_id"] = "C2"

    lever_c1_iter1 = _select_lever_for_cluster(c1, holder)
    _mark_lever_tried(holder, cluster_id="C1", lever=lever_c1_iter1)

    lever_c2_iter1 = _select_lever_for_cluster(c2, holder)
    # C2 has nothing tried — still gets the preferred lever.
    assert lever_c2_iter1 == 6
