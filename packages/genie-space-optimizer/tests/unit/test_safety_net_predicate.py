"""Unit tests for ``_should_invoke_safety_net`` — Plan A Part 2.

The predicate fires when ALL of these hold:

1. ``l5_ag_drops`` is empty (no structural-gate drops for this AG).
2. Lever 5 emitted zero proposals for this AG (``ag_proposals_so_far``
   contains no ``add_example_sql`` / ``update_example_sql`` patches).
3. At least one of the AG's source clusters has a failure label in
   ``_SQL_SHAPE_ROOT_CAUSES`` (checked via ``cluster_failure_keys`` so
   asi_failure_type and root_cause are both considered).
"""
from __future__ import annotations


def _cluster_with(*, cluster_id: str, root_cause: str = "", asi: str = "") -> dict:
    return {
        "cluster_id": cluster_id,
        "root_cause": root_cause,
        "asi_failure_type": asi,
        "question_ids": [f"q_{cluster_id}_001"],
    }


def test_fires_when_no_drops_no_l5_proposals_sql_shape_cluster() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        _should_invoke_safety_net,
    )

    ag = {
        "id": "AG_DECOMPOSED_H002",
        "source_cluster_ids": ["H002"],
    }
    clusters_by_id = {
        "H002": _cluster_with(
            cluster_id="H002",
            root_cause="plural_top_n_collapse",
            asi="wrong_aggregation",
        ),
    }
    ag_proposals = [
        {"patch_type": "add_text_instruction", "lever": 6},
    ]
    triggers = _should_invoke_safety_net(
        ag=ag,
        l5_ag_drops=[],
        iter_source_clusters_by_id=clusters_by_id,
        ag_proposals_so_far=ag_proposals,
    )
    assert triggers == [("H002", "wrong_aggregation")]


def test_does_not_fire_when_l5_drops_present() -> None:
    """If the structural gate fired (l5_ag_drops non-empty), the
    existing label-canonical dispatch covers the case — safety net
    must NOT also fire (it would double-synthesize).
    """
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        _should_invoke_safety_net,
    )

    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
    }
    clusters_by_id = {
        "H001": _cluster_with(
            cluster_id="H001",
            root_cause="plural_top_n_collapse",
            asi="wrong_aggregation",
        ),
    }
    drops = [{
        "ag_id": "AG_DECOMPOSED_H001",
        "source_clusters": ("H001",),
        "root_causes": ("wrong_aggregation",),
        "target_lever": 5,
    }]
    triggers = _should_invoke_safety_net(
        ag=ag,
        l5_ag_drops=drops,
        iter_source_clusters_by_id=clusters_by_id,
        ag_proposals_so_far=[],
    )
    assert triggers == []


def test_does_not_fire_when_l5_already_emitted_a_proposal() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        _should_invoke_safety_net,
    )

    ag = {
        "id": "AG_DECOMPOSED_H002",
        "source_cluster_ids": ["H002"],
    }
    clusters_by_id = {
        "H002": _cluster_with(
            cluster_id="H002",
            root_cause="plural_top_n_collapse",
        ),
    }
    ag_proposals = [
        {"patch_type": "add_example_sql", "lever": 5},
    ]
    triggers = _should_invoke_safety_net(
        ag=ag,
        l5_ag_drops=[],
        iter_source_clusters_by_id=clusters_by_id,
        ag_proposals_so_far=ag_proposals,
    )
    assert triggers == []


def test_update_example_sql_also_counts_as_l5_emitted() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        _should_invoke_safety_net,
    )

    ag = {
        "id": "AG_DECOMPOSED_H002",
        "source_cluster_ids": ["H002"],
    }
    clusters_by_id = {
        "H002": _cluster_with(cluster_id="H002", root_cause="wrong_aggregation"),
    }
    ag_proposals = [
        {"patch_type": "update_example_sql"},
    ]
    triggers = _should_invoke_safety_net(
        ag=ag,
        l5_ag_drops=[],
        iter_source_clusters_by_id=clusters_by_id,
        ag_proposals_so_far=ag_proposals,
    )
    assert triggers == []


def test_does_not_fire_when_no_cluster_has_sql_shape_root_cause() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        _should_invoke_safety_net,
    )

    ag = {
        "id": "AG_GENERIC",
        "source_cluster_ids": ["G001"],
    }
    clusters_by_id = {
        "G001": _cluster_with(
            cluster_id="G001",
            root_cause="ambiguous_question",
            asi="ambiguity",
        ),
    }
    triggers = _should_invoke_safety_net(
        ag=ag,
        l5_ag_drops=[],
        iter_source_clusters_by_id=clusters_by_id,
        ag_proposals_so_far=[],
    )
    assert triggers == []


def test_returns_one_trigger_per_sql_shape_cluster_in_order() -> None:
    """When the AG has multiple SQL-shape source clusters, the predicate
    returns one trigger per cluster, in the order ``source_cluster_ids``
    declares them. Caller decides whether to dispatch all or stop after
    the first viable.
    """
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        _should_invoke_safety_net,
    )

    ag = {
        "id": "AG_MULTI",
        "source_cluster_ids": ["H001", "G001", "H002"],
    }
    clusters_by_id = {
        "H001": _cluster_with(cluster_id="H001", root_cause="wrong_aggregation"),
        "G001": _cluster_with(cluster_id="G001", root_cause="ambiguous_question"),
        "H002": _cluster_with(
            cluster_id="H002",
            root_cause="plural_top_n_collapse",
            asi="wrong_filter_condition",
        ),
    }
    triggers = _should_invoke_safety_net(
        ag=ag,
        l5_ag_drops=[],
        iter_source_clusters_by_id=clusters_by_id,
        ag_proposals_so_far=[],
    )
    # H001 fires on its root_cause "wrong_aggregation"; G001 is skipped
    # (not SQL-shape); H002 fires on its asi "wrong_filter_condition"
    # (asi_failure_type is checked first by cluster_failure_keys order).
    assert triggers == [
        ("H001", "wrong_aggregation"),
        ("H002", "wrong_filter_condition"),
    ]


def test_returns_empty_when_ag_has_no_source_clusters() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        _should_invoke_safety_net,
    )

    ag = {"id": "AG_EMPTY", "source_cluster_ids": []}
    triggers = _should_invoke_safety_net(
        ag=ag,
        l5_ag_drops=[],
        iter_source_clusters_by_id={},
        ag_proposals_so_far=[],
    )
    assert triggers == []
