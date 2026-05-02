"""Track 4 — AG decomposition guardrail. Detects AGs that span multiple
unrelated root-cause families or unrelated table families and either
(a) splits them into per-cluster diagnostic AGs when budget cannot
preserve a direct-fix per cluster, or (b) keeps them only when the
patch bundle has an explicit shared direct fix with target coverage
for every cluster.
"""
from __future__ import annotations


def test_ag_root_cause_families_returns_distinct_families() -> None:
    """An AG spanning H001 (plural_top_n_collapse), H002 (missing_filter),
    H003 (column_disambiguation) returns three distinct families.
    """
    from genie_space_optimizer.optimization.control_plane import (
        ag_root_cause_families,
    )

    ag = {
        "id": "AG_BROAD",
        "source_cluster_ids": ["H001", "H002", "H003"],
    }
    clusters = [
        {"cluster_id": "H001", "root_cause": "plural_top_n_collapse"},
        {"cluster_id": "H002", "root_cause": "missing_filter"},
        {"cluster_id": "H003", "root_cause": "column_disambiguation"},
    ]
    families = ag_root_cause_families(ag, clusters)
    assert isinstance(families, frozenset)
    assert families == frozenset({
        "plural_top_n_collapse",
        "missing_filter",
        "column_disambiguation",
    })


def test_ag_table_families_groups_clusters_by_target_table() -> None:
    """An AG whose clusters touch ``cat.sch.fact_a`` and ``cat.sch.fact_b``
    returns two distinct table families.
    """
    from genie_space_optimizer.optimization.control_plane import (
        ag_table_families,
    )

    ag = {
        "id": "AG_BROAD",
        "source_cluster_ids": ["H001", "H002"],
    }
    clusters = [
        {
            "cluster_id": "H001",
            "blame_assets": ["cat.sch.fact_a"],
        },
        {
            "cluster_id": "H002",
            "blame_assets": ["cat.sch.fact_b"],
        },
    ]
    tables = ag_table_families(ag, clusters)
    assert tables == frozenset({"cat.sch.fact_a", "cat.sch.fact_b"})


def test_ag_has_shared_direct_fix_returns_true_when_one_patch_targets_every_cluster() -> None:
    """The exception path: a multi-cluster AG is allowed when its
    patch bundle has at least one direct-fix patch whose target_qids
    cover every cluster's question_ids.
    """
    from genie_space_optimizer.optimization.control_plane import (
        ag_has_shared_direct_fix,
    )

    ag = {
        "id": "AG_BROAD",
        "source_cluster_ids": ["H001", "H002"],
        "patches": [
            {
                "type": "add_sql_snippet_calculation",
                "lever": 5,
                "root_cause": "plural_top_n_collapse",
                "target_qids": ["q1", "q2"],
            },
        ],
    }
    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"]},
        {"cluster_id": "H002", "question_ids": ["q2"]},
    ]
    assert ag_has_shared_direct_fix(ag, clusters) is True


def test_ag_has_shared_direct_fix_returns_false_when_no_patch_covers_all_clusters() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        ag_has_shared_direct_fix,
    )

    ag = {
        "id": "AG_BROAD",
        "source_cluster_ids": ["H001", "H002"],
        "patches": [
            {
                "type": "add_sql_snippet_filter",
                "lever": 6,
                "root_cause": "missing_filter",
                "target_qids": ["q1"],  # H001 only
            },
            {
                "type": "update_column_description",
                "lever": 1,
                "target_qids": ["q2"],  # H002 only — different fix per cluster
            },
        ],
    }
    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"]},
        {"cluster_id": "H002", "question_ids": ["q2"]},
    ]
    assert ag_has_shared_direct_fix(ag, clusters) is False
