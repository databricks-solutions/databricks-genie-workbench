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


def test_decompose_overbroad_ag_splits_into_per_cluster_diagnostic_ags() -> None:
    """When an AG spans multiple root-cause families AND the patch
    bundle has no shared direct fix, ``decompose_overbroad_ag`` must
    return a list of per-cluster diagnostic AGs, one for each source
    cluster.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decompose_overbroad_ag,
    )

    ag = {
        "id": "AG_OVERBROAD",
        "source_cluster_ids": ["H001", "H002"],
        "affected_questions": ["q1", "q2"],
        "patches": [
            {
                "type": "add_sql_snippet_filter",
                "lever": 6,
                "root_cause": "missing_filter",
                "target_qids": ["q1"],
            },
            {
                "type": "update_column_description",
                "lever": 1,
                "target_qids": ["q2"],
            },
        ],
        "lever_directives": {
            "5": {"root_cause": "plural_top_n_collapse"},
        },
    }
    clusters = [
        {
            "cluster_id": "H001",
            "cluster_signature": "plural_top_n_collapse|fact_a|year",
            "question_ids": ["q1"],
            "root_cause": "plural_top_n_collapse",
        },
        {
            "cluster_id": "H002",
            "cluster_signature": "missing_filter|fact_b|month",
            "question_ids": ["q2"],
            "root_cause": "missing_filter",
        },
    ]

    decomposed = decompose_overbroad_ag(ag, clusters)
    assert isinstance(decomposed, list)
    assert len(decomposed) == 2, (
        f"expected one per-cluster diagnostic AG per source cluster; "
        f"got {len(decomposed)}: {[a.get('id') for a in decomposed]}"
    )

    cluster_ids_in_decomposed = {
        cid
        for a in decomposed
        for cid in a.get("source_cluster_ids") or []
    }
    assert cluster_ids_in_decomposed == {"H001", "H002"}

    # Every decomposed AG must carry a stable signature (Track D).
    for a in decomposed:
        assert "_stable_signature" in a, (
            f"decomposed AG {a.get('id')} missing _stable_signature"
        )

    # Every decomposed AG must have its qids scoped to its own cluster,
    # not the broad union.
    for a in decomposed:
        if "H001" in (a.get("source_cluster_ids") or []):
            assert set(a.get("affected_questions") or []) == {"q1"}
        if "H002" in (a.get("source_cluster_ids") or []):
            assert set(a.get("affected_questions") or []) == {"q2"}


def test_decompose_overbroad_ag_returns_unchanged_when_shared_direct_fix_covers_heterogeneous_clusters() -> None:
    """A heterogeneous AG (two distinct root_cause families) is allowed
    UNCHANGED when its patch bundle has a shared direct fix whose
    target_qids cover every cluster's question_ids.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decompose_overbroad_ag,
    )

    ag = {
        "id": "AG_BROAD_BUT_OK",
        "source_cluster_ids": ["H001", "H002"],
        "affected_questions": ["q1", "q2"],
        "patches": [
            {
                "type": "add_sql_snippet_calculation",
                "lever": 5,
                "root_cause": "plural_top_n_collapse",
                "target_qids": ["q1", "q2"],
            },
        ],
    }
    # Heterogeneous clusters — two distinct root_cause families.
    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"], "root_cause": "plural_top_n_collapse"},
        {"cluster_id": "H002", "question_ids": ["q2"], "root_cause": "missing_filter"},
    ]
    result = decompose_overbroad_ag(ag, clusters)
    assert result == [ag], (
        f"AG with shared direct fix was incorrectly decomposed; "
        f"got {len(result)} AGs: {[a.get('id') for a in result]}"
    )


def test_decompose_overbroad_ag_returns_unchanged_for_single_cluster_ag() -> None:
    """A single-cluster AG can never be over-broad; return unchanged."""
    from genie_space_optimizer.optimization.control_plane import (
        decompose_overbroad_ag,
    )

    ag = {
        "id": "AG_SINGLE",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["q1"],
        "patches": [],
    }
    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1"], "root_cause": "missing_filter"},
    ]
    assert decompose_overbroad_ag(ag, clusters) == [ag]


def test_harness_calls_decompose_overbroad_ag_before_sort() -> None:
    """Track 4 wiring — the harness must call ``decompose_overbroad_ag``
    on the merged action_groups list before sorting them into the
    priority queue.
    """
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness._run_lever_loop)
    sort_anchor = "action_groups = sorted(action_groups, key=_ag_sort_key)"
    assert src.count(sort_anchor) >= 1, (
        "harness sort line missing; did the AG-construction site move?"
    )
    sort_idx = src.find(sort_anchor)
    pre_sort = src[:sort_idx]
    assert "decompose_overbroad_ag" in pre_sort[-2500:], (
        "harness does not call decompose_overbroad_ag before sorting "
        "action_groups"
    )
