"""Pin that diagnostic AGs carry SQL-shape directives keyed on structured root_cause."""

from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    diagnostic_action_group_for_cluster,
)


def test_diagnostic_ag_for_temporal_window_includes_l5_directive() -> None:
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_026"],
        "root_cause": "missing_temporal_filter",
        "asi_counterfactual_fixes": ["use DATE_SUB(CURRENT_DATE(), 30)"],
    }
    ag = diagnostic_action_group_for_cluster(cluster)
    assert "L5" in ag["lever_directives"]
    assert ag["lever_directives"]["L5"]["root_cause"] == "missing_temporal_filter"


def test_diagnostic_ag_for_column_disambiguation_includes_l1_directive() -> None:
    cluster = {
        "cluster_id": "H002",
        "question_ids": ["gs_017"],
        "root_cause": "column_disambiguation",
        "asi_counterfactual_fixes": ["region means region_name not region_combination"],
    }
    ag = diagnostic_action_group_for_cluster(cluster)
    assert "L1" in ag["lever_directives"]
    assert ag["lever_directives"]["L1"]["root_cause"] == "column_disambiguation"


def test_diagnostic_ag_for_unknown_root_cause_keeps_legacy_shape() -> None:
    cluster = {
        "cluster_id": "H003",
        "question_ids": ["gs_001"],
        "root_cause": "unknown",
        "asi_counterfactual_fixes": [],
    }
    ag = diagnostic_action_group_for_cluster(cluster)
    assert ag["lever_directives"] == {}
