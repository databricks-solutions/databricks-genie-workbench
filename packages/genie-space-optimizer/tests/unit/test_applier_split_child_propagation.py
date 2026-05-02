"""Track B — section split-children must inherit the parent rewrite's
risk and cluster lineage so blast-radius and survival helpers see the
same shape they saw on the parent proposal."""
from __future__ import annotations


def _build_high_risk_rewrite_parent() -> dict:
    return {
        "type": "rewrite_instruction",
        "proposal_id": "P_REW",
        "parent_proposal_id": "P_REW",
        "expanded_patch_id": "P_REW",
        "lever": 5,
        "invoked_levers": [5],
        "cluster_id": "H001",
        "source_cluster_id": "H001",
        "source_cluster_ids": ["H001"],
        "primary_cluster_id": "H001",
        "passing_dependents": ["q010", "q011", "q012", "q013", "q014", "q015"],
        "passing_dependents_outside_target": ["q010", "q011", "q012", "q013", "q014", "q015"],
        "high_collateral_risk": True,
        "target_dependents": ["q001"],
        "root_cause": "column_disambiguation",
        "rca_kind": "metric_view_routing",
        "relevance_score": 0.88,
        "causal_attribution_tier": 3,
        "rca_id": "rca_q001",
        "target_qids": ["q001"],
        "_grounding_target_qids": ["q001"],
        "proposed_value": (
            "QUERY RULES:\n- rule one\n\n"
            "ASSET ROUTING:\n- rule two\n\n"
            "QUERY PATTERNS:\n- rule three\n"
        ),
    }


def test_split_children_inherit_passing_dependents_and_cluster_lineage() -> None:
    from genie_space_optimizer.optimization.applier import _expand_rewrite_splits

    parent = _build_high_risk_rewrite_parent()
    expanded = _expand_rewrite_splits([parent])
    children = [p for p in expanded if p.get("_split_from") == "rewrite_instruction"]

    assert children, "expected at least one split-child"
    for child in children:
        assert child.get("passing_dependents") == parent["passing_dependents"], (
            f"split-child lost passing_dependents: {child!r}"
        )
        assert child.get("high_collateral_risk") is True
        assert child.get("primary_cluster_id") == "H001"
        assert child.get("source_cluster_ids") == ["H001"]
        assert child.get("root_cause") == "column_disambiguation"
        assert child.get("relevance_score") == 0.88
        assert child.get("rca_id") == "rca_q001"
        assert child.get("_split_from") == "rewrite_instruction"
