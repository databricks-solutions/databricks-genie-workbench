"""Plan 8 Task 7 — when Plan 5's LLM intent declines and the lean
synthesis path produces a proposal, the proposal is stamped with a
typed RepairIntent via stamp_proposals_from_archetype."""
from __future__ import annotations

from unittest.mock import patch


def test_lean_fallback_proposal_carries_repair_intent_stamp():
    from genie_space_optimizer.optimization.optimizer import (
        _dispatch_lever_5b_for_cluster,
    )
    cluster = {
        "cluster_id": "H001", "question_ids": ["q1"],
        "root_cause": "wrong_aggregation",
        "asi_blame_set": ["catalog.s.t.col_a"],
    }
    metadata = {"schema_columns": ["catalog.s.t.col_a"], "iteration": 1}
    fake_proposal = {
        "example_question": "What is X?",
        "example_sql": "SELECT col_a FROM t",
        "parameters": [],
        "usage_guidance": "use it",
        "_archetype_name": "top_n_by_metric",
        "_archetype_patch_type": "add_example_sql",
    }
    with patch(
        "genie_space_optimizer.optimization.synthesis.synthesize_example_sqls",
        return_value=fake_proposal,
    ), patch(
        "genie_space_optimizer.common.config."
        "rich_synthesis_primary_for_sql_shape_enabled",
        return_value=False,
    ):
        out = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, ag_id="AG_X",
        )
    assert out, "Expected one proposal from the lean fallback"
    p = out[0]
    assert "repair_intent" in p, (
        "Lean-path fallback proposals must carry a stamped RepairIntent"
    )
    assert p.get("intent_id"), "intent_id must be populated"
