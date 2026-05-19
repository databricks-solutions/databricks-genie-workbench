"""Plan 8 Task 7 — the L5b rich-path fallback also stamps the
typed RepairIntent on each returned proposal."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch


def test_rich_fallback_proposals_carry_repair_intent_stamp():
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _dispatch_rich_synthesis_for_l5b,
    )
    fake_proposal = {
        "example_question": "Q?",
        "example_sql": "SELECT x FROM t",
        "parameters": [],
        "usage_guidance": "",
        "_archetype_name": "top_n_by_metric",
        "patch_type": "add_example_sql",
    }
    fake_result = SimpleNamespace(
        proposal=fake_proposal,
        attempted_archetypes=("top_n_by_metric",),
        skipped_reason=None,
    )

    cluster = {
        "cluster_id": "H001",
        "question_ids": ["q1"],
        "root_cause": "wrong_aggregation",
        "asi_blame_set": ["catalog.s.t.col_a"],
    }
    out = _dispatch_rich_synthesis_for_l5b(
        cluster=cluster,
        metadata_snapshot={"schema_columns": ["catalog.s.t.col_a"]},
        w=None,
        benchmarks=None,
        _synthesize=lambda *_a, **_k: fake_result,
    )
    assert out, "Expected at least one proposal"
    p = out[0]
    assert "repair_intent" in p
    assert p.get("intent_id")
