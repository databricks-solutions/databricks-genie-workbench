"""Plan 8 Task 15 — one iteration with every LLM short-circuit
declining. Verifies the deterministic fallbacks still produce the
SAME typed contracts so downstream consumers don't see fallback
holes.

  * Plan 3 fallback → typed PerQidRcaEvidence (Task 6)
  * Plan 5 lean fallback → stamped RepairIntent (Task 7)
"""
from __future__ import annotations

from unittest.mock import patch


def test_plan3_fallback_yields_typed_evidence():
    from genie_space_optimizer.optimization.rca import (
        _typed_evidence_from_metadata,
    )
    ev = _typed_evidence_from_metadata(
        "q1", "judge_asi",
        {"failure_type": "wrong_column",
         "blame_set": ["catalog.s.t.col_a"],
         "actual_objects": ["catalog.s.t.col_b"],
         "expected_objects": ["catalog.s.t.col_a"]},
        "SELECT col_b FROM t",
    )
    assert ev is not None
    assert ev.qid == "q1"
    assert "catalog.s.t.col_a" in ev.blame_set


def test_plan5_lean_fallback_carries_intent_after_stamp():
    """Lean fallback proposals now ship with a RepairIntent stamp
    when an _archetype_name is set on the synthesizer output."""
    from genie_space_optimizer.optimization.optimizer import (
        _dispatch_lever_5b_for_cluster,
    )
    fake_proposal = {
        "example_question": "Q?", "example_sql": "SELECT x",
        "parameters": [], "usage_guidance": "",
        "_archetype_name": "top_n_by_metric",
        "_archetype_patch_type": "add_example_sql",
    }
    with patch(
        "genie_space_optimizer.optimization.synthesis."
        "synthesize_example_sqls",
        return_value=fake_proposal,
    ), patch(
        "genie_space_optimizer.common.config."
        "rich_synthesis_primary_for_sql_shape_enabled",
        return_value=False,
    ):
        out = _dispatch_lever_5b_for_cluster(
            cluster={"cluster_id": "H001", "question_ids": ["q1"],
                      "root_cause": "x",
                      "asi_blame_set": ["catalog.s.t.col_a"]},
            metadata_snapshot={"schema_columns": ["catalog.s.t.col_a"]},
            w=None, benchmark_corpus=None, ag_id="AG_X",
        )
    assert out
    assert "repair_intent" in out[0]
    assert out[0].get("intent_id")
