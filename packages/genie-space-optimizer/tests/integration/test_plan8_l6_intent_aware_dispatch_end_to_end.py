"""Plan 8 Task 16 — Lever 6 intent-aware dispatch end-to-end.

When the harness is given an AG whose cluster's RCA evidence
points to a sql-snippet patch_type, the lever 6 generator is
invoked through the Plan-5 intent short-circuit and the returned
proposal carries the typed RepairIntent stamp."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairShape,
)


def test_l6_intent_aware_end_to_end():
    from genie_space_optimizer.optimization.optimizer import (
        _generate_lever6_proposal,
    )
    ev = {"q1": PerQidRcaEvidence(
        qid="q1", observed_failure="wrong_aggregation",
        generated_sql_issue="count instead of count distinct",
        expected_sql_shape="select count(distinct customer_id)",
        blame_set=("catalog.s.fact.customer_id",),
        suggested_repair_family="sql_snippet_measure",
        repair_hint_patch_type=PatchType.ADD_SQL_SNIPPET_MEASURE,
        confidence="high",
        quoted_evidence=("expected distinct",),
    )}
    llm_c = LlmCluster(
        cluster_id="H001",
        semantic_theme="distinct count needed",
        member_qids=("q1",),
        unifying_evidence="needs measure",
        suggested_repair_shape=RepairShape.SQL_EXPRESSION,
        primary_blame_set=("catalog.s.fact.customer_id",),
        confidence="high",
    )
    fake_proposal_obj = MagicMock()
    fake_proposal_obj.patch_type = PatchType.ADD_SQL_SNIPPET_MEASURE
    fake_proposal_obj.to_repair_intent = MagicMock()
    fake_legacy_proposal = {
        "snippet_type": "measure",
        "sql": "count(distinct customer_id)",
        "target": "catalog.s.fact",
    }
    with patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        return_value=fake_proposal_obj,
    ), patch(
        "genie_space_optimizer.optimization.optimizer."
        "_generate_lever6_proposal_legacy_body",
        return_value=fake_legacy_proposal,
    ), patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "stamp_repair_intent_on_proposal",
    ) as mock_stamp:
        out = _generate_lever6_proposal(
            cluster={"cluster_id": "H001", "question_ids": ["q1"],
                      "root_cause": "wrong_aggregation",
                      "asi_blame_set": ["catalog.s.fact.customer_id"]},
            metadata_snapshot={
                "schema_columns": ["catalog.s.fact.customer_id"],
                "iteration": 1,
            },
            rca_evidence_typed=ev, llm_cluster=llm_c,
            ag_id="AG_X", iteration=1,
        )
    assert out == fake_legacy_proposal
    assert mock_stamp.called, (
        "Lever 6 intent-aware short-circuit must stamp RepairIntent "
        "on the returned proposal"
    )
