"""Plan 12 — L6 callsite at optimizer.py:17811 must thread
rca_card_id and causal_target onto every emitted proposal."""
from unittest.mock import patch


def test_l6_proposal_carries_rca_card_id_after_threading():
    """We provide a single structural candidate and an upstream cluster
    carrying rca_card_id + causal_target + repair_hypothesis. The
    helper must stamp those four fields onto the emitted proposal dict
    along with original_patch_type."""
    from genie_space_optimizer.optimization.optimizer import (
        _generate_proposals_for_lever6,
    )

    action_group = {
        "ag_id": "AG1",
        "lever_directives": {"6": {"sql_expressions": []}},
        "_lever6_structural_candidates": [
            {
                "patch_type": "add_sql_snippet_filter",
                "lever": 6,
                "snippet_type": "filter",
                "display_name": "MTD filter",
                "sql": "order_date >= DATE_TRUNC('month', CURRENT_DATE)",
                "rationale": "Judge identified trailing-30 misuse",
                "target_table": "catalog.schema.orders",
                "target_qids": ["gs_021"],
                "source": "rca_failed_question_sql",
                "source_question_id": "gs_021",
            },
        ],
        "affected_questions": ["gs_021"],
        "source_cluster_ids": ["H001"],
    }
    cluster_with_rca = {
        "cluster_id": "H001",
        "rca_card_id": "rca_42",
        "causal_target": "catalog.schema.orders.order_date",
        "repair_hypothesis": "Replace trailing-30 with MTD",
    }
    metadata_snapshot = {
        "_failure_clusters": [cluster_with_rca],
        "iteration": 1,
        "optimization_run_id": "run_x",
    }

    # Patch _proposal_from_structural_sql_candidate to bypass its
    # validation / firewall paths (it requires a live spark or w + warehouse
    # to fully exercise; the test focuses on the threading layer).
    def _fake_legacy_proposal(candidate, **kwargs):
        # Mirror the legacy output shape with the fields the helper threads on.
        return {
            "patch_type": candidate["patch_type"],
            "lever": 6,
            "snippet_type": candidate["snippet_type"],
            "display_name": candidate.get("display_name", ""),
            "sql": candidate.get("sql", ""),
            "rationale": candidate.get("rationale", ""),
            "target_table": candidate.get("target_table", ""),
            "affected_questions": list(kwargs.get("target_qids") or ()),
            "target_qids": list(kwargs.get("target_qids") or ()),
            "source": candidate.get("source", ""),
            "source_question_id": candidate.get("source_question_id", ""),
            "cluster_id": kwargs.get("cluster_id", ""),
        }

    with patch(
        "genie_space_optimizer.optimization.optimizer._proposal_from_structural_sql_candidate",
        side_effect=_fake_legacy_proposal,
    ):
        proposals = _generate_proposals_for_lever6(
            action_group=action_group,
            metadata_snapshot=metadata_snapshot,
            ag_id="AG1",
            spark=None,
            catalog="",
            gold_schema="",
            warehouse_id="",
            w=None,
            benchmarks=None,
        )

    assert proposals, "expected at least one L6 proposal"
    p = proposals[0]
    assert p.get("rca_card_id") == "rca_42"
    assert p.get("causal_target") == "catalog.schema.orders.order_date"
    assert p.get("original_patch_type") == "add_sql_snippet_filter"
    assert p.get("repair_hypothesis") == "Replace trailing-30 with MTD"
    assert p.get("target_qids") == ["gs_021"]
