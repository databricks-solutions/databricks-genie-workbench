"""Plan 12 — L6 lane must emit GSO_PATCH_OUTCOME_V1 at every terminal
state. The adapter-returns-None terminal (legacy candidate that fails
to promote because target_table or target_qids is empty) emits
CONTRACT_FAILED."""
from unittest.mock import patch


def test_l6_emits_contract_failed_when_legacy_proposal_is_none(capsys):
    from genie_space_optimizer.optimization.optimizer import (
        _generate_proposals_for_lever6,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()

    action_group = {
        "ag_id": "AG1",
        "_lever6_structural_candidates": [
            {
                "patch_type": "add_sql_snippet_filter",
                "snippet_type": "filter",
                "sql": "order_date >= ...",
                "target_table": "catalog.schema.orders",
                "target_qids": ["gs_021"],
                "source": "rca_failed_question_sql",
                "source_question_id": "gs_021",
            },
        ],
        "affected_questions": ["gs_021"],
        "source_cluster_ids": ["H001"],
    }
    metadata_snapshot = {
        "_failure_clusters": [
            {
                "cluster_id": "H001",
                "rca_card_id": "rca_42",
                "causal_target": "catalog.schema.orders.order_date",
                "repair_hypothesis": "MTD",
            }
        ],
        "iteration": 1,
        "optimization_run_id": "run_x",
    }

    # Force the legacy converter to return None — simulating a candidate
    # that fails the identifier-allowlist or SQL-validation firewall.
    with patch(
        "genie_space_optimizer.optimization.optimizer."
        "_proposal_from_structural_sql_candidate",
        return_value=None,
    ):
        proposals = _generate_proposals_for_lever6(
            action_group=action_group,
            metadata_snapshot=metadata_snapshot,
            ag_id="AG1",
        )

    assert proposals == []
    out = capsys.readouterr().out
    assert "GSO_PATCH_OUTCOME_V1" in out
    assert '"outcome_kind":"contract_failed"' in out.replace(" ", "")
    # Terminal reason should name the L6 pre-promote path.
    assert (
        '"terminal_reason":"l6_structural_candidate_failed_pre_repair_proposal"'
        in out.replace(" ", "")
    )


def test_l6_no_marker_when_candidate_promotes_to_proposal(capsys):
    """The CONTRACT_FAILED emission must not fire when the legacy
    converter returns a populated proposal — downstream stages
    (validation / blast-radius / applier) will emit the eventual
    terminal outcome."""
    from genie_space_optimizer.optimization.optimizer import (
        _generate_proposals_for_lever6,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()

    action_group = {
        "ag_id": "AG1",
        "_lever6_structural_candidates": [
            {
                "patch_type": "add_sql_snippet_filter",
                "snippet_type": "filter",
                "sql": "order_date >= ...",
                "target_table": "catalog.schema.orders",
                "target_qids": ["gs_021"],
                "source": "rca_failed_question_sql",
                "source_question_id": "gs_021",
            },
        ],
        "affected_questions": ["gs_021"],
        "source_cluster_ids": ["H001"],
    }
    metadata_snapshot = {
        "_failure_clusters": [
            {
                "cluster_id": "H001",
                "rca_card_id": "rca_42",
                "causal_target": "catalog.schema.orders.order_date",
                "repair_hypothesis": "MTD",
            }
        ],
        "iteration": 1,
        "optimization_run_id": "run_x",
    }

    def _fake_proposal(candidate, **kwargs):
        return {
            "patch_type": candidate["patch_type"],
            "lever": 6,
            "snippet_type": candidate["snippet_type"],
            "display_name": "MTD filter",
            "sql": candidate.get("sql", ""),
            "target_table": candidate.get("target_table", ""),
            "affected_questions": list(kwargs.get("target_qids") or ()),
            "target_qids": list(kwargs.get("target_qids") or ()),
            "source": "rca_failed_question_sql",
            "source_question_id": "gs_021",
            "cluster_id": kwargs.get("cluster_id", ""),
        }

    with patch(
        "genie_space_optimizer.optimization.optimizer."
        "_proposal_from_structural_sql_candidate",
        side_effect=_fake_proposal,
    ):
        proposals = _generate_proposals_for_lever6(
            action_group=action_group,
            metadata_snapshot=metadata_snapshot,
            ag_id="AG1",
        )

    assert proposals, "expected proposal to promote"
    out = capsys.readouterr().out
    assert "GSO_PATCH_OUTCOME_V1" not in out, (
        "L6 must not emit a terminal outcome at the synth-success path; "
        "downstream stages own that."
    )
