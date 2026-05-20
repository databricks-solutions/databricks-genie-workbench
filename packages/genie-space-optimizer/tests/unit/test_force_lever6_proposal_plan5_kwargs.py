"""Plan 9 Task 5 — _force_lever6_proposal_for_ag threads Plan-5
typed inputs into generate_lever6.

Verifies the four kwargs are accepted on _force_lever6_proposal_for_ag
signature and forwarded into the generator call so
_generate_lever6_proposal's Plan-5 short-circuit
(optimizer.py:14122) actually fires when forced.
"""
import inspect

from genie_space_optimizer.optimization.harness import (
    _force_lever6_proposal_for_ag,
)


def test_force_lever6_proposal_for_ag_accepts_plan5_kwargs():
    sig = inspect.signature(_force_lever6_proposal_for_ag)
    assert "rca_evidence_typed" in sig.parameters
    assert "llm_cluster" in sig.parameters
    # iteration + ag_id are already explicit on the signature.
    assert "iteration" in sig.parameters
    assert "ag_id" in sig.parameters


def test_force_lever6_proposal_forwards_plan5_kwargs_to_generator():
    captured: dict = {}

    def fake_generator(cluster, metadata_snapshot, **kwargs):
        captured.update(kwargs)
        return {
            "proposal_id": "p_001",
            "patch_type": "add_sql_snippet_expression",
            "lever": 6,
            "patch_body": {"name": "x", "sql_expression": "1"},
            "provenance": {},
        }

    fake_rca = {"q_001": object()}
    fake_cluster_typed = object()

    proposal = _force_lever6_proposal_for_ag(
        run_id="run_test",
        iteration=3,
        ag_id="AG_001",
        cluster={
            # missing_filter is in _SQL_SHAPE_ROOT_CAUSES so the
            # _should_force_lever6_proposal predicate fires.
            "root_cause": "missing_filter",
            "recommended_levers": [6],
            "cluster_id": "c_001",
        },
        ag_target_qids=("q_001",),
        ag_proposals_so_far=[],
        metadata_snapshot={},
        decision_emit=lambda _: None,
        generate_lever6=fake_generator,
        # Plan 9 Task 5 — typed inputs that must flow through to the generator.
        rca_evidence_typed=fake_rca,
        llm_cluster=fake_cluster_typed,
    )

    assert proposal is not None
    assert captured.get("rca_evidence_typed") is fake_rca
    assert captured.get("llm_cluster") is fake_cluster_typed
    assert captured.get("ag_id") == "AG_001"
    assert captured.get("iteration") == 3
