"""Plan 9 Task 4 — generate_proposals_from_strategy accepts and
threads Plan-5 typed kwargs.

Verifies the four kwargs (rca_evidence_typed, llm_cluster_by_cluster_id,
ag_id, iteration) are on the function signature, default to None / 0,
and are forwarded into _select_lever_5_holistic_path so the Plan-5
short-circuit can actually fire.
"""
import inspect

from genie_space_optimizer.optimization.optimizer import (
    generate_proposals_from_strategy,
    _select_lever_5_holistic_path,
)


def test_generate_proposals_from_strategy_accepts_plan5_kwargs():
    sig = inspect.signature(generate_proposals_from_strategy)
    assert "rca_evidence_typed" in sig.parameters
    assert "llm_cluster_by_cluster_id" in sig.parameters
    assert "ag_id" in sig.parameters
    assert "iteration" in sig.parameters
    assert sig.parameters["rca_evidence_typed"].default is None
    assert sig.parameters["llm_cluster_by_cluster_id"].default is None
    assert sig.parameters["ag_id"].default is None
    assert sig.parameters["iteration"].default == 0


def test_select_lever_5_holistic_path_accepts_plan5_kwargs():
    sig = inspect.signature(_select_lever_5_holistic_path)
    assert "rca_evidence_typed" in sig.parameters
    assert "llm_cluster_by_cluster_id" in sig.parameters
    assert "ag_id" in sig.parameters
    assert "iteration" in sig.parameters


def test_generate_proposals_forwards_plan5_kwargs_to_lever6(monkeypatch):
    """Spy on _generate_lever6_proposal; verify the Plan-5 typed kwargs
    flow through to it when target_lever=6.

    Plan 9 v1 originally proposed spying on _select_lever_5_holistic_path
    here, but that function lives inside generate_metadata_proposals
    (the ASI-driven path), NOT inside generate_proposals_from_strategy
    (the harness's main path). The Plan-5 LLM intent short-circuit is
    in _generate_lever6_proposal (and the L6 dispatcher), which IS
    reached from generate_proposals_from_strategy. So we spy there.
    """
    captured: dict = {}

    def spy(cluster, metadata_snapshot, *args, **kwargs):
        captured.update(kwargs)
        return None  # decline to keep the surrounding logic short

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.optimizer._generate_lever6_proposal",
        spy,
    )

    fake_rca = {"q_001": object()}
    fake_clusters_typed = {"c_001": object()}
    generate_proposals_from_strategy(
        strategy={
            "clusters": [{
                "cluster_id": "c_001",
                "root_cause": "sql_expression_missing",
                "question_ids": ["q_001"],
            }],
            "action_groups": [],
        },
        action_group={
            "id": "AG_001",
            "lever_directives": {
                "sql_snippets": [{
                    "snippet_type": "expression",
                    "name": "x",
                    "rationale": "test",
                }],
            },
            "source_cluster_ids": ["c_001"],
            "affected_questions": ["q_001"],
            "root_cause_summary": "sql_expression_missing",
        },
        metadata_snapshot={
            "_rca_evidence_typed": fake_rca,
            "_failure_clusters": [{
                "cluster_id": "c_001",
                "root_cause": "sql_expression_missing",
                "question_ids": ["q_001"],
            }],
        },
        target_lever=6,
        rca_evidence_typed=fake_rca,
        llm_cluster_by_cluster_id=fake_clusters_typed,
        ag_id="AG_001",
        iteration=2,
    )

    assert captured.get("rca_evidence_typed") is fake_rca
    # L6 spy receives the per-cluster typed cluster (resolved from the
    # bundle by cluster_id="c_001"), not the full map.
    assert captured.get("llm_cluster") is fake_clusters_typed["c_001"]
    assert captured.get("ag_id") == "AG_001"
    assert captured.get("iteration") == 2
