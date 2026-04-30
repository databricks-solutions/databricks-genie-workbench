from __future__ import annotations


def test_static_replay_accepts_bounded_soft_to_hard_debt() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    result = run_static_judge_replay(
        baseline_accuracy=71.4,
        candidate_accuracy=78.6,
        baseline_rows=[
            {
                "question_id": "q005",
                "feedback/arbiter/value": "ground_truth_correct",
                "feedback/result_correctness/value": "no",
            },
            {
                "question_id": "q002",
                "feedback/arbiter/value": "ground_truth_correct",
                "feedback/result_correctness/value": "no",
            },
            {
                "question_id": "q009",
                "feedback/arbiter/value": "ground_truth_correct",
                "feedback/result_correctness/value": "no",
            },
            {
                "question_id": "q014",
                "feedback/arbiter/value": "both_correct",
                "feedback/schema_accuracy/value": "no",
            },
        ],
        candidate_rows=[
            {
                "question_id": "q005",
                "feedback/arbiter/value": "both_correct",
                "feedback/result_correctness/value": "yes",
            },
            {
                "question_id": "q002",
                "feedback/arbiter/value": "both_correct",
                "feedback/result_correctness/value": "yes",
            },
            {
                "question_id": "q009",
                "feedback/arbiter/value": "both_correct",
                "feedback/result_correctness/value": "yes",
            },
            {
                "question_id": "q014",
                "feedback/arbiter/value": "ground_truth_correct",
                "feedback/result_correctness/value": "no",
            },
        ],
        action_group={
            "id": "AG1",
            "affected_questions": ["q005", "q002", "q009"],
            "source_cluster_ids": ["H001"],
        },
        source_clusters=[
            {
                "cluster_id": "H001",
                "question_ids": ["q005", "q002", "q009"],
            },
        ],
        proposals=[
            {
                "proposal_id": "P_filter",
                "patch_type": "add_sql_snippet_filter",
                "rca_kind": "missing_filter",
                "target_qids": ["q005", "q002", "q009"],
                "relevance_score": 0.95,
            }
        ],
        patches=[
            {
                "proposal_id": "P_filter",
                "type": "add_sql_snippet_filter",
                "target_qids": ["q005", "q002", "q009"],
                "relevance_score": 0.95,
            }
        ],
        max_patches=3,
        min_gain_pp=1.0,
        max_new_hard_regressions=1,
        max_new_passing_to_hard_regressions=0,
    )

    assert result.acceptance.accepted is True
    assert result.acceptance.reason_code == "accepted_with_regression_debt"
    assert result.acceptance.target_fixed_qids == ("q002", "q005", "q009")
    assert result.acceptance.regression_debt_qids == ("q014",)
    assert [p["proposal_id"] for p in result.kept_patches] == ["P_filter"]
