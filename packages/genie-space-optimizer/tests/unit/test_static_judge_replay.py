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
    assert result.acceptance.soft_to_hard_regressed_qids == ("q014",)
    assert [p["proposal_id"] for p in result.kept_patches] == ["P_filter"]


def test_static_replay_drops_global_add_instruction_before_acceptance() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    result = run_static_judge_replay(
        baseline_accuracy=50.0,
        candidate_accuracy=75.0,
        baseline_rows=[
            {
                "question_id": "q_target",
                "feedback/arbiter/value": "ground_truth_correct",
            }
        ],
        candidate_rows=[
            {
                "question_id": "q_target",
                "feedback/arbiter/value": "both_correct",
            }
        ],
        action_group={"affected_questions": ["q_target"]},
        source_clusters=[],
        proposals=[
            {
                "proposal_id": "P_global",
                "patch_type": "add_instruction",
                "rca_kind": "missing_filter",
                "section_name": "QUERY RULES",
                "new_text": "Always use UNION ALL for APSD KPI queries.",
                "relevance_score": 0.99,
            }
        ],
        patches=[
            {
                "proposal_id": "P_global",
                "type": "add_instruction",
                "section_name": "QUERY RULES",
                "new_text": "Always use UNION ALL for APSD KPI queries.",
                "relevance_score": 0.99,
            }
        ],
    )

    assert result.kept_patches == []
    assert result.dropped_patches[0]["_drop_reason"] == (
        "global_instruction_scope_without_dependents"
    )


def test_static_replay_drops_measure_patch_for_missing_filter_rca() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    result = run_static_judge_replay(
        baseline_accuracy=80.0,
        candidate_accuracy=85.0,
        baseline_rows=[
            {
                "question_id": "q_target",
                "feedback/arbiter/value": "ground_truth_correct",
            }
        ],
        candidate_rows=[
            {
                "question_id": "q_target",
                "feedback/arbiter/value": "both_correct",
            }
        ],
        action_group={"affected_questions": ["q_target"]},
        source_clusters=[],
        proposals=[
            {
                "proposal_id": "P_measure",
                "patch_type": "add_sql_snippet_measure",
                "rca_kind": "missing_filter",
                "expression": "SUM(cy_sales)",
                "target_qids": ["q_target"],
            }
        ],
    )

    assert result.kept_proposals == []
    assert result.dropped_proposals[0]["_drop_reason"] == (
        "patch_type_incompatible_with_rca_kind"
    )


def test_static_replay_patch_cap_preserves_secondary_target_qid() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    result = run_static_judge_replay(
        baseline_accuracy=60.0,
        candidate_accuracy=80.0,
        baseline_rows=[
            {"question_id": "q_big", "feedback/arbiter/value": "ground_truth_correct"},
            {"question_id": "q_small", "feedback/arbiter/value": "ground_truth_correct"},
        ],
        candidate_rows=[
            {"question_id": "q_big", "feedback/arbiter/value": "both_correct"},
            {"question_id": "q_small", "feedback/arbiter/value": "both_correct"},
        ],
        action_group={"affected_questions": ["q_big", "q_small"]},
        source_clusters=[],
        proposals=[
            {
                "proposal_id": "P_big_1",
                "patch_type": "add_sql_snippet_filter",
                "rca_kind": "missing_filter",
                "target_qids": ["q_big"],
                "relevance_score": 0.98,
            },
            {
                "proposal_id": "P_big_2",
                "patch_type": "add_instruction",
                "rca_kind": "missing_filter",
                "target_qids": ["q_big"],
                "relevance_score": 0.97,
            },
            {
                "proposal_id": "P_small",
                "patch_type": "update_column_description",
                "rca_kind": "wrong_column",
                "target_qids": ["q_small"],
                "relevance_score": 0.70,
            },
        ],
        max_patches=2,
    )

    assert [p["proposal_id"] for p in result.kept_patches] == [
        "P_big_1",
        "P_small",
    ]


def test_static_replay_surfaces_quarantined_patchable_hard_failures() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    result = run_static_judge_replay(
        baseline_accuracy=70.0,
        candidate_accuracy=70.0,
        baseline_rows=[
            {"question_id": "q021", "feedback/arbiter/value": "ground_truth_correct"}
        ],
        candidate_rows=[
            {"question_id": "q021", "feedback/arbiter/value": "ground_truth_correct"}
        ],
        action_group={"affected_questions": ["q021"]},
        source_clusters=[],
        proposals=[],
        quarantined_qids={"q021"},
        unresolved_patchable_qids={"q021"},
        hard_cluster_count_after_prune=0,
        soft_cluster_count_after_prune=2,
    )

    assert result.quarantine_decision is not None
    assert result.quarantine_decision["action"] == "stop_for_human_review"
    assert result.quarantine_decision["blocking_qids"] == ["q021"]
