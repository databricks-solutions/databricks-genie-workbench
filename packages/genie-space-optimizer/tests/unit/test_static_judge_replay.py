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


def _hard_row(qid: str) -> dict:
    return {
        "question_id": qid,
        "feedback/arbiter/value": "ground_truth_correct",
        "feedback/result_correctness/value": "no",
    }


def _passing_row(qid: str) -> dict:
    return {
        "question_id": qid,
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": "yes",
    }


def test_static_replay_observed_ag1_reports_target_fixed_qid() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    baseline_rows = [
        _hard_row("gs_026"),
        _hard_row("gs_021"),
        _hard_row("gs_004"),
        _hard_row("gs_001"),
        _passing_row("gs_018"),
    ]
    candidate_rows = [
        _hard_row("gs_026"),
        _hard_row("gs_021"),
        _passing_row("gs_004"),
        _passing_row("gs_001"),
        _hard_row("gs_018"),
    ]
    action_group = {
        "id": "AG1",
        "source_cluster_ids": ["H004"],
        "affected_questions": ["gs_004"],
    }
    proposals = [
        {
            "proposal_id": "P004_asset_routing",
            "type": "rewrite_instruction",
            "relevance_score": 1.0,
            "rca_id": "rca_gs004_asset_routing",
            "target_qids": ["gs_004"],
        },
    ]

    result = run_static_judge_replay(
        baseline_accuracy=20.0,
        candidate_accuracy=40.0,
        baseline_rows=baseline_rows,
        candidate_rows=candidate_rows,
        action_group=action_group,
        source_clusters=[{"cluster_id": "H004", "question_ids": ["gs_004"]}],
        proposals=proposals,
        max_patches=3,
        max_new_hard_regressions=1,
    )

    assert result.acceptance.target_qids == ("gs_004",)
    assert result.acceptance.target_fixed_qids == ("gs_004",)
    assert result.acceptance.target_still_hard_qids == ()
    assert result.acceptance.regression_debt_qids == ("gs_018",)
    assert result.acceptance.reason_code == "accepted_with_regression_debt"


def test_static_replay_preserves_rca_patches_and_accepts_bounded_debt() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    baseline_rows = [
        _hard_row("q007"),
        _hard_row("q009"),
        _hard_row("q005"),
        _hard_row("q002"),
        _passing_row("q015"),
    ]
    candidate_rows = [
        _passing_row("q007"),
        _passing_row("q009"),
        _hard_row("q005"),
        _hard_row("q002"),
        _hard_row("q015"),
    ]
    action_group = {
        "id": "AG2",
        "source_cluster_ids": ["H001", "H003", "H005", "H006"],
        "affected_questions": ["q007", "q009", "q005", "q002"],
    }
    proposals = [
        {
            "proposal_id": "P002_broad",
            "type": "update_column_description",
            "relevance_score": 1.0,
            "target_qids": ["q007", "q009", "q005", "q002"],
        },
        {
            "proposal_id": "P008_rca",
            "type": "update_column_description",
            "relevance_score": 1.0,
            "rca_id": "rca_q007_measure_swap",
            "target_qids": ["q007"],
        },
        {
            "proposal_id": "P047_filter",
            "type": "add_sql_snippet_filter",
            "relevance_score": 1.0,
            "rca_id": "rca_q007_filter",
            "_grounding_target_qids": ["q007"],
        },
    ]

    result = run_static_judge_replay(
        baseline_accuracy=57.1,
        candidate_accuracy=64.3,
        baseline_rows=baseline_rows,
        candidate_rows=candidate_rows,
        action_group=action_group,
        source_clusters=[
            {"cluster_id": "H001", "question_ids": ["q007"]},
            {"cluster_id": "H003", "question_ids": ["q009"]},
            {"cluster_id": "H005", "question_ids": ["q005"]},
            {"cluster_id": "H006", "question_ids": ["q002"]},
        ],
        proposals=proposals,
        max_patches=3,
        max_new_hard_regressions=1,
    )

    kept_ids = [patch["proposal_id"] for patch in result.kept_patches]
    assert "P008_rca" in kept_ids
    assert "P047_filter" in kept_ids
    assert result.acceptance.accepted is True
    assert result.acceptance.reason_code == "accepted_with_regression_debt"
    assert result.acceptance.regression_debt_qids == ("q015",)


def test_static_replay_observed_ag4_passing_to_hard_is_accepted_with_debt() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        run_static_judge_replay,
    )

    baseline_rows = [
        _hard_row("gs_001"),
        _passing_row("gs_021"),
        _passing_row("gs_004"),
        _passing_row("gs_018"),
        _passing_row("gs_026"),
    ]
    candidate_rows = [
        _passing_row("gs_001"),
        _hard_row("gs_021"),
        _passing_row("gs_004"),
        _passing_row("gs_018"),
        _passing_row("gs_026"),
    ]
    action_group = {
        "id": "AG4",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["gs_001"],
    }
    proposals = [
        {
            "proposal_id": "P001_asset_routing",
            "type": "rewrite_instruction",
            "relevance_score": 1.0,
            "rca_id": "rca_gs001_asset_routing",
            "target_qids": ["gs_001"],
        },
    ]

    result = run_static_judge_replay(
        baseline_accuracy=75.0,
        candidate_accuracy=80.0,
        baseline_rows=baseline_rows,
        candidate_rows=candidate_rows,
        action_group=action_group,
        source_clusters=[{"cluster_id": "H001", "question_ids": ["gs_001"]}],
        proposals=proposals,
        max_patches=3,
        max_new_hard_regressions=1,
    )

    assert result.acceptance.target_qids == ("gs_001",)
    assert result.acceptance.target_fixed_qids == ("gs_001",)
    assert result.acceptance.passing_to_hard_regressed_qids == ("gs_021",)
    assert result.acceptance.regression_debt_qids == ("gs_021",)
    assert result.acceptance.reason_code == "accepted_with_regression_debt"
    assert result.acceptance.accepted is True
