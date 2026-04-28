from __future__ import annotations


def test_response_quality_only_row_is_not_actionable_soft_signal() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        actionable_failed_judges,
        is_actionable_soft_signal_row,
    )

    row = {
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": "yes",
        "feedback/response_quality/value": "no",
        "response_quality/metadata": {
            "failure_type": "other",
            "blame_set": ["wording"],
        },
    }

    assert actionable_failed_judges(row) == ()
    assert is_actionable_soft_signal_row(row) is False


def test_schema_soft_row_is_actionable_but_not_hard() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        actionable_failed_judges,
        is_actionable_soft_signal_row,
    )

    row = {
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": "yes",
        "feedback/schema_accuracy/value": "no",
        "schema_accuracy/metadata": {
            "failure_type": "wrong_column",
            "blame_set": ["region_name", "region_combination"],
        },
    }

    assert actionable_failed_judges(row) == ("schema_accuracy",)
    assert is_actionable_soft_signal_row(row) is True


def test_target_qids_from_action_group_prefers_explicit_affected_questions() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        target_qids_from_action_group,
    )

    ag = {
        "action_group_id": "AG1",
        "affected_questions": [
            "q_hard_1",
            {"question_id": "q_hard_2"},
            {"id": "q_hard_3"},
        ],
        "source_cluster_ids": ["H001"],
    }
    clusters = [
        {"cluster_id": "H001", "question_ids": ["q_from_cluster"]},
    ]

    assert target_qids_from_action_group(ag, clusters) == (
        "q_hard_1",
        "q_hard_2",
        "q_hard_3",
    )


def test_target_qids_from_action_group_falls_back_to_source_clusters() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        target_qids_from_action_group,
    )

    ag = {
        "action_group_id": "AG1",
        "source_cluster_ids": ["H001", "H002"],
    }
    clusters = [
        {"cluster_id": "H001", "question_ids": ["q1", "q2"]},
        {"cluster_id": "H002", "question_ids": ["q2", "q3"]},
        {"cluster_id": "S001", "question_ids": ["soft_only"]},
    ]

    assert target_qids_from_action_group(ag, clusters) == ("q1", "q2", "q3")


def test_strategy_clusters_hard_first_ignores_soft_while_hard_remain() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        clusters_for_strategy,
    )

    hard = [{"cluster_id": "H001", "question_ids": ["q_hard"]}]
    soft = [{"cluster_id": "S001", "question_ids": ["q_soft"]}]

    selected_hard, selected_soft = clusters_for_strategy(hard, soft)

    assert selected_hard == hard
    assert selected_soft == []


def test_strategy_clusters_can_return_actionable_soft_when_no_hard_remain() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        clusters_for_strategy,
    )

    soft = [
        {
            "cluster_id": "S001",
            "question_ids": ["q_soft"],
            "affected_judges": ["schema_accuracy"],
        },
        {
            "cluster_id": "S002",
            "question_ids": ["q_text"],
            "affected_judges": ["response_quality"],
        },
    ]

    selected_hard, selected_soft = clusters_for_strategy([], soft)

    assert selected_hard == []
    assert [c["cluster_id"] for c in selected_soft] == ["S001"]


def test_control_plane_acceptance_requires_target_improvement() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {
            "question_id": "q_target",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        },
        {
            "question_id": "q_other",
            "feedback/result_correctness/value": "yes",
            "feedback/arbiter/value": "both_correct",
        },
    ]
    post_rows = [
        {
            "question_id": "q_target",
            "feedback/result_correctness/value": "yes",
            "feedback/arbiter/value": "both_correct",
        },
        {
            "question_id": "q_other",
            "feedback/result_correctness/value": "yes",
            "feedback/arbiter/value": "both_correct",
        },
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=95.5,
        candidate_accuracy=100.0,
        target_qids=("q_target",),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )

    assert decision.accepted is True
    assert decision.reason_code == "accepted"
    assert decision.target_fixed_qids == ("q_target",)


def test_control_plane_acceptance_rejects_unrelated_global_gain() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {
            "question_id": "q_target",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        },
        {
            "question_id": "q_other",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        },
    ]
    post_rows = [
        {
            "question_id": "q_target",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        },
        {
            "question_id": "q_other",
            "feedback/result_correctness/value": "yes",
            "feedback/arbiter/value": "both_correct",
        },
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=90.0,
        candidate_accuracy=95.0,
        target_qids=("q_target",),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )

    assert decision.accepted is False
    assert decision.reason_code == "target_qids_not_improved"


def test_control_plane_acceptance_rejects_when_targets_missing() -> None:
    """Empty ``target_qids`` means no causal contract was declared; the
    iteration must reject even if global accuracy improved.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {
            "question_id": "q_target",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        },
    ]
    post_rows = [
        {
            "question_id": "q_target",
            "feedback/result_correctness/value": "yes",
            "feedback/arbiter/value": "both_correct",
        },
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=90.0,
        candidate_accuracy=100.0,
        target_qids=(),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )

    assert decision.accepted is False
    assert decision.reason_code == "missing_target_qids"
    assert decision.delta_pp == 10.0


def test_control_plane_acceptance_missing_targets_priority_over_no_gain() -> None:
    """When both target_qids are missing AND global accuracy is flat,
    surface the missing-targets reason first so operators see the
    causal-contract violation.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=90.0,
        candidate_accuracy=90.0,
        target_qids=(),
        pre_rows=[],
        post_rows=[],
    )

    assert decision.accepted is False
    assert decision.reason_code == "missing_target_qids"


def test_control_plane_ignored_judges_match_config() -> None:
    """control_plane.IGNORED_OPTIMIZATION_JUDGES must mirror the config
    source so ``GSO_IGNORED_OPTIMIZATION_JUDGES`` is the single policy
    knob across the optimizer engine.
    """
    from genie_space_optimizer.common.config import (
        IGNORED_OPTIMIZATION_JUDGES as CONFIG_IGNORED,
    )
    from genie_space_optimizer.optimization.control_plane import (
        IGNORED_OPTIMIZATION_JUDGES as CONTROL_PLANE_IGNORED,
    )

    assert isinstance(CONTROL_PLANE_IGNORED, frozenset)
    assert CONTROL_PLANE_IGNORED == frozenset(CONFIG_IGNORED)
