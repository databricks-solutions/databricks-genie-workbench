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
        {
            "cluster_id": "H001",
            "question_ids": ["q_hard_1", "q_hard_2", "q_hard_3", "q_from_cluster"],
        },
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


def test_control_plane_acceptance_accepts_unrelated_global_gain_as_attribution_drift() -> None:
    """Track F (Phase A burn-down MVP): when the named target qid does not
    flip but every regression budget stays at zero, the candidate is a real
    net win and must accept under ``accepted_with_attribution_drift``. The
    rationale is that RCA, clustering, cap, applier, and rollback all
    worked — the only discrepancy is attribution drift between the named
    target and the qids that actually flipped.
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

    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_attribution_drift"


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


def test_target_qids_falls_back_when_affected_questions_are_question_text() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        target_qids_from_action_group,
    )

    ag = {
        "id": "AG5",
        "affected_questions": ["Which zone VPs stores have the highest total CY sales"],
        "source_cluster_ids": ["H001"],
    }
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["7now_delivery_analytics_space_gs_025"],
        }
    ]

    assert target_qids_from_action_group(ag, clusters) == (
        "7now_delivery_analytics_space_gs_025",
    )


def test_target_qids_keeps_valid_explicit_qids() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        target_qids_from_action_group,
    )

    ag = {
        "id": "AG1",
        "affected_questions": ["7now_delivery_analytics_space_gs_025"],
        "source_cluster_ids": ["H001"],
    }
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["7now_delivery_analytics_space_gs_025"],
        }
    ]

    assert target_qids_from_action_group(ag, clusters) == (
        "7now_delivery_analytics_space_gs_025",
    )


def test_format_control_plane_acceptance_detail_includes_reason_and_qids() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
        format_control_plane_acceptance_detail,
    )

    detail = format_control_plane_acceptance_detail(
        ControlPlaneAcceptance(
            accepted=False,
            reason_code="target_qids_not_improved",
            baseline_accuracy=85.7,
            candidate_accuracy=100.0,
            delta_pp=14.3,
            target_qids=("q022",),
            target_fixed_qids=(),
            target_still_hard_qids=("q022",),
            out_of_target_regressed_qids=("q020",),
        )
    )

    assert "reason=target_qids_not_improved" in detail
    assert "target_qids=q022" in detail
    assert "target_fixed_qids=(none)" in detail
    assert "target_still_hard_qids=q022" in detail
    assert "out_of_target_regressed_qids=q020" in detail


def test_patchable_hard_failure_qids_include_only_ground_truth_correct() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        ambiguous_failure_qids,
        patchable_hard_failure_qids,
    )

    rows = [
        {
            "question_id": "q_gt",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "ground_truth_correct",
        },
        {
            "question_id": "q_neither",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "neither_correct",
        },
        {
            "question_id": "q_both",
            "feedback/result_correctness/value": "no",
            "feedback/arbiter/value": "both_correct",
        },
    ]

    assert patchable_hard_failure_qids(rows) == ("q_gt",)
    assert ambiguous_failure_qids(rows) == ("q_neither",)


def test_uncovered_patchable_clusters_detects_missing_hard_cluster() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        uncovered_patchable_clusters,
    )

    clusters = [
        {"cluster_id": "H001", "question_ids": ["q021"]},
        {"cluster_id": "H002", "question_ids": ["q022"]},
        {"cluster_id": "H003", "question_ids": ["q013"]},
    ]
    action_groups = [
        {
            "id": "AG1",
            "source_cluster_ids": ["H002"],
            "affected_questions": ["q022"],
        }
    ]

    uncovered = uncovered_patchable_clusters(clusters, action_groups)

    assert [c["cluster_id"] for c in uncovered] == ["H001", "H003"]


def test_diagnostic_action_group_for_uncovered_cluster_is_single_cluster_scoped() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        diagnostic_action_group_for_cluster,
    )

    ag = diagnostic_action_group_for_cluster({
        "cluster_id": "H003",
        "root_cause": "wrong_filter_condition",
        "question_ids": ["q013"],
        "asi_counterfactual_fixes": [
            "Pivot day and mtd metrics into separate columns."
        ],
    })

    assert ag["source_cluster_ids"] == ["H003"]
    assert ag["affected_questions"] == ["q013"]
    assert ag["coverage_reason"] == "strategist_omitted_patchable_hard_cluster"
    assert "wrong_filter_condition" in ag["root_cause_summary"]


def test_clusters_for_strategy_includes_large_soft_cluster_when_hard_count_is_small() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        clusters_for_strategy,
    )

    hard = [
        {"cluster_id": "H001", "question_ids": ["q022"]},
        {"cluster_id": "H002", "question_ids": ["q013"]},
    ]
    large_soft = {
        "cluster_id": "S001",
        "question_ids": [f"soft_{i}" for i in range(8)],
        "affected_judges": ["expected_response"],
    }
    small_soft = {
        "cluster_id": "S002",
        "question_ids": ["soft_small"],
        "affected_judges": ["schema_accuracy"],
    }

    selected_hard, selected_soft = clusters_for_strategy(
        hard,
        [small_soft, large_soft],
        hard_only_threshold=3,
        soft_min_questions=5,
        max_soft_clusters=1,
    )

    assert selected_hard == hard
    assert selected_soft == [large_soft]


def test_clusters_for_strategy_still_withholds_soft_when_many_hard_clusters_remain() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        clusters_for_strategy,
    )

    hard = [
        {"cluster_id": "H001", "question_ids": ["q1"]},
        {"cluster_id": "H002", "question_ids": ["q2"]},
        {"cluster_id": "H003", "question_ids": ["q3"]},
        {"cluster_id": "H004", "question_ids": ["q4"]},
    ]
    soft = [{
        "cluster_id": "S001",
        "question_ids": [f"soft_{i}" for i in range(8)],
        "affected_judges": ["expected_response"],
    }]

    selected_hard, selected_soft = clusters_for_strategy(
        hard,
        soft,
        hard_only_threshold=3,
        soft_min_questions=5,
        max_soft_clusters=1,
    )

    assert selected_hard == hard
    assert selected_soft == []


def test_decide_control_plane_acceptance_credits_target_fix_when_pre_rows_present() -> None:
    """Iter-2 shape: q009 fixed, q021 still hard, q001 regressed, +4.5pp."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {
            "id": "q009",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q021",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q026",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q001",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
    ]
    post_rows = [
        {
            "id": "q009",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
        {
            "id": "q021",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q026",
            "feedback/arbiter/value": "genie_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q001",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=86.4,
        candidate_accuracy=90.9,
        target_qids=("q009", "q021"),
        pre_rows=pre_rows,
        post_rows=post_rows,
    )
    assert "q009" in decision.target_fixed_qids, (
        "q009 demonstrably moved from hard to passing — must be in target_fixed_qids"
    )
    assert "q021" in decision.target_still_hard_qids
    assert "q001" in decision.out_of_target_regressed_qids, (
        "q001 demonstrably moved from passing to hard — must be tracked as regression debt"
    )
    # Task 3 — passing→hard regressions are rejected by default
    # (max_new_passing_to_hard_regressions=0); the older debt-acceptance
    # path applied only to soft→hard regressions.
    assert decision.reason_code in {
        "accepted_with_regression_debt",
        "out_of_target_hard_regression",
        "rejected_unbounded_collateral",
    }


def test_decide_control_plane_acceptance_flags_stale_candidate_like_baseline() -> None:
    """Smoking-gun guard: when pre and post hard sets are identical AND there is
    a non-zero accuracy delta, the gate is not comparing against the accepted
    baseline."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    candidate_rows = [
        {
            "id": "q021",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "id": "q001",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=86.4,
        candidate_accuracy=90.9,
        target_qids=("q009", "q021"),
        pre_rows=candidate_rows,
        post_rows=candidate_rows,
    )
    assert decision.accepted is False
    assert decision.reason_code == "stale_or_candidate_pre_rows"
    assert decision.target_fixed_qids == ()
    assert decision.target_still_hard_qids == ()
    assert decision.out_of_target_regressed_qids == ()


def test_decide_control_plane_acceptance_records_empty_pre_rows_as_distinct_reason() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=86.4,
        candidate_accuracy=86.4,
        target_qids=("q009",),
        pre_rows=[],
        post_rows=[
            {
                "id": "q009",
                "feedback/arbiter/value": "both_correct",
                "feedback/result_correctness/value": "yes",
            },
        ],
    )
    assert decision.reason_code == "missing_pre_rows"


def test_control_plane_accepts_soft_to_hard_debt_when_budget_allows() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {
            "question_id": "q_fix_1",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "question_id": "q_fix_2",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "question_id": "q_soft",
            "feedback/arbiter/value": "both_correct",
            "feedback/schema_accuracy/value": "no",
        },
    ]
    post_rows = [
        {
            "question_id": "q_fix_1",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
        {
            "question_id": "q_fix_2",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
        {
            "question_id": "q_soft",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=71.4,
        candidate_accuracy=78.6,
        target_qids=("q_fix_1", "q_fix_2"),
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=1.0,
        max_new_hard_regressions=1,
        max_new_passing_to_hard_regressions=0,
    )

    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_regression_debt"
    assert decision.regression_debt_qids == ("q_soft",)
    assert decision.soft_to_hard_regressed_qids == ("q_soft",)
    assert decision.passing_to_hard_regressed_qids == ()


def test_control_plane_rejects_passing_to_hard_regression_by_default() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {
            "question_id": "q_fix",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "question_id": "q_clean",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
    ]
    post_rows = [
        {
            "question_id": "q_fix",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
        {
            "question_id": "q_clean",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
    ]

    # Task 7 — explicit ``max_new_passing_to_hard_regressions=0`` to opt
    # into the strict legacy policy. The library default now derives
    # the passing-to-hard cap from ``max_new_hard_regressions`` so a
    # single bounded passing-to-hard regression no longer rejects a
    # net-positive AG that fixed its declared causal target.
    decision = decide_control_plane_acceptance(
        baseline_accuracy=50.0,
        candidate_accuracy=75.0,
        target_qids=("q_fix",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=1.0,
        max_new_passing_to_hard_regressions=0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "rejected_unbounded_collateral"
    assert decision.passing_to_hard_regressed_qids == ("q_clean",)


def test_acceptance_detail_includes_regression_tiers() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
        format_control_plane_acceptance_detail,
    )

    detail = format_control_plane_acceptance_detail(
        ControlPlaneAcceptance(
            accepted=True,
            reason_code="accepted_with_regression_debt",
            baseline_accuracy=71.4,
            candidate_accuracy=78.6,
            delta_pp=7.2,
            target_qids=("q005",),
            target_fixed_qids=("q005",),
            target_still_hard_qids=(),
            out_of_target_regressed_qids=("q014",),
            regression_debt_qids=("q014",),
            protected_regressed_qids=(),
            soft_to_hard_regressed_qids=("q014",),
            passing_to_hard_regressed_qids=(),
        )
    )

    assert "regression_debt_qids=q014" in detail
    assert "soft_to_hard_regressed_qids=q014" in detail
    assert "passing_to_hard_regressed_qids=(none)" in detail


def test_ag4_passing_to_hard_within_overall_budget_is_accepted_with_debt() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    def _hard(qid: str) -> dict:
        return {
            "question_id": qid,
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        }

    def _pass(qid: str) -> dict:
        return {
            "question_id": qid,
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        }

    pre_rows = [
        _hard("gs_001"),
        _pass("gs_021"),
        _pass("gs_004"),
        _pass("gs_018"),
        _pass("gs_026"),
    ]
    post_rows = [
        _pass("gs_001"),
        _hard("gs_021"),
        _pass("gs_004"),
        _pass("gs_018"),
        _pass("gs_026"),
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=75.0,
        candidate_accuracy=80.0,
        target_qids=("gs_001",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        max_new_hard_regressions=1,
    )

    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_regression_debt"
    assert decision.target_fixed_qids == ("gs_001",)
    assert decision.target_still_hard_qids == ()
    assert decision.passing_to_hard_regressed_qids == ("gs_021",)
    assert decision.out_of_target_regressed_qids == ("gs_021",)
    assert decision.regression_debt_qids == ("gs_021",)


def test_passing_to_hard_budget_can_be_tightened_below_overall_budget() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    pre_rows = [
        {
            "question_id": "q1",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
        {
            "question_id": "q2",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
    ]
    post_rows = [
        {
            "question_id": "q1",
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": "yes",
        },
        {
            "question_id": "q2",
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": "no",
        },
    ]

    decision = decide_control_plane_acceptance(
        baseline_accuracy=50.0,
        candidate_accuracy=51.0,
        target_qids=("q1",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        max_new_hard_regressions=1,
        max_new_passing_to_hard_regressions=0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "rejected_unbounded_collateral"
    assert decision.passing_to_hard_regressed_qids == ("q2",)


def test_pre_arbiter_regression_without_target_fix_rejects_candidate():
    from genie_space_optimizer.optimization.control_plane import (
        decide_pre_arbiter_regression_guardrail,
    )

    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=69.6,
        candidate_pre_arbiter_accuracy=60.9,
        target_fixed_qids=(),
        max_pre_arbiter_regression_pp=5.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "pre_arbiter_regression_without_target_fix"
    assert decision.delta_pp == -8.7


def test_diagnostic_ag_directives_keys_are_numeric_strings() -> None:
    """Pin the lever-key contract: every directive lever id must be a digit-only string.

    Consumers (_ag_collision_key in harness.py, union_execution_levers in
    rca_execution.py, generate_metadata_proposals lookup tables) all assume
    numeric-string keys. A single producer drifting to "L5"/"L6" hard-crashes
    the loop the first time a coverage AG fires.
    """
    from genie_space_optimizer.optimization.control_plane import (
        _DIAGNOSTIC_AG_DIRECTIVES,
    )

    bad = {
        root: spec["lever"]
        for root, spec in _DIAGNOSTIC_AG_DIRECTIVES.items()
        if not str(spec["lever"]).isdigit()
    }
    assert not bad, (
        "Diagnostic AG directives must use numeric-string lever ids "
        f"(e.g. '5', '6'); found non-digit values: {bad}"
    )


def test_diagnostic_action_group_emits_numeric_lever_directive_keys() -> None:
    """Every diagnostic AG must round-trip through int(lever_key) without raising."""
    from genie_space_optimizer.optimization.control_plane import (
        _DIAGNOSTIC_AG_DIRECTIVES,
        diagnostic_action_group_for_cluster,
    )

    for cluster_root in _DIAGNOSTIC_AG_DIRECTIVES:
        ag = diagnostic_action_group_for_cluster({
            "cluster_id": "H_TEST",
            "root_cause": cluster_root,
            "question_ids": ["q1"],
            "asi_counterfactual_fixes": ["test fix"],
        })
        keys = list(ag["lever_directives"].keys())
        assert keys, f"diagnostic AG for {cluster_root!r} must have a directive"
        assert all(k.isdigit() for k in keys), (
            f"diagnostic AG for {cluster_root!r} emitted non-digit keys: {keys}"
        )
        # Must round-trip cleanly through the harness collision-key path.
        assert all(int(k) >= 1 for k in keys)
