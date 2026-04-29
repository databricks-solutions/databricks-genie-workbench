from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme


def _theme(
    *,
    rca_id: str = "rca_fn",
    kind: RcaKind = RcaKind.FILTER_LOGIC_MISMATCH,
    patches: tuple[dict, ...] = (),
    target_qids: tuple[str, ...] = ("q_fn_1",),
    touched_objects: tuple[str, ...] = ("fn_mtd_or_mtday",),
) -> RcaPatchTheme:
    return RcaPatchTheme(
        rca_id=rca_id,
        rca_kind=kind,
        patch_family="function_routing_guidance",
        patches=patches,
        target_qids=target_qids,
        touched_objects=touched_objects,
        confidence=0.9,
        evidence_summary="Genie inlined CASE logic instead of using fn_mtd_or_mtday.",
    )


def test_build_rca_execution_plan_extracts_required_levers_and_terms() -> None:
    from genie_space_optimizer.optimization.rca_execution import (
        build_rca_execution_plans,
    )

    plans = build_rca_execution_plans([
        _theme(
            patches=(
                {
                    "type": "add_instruction",
                    "lever": 3,
                    "intent": "Route fn_mtd_or_mtday requests to the TVF.",
                    "target": "fn_mtd_or_mtday",
                },
                {
                    "type": "add_sql_snippet_expression",
                    "lever": 6,
                    "snippet_name": "fn_mtd_or_mtday_usage",
                    "expression": "fn_mtd_or_mtday(MEASURE(`_7now_cy_sales_mtd`))",
                },
            ),
        )
    ])

    assert len(plans) == 1
    plan = plans[0]
    assert plan.rca_id == "rca_fn"
    assert plan.target_qids == ("q_fn_1",)
    assert plan.required_levers == (3, 6)
    assert "fn_mtd_or_mtday" in plan.grounding_terms
    assert "fn_mtd_or_mtday_usage" in plan.grounding_terms
    assert plan.defect_key == "function_routing_guidance:fn_mtd_or_mtday"


def test_required_levers_for_action_group_include_matching_rca_plan() -> None:
    from genie_space_optimizer.optimization.rca_execution import (
        build_rca_execution_plans,
        required_levers_for_action_group,
    )

    plans = build_rca_execution_plans([
        _theme(
            patches=(
                {"type": "add_instruction", "lever": 3, "target": "fn_mtd_or_mtday"},
                {"type": "add_sql_snippet_expression", "lever": 6, "target": "fn_mtd_or_mtday"},
            ),
            target_qids=("q_a", "q_b"),
        )
    ])
    action_group = {
        "id": "AG1",
        "affected_questions": ["q_b"],
        "lever_directives": {"5": {"instruction_sections": {"QUERY RULES": "Use the function."}}},
    }

    assert required_levers_for_action_group(action_group, plans) == (3, 6)


def test_union_levers_preserves_order_and_adds_required_rca_levers() -> None:
    from genie_space_optimizer.optimization.rca_execution import union_execution_levers

    assert union_execution_levers(["5"], (3, 6)) == ["5", "3", "6"]
    assert union_execution_levers(["6", "5"], (3, 6)) == ["6", "5", "3"]


def test_repeated_no_grounded_patches_forces_required_levers() -> None:
    from genie_space_optimizer.optimization.rca_execution import forced_levers_from_reflections

    reflection_buffer = [
        {
            "accepted": False,
            "rollback_reason": "no_grounded_patches",
            "rca_execution": {"rca_ids": ["rca_fn"], "required_levers": [3, 6]},
        },
        {
            "accepted": False,
            "rollback_reason": "no_grounded_patches",
            "rca_execution": {"rca_ids": ["rca_fn"], "required_levers": [3, 6]},
        },
    ]

    assert forced_levers_from_reflections(
        reflection_buffer,
        target_rca_ids=("rca_fn",),
        min_repeats=2,
    ) == (3, 6)


def test_clusters_with_same_function_blame_are_compatible_despite_root_cause_label() -> None:
    from genie_space_optimizer.optimization.rca_execution import clusters_share_defect_identity

    h003 = {
        "cluster_id": "H003",
        "root_cause": "wrong_table",
        "asi_blame_set": ["fn_mtd_or_mtday"],
        "question_ids": ["q_022"],
    }
    h004 = {
        "cluster_id": "H004",
        "root_cause": "wrong_filter_condition",
        "asi_blame_set": ["asset_routing:TVF", "fn_mtd_or_mtday"],
        "question_ids": ["q_031"],
    }

    assert clusters_share_defect_identity(h003, h004) is True


def test_clusters_with_different_blame_do_not_share_defect_identity() -> None:
    from genie_space_optimizer.optimization.rca_execution import clusters_share_defect_identity

    zone = {
        "cluster_id": "H002",
        "root_cause": "plural_top_n_collapse",
        "asi_blame_set": ["zone_vp_name", "zone_combination"],
        "question_ids": ["q_025"],
    }
    fn = {
        "cluster_id": "H004",
        "root_cause": "wrong_filter_condition",
        "asi_blame_set": ["fn_mtd_or_mtday"],
        "question_ids": ["q_031"],
    }

    assert clusters_share_defect_identity(zone, fn) is False


def test_optimizer_uses_defect_identity_for_ag_scope_binding() -> None:
    import inspect

    from genie_space_optimizer.optimization import optimizer

    src = inspect.getsource(optimizer)

    assert "clusters_share_defect_identity" in src
    assert "AG scope bound (RCA defect identity)" in src


def test_execution_plan_uses_theme_recommended_levers_even_if_patch_intents_are_sparse() -> None:
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme
    from genie_space_optimizer.optimization.rca_execution import build_rca_execution_plans

    theme = RcaPatchTheme(
        rca_id="rca_topn",
        rca_kind=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        patch_family="cardinality_preserving_top_n_guidance",
        patches=(
            {
                "type": "request_example_sql_synthesis",
                "lever": 5,
                "root_cause": "plural_top_n_collapse",
                "intent": "synthesize ordered-list example SQL",
            },
        ),
        target_qids=("q_topn",),
        touched_objects=("rank_filter",),
        confidence=0.9,
        evidence_summary="Remove WHERE rank = 1.",
        recommended_levers=(1, 5, 6),
    )

    plans = build_rca_execution_plans([theme])

    assert plans[0].required_levers == (1, 5, 6)


def test_repeated_no_overlap_forces_patch_family_rotation() -> None:
    from genie_space_optimizer.optimization.rca_execution import (
        next_grounding_remediation,
    )

    reflection_buffer = [
        {
            "accepted": False,
            "rollback_reason": "no_grounded_patches",
            "rca_execution": {"rca_ids": ["rca_topn"], "required_levers": [1, 5, 6]},
            "grounding_failure_category": "no_overlap",
        },
        {
            "accepted": False,
            "rollback_reason": "no_grounded_patches",
            "rca_execution": {"rca_ids": ["rca_topn"], "required_levers": [1, 5, 6]},
            "grounding_failure_category": "no_overlap",
        },
    ]

    remediation = next_grounding_remediation(
        reflection_buffer,
        target_rca_ids=("rca_topn",),
    )

    assert remediation["action"] == "rotate_patch_family"
    assert remediation["forced_levers"] == (5, 6)


def test_execution_plan_lookup_recovers_qids_from_source_clusters_when_affected_questions_are_text() -> None:
    from genie_space_optimizer.optimization.rca_execution import (
        RcaExecutionPlan,
        plans_for_action_group,
        required_levers_for_action_group,
        target_qids_for_action_group_execution,
    )

    plans = [
        RcaExecutionPlan(
            rca_id="rca_q1_topn",
            rca_kind="top_n_cardinality_collapse",
            patch_family="cardinality_preserving_top_n_guidance",
            target_qids=("q1",),
            required_levers=(1, 5, 6),
            grounding_terms=("zone_vp", "rank", "plural_top_n_collapse"),
            defect_key="cardinality_preserving_top_n_guidance:zone_vp",
            patch_intents=(),
        )
    ]
    ag = {
        "id": "AG5",
        "source_cluster_ids": ["cluster_topn"],
        "affected_questions": ["Which zone VPs stores have the highest CY sales?"],
    }
    source_clusters = [
        {
            "cluster_id": "cluster_topn",
            "question_ids": ["q1"],
            "root_cause": "plural_top_n_collapse",
        }
    ]

    assert target_qids_for_action_group_execution(ag, source_clusters) == ("q1",)
    assert required_levers_for_action_group(ag, plans, source_clusters=source_clusters) == (1, 5, 6)
    assert [p.rca_id for p in plans_for_action_group(ag, plans, source_clusters=source_clusters)] == ["rca_q1_topn"]
