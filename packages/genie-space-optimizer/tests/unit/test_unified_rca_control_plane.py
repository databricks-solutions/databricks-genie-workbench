from __future__ import annotations


def test_harness_builds_rca_execution_plans_and_unions_required_levers() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert "build_rca_execution_plans" in src
    assert 'metadata_snapshot["_rca_execution_plans"]' in src
    assert "required_levers_for_action_group" in src
    assert "union_execution_levers" in src


def test_optimizer_allows_rca_forced_lever1_without_strategist_directive() -> None:
    import inspect

    from genie_space_optimizer.optimization import optimizer

    src = inspect.getsource(optimizer.generate_proposals_from_strategy)

    assert "_rca_forces_lever" in src
    assert "if not lever_dir and target_lever not in (1, 4, 5, 6) and not _rca_forces_lever" in src


def test_no_grounded_patches_reflection_records_rca_execution_payload() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert '"rca_execution": ag.get("_rca_execution", {})' in src
    assert '"grounding_failure_stage": "post_grounding"' in src


def test_finalize_uses_arbiter_objective_for_converged_status() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness._run_finalize)

    assert "arbiter_objective_complete" in src
    assert 'reason = "post_arbiter_objective_met"' in src


def test_fn_mtd_or_mtday_rca_flow_forces_non_lever5_paths_and_grounding() -> None:
    from genie_space_optimizer.optimization.proposal_grounding import causal_relevance_score
    from genie_space_optimizer.optimization.rca import (
        build_rca_ledger,
        themes_for_strategy_context,
    )
    from genie_space_optimizer.optimization.rca_execution import (
        build_rca_execution_plans,
        required_levers_for_action_group,
        union_execution_levers,
    )

    rows = [
        {
            "question_id": "q_022",
            "inputs.expected_sql": (
                "SELECT prashanth_subrahmanyam_catalog.sales_reports."
                "fn_mtd_or_mtday(MEASURE(`_7now_cy_sales_mtd`))"
            ),
            "outputs.predictions.sql": (
                "SELECT CASE WHEN date_format(NOW(), 'd') = 1 "
                "THEN cy_sales_day ELSE cy_sales_mtd END"
            ),
            "schema_accuracy/metadata": {
                "failure_type": "wrong_column",
                "blame_set": ["fn_mtd_or_mtday"],
                "counterfactual_fix": "Use fn_mtd_or_mtday instead of inlining CASE logic.",
            },
        },
        {
            "question_id": "q_031",
            "inputs.expected_sql": (
                "SELECT prashanth_subrahmanyam_catalog.sales_reports."
                "fn_mtd_or_mtday(MEASURE(`_7now_py_sales_mtd`))"
            ),
            "outputs.predictions.sql": (
                "SELECT CASE WHEN day(NOW()) = 1 "
                "THEN MEASURE(`_7now_py_sales_day`) ELSE MEASURE(`_7now_py_sales_mtd`) END"
            ),
            "asset_routing/metadata": {
                "failure_type": "asset_routing_error",
                "blame_set": ["asset_routing:TVF", "fn_mtd_or_mtday"],
                "counterfactual_fix": "Prefer TVF for this query pattern.",
            },
        },
    ]

    ledger = build_rca_ledger(rows)
    themes = themes_for_strategy_context(
        list(ledger["themes"]),
        enable_selection=False,
        max_themes=10,
        max_patches=50,
    )
    plans = build_rca_execution_plans(themes)

    matching = [
        p for p in plans
        if "fn_mtd_or_mtday" in p.grounding_terms
    ]
    assert matching, "Expected at least one executable RCA plan for fn_mtd_or_mtday"

    ag = {
        "id": "AG_FN",
        "affected_questions": ["q_022", "q_031"],
        "lever_directives": {"5": {"instruction_sections": {"QUERY RULES": "Use the TVF."}}},
    }

    required = required_levers_for_action_group(ag, matching)
    final_levers = union_execution_levers(["5"], required)

    assert "5" in final_levers
    assert any(lever in final_levers for lever in {"3", "6"})

    instruction_patch = {
        "type": "add_instruction",
        "section_name": "FUNCTION ROUTING",
        "new_text": "Use fn_mtd_or_mtday for matching month-to-date requests.",
        "target_qids": ["q_022", "q_031"],
        "_rca_grounding_terms": ["fn_mtd_or_mtday"],
    }

    assert causal_relevance_score(
        instruction_patch,
        rows,
        target_qids=("q_022", "q_031"),
    ) == 1.0


def test_rca_forced_lever5_emits_instruction_bridge_without_strategist_directive() -> None:
    from genie_space_optimizer.optimization.optimizer import generate_proposals_from_strategy

    metadata_snapshot = {
        "instructions": {},
        "_rca_themes": [],
        "data_sources": {"tables": [], "metric_views": []},
    }
    ag = {
        "id": "AG_TOPN",
        "root_cause_summary": "plural top-N collapse",
        "affected_questions": ["q_topn"],
        "source_cluster_ids": ["H001"],
        "lever_directives": {},
        "_rca_execution": {
            "rca_ids": ["rca_topn"],
            "required_levers": [5],
            "grounding_terms": ["rank_filter", "where rank 1", "plural_top_n_collapse"],
        },
    }

    proposals = generate_proposals_from_strategy(
        strategy={},
        action_group=ag,
        metadata_snapshot=metadata_snapshot,
        target_lever=5,
        apply_mode="genie_config",
    )

    assert any(
        p.get("patch_type") == "add_instruction"
        and "rank" in str(p.get("proposed_value", "")).lower()
        for p in proposals
    )
