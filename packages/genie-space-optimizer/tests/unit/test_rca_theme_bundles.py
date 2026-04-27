from __future__ import annotations

from genie_space_optimizer.optimization.rca import (
    RcaFinding,
    RcaKind,
    compile_patch_themes,
    detect_theme_conflicts,
)


def test_compile_metric_routing_theme_contains_contrastive_metadata_patches():
    finding = RcaFinding(
        rca_id="rca_avg_txn",
        question_id="retail_010",
        rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
        confidence=0.9,
        expected_objects=("mv_esr_store_sales", "avg_txn_day", "avg_txn_mtd"),
        actual_objects=("mv_7now_store_sales", "7now_avg_txn_cy_day"),
        recommended_levers=(1, 5),
        patch_family="contrastive_metric_routing",
        target_qids=("retail_010", "retail_027"),
    )

    themes = compile_patch_themes([finding], metadata_snapshot={})

    assert len(themes) == 1
    theme = themes[0]
    assert theme.rca_id == "rca_avg_txn"
    assert theme.patch_family == "contrastive_metric_routing"
    assert set(theme.target_qids) == {"retail_010", "retail_027"}
    assert any(p["type"] == "update_column_description" for p in theme.patches)
    assert any(p["type"] == "update_description" for p in theme.patches)


def test_detect_theme_conflicts_on_same_instruction_section():
    themes = compile_patch_themes([
        RcaFinding(
            rca_id="rca_a",
            question_id="q1",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            confidence=0.8,
            actual_objects=("IS NOT NULL",),
            recommended_levers=(5,),
            patch_family="avoid_unrequested_defensive_filters",
            target_qids=("q1",),
        ),
        RcaFinding(
            rca_id="rca_b",
            question_id="q2",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            confidence=0.8,
            actual_objects=("IS NOT NULL",),
            recommended_levers=(5,),
            patch_family="avoid_unrequested_defensive_filters",
            target_qids=("q2",),
        ),
    ], metadata_snapshot={})

    conflicts = detect_theme_conflicts(themes)

    assert conflicts
    assert conflicts[0].object_id == "QUERY CONSTRUCTION"


def test_theme_patch_metadata_survives_proposal_to_patch_conversion():
    from genie_space_optimizer.optimization.applier import proposals_to_patches

    proposal = {
        "patch_type": "update_column_description",
        "lever": 1,
        "column": "avg_txn_day",
        "description": "Enterprise average transaction value.",
        "rca_id": "rca_avg_txn",
        "patch_family": "contrastive_metric_routing",
        "target_qids": ["retail_010"],
    }

    patches = proposals_to_patches([proposal])

    assert patches[0]["rca_id"] == "rca_avg_txn"
    assert patches[0]["patch_family"] == "contrastive_metric_routing"
    assert patches[0]["target_qids"] == ["retail_010"]


def test_attribute_theme_outcome_partitions_fixed_still_failing_and_regressed_qids():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        RcaPatchTheme,
        attribute_theme_outcomes,
    )

    themes = [
        RcaPatchTheme(
            rca_id="rca_avg_txn",
            rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
            patch_family="contrastive_metric_routing",
            patches=(),
            target_qids=("retail_010", "retail_027"),
            touched_objects=("avg_txn_day",),
        )
    ]

    out = attribute_theme_outcomes(
        themes,
        prev_failure_qids={"retail_010", "retail_027"},
        new_failure_qids={"retail_027", "retail_003"},
    )

    assert out[0].fixed_qids == ("retail_010",)
    assert out[0].still_failing_qids == ("retail_027",)
    assert out[0].target_regressed_qids == ()
    assert out[0].global_regressed_qids == ("retail_003",)
    assert out[0].regressed_qids == ()


def test_attribute_theme_outcome_separates_target_and_global_regressions():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        RcaPatchTheme,
        attribute_theme_outcomes,
    )

    themes = [
        RcaPatchTheme(
            rca_id="rca_q1",
            rca_kind=RcaKind.MEASURE_SWAP,
            patch_family="contrastive_measure_disambiguation",
            patches=(),
            target_qids=("q1",),
            touched_objects=("m1",),
        )
    ]

    out = attribute_theme_outcomes(
        themes,
        prev_failure_qids={"q1"},
        new_failure_qids={"q3"},
    )

    assert out[0].fixed_qids == ("q1",)
    assert out[0].target_regressed_qids == ()
    assert out[0].global_regressed_qids == ("q3",)


def test_metric_view_routing_confusion_does_not_recommend_lever6():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        recommended_levers_for_rca_kind,
    )

    assert 6 not in recommended_levers_for_rca_kind(RcaKind.METRIC_VIEW_ROUTING_CONFUSION)


def test_recommended_levers_cover_metadata_synonyms_sql_joins_instructions_and_examples():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        recommended_levers_for_rca_kind,
    )

    expected = {
        RcaKind.METRIC_VIEW_ROUTING_CONFUSION: (1, 2, 5),
        RcaKind.MEASURE_SWAP: (1, 2, 5, 6),
        RcaKind.CANONICAL_DIMENSION_MISSED: (1, 2, 5, 6),
        RcaKind.MISSING_REQUIRED_DIMENSION: (1, 5, 6),
        RcaKind.EXTRA_DEFENSIVE_FILTER: (5,),
        RcaKind.JOIN_SPEC_MISSING_OR_WRONG: (4, 5),
        RcaKind.FILTER_LOGIC_MISMATCH: (2, 5, 6),
        RcaKind.GRAIN_OR_GROUPING_MISMATCH: (1, 5, 6),
        RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING: (1,),
        RcaKind.SQL_EXPRESSION_MISSING: (6,),
        RcaKind.EXAMPLE_SQL_SHAPE_NEEDED: (5,),
    }

    for kind, levers in expected.items():
        assert recommended_levers_for_rca_kind(kind) == levers


def test_extra_defensive_filter_routes_to_instruction_not_sql_snippet():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        recommended_levers_for_rca_kind,
    )

    assert recommended_levers_for_rca_kind(RcaKind.EXTRA_DEFENSIVE_FILTER) == (5,)


def test_extra_defensive_filter_recommendations_match_theme_patch_levers():
    from genie_space_optimizer.optimization.rca import (
        RcaFinding,
        RcaKind,
        compile_patch_themes,
        recommended_levers_for_rca_kind,
    )

    finding = RcaFinding(
        rca_id="rca_filter",
        question_id="q_filter",
        rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
        confidence=0.8,
        actual_objects=("cy_cust_count IS NOT NULL",),
        recommended_levers=recommended_levers_for_rca_kind(
            RcaKind.EXTRA_DEFENSIVE_FILTER,
        ),
        patch_family="avoid_unrequested_defensive_filters",
        target_qids=("q_filter",),
    )

    theme = compile_patch_themes([finding], metadata_snapshot={})[0]

    assert set(finding.recommended_levers) == {p["lever"] for p in theme.patches}


def test_rca_theme_levers_override_wrong_aggregation_coarse_route():
    from genie_space_optimizer.optimization.rca import (
        RcaFinding,
        RcaKind,
        compile_patch_themes,
    )

    finding = RcaFinding(
        rca_id="rca_avg_txn",
        question_id="retail_010",
        rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
        confidence=0.9,
        expected_objects=("mv_esr_store_sales", "avg_txn_day"),
        actual_objects=("mv_7now_store_sales", "7now_avg_txn_cy_day"),
        recommended_levers=(1, 5),
        patch_family="contrastive_metric_routing",
        target_qids=("retail_010",),
    )

    theme = compile_patch_themes([finding], metadata_snapshot={})[0]

    assert {p["lever"] for p in theme.patches} == {1}


def test_compile_measure_swap_theme_emits_metadata_synonym_sql_and_example_intents():
    from genie_space_optimizer.optimization.rca import (
        RcaFinding,
        RcaKind,
        compile_patch_themes,
        recommended_levers_for_rca_kind,
    )

    finding = RcaFinding(
        rca_id="rca_measure_swap",
        question_id="q_measure",
        rca_kind=RcaKind.MEASURE_SWAP,
        confidence=0.86,
        expected_objects=("gross_sales",),
        actual_objects=("net_sales",),
        evidence=(),
        recommended_levers=recommended_levers_for_rca_kind(RcaKind.MEASURE_SWAP),
        patch_family="contrastive_measure_disambiguation",
        target_qids=("q_measure",),
    )

    theme = compile_patch_themes([finding], metadata_snapshot={})[0]
    patch_types = {p["type"] for p in theme.patches}

    assert "update_column_description" in patch_types
    assert "add_column_synonym" in patch_types
    assert "add_sql_snippet_measure" in patch_types
    assert "request_example_sql_synthesis" in patch_types
    assert {1, 5, 6}.issubset({p["lever"] for p in theme.patches})


def test_compile_join_theme_emits_join_spec_and_example_intents():
    from genie_space_optimizer.optimization.rca import (
        RcaFinding,
        RcaKind,
        compile_patch_themes,
        recommended_levers_for_rca_kind,
    )

    finding = RcaFinding(
        rca_id="rca_join",
        question_id="q_join",
        rca_kind=RcaKind.JOIN_SPEC_MISSING_OR_WRONG,
        confidence=0.83,
        expected_objects=("orders.customer_id", "customers.customer_id"),
        actual_objects=(),
        recommended_levers=recommended_levers_for_rca_kind(
            RcaKind.JOIN_SPEC_MISSING_OR_WRONG
        ),
        patch_family="join_spec_guidance",
        target_qids=("q_join",),
    )

    theme = compile_patch_themes([finding], metadata_snapshot={})[0]
    patch_types = {p["type"] for p in theme.patches}

    assert "add_join_spec" in patch_types
    assert "request_example_sql_synthesis" in patch_types
    assert {4, 5}.issubset({p["lever"] for p in theme.patches})


def test_compile_extra_defensive_filter_stays_instruction_only():
    from genie_space_optimizer.optimization.rca import (
        RcaFinding,
        RcaKind,
        compile_patch_themes,
    )

    finding = RcaFinding(
        rca_id="rca_defensive_filter",
        question_id="q_filter",
        rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
        confidence=0.8,
        actual_objects=("IS NOT NULL",),
        recommended_levers=(5,),
        patch_family="avoid_unrequested_defensive_filters",
        target_qids=("q_filter",),
    )

    theme = compile_patch_themes([finding], metadata_snapshot={})[0]

    assert [p["type"] for p in theme.patches] == ["add_instruction"]
    assert [p["lever"] for p in theme.patches] == [5]


def test_select_compatible_themes_keeps_non_conflicting_high_confidence_themes():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        RcaPatchTheme,
        select_compatible_themes,
    )

    themes = [
        RcaPatchTheme(
            rca_id="rca_avg_txn",
            rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
            patch_family="contrastive_metric_routing",
            patches=({"type": "update_column_description", "column": "avg_txn_day"},),
            target_qids=("retail_010",),
            touched_objects=("avg_txn_day",),
            confidence=0.9,
        ),
        RcaPatchTheme(
            rca_id="rca_calendar_month",
            rca_kind=RcaKind.CANONICAL_DIMENSION_MISSED,
            patch_family="canonical_dimension_guidance",
            patches=({"type": "update_column_description", "column": "calendar_month"},),
            target_qids=("retail_003",),
            touched_objects=("calendar_month",),
            confidence=0.85,
        ),
    ]

    selected = select_compatible_themes(themes, max_themes=3, max_patches=5)

    assert [t.rca_id for t in selected] == ["rca_avg_txn", "rca_calendar_month"]


def test_select_compatible_themes_drops_lower_confidence_conflict():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        RcaPatchTheme,
        select_compatible_themes,
    )

    themes = [
        RcaPatchTheme(
            rca_id="rca_high",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            patch_family="avoid_unrequested_defensive_filters",
            patches=({"type": "add_instruction", "instruction_section": "QUERY CONSTRUCTION"},),
            target_qids=("q1",),
            touched_objects=("QUERY CONSTRUCTION",),
            confidence=0.9,
        ),
        RcaPatchTheme(
            rca_id="rca_low",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            patch_family="avoid_unrequested_defensive_filters",
            patches=({"type": "add_instruction", "instruction_section": "QUERY CONSTRUCTION"},),
            target_qids=("q2",),
            touched_objects=("QUERY CONSTRUCTION",),
            confidence=0.6,
        ),
    ]

    selected = select_compatible_themes(themes, max_themes=3, max_patches=5)

    assert [t.rca_id for t in selected] == ["rca_high"]


def test_strategy_context_theme_selection_flag_controls_pruning():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        RcaPatchTheme,
        themes_for_strategy_context,
    )

    themes = [
        RcaPatchTheme(
            rca_id="rca_high",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            patch_family="avoid_unrequested_defensive_filters",
            patches=({"type": "add_instruction", "instruction_section": "QUERY CONSTRUCTION"},),
            target_qids=("q1",),
            touched_objects=("QUERY CONSTRUCTION",),
            confidence=0.9,
        ),
        RcaPatchTheme(
            rca_id="rca_low",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            patch_family="avoid_unrequested_defensive_filters",
            patches=({"type": "add_instruction", "instruction_section": "QUERY CONSTRUCTION"},),
            target_qids=("q2",),
            touched_objects=("QUERY CONSTRUCTION",),
            confidence=0.6,
        ),
    ]

    all_themes = themes_for_strategy_context(
        themes,
        enable_selection=False,
        max_themes=3,
        max_patches=5,
    )
    selected = themes_for_strategy_context(
        themes,
        enable_selection=True,
        max_themes=3,
        max_patches=5,
    )

    assert [t.rca_id for t in all_themes] == ["rca_high", "rca_low"]
    assert [t.rca_id for t in selected] == ["rca_high"]


def test_rca_theme_selector_flag_off_returns_empty_strategy_context():
    from genie_space_optimizer.optimization.optimizer import (
        _format_rca_themes_for_strategy,
    )

    assert _format_rca_themes_for_strategy([], []) == "(No typed RCA themes available.)"
