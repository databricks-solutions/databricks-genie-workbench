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
    assert out[0].regressed_qids == ("retail_003",)


def test_metric_view_routing_confusion_does_not_recommend_lever6():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        recommended_levers_for_rca_kind,
    )

    assert recommended_levers_for_rca_kind(RcaKind.METRIC_VIEW_ROUTING_CONFUSION) == (1, 5)


def test_extra_defensive_filter_routes_to_instruction_not_sql_snippet():
    from genie_space_optimizer.optimization.rca import (
        RcaKind,
        recommended_levers_for_rca_kind,
    )

    assert recommended_levers_for_rca_kind(RcaKind.EXTRA_DEFENSIVE_FILTER) == (3, 5)


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
