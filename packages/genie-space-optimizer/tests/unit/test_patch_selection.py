from __future__ import annotations


def test_causal_patch_cap_keeps_function_routing_over_lower_relevance_instruction_split() -> None:
    from genie_space_optimizer.optimization.patch_selection import select_causal_patch_cap

    patches = [
        {
            "proposal_id": "P001",
            "lever": 5,
            "patch_type": "update_instruction_section",
            "section_name": "QUERY RULES",
            "new_text": "Use store_count for store counts.",
            "relevance_score": 0.55,
            "risk_level": "low",
            "confidence": 0.90,
        },
        {
            "proposal_id": "P015",
            "lever": 5,
            "patch_type": "update_instruction_section",
            "section_name": "FUNCTION ROUTING",
            "new_text": "When the user asks for month-to-date/day logic, use fn_mtd_or_mtday.",
            "relevance_score": 1.0,
            "risk_level": "medium",
            "confidence": 0.78,
            "rca_id": "rca_q028_function_routing",
            "target_qids": ["q028"],
        },
        {
            "proposal_id": "P003",
            "lever": 1,
            "patch_type": "update_column_description",
            "column": "store_count",
            "relevance_score": 0.45,
            "risk_level": "low",
            "confidence": 0.95,
        },
    ]

    selected, decisions = select_causal_patch_cap(patches, max_patches=1)

    assert [p["proposal_id"] for p in selected] == ["P015"]
    assert {d["proposal_id"]: d["decision"] for d in decisions} == {
        "P001": "dropped",
        "P015": "selected",
        "P003": "dropped",
    }
    assert decisions[1]["selection_reason"] == "highest_causal_relevance"


def test_causal_patch_cap_uses_lever_diversity_only_after_relevance_ties() -> None:
    from genie_space_optimizer.optimization.patch_selection import select_causal_patch_cap

    patches = [
        {"proposal_id": "P1", "lever": 5, "patch_type": "add_instruction", "relevance_score": 0.9},
        {"proposal_id": "P2", "lever": 5, "patch_type": "add_instruction", "relevance_score": 0.9},
        {"proposal_id": "P3", "lever": 3, "patch_type": "update_function_description", "relevance_score": 0.9},
    ]

    selected, decisions = select_causal_patch_cap(patches, max_patches=2)

    assert [p["proposal_id"] for p in selected] == ["P1", "P3"]
    assert [d["decision"] for d in decisions if d["proposal_id"] in {"P1", "P3"}] == [
        "selected",
        "selected",
    ]


def test_causal_patch_cap_returns_all_patches_when_under_limit() -> None:
    from genie_space_optimizer.optimization.patch_selection import select_causal_patch_cap

    patches = [
        {"proposal_id": "P1", "lever": 5, "patch_type": "add_instruction", "relevance_score": 0.2},
        {"proposal_id": "P2", "lever": 6, "patch_type": "add_sql_snippet_measure", "relevance_score": 0.3},
    ]

    selected, decisions = select_causal_patch_cap(patches, max_patches=4)

    assert selected == patches
    assert [d["decision"] for d in decisions] == ["selected", "selected"]


def test_patch_selection_uses_source_proposal_id_fallback() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_causal_patch_cap,
    )

    patches = [
        {
            "source_proposal_id": "PARENT_A",
            "patch_type": "add_instruction",
            "relevance_score": 1.0,
            "lever": 5,
        },
        {
            "source_proposal_id": "PARENT_B",
            "patch_type": "update_column_description",
            "relevance_score": 0.5,
            "lever": 1,
        },
    ]

    selected, decisions = select_causal_patch_cap(patches, max_patches=1)

    assert selected == [patches[0]]
    assert decisions[0]["proposal_id"] == "PARENT_A"
    assert decisions[1]["proposal_id"] == "PARENT_B"


def test_target_aware_cap_keeps_one_patch_per_target_before_global_rank() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P_day",
            "patch_type": "add_sql_snippet_filter",
            "relevance_score": 0.95,
            "target_qids": ["q002", "q005", "q009"],
            "risk_level": "low",
        },
        {
            "proposal_id": "P_instruction",
            "patch_type": "add_instruction",
            "relevance_score": 0.90,
            "target_qids": ["q002", "q005", "q009"],
            "risk_level": "medium",
        },
        {
            "proposal_id": "P_q008_column",
            "patch_type": "update_column_description",
            "relevance_score": 0.62,
            "target_qids": ["q008"],
            "risk_level": "low",
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q002", "q005", "q008", "q009"),
        max_patches=2,
    )

    assert [p["proposal_id"] for p in selected] == ["P_day", "P_q008_column"]
    assert {d["proposal_id"]: d["decision"] for d in decisions} == {
        "P_day": "selected",
        "P_instruction": "dropped",
        "P_q008_column": "selected",
    }
    assert {
        d["proposal_id"]: d["selection_reason"] for d in decisions
    }["P_q008_column"] == "target_coverage"


def test_target_aware_cap_prefers_explicit_rca_attribution_over_broad_ag_ties() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P002_broad_location",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007", "q005", "q002", "q009"],
            "source_cluster_ids": ["H001", "H003", "H005", "H006"],
        },
        {
            "proposal_id": "P008_rca_sales_day",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007"],
            "rca_id": "rca_q007_measure_swap",
        },
        {
            "proposal_id": "P047_filter",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "relevance_score": 1.0,
            "risk_level": "low",
            "_grounding_target_qids": ["q007"],
            "rca_id": "rca_q007_filter_logic_mismatch",
        },
        {
            "proposal_id": "P045_rewrite_instruction",
            "type": "rewrite_instruction",
            "lever": 5,
            "relevance_score": 1.0,
            "risk_level": "high",
            "target_qids": ["q007", "q005", "q002", "q009"],
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q007", "q005", "q002", "q009"),
        max_patches=3,
    )

    selected_ids = [p["proposal_id"] for p in selected]
    assert "P008_rca_sales_day" in selected_ids
    assert "P047_filter" in selected_ids
    by_attribution = {d["proposal_id"]: d["causal_attribution_tier"] for d in decisions}
    assert by_attribution["P008_rca_sales_day"] > by_attribution["P002_broad_location"]

    by_id = {d["proposal_id"]: d for d in decisions}
    rca_decision = by_id["P008_rca_sales_day"]
    assert rca_decision["rca_id"] == "rca_q007_measure_swap"
    assert rca_decision["target_qids"] == ["q007"]
    assert rca_decision["lever"] == 1
    assert rca_decision["patch_type"] == "update_column_description"
    assert rca_decision["parent_proposal_id"] == "P008_rca_sales_day"
    assert rca_decision["expanded_patch_id"] == "P008_rca_sales_day"
    assert rca_decision["causal_attribution_tier"] == 3


def test_target_aware_cap_dedupes_selected_and_dropped_by_expanded_patch_id() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P001",
            "expanded_patch_id": "P001#2",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007"],
        },
        {
            "proposal_id": "P001",
            "expanded_patch_id": "P001#2",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "risk_level": "low",
            "target_qids": ["q007"],
        },
        {
            "proposal_id": "P002",
            "expanded_patch_id": "P002#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 0.9,
            "risk_level": "low",
            "target_qids": ["q007"],
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q007",),
        max_patches=1,
    )

    selected_identities = [
        d.get("expanded_patch_id") or d.get("proposal_id") for d in selected
    ]
    decision_identities = [
        d.get("expanded_patch_id") or d.get("proposal_id") for d in decisions
    ]

    assert selected_identities.count("P001#2") <= 1
    assert decision_identities.count("P001#2") <= 1


def test_behavior_failure_cap_preserves_direct_lever6_patch() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P001#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P002#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P003#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P023#1",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "relevance_score": 0.9,
            "target_qids": ["q1"],
            "root_cause": "missing_filter",
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q1",),
        max_patches=3,
    )

    selected_ids = {p["proposal_id"] for p in selected}
    assert "P023#1" in selected_ids
    selected_reasons = {
        d["proposal_id"]: d["selection_reason"]
        for d in decisions
        if d["decision"] == "selected"
    }
    assert selected_reasons["P023#1"] == "behavior_direct_fix_reserved"


def test_non_behavior_failure_keeps_existing_causal_ranking() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        {
            "proposal_id": "P001#1",
            "type": "update_column_description",
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_q1",
            "target_qids": ["q1"],
            "root_cause": "column_disambiguation",
        },
        {
            "proposal_id": "P006#1",
            "type": "add_instruction",
            "lever": 5,
            "relevance_score": 0.1,
            "target_qids": ["q1"],
            "root_cause": "column_disambiguation",
        },
    ]

    selected, _decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q1",),
        max_patches=1,
    )

    assert [p["proposal_id"] for p in selected] == ["P001#1"]


def test_harness_patch_cap_log_discloses_dropped_count_and_truncation() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    cap_idx = source.index("PATCH CAP APPLIED (causal-first)")
    snippet = source[cap_idx - 800 : cap_idx + 1200]
    assert "Dropped count" in snippet
    assert "Dropped shown" in snippet
    assert "Dropped truncated" in snippet
