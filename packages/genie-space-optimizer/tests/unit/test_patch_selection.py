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
