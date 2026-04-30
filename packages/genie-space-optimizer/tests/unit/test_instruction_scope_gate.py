from __future__ import annotations

from genie_space_optimizer.optimization.proposal_grounding import (
    instruction_patch_scope_is_safe,
)


def test_rejects_global_rewrite_without_targets_or_dependents() -> None:
    patch = {
        "type": "rewrite_instruction",
        "section_name": "QUERY RULES",
        "new_text": "QUERY RULES:\n- Always enforce time_window filters.",
    }
    decision = instruction_patch_scope_is_safe(
        patch,
        ag_target_qids=("q009", "q021"),
    )
    assert decision["safe"] is False
    assert decision["reason"] == "global_instruction_scope_without_dependents"


def test_rejects_asset_routing_section_without_specific_target() -> None:
    patch = {
        "type": "update_instruction_section",
        "section_name": "ASSET ROUTING",
        "new_text": "Prefer fact_sales for all sales questions.",
    }
    decision = instruction_patch_scope_is_safe(
        patch,
        ag_target_qids=("q013",),
    )
    assert decision["safe"] is False
    assert decision["reason"] == "global_instruction_scope_without_dependents"


def test_allows_instruction_with_counterfactual_dependents_checked_elsewhere() -> None:
    patch = {
        "type": "update_instruction_section",
        "section_name": "QUERY RULES",
        "passing_dependents": ["q009"],
        "new_text": "For q009-style current-day facts, use time_window = 'day'.",
    }
    decision = instruction_patch_scope_is_safe(
        patch,
        ag_target_qids=("q009",),
    )
    assert decision["safe"] is True
    assert decision["reason"] == "has_counterfactual_dependents"


def test_allows_narrow_non_global_instruction_section() -> None:
    patch = {
        "type": "update_instruction_section",
        "section_name": "DATA QUALITY NOTES",
        "target_qids": ["q021"],
        "new_text": "For q021, month-to-date means time_window = 'mtd'.",
    }
    decision = instruction_patch_scope_is_safe(
        patch,
        ag_target_qids=("q021",),
    )
    assert decision["safe"] is True
    assert decision["reason"] == "narrow_instruction_scope"


def test_rejects_global_add_instruction_without_targets_or_dependents() -> None:
    patch = {
        "type": "add_instruction",
        "section_name": "QUERY RULES",
        "new_text": "APSD KPI queries require UNION ALL of Day and MTD periods.",
    }

    decision = instruction_patch_scope_is_safe(
        patch,
        ag_target_qids=("q002", "q005", "q009"),
    )

    assert decision["safe"] is False
    assert decision["reason"] == "global_instruction_scope_without_dependents"
