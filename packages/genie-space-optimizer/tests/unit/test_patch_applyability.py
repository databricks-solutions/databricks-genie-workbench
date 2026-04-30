from __future__ import annotations

from genie_space_optimizer.optimization.patch_applyability import (
    PatchApplyabilityDecision,
    check_patch_applyability,
    filter_applyable_patches,
)


def _snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.mv_esr_store_sales",
                    "name": "mv_esr_store_sales",
                    "column_configs": [
                        {
                            "column_name": "apsd_sales_usd_py_day",
                            "data_type": "DOUBLE",
                            "description": [],
                        },
                        {
                            "column_name": "is_finance_monthly_same_store",
                            "data_type": "STRING",
                            "description": [],
                        },
                    ],
                }
            ]
        },
        "instructions": {"text_instructions": [{"content": "PURPOSE:\n- test"}]},
    }


def test_column_patch_without_table_is_not_applyable() -> None:
    patch = {
        "proposal_id": "P003#1",
        "type": "update_column_description",
        "column": "apsd_sales_usd_py_day",
        "structured_sections": {"purpose": "Prior-year APSD sales."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision == PatchApplyabilityDecision(
        proposal_id="P003#1",
        expanded_patch_id="P003#1",
        patch_type="update_column_description",
        target="",
        table="",
        column="apsd_sales_usd_py_day",
        applyable=False,
        reason="missing_table",
        error_excerpt="",
    )


def test_column_patch_with_multi_column_target_is_not_applyable() -> None:
    patch = {
        "proposal_id": "P002#1",
        "type": "update_column_description",
        "table": "main.sales.mv_esr_store_sales",
        "column": ["apsd_sales_usd_py_day", "apsd_sales_usd_day"],
        "structured_sections": {"description": "APSD fields."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is False
    assert decision.reason == "invalid_column_target"
    assert decision.table == "main.sales.mv_esr_store_sales"


def test_column_patch_with_missing_config_table_is_not_applyable() -> None:
    patch = {
        "proposal_id": "P004#1",
        "type": "update_column_description",
        "table": "main.sales.missing_table",
        "column": "apsd_sales_usd_py_day",
        "structured_sections": {"purpose": "Prior-year APSD sales."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is False
    assert decision.reason == "missing_table"
    assert decision.table == "main.sales.missing_table"


def test_well_formed_column_patch_is_applyable() -> None:
    patch = {
        "proposal_id": "P005#1",
        "type": "update_column_description",
        "table": "main.sales.mv_esr_store_sales",
        "column": "apsd_sales_usd_py_day",
        "structured_sections": {"purpose": "Prior-year APSD sales."},
        "lever": 1,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is True
    assert decision.reason == "applyable"


def test_instruction_section_patch_is_applyable() -> None:
    patch = {
        "proposal_id": "P001#1",
        "type": "update_instruction_section",
        "target": "QUERY RULES",
        "section_name": "QUERY RULES",
        "new_text": "- Always filter same-store APSD questions.",
        "lever": 5,
    }
    decision = check_patch_applyability(
        patch=patch,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert decision.applyable is True
    assert decision.reason == "applyable"


def test_filter_applyable_patches_splits_kept_and_dropped() -> None:
    patches = [
        {
            "proposal_id": "bad",
            "type": "update_column_description",
            "column": "apsd_sales_usd_py_day",
            "structured_sections": {"description": "missing table"},
            "lever": 1,
        },
        {
            "proposal_id": "good",
            "type": "update_instruction_section",
            "target": "QUERY RULES",
            "section_name": "QUERY RULES",
            "new_text": "- Rule",
            "lever": 5,
        },
    ]
    kept, decisions = filter_applyable_patches(
        patches=patches,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    assert [p["proposal_id"] for p in kept] == ["good"]
    assert [(d.proposal_id, d.applyable, d.reason) for d in decisions] == [
        ("bad", False, "missing_table"),
        ("good", True, "applyable"),
    ]
