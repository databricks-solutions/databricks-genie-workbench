from __future__ import annotations

from genie_space_optimizer.optimization.patch_applyability import (
    filter_applyable_patches,
)
from genie_space_optimizer.optimization.patch_selection import (
    select_target_aware_causal_patch_cap,
)


def _snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.mv_7now_fact_sales",
                    "name": "mv_7now_fact_sales",
                    "column_configs": [{"column_name": "time_window", "description": []}],
                },
                {
                    "identifier": "main.sales.mv_7now_store_sales",
                    "name": "mv_7now_store_sales",
                    "column_configs": [{"column_name": "time_window", "description": []}],
                },
                {
                    "identifier": "main.sales.mv_esr_store_sales",
                    "name": "mv_esr_store_sales",
                    "column_configs": [
                        {"column_name": "apsd_sales_usd_py_day", "description": []},
                        {"column_name": "apsd_sales_usd_day", "description": []},
                        {"column_name": "is_finance_monthly_same_store", "description": []},
                    ],
                },
            ]
        },
        "instructions": {"text_instructions": [{"content": "PURPOSE:\n- test"}]},
    }


def test_7now_malformed_l1_bundle_cannot_displace_applyable_filter() -> None:
    patches = [
        {
            "proposal_id": "P005#1",
            "type": "update_column_description",
            "table": "main.sales.mv_7now_store_sales",
            "column": [],
            "structured_sections": {"description": "bad"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_013",
            "target_qids": ["gs_013"],
            "root_cause": "wrong_aggregation",
        },
        {
            "proposal_id": "P006#1",
            "type": "update_column_description",
            "table": "main.sales.mv_7now_store_sales",
            "column": ["zone_combination", "7now_avg_txn_diff_day"],
            "structured_sections": {"description": "bad"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_013",
            "target_qids": ["gs_013"],
            "root_cause": "wrong_aggregation",
        },
        {
            "proposal_id": "P013#1",
            "type": "update_instruction_section",
            "target": "QUERY RULES",
            "section_name": "QUERY RULES",
            "new_text": "- Use mv_7now_fact_sales.time_window = 'mtd'.",
            "lever": 5,
            "relevance_score": 0.8,
            "target_qids": ["gs_021"],
            "root_cause": "missing_filter",
        },
    ]
    applyable, decisions = filter_applyable_patches(
        patches=patches,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    selected, _cap_decisions = select_target_aware_causal_patch_cap(
        applyable,
        target_qids=("gs_013", "gs_021"),
        max_patches=3,
    )
    assert {d.reason for d in decisions if not d.applyable} == {
        "invalid_column_target"
    }
    assert [p["proposal_id"] for p in selected] == ["P013#1"]


def test_esr_missing_table_l1_bundle_cannot_displace_applyable_instruction() -> None:
    patches = [
        {
            "proposal_id": "P003#1",
            "type": "update_column_description",
            "column": "apsd_sales_usd_py_day",
            "structured_sections": {"description": "missing table"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_002",
            "target_qids": ["gs_002"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P004#1",
            "type": "update_column_description",
            "column": "apsd_sales_usd_day",
            "structured_sections": {"description": "missing table"},
            "lever": 1,
            "relevance_score": 1.0,
            "rca_id": "rca_gs_002",
            "target_qids": ["gs_002"],
            "root_cause": "missing_filter",
        },
        {
            "proposal_id": "P023#1",
            "type": "update_instruction_section",
            "target": "QUERY RULES",
            "section_name": "QUERY RULES",
            "new_text": "- APSD KPI questions must filter is_finance_monthly_same_store = 'Y'.",
            "lever": 5,
            "relevance_score": 0.9,
            "target_qids": ["gs_002"],
            "root_cause": "missing_filter",
        },
    ]
    applyable, decisions = filter_applyable_patches(
        patches=patches,
        metadata_snapshot=_snapshot(),
        space_id="space_1",
    )
    selected, _cap_decisions = select_target_aware_causal_patch_cap(
        applyable,
        target_qids=("gs_002",),
        max_patches=3,
    )
    assert {d.reason for d in decisions if not d.applyable} == {"missing_table"}
    assert [p["proposal_id"] for p in selected] == ["P023#1"]
