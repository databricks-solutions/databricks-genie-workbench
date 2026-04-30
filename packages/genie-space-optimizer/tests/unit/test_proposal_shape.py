from __future__ import annotations

from genie_space_optimizer.optimization.proposal_shape import (
    normalize_column_proposals,
)


def _uc_columns() -> list[dict]:
    return [
        {
            "table_full_name": "main.sales.mv_esr_store_sales",
            "table_name": "mv_esr_store_sales",
            "column_name": "apsd_sales_usd_py_day",
        },
        {
            "table_full_name": "main.sales.mv_esr_store_sales",
            "table_name": "mv_esr_store_sales",
            "column_name": "apsd_sales_usd_day",
        },
        {
            "table_full_name": "main.sales.mv_7now_fact_sales",
            "table_name": "mv_7now_fact_sales",
            "column_name": "time_window",
        },
        {
            "table_full_name": "main.sales.mv_7now_store_sales",
            "table_name": "mv_7now_store_sales",
            "column_name": "time_window",
        },
    ]


def _proposal(**overrides: object) -> dict:
    base = {
        "id": "P001",
        "proposal_id": "P001",
        "patch_type": "update_column_description",
        "type": "update_column_description",
        "lever": 1,
        "rca_id": "rca_q1_measure_swap",
        "target_qids": ["q1"],
        "column_description": ["description"],
    }
    base.update(overrides)
    return base


def test_empty_column_is_dropped_with_reason() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column=[])],
        uc_columns=_uc_columns(),
    )
    assert out == []
    assert decisions[0]["decision"] == "dropped"
    assert decisions[0]["reason"] == "missing_column"


def test_multi_column_list_fans_out_into_single_column_proposals() -> None:
    out, decisions = normalize_column_proposals(
        [
            _proposal(
                column=["apsd_sales_usd_py_day", "apsd_sales_usd_day"],
                table="main.sales.mv_esr_store_sales",
            )
        ],
        uc_columns=_uc_columns(),
    )
    assert [p["column"] for p in out] == [
        "apsd_sales_usd_py_day",
        "apsd_sales_usd_day",
    ]
    assert [p["table"] for p in out] == [
        "main.sales.mv_esr_store_sales",
        "main.sales.mv_esr_store_sales",
    ]
    assert [p["proposal_id"] for p in out] == ["P001#col1", "P001#col2"]
    assert decisions[0]["decision"] == "expanded"
    assert decisions[0]["reason"] == "multi_column_fanout"


def test_qualified_column_splits_table_and_column() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column="mv_7now_fact_sales.time_window")],
        uc_columns=_uc_columns(),
    )
    assert len(out) == 1
    assert out[0]["table"] == "main.sales.mv_7now_fact_sales"
    assert out[0]["column"] == "time_window"
    assert decisions[0]["decision"] == "normalized"
    assert decisions[0]["reason"] == "qualified_column_split"


def test_missing_table_is_inferred_when_unique_column_match_exists() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column="apsd_sales_usd_py_day")],
        uc_columns=_uc_columns(),
    )
    assert len(out) == 1
    assert out[0]["table"] == "main.sales.mv_esr_store_sales"
    assert out[0]["column"] == "apsd_sales_usd_py_day"
    assert decisions[0]["decision"] == "normalized"
    assert decisions[0]["reason"] == "inferred_table_from_uc_columns"


def test_missing_table_is_dropped_when_column_match_is_ambiguous() -> None:
    out, decisions = normalize_column_proposals(
        [_proposal(column="time_window")],
        uc_columns=_uc_columns(),
    )
    assert out == []
    assert decisions[0]["decision"] == "dropped"
    assert decisions[0]["reason"] == "ambiguous_table_for_column"


def test_non_column_proposal_passes_through() -> None:
    proposal = {
        "proposal_id": "P010",
        "type": "add_instruction",
        "lever": 5,
        "proposed_value": "Add rule",
    }
    out, decisions = normalize_column_proposals(
        [proposal],
        uc_columns=_uc_columns(),
    )
    assert out == [proposal]
    assert decisions == []
