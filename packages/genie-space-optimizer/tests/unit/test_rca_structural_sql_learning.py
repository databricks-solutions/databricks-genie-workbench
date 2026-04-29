from __future__ import annotations

from genie_space_optimizer.optimization.rca import (
    RcaKind,
    build_rca_ledger,
    extract_rca_findings_from_row,
)


def test_ground_truth_correct_function_gap_becomes_function_rca():
    row = {
        "question_id": "q_fn",
        "arbiter/value": "ground_truth_correct",
        "inputs.expected_sql": (
            "SELECT prashanth_subrahmanyam_catalog.sales_reports."
            "fn_mtd_or_mtday(MEASURE(`_7now_py_sales_mtd`))"
        ),
        "outputs.predictions.sql": (
            "SELECT CASE WHEN day(now()) = 1 "
            "THEN MEASURE(`_7now_py_sales_day`) "
            "ELSE MEASURE(`_7now_py_sales_mtd`) END"
        ),
        "asset_routing/metadata": {
            "failure_type": "asset_routing_error",
            "blame_set": ["fn_mtd_or_mtday"],
            "counterfactual_fix": "Use fn_mtd_or_mtday instead of inlining CASE logic.",
        },
    }

    findings = extract_rca_findings_from_row(row)

    assert any(f.rca_kind is RcaKind.FUNCTION_OR_TVF_NOT_INVOKED for f in findings)
    fn_findings = [f for f in findings if f.rca_kind is RcaKind.FUNCTION_OR_TVF_NOT_INVOKED]
    assert "fn_mtd_or_mtday" in " ".join(fn_findings[0].expected_objects)
    assert 6 in fn_findings[0].recommended_levers


def test_ground_truth_correct_filter_gap_becomes_filter_logic_rca():
    row = {
        "question_id": "q_filter",
        "arbiter/value": "ground_truth_correct",
        "expected_sql": (
            "SELECT SUM(amount) FROM cat.sch.orders "
            "WHERE order_date >= DATE_TRUNC('MONTH', CURRENT_DATE())"
        ),
        "generated_sql": "SELECT SUM(amount) FROM cat.sch.orders",
    }

    findings = extract_rca_findings_from_row(row)

    assert any(f.rca_kind is RcaKind.FILTER_LOGIC_MISMATCH for f in findings)


def test_sql_expression_rca_theme_requests_lever6_snippet():
    row = {
        "question_id": "q_expr",
        "arbiter/value": "ground_truth_correct",
        "expected_sql": "SELECT DATE_TRUNC('quarter', order_date) FROM cat.sch.orders",
        "generated_sql": "SELECT MONTH(order_date) FROM cat.sch.orders",
    }

    ledger = build_rca_ledger([row])

    themes = ledger["themes"]
    assert any(t.rca_kind is RcaKind.SQL_EXPRESSION_MISSING for t in themes)
    snippet_patches = [
        p
        for theme in themes
        for p in theme.patches
        if p.get("type") == "add_sql_snippet_expression"
    ]
    assert snippet_patches
    assert all(p.get("source") == "rca_failed_question_sql" for p in snippet_patches)
