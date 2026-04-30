from __future__ import annotations


def test_strip_trailing_statement_semicolon_before_sample_wrap() -> None:
    from genie_space_optimizer.optimization.evaluation import (
        _strip_trailing_statement_semicolon,
    )

    sql = "SELECT * FROM cat.sch.table ORDER BY id;\n"

    assert _strip_trailing_statement_semicolon(sql) == (
        "SELECT * FROM cat.sch.table ORDER BY id"
    )


def test_same_store_filter_alignment_rejects_unmentioned_filter() -> None:
    from genie_space_optimizer.optimization.benchmarks import (
        deterministic_question_sql_alignment_issues,
    )

    issues = deterministic_question_sql_alignment_issues(
        {
            "question": "Show country-level performance: total sales and store count.",
            "expected_sql": (
                "SELECT country_code, SUM(total_sales_usd_day) "
                "FROM mv_esr_store_sales "
                "WHERE is_finance_monthly_same_store = 'Y' "
                "GROUP BY country_code"
            ),
        }
    )

    assert issues == [
        "EXTRA_FILTER: SQL filters on is_finance_monthly_same_store but the question does not ask for same-store or finance-monthly same-store results."
    ]


def test_same_store_filter_alignment_allows_mentioned_filter() -> None:
    from genie_space_optimizer.optimization.benchmarks import (
        deterministic_question_sql_alignment_issues,
    )

    issues = deterministic_question_sql_alignment_issues(
        {
            "question": "Show same-store country-level APSD sales.",
            "expected_sql": (
                "SELECT country_code, MEASURE(apsd_sales_usd_day) "
                "FROM mv_esr_store_sales "
                "WHERE is_finance_monthly_same_store = 'Y' "
                "GROUP BY country_code"
            ),
        }
    )

    assert issues == []
