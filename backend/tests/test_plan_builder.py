"""Tests for parallel plan generation helpers."""

from backend.services import plan_builder


METRIC_VIEW_ERROR = (
    "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] The usage of measure column "
    "[Net Sales] of a metric view requires a MEASURE() aggregate function"
)


def test_shared_context_marks_metric_views_and_includes_query_rules():
    shared = plan_builder._build_shared_context(
        tables_context=[
            {
                "table": "cat.sch.mv_sales",
                "table_type": "METRIC_VIEW",
                "columns": [{"name": "Product Category"}, {"name": "Net Sales"}],
            }
        ],
        inspection_summaries={},
        user_requirements="Answer sales questions.",
    )

    assert "**cat.sch.mv_sales** [METRIC_VIEW]" in shared
    assert "Wrap every metric-view measure column in MEASURE()" in shared
    assert "Do not join a metric view directly to another table" in shared


def test_example_sql_prompt_includes_metric_view_rules(monkeypatch):
    captured = {}

    def fake_call(prompt, *, max_tokens, section_name):
        captured["prompt"] = prompt
        return {"example_sqls": []}

    monkeypatch.setattr(plan_builder, "_call_llm_section", fake_call)

    plan_builder._gen_example_sqls("## Tables\n- **cat.sch.mv_sales** [METRIC_VIEW]")

    assert "METRIC VIEW SQL RULES" in captured["prompt"]
    assert "MEASURE(`Measure Name`)" in captured["prompt"]
    assert "query it in a CTE first" in captured["prompt"]


def test_gen_tables_splits_metric_views_out_of_tables(monkeypatch):
    def fake_call(prompt, *, max_tokens, section_name):
        assert "cat.sch.mv_sales" not in prompt
        return {
            "tables": [{
                "identifier": "cat.sch.sales",
                "description": "Sales facts.",
                "column_configs": [{"column_name": "sale_id"}],
            }]
        }

    monkeypatch.setattr(plan_builder, "_call_llm_section", fake_call)

    result = plan_builder._gen_tables(
        "shared context",
        [
            {
                "table": "cat.sch.sales",
                "table_type": "MANAGED",
                "columns": [{"name": "sale_id"}],
            },
            {
                "table": "cat.sch.mv_sales",
                "table_type": "METRIC_VIEW",
                "columns": [{"name": "Product Category"}, {"name": "Net Sales"}],
            },
        ],
    )

    assert [t["identifier"] for t in result["tables"]] == ["cat.sch.sales"]
    assert [mv["identifier"] for mv in result["metric_views"]] == ["cat.sch.mv_sales"]
    assert result["metric_views"][0]["column_configs"] == [
        {"column_name": "Product Category", "enable_format_assistance": True},
        {"column_name": "Net Sales", "enable_format_assistance": True},
    ]


def test_assemble_fallback_keeps_metric_views_separate_from_tables():
    plan = plan_builder._assemble(
        results={"tables": {}},
        tables_context=[
            {
                "table": "cat.sch.sales",
                "table_type": "MANAGED",
                "columns": [{"name": "sale_id"}],
            },
            {
                "table": "cat.sch.mv_sales",
                "table_type": "METRIC_VIEW",
                "columns": [{"name": "Net Sales"}],
            },
        ],
    )

    assert [t["identifier"] for t in plan["tables"]] == ["cat.sch.sales"]
    assert [mv["identifier"] for mv in plan["metric_views"]] == ["cat.sch.mv_sales"]


def test_validate_plan_sqls_repairs_metric_view_example_sql(monkeypatch):
    calls = []

    def fake_test_sql(sql, parameters=None):
        calls.append(sql)
        if "MEASURE(`Net Sales`)" in sql:
            return {"success": True, "row_count": 1}
        return {"success": False, "error": METRIC_VIEW_ERROR}

    def fake_call(prompt, *, max_tokens, section_name):
        assert section_name == "metric view SQL repair"
        return {
            "sql": (
                "SELECT `Product Category`, MEASURE(`Net Sales`) AS net_sales "
                "FROM cat.sch.mv_sales GROUP BY ALL"
            )
        }

    monkeypatch.setattr(plan_builder, "_test_sql", fake_test_sql)
    monkeypatch.setattr(plan_builder, "_call_llm_section", fake_call)

    plan = {
        "example_sqls": [{
            "question": "What is net sales by product category?",
            "sql": "SELECT `Product Category`, `Net Sales` FROM cat.sch.mv_sales GROUP BY ALL",
            "parameters": [],
        }]
    }

    warnings = plan_builder._validate_plan_sqls(plan, shared_context="metric view context")

    assert "MEASURE(`Net Sales`)" in plan["example_sqls"][0]["sql"]
    assert any("added MEASURE() for metric view measures" in w for w in warnings)
    assert len(calls) == 2


def test_validate_plan_sqls_repairs_metric_view_benchmark(monkeypatch):
    def fake_test_sql(sql, parameters=None):
        if "MEASURE(`Net Sales`)" in sql:
            return {"success": True, "row_count": 1}
        return {"success": False, "error": METRIC_VIEW_ERROR}

    def fake_call(prompt, *, max_tokens, section_name):
        return {
            "expected_sql": (
                "SELECT `Channel`, MEASURE(`Net Sales`) AS net_sales "
                "FROM cat.sch.mv_sales GROUP BY ALL"
            )
        }

    monkeypatch.setattr(plan_builder, "_test_sql", fake_test_sql)
    monkeypatch.setattr(plan_builder, "_call_llm_section", fake_call)

    plan = {
        "benchmarks": [{
            "question": "What is net sales by channel?",
            "expected_sql": "SELECT `Channel`, `Net Sales` FROM cat.sch.mv_sales GROUP BY ALL",
        }]
    }

    warnings = plan_builder._validate_plan_sqls(plan, shared_context="metric view context")

    assert "MEASURE(`Net Sales`)" in plan["benchmarks"][0]["expected_sql"]
    assert any("Repaired benchmark #1" in w for w in warnings)


def test_validate_plan_sqls_drops_metric_view_sql_when_repair_still_fails(monkeypatch):
    def fake_test_sql(sql, parameters=None):
        return {"success": False, "error": METRIC_VIEW_ERROR if "MEASURE" not in sql else "still invalid"}

    def fake_call(prompt, *, max_tokens, section_name):
        return {
            "sql": (
                "SELECT `Product Category`, MEASURE(`Net Sales`) AS net_sales "
                "FROM cat.sch.mv_sales GROUP BY ALL"
            )
        }

    monkeypatch.setattr(plan_builder, "_test_sql", fake_test_sql)
    monkeypatch.setattr(plan_builder, "_call_llm_section", fake_call)

    plan = {
        "example_sqls": [{
            "question": "What is net sales by product category?",
            "sql": "SELECT `Product Category`, `Net Sales` FROM cat.sch.mv_sales GROUP BY ALL",
            "parameters": [],
        }]
    }

    warnings = plan_builder._validate_plan_sqls(plan, shared_context="metric view context")

    assert plan["example_sqls"] == []
    assert any("Dropped example_sql #1" in w for w in warnings)
