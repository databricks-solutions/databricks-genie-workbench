"""
End-to-end test: exercise every serialized_space schema feature through
generate_config → validate_config → clean_config → enforce_constraints.

Also validates the canonical reference JSON directly.
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.services.create_agent_tools import _generate_config, _validate_config
from backend.genie_creator import _clean_config, _enforce_constraints


# ---------------------------------------------------------------------------
# 1. Build a full-schema config via _generate_config (the LLM path)
# ---------------------------------------------------------------------------

TABLES = [
    {
        "identifier": "catalog.schema.orders",
        "description": "Daily sales transactions with line-item details",
        "column_configs": [
            {"column_name": "order_id", "description": "Unique order identifier"},
            {"column_name": "customer_id", "description": "Foreign key to customers table", "synonyms": ["cust_id", "account_id"]},
            {"column_name": "order_date", "description": "Date the order was placed"},
            {"column_name": "region", "description": "Sales region: AMER, EMEA, APJ, LATAM", "synonyms": ["area", "territory"]},
            {"column_name": "order_amount", "description": "Total order value in USD"},
            {"column_name": "status", "description": "Order status: Active, Pending, Cancelled"},
            {"column_name": "etl_timestamp", "exclude": True},
        ],
    },
    {
        "identifier": "catalog.schema.customers",
        "description": "Customer master data including contact information",
        "column_configs": [
            {"column_name": "customer_id", "description": "Unique customer identifier"},
            {"column_name": "customer_name", "description": "Full name of the customer"},
            {"column_name": "email", "description": "Contact email address", "enable_matching": False},
            {"column_name": "signup_date", "description": "Date the customer registered"},
        ],
    },
    {
        "identifier": "catalog.schema.products",
        "description": "Product catalog with pricing information",
        "column_configs": [
            {"column_name": "product_id", "description": "Unique product identifier"},
            {"column_name": "product_name", "description": "Display name of the product"},
            {"column_name": "category", "description": "Product category", "synonyms": ["type", "product type"]},
            {"column_name": "unit_price", "description": "Price per unit in USD"},
        ],
    },
]

METRIC_VIEWS = [
    {
        "identifier": "catalog.schema.revenue_metrics",
        "description": "Pre-aggregated revenue metrics by region and time period",
        "column_configs": [
            {"column_name": "period", "description": "Time period (monthly, quarterly, yearly)", "enable_format_assistance": True},
            {"column_name": "region", "description": "Sales region"},
        ],
    },
]

SAMPLE_QUESTIONS = [
    "What were total sales last month?",
    "Show top 10 customers by revenue",
    "Which product category has the highest sales?",
    "How many orders were placed this year?",
    "What is the average order value by region?",
]

TEXT_INSTRUCTIONS = [
    "'revenue' means net revenue (after returns and discounts), NOT gross revenue.",
    "When no time range is specified, default to the current calendar year.",
    "Fiscal quarters: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec.",
    "The `status` column has inconsistent casing — ALWAYS use LOWER(status) when filtering.",
    "'active customer' = customer with at least 1 order in the last 90 days.",
]

EXAMPLE_SQLS = [
    {
        "question": "Show top 10 customers by revenue",
        "sql": "SELECT c.customer_name, SUM(o.order_amount) as total_revenue\nFROM catalog.schema.orders o\nJOIN catalog.schema.customers c ON o.customer_id = c.customer_id\nGROUP BY c.customer_name\nORDER BY total_revenue DESC\nLIMIT 10",
        "usage_guidance": "Use this pattern for any top-N ranking question by a numeric metric",
    },
    {
        "question": "What were total sales last month",
        "sql": "SELECT SUM(order_amount) as total_sales\nFROM catalog.schema.orders\nWHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH)\nAND order_date < DATE_TRUNC('month', CURRENT_DATE)",
        "usage_guidance": "Use for questions about the previous calendar month",
    },
    {
        "question": "Show sales for North America",
        "sql": "SELECT SUM(order_amount) as total_sales\nFROM catalog.schema.orders\nWHERE region = :region_name",
        "parameters": [
            {
                "name": "region_name",
                "type_hint": "STRING",
                "description": "The region to filter by. Values: AMER, EMEA, APJ, LATAM",
                "default_value": "AMER",
            },
        ],
        "usage_guidance": "Use when user asks about sales filtered by a specific geographic region",
    },
    {
        "question": "Show orders above $1000 for a specific category",
        "sql": "SELECT o.order_id, o.order_amount, p.product_name, p.category\nFROM catalog.schema.orders o\nJOIN catalog.schema.products p ON o.product_id = p.product_id\nWHERE o.order_amount > :min_amount\nAND p.category = :category",
        "parameters": [
            {
                "name": "category",
                "type_hint": "STRING",
                "description": "Product category. Values: Electronics, Clothing, Food",
                "default_value": "Electronics",
            },
            {
                "name": "min_amount",
                "type_hint": "NUMBER",
                "description": "Minimum order amount threshold",
                "default_value": "1000",
            },
        ],
        "usage_guidance": "Use for filtered order queries with both amount threshold and category",
    },
    {
        "question": "What is the month-over-month growth in orders?",
        "sql": "SELECT DATE_TRUNC('month', order_date) as month,\nCOUNT(*) as order_count,\nLAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', order_date)) as prev_month,\nROUND((COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', order_date))) * 100.0 / NULLIF(LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', order_date)), 0), 2) as growth_pct\nFROM catalog.schema.orders\nGROUP BY DATE_TRUNC('month', order_date)\nORDER BY month",
        "usage_guidance": "Use for month-over-month trend and growth analysis",
    },
]

MEASURES = [
    {
        "alias": "total_revenue",
        "sql": "SUM(orders.order_amount)",
        "display_name": "Total Revenue",
        "synonyms": ["revenue", "total sales", "sales"],
        "instruction": "Use for any revenue aggregation",
        "comment": "Revenue includes all non-cancelled order line items",
    },
    {
        "alias": "order_count",
        "sql": "COUNT(DISTINCT orders.order_id)",
        "display_name": "Order Count",
        "synonyms": ["number of orders", "total orders"],
        "instruction": "Use for counting distinct orders",
    },
    {
        "alias": "avg_order_value",
        "sql": "AVG(orders.order_amount)",
        "display_name": "Average Order Value",
        "synonyms": ["AOV", "average order", "mean order value"],
        "instruction": "Use when asked about average order size or value",
    },
]

FILTERS = [
    {
        "display_name": "high value orders",
        "sql": "orders.order_amount > 1000",
        "synonyms": ["large orders", "big purchases"],
        "instruction": "Apply when users ask about high-value or large orders",
        "comment": "Threshold aligned with finance team's definition",
    },
    {
        "display_name": "current year",
        "sql": "YEAR(orders.order_date) = YEAR(CURRENT_DATE())",
        "synonyms": ["this year", "YTD"],
        "instruction": "Apply when user asks about current year or YTD data",
    },
    {
        "display_name": "active orders",
        "sql": "LOWER(orders.status) = 'active'",
        "synonyms": ["live orders", "open orders"],
        "instruction": "Apply when user asks about active or current orders",
        "comment": "Uses LOWER() due to inconsistent casing in status column",
    },
]

EXPRESSIONS = [
    {
        "alias": "order_year",
        "sql": "YEAR(orders.order_date)",
        "display_name": "Order Year",
        "synonyms": ["year", "fiscal year"],
        "instruction": "Use for any year-based grouping of orders",
        "comment": "Standard date dimension for annual reporting",
    },
    {
        "alias": "order_month",
        "sql": "DATE_TRUNC('month', orders.order_date)",
        "display_name": "Order Month",
        "synonyms": ["month", "monthly"],
        "instruction": "Use for monthly trend analysis",
    },
    {
        "alias": "order_value_tier",
        "sql": "CASE WHEN orders.order_amount > 5000 THEN 'Premium' WHEN orders.order_amount > 1000 THEN 'Standard' ELSE 'Basic' END",
        "display_name": "Order Value Tier",
        "synonyms": ["value tier", "order tier", "price tier"],
        "instruction": "Use when grouping orders by value tier",
    },
]

JOIN_SPECS = [
    {
        "left_table": "catalog.schema.orders",
        "left_alias": "orders",
        "right_table": "catalog.schema.customers",
        "right_alias": "customers",
        "left_column": "customer_id",
        "right_column": "customer_id",
        "relationship": "MANY_TO_ONE",
        "instruction": "Use when customer demographics are needed for order analysis",
        "comment": "Join orders to customers on customer_id",
    },
    {
        "left_table": "catalog.schema.orders",
        "left_alias": "orders",
        "right_table": "catalog.schema.products",
        "right_alias": "products",
        "left_column": "product_id",
        "right_column": "product_id",
        "relationship": "MANY_TO_ONE",
        "instruction": "Use when product details are needed for order analysis",
        "comment": "Join orders to products on product_id",
    },
]

BENCHMARKS = [
    {"question": "What is the average order value?", "expected_sql": "SELECT AVG(order_amount) as avg_order_value FROM catalog.schema.orders"},
    {"question": "How many customers placed orders this year?", "expected_sql": "SELECT COUNT(DISTINCT customer_id) FROM catalog.schema.orders WHERE YEAR(order_date) = YEAR(CURRENT_DATE())"},
    {"question": "What is total revenue by region?", "expected_sql": "SELECT region, SUM(order_amount) as total_revenue FROM catalog.schema.orders GROUP BY region ORDER BY total_revenue DESC"},
    {"question": "Show the top 5 product categories by sales", "expected_sql": "SELECT p.category, SUM(o.order_amount) as total_sales FROM catalog.schema.orders o JOIN catalog.schema.products p ON o.product_id = p.product_id GROUP BY p.category ORDER BY total_sales DESC LIMIT 5"},
    {"question": "What percentage of orders are high-value (over $1000)?", "expected_sql": "SELECT ROUND(COUNT(CASE WHEN order_amount > 1000 THEN 1 END) * 100.0 / COUNT(*), 2) as high_value_pct FROM catalog.schema.orders"},
]


# ---------------------------------------------------------------------------
# 2. The canonical reference JSON from the linked schema
# ---------------------------------------------------------------------------

CANONICAL_JSON = {
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": "a1b2c3d4e5f60000000000000000000a", "question": ["What were total sales last month?"]},
        ]
    },
    "data_sources": {
        "tables": [
            {
                "identifier": "catalog.schema.orders",
                "description": ["Daily sales transactions with line-item details"],
                "column_configs": [
                    {
                        "column_name": "etl_timestamp",
                        "exclude": True,
                        "enable_entity_matching": False,
                        "enable_format_assistance": False,
                    },
                    {
                        "column_name": "region",
                        "description": ["Sales region code: AMER, EMEA, APJ, LATAM"],
                        "synonyms": ["area", "territory", "sales region"],
                        "exclude": False,
                        "enable_entity_matching": True,
                        "enable_format_assistance": True,
                    },
                ],
            },
            {
                "identifier": "catalog.schema.products",
                "column_configs": [
                    {"column_name": "category", "enable_entity_matching": True, "enable_format_assistance": True},
                ],
            },
        ],
        "metric_views": [
            {"identifier": "catalog.schema.revenue_metrics", "description": ["Revenue metrics"]},
        ],
    },
    "instructions": {
        "text_instructions": [
            {
                "id": "b2c3d4e5f6a70000000000000000000b",
                "content": ["Revenue = quantity * unit_price.\n", "Fiscal year starts April 1st."],
            }
        ],
        "example_question_sqls": [
            {
                "id": "c3d4e5f6a7b80000000000000000000c",
                "question": ["Show top 10 customers by revenue"],
                "sql": [
                    "SELECT\n", "  customer_name,\n", "  SUM(amount) as total\n",
                    "FROM catalog.schema.orders\n", "GROUP BY customer_name\n",
                    "ORDER BY total DESC\n", "LIMIT 10",
                ],
                "usage_guidance": ["Use this pattern for any top-N ranking question by a numeric metric"],
            }
        ],
        "sql_functions": [
            {
                "id": "d4e5f6a7b8c90000000000000000000d",
                "identifier": "catalog.schema.fiscal_quarter",
                "description": "Calculates the fiscal quarter from a date (fiscal year starts April 1)",
            }
        ],
        "join_specs": [
            {
                "id": "e5f6a7b8c9d00000000000000000000e",
                "left": {"identifier": "catalog.schema.orders", "alias": "orders"},
                "right": {"identifier": "catalog.schema.customers", "alias": "customers"},
                "sql": [
                    "`orders`.`customer_id` = `customers`.`customer_id`",
                    "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
                ],
                "comment": ["Join orders to customers on customer_id"],
                "instruction": ["Use this join when relating orders to customer demographics"],
            }
        ],
        "sql_snippets": {
            "filters": [
                {
                    "id": "f6a7b8c9d0e10000000000000000000f",
                    "display_name": "high value",
                    "sql": ["orders.amount > 1000"],
                    "synonyms": ["big deal", "large order"],
                    "instruction": ["Apply when users ask about high-value or large orders"],
                    "comment": ["Threshold aligned with finance team's definition"],
                }
            ],
            "expressions": [
                {
                    "id": "a7b8c9d0e1f20000000000000000000a",
                    "alias": "order_year",
                    "display_name": "Order Year",
                    "sql": ["YEAR(orders.order_date)"],
                    "synonyms": ["year"],
                    "instruction": ["Use for any year-based grouping of orders"],
                    "comment": ["Standard date dimension for annual reporting"],
                }
            ],
            "measures": [
                {
                    "id": "b8c9d0e1f2a30000000000000000000b",
                    "alias": "total_revenue",
                    "display_name": "Total Revenue",
                    "sql": ["SUM(orders.quantity * orders.unit_price)"],
                    "synonyms": ["revenue", "sales", "total sales"],
                    "instruction": ["Use for any revenue aggregation"],
                    "comment": ["Revenue includes all non-cancelled order line items"],
                }
            ],
        },
    },
    "benchmarks": {
        "questions": [
            {
                "id": "c9d0e1f2a3b40000000000000000000c",
                "question": ["What is average order value?"],
                "answer": [{"format": "SQL", "content": ["SELECT AVG(amount) FROM catalog.schema.orders"]}],
            }
        ]
    },
}


def _pp(label: str, ok: bool, detail: str = ""):
    sym = "PASS" if ok else "FAIL"
    msg = f"  [{sym}] {label}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    return ok


def run_tests():
    all_passed = True
    print("=" * 70)
    print("TEST 1: _generate_config with ALL schema features")
    print("=" * 70)

    result = _generate_config(
        tables=TABLES,
        sample_questions=SAMPLE_QUESTIONS,
        text_instructions=TEXT_INSTRUCTIONS,
        example_sqls=EXAMPLE_SQLS,
        measures=MEASURES,
        filters=FILTERS,
        expressions=EXPRESSIONS,
        join_specs=JOIN_SPECS,
        benchmarks=BENCHMARKS,
        metric_views=METRIC_VIEWS,
    )

    if "error" in result:
        print(f"  [FAIL] generate_config returned error: {result['error']}")
        return False

    config = result["config"]
    summary = result["summary"]

    all_passed &= _pp("Config has version 2", config.get("version") == 2)
    all_passed &= _pp(f"Tables: {summary['tables']}", summary["tables"] == 3)
    all_passed &= _pp(f"Sample questions: {summary['sample_questions']}", summary["sample_questions"] == 5)
    all_passed &= _pp(f"Example SQLs: {summary['example_sqls']}", summary["example_sqls"] == 5)
    all_passed &= _pp(f"Measures: {summary['measures']}", summary["measures"] == 3)
    all_passed &= _pp(f"Filters: {summary['filters']}", summary["filters"] == 3)
    all_passed &= _pp(f"Expressions: {summary['expressions']}", summary["expressions"] == 3)
    all_passed &= _pp(f"Join specs: {summary['join_specs']}", summary["join_specs"] == 2)
    all_passed &= _pp(f"Benchmarks: {summary['benchmarks']}", summary["benchmarks"] == 5)
    all_passed &= _pp(f"Text instructions: {summary['text_instructions']}", summary["text_instructions"] == 5)

    # Check metric views
    mvs = config.get("data_sources", {}).get("metric_views", [])
    all_passed &= _pp(f"Metric views: {len(mvs)}", len(mvs) == 1)
    if mvs:
        all_passed &= _pp("Metric view has column_configs", "column_configs" in mvs[0])

    # Check join_specs have --rt= annotation
    jss = config.get("instructions", {}).get("join_specs", [])
    for i, js in enumerate(jss):
        has_rt = any(s.startswith("--rt=") for s in js.get("sql", []))
        all_passed &= _pp(f"join_spec[{i}] has --rt= annotation", has_rt)
        has_backticks = any("`" in s for s in js.get("sql", []) if not s.startswith("--rt="))
        all_passed &= _pp(f"join_spec[{i}] uses backtick-quoted refs", has_backticks)

    # Check text_instructions have \n suffix
    ti = config.get("instructions", {}).get("text_instructions", [])
    if ti:
        for j, line in enumerate(ti[0].get("content", [])):
            has_nl = line.endswith("\n")
            all_passed &= _pp(f"text_instruction content[{j}] ends with \\n", has_nl)

    # Check example_sqls parameters are present
    eqs = config.get("instructions", {}).get("example_question_sqls", [])
    param_count = sum(1 for eq in eqs if eq.get("parameters"))
    all_passed &= _pp(f"Example SQLs with parameters: {param_count}", param_count == 2)

    # Check usage_guidance is present
    ug_count = sum(1 for eq in eqs if eq.get("usage_guidance"))
    all_passed &= _pp(f"Example SQLs with usage_guidance: {ug_count}", ug_count == 5)

    # Check default_value wrapping
    for eq in eqs:
        for p in eq.get("parameters", []):
            dv = p.get("default_value", {})
            has_values = isinstance(dv, dict) and "values" in dv
            all_passed &= _pp(f"Param '{p.get('name')}' default_value has values array", has_values)

    # Check excluded column has matching=false
    orders_tbl = next(t for t in config["data_sources"]["tables"] if "orders" in t["identifier"])
    etl_col = next((c for c in orders_tbl.get("column_configs", []) if c["column_name"] == "etl_timestamp"), None)
    if etl_col:
        all_passed &= _pp("Excluded column has exclude=true", etl_col.get("exclude") is True)
        all_passed &= _pp("Excluded column has entity_matching=false", etl_col.get("enable_entity_matching") is False)
        all_passed &= _pp("Excluded column has format_assistance=false", etl_col.get("enable_format_assistance") is False)

    # Check non-excluded column has matching=true (default)
    region_col = next((c for c in orders_tbl.get("column_configs", []) if c["column_name"] == "region"), None)
    if region_col:
        all_passed &= _pp("Non-excluded column has entity_matching=true", region_col.get("enable_entity_matching") is True)
        all_passed &= _pp("Non-excluded column has format_assistance=true", region_col.get("enable_format_assistance") is True)

    # Check benchmark answer format
    bench_qs = config.get("benchmarks", {}).get("questions", [])
    for i, bq in enumerate(bench_qs):
        answers = bq.get("answer", [])
        all_passed &= _pp(f"benchmark[{i}] has exactly 1 answer", len(answers) == 1)
        if answers:
            all_passed &= _pp(f"benchmark[{i}] answer format is SQL", answers[0].get("format") == "SQL")

    # ── Validate the generated config ──
    print()
    print("-" * 70)
    print("TEST 2: _validate_config on generated config")
    print("-" * 70)

    val_result = _validate_config(config=config)
    errors = val_result.get("errors", [])
    warnings = val_result.get("warnings", [])

    all_passed &= _pp(f"Validation errors: {len(errors)}", len(errors) == 0,
                       "; ".join(f"{e['path']}: {e['message']}" for e in errors) if errors else "")
    _pp(f"Validation warnings: {len(warnings)}", True,
        "; ".join(f"{w['path']}: {w['message']}" for w in warnings) if warnings else "")

    # ── Clean + enforce constraints ──
    print()
    print("-" * 70)
    print("TEST 3: _clean_config + _enforce_constraints pipeline")
    print("-" * 70)

    try:
        constrained = _enforce_constraints(config)
        cleaned = _clean_config(constrained)
        serialized = json.dumps(cleaned)
        all_passed &= _pp("enforce_constraints succeeded", True)
        all_passed &= _pp("clean_config succeeded", True)
        all_passed &= _pp(f"Serialized size: {len(serialized):,} bytes", len(serialized) < 3_500_000)

        # Verify cleaned config is valid JSON
        reparsed = json.loads(serialized)
        all_passed &= _pp("Serialized JSON is parseable", reparsed is not None)

        # Verify all arrays are still sorted after cleaning
        tables_sorted = all(
            reparsed["data_sources"]["tables"][i]["identifier"] <= reparsed["data_sources"]["tables"][i + 1]["identifier"]
            for i in range(len(reparsed["data_sources"]["tables"]) - 1)
        )
        all_passed &= _pp("Tables sorted after cleaning", tables_sorted)
    except Exception as e:
        all_passed &= _pp(f"Pipeline failed: {e}", False)

    # ── Validate the canonical JSON directly ──
    print()
    print("-" * 70)
    print("TEST 4: _validate_config on CANONICAL reference JSON")
    print("-" * 70)

    val_canonical = _validate_config(config=CANONICAL_JSON)
    can_errors = val_canonical.get("errors", [])
    can_warnings = val_canonical.get("warnings", [])

    all_passed &= _pp(f"Canonical validation errors: {len(can_errors)}", len(can_errors) == 0,
                       "; ".join(f"{e['path']}: {e['message']}" for e in can_errors) if can_errors else "")
    _pp(f"Canonical validation warnings: {len(can_warnings)}", True,
        "; ".join(f"{w['path']}: {w['message']}" for w in can_warnings) if can_warnings else "")

    # ── Clean + enforce on canonical ──
    print()
    print("-" * 70)
    print("TEST 5: _clean_config + _enforce_constraints on CANONICAL JSON")
    print("-" * 70)

    try:
        can_constrained = _enforce_constraints(CANONICAL_JSON)
        can_cleaned = _clean_config(can_constrained)
        can_serialized = json.dumps(can_cleaned)
        all_passed &= _pp("Canonical enforce_constraints succeeded", True)
        all_passed &= _pp("Canonical clean_config succeeded", True)
        all_passed &= _pp(f"Canonical serialized size: {len(can_serialized):,} bytes", True)
    except Exception as e:
        all_passed &= _pp(f"Canonical pipeline failed: {e}", False)

    # ── Summary ──
    print()
    print("=" * 70)
    verdict = "ALL TESTS PASSED" if all_passed else "SOME TESTS FAILED"
    print(f"RESULT: {verdict}")
    print("=" * 70)

    return all_passed


if __name__ == "__main__":
    ok = run_tests()
    sys.exit(0 if ok else 1)
