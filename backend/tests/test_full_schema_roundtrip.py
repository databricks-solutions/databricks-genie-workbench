"""
Round trip: exercise every serialized_space schema feature through
generate_config → validate_config → enforce_constraints → clean_config.

Also validates the canonical reference JSON directly.

Ported from the retired root script tests/test_full_schema.py, which no runner
collected. The fixture constants below are unchanged from that script.
"""

import copy
import json

import pytest

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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.fixture
def generated() -> dict:
    """A fresh _generate_config result per test (inputs deep-copied so no test
    can leak mutations into another)."""
    result = _generate_config(
        tables=copy.deepcopy(TABLES),
        sample_questions=copy.deepcopy(SAMPLE_QUESTIONS),
        text_instructions=copy.deepcopy(TEXT_INSTRUCTIONS),
        example_sqls=copy.deepcopy(EXAMPLE_SQLS),
        measures=copy.deepcopy(MEASURES),
        filters=copy.deepcopy(FILTERS),
        expressions=copy.deepcopy(EXPRESSIONS),
        join_specs=copy.deepcopy(JOIN_SPECS),
        benchmarks=copy.deepcopy(BENCHMARKS),
        metric_views=copy.deepcopy(METRIC_VIEWS),
    )
    assert "error" not in result, result.get("error")
    return result


@pytest.fixture
def config(generated) -> dict:
    return generated["config"]


def _example_sqls(config: dict) -> list[dict]:
    return config.get("instructions", {}).get("example_question_sqls", [])


def _orders_column(config: dict, name: str) -> dict:
    orders = next(t for t in config["data_sources"]["tables"] if "orders" in t["identifier"])
    col = next((c for c in orders.get("column_configs", []) if c["column_name"] == name), None)
    assert col is not None, f"orders.{name} missing from column_configs"
    return col


# --- 1. _generate_config with every schema feature ---

def test_generated_config_version_and_section_counts(config, generated):
    assert config.get("version") == 2
    expected = {
        "tables": 3,
        "sample_questions": 5,
        "example_sqls": 5,
        "measures": 3,
        "filters": 3,
        "expressions": 3,
        "join_specs": 2,
        "benchmarks": 5,
        "text_instructions": 5,
    }
    summary = generated["summary"]
    assert {k: summary.get(k) for k in expected} == expected


def test_metric_view_carries_column_configs(config):
    mvs = config.get("data_sources", {}).get("metric_views", [])
    assert len(mvs) == 1
    assert "column_configs" in mvs[0]


def test_join_specs_have_rt_annotation_and_backtick_refs(config):
    join_specs = config.get("instructions", {}).get("join_specs", [])
    assert join_specs
    for js in join_specs:
        sql = js.get("sql", [])
        assert any(s.startswith("--rt=") for s in sql), js
        assert any("`" in s for s in sql if not s.startswith("--rt=")), js


def test_text_instruction_lines_end_with_newline(config):
    ti = config.get("instructions", {}).get("text_instructions", [])
    assert ti
    for line in ti[0].get("content", []):
        assert line.endswith("\n"), repr(line)


def test_example_sqls_parameters_and_usage_guidance(config):
    eqs = _example_sqls(config)
    assert sum(1 for eq in eqs if eq.get("parameters")) == 2
    assert sum(1 for eq in eqs if eq.get("usage_guidance")) == 5


def test_parameter_default_values_are_wrapped(config):
    params = [p for eq in _example_sqls(config) for p in eq.get("parameters", [])]
    assert params
    for p in params:
        dv = p.get("default_value", {})
        assert isinstance(dv, dict) and "values" in dv, p


def test_excluded_column_disables_matching_and_format_assistance(config):
    col = _orders_column(config, "etl_timestamp")
    assert col.get("exclude") is True
    assert col.get("enable_entity_matching") is False
    assert col.get("enable_format_assistance") is False


def test_non_excluded_column_keeps_matching_and_format_assistance(config):
    col = _orders_column(config, "region")
    assert col.get("enable_entity_matching") is True
    assert col.get("enable_format_assistance") is True


def test_benchmarks_have_exactly_one_sql_answer(config):
    questions = config.get("benchmarks", {}).get("questions", [])
    assert questions
    for bq in questions:
        answers = bq.get("answer", [])
        assert len(answers) == 1, bq
        assert answers[0].get("format") == "SQL", bq


# --- 2. _validate_config on the generated config ---

def test_generated_config_validates_without_errors(config):
    result = _validate_config(config=config)
    assert result["errors"] == []
    assert result["valid"] is True


# --- 3. _enforce_constraints + _clean_config on the generated config ---

def test_generated_config_survives_enforce_and_clean(config):
    cleaned = _clean_config(_enforce_constraints(config))
    serialized = json.dumps(cleaned)
    assert len(serialized) < 3_500_000

    reparsed = json.loads(serialized)
    identifiers = [t["identifier"] for t in reparsed["data_sources"]["tables"]]
    assert identifiers == sorted(identifiers)


# --- 4./5. The canonical reference JSON ---

def test_canonical_config_validates_without_errors():
    result = _validate_config(config=copy.deepcopy(CANONICAL_JSON))
    assert result["errors"] == []
    assert result["valid"] is True


def test_canonical_config_survives_enforce_and_clean():
    cleaned = _clean_config(_enforce_constraints(copy.deepcopy(CANONICAL_JSON)))
    assert isinstance(json.loads(json.dumps(cleaned)), dict)
