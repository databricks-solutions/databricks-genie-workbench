"""Shared test fixtures for backend unit tests."""

from types import SimpleNamespace

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_space_access: run against the real MV-D109 resolver, not the default grant",
    )


def _make_table(name, *, description=None, columns=None, column_configs=None,
                entity_matching=False, format_assistance=False, row_filter=None):
    """Helper to build a table dict matching Genie Agent config format."""
    t = {"name": name, "identifier": f"catalog.schema.{name}"}
    if description:
        t["description"] = description
    cols = []
    for c in (columns or []):
        col = {"name": c["name"]}
        if c.get("description"):
            col["description"] = c["description"]
        if c.get("type"):
            col["type"] = c["type"]
        if entity_matching or c.get("enable_entity_matching"):
            col["enable_entity_matching"] = True
        if format_assistance or c.get("format_assistance_enabled"):
            col["format_assistance_enabled"] = True
        if c.get("synonyms"):
            col["synonyms"] = c["synonyms"]
        cols.append(col)
    if cols:
        t["columns"] = cols
    if column_configs:
        t["column_configs"] = column_configs
    if row_filter:
        t["row_filter"] = row_filter
    return t


def _make_example_sql(n):
    """Generate n example SQL entries."""
    return [
        {"id": str(i), "question": [f"Question {i}?"], "sql": [f"SELECT {i}"],
         "usage_guidance": [f"Use when asking about {i}"]}
        for i in range(n)
    ]


@pytest.fixture
def full_space_data():
    """Space config that passes all 10 config checks.

    - 3 tables with descriptions and described columns (100% coverage)
    - Text instructions > 50 chars
    - Join specs present
    - 10 example SQLs with usage_guidance
    - SQL snippets (filters + measures + expressions + functions)
    - Entity matching enabled
    - 20 benchmark questions
    """
    tables = [
        _make_table("orders", description="Customer orders table", columns=[
            {"name": "order_id", "description": "Unique order ID", "type": "bigint",
             "enable_entity_matching": True},
            {"name": "customer_id", "description": "Customer FK", "type": "bigint"},
            {"name": "amount", "description": "Order total", "type": "decimal",
             "format_assistance_enabled": True},
            {"name": "order_date", "description": "Date placed", "type": "date"},
        ]),
        _make_table("customers", description="Customer master data", columns=[
            {"name": "customer_id", "description": "Primary key", "type": "bigint"},
            {"name": "name", "description": "Customer name", "type": "string",
             "enable_entity_matching": True},
            {"name": "region", "description": "Sales region", "type": "string",
             "enable_entity_matching": True, "synonyms": ["area", "territory"]},
        ]),
        _make_table("products", description="Product catalog", columns=[
            {"name": "product_id", "description": "Primary key", "type": "bigint"},
            {"name": "product_name", "description": "Product name", "type": "string"},
        ]),
    ]

    return {
        "description": (
            "This space answers e-commerce analytics questions for revenue and "
            "operations teams using orders, customers, and product data."
        ),
        "data_sources": {"tables": tables},
        "instructions": {
            "text_instructions": [
                {"content": ["This space covers e-commerce analytics including orders, "
                             "customers, and products. Revenue is calculated as sum of amount."]}
            ],
            "join_specs": [
                {"id": "j1", "sql": ["ON orders.customer_id = customers.customer_id "
                                      "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
                {"id": "j2", "sql": ["ON orders.product_id = products.product_id "
                                      "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
            ],
            "example_question_sqls": _make_example_sql(10),
            "sql_functions": [{"id": "f1", "identifier": "revenue", "sql": ["SUM(amount)"]}],
            "sql_snippets": {
                "filters": [{"id": "sf1", "sql": ["WHERE region = 'AMER'"]}],
                "measures": [{"id": "sm1", "sql": ["SUM(amount) AS total_revenue"]}],
                "expressions": [{"id": "se1", "sql": ["CASE WHEN amount > 100 THEN 'high' END"]}],
            },
        },
        "benchmarks": {
            "questions": [{"id": str(i), "question": [f"Benchmark {i}?"]} for i in range(20)]
        },
    }


@pytest.fixture
def empty_space_data():
    """Minimal space config that fails all checks."""
    return {
        "data_sources": {"tables": []},
        "instructions": {},
        "benchmarks": {},
    }


@pytest.fixture
def metric_view_only_space():
    """Space with only metric views, no tables."""
    return {
        "data_sources": {"metric_views": [{"identifier": "cat.sch.mv1"}]},
        "instructions": {},
        "benchmarks": {},
    }


@pytest.fixture(autouse=True)
def _grant_space_access_by_default(request, monkeypatch):
    """Route tests exercise their route, not the access gate; the gate has its own tests."""
    from backend.services import space_access
    from genie_space_optimizer.common.genie_client import SpaceAccessCheck

    space_access.clear_cache()
    if request.node.get_closest_marker("real_space_access") is None:
        monkeypatch.setattr(
            space_access, "require_obo_workspace_client",
            lambda: SimpleNamespace(config=SimpleNamespace(token="test-default-grant")),
        )
        monkeypatch.setattr(
            space_access, "check_space_access",
            lambda client, space_id, level: SpaceAccessCheck(True, 200),
        )
        # Keep this fixture about access, not SDK cloning of the fake OBO client.
        monkeypatch.setattr(space_access, "bounded_read_client", lambda c, **k: c)
    yield
    space_access.clear_cache()
