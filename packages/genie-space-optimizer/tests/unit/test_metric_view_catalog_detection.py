"""Unit tests for catalog-level metric-view detection.

Locks the contract that ``_detect_metric_views_via_catalog`` correctly
classifies entities based on their UC ``DESCRIBE TABLE EXTENDED ... AS
JSON`` payload, and that
``effective_metric_view_identifiers_with_catalog`` unions the catalog
detection with the existing column-config heuristic.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


METRIC_VIEW_YAML = """\
version: 0.1
source: cat.sch.fact_sales
dimensions:
  - name: store_id
    expr: store_id
  - name: sale_date
    expr: sale_date
measures:
  - name: total_revenue
    expr: SUM(revenue)
  - name: avg_revenue
    expr: AVG(revenue)
"""


REGULAR_VIEW_DDL = (
    "CREATE VIEW cat.sch.regular_view AS\n"
    "SELECT * FROM cat.sch.fact_sales WHERE region = 'US'\n"
)


def _make_describe_response(view_text: str) -> str:
    """Mimic the JSON shape Spark emits for DESCRIBE TABLE EXTENDED AS JSON."""
    return json.dumps({"view_text": view_text, "table_type": "VIEW"})


def test_detect_metric_view_yaml_payload():
    """A ref whose DESCRIBE returns metric-view YAML is classified as MV."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [("cat", "sch", "mv_sales")]

    def fake_exec_sql(sql, *args, **kwargs):
        import pandas as pd

        return pd.DataFrame(
            [{"json_metadata": _make_describe_response(METRIC_VIEW_YAML)}]
        )

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert "cat.sch.mv_sales" in detected
    assert "cat.sch.mv_sales" in yamls
    yaml_dict = yamls["cat.sch.mv_sales"]
    assert yaml_dict.get("source") == "cat.sch.fact_sales"
    assert {d["name"] for d in yaml_dict.get("dimensions", [])} == {
        "store_id",
        "sale_date",
    }
    assert {m["name"] for m in yaml_dict.get("measures", [])} == {
        "total_revenue",
        "avg_revenue",
    }


def test_detect_regular_view_not_classified():
    """A ref whose DESCRIBE returns regular view DDL is NOT classified."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [("cat", "sch", "regular_view")]

    def fake_exec_sql(sql, *args, **kwargs):
        import pandas as pd

        return pd.DataFrame(
            [{"json_metadata": _make_describe_response(REGULAR_VIEW_DDL)}]
        )

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert detected == set()
    assert yamls == {}


def test_detect_failure_does_not_propagate():
    """When DESCRIBE raises, ref is treated as non-MV; no exception escapes."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [("cat", "sch", "missing_table")]

    def fake_exec_sql(sql, *args, **kwargs):
        raise RuntimeError("table not found")

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert detected == set()
    assert yamls == {}


def test_detect_handles_multiple_refs_mixed_outcomes():
    """One MV + one regular view + one failure → exactly one MV classified."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [
        ("cat", "sch", "mv_sales"),
        ("cat", "sch", "regular_view"),
        ("cat", "sch", "broken_table"),
    ]

    def fake_exec_sql(sql, *args, **kwargs):
        import pandas as pd

        if "mv_sales" in sql:
            return pd.DataFrame(
                [{"json_metadata": _make_describe_response(METRIC_VIEW_YAML)}]
            )
        if "regular_view" in sql:
            return pd.DataFrame(
                [{"json_metadata": _make_describe_response(REGULAR_VIEW_DDL)}]
            )
        raise RuntimeError("not found")

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert detected == {"cat.sch.mv_sales"}
    assert set(yamls) == {"cat.sch.mv_sales"}


def test_effective_with_catalog_unions_sources():
    """Catalog-detected MV unions with column_config heuristic."""
    from genie_space_optimizer.optimization.evaluation import (
        effective_metric_view_identifiers_with_catalog,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                # column_config heuristic catches this one.
                "tables": [
                    {
                        "identifier": "cat.sch.mv_via_columns",
                        "column_configs": [
                            {"column_name": "amount", "column_type": "measure"},
                        ],
                    },
                ],
                "metric_views": [
                    {"identifier": "cat.sch.mv_explicit"},
                ],
            },
        },
        # Catalog detector adds this one (no measure column configs).
        "_metric_view_yaml": {
            "cat.sch.mv_via_catalog": {"source": "cat.sch.fact"},
            # Already known by column-config path; should still appear once.
            "cat.sch.mv_via_columns": {"source": "cat.sch.fact2"},
        },
    }

    result = effective_metric_view_identifiers_with_catalog(config)

    # Identifiers come back lower-cased? Reuse evaluation's existing
    # casing rules — the function returns identifiers as-stored.
    lowered = {ident.lower() for ident in result}
    assert "cat.sch.mv_via_columns" in lowered
    assert "cat.sch.mv_explicit" in lowered
    assert "cat.sch.mv_via_catalog" in lowered


def test_effective_with_catalog_falls_back_when_yaml_cache_absent():
    """No catalog cache on config → behaviour matches column-config path."""
    from genie_space_optimizer.optimization.evaluation import (
        effective_metric_view_identifiers,
        effective_metric_view_identifiers_with_catalog,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "metric_views": [{"identifier": "cat.sch.mv_only"}],
                "tables": [],
            },
        },
    }

    base = effective_metric_view_identifiers(config)
    extended = effective_metric_view_identifiers_with_catalog(config)
    assert base == extended
