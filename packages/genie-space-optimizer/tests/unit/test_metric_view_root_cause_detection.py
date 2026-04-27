from __future__ import annotations

import json
from dataclasses import dataclass

import pandas as pd


class ExplodingSparkConf:
    def __init__(self) -> None:
        self.set_calls: list[tuple[str, str]] = []

    def set(self, key: str, value: str) -> None:
        self.set_calls.append((key, value))
        raise RuntimeError(
            "[CONFIG_NOT_AVAILABLE] Configuration "
            "spark.databricks.metadata.metricview.enabled is not available"
        )


class FakeSpark:
    def __init__(self) -> None:
        self.conf = ExplodingSparkConf()

    def sql(self, sql: str):
        raise AssertionError(f"Spark SQL fallback should not run: {sql}")


@dataclass
class FakeWarehouse:
    statements: list[str]


def _mv_describe_json() -> pd.DataFrame:
    envelope = {
        "type": "METRIC_VIEW",
        "language": "YAML",
        "view_text": """
version: 1.1
source: cat.sch.fact
dimensions:
  - name: zone_combination
    expr: zone_combination
measures:
  - name: 7now_orders_diff_mtd
    expr: sum(orders_diff_mtd)
""",
        "columns": [
            {"name": "zone_combination", "type_text": "string"},
            {
                "name": "7now_orders_diff_mtd",
                "type_text": "double",
                "is_measure": True,
            },
        ],
    }
    return pd.DataFrame({"json": [json.dumps(envelope)]})


def test_metric_view_detection_uses_warehouse_without_spark_conf():
    from genie_space_optimizer.common.metric_view_catalog import (
        detect_metric_views_via_catalog_with_outcomes,
    )

    spark = FakeSpark()
    warehouse = FakeWarehouse(statements=[])

    def exec_sql(sql, spark_arg, *, w=None, warehouse_id="", catalog="", schema=""):
        assert w is warehouse
        assert warehouse_id == "wh-123"
        assert "DESCRIBE TABLE EXTENDED" in sql
        assert "AS JSON" in sql
        warehouse.statements.append(sql)
        return _mv_describe_json()

    detected, yamls, outcomes = detect_metric_views_via_catalog_with_outcomes(
        spark,
        [("cat", "sch", "mv_sales")],
        w=warehouse,
        warehouse_id="wh-123",
        catalog="cat",
        schema="sch",
        exec_sql=exec_sql,
    )

    assert detected == {"cat.sch.mv_sales"}
    assert yamls["cat.sch.mv_sales"]["measures"][0]["name"] == "7now_orders_diff_mtd"
    assert outcomes["cat.sch.mv_sales"].startswith("detected")
    assert spark.conf.set_calls == []
    assert warehouse.statements == [
        "DESCRIBE TABLE EXTENDED `cat`.`sch`.`mv_sales` AS JSON"
    ]


def test_metric_view_detection_records_no_warehouse_without_conf_noise():
    from genie_space_optimizer.common.metric_view_catalog import (
        OUTCOME_NO_WAREHOUSE,
        detect_metric_views_via_catalog_with_outcomes,
    )

    spark = FakeSpark()
    diagnostic_samples: dict[str, str] = {}

    detected, yamls, outcomes = detect_metric_views_via_catalog_with_outcomes(
        spark,
        [("cat", "sch", "mv_sales")],
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        diagnostic_samples=diagnostic_samples,
    )

    assert detected == set()
    assert yamls == {}
    assert outcomes["cat.sch.mv_sales"] == OUTCOME_NO_WAREHOUSE
    assert "warehouse_id" in diagnostic_samples["cat.sch.mv_sales"]
    assert spark.conf.set_calls == []


def test_extract_metric_view_fqns_from_spark_plan_error():
    from genie_space_optimizer.common.asset_semantics import (
        extract_metric_view_identifiers_from_error,
    )

    message = """
    Project [...]
    +- SubqueryAlias prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales
       +- MetricView `prashanth_subrahmanyam_catalog`.`sales_reports`.`mv_7now_store_sales`
    +- SubqueryAlias cat.sch.mv_other
       +- MetricView `cat`.`sch`.`mv_other`
    """

    assert extract_metric_view_identifiers_from_error(message) == {
        "prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales",
        "cat.sch.mv_other",
    }


def test_stamp_metric_views_from_planner_errors_upgrades_unknown_semantics():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        get_asset_semantics,
        stamp_metric_views_from_planner_errors,
    )

    config = {
        "_parsed_space": {
            "data_sources": {"tables": [{"name": "cat.sch.mv_sales"}]},
        },
        "_asset_semantics": {
            "cat.sch.mv_sales": {
                "identifier": "cat.sch.mv_sales",
                "short_name": "mv_sales",
                "kind": "table",
                "measures": [],
                "dimensions": ["zone_combination"],
                "provenance": ["genie_tables"],
                "outcome": "no_view_text",
                "detection_errors": [],
            }
        },
    }
    errors = [
        "Error occurred during query planning: "
        "MetricView `cat`.`sch`.`mv_sales` "
        "[METRIC_VIEW_MISSING_MEASURE_FUNCTION]",
    ]

    changed = stamp_metric_views_from_planner_errors(config, errors)

    semantics = get_asset_semantics(config)
    assert changed == {"cat.sch.mv_sales"}
    assert semantics["cat.sch.mv_sales"]["kind"] == KIND_METRIC_VIEW
    assert "planner_error" in semantics["cat.sch.mv_sales"]["provenance"]
    assert "planner_error_metric_view" in semantics["cat.sch.mv_sales"]["outcome"]
    assert config["_parsed_space"]["_asset_semantics"]["cat.sch.mv_sales"]["kind"] == KIND_METRIC_VIEW


def test_pre_execute_repair_order_by_measure_original_name_after_alias_collision():
    from genie_space_optimizer.optimization.evaluation import apply_pre_execute_repairs

    sql = """
    SELECT
      zone_combination,
      MEASURE(`7now_orders_diff_mtd`) AS `7now_orders_diff_mtd`
    FROM cat.sch.mv_7now_store_sales
    GROUP BY zone_combination
    ORDER BY MEASURE(`7now_orders_diff_mtd`) DESC
    """
    counters: dict[str, int] = {}

    repaired = apply_pre_execute_repairs(
        sql,
        mv_measures={
            "mv_7now_store_sales": {
                "7now_orders_diff_mtd",
            },
        },
        mv_short_set={"mv_7now_store_sales"},
        canonical_assets=["cat.sch.mv_7now_store_sales"],
        counters=counters,
    )

    assert "AS `7now_orders_diff_mtd_value`" in repaired or "AS 7now_orders_diff_mtd_value" in repaired
    assert "ORDER BY `7now_orders_diff_mtd_value` DESC" in repaired or "ORDER BY 7now_orders_diff_mtd_value DESC" in repaired
    assert "ORDER BY MEASURE(`7now_orders_diff_mtd`)" not in repaired
    assert counters["repaired_measure_alias_collisions"] == 1
    assert counters["repaired_order_by_measure_alias"] == 1


def test_resolve_warehouse_id_precedence(monkeypatch):
    from genie_space_optimizer.common.warehouse import resolve_warehouse_id

    monkeypatch.setenv("SQL_WAREHOUSE_ID", "sql-wh")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "gso-wh")
    monkeypatch.setenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "legacy-wh")

    assert resolve_warehouse_id("explicit-wh") == "explicit-wh"
    assert resolve_warehouse_id("") == "legacy-wh"

    monkeypatch.delenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID")
    assert resolve_warehouse_id("") == "gso-wh"

    monkeypatch.delenv("GSO_WAREHOUSE_ID")
    assert resolve_warehouse_id("") == "sql-wh"

    monkeypatch.delenv("SQL_WAREHOUSE_ID")
    assert resolve_warehouse_id("") == ""


def test_export_warehouse_id_sets_all_runtime_names(monkeypatch):
    import os

    from genie_space_optimizer.common.warehouse import export_warehouse_id

    export_warehouse_id("wh-abc")

    assert os.environ["GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID"] == "wh-abc"
    assert os.environ["GSO_WAREHOUSE_ID"] == "wh-abc"


def test_prepare_lever_loop_uses_resolved_warehouse_id_for_catalog_detection(monkeypatch):
    from genie_space_optimizer.common.warehouse import resolve_warehouse_id

    monkeypatch.delenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", raising=False)
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "gso-wh-123")

    assert resolve_warehouse_id("") == "gso-wh-123"
