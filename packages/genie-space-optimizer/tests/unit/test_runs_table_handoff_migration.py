"""Tests for genie_opt_runs schema additions for cross-task state resilience.

The handoff plan widens genie_opt_runs with nullable columns so values can be
recovered from Delta when taskValues do not propagate on Repair Run.
"""
from unittest.mock import MagicMock

import pandas as pd

from genie_space_optimizer.optimization.ddl import _GENIE_OPT_RUNS_DDL
from genie_space_optimizer.optimization.state import _migrate_add_columns


def test_runs_ddl_includes_handoff_columns():
    """The fresh DDL must declare the handoff/telemetry columns."""
    assert "warehouse_id" in _GENIE_OPT_RUNS_DDL
    assert "max_benchmark_count" in _GENIE_OPT_RUNS_DDL
    assert "llm_model" in _GENIE_OPT_RUNS_DDL


def test_migration_adds_handoff_columns_when_missing():
    """_migrate_add_columns must ALTER TABLE for each handoff column."""
    spark = MagicMock()
    # First DESCRIBE returns no handoff columns; subsequent ones return
    # whatever the test wants — we only assert ALTER TABLE was issued.
    spark.sql.return_value.collect.return_value = [
        {"col_name": "run_id"}, {"col_name": "space_id"},
    ]

    _migrate_add_columns(spark, "test_catalog", "test_schema")

    issued = [str(call.args[0]) for call in spark.sql.call_args_list]
    altered = [s for s in issued if s.startswith("ALTER TABLE")]
    assert any("warehouse_id" in s for s in altered)
    assert any("max_benchmark_count" in s for s in altered)
    assert any("llm_model" in s for s in altered)


def test_migration_idempotent_when_columns_already_exist():
    """When columns already exist, _migrate_add_columns must not ALTER."""
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [
        {"col_name": "run_id"},
        {"col_name": "warehouse_id"},
        {"col_name": "max_benchmark_count"},
        {"col_name": "llm_model"},
    ]

    _migrate_add_columns(spark, "test_catalog", "test_schema")

    issued = [str(call.args[0]) for call in spark.sql.call_args_list]
    handoff_alters = [
        s for s in issued
        if s.startswith("ALTER TABLE") and (
            "warehouse_id" in s
            or "max_benchmark_count" in s
            or "llm_model" in s
        )
    ]
    assert handoff_alters == [], (
        f"expected zero handoff ALTERs when columns already exist, got: "
        f"{handoff_alters}"
    )


def test_warehouse_ensure_adds_llm_model_when_runs_table_is_old(monkeypatch):
    """Warehouse-first trigger must migrate old run tables before INSERT."""
    from genie_space_optimizer.common import warehouse

    ws = MagicMock()
    warehouse_id = "wh"
    executed: list[str] = []
    columns_by_table: dict[str, set[str]] = {
        "test_catalog.test_schema.genie_opt_runs": {
            "run_id",
            "space_id",
            "domain",
            "catalog",
            "uc_schema",
            "status",
            "started_at",
            "job_run_id",
            "max_iterations",
            "levers",
            "apply_mode",
            "experiment_name",
            "triggered_by",
            "config_snapshot",
            "updated_at",
        }
    }

    def fake_execute(_ws, _warehouse_id, sql):
        executed.append(sql)
        parts = sql.split()
        if parts[:5] == ["ALTER", "TABLE", parts[2], "ADD", "COLUMN"]:
            columns_by_table.setdefault(parts[2], set()).add(parts[5].lower())

    def fake_query(_ws, _warehouse_id, sql):
        fqn = sql.split()[-1]
        return pd.DataFrame(
            [{"col_name": col} for col in sorted(columns_by_table.get(fqn, set()))]
        )

    monkeypatch.setattr(warehouse, "sql_warehouse_execute", fake_execute)
    monkeypatch.setattr(warehouse, "sql_warehouse_query", fake_query)

    warehouse.wh_ensure_optimization_tables(
        ws,
        warehouse_id,
        "test_catalog",
        "test_schema",
    )

    assert "llm_model" in columns_by_table["test_catalog.test_schema.genie_opt_runs"]
    assert any(
        "ALTER TABLE test_catalog.test_schema.genie_opt_runs ADD COLUMN llm_model" in sql
        for sql in executed
    )
