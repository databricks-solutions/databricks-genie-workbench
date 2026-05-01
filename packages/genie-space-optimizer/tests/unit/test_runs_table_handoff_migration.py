"""Tests for genie_opt_runs schema additions for cross-task state resilience.

The handoff plan widens genie_opt_runs with 3 nullable columns so that
warehouse_id, human_corrections_json, and max_benchmark_count can be
recovered from Delta when taskValues do not propagate on Repair Run.
"""
from unittest.mock import MagicMock

from genie_space_optimizer.optimization.ddl import _GENIE_OPT_RUNS_DDL
from genie_space_optimizer.optimization.state import _migrate_add_columns


def test_runs_ddl_includes_handoff_columns():
    """The fresh DDL must declare the 3 handoff columns."""
    assert "warehouse_id" in _GENIE_OPT_RUNS_DDL
    assert "human_corrections_json" in _GENIE_OPT_RUNS_DDL
    assert "max_benchmark_count" in _GENIE_OPT_RUNS_DDL


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
    assert any("human_corrections_json" in s for s in altered)
    assert any("max_benchmark_count" in s for s in altered)


def test_migration_idempotent_when_columns_already_exist():
    """When columns already exist, _migrate_add_columns must not ALTER."""
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [
        {"col_name": "run_id"},
        {"col_name": "warehouse_id"},
        {"col_name": "human_corrections_json"},
        {"col_name": "max_benchmark_count"},
    ]

    _migrate_add_columns(spark, "test_catalog", "test_schema")

    issued = [str(call.args[0]) for call in spark.sql.call_args_list]
    handoff_alters = [
        s for s in issued
        if s.startswith("ALTER TABLE") and (
            "warehouse_id" in s
            or "human_corrections_json" in s
            or "max_benchmark_count" in s
        )
    ]
    assert handoff_alters == [], (
        f"expected zero handoff ALTERs when columns already exist, got: "
        f"{handoff_alters}"
    )
