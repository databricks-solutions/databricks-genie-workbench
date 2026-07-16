"""Tests for additive migration of active four-task tables."""

from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.optimization import state
from genie_space_optimizer.optimization.ddl import (
    ADDITIVE_COLUMN_MIGRATIONS,
    TABLE_ITERATIONS,
    _GENIE_OPT_ITERATIONS_DDL,
)


class FakeSpark:
    def __init__(self, existing: tuple[str, ...] = ()) -> None:
        self.existing = existing
        self.sql_calls: list[str] = []

    def sql(self, statement: str):
        self.sql_calls.append(statement)
        result = MagicMock()
        if statement.upper().lstrip().startswith("DESCRIBE TABLE"):
            result.collect.return_value = [{"col_name": name} for name in self.existing]
        return result


def test_active_migrations_include_write_critical_columns() -> None:
    columns = {
        column
        for table, column, _definition in ADDITIVE_COLUMN_MIGRATIONS
        if table == TABLE_ITERATIONS
    }
    assert {
        "evaluated_count",
        "excluded_count",
        "rolled_back",
        "config_json",
        "is_champion",
        "num_needs_review",
        "attempt_no",
        "terminal_reason",
        "target_accuracy",
        "max_attempts",
    } <= columns


def test_fresh_iteration_ddl_contains_active_columns_only() -> None:
    ddl = _GENIE_OPT_ITERATIONS_DDL.lower()
    for column in (
        "evaluated_count",
        "excluded_count",
        "rolled_back",
        "config_json",
        "is_champion",
        "num_needs_review",
        "attempt_no",
        "terminal_reason",
    ):
        assert column in ddl
    for retired in (
        "arbiter_actions_json",
        "repeatability_pct",
        "repeatability_json",
        "quarantined_benchmarks_json",
        "both_correct_count",
        "synthesis_slots_persisted",
    ):
        assert retired not in ddl


def test_apply_one_migration_strips_default_from_add_column() -> None:
    spark = FakeSpark()
    state._apply_one_migration(
        spark,
        fqn="cat.sch.genie_opt_iterations",
        col="rolled_back",
        col_def="BOOLEAN DEFAULT false COMMENT 'rollback marker'",
    )

    add = next(sql for sql in spark.sql_calls if " ADD COLUMN " in sql.upper())
    assert "DEFAULT" not in add.upper()
    assert any(" SET DEFAULT false" in sql for sql in spark.sql_calls)


def test_migrate_add_columns_enables_defaults_before_alters(monkeypatch) -> None:
    spark = FakeSpark(existing=tuple(column for _table, column, _definition in ADDITIVE_COLUMN_MIGRATIONS))
    monkeypatch.setattr(state, "_verify_required_columns", lambda *_args: None)

    state._migrate_add_columns(spark, "cat", "sch")

    assert "allowColumnDefaults" in spark.sql_calls[0]
