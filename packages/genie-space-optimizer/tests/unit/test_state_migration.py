"""Regression tests for ``_migrate_add_columns`` in ``state.py``.

These tests cover the specific bug where migration entries that declared
``DEFAULT <literal>`` (unquoted, e.g. ``DEFAULT false``) silently failed
because the default-stripping regex only matched single-quoted string
defaults. The resulting ``ALTER TABLE ADD COLUMN <col> BOOLEAN DEFAULT
false COMMENT '…'`` is rejected on pre-existing Delta tables that lack
the ``allowColumnDefaults`` feature, the error is swallowed as a WARN,
and downstream ``INSERT`` statements fail with
``[UNRESOLVED_COLUMN.WITH_SUGGESTION]``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.optimization import state as state_mod


class _FakeSpark:
    """Minimal Spark stub that records the SQL statements executed.

    * ``DESCRIBE TABLE`` returns ``existing_cols`` — controls whether the
      migration sees a column as already present.
    * ``ALTER TABLE ... ADD COLUMN ... DEFAULT <literal>`` raises the
      given ``add_default_error`` to simulate a Delta table that rejects
      unquoted defaults (``delta.feature.allowColumnDefaults`` absent).
    """

    def __init__(
        self,
        existing_cols: list[str] | None = None,
        *,
        add_default_error: Exception | None = None,
    ) -> None:
        self.sql_calls: list[str] = []
        self._existing = existing_cols or []
        self._add_default_error = add_default_error

    def sql(self, stmt: str):
        self.sql_calls.append(stmt)
        upper = stmt.upper().lstrip()
        if upper.startswith("DESCRIBE TABLE"):
            rows = [{"col_name": c} for c in self._existing]
            result = MagicMock()
            result.collect.return_value = rows
            return result
        if (
            upper.startswith("ALTER TABLE")
            and " ADD COLUMN " in upper
            and " DEFAULT " in upper
            and self._add_default_error is not None
        ):
            raise self._add_default_error
        return MagicMock()


def test_rolled_back_entry_is_present_in_real_migrations():
    """The real migration list must include ``rolled_back``.

    Guards against future refactors that accidentally drop the entry and
    cause the original UNRESOLVED_COLUMN error to reappear on fresh
    schemas. The migration list was extracted from ``_migrate_add_columns``
    into ``ddl.ADDITIVE_COLUMN_MIGRATIONS`` (the loop now iterates it), so
    the guard inspects the real list rather than the function source.
    """
    from genie_space_optimizer.optimization.ddl import (
        ADDITIVE_COLUMN_MIGRATIONS,
        TABLE_ITERATIONS,
    )

    entry = next(
        (
            (table, col, col_def)
            for table, col, col_def in ADDITIVE_COLUMN_MIGRATIONS
            if col == "rolled_back"
        ),
        None,
    )
    assert entry is not None, "migration for rolled_back missing"
    table, _col, col_def = entry
    assert table == TABLE_ITERATIONS
    assert "BOOLEAN" in col_def and "DEFAULT false" in col_def


def test_migration_handles_unquoted_default_literal():
    """When a column has ``DEFAULT <unquoted-literal>`` (e.g. ``false``,
    ``0``, ``NULL``), the migration must strip the DEFAULT from the ADD
    COLUMN statement so the column is created even on Delta tables that
    do not have the ``allowColumnDefaults`` feature. The DEFAULT is then
    applied in a separate ``ALTER COLUMN ... SET DEFAULT`` that is
    allowed to fail without blocking the ADD.
    """
    spark = _FakeSpark(existing_cols=["run_id", "iteration"])

    from genie_space_optimizer.optimization.state import _apply_one_migration  # type: ignore[attr-defined]

    _apply_one_migration(
        spark,
        fqn="cat.sch.genie_opt_iterations",
        col="rolled_back",
        col_def="BOOLEAN DEFAULT false COMMENT 'test'",
    )

    add_stmts = [s for s in spark.sql_calls if " ADD COLUMN " in s.upper()]
    assert len(add_stmts) == 1, f"expected exactly one ADD COLUMN, got {add_stmts}"
    assert "DEFAULT" not in add_stmts[0].upper(), (
        f"ADD COLUMN must not include DEFAULT so it works on Delta tables "
        f"without allowColumnDefaults; got: {add_stmts[0]}"
    )
    assert "BOOLEAN" in add_stmts[0].upper()
    assert "rolled_back" in add_stmts[0]


def test_migration_add_column_succeeds_when_set_default_rejected():
    """If ``ALTER COLUMN ... SET DEFAULT`` is rejected by the engine,
    the migration must still leave the column present — i.e. it must not
    roll back or re-raise, and subsequent ``INSERT`` (which provides the
    value explicitly anyway) must not be blocked."""
    from genie_space_optimizer.optimization.state import _apply_one_migration  # type: ignore[attr-defined]

    class _SparkSetDefaultRejects(_FakeSpark):
        def sql(self, stmt: str):
            super().sql(stmt)
            if stmt.upper().lstrip().startswith("ALTER TABLE") and " SET DEFAULT" in stmt.upper():
                raise RuntimeError(
                    "DEFAULT values are not supported for new columns on existing Delta tables"
                )
            return MagicMock()

    spark = _SparkSetDefaultRejects(existing_cols=["run_id"])
    _apply_one_migration(
        spark,
        fqn="cat.sch.genie_opt_iterations",
        col="rolled_back",
        col_def="BOOLEAN DEFAULT false COMMENT 'test'",
    )
    add_stmts = [s for s in spark.sql_calls if " ADD COLUMN " in s.upper()]
    assert len(add_stmts) == 1, "ADD COLUMN must have been issued once"


def test_iterations_ddl_includes_tier_one_columns():
    """Fresh installs must not rely on the migration loop to add the
    write-critical Tier 1.1 / Tier 1.7 columns. They should be declared
    in ``_GENIE_OPT_ITERATIONS_DDL`` so ``CREATE TABLE`` creates them
    up-front, and the migration loop only runs for existing tables.

    This regression test pins the DDL to the contract that writers rely
    on — if a new write-critical column is introduced it should be added
    both here AND in the DDL so new deployments pick it up without
    round-tripping through ALTER TABLE.
    """
    from genie_space_optimizer.optimization.ddl import _GENIE_OPT_ITERATIONS_DDL

    ddl = _GENIE_OPT_ITERATIONS_DDL.lower()
    for col in (
        "rolled_back",
        "rolled_back_at",
        "rollback_reason",
        "both_correct_count",
        "both_correct_rate",
    ):
        assert f" {col} " in ddl or f" {col}\n" in ddl, (
            f"Expected {col} to be declared in _GENIE_OPT_ITERATIONS_DDL "
            f"so fresh installs get it directly via CREATE TABLE."
        )


def test_iterations_ddl_enables_allow_column_defaults():
    """The iterations DDL must opt into the ``allowColumnDefaults`` Delta
    table feature so future migrations that declare ``DEFAULT`` literals
    succeed on fresh tables without an extra ALTER TABLE round-trip.
    """
    from genie_space_optimizer.optimization.ddl import _GENIE_OPT_ITERATIONS_DDL

    ddl = _GENIE_OPT_ITERATIONS_DDL
    assert "delta.feature.allowColumnDefaults" in ddl, (
        "Expected 'delta.feature.allowColumnDefaults' = 'supported' in the "
        "iterations DDL TBLPROPERTIES so DEFAULTs are honored on fresh tables."
    )


def test_migration_enables_allow_column_defaults_on_iterations_first():
    """Before applying any ``ADD COLUMN … DEFAULT`` migrations to the
    iterations table, ``_migrate_add_columns`` must opt the table into
    the ``allowColumnDefaults`` feature so the ``SET DEFAULT`` step
    actually sticks on upgraded tables.

    The enable must be best-effort (try/except) so a permission denial
    does not block the rest of the migration — the column-add path still
    works via the DEFAULT-stripping fallback in ``_apply_one_migration``.
    """
    spark = _FakeSpark(
        existing_cols=[
            "run_id", "iteration", "lever", "eval_scope", "timestamp",
            "mlflow_run_id", "model_id", "overall_accuracy",
            "total_questions", "correct_count", "scores_json",
            "failures_json", "remaining_failures", "arbiter_actions_json",
            "repeatability_pct", "repeatability_json", "thresholds_met",
            "rows_json", "reflection_json", "evaluated_count",
            "excluded_count", "quarantined_benchmarks_json",
            "leakage_count_by_type", "firewall_rejection_count_by_type",
            "secondary_mining_blocked", "synthesis_slots_persisted",
            "arbiter_rejection_count",
            "cluster_fallback_to_instruction_count",
            "synthesis_archetype_distribution",
            "rolled_back", "rolled_back_at", "rollback_reason",
            "both_correct_count", "both_correct_rate",
        ],
    )

    state_mod._migrate_add_columns(spark, "cat", "sch")

    enable_stmts = [
        s for s in spark.sql_calls
        if "set tblproperties" in s.lower()
        and "allowcolumndefaults" in s.lower()
        and "genie_opt_iterations" in s.lower()
    ]
    assert enable_stmts, (
        "Expected _migrate_add_columns to issue SET TBLPROPERTIES to enable "
        "delta.feature.allowColumnDefaults on genie_opt_iterations; "
        f"observed SQL calls: {spark.sql_calls}"
    )


def test_migration_continues_when_allow_column_defaults_enable_fails():
    """If enabling ``allowColumnDefaults`` fails (e.g. permission denied),
    the rest of the migration loop must still run. The existing
    DEFAULT-stripping path in ``_apply_one_migration`` already handles
    tables without the feature, so this is strictly defense-in-depth.
    """

    class _SparkEnableFails(_FakeSpark):
        def sql(self, stmt: str):
            self.sql_calls.append(stmt)
            lower = stmt.lower()
            if "set tblproperties" in lower and "allowcolumndefaults" in lower:
                raise RuntimeError("PERMISSION_DENIED: cannot alter tblproperties")
            upper = stmt.upper().lstrip()
            if upper.startswith("DESCRIBE TABLE"):
                rows = [{"col_name": c} for c in self._existing]
                result = MagicMock()
                result.collect.return_value = rows
                return result
            return MagicMock()

    spark = _SparkEnableFails(
        existing_cols=[
            "run_id", "iteration", "lever", "eval_scope", "timestamp",
            "mlflow_run_id", "model_id", "overall_accuracy",
            "total_questions", "correct_count", "scores_json",
            "failures_json", "remaining_failures", "arbiter_actions_json",
            "repeatability_pct", "repeatability_json", "thresholds_met",
            "rows_json", "reflection_json", "evaluated_count",
            "excluded_count", "quarantined_benchmarks_json",
            "leakage_count_by_type", "firewall_rejection_count_by_type",
            "secondary_mining_blocked", "synthesis_slots_persisted",
            "arbiter_rejection_count",
            "cluster_fallback_to_instruction_count",
            "synthesis_archetype_distribution",
            "rolled_back", "rolled_back_at", "rollback_reason",
            "both_correct_count", "both_correct_rate",
        ],
    )

    state_mod._migrate_add_columns(spark, "cat", "sch")


def test_migration_verifies_target_columns_present_after_loop(caplog):
    """After the migration loop runs, the function must verify that the
    columns the writer relies on (notably ``rolled_back``) are actually
    present in the iterations table, and log a clear, loud error if any
    are missing — otherwise write_iteration fails much later with
    UNRESOLVED_COLUMN far from the root cause.
    """
    import logging

    spark = _FakeSpark(
        existing_cols=[
            # Every iteration column EXCEPT rolled_back — this is exactly
            # the state the production table was in.
            "run_id", "iteration", "lever", "eval_scope", "timestamp",
            "mlflow_run_id", "model_id", "overall_accuracy",
            "total_questions", "correct_count", "scores_json",
            "failures_json", "remaining_failures", "arbiter_actions_json",
            "repeatability_pct", "repeatability_json", "thresholds_met",
            "rows_json", "reflection_json",
            "evaluated_count", "excluded_count",
            "quarantined_benchmarks_json",
            "leakage_count_by_type", "firewall_rejection_count_by_type",
            "secondary_mining_blocked", "synthesis_slots_persisted",
            "arbiter_rejection_count",
            "cluster_fallback_to_instruction_count",
            "synthesis_archetype_distribution",
            "rolled_back_at", "rollback_reason",
            "both_correct_count", "both_correct_rate",
        ],
        add_default_error=RuntimeError(
            "DEFAULT values are not supported for new columns on existing Delta tables"
        ),
    )

    with caplog.at_level(logging.ERROR, logger=state_mod.__name__):
        state_mod._migrate_add_columns(spark, "cat", "sch")

    error_msgs = " ".join(r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)
    assert "rolled_back" in error_msgs, (
        "expected loud ERROR naming the missing 'rolled_back' column; "
        f"got: {error_msgs}"
    )


# ── GSO v2: retired-table migration skip (log-noise cleanup) ──────────────


_FULL_ITERATION_COLS = [
    "run_id", "iteration", "lever", "eval_scope", "timestamp",
    "mlflow_run_id", "model_id", "overall_accuracy",
    "total_questions", "correct_count", "scores_json",
    "failures_json", "remaining_failures", "arbiter_actions_json",
    "repeatability_pct", "repeatability_json", "thresholds_met",
    "rows_json", "reflection_json", "evaluated_count", "excluded_count",
    "quarantined_benchmarks_json", "leakage_count_by_type",
    "firewall_rejection_count_by_type", "secondary_mining_blocked",
    "synthesis_slots_persisted", "arbiter_rejection_count",
    "cluster_fallback_to_instruction_count", "synthesis_archetype_distribution",
    "rolled_back", "rolled_back_at", "rollback_reason",
    "both_correct_count", "both_correct_rate",
    # Writer-required columns verified by _verify_required_columns — include
    # them so the post-loop verify does not log a spurious [MIGRATION INCOMPLETE].
    "config_json", "is_champion",
    "num_needs_review", "eval_run_id", "eval_run_status",
]


class _SparkRetiredTablesAbsent(_FakeSpark):
    """Fake Spark where ANY statement touching a retired table raises
    TABLE_OR_VIEW_NOT_FOUND — exactly the fresh-v2-install condition that
    produced ``[MIGRATION FAILED]`` ERROR spam before the skip landed.
    """

    def __init__(self, retired: tuple[str, ...], **kwargs):
        super().__init__(**kwargs)
        self._retired = retired

    def sql(self, stmt: str):
        if any(t in stmt for t in self._retired):
            self.sql_calls.append(stmt)
            raise RuntimeError(
                "[TABLE_OR_VIEW_NOT_FOUND] The table or view cannot be found."
            )
        return super().sql(stmt)


def test_migration_skips_retired_tables_no_spam(caplog):
    """ADDITIVE_COLUMN_MIGRATIONS still lists entries for tables v2 retired
    (e.g. genie_eval_asi_results, genie_opt_data_access_grants). On a fresh v2
    install those tables don't exist, so migrating them would throw
    TABLE_OR_VIEW_NOT_FOUND and log ``[MIGRATION FAILED]``. The loop must skip
    any entry whose target table is not in the active DDL set — so no SQL is
    ever issued against a retired table and no MIGRATION FAILED is logged.
    """
    import logging

    from genie_space_optimizer.optimization.ddl import (
        ADDITIVE_COLUMN_MIGRATIONS,
        RETIRED_TABLES,
        _ALL_DDL,
    )

    # Guard: the scenario is only meaningful if some migration entry actually
    # targets a retired table.
    retired_targets = {
        table for table, _c, _d in ADDITIVE_COLUMN_MIGRATIONS
        if table in RETIRED_TABLES
    }
    assert retired_targets, (
        "expected at least one ADDITIVE_COLUMN_MIGRATIONS entry targeting a "
        "retired table for this regression to be meaningful"
    )
    assert retired_targets.isdisjoint(_ALL_DDL), (
        "retired tables must not be in the active DDL set"
    )

    spark = _SparkRetiredTablesAbsent(
        RETIRED_TABLES, existing_cols=list(_FULL_ITERATION_COLS),
    )

    with caplog.at_level(logging.ERROR, logger=state_mod.__name__):
        state_mod._migrate_add_columns(spark, "cat", "sch")

    # No SQL statement ever referenced a retired table.
    for table in RETIRED_TABLES:
        touched = [s for s in spark.sql_calls if table in s]
        assert not touched, f"retired table {table} was touched by: {touched}"

    # No MIGRATION FAILED / MIGRATION INCOMPLETE ERROR spam.
    error_msgs = " ".join(
        r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR
    )
    assert "MIGRATION FAILED" not in error_msgs, error_msgs
    assert "MIGRATION INCOMPLETE" not in error_msgs, error_msgs
