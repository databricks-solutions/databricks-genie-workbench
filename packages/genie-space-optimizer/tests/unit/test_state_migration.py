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
    schemas.
    """
    import inspect

    src = inspect.getsource(state_mod._migrate_add_columns)
    assert '"rolled_back"' in src, "migration for rolled_back missing"
    assert "BOOLEAN" in src and "DEFAULT false" in src


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
