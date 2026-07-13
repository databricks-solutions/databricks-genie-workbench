"""Tests for the GSO v2 Phase 7 Delta schema reconciliation (progress §5).

Covers the locked schema decisions:
  * ADD ``genie_opt_artifacts`` scoped to the active fat-blob kinds.
  * EXTEND ``genie_opt_iterations`` with the 9 loop-state columns (DDL +
    additive migration).
  * KEEP ``genie_opt_patches`` + ``genie_eval_lever_loop_decisions``.
  * DROP the 8 retired 6-notebook tables + ``genie_opt_data_access_grants``
    (removed from ``_ALL_DDL``; one-shot rename-to-``*_deprecated`` migration).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.common.config import (
    TABLE_ARTIFACTS,
    TABLE_ITERATIONS,
    TABLE_PATCHES,
)
from genie_space_optimizer.optimization import ddl as ddl_mod
from genie_space_optimizer.optimization import state as state_mod
from genie_space_optimizer.optimization.ddl import (
    ADDITIVE_COLUMN_MIGRATIONS,
    RETIRED_TABLES,
    TABLE_LEVER_LOOP_DECISIONS,
    _ALL_DDL,
    _GENIE_OPT_ARTIFACTS_DDL,
    _GENIE_OPT_ITERATIONS_DDL,
)

_LOOP_STATE_COLUMNS = (
    "attempt_no",
    "attempt_mode",
    "best_accuracy",
    "best_config_version_id",
    "current_hypothesis",
    "do_not_repeat",
    "terminal_reason",
    "decision",
    "decision_reason",
)

_ARTIFACT_KINDS = (
    "run_manifest",
    "benchmark_qc",
    "space_quality_enrichment",
    "publish_record",
)

_RETIRED_EXPECTED = {
    "genie_eval_asi_results",
    "genie_eval_human_required",
    "genie_eval_proactive_corpus_profile",
    "genie_eval_proactive_patches",
    "genie_opt_finalize_attestation_matrix",
    "genie_opt_suggestions",
    "genie_eval_question_regressions",
    "genie_eval_gt_correction_candidates",
    "genie_opt_data_access_grants",
}


# ── genie_opt_artifacts ──────────────────────────────────────────────────


def test_artifacts_table_is_created():
    """``genie_opt_artifacts`` is registered in ``_ALL_DDL`` so fresh installs
    create it via ``ensure_optimization_tables``."""
    assert TABLE_ARTIFACTS in _ALL_DDL
    assert _ALL_DDL[TABLE_ARTIFACTS] is _GENIE_OPT_ARTIFACTS_DDL


def test_artifacts_ddl_has_required_columns_and_kinds():
    ddl = _GENIE_OPT_ARTIFACTS_DDL.lower()
    for col in (
        "run_id",
        "stage_name",
        "iteration",
        "artifact_kind",
        "artifact_json",
        "content_hash",
        "parent_artifact_id",
        "source_notebook",
        "created_at",
    ):
        assert f" {col} " in ddl or f" {col}\n" in ddl, col
    # The scoped kinds are documented in the artifact_kind comment.
    for kind in _ARTIFACT_KINDS:
        assert kind in _GENIE_OPT_ARTIFACTS_DDL


def test_artifacts_table_is_partitioned_by_run_id():
    assert "PARTITIONED BY (run_id)" in _GENIE_OPT_ARTIFACTS_DDL


# ── genie_opt_iterations loop-state columns ──────────────────────────────


def test_iterations_ddl_declares_loop_state_columns():
    """Fresh installs get the loop-state columns directly via CREATE TABLE."""
    ddl = _GENIE_OPT_ITERATIONS_DDL.lower()
    for col in _LOOP_STATE_COLUMNS:
        assert f" {col} " in ddl or f" {col}\n" in ddl, (
            f"loop-state column {col} missing from _GENIE_OPT_ITERATIONS_DDL"
        )


def test_iterations_migration_adds_loop_state_columns():
    """Existing installs get the loop-state columns via the additive
    migration list (ALTER TABLE ADD COLUMN)."""
    migrated = {
        col for (table, col, _def) in ADDITIVE_COLUMN_MIGRATIONS
        if table == TABLE_ITERATIONS
    }
    for col in _LOOP_STATE_COLUMNS:
        assert col in migrated, f"{col} not in ADDITIVE_COLUMN_MIGRATIONS"


def test_do_not_repeat_is_json_array_column():
    """``do_not_repeat`` is a JSON ARRAY (stored as a STRING JSON payload)."""
    entry = next(
        (d for (t, c, d) in ADDITIVE_COLUMN_MIGRATIONS if c == "do_not_repeat"),
        None,
    )
    assert entry is not None
    assert "STRING" in entry and "ARRAY" in entry.upper()


# ── kept tables ──────────────────────────────────────────────────────────


def test_patches_and_lever_loop_decisions_are_kept():
    assert TABLE_PATCHES in _ALL_DDL
    assert TABLE_LEVER_LOOP_DECISIONS in _ALL_DDL


# ── dropped / retired tables ─────────────────────────────────────────────


def test_retired_tables_match_spec_and_are_not_created():
    assert set(RETIRED_TABLES) == _RETIRED_EXPECTED
    for t in RETIRED_TABLES:
        assert t not in _ALL_DDL, f"retired table {t} must not be in _ALL_DDL"


# ── retire migration: rename-to-deprecated / drop-stray / skip ───────────


class _RetireFakeSpark:
    """Spark stub modelling table existence for the retire migration.

    ``existing`` is the set of fully-qualified table names that "exist".
    ``DESCRIBE TABLE x`` succeeds iff x is in ``existing``; ``ALTER ... RENAME``
    and ``DROP TABLE IF EXISTS`` mutate the set and are recorded.
    """

    def __init__(self, existing: set[str]):
        self.existing = set(existing)
        self.sql_calls: list[str] = []

    def sql(self, stmt: str):
        self.sql_calls.append(stmt)
        upper = stmt.upper().lstrip()
        if upper.startswith("DESCRIBE TABLE"):
            fqn = stmt.split("DESCRIBE TABLE", 1)[1].strip()
            result = MagicMock()
            if fqn in self.existing:
                result.collect.return_value = [{"col_name": "run_id"}]
                return result
            raise RuntimeError(f"TABLE_OR_VIEW_NOT_FOUND: {fqn}")
        if upper.startswith("ALTER TABLE") and " RENAME TO " in upper:
            left = stmt.split("ALTER TABLE", 1)[1].split(" RENAME TO ")[0].strip()
            right = stmt.split(" RENAME TO ", 1)[1].strip()
            self.existing.discard(left)
            self.existing.add(right)
            return MagicMock()
        if upper.startswith("DROP TABLE IF EXISTS"):
            fqn = stmt.split("DROP TABLE IF EXISTS", 1)[1].strip()
            self.existing.discard(fqn)
            return MagicMock()
        return MagicMock()


def _fqn(table: str) -> str:
    return f"cat.sch.{table}"


def test_retire_renames_existing_tables_to_deprecated():
    """An existing retired table with data is renamed to ``*_deprecated``
    (never hard-dropped)."""
    asi = _fqn("genie_eval_asi_results")
    spark = _RetireFakeSpark(existing={asi})

    state_mod.migrate_retire_dropped_tables(spark, "cat", "sch")

    renames = [s for s in spark.sql_calls if " RENAME TO " in s.upper()]
    assert any(
        asi in s and "genie_eval_asi_results_deprecated" in s for s in renames
    ), f"expected RENAME of {asi}; got {renames}"
    # No hard DROP of the data-bearing table.
    drops = [s for s in spark.sql_calls if s.upper().lstrip().startswith("DROP TABLE")]
    assert not any(
        s.strip().endswith(asi) for s in drops
    ), "data-bearing retired table must be renamed, not dropped"


def test_retire_is_noop_on_fresh_install():
    """Fresh install — none of the retired tables exist — issues no
    RENAME/DROP (the tables were simply never created)."""
    spark = _RetireFakeSpark(existing=set())

    state_mod.migrate_retire_dropped_tables(spark, "cat", "sch")

    mutating = [
        s for s in spark.sql_calls
        if " RENAME TO " in s.upper()
        or s.upper().lstrip().startswith("DROP TABLE")
    ]
    assert mutating == [], f"fresh install must not mutate; got {mutating}"


def test_retire_drops_stray_when_deprecated_already_exists():
    """Idempotency: if a ``*_deprecated`` copy already exists (retire ran on a
    prior deploy) and the active table reappeared, the stray active table is
    dropped — the preserved deprecated copy is left intact."""
    asi = _fqn("genie_eval_asi_results")
    asi_dep = _fqn("genie_eval_asi_results_deprecated")
    spark = _RetireFakeSpark(existing={asi, asi_dep})

    state_mod.migrate_retire_dropped_tables(spark, "cat", "sch")

    drops = [s for s in spark.sql_calls if s.upper().lstrip().startswith("DROP TABLE")]
    assert any(asi in s for s in drops), f"expected DROP of stray {asi}; got {drops}"
    # The preserved deprecated copy is never dropped.
    assert not any(asi_dep in s for s in drops)
    # And it is NOT renamed again.
    renames = [s for s in spark.sql_calls if " RENAME TO " in s.upper()]
    assert not any(asi in s and " RENAME TO " in s.upper() for s in renames)


def test_retire_is_idempotent_second_run_noop():
    """Running the migration twice converges: after the first run renames the
    table, a second run (deprecated present, active absent) does nothing."""
    asi = _fqn("genie_eval_asi_results")
    spark = _RetireFakeSpark(existing={asi})

    state_mod.migrate_retire_dropped_tables(spark, "cat", "sch")  # renames
    spark.sql_calls.clear()
    state_mod.migrate_retire_dropped_tables(spark, "cat", "sch")  # should no-op

    mutating = [
        s for s in spark.sql_calls
        if " RENAME TO " in s.upper()
        or s.upper().lstrip().startswith("DROP TABLE")
    ]
    assert mutating == [], f"second run must be a no-op; got {mutating}"


def test_retire_continues_past_a_failing_table():
    """A failure retiring one table does not abort the rest (best-effort)."""

    class _Boom(_RetireFakeSpark):
        def sql(self, stmt: str):
            if " RENAME TO " in stmt.upper() and "genie_eval_asi_results " in stmt:
                self.sql_calls.append(stmt)
                raise RuntimeError("PERMISSION_DENIED")
            return super().sql(stmt)

    asi = _fqn("genie_eval_asi_results")
    sugg = _fqn("genie_opt_suggestions")
    spark = _Boom(existing={asi, sugg})

    # Must not raise.
    state_mod.migrate_retire_dropped_tables(spark, "cat", "sch")

    # The second table is still retired despite the first failing.
    renames = [s for s in spark.sql_calls if " RENAME TO " in s.upper()]
    assert any(sugg in s for s in renames)


# ── write_artifact ───────────────────────────────────────────────────────


def test_write_artifact_inserts_row_with_kind_and_hash(monkeypatch):
    captured = {}

    def fake_insert_row(spark, catalog, schema, table, payload):
        captured["table"] = table
        captured["payload"] = payload

    monkeypatch.setattr(state_mod, "insert_row", fake_insert_row)
    spark = MagicMock()

    artifact_id = state_mod.write_artifact(
        spark, "run-1", "run_manifest", {"config_hash": "abc"},
        catalog="cat", schema="sch", stage_name="intake_and_snapshot",
        source_notebook="run_intake_and_snapshot.py",
    )

    assert artifact_id is not None
    assert captured["table"] == TABLE_ARTIFACTS
    p = captured["payload"]
    assert p["run_id"] == "run-1"
    assert p["artifact_kind"] == "run_manifest"
    assert p["content_hash"]  # hash computed for non-null payload
    assert '"config_hash": "abc"' in p["artifact_json"]
    assert p["stage_name"] == "intake_and_snapshot"


def test_write_artifact_swallows_write_failure(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("delta unavailable")

    monkeypatch.setattr(state_mod, "insert_row", boom)
    # Best-effort: returns None rather than raising.
    assert state_mod.write_artifact(
        MagicMock(), "run-1", "benchmark_qc", {"x": 1}, catalog="c", schema="s",
    ) is None


def test_artifact_kinds_constant_matches_spec():
    assert set(state_mod.ARTIFACT_KINDS) == set(_ARTIFACT_KINDS)


def test_publish_record_payload_shape_round_trips_through_write_artifact(monkeypatch):
    """Phase 9: the real ``publish_record`` payload (champion pointer, stamped
    terminal_reason, published flag, LLM audit summary, improvement trajectory,
    concerns) is a valid ``publish_record`` artifact and serializes intact —
    superseding the Phase-7 shell's None/[] placeholders + 'Phase 7 shell' note."""
    from genie_space_optimizer.optimization.publish import build_publish_record

    captured = {}

    def fake_insert_row(spark, catalog, schema, table, payload):
        captured["table"] = table
        captured["payload"] = payload

    monkeypatch.setattr(state_mod, "insert_row", fake_insert_row)

    record = build_publish_record(
        run_id="run-1", space_id="space-1", run_status="CONVERGED",
        terminal_reason="TARGET_REACHED", published=True, publish_outcome="published",
        champion_iteration=2, champion_accuracy=91.0,
        champion_config_version_id="cfg-v2", target_accuracy=90.0, max_attempts=3,
        audit_summary="Baseline 80% climbed to 91%; champion iter 2 published.",
        improvement_trajectory=[{"iteration": 0, "accuracy": 80.0}],
        concerns=[],
    )
    # The Phase-9 payload carries the real fields, not the shell placeholders.
    for field in (
        "final_status", "terminal_reason", "published", "publish_outcome",
        "champion_iteration", "champion_accuracy", "champion_config_version_id",
        "audit_summary", "improvement_trajectory", "concerns",
    ):
        assert field in record
    assert "note" not in record  # no 'Phase 7 shell' placeholder note

    aid = state_mod.write_artifact(
        MagicMock(), "run-1", "publish_record", record,
        catalog="c", schema="s", stage_name="PUBLISH_AND_AUDIT",
        source_notebook="run_publish_and_audit.py",
    )
    assert aid is not None
    assert captured["table"] == TABLE_ARTIFACTS
    assert captured["payload"]["artifact_kind"] == "publish_record"
    assert captured["payload"]["content_hash"]
    assert '"terminal_reason": "TARGET_REACHED"' in captured["payload"]["artifact_json"]


# ── ensure_optimization_tables wires the retire migration ────────────────


def test_ensure_optimization_tables_invokes_retire(monkeypatch):
    calls = {"retire": 0, "migrate": 0}

    monkeypatch.setattr(
        state_mod, "_migrate_add_columns",
        lambda *a, **k: calls.__setitem__("migrate", calls["migrate"] + 1),
    )
    monkeypatch.setattr(
        state_mod, "migrate_retire_dropped_tables",
        lambda *a, **k: calls.__setitem__("retire", calls["retire"] + 1),
    )
    spark = MagicMock()

    state_mod.ensure_optimization_tables(spark, "cat", "sch")

    assert calls["migrate"] == 1
    assert calls["retire"] == 1
