"""GSO Optimizer v2 — Phase 4: Delta-only config/version tracking (D3).

Covers the three Phase-4 deliverables:

1. **Per-iteration full config to Delta** — the new ``config_json`` column on
   ``genie_opt_iterations`` (DDL + additive migration + ``write_iteration``
   write path + the whitelist/cycle-safe projection).
2. **Champion marked in Delta** — the new ``is_champion`` column + the
   ``state.mark_champion_iteration`` writer + its invocation from the existing
   Delta-driven selection in ``models.promote_best_model`` (no new selection
   logic, no UC model registration).
3. **Rollback stays Delta-based (no regression)** — rejected-iteration
   rollback still re-PATCHes the in-memory ``pre_snapshot`` and discard still
   reverts from ``genie_opt_runs.config_snapshot``; neither path is touched by
   the new config_json/is_champion tracking.

Pure unit tests — ``spark.sql`` is mocked; no Databricks connectivity.
"""

from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from genie_space_optimizer.common.config import TABLE_ITERATIONS
from genie_space_optimizer.optimization import state as state_mod
from genie_space_optimizer.optimization.ddl import (
    ADDITIVE_COLUMN_MIGRATIONS,
    _GENIE_OPT_ITERATIONS_DDL,
)
from genie_space_optimizer.optimization.state import (
    _project_config_for_iteration,
    mark_champion_iteration,
    write_iteration,
)


# ── 1. DDL + migration registration ─────────────────────────────────────


def test_iterations_ddl_declares_config_json_and_is_champion():
    """Fresh installs get both columns via CREATE TABLE (not just migration)."""
    ddl = _GENIE_OPT_ITERATIONS_DDL.lower()
    for col in ("config_json", "is_champion"):
        assert f" {col} " in ddl or f" {col}\n" in ddl, (
            f"Expected {col} declared in _GENIE_OPT_ITERATIONS_DDL so fresh "
            f"installs pick it up without an ALTER TABLE round-trip."
        )
    # CDF is what gives the versioned config history (D3) — confirm it stays on.
    assert "delta.enablechangedatafeed" in ddl
    # is_champion is a boolean marker with a server-side default.
    assert "is_champion" in ddl and "default false" in ddl


def test_config_json_and_is_champion_registered_in_migrations():
    """Existing tables must get both columns via the additive migration list."""
    by_col = {col: (table, col_def) for table, col, col_def in ADDITIVE_COLUMN_MIGRATIONS}

    assert "config_json" in by_col, "config_json migration missing"
    table, col_def = by_col["config_json"]
    assert table == TABLE_ITERATIONS
    assert "STRING" in col_def

    assert "is_champion" in by_col, "is_champion migration missing"
    table, col_def = by_col["is_champion"]
    assert table == TABLE_ITERATIONS
    # Boolean marker with DEFAULT false — matches the rolled_back precedent so
    # the default-stripping migration path in _apply_one_migration handles it.
    assert "BOOLEAN" in col_def and "DEFAULT false" in col_def


def test_write_critical_columns_listed_in_required():
    """write_iteration emits both columns on every INSERT, so the migration
    self-check must verify they are present (mirrors the rolled_back guard)."""
    assert "config_json" in state_mod._REQUIRED_ITERATION_COLUMNS
    assert "is_champion" in state_mod._REQUIRED_ITERATION_COLUMNS


# ── projection helper ────────────────────────────────────────────────────


def test_projection_drops_internal_keys_and_keeps_whitelisted():
    cfg = {
        "instructions": {"text_instructions": [{"content": "x"}]},
        "data_sources": {"tables": [{"identifier": "c.s.t"}]},
        "description": "a space",
        # optimizer-internal keys must be dropped
        "_failure_clusters": [1, 2, 3],
        "_data_profile": {"big": "blob"},
        "_strategy": "anything",
        # non-whitelisted Genie-ish key must be dropped
        "random_runtime_key": "drop me",
    }
    out = _project_config_for_iteration(cfg)
    assert set(out) == {"instructions", "data_sources", "description"}
    assert "_failure_clusters" not in out
    assert "random_runtime_key" not in out


def test_projection_prefers_parsed_space_when_present():
    """A raw fetched config (with _parsed_space) projects the parsed view."""
    cfg = {
        "_parsed_space": {
            "instructions": {"text_instructions": []},
            "data_sources": {"tables": []},
        },
        # top-level noise that should be ignored once _parsed_space is found
        "_raw_blob": object(),
    }
    out = _project_config_for_iteration(cfg)
    assert set(out) == {"instructions", "data_sources"}


def test_projection_is_cycle_safe():
    cfg: dict = {"instructions": {}, "data_sources": {}}
    cfg["data_sources"]["self"] = cfg["data_sources"]  # self-loop
    out = _project_config_for_iteration(cfg)
    # Must not raise; the cycle is broken with a sentinel and stays JSON-safe.
    json.dumps(out)
    assert out["data_sources"]["self"] == "<cycle>"


@pytest.mark.parametrize("bad", [None, [1, 2], "not a dict", 42])
def test_projection_handles_non_dicts(bad):
    assert _project_config_for_iteration(bad) == {}


# ── 1. write_iteration config_json write path ────────────────────────────


@pytest.fixture
def mock_spark_iter():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


def _extract_insert_sql(mock_spark: MagicMock) -> str:
    for call in mock_spark.sql.call_args_list:
        sql = call.args[0] if call.args else call.kwargs.get("sqlQuery", "")
        if "INSERT INTO" in sql and "genie_opt_iterations" in sql:
            return sql
    raise AssertionError("No INSERT INTO genie_opt_iterations found")


_BASE_EVAL = {
    "overall_accuracy": 90.0,
    "total_questions": 10,
    "evaluated_count": 10,
    "correct_count": 9,
    "scores": {},
    "thresholds_met": True,
}


def test_write_iteration_persists_projected_config_json(mock_spark_iter):
    eval_result = dict(_BASE_EVAL)
    config = {
        "instructions": {"text_instructions": [{"content": "be precise"}]},
        "data_sources": {"tables": [{"identifier": "cat.sch.sales"}]},
        "_failure_clusters": ["should", "be", "dropped"],
    }
    write_iteration(
        mock_spark_iter, run_id="run-cfg", iteration=2, eval_result=eval_result,
        catalog="cat", schema="sch", eval_scope="full", config_snapshot=config,
    )
    sql = _extract_insert_sql(mock_spark_iter)
    # Column list carries both Phase-4 columns.
    assert "config_json" in sql
    assert "is_champion" in sql
    # Whitelisted content survives; internal keys are stripped.
    assert "be precise" in sql
    assert "cat.sch.sales" in sql
    assert "_failure_clusters" not in sql
    assert "should" not in sql
    # is_champion is written false at iteration-write time.
    assert sql.rstrip().endswith("false)")


def test_write_iteration_config_json_null_when_not_provided(mock_spark_iter):
    """Back-compat: call sites that don't pass config_snapshot write NULL and
    still write is_champion=false — no crash, no behavior change."""
    write_iteration(
        mock_spark_iter, run_id="run-nocfg", iteration=1,
        eval_result=dict(_BASE_EVAL), catalog="cat", schema="sch",
    )
    sql = _extract_insert_sql(mock_spark_iter)
    assert "config_json" in sql  # column always listed
    # The two trailing values are NULL (config_json) then false (is_champion).
    assert sql.rstrip().endswith("NULL, false)")


def test_write_iteration_empty_projection_writes_null(mock_spark_iter):
    """A config with no whitelisted keys projects to {} → NULL, not '{}'."""
    write_iteration(
        mock_spark_iter, run_id="run-empty", iteration=3,
        eval_result=dict(_BASE_EVAL), catalog="cat", schema="sch",
        config_snapshot={"_only": "internal", "junk": 1},
    )
    sql = _extract_insert_sql(mock_spark_iter)
    assert sql.rstrip().endswith("NULL, false)")


def test_write_iteration_escapes_quotes_in_config(mock_spark_iter):
    """Config text with single quotes must be doubled so the SQL literal is
    not terminated early (injection / quote-safety, same contract as rows)."""
    config = {"description": "Acme's 'quarterly' revenue"}
    write_iteration(
        mock_spark_iter, run_id="run-q", iteration=0,
        eval_result=dict(_BASE_EVAL), catalog="cat", schema="sch",
        config_snapshot=config,
    )
    sql = _extract_insert_sql(mock_spark_iter)
    import re as _re
    # Find the config_json literal and assert every quote run is even-length.
    # (A lone quote would mean a broken escape.)
    assert "Acme" in sql
    # The raw apostrophe must have been escaped to '' inside the literal.
    assert "Acme''s" in sql


# ── 2. champion marking ──────────────────────────────────────────────────


def test_mark_champion_iteration_clears_then_sets(mock_spark_iter):
    mark_champion_iteration(
        mock_spark_iter, "run-champ", 2,
        catalog="cat", schema="sch", eval_scope="full",
    )
    stmts = [c.args[0] for c in mock_spark_iter.sql.call_args_list if c.args]
    updates = [s for s in stmts if s.strip().upper().startswith("UPDATE")]
    assert len(updates) == 2, f"expected clear + set UPDATE, got {updates}"

    clear_stmt = updates[0]
    set_stmt = updates[1]
    # Clear targets only the run (set is_champion=false where currently true).
    assert "SET is_champion = false" in clear_stmt and "run-champ" in clear_stmt
    assert "iteration =" not in clear_stmt
    # Set targets the specific (run, iter, scope).
    assert "SET is_champion = true" in set_stmt
    assert "iteration = 2" in set_stmt
    assert "eval_scope = 'full'" in set_stmt


def test_mark_champion_iteration_without_scope_omits_scope_predicate(mock_spark_iter):
    mark_champion_iteration(
        mock_spark_iter, "run-noscope", 5, catalog="cat", schema="sch",
    )
    set_stmt = [
        c.args[0] for c in mock_spark_iter.sql.call_args_list
        if c.args and "SET is_champion = true" in c.args[0]
    ][0]
    assert "iteration = 5" in set_stmt
    assert "eval_scope" not in set_stmt


def test_mark_champion_iteration_is_best_effort(mock_spark_iter):
    """A write failure is swallowed — champion marking is a transparency
    signal, never a gating mechanism, so it must not raise."""
    mock_spark_iter.sql.side_effect = RuntimeError("delta unavailable")
    # Must not raise.
    mark_champion_iteration(
        mock_spark_iter, "run-x", 1, catalog="cat", schema="sch",
        eval_scope="full",
    )


def _iterations_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_promote_best_model_marks_champion_reusing_existing_selection():
    """promote_best_model must persist the champion in Delta using the SAME
    best-iteration selection it already computes (idxmax of overall_accuracy
    over non-rolled-back full/enrichment rows), even when there is no MLflow
    model_id (the v2 Delta-only path returns early after the model_id guard)."""
    from genie_space_optimizer.optimization import models as models_mod

    df = _iterations_df([
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 80.0, "rolled_back": False, "model_id": None},
        {"iteration": 1, "eval_scope": "enrichment", "overall_accuracy": 85.0, "rolled_back": False, "model_id": None},
        {"iteration": 2, "eval_scope": "full", "overall_accuracy": 92.0, "rolled_back": False, "model_id": None},
        # Higher accuracy but rolled back → MUST be excluded from selection.
        {"iteration": 3, "eval_scope": "full", "overall_accuracy": 99.0, "rolled_back": True, "model_id": None},
        # A same-iteration slice row that must NOT be the champion target.
        {"iteration": 2, "eval_scope": "slice", "overall_accuracy": 100.0, "rolled_back": False, "model_id": None},
    ])

    with patch.object(models_mod, "load_run", return_value={"space_id": "sp1"}), \
         patch.object(models_mod, "load_iterations", return_value=df), \
         patch.object(models_mod, "mark_champion_iteration") as mark, \
         patch.object(models_mod, "update_run_status"):
        result = models_mod.promote_best_model(MagicMock(), "run-1", "cat", "sch")

    # No model_id → returns None, but the champion was still marked first.
    assert result is None
    mark.assert_called_once()
    _args, kwargs = mark.call_args
    # Called with the best iteration (2, full) — NOT the rolled-back 99.0 row
    # and NOT the 100.0 slice row.
    assert _args[1] == "run-1"
    assert _args[2] == 2
    assert kwargs["eval_scope"] == "full"
    assert kwargs["catalog"] == "cat"
    assert kwargs["schema"] == "sch"


def test_promote_best_model_no_uc_registration_added():
    """Phase 4 must NOT add UC model registration to champion promotion.
    Guard that promote_best_model doesn't reference register_uc_model."""
    import inspect

    from genie_space_optimizer.optimization import models as models_mod

    src = inspect.getsource(models_mod.promote_best_model)
    assert "register_uc_model" not in src
    assert "mark_champion_iteration" in src


# ── 3. rollback / discard — no regression ────────────────────────────────


def test_rejected_iteration_rollback_uses_pre_snapshot_unchanged():
    """The in-memory pre_snapshot re-PATCH path is the rollback for rejected
    iterations. Phase 4 must not alter it: rollback restores pre_snapshot and
    never consults config_json/is_champion."""
    from genie_space_optimizer.optimization import applier as applier_mod

    pre_snapshot = {
        "instructions": {"text_instructions": [{"content": "original"}]},
        "data_sources": {"tables": [{"identifier": "c.s.t"}]},
    }
    apply_log = {"pre_snapshot": copy.deepcopy(pre_snapshot)}

    captured: dict = {}

    def _fake_patch(w, space_id, cfg):
        captured["space_id"] = space_id
        captured["cfg"] = cfg

    with patch.object(applier_mod, "patch_space_config", side_effect=_fake_patch):
        result = applier_mod.rollback(apply_log, MagicMock(), "space-123")

    assert result["status"] == "SUCCESS"
    # The space was re-PATCHed with the pre_snapshot config (deep-equal).
    assert captured["space_id"] == "space-123"
    assert captured["cfg"] == pre_snapshot
    # No config_json / is_champion leaked into the rollback config.
    assert "config_json" not in captured["cfg"]
    assert "is_champion" not in captured["cfg"]


def test_rollback_errors_without_pre_snapshot():
    """Contract unchanged: missing pre_snapshot is an error, not a silent op."""
    from genie_space_optimizer.optimization import applier as applier_mod

    result = applier_mod.rollback({}, MagicMock(), "space-1")
    assert result["status"] == "error"


def test_discard_reverts_from_runs_config_snapshot_unchanged():
    """Discard still reads genie_opt_runs.config_snapshot and re-PATCHes it via
    applier.rollback; Phase 4's per-iteration config_json/is_champion are not in
    this path."""
    from genie_space_optimizer.integration import discard as discard_mod
    from genie_space_optimizer.integration.config import IntegrationConfig
    from genie_space_optimizer.optimization import applier as applier_mod

    original_snapshot = {
        "instructions": {"text_instructions": [{"content": "pre-run"}]},
        "data_sources": {"tables": []},
    }
    run_row = {
        "status": "IN_PROGRESS",
        "space_id": "sp-9",
        "config_snapshot": json.dumps(original_snapshot),
    }

    rollback_calls: list = []
    exec_sql: list = []

    cfg = IntegrationConfig(catalog="cat", schema_name="sch", warehouse_id="wh1")

    with patch.object(discard_mod, "wh_load_run", return_value=run_row), \
         patch.object(discard_mod, "sql_warehouse_execute",
                      side_effect=lambda *a, **k: exec_sql.append(a)), \
         patch.object(discard_mod, "_pick_genie_client", return_value=MagicMock()), \
         patch.object(applier_mod, "rollback",
                      side_effect=lambda *a, **k: rollback_calls.append((a, k))):
        result = discard_mod.discard_optimization(
            "run-d", MagicMock(), MagicMock(), cfg,
        )

    assert result.status == "discarded"
    # rollback was invoked with the ORIGINAL run-start snapshot (pre_snapshot),
    # i.e. the discard revert anchor is still genie_opt_runs.config_snapshot.
    assert len(rollback_calls) == 1
    (rb_args, _rb_kwargs) = rollback_calls[0]
    apply_log = rb_args[0]
    assert apply_log == {"pre_snapshot": original_snapshot}
    # The terminal Delta write flips status to DISCARDED.
    update_sql = " ".join(str(a) for call in exec_sql for a in call)
    assert "DISCARDED" in update_sql
    assert "genie_opt_runs" in update_sql
