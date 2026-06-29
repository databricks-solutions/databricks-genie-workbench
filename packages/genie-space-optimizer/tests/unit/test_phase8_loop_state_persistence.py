"""GSO v2 Phase 8 — per-attempt loop-state Delta persistence + checkpoint/resume.

``genie_opt_iterations`` is the single per-attempt truth (arch §7.4). Phase 8:
  * extends the DDL + additive migration with surgical_attempts_used /
    next_hypothesis / target_accuracy / max_attempts;
  * makes ``write_iteration`` actually COMMIT the loop-state columns on INSERT;
  * adds ``update_iteration_loop_state`` for the post-decision per-attempt commit
    (arch §7.5) that the controller calls at the end of each attempt; and
  * leans on the existing ``load_latest_state_iteration`` for resume (it already
    SELECT *'s, so the loop-state columns round-trip).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import ddl
from genie_space_optimizer.optimization import state as S
from genie_space_optimizer.optimization.control_plane import build_loop_state


_NEW_PHASE8_COLUMNS = (
    "surgical_attempts_used",
    "next_hypothesis",
    "target_accuracy",
    "max_attempts",
)
_PHASE7_LOOP_COLUMNS = (
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


def _capture_writes():
    captured: list[str] = []

    def _fake(spark, sql, **kwargs):  # noqa: ANN001
        captured.append(sql)

    return captured, _fake


# ── DDL + migration carry the Phase 8 loop-state columns ────────────────────
def test_iterations_ddl_declares_phase8_loop_state_columns() -> None:
    sql = ddl._GENIE_OPT_ITERATIONS_DDL
    for col in _NEW_PHASE8_COLUMNS:
        assert col in sql, f"{col} missing from genie_opt_iterations DDL"


def test_additive_migration_adds_phase8_loop_state_columns() -> None:
    iter_cols = {
        col for (tbl, col, _spec) in ddl.ADDITIVE_COLUMN_MIGRATIONS
        if tbl == ddl.TABLE_ITERATIONS
    }
    for col in _NEW_PHASE8_COLUMNS:
        assert col in iter_cols, f"{col} missing from ADDITIVE_COLUMN_MIGRATIONS"


# ── write_iteration commits the loop-state columns on INSERT ───────────────
def test_write_iteration_commits_loop_state_columns() -> None:
    captured, fake = _capture_writes()
    ls = build_loop_state(
        attempt_no=2, attempt_mode="surgical", surgical_attempts_used=1,
        max_attempts=3, target_accuracy=90.0, best_accuracy=88.5, best_iteration=2,
        current_hypothesis={"ag_id": "AG1"}, decision="accept", decision_reason="ok",
    )
    with patch.object(S, "execute_delta_write_with_retry", fake):
        S.write_iteration(
            MagicMock(), "run1", 2,
            {"overall_accuracy": 88.5, "total_questions": 30, "correct_count": 26, "scores": {}},
            catalog="c", schema="s", eval_scope="full", loop_state=ls,
        )
    sql = captured[-1]
    for col in _PHASE7_LOOP_COLUMNS + _NEW_PHASE8_COLUMNS:
        assert col in sql, f"{col} not in write_iteration INSERT"
    assert "'surgical'" in sql
    assert "90.0" in sql  # target_accuracy literal
    # best_iteration is a genie_opt_runs column, NOT an iterations column.
    assert "best_iteration" not in sql


def test_write_iteration_loop_state_columns_default_null_on_legacy_write() -> None:
    captured, fake = _capture_writes()
    with patch.object(S, "execute_delta_write_with_retry", fake):
        S.write_iteration(
            MagicMock(), "run1", 0,
            {"overall_accuracy": 80.0, "total_questions": 30, "correct_count": 24, "scores": {}},
            catalog="c", schema="s", eval_scope="full",
        )
    sql = captured[-1]
    # Columns are always listed (so the INSERT column-list matches the table).
    for col in _NEW_PHASE8_COLUMNS:
        assert col in sql
    # Legacy write ⇒ all loop-state values NULL; the row ends with NULLs.
    assert sql.rstrip().endswith("NULL)")


# ── update_iteration_loop_state: post-decision per-attempt commit (§7.5) ────
def test_update_iteration_loop_state_emits_scoped_update() -> None:
    captured, fake = _capture_writes()
    with patch.object(S, "execute_delta_write_with_retry", fake):
        S.update_iteration_loop_state(
            MagicMock(), "run1", 2, catalog="c", schema="s",
            loop_state={
                "decision": "reject",
                "decision_reason": "rolled_back (content_regression)",
                "terminal_reason": "MAX_ATTEMPTS",
                "best_accuracy": 88.5,
            },
        )
    sql = captured[-1]
    assert sql.startswith("UPDATE")
    assert "decision = 'reject'" in sql
    assert "terminal_reason = 'MAX_ATTEMPTS'" in sql
    assert "best_accuracy = 88.5" in sql
    assert "run_id = 'run1'" in sql and "iteration = 2" in sql
    # Only the provided keys are SET — additive over the candidate's eval row.
    assert "attempt_no" not in sql


def test_update_iteration_loop_state_noop_on_empty() -> None:
    captured, fake = _capture_writes()
    with patch.object(S, "execute_delta_write_with_retry", fake):
        S.update_iteration_loop_state(
            MagicMock(), "run1", 2, catalog="c", schema="s", loop_state={},
        )
    assert captured == []  # nothing to update ⇒ no SQL emitted


def test_update_iteration_loop_state_swallows_write_failure() -> None:
    def _boom(*a, **k):  # noqa: ANN002, ANN003
        raise RuntimeError("delta down")

    with patch.object(S, "execute_delta_write_with_retry", _boom):
        # Best-effort: a failed checkpoint must never abort the loop.
        S.update_iteration_loop_state(
            MagicMock(), "run1", 2, catalog="c", schema="s",
            loop_state={"decision": "accept"},
        )


# ── resume: load_latest_state_iteration round-trips loop-state columns ──────
def test_resume_round_trips_loop_state_via_select_star() -> None:
    import pandas as pd

    row = {
        "iteration": 2, "eval_scope": "full", "rolled_back": False,
        "overall_accuracy": 88.5, "scores_json": "{}",
        "attempt_no": 2, "attempt_mode": "surgical",
        "surgical_attempts_used": 1, "max_attempts": 3, "target_accuracy": 90.0,
        "decision": "accept", "terminal_reason": None,
    }
    spark = MagicMock()
    with patch.object(S, "run_query", return_value=pd.DataFrame([row])):
        out = S.load_latest_state_iteration(spark, "run1", "c", "s")
    assert out is not None
    # SELECT * means every loop-state column the controller wrote comes back, so
    # a rerun resumes from the last committed attempt.
    assert out["attempt_no"] == 2
    assert out["attempt_mode"] == "surgical"
    assert out["surgical_attempts_used"] == 1
    assert out["target_accuracy"] == 90.0
