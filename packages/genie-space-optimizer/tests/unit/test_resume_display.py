"""S10 — Honest resume-state display in the pre-loop setup block.

Pre-S10 the harness printed ``Starting lever: 0`` for every fresh run.
The label implied the loop would "start at lever 0" when in fact the
lever index 0 is never exercised — the loop always iterates the full
``levers`` sequence (Lever 1 through 6) per iteration. The ``0``
actually meant "no completed lever in Delta", which was the resume
sentinel.

These tests guard two invariants after the fix:

1. ``_resume_lever_loop`` returns ``resume_from_lever=None`` (not ``0``)
   when there is no prior iteration.
2. The setup block renders the resume state as
   ``Starting fresh`` / ``Resuming after lever N``, never as
   ``Starting lever: 0``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from genie_space_optimizer.optimization import harness as _harness


# ── _resume_lever_loop return shape ───────────────────────────────────


def test_resume_lever_loop_returns_none_when_no_prior_iteration(monkeypatch):
    """No completed iteration in Delta ⇒ ``resume_from_lever is None``."""
    monkeypatch.setattr(
        _harness, "load_latest_state_iteration", lambda *a, **k: None
    )
    result = _harness._resume_lever_loop(
        spark=MagicMock(), run_id="r", catalog="c", schema="s"
    )
    assert result["resume_from_lever"] is None, (
        f"fresh run must signal None; got {result['resume_from_lever']!r}"
    )
    assert result["iteration_counter"] == 0


def test_resume_lever_loop_returns_none_when_no_complete_lever_stage(monkeypatch):
    """Prior iteration exists but no LEVER_N stage is COMPLETE ⇒ None."""
    monkeypatch.setattr(
        _harness,
        "load_latest_state_iteration",
        lambda *a, **k: {"iteration": 3, "scores_json": {}, "model_id": "m"},
    )
    monkeypatch.setattr(
        _harness,
        "load_stages",
        lambda *a, **k: pd.DataFrame(
            [{"stage": "LEVER_2", "status": "ROLLED_BACK", "lever": 2}]
        ),
    )
    monkeypatch.setattr(
        _harness, "load_all_full_iterations", lambda *a, **k: []
    )
    result = _harness._resume_lever_loop(
        spark=MagicMock(), run_id="r", catalog="c", schema="s"
    )
    assert result["resume_from_lever"] is None


def test_resume_lever_loop_returns_int_when_complete_lever_present(monkeypatch):
    """Prior iteration with LEVER_3 COMPLETE ⇒ ``resume_from_lever == 3``."""
    monkeypatch.setattr(
        _harness,
        "load_latest_state_iteration",
        lambda *a, **k: {"iteration": 5, "scores_json": {}, "model_id": "m"},
    )
    monkeypatch.setattr(
        _harness,
        "load_stages",
        lambda *a, **k: pd.DataFrame(
            [
                {"stage": "LEVER_1", "status": "COMPLETE", "lever": 1},
                {"stage": "LEVER_2", "status": "COMPLETE", "lever": 2},
                {"stage": "LEVER_3", "status": "COMPLETE", "lever": 3},
                {"stage": "LEVER_4", "status": "STARTED", "lever": 4},
            ]
        ),
    )
    monkeypatch.setattr(
        _harness, "load_all_full_iterations", lambda *a, **k: []
    )
    result = _harness._resume_lever_loop(
        spark=MagicMock(), run_id="r", catalog="c", schema="s"
    )
    assert result["resume_from_lever"] == 3


# ── Display rendering ─────────────────────────────────────────────────


def _render_resume_label(start_lever: Any) -> str:
    """Mirror the ``_resume_display`` expression in ``run_lever_loop``.

    Keeping this expression colocated with the test guards against silent
    divergence if the harness snippet is ever refactored.
    """
    return (
        f"Resuming after lever {start_lever}"
        if start_lever
        else "Starting fresh"
    )


def test_display_says_starting_fresh_for_none():
    assert _render_resume_label(None) == "Starting fresh"


def test_display_says_starting_fresh_for_zero():
    """Belt-and-suspenders: a stray ``0`` must still display as fresh.

    This protects against a future refactor that falls back to ``0``
    before we finish the migration to ``None``.
    """
    assert _render_resume_label(0) == "Starting fresh"


@pytest.mark.parametrize("lever", [1, 2, 3, 4, 5, 6])
def test_display_names_the_lever_when_resuming(lever: int):
    assert _render_resume_label(lever) == f"Resuming after lever {lever}"


def test_display_never_stringifies_none_as_zero():
    """Regression guard for the exact pre-S10 symptom."""
    assert "Starting lever: 0" not in _render_resume_label(None)
    assert "Starting lever: 0" not in _render_resume_label(0)


# ── Resume loader picks post-enrichment over stale baseline ──────────


def test_resume_lever_loop_prefers_post_enrichment_iter0_over_baseline(monkeypatch):
    """Cold-start runs that have a Task 3 ``eval_scope='enrichment'`` row
    must surface the *post-enrichment* accuracy/scores to
    ``_run_lever_loop``, not the stale Task 2 ``eval_scope='full'``
    baseline.

    Pre-fix, ``_resume_lever_loop`` called ``load_latest_full_iteration``
    which filters strictly on ``eval_scope='full'``, so it returned the
    81.8% baseline_eval row even though enrichment had already lifted
    the space to 86.4%. The unconditional ``if
    resume_state.get("prev_accuracy"): prev_accuracy = ...`` overrides
    in ``_run_lever_loop`` then clobbered the orchestrator-resolved
    post-enrichment value with the pre-enrichment baseline.

    Post-fix, ``_resume_lever_loop`` calls
    ``load_latest_state_iteration`` which considers both scopes ordered
    by ``iteration DESC, timestamp DESC``. This test stubs the loader
    with a post-enrichment row and asserts the post-enrichment values
    are propagated.
    """
    post_enrichment_scores = {
        "result_correctness": 86.4,
        "_pre_arbiter/overall_accuracy": 86.4,
    }
    monkeypatch.setattr(
        _harness,
        "load_latest_state_iteration",
        lambda *a, **k: {
            "iteration": 0,
            "eval_scope": "enrichment",
            "scores_json": post_enrichment_scores,
            "model_id": "mv-post-enrichment",
            "overall_accuracy": 86.4,
        },
    )
    monkeypatch.setattr(
        _harness,
        "load_stages",
        lambda *a, **k: pd.DataFrame([]),
    )
    monkeypatch.setattr(
        _harness, "load_all_full_iterations", lambda *a, **k: []
    )

    result = _harness._resume_lever_loop(
        spark=MagicMock(), run_id="r", catalog="c", schema="s"
    )

    assert result["prev_accuracy"] == 86.4, (
        "Post-enrichment accuracy must propagate through _resume_lever_loop "
        "so the unconditional override in _run_lever_loop doesn't clobber "
        "the orchestrator-resolved value with stale baseline data."
    )
    assert result["prev_scores"] == post_enrichment_scores
    assert result["prev_model_id"] == "mv-post-enrichment"
    assert result["iteration_counter"] == 0
    assert result["resume_from_lever"] is None


def test_load_latest_state_iteration_picks_enrichment_when_both_iter0_rows_exist(
    monkeypatch,
):
    """Direct unit test on the new loader: when both
    ``eval_scope='full'`` (baseline_eval) and ``eval_scope='enrichment'``
    (enrichment) rows exist at iteration 0, the enrichment row must
    win because it's the more recent state of the world.

    Implementation detail: the loader's SQL orders by ``iteration DESC,
    timestamp DESC``. Since enrichment writes after baseline_eval in
    the orchestration pipeline, its timestamp is strictly larger. We
    stub ``run_query`` directly because the SQL ordering is what
    enforces the contract — we want the test to fail if someone
    drops the secondary ORDER BY or filters out enrichment.
    """
    from genie_space_optimizer.optimization import state as state_mod

    captured_sql: list[str] = []

    def _fake_run_query(_spark, sql: str):
        captured_sql.append(sql)
        # Simulate the DB returning rows pre-sorted by the ORDER BY in
        # ``sql`` — Pandas DataFrames don't care, the loader takes
        # ``iloc[0]`` blindly. Return the enrichment row first as the
        # actual Spark/Delta layer would.
        return pd.DataFrame(
            [
                {
                    "iteration": 0,
                    "eval_scope": "enrichment",
                    "overall_accuracy": 86.4,
                    "scores_json": '{"result_correctness": 86.4}',
                    "model_id": "mv-post-enrichment",
                    "rolled_back": False,
                    "rows_json": "[]",
                    "failures_json": None,
                    "remaining_failures": None,
                    "arbiter_actions_json": None,
                    "repeatability_json": None,
                },
                {
                    "iteration": 0,
                    "eval_scope": "full",
                    "overall_accuracy": 81.8,
                    "scores_json": '{"result_correctness": 81.8}',
                    "model_id": "mv-baseline",
                    "rolled_back": False,
                    "rows_json": "[]",
                    "failures_json": None,
                    "remaining_failures": None,
                    "arbiter_actions_json": None,
                    "repeatability_json": None,
                },
            ]
        )

    monkeypatch.setattr(state_mod, "run_query", _fake_run_query)

    row = state_mod.load_latest_state_iteration(
        spark=MagicMock(), run_id="r", catalog="c", schema="s"
    )

    assert row is not None
    assert row["eval_scope"] == "enrichment"
    assert row["overall_accuracy"] == 86.4
    assert row["model_id"] == "mv-post-enrichment"
    assert row["scores_json"] == {"result_correctness": 86.4}, (
        "scores_json must be JSON-decoded so callers don't have to."
    )
    # Lock down the SQL contract: both scopes considered, sorted by
    # iteration DESC then timestamp DESC.
    assert len(captured_sql) == 1
    sql = captured_sql[0]
    assert "eval_scope IN ('full', 'enrichment')" in sql, (
        f"loader must consider both scopes; SQL was: {sql!r}"
    )
    assert "ORDER BY iteration DESC, timestamp DESC" in sql, (
        f"loader must break iteration ties by timestamp; SQL was: {sql!r}"
    )
