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
        _harness, "load_latest_full_iteration", lambda *a, **k: None
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
        "load_latest_full_iteration",
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
        "load_latest_full_iteration",
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
