"""Pin control-plane baseline source alignment."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness, control_plane


def test_select_control_plane_baseline_rows_exists() -> None:
    assert hasattr(control_plane, "select_control_plane_baseline_rows")


def test_harness_uses_select_control_plane_baseline_rows() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "select_control_plane_baseline_rows" in src


def test_prefers_state_iteration() -> None:
    state = {"iteration": 0, "eval_scope": "enrichment", "rows": [{"q": 1}]}
    full = {"iteration": 0, "eval_scope": "full", "rows": [{"q": 2}]}
    rows, scope = control_plane.select_control_plane_baseline_rows(
        latest_state_iteration=state, latest_full_iteration=full,
    )
    assert rows == state["rows"]
    assert scope == "enrichment"


def test_falls_back_to_full() -> None:
    full = {"iteration": 1, "eval_scope": "full", "rows": [{"q": 1}]}
    rows, scope = control_plane.select_control_plane_baseline_rows(
        latest_state_iteration=None, latest_full_iteration=full,
    )
    assert rows == full["rows"]
    assert scope == "full"


def test_handles_empty_inputs() -> None:
    rows, scope = control_plane.select_control_plane_baseline_rows(
        latest_state_iteration=None, latest_full_iteration=None,
    )
    assert rows == []
    assert scope == "unknown"
