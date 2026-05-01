"""Pin plateau resolver inputs."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_uses_load_latest_state_iteration_for_plateau() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    plateau_block = src.split("resolve_terminal_on_plateau", 1)[1].split("break", 1)[0]
    assert "load_latest_state_iteration" in plateau_block


def test_passes_sql_delta_qids() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    plateau_block = src.split("resolve_terminal_on_plateau", 1)[1].split("break", 1)[0]
    assert "sql_delta_qids" in plateau_block
