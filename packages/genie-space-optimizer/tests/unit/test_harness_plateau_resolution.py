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


def test_plateau_block_passes_pending_diagnostic_ags() -> None:
    """Track G — the plateau block must pass pending_diagnostic_ags into
    the resolver so a queued AG covering a hard qid blocks termination.
    """
    src = inspect.getsource(harness._run_lever_loop)
    plateau_block = src.split("resolve_terminal_on_plateau", 1)[1].split("break", 1)[0]
    assert "pending_diagnostic_ags" in plateau_block, (
        "plateau block does not pass pending_diagnostic_ags into the resolver"
    )
    # The list must include both the buffered and diagnostic queues.
    assert "pending_action_groups" in plateau_block
    assert "diagnostic_action_queue" in plateau_block
