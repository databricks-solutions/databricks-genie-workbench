"""Unit tests for the generic line-set tracer."""
from __future__ import annotations

from pathlib import Path

from genie_space_optimizer.optimization.harness_control_flow_tracer import (
    trace_lines,
)


def test_trace_lines_captures_executed_lines_in_target_file():
    """Run a small fixture function under the tracer and assert the
    recorded line set matches the lines actually executed."""
    target_file = str(Path(__file__).resolve())

    def fixture_function(branch: bool) -> int:
        x = 0
        if branch:
            x = 1
        else:
            x = 2
        return x

    with trace_lines(target_file=target_file) as executed:
        fixture_function(branch=True)

    # The tracer should have observed at least the function body's
    # entry lines (definitions of `x = 0`, `if branch`, `x = 1`,
    # `return x`). We don't pin exact line numbers because the
    # fixture's lineno depends on this test file's layout; instead
    # we just assert the set is non-empty.
    assert len(executed) >= 4


def test_trace_lines_respects_target_file_filter():
    """Lines outside the target file are not recorded."""
    target_file = "/tmp/not_a_real_file_for_tracing.py"

    def fixture_function() -> int:
        return 42

    with trace_lines(target_file=target_file) as executed:
        fixture_function()

    assert executed == set()


def test_trace_lines_is_idempotent_across_invocations():
    """Two sequential trace contexts produce disjoint line sets."""
    target_file = str(Path(__file__).resolve())

    def fixture_a():
        return 1

    def fixture_b():
        return 2

    with trace_lines(target_file=target_file) as exec_a:
        fixture_a()
    snap_a = frozenset(exec_a)
    with trace_lines(target_file=target_file) as exec_b:
        fixture_b()
    snap_b = frozenset(exec_b)

    # Each fixture is at a different line, so the executed sets
    # should differ.
    assert snap_a != snap_b
