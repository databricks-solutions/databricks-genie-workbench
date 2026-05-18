"""Dynamic reachability tracer for ``_run_lever_loop``.

Two layers:

  * ``trace_lines(target_file)``: generic context manager that
    activates ``sys.settrace`` and records the set of lines executed
    in ``target_file``. Independent of the harness; testable on its
    own with a tiny fixture function.
  * ``trace_run_lever_loop(tape, **kwargs)``: drives the real
    ``_run_lever_loop`` against an anchor tape under
    ``LeverLoopReplayHarness``, returns the set of executed lines
    inside ``harness.py``. Used by the audit script (Task 3) and
    the regression test (Task 6).

The tracer deliberately does not measure branch outcomes — only
*reachability*. That's sufficient for the named-site catalog this
audit produces.
"""
from __future__ import annotations

import contextlib
import io
import os
import sys
from contextlib import redirect_stdout
from typing import Any, Iterator
from unittest.mock import MagicMock


@contextlib.contextmanager
def trace_lines(*, target_file: str) -> Iterator[set[int]]:
    """Context manager: activates ``sys.settrace`` and records the
    set of line numbers executed in ``target_file``.

    On context exit, the yielded set is finalized — callers can read
    it via the yielded object reference (it's a mutable set during
    the context; convert to frozenset after if you want immutability).

    Restores the previous trace function on exit.
    """
    executed_lines: set[int] = set()
    target_abs = os.path.abspath(target_file)

    def _local_tracer(frame, event, arg):
        if event == "line":
            executed_lines.add(frame.f_lineno)
        return _local_tracer

    def _global_tracer(frame, event, arg):
        if event != "call":
            return None
        if os.path.abspath(frame.f_code.co_filename) != target_abs:
            return None
        return _local_tracer

    previous = sys.gettrace()
    sys.settrace(_global_tracer)
    try:
        yield executed_lines
    finally:
        sys.settrace(previous)


def _frozenize(s: set[int]) -> frozenset[int]:
    return frozenset(s)


def trace_run_lever_loop(
    *,
    tape: Any,
    run_id: str,
    space_id: str,
    domain: str,
    prev_accuracy: float,
    levers: list[int] | None = None,
    max_iterations: int | None = None,
) -> frozenset[int]:
    """Drive ``_run_lever_loop`` against the given tape under
    ``LeverLoopReplayHarness`` with line-level tracing enabled.

    Returns the frozenset of line numbers in ``harness.py`` that
    executed during the run.
    """
    from genie_space_optimizer.optimization import harness as _harness
    from genie_space_optimizer.optimization.tape_replay_harness import (
        LeverLoopReplayHarness,
    )

    harness_file = _harness.__file__
    n_iters = max_iterations or (
        max(tape.evals_by_iteration) + 1 if tape.evals_by_iteration else 4
    )

    buf = io.StringIO()
    with trace_lines(target_file=harness_file) as executed, \
            LeverLoopReplayHarness(tape=tape), \
            redirect_stdout(buf):
        _harness._run_lever_loop(
            w=MagicMock(name="w_trace"),
            spark=MagicMock(name="spark_trace"),
            run_id=run_id,
            space_id=space_id,
            domain=domain,
            benchmarks=[],
            exp_name="audit-exp",
            prev_scores={},
            prev_accuracy=prev_accuracy,
            prev_model_id="",
            config={},
            catalog="audit_catalog",
            schema="audit_schema",
            levers=levers or [5],
            max_iterations=n_iters,
            apply_mode="replay",
        )

    return _frozenize(executed)
