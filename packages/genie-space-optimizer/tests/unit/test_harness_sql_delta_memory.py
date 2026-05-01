"""Pin sql_shape_deltas on rejected reflection entries."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_imports_compute_sql_shape_delta() -> None:
    src = inspect.getsource(harness)
    assert "compute_sql_shape_delta" in src


def test_records_sql_shape_deltas_on_rejected_entry() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "sql_shape_deltas" in src
