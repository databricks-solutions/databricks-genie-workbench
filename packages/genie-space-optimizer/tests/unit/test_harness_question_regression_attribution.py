"""GSO v2 Phase 8: the per-question regression write is RETIRED.

``genie_eval_question_regressions`` was a subset-first-3-gate vestige tracking
slice/P0 regression rows. With full-benchmark-only eval inside the loop (arch
§7.3) the per-attempt regression truth lives on the ``genie_opt_iterations`` row
(rows_json + the loop-state ``decision`` / ``decision_reason`` columns), and
Phase 7 already dropped/renamed the table. So ``_run_lever_loop`` must no longer
build or write those rows.
"""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_question_regression_write_is_retired() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "build_question_regression_rows(" not in src, (
        "Phase 8 retires the subset-gate per-question regression rows; "
        "_run_lever_loop must not build them anymore."
    )
    assert "write_question_regressions(" not in src, (
        "Phase 8 retires the genie_eval_question_regressions write from the loop."
    )
