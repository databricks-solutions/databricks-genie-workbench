"""RCO-2b — run_lever_loop.py wires enforce_merge_gate into the
end-of-task path. Source-level structural guard."""
from __future__ import annotations

import pathlib


JOB_SRC_PATH = pathlib.Path(
    "src/genie_space_optimizer/jobs/run_lever_loop.py"
)


def test_run_lever_loop_imports_enforce_merge_gate() -> None:
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    assert (
        "from genie_space_optimizer.optimization.contract_health import"
        in src
    ), (
        "run_lever_loop.py must import from "
        "genie_space_optimizer.optimization.contract_health"
    )
    assert "enforce_merge_gate" in src, (
        "run_lever_loop.py must reference enforce_merge_gate"
    )


def test_enforce_merge_gate_is_called_before_notebook_exit() -> None:
    """The call must precede the final ``dbutils.notebook.exit(...)``
    so task values are published before the raise, but the raise
    actually marks the task failed."""
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    enforce_pos = src.find("enforce_merge_gate(loop_out)")
    assert enforce_pos > 0, (
        "run_lever_loop.py must call enforce_merge_gate(loop_out)"
    )
    final_exit_pos = src.rfind(
        "dbutils.notebook.exit(json.dumps(debug_info, default=str))"
    )
    assert final_exit_pos > 0, (
        "run_lever_loop.py must still have a final notebook.exit"
    )
    assert enforce_pos < final_exit_pos, (
        "enforce_merge_gate(loop_out) must be invoked BEFORE the final "
        "dbutils.notebook.exit(...) — otherwise the exit short-circuits "
        "the raise and the task returns success on blocked status"
    )
