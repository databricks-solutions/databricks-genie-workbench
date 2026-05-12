"""RCO-2b — structural guard that the production posture has been
flipped.

Replaces ``test_rco2a_strict_mode_posture_guard.py`` (which locked in
the unflipped posture). This guard asserts the three positive symptoms
of the flip:

  1. ``run_lever_loop.py`` no longer pins
     ``GSO_LOOP_INVARIANTS_STRICT`` to ``"0"`` via ``setdefault``.
  2. ``run_lever_loop.py`` references ``enforce_merge_gate`` and calls
     it before the final ``dbutils.notebook.exit(...)``.
  3. ``loop_invariants_strict()`` still reads via ``_flag_default_on``
     (its accessor shape did not change; only the runtime override
     went away).

When all three hold, RCO-2b's production posture is live.
"""
from __future__ import annotations

import pathlib


JOB_SRC_PATH = pathlib.Path(
    "src/genie_space_optimizer/jobs/run_lever_loop.py"
)
CONFIG_SRC_PATH = pathlib.Path(
    "src/genie_space_optimizer/common/config.py"
)


def test_run_lever_loop_no_longer_pins_invariants_strict_to_off() -> None:
    """The ``setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")`` override
    must be removed. Reintroducing it would silently revert RCO-2b."""
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    assert 'setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")' not in src, (
        "RCO-2b removed this override — readding it reverts the "
        "production posture flip. If a rollback is needed, set "
        "GSO_LOOP_INVARIANTS_STRICT=0 in the job widget instead."
    )


def test_run_lever_loop_enforces_merge_gate_before_exit() -> None:
    """The merge-gate enforcement must precede the final notebook
    exit. (Same assertion shape as
    test_rco2b_run_lever_loop_calls_enforce.py; duplicated here as a
    structural posture guard so deleting the wiring trips a guard
    even if the dedicated wiring test is also removed.)"""
    src = JOB_SRC_PATH.read_text(encoding="utf-8")
    assert "enforce_merge_gate(loop_out)" in src, (
        "run_lever_loop.py must call enforce_merge_gate(loop_out) — "
        "this is the merge-gate production-posture entry point"
    )
    enforce_pos = src.find("enforce_merge_gate(loop_out)")
    final_exit_pos = src.rfind(
        "dbutils.notebook.exit(json.dumps(debug_info, default=str))"
    )
    assert enforce_pos < final_exit_pos, (
        "enforce_merge_gate(loop_out) must come before the final "
        "dbutils.notebook.exit(...)"
    )


def test_loop_invariants_strict_accessor_unchanged() -> None:
    """The accessor's shape must still read via ``_flag_default_on``.
    RCO-2b removed the runtime override, not the accessor."""
    src = CONFIG_SRC_PATH.read_text(encoding="utf-8")
    assert "def loop_invariants_strict()" in src
    assert '_flag_default_on("GSO_LOOP_INVARIANTS_STRICT")' in src
