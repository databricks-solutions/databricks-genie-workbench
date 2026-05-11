"""RCO-2a Task 11 — structural guard that RCO-2b's posture flip has
NOT been made as part of RCO-2a.

This guard locks the scope split: RCO-2a wires the merge-gate
categories but does NOT change production behavior. The flip is
deferred to RCO-2b under the named blocker in
``docs/2026-05-12-rco-2b-deferral.md``.

When the trial blocker clears and RCO-2b actually ships, this
guard's assertions should be promoted to assertions of the new
posture; this is the marker for a real review.
"""
from __future__ import annotations

import pathlib


def test_run_lever_loop_still_pins_invariants_strict_to_off() -> None:
    """``run_lever_loop.py`` must still default ``GSO_LOOP_INVARIANTS_STRICT``
    to ``"0"``. Removing this line is an RCO-2b decision, not RCO-2a."""
    src = pathlib.Path(
        "src/genie_space_optimizer/jobs/run_lever_loop.py"
    ).read_text(encoding="utf-8")
    assert 'setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")' in src


def test_loop_invariants_strict_accessor_unchanged() -> None:
    """``loop_invariants_strict`` must still read the env var via
    ``_flag_default_on`` (its semantics, not its default, are what
    matters here — the production override pins it off)."""
    src = pathlib.Path(
        "src/genie_space_optimizer/common/config.py"
    ).read_text(encoding="utf-8")
    assert 'def loop_invariants_strict()' in src
    assert '_flag_default_on("GSO_LOOP_INVARIANTS_STRICT")' in src


def test_no_job_level_exit_code_change_for_merge_gate_blocked() -> None:
    """The lever-loop job must NOT consult ``merge_gate_status`` to drive
    its exit code in RCO-2a. RCO-2b will add that wiring."""
    job_src = pathlib.Path(
        "src/genie_space_optimizer/jobs/run_lever_loop.py"
    ).read_text(encoding="utf-8")
    # We grep for the enum-value string AND the marker name; both must
    # be absent from the job entry point in RCO-2a.
    assert "merge_gate_blocked" not in job_src
    assert "GSO_CONTRACT_HEALTH_V1" not in job_src
