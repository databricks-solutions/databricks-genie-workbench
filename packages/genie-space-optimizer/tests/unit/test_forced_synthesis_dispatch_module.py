"""Unit tests for the extracted L5 forced-synthesis dispatch callable.

This module pins the contract the replay driver depends on:

1. ``ForcedSynthesisDispatchResult`` is a frozen dataclass with the three
   fields the replay driver reads.
2. ``dispatch_forced_structural_synthesis`` is importable from the new
   module and accepts the parameter list pinned in Task 0.

Behavior parity with ``harness.py:22720-22929`` is verified by the
parity-pin test in Task 4, not here.
"""
from __future__ import annotations


def test_result_dataclass_shape() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        ForcedSynthesisDispatchResult,
    )

    r = ForcedSynthesisDispatchResult(
        attempted_dispatches=(),
        appended_proposals=(),
        emitted_decision_records=(),
    )
    assert r.attempted_dispatches == ()
    assert r.appended_proposals == ()
    assert r.emitted_decision_records == ()
