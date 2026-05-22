"""Phase 1 PR 1.5 merge gate: all four anchors produce honest trajectories."""
from pathlib import Path

import pytest

from tests.replay.anchors.replay_harness import run_anchor_replay


ANCHORS = ("gs_009", "gs_024", "gs_026", "gs_021")


@pytest.mark.parametrize("anchor", ANCHORS)
def test_anchor_produces_complete_trajectory(anchor: str):
    fixture = Path(__file__).parent / "fixtures" / f"{anchor}_baseline.json"
    result = run_anchor_replay(fixture)
    # At minimum: state machine produced a non-empty trajectory with at least one transition.
    assert result.trajectory is not None
    assert len(result.trajectory.iterations) == 1
    assert len(result.trajectory.iterations[0].transitions) >= 2  # diagnosed, clustered minimum
    # For these fixtures, deepest must be >= PROPOSED.
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
        stage_index,
    )
    assert stage_index(result.deepest_reached) >= stage_index(FunnelStage.PROPOSED), (
        f"{anchor} only reached {result.deepest_reached.value}"
    )


@pytest.mark.parametrize("anchor", ANCHORS)
def test_anchor_no_silent_fallthrough(anchor: str):
    """No anchor's trajectory contains an OPTIMIZER_INVARIANT_VIOLATION terminal."""
    fixture = Path(__file__).parent / "fixtures" / f"{anchor}_baseline.json"
    result = run_anchor_replay(fixture)
    for it in result.trajectory.iterations:
        if it.terminal is not None:
            assert it.terminal.kind != "OPTIMIZER_INVARIANT_VIOLATION", (
                f"{anchor} terminated with INVARIANT_VIOLATION: {it.terminal.reason}"
            )
