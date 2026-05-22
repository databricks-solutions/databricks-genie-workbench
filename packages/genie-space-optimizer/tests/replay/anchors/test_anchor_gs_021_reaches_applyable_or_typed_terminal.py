"""gs_021 anchor — MTD filter, routes to lever 6, no silent termination."""
from pathlib import Path

from tests.replay.anchors.replay_harness import run_anchor_replay


FIXTURE = Path(__file__).parent / "fixtures" / "gs_021_baseline.json"


def test_gs_021_proposal_landed_and_routed_to_lever_6():
    result = run_anchor_replay(FIXTURE)
    assert result.proposal_attempts_count >= 1
    latest = result.trajectory.iterations[-1]
    assert latest.clustered.effective_target_lever == 6


def test_gs_021_no_silent_termination():
    result = run_anchor_replay(FIXTURE)
    for it in result.trajectory.iterations:
        if it.terminal is not None:
            assert it.terminal.kind != "OPTIMIZER_INVARIANT_VIOLATION", it.terminal.reason
