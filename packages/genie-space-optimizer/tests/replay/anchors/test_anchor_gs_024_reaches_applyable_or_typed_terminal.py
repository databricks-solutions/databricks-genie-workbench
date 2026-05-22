"""gs_024 anchor: missing-filter patch must reach at least PROPOSED or a typed terminal."""
from pathlib import Path

from tests.replay.anchors.replay_harness import run_anchor_replay


FIXTURE = Path(__file__).parent / "fixtures" / "gs_024_baseline.json"


def test_gs_024_proposal_attempt_landed():
    result = run_anchor_replay(FIXTURE)
    assert result.proposal_attempts_count >= 1


def test_gs_024_routing_chose_lever_6_for_missing_filter():
    result = run_anchor_replay(FIXTURE)
    latest = result.trajectory.iterations[-1]
    assert latest.clustered is not None
    assert latest.clustered.effective_target_lever == 6
    assert latest.clustered.routing_evidence_kind == "missing_filter"


def test_gs_024_no_silent_termination():
    """gs_024 must NOT terminate with INVARIANT_VIOLATION or empty evidence_kind."""
    result = run_anchor_replay(FIXTURE)
    for it in result.trajectory.iterations:
        if it.terminal is not None:
            assert it.terminal.kind != "OPTIMIZER_INVARIANT_VIOLATION", it.terminal.reason
