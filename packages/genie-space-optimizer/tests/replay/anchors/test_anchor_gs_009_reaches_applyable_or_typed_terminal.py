"""gs_009 anchor: ROW_NUMBER patch must reach at least PROPOSED or a typed terminal."""
from pathlib import Path

from tests.replay.anchors.replay_harness import run_anchor_replay


FIXTURE = Path(__file__).parent / "fixtures" / "gs_009_baseline.json"


def test_gs_009_replay_produces_trajectory():
    result = run_anchor_replay(FIXTURE)
    assert result.trajectory.qid == "gs_009"


def test_gs_009_proposal_attempt_landed():
    result = run_anchor_replay(FIXTURE)
    assert result.proposal_attempts_count >= 1, (
        "gs_009 must produce at least one ProposalAttempt — Phase 1 PR 1.5 merge gate."
    )


def test_gs_009_deepest_reached_or_typed_terminal():
    """The trajectory must reach at least PROPOSED OR end with a typed terminal record
    that is NOT a silent fallthrough.

    Phase 1's deterministic harness drives to PROPOSED via mocked diagnosis +
    clustering + synthesis (downstream gates that promote to APPLYABLE are
    not wired in Phase 1; they're Phase 2's job).
    """
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
        stage_index,
    )
    result = run_anchor_replay(FIXTURE)
    reached_proposed_or_better = stage_index(result.deepest_reached) >= stage_index(FunnelStage.PROPOSED)
    has_typed_terminal = any(
        it.terminal is not None and it.terminal.kind not in (
            "OPTIMIZER_SKIPPED_INPUT_GAP",
            "OPTIMIZER_INVARIANT_VIOLATION",
        )
        for it in result.trajectory.iterations
    )
    assert reached_proposed_or_better or has_typed_terminal, (
        f"gs_009 deepest_reached={result.deepest_reached.value} and no typed terminal; "
        "this is exactly the silent-fallthrough mode Phase 1 PR 1.5 prevents."
    )


def test_gs_009_routing_chose_lever_6():
    """Plan 12 routing must select lever 6 (structural) for plural_top_n_collapse."""
    result = run_anchor_replay(FIXTURE)
    latest = result.trajectory.iterations[-1]
    assert latest.clustered is not None
    assert latest.clustered.effective_target_lever == 6
    assert latest.clustered.routing_evidence_kind == "plural_top_n_collapse"
