"""P4 C7 unit tests — OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT outcome
and AGGREGATE_GAIN_TARGET_DEBT terminal reason."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.outcome import (
    classify_run_outcome,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


@dataclass
class _StubAccepted:
    decision: str


@dataclass
class _StubEvaluated:
    pre_apply_score: float
    post_apply_score: float


@dataclass
class _StubIteration:
    current_stage: FunnelStage
    accepted: _StubAccepted | None = None
    evaluated: _StubEvaluated | None = None
    applied: Any = None
    terminal: Any = None


@dataclass
class _StubTrajectory:
    iterations: tuple[_StubIteration, ...]
    deepest_stage_ever: FunnelStage = FunnelStage.ACCEPTED


def _accepted_trajectory_with_gain() -> _StubTrajectory:
    it = _StubIteration(
        current_stage=FunnelStage.ACCEPTED,
        accepted=_StubAccepted(decision="accepted"),
        evaluated=_StubEvaluated(pre_apply_score=0.50, post_apply_score=0.75),
    )
    return _StubTrajectory(iterations=(it,))


def test_aggregate_gain_target_debt_outcome_value():
    """Pin the literal string for marker compatibility."""
    from genie_space_optimizer.optimization.state_machine.outcome import (
        RunOutcome,
    )

    # Literal types don't have runtime members; the assertion below
    # is a contract check.
    outcome = classify_run_outcome(
        trajectories=(_accepted_trajectory_with_gain(),),
        target_qids=("gs_009",),
        target_fixed_qids=(),
    )
    assert outcome == "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"


def test_aggregate_gain_with_all_targets_fixed_returns_improved():
    outcome = classify_run_outcome(
        trajectories=(_accepted_trajectory_with_gain(),),
        target_qids=("gs_009",),
        target_fixed_qids=("gs_009",),
    )
    assert outcome == "OPTIMIZER_IMPROVED"


def test_aggregate_gain_with_partial_target_fixed_returns_debt():
    """e943 canonical case: targets {gs_009, gs_010, gs_011}; only
    gs_010, gs_011 fixed. gs_009 still hard."""
    outcome = classify_run_outcome(
        trajectories=(_accepted_trajectory_with_gain(),),
        target_qids=("gs_009", "gs_010", "gs_011"),
        target_fixed_qids=("gs_010", "gs_011"),
    )
    assert outcome == "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"


def test_aggregate_gain_without_target_set_returns_improved():
    """Pre-P4 callers that pass no target_qids fall back to legacy
    IMPROVED precedence."""
    outcome = classify_run_outcome(
        trajectories=(_accepted_trajectory_with_gain(),),
        target_qids=(),
        target_fixed_qids=(),
    )
    assert outcome == "OPTIMIZER_IMPROVED"


def test_target_debt_precedence_over_tried_insufficient_gain():
    """An accepted iteration with target debt MUST NOT be classified
    as TRIED_INSUFFICIENT_GAIN even if a later iteration was
    kept_insufficient."""
    it_accepted = _StubIteration(
        current_stage=FunnelStage.ACCEPTED,
        accepted=_StubAccepted(decision="accepted"),
        evaluated=_StubEvaluated(pre_apply_score=0.50, post_apply_score=0.75),
    )
    it_kept_insuff = _StubIteration(
        current_stage=FunnelStage.ACCEPTED,
        accepted=_StubAccepted(decision="kept_insufficient"),
    )
    traj = _StubTrajectory(iterations=(it_accepted, it_kept_insuff))
    outcome = classify_run_outcome(
        trajectories=(traj,),
        target_qids=("gs_009",),
        target_fixed_qids=(),
    )
    assert outcome == "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"


def test_terminal_reason_aggregate_gain_target_debt_value():
    assert (
        TerminalReason.AGGREGATE_GAIN_TARGET_DEBT.value
        == "aggregate_gain_target_debt"
    )


def test_no_accepted_iteration_does_not_classify_as_target_debt():
    """If no iteration was accepted, the target_debt path is not
    reachable regardless of target_qids."""
    it = _StubIteration(
        current_stage=FunnelStage.TERMINATED,
        accepted=None,
        evaluated=None,
    )
    traj = _StubTrajectory(
        iterations=(it,),
        deepest_stage_ever=FunnelStage.TERMINATED,
    )
    outcome = classify_run_outcome(
        trajectories=(traj,),
        target_qids=("gs_009",),
        target_fixed_qids=(),
    )
    assert outcome != "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"
