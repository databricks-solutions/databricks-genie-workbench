"""classify_run_outcome — trajectories -> OPTIMIZER_* outcome string."""
from __future__ import annotations

from typing import Literal

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.trajectory import (
    QuestionTrajectory,
)


RunOutcome = Literal[
    "OPTIMIZER_IMPROVED",
    "OPTIMIZER_TRIED_NO_GAIN",
    "OPTIMIZER_STALLED_NO_APPLIED_PATCHES",
    "OPTIMIZER_NO_CANDIDATES",
    "OPTIMIZER_SKIPPED_INPUT_GAP",
    "OPTIMIZER_STALLED_SAFE_NOOP",
    "OPTIMIZER_INVARIANT_VIOLATION",
]


def classify_run_outcome(
    trajectories: tuple[QuestionTrajectory, ...],
    *,
    hard_rows_in_eval: bool = False,
) -> RunOutcome:
    """Classify the run-level outcome from the iteration trajectories.

    Precedence (highest first):
      1. Invariant violation — any trajectory with current_stage not in
         {ACCEPTED, TERMINATED} on its latest iteration (SM1 violation).
      2. Skipped input gap — eval had hard rows but no trajectories created.
      3. Improved — any trajectory accepted with positive accuracy delta.
      4. Tried no gain — any trajectory reached APPLIED but rolled back.
      5. Stalled no applied — any trajectory deepest in [PROPOSED, APPLYABLE].
      6. Stalled safe noop — all trajectories terminated via the escalation ladder.
      7. No candidates — all trajectories deepest below PROPOSED.
    """
    if not trajectories and hard_rows_in_eval:
        return "OPTIMIZER_SKIPPED_INPUT_GAP"

    # SM1 check
    for traj in trajectories:
        latest = traj.iterations[-1]
        if latest.current_stage not in (FunnelStage.ACCEPTED, FunnelStage.TERMINATED):
            return "OPTIMIZER_INVARIANT_VIOLATION"

    # Improved
    for traj in trajectories:
        for it in traj.iterations:
            if (
                it.accepted is not None
                and it.evaluated is not None
                and it.evaluated.post_apply_score > it.evaluated.pre_apply_score
            ):
                return "OPTIMIZER_IMPROVED"

    # Tried no gain
    for traj in trajectories:
        for it in traj.iterations:
            if (
                it.applied is not None
                and it.terminal is not None
                and it.terminal.kind == "OPTIMIZER_TRIED_NO_GAIN"
            ):
                return "OPTIMIZER_TRIED_NO_GAIN"
            if (
                it.applied is not None
                and it.accepted is None
                and it.terminal is not None
            ):
                return "OPTIMIZER_TRIED_NO_GAIN"

    # Stalled no applied patches
    propose_or_deeper = (
        FunnelStage.PROPOSED,
        FunnelStage.NORMALIZED,
        FunnelStage.APPLYABLE,
    )
    for traj in trajectories:
        if traj.deepest_stage_ever in propose_or_deeper:
            # If every terminal record is OPTIMIZER_STALLED_SAFE_NOOP, classify as that instead.
            terminals = [it.terminal for it in traj.iterations if it.terminal is not None]
            if terminals and all(t.kind == "OPTIMIZER_STALLED_SAFE_NOOP" for t in terminals):
                continue
            return "OPTIMIZER_STALLED_NO_APPLIED_PATCHES"

    # Stalled safe noop
    for traj in trajectories:
        for it in traj.iterations:
            if it.terminal is not None and it.terminal.kind == "OPTIMIZER_STALLED_SAFE_NOOP":
                return "OPTIMIZER_STALLED_SAFE_NOOP"

    # No candidates
    return "OPTIMIZER_NO_CANDIDATES"
