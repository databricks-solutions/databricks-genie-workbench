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
    # P4 C7 — aggregate accuracy gain landed but the target QID set
    # still has hard rows. Distinct from OPTIMIZER_IMPROVED so the
    # harness loop-control does not terminate when budget remains and
    # target debt persists. Triggered when an accepted iteration has
    # target_fixed_qids ⊊ target_qids. See ``classify_run_outcome``
    # and the e943 postmortem ("aggregate gain accepted; gs_009 stays
    # hard"). Precedence sits between IMPROVED and
    # TRIED_INSUFFICIENT_GAIN: a run that fixed some-but-not-all
    # targets is classified as AGGREGATE_GAIN_TARGET_DEBT regardless
    # of whether the overall accuracy moved positive.
    "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT",
    # Trial 18 Step 3 — middle-ground outcome: no ACCEPTED, at least
    # one KEPT_INSUFFICIENT, no terminal collateral regressions on the
    # target side. Distinct from ``OPTIMIZER_TRIED_NO_GAIN`` so
    # dashboards can see "cumulative learning landed but the headline
    # didn't move" without conflating it with "every applied patch
    # rolled back".
    "OPTIMIZER_TRIED_INSUFFICIENT_GAIN",
    "OPTIMIZER_TRIED_NO_GAIN",
    "OPTIMIZER_STALLED_NO_APPLIED_PATCHES",
    "OPTIMIZER_NO_CANDIDATES",
    "OPTIMIZER_SKIPPED_INPUT_GAP",
    "OPTIMIZER_STALLED_SAFE_NOOP",
    "OPTIMIZER_INVARIANT_VIOLATION",
]


# Trial 21 W8+C7 — the predicate that gates
# OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT must accept the post-Trial-19
# attribution-drift decision label too. Run A's attribution_drift
# accept missed this branch in the previous shape — the accepted
# decision was ``"accepted_with_attribution_drift"`` (not ``"accepted"``)
# so the gain-with-debt branch never fired and the run misclassified
# as OPTIMIZER_TRIED_INSUFFICIENT_GAIN. Both decision values must
# count as "an aggregate-positive accept landed".
_ACCEPTED_DECISIONS_FOR_AGGREGATE_GAIN = frozenset(
    {
        "accepted",
        "accepted_with_attribution_drift",
    }
)


# d139 postmortem — a blast-radius rejection cycles the QID
# NORMALIZED -> PROPOSED (so Phase 3 escalation can try a narrower
# artifact). When no narrower candidate materializes that iteration,
# the QID legitimately ends in PROPOSED with a deliberate gate
# rejection on its latest attempt. That is a SETTLED, honest
# no-candidate terminal-equivalent — NOT an SM1 invariant breakage.
# Treating it as OPTIMIZER_INVARIANT_VIOLATION masked the run's real
# story behind a spurious violation. Membership here means "a PROPOSED
# state with this latest-attempt outcome is settled for SM1 purposes".
_SETTLED_PROPOSED_REJECTION_OUTCOMES = frozenset(
    {
        "blast_radius_rejected",
    }
)


def _is_settled_for_sm1(latest: "QuestionStateInIteration") -> bool:
    """True when ``latest`` is a terminal-equivalent for the SM1 check.

    ACCEPTED / TERMINATED are always settled. A PROPOSED state whose
    latest proposal attempt was a deliberate gate rejection (e.g.
    ``blast_radius_rejected``) is also settled — it is an honest
    "tried, gate-blocked, no narrower candidate this iteration"
    outcome rather than a state-machine breakage.
    """
    if latest.current_stage in (FunnelStage.ACCEPTED, FunnelStage.TERMINATED):
        return True
    if (
        latest.current_stage == FunnelStage.PROPOSED
        and latest.proposals
        and str(latest.proposals[-1].outcome)
        in _SETTLED_PROPOSED_REJECTION_OUTCOMES
    ):
        return True
    return False


def classify_run_outcome_from_aggregates(
    *,
    any_iteration_accepted: bool,
    any_iteration_post_gt_pre: bool,
    last_accepted_decision: str,
    target_qids: tuple[str, ...] = (),
    target_fixed_qids: tuple[str, ...] = (),
    target_still_hard_qids: tuple[str, ...] = (),
) -> str:
    """Trial 21 W8+C7 — scalar-input outcome classifier.

    Stripped-down companion to :func:`classify_run_outcome` that
    consumes already-extracted aggregates from the iteration ledger
    instead of the full ``QuestionTrajectory`` graph. The harness
    threads these scalars onto the full-eval marker; the Trial 21
    Evidence Actuator and the postmortem-replay suite read the same
    scalars to verify the gain-with-target-debt branch fires.

    The predicate set is identical to the trajectory-based classifier
    so behaviour does not drift:

      * Accept-with-gain  iff  ``any_iteration_accepted=True`` AND
        ``last_accepted_decision`` is one of
        ``{"accepted", "accepted_with_attribution_drift"}`` AND
        ``any_iteration_post_gt_pre=True``.
      * Gain-with-target-debt fires when accept-with-gain AND the
        target QID set is non-empty AND
        ``target_fixed_qids`` is a strict subset of ``target_qids``
        OR ``target_still_hard_qids`` is non-empty.

    Returns one of the :data:`RunOutcome` strings.
    """
    decision = str(last_accepted_decision or "").strip()
    accept_with_gain = (
        bool(any_iteration_accepted)
        and bool(any_iteration_post_gt_pre)
        and decision in _ACCEPTED_DECISIONS_FOR_AGGREGATE_GAIN
    )

    if accept_with_gain:
        target_set = frozenset(t for t in target_qids if str(t).strip())
        fixed_set = frozenset(t for t in target_fixed_qids if str(t).strip())
        still_hard = frozenset(
            t for t in target_still_hard_qids if str(t).strip()
        )
        has_target_debt = bool(
            target_set
            and (not target_set.issubset(fixed_set) or still_hard)
        )
        if has_target_debt:
            return "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"
        return "OPTIMIZER_IMPROVED"

    if any_iteration_accepted:
        return "OPTIMIZER_TRIED_INSUFFICIENT_GAIN"

    return "OPTIMIZER_TRIED_NO_GAIN"


def classify_run_outcome(
    trajectories: tuple[QuestionTrajectory, ...],
    *,
    hard_rows_in_eval: bool = False,
    target_qids: tuple[str, ...] = (),
    target_fixed_qids: tuple[str, ...] = (),
) -> RunOutcome:
    """Classify the run-level outcome from the iteration trajectories.

    Precedence (highest first):
      1. Invariant violation — any trajectory with current_stage not in
         {ACCEPTED, TERMINATED} on its latest iteration (SM1 violation).
      2. Skipped input gap — eval had hard rows but no trajectories created.
      3. Aggregate-gain-target-debt (P4 C7) — accepted iteration AND
         ``target_qids`` is non-empty AND ``target_fixed_qids ⊊
         target_qids``. Sits between IMPROVED and IMPROVED's pre-P4
         path so the harness loop-control does not terminate while
         target debt remains.
      4. Improved — any trajectory accepted with positive accuracy delta.
      5. Tried no gain — any trajectory reached APPLIED but rolled back.
      6. Stalled no applied — any trajectory deepest in [PROPOSED, APPLYABLE].
      7. Stalled safe noop — all trajectories terminated via the escalation ladder.
      8. No candidates — all trajectories deepest below PROPOSED.

    ``target_qids`` is the run-level target QID set the harness is
    trying to fix. ``target_fixed_qids`` is the subset that actually
    flipped from incorrect-pre-apply to correct-post-apply on the
    accepted iteration. Both default to ``()`` for pre-P4 callers
    that do not yet supply the target context; behaviour falls back
    to the legacy ``IMPROVED`` precedence in that case.
    """
    if not trajectories and hard_rows_in_eval:
        return "OPTIMIZER_SKIPPED_INPUT_GAP"

    # SM1 check
    for traj in trajectories:
        latest = traj.iterations[-1]
        if not _is_settled_for_sm1(latest):
            return "OPTIMIZER_INVARIANT_VIOLATION"

    # P4 C7 — Aggregate-gain-target-debt sits ABOVE the legacy
    # IMPROVED precedence. We re-use the same accepted-with-positive-
    # delta predicate, then additionally require unresolved target
    # debt.
    accepted_with_gain = False
    for traj in trajectories:
        for it in traj.iterations:
            if (
                it.accepted is not None
                and it.accepted.decision
                in _ACCEPTED_DECISIONS_FOR_AGGREGATE_GAIN
                and it.evaluated is not None
                and it.evaluated.post_apply_score > it.evaluated.pre_apply_score
            ):
                accepted_with_gain = True
                break
        if accepted_with_gain:
            break
    if accepted_with_gain:
        target_set = frozenset(t for t in target_qids if str(t).strip())
        fixed_set = frozenset(t for t in target_fixed_qids if str(t).strip())
        if target_set and not target_set.issubset(fixed_set):
            return "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"
        return "OPTIMIZER_IMPROVED"

    # Trial 18 Step 3 — tried with insufficient gain. Sits between
    # ``OPTIMIZER_IMPROVED`` (precedence higher) and ``OPTIMIZER_TRIED_NO_GAIN``
    # (precedence lower). Fires when at least one iteration reached
    # the ``kept_insufficient`` lane (config kept live, signature
    # emitted) AND none reached ``OPTIMIZER_IMPROVED``. This is the
    # honest middle ground: "we landed cumulative-learning patches
    # but the headline didn't move on the target".
    for traj in trajectories:
        for it in traj.iterations:
            if (
                it.accepted is not None
                and it.accepted.decision in (
                    "kept_insufficient",
                    # Trial 19 C2 — same outcome bucket: a no-op apply
                    # against an already-arbiter-correct QID is also
                    # a "tried but no gain on target" outcome at the
                    # run level. The run-level distinguishes C2 vs.
                    # legacy kept_insufficient via the
                    # ``hard_qids_already_correct_count`` counter on
                    # ``GSO_OPTIMIZER_OUTCOME_V1`` (C4).
                    "already_correct_under_arbiter",
                )
            ):
                return "OPTIMIZER_TRIED_INSUFFICIENT_GAIN"

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
