"""Single-criterion lever-loop acceptance.

Background
----------
The previous acceptance gate ran two full evaluations per iteration,
estimated run-to-run variance, widened the regression tolerance,
switched between pre-arbiter / post-arbiter / blended objectives, and
applied a K-of-N strict check. The retail run still accepted a
``-4.6pp`` post-arbiter regression on AG2 because pre-arbiter
improvement (under ``OPTIMIZATION_OBJECTIVE='pre_arbiter'``) masked the
post-arbiter loss.

After re-deriving the model with the user, we replaced all of that with
a single criterion: **post-arbiter accuracy must improve by at least
``min_gain_pp`` percentage points over the carried baseline**. Variance
is no longer estimated. There is no second confirmation eval. There is
no objective switch — the arbiter's adjudication is the headline metric
end-to-end.

Safety net (Option D)
---------------------
The gain floor itself acts as a guardrail: setting
``MIN_POST_ARBITER_GAIN_PP=2.0`` means any iteration that lands within
``±2pp`` of the baseline is rejected, removing the noise band entirely.
Cross-iteration drift (a "lucky" accept whose true position regresses
later) is caught by a separate post-hoc diagnostic in
``harness.py`` — it logs a ``suspected_stale_baseline`` decision-audit
row but never auto-rolls back. Operator review.

The function is pure over its inputs so unit tests can replay AG1/AG2
metrics without a Databricks cluster.
"""

from __future__ import annotations

from dataclasses import dataclass, field

ACCEPTED = "accepted"
REJECTED_INSUFFICIENT_GAIN = "rejected_insufficient_gain"
REJECTED_REGRESSION = "rejected_regression"
SUSPECTED_STALE_BASELINE = "suspected_stale_baseline"


@dataclass(frozen=True)
class GainGateDecision:
    """RCO-5 — outcome of the single-criterion gain-gate.

    This is the UPSTREAM gain-gate sub-decision: did the candidate
    beat the baseline by at least ``min_gain_pp``? It is NOT the
    canonical Stage-9 acceptance decision — see
    ``optimization.control_plane.ControlPlaneAcceptance`` for that.

    The harness combines this gain-gate result with target-set
    classification (target_fixed / target_still_hard / regression
    buckets) to produce the canonical ``ControlPlaneAcceptance``.

    Renamed from ``AcceptanceDecision`` in RCO-5 to make the
    upstream-feeder role structurally explicit.

    ``reason_code`` is one of:

    * ``accepted`` — candidate exceeded baseline by at least
      ``min_gain_pp``.
    * ``rejected_insufficient_gain`` — candidate did not regress, but
      the gain was below ``min_gain_pp``.
    * ``rejected_regression`` — candidate is strictly below baseline.
    """

    accepted: bool
    post_arbiter_candidate: float
    post_arbiter_baseline: float
    delta_pp: float
    min_gain_pp: float
    reason_code: str


def decide_acceptance(
    *,
    post_arbiter_candidate: float,
    post_arbiter_baseline: float,
    min_gain_pp: float,
) -> GainGateDecision:
    """Decide whether to accept the candidate state for this iteration.

    Single criterion: ``candidate >= baseline + min_gain_pp``. Pure
    function — no I/O, no globals, no side effects.

    Returns
    -------
    :class:`GainGateDecision`
        ``accepted=True`` only when the candidate strictly cleared the
        gain floor. ``delta_pp`` is candidate minus baseline, rounded to
        one decimal so audit rows are comparable across iterations.
    """
    delta = round(float(post_arbiter_candidate) - float(post_arbiter_baseline), 1)
    min_gain = float(min_gain_pp)

    if delta > 0 and delta >= min_gain:
        reason = ACCEPTED
        accepted = True
    elif delta < 0:
        reason = REJECTED_REGRESSION
        accepted = False
    else:
        reason = REJECTED_INSUFFICIENT_GAIN
        accepted = False

    return GainGateDecision(
        accepted=accepted,
        post_arbiter_candidate=round(float(post_arbiter_candidate), 1),
        post_arbiter_baseline=round(float(post_arbiter_baseline), 1),
        delta_pp=delta,
        min_gain_pp=round(min_gain, 1),
        reason_code=reason,
    )


def arbiter_objective_complete(
    post_arbiter_accuracy: float,
    *,
    target_accuracy: float = 100.0,
) -> bool:
    """Return true when the run has reached the terminal arbiter objective."""
    return float(post_arbiter_accuracy) >= float(target_accuracy)


def arbiter_objective_complete_from_counts(
    *,
    post_arbiter_accuracy: float,
    total_questions: int,
    evaluated_count: int,
    blocking_excluded_count: int,
    target_accuracy: float = 100.0,
) -> bool:
    """Return true only when the full benchmark objective is complete.

    A run with 100% on 20 scored rows and 1 unresolved excluded row is not
    complete. That is a 20/20 scored success, not a 21/21 benchmark success.
    """
    if int(total_questions or 0) <= 0:
        return False
    if int(blocking_excluded_count or 0) > 0:
        return False
    if int(evaluated_count or 0) < int(total_questions or 0):
        return False
    return float(post_arbiter_accuracy) >= float(target_accuracy)


@dataclass(frozen=True)
class BaselineDriftDiagnostic:
    """Outcome of the post-hoc baseline drift check.

    The previous iteration's accept may have ridden noise. If the
    *current* iteration's post-arbiter accuracy lands materially below
    the *pre-acceptance* baseline that was carried into the previous
    iteration, we mark the prior accept as a suspected stale baseline
    so a human can review.

    No auto-rollback. ``triggered=True`` only writes a decision-audit
    row.
    """

    triggered: bool
    post_arbiter_current: float
    prev_iter_pre_accept_baseline: float | None
    delta_pp: float | None
    threshold_pp: float
    reason_code: str | None


def decide_baseline_drift(
    *,
    post_arbiter_current: float,
    prev_iter_pre_accept_baseline: float | None,
    threshold_pp: float,
) -> BaselineDriftDiagnostic:
    """Compute whether the post-hoc baseline drift diagnostic fires.

    Pure function. Called at iteration ``N+1`` entry, where
    ``prev_iter_pre_accept_baseline`` is the carried baseline that was
    in force at the *start* of iteration ``N`` (i.e. before iter N's
    accept). On the very first iteration there is no prior baseline,
    so we return an inert decision (``triggered=False``,
    ``reason_code=None``).

    The threshold is treated as a magnitude: any negative delta whose
    absolute value meets or exceeds ``threshold_pp`` triggers.
    """
    if prev_iter_pre_accept_baseline is None:
        return BaselineDriftDiagnostic(
            triggered=False,
            post_arbiter_current=round(float(post_arbiter_current), 1),
            prev_iter_pre_accept_baseline=None,
            delta_pp=None,
            threshold_pp=round(float(threshold_pp), 1),
            reason_code=None,
        )

    delta = round(
        float(post_arbiter_current) - float(prev_iter_pre_accept_baseline),
        1,
    )
    threshold = float(threshold_pp)
    triggered = delta <= -threshold

    return BaselineDriftDiagnostic(
        triggered=triggered,
        post_arbiter_current=round(float(post_arbiter_current), 1),
        prev_iter_pre_accept_baseline=round(
            float(prev_iter_pre_accept_baseline), 1,
        ),
        delta_pp=delta,
        threshold_pp=round(threshold, 1),
        reason_code=SUSPECTED_STALE_BASELINE if triggered else None,
    )


# ── Cycle 14B-T1: partial-harvest with bounded regression debt ────────
#
# RegressionDebtPolicy parameterizes the new accept-with-debt branch
# in decide_control_plane_acceptance. Hard-zero defaults preserve
# byte-stability when the GSO_PARTIAL_HARVEST_WITH_DEBT flag is off;
# regression_debt_policy_pilot_default() is the production policy
# applied when the flag is on.

# Imported lazily inside the class so the module-level import order
# (acceptance_policy → control_plane) does not become circular if
# control_plane ever needs to import acceptance_policy in the future.
from genie_space_optimizer.optimization.control_plane import DeltaState


@dataclass(frozen=True)
class RegressionDebtPolicy:
    """Policy governing accept-with-debt acceptance decisions.

    Fields:
        max_debt_qids: Maximum out-of-target regressions admitted in
            a single iteration. Default 0 (no debt — legacy behavior).
        allowed_debt_buckets: Subset of DeltaState values that may
            count as admissible debt. A debt QID whose state is
            outside this set forces full rollback regardless of
            count. Default empty.
        min_aggregate_improvement_pp: Minimum post-arbiter accuracy
            gain (candidate − baseline) required for accept-with-debt.
            A net-zero or net-negative iteration cannot accept debt.
        min_target_clusters_fixed: Minimum count of target QIDs
            whose DeltaState is FIXED. Zero means "no causal-fix
            requirement"; the pilot policy uses 1.
        min_threshold_pass_rate: Minimum fraction of run-level
            thresholds the candidate must satisfy (e.g., overall
            accuracy ≥95%, soft-failure rate ≤5%). The harness
            computes this fraction; the policy field is the gate.
        cumulative_debt_max: Cap on total accepted debt across the
            entire run (sum across iterations). Once hit, no
            subsequent iteration may accept debt until a
            debt-clearing iteration (a candidate that fixes a
            previously-accepted debt QID — bookkeeping in the
            harness).

    Validation runs in __post_init__ so an invalid policy fails
    at construction, never at evaluation.
    """

    max_debt_qids: int = 0
    allowed_debt_buckets: frozenset[DeltaState] = field(default_factory=frozenset)
    min_aggregate_improvement_pp: float = 0.0
    min_target_clusters_fixed: int = 0
    min_threshold_pass_rate: float = 0.0
    cumulative_debt_max: int = 0

    def __post_init__(self) -> None:
        if self.max_debt_qids < 0:
            raise ValueError(
                f"max_debt_qids must be >= 0, got {self.max_debt_qids}"
            )
        if not 0.0 <= self.min_threshold_pass_rate <= 1.0:
            raise ValueError(
                f"min_threshold_pass_rate must be in [0, 1], got "
                f"{self.min_threshold_pass_rate}"
            )
        if self.min_aggregate_improvement_pp < 0.0:
            raise ValueError(
                f"min_aggregate_improvement_pp must be >= 0, got "
                f"{self.min_aggregate_improvement_pp}"
            )
        if self.min_target_clusters_fixed < 0:
            raise ValueError(
                f"min_target_clusters_fixed must be >= 0, got "
                f"{self.min_target_clusters_fixed}"
            )
        if self.cumulative_debt_max < 0:
            raise ValueError(
                f"cumulative_debt_max must be >= 0, got {self.cumulative_debt_max}"
            )
        if self.cumulative_debt_max < self.max_debt_qids:
            raise ValueError(
                f"cumulative_debt_max ({self.cumulative_debt_max}) must be "
                f">= max_debt_qids ({self.max_debt_qids}); otherwise the "
                f"first debt-accepting iteration would exceed the "
                f"cumulative cap and the policy is unreachable"
            )
        for bucket in self.allowed_debt_buckets:
            if not isinstance(bucket, DeltaState):
                raise TypeError(
                    f"allowed_debt_buckets entry must be DeltaState, got "
                    f"{type(bucket).__name__} ({bucket!r})"
                )


def regression_debt_policy_pilot_default() -> RegressionDebtPolicy:
    """Pilot policy applied when GSO_PARTIAL_HARVEST_WITH_DEBT=1.

    Values match the roadmap's Cycle 14B "What changes" block:
    one debt QID per iteration, soft-to-hard only, ≥10pp aggregate
    gain, ≥1 target fixed, ≥95% threshold pass rate, ≤3 cumulative
    debt across the run.
    """
    return RegressionDebtPolicy(
        max_debt_qids=1,
        allowed_debt_buckets=frozenset({DeltaState.SOFT_TO_HARD}),
        min_aggregate_improvement_pp=10.0,
        min_target_clusters_fixed=1,
        min_threshold_pass_rate=0.95,
        cumulative_debt_max=3,
    )


def regression_debt_policy_from_config() -> RegressionDebtPolicy:
    """Build a RegressionDebtPolicy from environment configuration.

    Reads ``GSO_PARTIAL_HARVEST_WITH_DEBT`` to choose the base policy:
    pilot default when on, hard-zero default when off. Per-field
    overrides (``GSO_PARTIAL_HARVEST_MAX_DEBT_QIDS``,
    ``GSO_PARTIAL_HARVEST_CUMULATIVE_MAX``,
    ``GSO_PARTIAL_HARVEST_MIN_AGG_IMPROVEMENT_PP``,
    ``GSO_PARTIAL_HARVEST_MIN_TARGETS_FIXED``,
    ``GSO_PARTIAL_HARVEST_MIN_THRESHOLD_PASS_RATE``) are applied on
    top of the base.
    """
    import os

    from genie_space_optimizer.common.config import (
        partial_harvest_with_debt_enabled,
    )

    if not partial_harvest_with_debt_enabled():
        base = RegressionDebtPolicy()  # hard-zero default
    else:
        base = regression_debt_policy_pilot_default()

    def _int_override(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return default
        stripped = raw.strip()
        return int(stripped) if stripped.lstrip("-").isdigit() else default

    def _float_override(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    return RegressionDebtPolicy(
        max_debt_qids=_int_override(
            "GSO_PARTIAL_HARVEST_MAX_DEBT_QIDS", base.max_debt_qids
        ),
        allowed_debt_buckets=base.allowed_debt_buckets,
        min_aggregate_improvement_pp=_float_override(
            "GSO_PARTIAL_HARVEST_MIN_AGG_IMPROVEMENT_PP",
            base.min_aggregate_improvement_pp,
        ),
        min_target_clusters_fixed=_int_override(
            "GSO_PARTIAL_HARVEST_MIN_TARGETS_FIXED",
            base.min_target_clusters_fixed,
        ),
        min_threshold_pass_rate=_float_override(
            "GSO_PARTIAL_HARVEST_MIN_THRESHOLD_PASS_RATE",
            base.min_threshold_pass_rate,
        ),
        cumulative_debt_max=_int_override(
            "GSO_PARTIAL_HARVEST_CUMULATIVE_MAX",
            base.cumulative_debt_max,
        ),
    )


# ── Phase 1 (2026-05-13): attribution-drift acceptance tier ──────────
#
# Sibling of the partial-harvest tier above. Used by the new branch in
# decide_control_plane_acceptance gated on GSO_ATTRIBUTION_DRIFT_WITH_DEBT.
# Design rationale: 2026-05-13-acceptance-gate-redesign-design-record.md
# (decisions D1-D4).


def attribution_drift_policy_pilot_default() -> RegressionDebtPolicy:
    """Pilot policy applied when GSO_ATTRIBUTION_DRIFT_WITH_DEBT=1.

    Differs from regression_debt_policy_pilot_default along three axes:
    - min_target_clusters_fixed=0 (accepts even when no target is fixed)
    - min_aggregate_improvement_pp=4.0 (lower aggregate floor)
    - allowed_debt_buckets includes LOOKUP_FAILED (unknown_to_hard)
      alongside SOFT_TO_HARD; passing_to_hard remains excluded.

    The cumulative_debt_max is shared with the partial-harvest tier
    (single harness counter); per-tier accounting is a follow-up.
    """
    return RegressionDebtPolicy(
        max_debt_qids=1,
        allowed_debt_buckets=frozenset(
            {DeltaState.SOFT_TO_HARD, DeltaState.LOOKUP_FAILED}
        ),
        min_aggregate_improvement_pp=4.0,
        min_target_clusters_fixed=0,
        min_threshold_pass_rate=0.95,
        cumulative_debt_max=3,
    )


def attribution_drift_policy_from_config() -> RegressionDebtPolicy:
    """Build the attribution-drift policy honoring env overrides.

    Reads GSO_ATTRIBUTION_DRIFT_WITH_DEBT to choose the base policy:
    pilot default when on, hard-zero default when off. Per-field
    overrides mirror the partial-harvest from_config pattern:

    - GSO_ATTRIBUTION_DRIFT_MAX_DEBT_QIDS
    - GSO_ATTRIBUTION_DRIFT_CUMULATIVE_MAX
    - GSO_ATTRIBUTION_DRIFT_MIN_AGG_IMPROVEMENT_PP
    - GSO_ATTRIBUTION_DRIFT_MIN_TARGETS_FIXED
    - GSO_ATTRIBUTION_DRIFT_MIN_THRESHOLD_PASS_RATE
    """
    import os

    from genie_space_optimizer.common.config import (
        attribution_drift_with_debt_enabled,
    )

    if not attribution_drift_with_debt_enabled():
        base = RegressionDebtPolicy()  # hard-zero default
    else:
        base = attribution_drift_policy_pilot_default()

    def _int_override(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return default
        stripped = raw.strip()
        return int(stripped) if stripped.lstrip("-").isdigit() else default

    def _float_override(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    return RegressionDebtPolicy(
        max_debt_qids=_int_override(
            "GSO_ATTRIBUTION_DRIFT_MAX_DEBT_QIDS", base.max_debt_qids
        ),
        allowed_debt_buckets=base.allowed_debt_buckets,
        min_aggregate_improvement_pp=_float_override(
            "GSO_ATTRIBUTION_DRIFT_MIN_AGG_IMPROVEMENT_PP",
            base.min_aggregate_improvement_pp,
        ),
        min_target_clusters_fixed=_int_override(
            "GSO_ATTRIBUTION_DRIFT_MIN_TARGETS_FIXED",
            base.min_target_clusters_fixed,
        ),
        min_threshold_pass_rate=_float_override(
            "GSO_ATTRIBUTION_DRIFT_MIN_THRESHOLD_PASS_RATE",
            base.min_threshold_pass_rate,
        ),
        cumulative_debt_max=_int_override(
            "GSO_ATTRIBUTION_DRIFT_CUMULATIVE_MAX",
            base.cumulative_debt_max,
        ),
    )


# ── Phase 1 Action 1.2: four-tier acceptance gate ─────────────────────
#
# The four-tier classifier consumes the canonical
# ControlPlaneAcceptance and returns a typed TierVerdict. It is
# additive: existing acceptance_policy callers ignore it; only the
# new stages.acceptance.decide branch (gated on the four-tier flag)
# routes acceptance through this function.

from enum import Enum as _Enum


class AcceptedClass(str, _Enum):
    """Phase 1 Action 1.2 — four-tier acceptance vocabulary.

    Used by ``classify_acceptance_tier`` to grade an acceptance
    decision. The ``LOSS`` tier is the only one that maps to a
    rollback under the legacy two-tier model (``rejected_*``);
    ``DIAGNOSTIC_HOLD`` is also a rollback but emits richer
    reflection input for the strategist.
    """

    STRICT_WIN = "strict_win"
    NET_WIN_WITH_DEBT = "net_win_with_debt"
    # Trial 23 W2 — a net win on global accuracy whose target QID debt
    # is unresolved. Records the global delta_pp as evidence but is
    # NOT deployable: "accepted" must mean the target was fixed, not
    # that the global scoreboard moved.
    NET_WIN_NON_DEPLOYABLE = "net_win_non_deployable"
    DIAGNOSTIC_HOLD = "diagnostic_hold"
    LOSS = "loss"


@dataclass(frozen=True)
class TierAcceptancePolicy:
    """Tunable bounds for the four-tier classifier.

    Defaults match the Phase 1 spec's Action 1.2 worked example.
    """

    net_win_min_delta_pp: float = 3.0
    net_win_max_unknown_to_hard: int = 1
    net_win_fixes_minus_regressions_floor: int = 2
    diagnostic_hold_min_delta_pp: float = 1.0


def tier_acceptance_policy_pilot_default() -> TierAcceptancePolicy:
    """Phase 1 Action 1.2 — default tier-acceptance policy."""
    return TierAcceptancePolicy()


def tier_acceptance_policy_from_config() -> TierAcceptancePolicy:
    """Build a TierAcceptancePolicy from environment configuration."""
    import os

    base = tier_acceptance_policy_pilot_default()

    def _float(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    def _int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return default
        stripped = raw.strip()
        return int(stripped) if stripped.lstrip("-").isdigit() else default

    return TierAcceptancePolicy(
        net_win_min_delta_pp=_float(
            "GSO_TIER_NET_WIN_MIN_DELTA_PP", base.net_win_min_delta_pp,
        ),
        net_win_max_unknown_to_hard=_int(
            "GSO_TIER_NET_WIN_MAX_UNKNOWN_TO_HARD",
            base.net_win_max_unknown_to_hard,
        ),
        net_win_fixes_minus_regressions_floor=_int(
            "GSO_TIER_NET_WIN_FIXES_MINUS_REGRESSIONS_FLOOR",
            base.net_win_fixes_minus_regressions_floor,
        ),
        diagnostic_hold_min_delta_pp=_float(
            "GSO_TIER_DIAGNOSTIC_HOLD_MIN_DELTA_PP",
            base.diagnostic_hold_min_delta_pp,
        ),
    )


@dataclass(frozen=True)
class TierVerdict:
    """Result of running ``classify_acceptance_tier``."""

    accepted_class: AcceptedClass
    accept: bool
    debt_classification: dict
    reflection_payload: dict


def classify_acceptance_tier(
    *,
    decision,
    policy: TierAcceptancePolicy,
    demote_on_unresolved_target_debt: bool = False,
) -> TierVerdict:
    """Phase 1 Action 1.2 — pure-function four-tier classifier.

    Order of checks (first-match wins so the verdict is deterministic):

      1. LOSS  if delta_pp <= 0 OR passing_to_hard non-empty OR
                                       protected non-empty.
      2. STRICT_WIN  if every target qid is fixed AND no
                     out-of-target regressions of any flavour.
      3. NET_WIN_WITH_DEBT  if delta_pp >= net_win_min_delta_pp AND
                            passing_to_hard empty AND
                            soft_to_hard empty AND
                            unknown_to_hard count <= bound AND
                            fixes_count >= regressions_count + floor.
      4. DIAGNOSTIC_HOLD  if delta_pp >= diagnostic_hold_min_delta_pp AND
                          target_not_fixed AND any net_win bound
                          exceeded. Otherwise → LOSS.
    """
    delta_pp = float(decision.delta_pp or 0.0)
    target_qids = tuple(decision.target_qids or ())
    target_fixed = tuple(decision.target_fixed_qids or ())
    target_still_hard = tuple(decision.target_still_hard_qids or ())
    out_of_target = tuple(decision.out_of_target_regressed_qids or ())
    passing_to_hard = tuple(decision.passing_to_hard_regressed_qids or ())
    soft_to_hard = tuple(decision.soft_to_hard_regressed_qids or ())
    unknown_to_hard = tuple(decision.unknown_to_hard_regressed_qids or ())
    protected = tuple(decision.protected_regressed_qids or ())

    fixes_count = len(target_fixed)
    regressions_count = len(out_of_target)
    target_fixed_set = set(target_fixed)
    target_fully_fixed = bool(target_qids) and all(
        q in target_fixed_set for q in target_qids
    )

    # 1. LOSS gate.
    if delta_pp <= 0 or passing_to_hard or protected:
        return TierVerdict(
            accepted_class=AcceptedClass.LOSS,
            accept=False,
            debt_classification={},
            reflection_payload={
                "delta_pp": delta_pp,
                "fixes_count": fixes_count,
                "regressions_count": regressions_count,
                "passing_to_hard": list(passing_to_hard),
                "protected_regressed": list(protected),
            },
        )

    # 2. STRICT_WIN
    if target_fully_fixed and not out_of_target and not protected:
        return TierVerdict(
            accepted_class=AcceptedClass.STRICT_WIN,
            accept=True,
            debt_classification={},
            reflection_payload={},
        )

    # 3. NET_WIN_WITH_DEBT predicate.
    net_win_predicates = {
        "delta_pp_ok": delta_pp >= float(policy.net_win_min_delta_pp),
        "no_passing_to_hard": not passing_to_hard,
        "no_soft_to_hard": not soft_to_hard,
        "unknown_to_hard_within_bound": (
            len(unknown_to_hard) <= int(policy.net_win_max_unknown_to_hard)
        ),
        "fixes_margin_ok": (
            fixes_count
            >= regressions_count
            + int(policy.net_win_fixes_minus_regressions_floor)
        ),
    }
    if all(net_win_predicates.values()):
        debt = _build_debt_classification(
            unknown_to_hard=unknown_to_hard,
            soft_to_hard=soft_to_hard,
            passing_to_hard=passing_to_hard,
        )
        _target_not_fixed = not target_fully_fixed and bool(target_still_hard)
        # Trial 23 W2 — target-honest acceptance. A global net win whose
        # named target QID is still hard is NOT deployable: it is
        # attribution drift, not a fix of the stated goal. Demote to a
        # non-deployable diagnostic class while preserving the delta_pp
        # evidence. Default OFF (caller passes the flag) so the
        # pre-Trial-23 NET_WIN_WITH_DEBT accept path is byte-stable.
        if demote_on_unresolved_target_debt and _target_not_fixed:
            return TierVerdict(
                accepted_class=AcceptedClass.NET_WIN_NON_DEPLOYABLE,
                accept=False,
                debt_classification=debt,
                reflection_payload={
                    "delta_pp": delta_pp,
                    "fixes_count": fixes_count,
                    "regressions_count": regressions_count,
                    "global_improvement_target_not_fixed": True,
                    "unresolved_target_debt_qids": list(target_still_hard),
                    "demoted_from": AcceptedClass.NET_WIN_WITH_DEBT.value,
                    "predicates": net_win_predicates,
                },
            )
        return TierVerdict(
            accepted_class=AcceptedClass.NET_WIN_WITH_DEBT,
            accept=True,
            debt_classification=debt,
            reflection_payload={
                "delta_pp": delta_pp,
                "fixes_count": fixes_count,
                "regressions_count": regressions_count,
                "global_improvement_target_not_fixed": _target_not_fixed,
                "predicates": net_win_predicates,
            },
        )

    # 4. DIAGNOSTIC_HOLD.
    target_not_fixed = bool(target_qids) and not target_fully_fixed
    diagnostic_hold_predicates = {
        "delta_pp_ok": delta_pp >= float(policy.diagnostic_hold_min_delta_pp),
        "target_not_fixed": target_not_fixed,
        "net_win_bounds_exceeded": not all(net_win_predicates.values()),
    }
    if all(diagnostic_hold_predicates.values()):
        debt = _build_debt_classification(
            unknown_to_hard=unknown_to_hard,
            soft_to_hard=soft_to_hard,
            passing_to_hard=passing_to_hard,
        )
        return TierVerdict(
            accepted_class=AcceptedClass.DIAGNOSTIC_HOLD,
            accept=False,
            debt_classification=debt,
            reflection_payload={
                "delta_pp": delta_pp,
                "fixes_count": fixes_count,
                "regressions_count": regressions_count,
                "fixes_vs_regressions": (
                    f"fixes={fixes_count}, "
                    f"regressions={regressions_count}, "
                    f"floor={int(policy.net_win_fixes_minus_regressions_floor)}"
                ),
                "tripped_net_win_bounds": [
                    name for name, ok in net_win_predicates.items() if not ok
                ],
                "improvement_with_unbounded_debt": True,
            },
        )

    # Fallthrough → LOSS.
    return TierVerdict(
        accepted_class=AcceptedClass.LOSS,
        accept=False,
        debt_classification={},
        reflection_payload={
            "delta_pp": delta_pp,
            "fixes_count": fixes_count,
            "regressions_count": regressions_count,
            "diagnostic_hold_predicates": diagnostic_hold_predicates,
        },
    )


def _build_debt_classification(
    *,
    unknown_to_hard,
    soft_to_hard,
    passing_to_hard,
) -> dict:
    """Compose the debt_classification dict for accept/diagnostic tiers."""
    out: dict = {}
    if unknown_to_hard:
        out["unknown_to_hard"] = sorted(unknown_to_hard)
    if soft_to_hard:
        out["soft_to_hard"] = sorted(soft_to_hard)
    if passing_to_hard:
        out["passing_to_hard"] = sorted(passing_to_hard)
    return out


# ---------------------------------------------------------------------------
# Phase 1 Addendum — observability-only soft-signal pass rate.
# ---------------------------------------------------------------------------


def compute_soft_signal_pass_rate(eval_rows: list[dict]) -> float:
    """Phase 1 Addendum — observability-only soft-signal pass rate.

    Computed as ``passes / total`` across every (qid, soft-signal)
    pair in ``eval_rows``. Returns ``0.0`` when no soft-signal
    results exist (caller treats this as "no evidence", not as a
    failing rate).

    **Important:** This is **not** an input to ``classify_acceptance_tier``.
    The four-tier gate is intentionally hard-correctness-only. Surfacing
    this rate on ``tier_classification_record.metric_payload`` is
    observability so postmortems can correlate accuracy moves with
    judge-score moves; it does not gate accept/reject decisions.

    If the gate accepted/rejected on this signal, the optimizer would
    learn to chase judge scores instead of fixing real failures. The
    classifier's signature does not accept any soft-signal parameter,
    and an anti-gaming invariant test (T A1.2.A.2) enforces this
    contract.
    """
    total = 0
    passes = 0
    for row in eval_rows or ():
        results = (row or {}).get("soft_signal_results") or {}
        if not isinstance(results, dict):
            continue
        for verdict in results.values():
            total += 1
            if str(verdict).lower() == "pass":
                passes += 1
    if total == 0:
        return 0.0
    return passes / total
