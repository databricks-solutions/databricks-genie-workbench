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

from dataclasses import dataclass

ACCEPTED = "accepted"
REJECTED_INSUFFICIENT_GAIN = "rejected_insufficient_gain"
REJECTED_REGRESSION = "rejected_regression"
SUSPECTED_STALE_BASELINE = "suspected_stale_baseline"


@dataclass(frozen=True)
class AcceptanceDecision:
    """Outcome of the single-criterion full-eval acceptance gate.

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
) -> AcceptanceDecision:
    """Decide whether to accept the candidate state for this iteration.

    Single criterion: ``candidate >= baseline + min_gain_pp``. Pure
    function — no I/O, no globals, no side effects.

    Returns
    -------
    :class:`AcceptanceDecision`
        ``accepted=True`` only when the candidate strictly cleared the
        gain floor. ``delta_pp`` is candidate minus baseline, rounded to
        one decimal so audit rows are comparable across iterations.
    """
    delta = round(float(post_arbiter_candidate) - float(post_arbiter_baseline), 1)
    min_gain = float(min_gain_pp)

    if delta >= min_gain:
        reason = ACCEPTED
        accepted = True
    elif delta < 0:
        reason = REJECTED_REGRESSION
        accepted = False
    else:
        reason = REJECTED_INSUFFICIENT_GAIN
        accepted = False

    return AcceptanceDecision(
        accepted=accepted,
        post_arbiter_candidate=round(float(post_arbiter_candidate), 1),
        post_arbiter_baseline=round(float(post_arbiter_baseline), 1),
        delta_pp=delta,
        min_gain_pp=round(min_gain, 1),
        reason_code=reason,
    )


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
