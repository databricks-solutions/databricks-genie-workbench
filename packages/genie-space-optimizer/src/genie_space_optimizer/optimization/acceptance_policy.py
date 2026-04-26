"""Strict K-of-N acceptance policy for full-eval gate decisions.

Background
----------
The decoded retail run accepted AG2 because pre-arbiter accuracy
improved (50→68%) while post-arbiter accuracy dropped (-4.6pp), staying
just inside the previous 5.0pp guardrail. The slice and P0 gates also
passed silently. The acceptance policy below replaces the
average-with-soft-cap approach with a strict per-run check that:

* Treats post-arbiter as a hard guardrail in every objective mode,
  including ``blended`` — a pre-arbiter-driven blend cannot mask a
  post-arbiter regression.
* Composes with ``harness._compute_eval_variance`` /
  ``effective_regression_tol`` widening: variance can only TIGHTEN the
  guardrail (protecting against noise on small corpora), never loosen
  it. ``effective_guardrail_pp = min(max_post_arbiter_drop_pp,
  variance_widened_tol_pp)``.
* Requires *every* confirmation run — not just the average — to clear
  the primary-gain floor. This is K-of-N strict, where a single bad
  run rejects the bundle.

The function is pure over its inputs so the gate sequence in
``harness.py::_run_gate_checks`` can call it directly and unit tests
can replay AG1/AG2 metrics without a Databricks cluster.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Objective = Literal["pre_arbiter", "post_arbiter", "blended"]


@dataclass(frozen=True)
class AcceptanceDecision:
    """Outcome of a full-eval acceptance gate.

    ``reason_code`` is one of:

    * ``accepted``                          — every run cleared both gates.
    * ``primary_not_improved_in_every_run`` — at least one run failed the
      per-run primary-gain floor.
    * ``post_arbiter_guardrail``            — at least one run dropped
      raw post-arbiter beyond the effective guardrail.
    * ``missing_confirmation_runs``         — no confirmation runs supplied.
    """

    accepted: bool
    reason_code: str
    primary_delta_pp: float
    secondary_delta_pp: float
    min_run_primary: float
    min_run_post_arbiter: float
    effective_guardrail_pp: float


def _primary_value(pre: float, post: float, objective: Objective) -> float:
    if objective == "pre_arbiter":
        return pre
    if objective == "post_arbiter":
        return post
    # blended: simple mean. Higher-leverage modes can be added later via
    # config; the contract that matters is that ``post_arbiter`` is
    # ALSO checked as a guardrail regardless of the objective.
    return 0.5 * pre + 0.5 * post


def decide_full_eval_acceptance(
    *,
    objective: Objective,
    previous_pre_arbiter: float,
    previous_post_arbiter: float,
    run_pre_arbiter: list[float],
    run_post_arbiter: list[float],
    min_primary_gain_pp: float,
    max_post_arbiter_drop_pp: float,
    variance_widened_tol_pp: float,
) -> AcceptanceDecision:
    """Strict K-of-N acceptance.

    Returns an :class:`AcceptanceDecision`. Pure: no I/O, no globals.

    Composition with variance widening:
        effective_guardrail_pp = min(max_post_arbiter_drop_pp,
                                     variance_widened_tol_pp)

    Variance can only tighten the guardrail. A noisy small corpus can
    never reproduce the AG2 false-accept condition because variance
    widening cannot relax the post-arbiter cap.
    """
    if not run_pre_arbiter or not run_post_arbiter:
        return AcceptanceDecision(
            accepted=False,
            reason_code="missing_confirmation_runs",
            primary_delta_pp=0.0,
            secondary_delta_pp=0.0,
            min_run_primary=0.0,
            min_run_post_arbiter=0.0,
            effective_guardrail_pp=0.0,
        )

    primary_runs = [
        _primary_value(pre, post, objective)
        for pre, post in zip(run_pre_arbiter, run_post_arbiter)
    ]
    previous_primary = _primary_value(
        previous_pre_arbiter, previous_post_arbiter, objective,
    )
    avg_primary = sum(primary_runs) / len(primary_runs)
    avg_post = sum(run_post_arbiter) / len(run_post_arbiter)
    primary_delta = round(avg_primary - previous_primary, 1)
    secondary_delta = round(avg_post - previous_post_arbiter, 1)
    min_primary = min(primary_runs)
    min_post = min(run_post_arbiter)

    # Variance widens the guardrail only as a *floor* against
    # variance-driven false rejections. It can never make the cap
    # looser than the configured maximum drop.
    effective_guardrail = min(max_post_arbiter_drop_pp, variance_widened_tol_pp)

    if min_primary < previous_primary + min_primary_gain_pp:
        return AcceptanceDecision(
            accepted=False,
            reason_code="primary_not_improved_in_every_run",
            primary_delta_pp=primary_delta,
            secondary_delta_pp=secondary_delta,
            min_run_primary=round(min_primary, 1),
            min_run_post_arbiter=round(min_post, 1),
            effective_guardrail_pp=round(effective_guardrail, 2),
        )

    if min_post < previous_post_arbiter - effective_guardrail:
        return AcceptanceDecision(
            accepted=False,
            reason_code="post_arbiter_guardrail",
            primary_delta_pp=primary_delta,
            secondary_delta_pp=secondary_delta,
            min_run_primary=round(min_primary, 1),
            min_run_post_arbiter=round(min_post, 1),
            effective_guardrail_pp=round(effective_guardrail, 2),
        )

    return AcceptanceDecision(
        accepted=True,
        reason_code="accepted",
        primary_delta_pp=primary_delta,
        secondary_delta_pp=secondary_delta,
        min_run_primary=round(min_primary, 1),
        min_run_post_arbiter=round(min_post, 1),
        effective_guardrail_pp=round(effective_guardrail, 2),
    )
