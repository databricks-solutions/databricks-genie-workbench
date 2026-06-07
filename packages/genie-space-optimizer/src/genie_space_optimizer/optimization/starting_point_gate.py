"""Trial 27 W27.3 — Starting Point Gate force-lever-loop override.

Extracts the pure decision logic from
``jobs/run_lever_loop.py``'s baseline-gate block so it is unit-testable
without spinning up a notebook context (dbutils, spark, mlflow).

Decision inputs:

* ``thresholds_met`` — sourced from the post-enrichment compact handoff
  when present, else the baseline_eval value (the notebook still does
  this resolution; the gate consumes the resolved bool).
* ``accuracy_source`` — ``"enrichment.post_enrichment_accuracy"`` or
  ``"baseline_eval"``; used only to compute the human-readable skip
  reason string.
* ``force_lever_loop_signal`` — per-run boolean set by the harness
  (``gso-lever-loop-replay --force-lever-loop`` / job parameter
  ``force_lever_loop=true``). Default False on every other run, so
  the gate is byte-stable for normal (non-verification) replays.

The deploy-time capability flag
(``GSO_TRIAL27_FORCE_LEVER_LOOP_OVERRIDE``, default ON when the master
``GSO_TRIAL27_STAGE3_DESTARVE`` is ON) gates whether the signal can
take effect at all — emergency rollback knob, no harness change
required.

Output: :class:`SkipDecision` with the gate verdict, the
human-readable skip reason (matches the live notebook's decoded skip
string for postmortem grep parity), and observability hooks for the
``GSO_TRIAL27_FORCE_LEVER_LOOP_V1`` marker.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from genie_space_optimizer.optimization.trial27_flags import (
    trial27_force_lever_loop_override_enabled,
)


def _skip_reason_from_source(accuracy_source: str) -> str:
    """Compute the live-notebook-parity skip reason string."""
    return (
        "post_enrichment_meets_thresholds"
        if accuracy_source == "enrichment.post_enrichment_accuracy"
        else "baseline_meets_thresholds"
    )


@dataclass(frozen=True)
class SkipDecision:
    """Verdict of the Starting Point Gate.

    Attributes:
        skip: True iff the lever_loop should be skipped.
        reason: When ``skip=True``, the canonical reason string
            (matches the pre-Trial-27 notebook decoded output:
            ``post_enrichment_meets_thresholds`` or
            ``baseline_meets_thresholds``). Empty when ``skip=False``.
        override_engaged: True iff the W27.3 force-lever-loop
            override flipped a would-be skip into a no-skip. Drives
            whether the postmortem-side marker is emitted.
        would_have_skipped_reason: When ``override_engaged=True``,
            the skip reason the gate would have used. None otherwise.
    """

    skip: bool
    reason: str
    override_engaged: bool
    would_have_skipped_reason: Optional[str]


def should_skip_starting_point_gate(
    *,
    thresholds_met: bool,
    accuracy_source: str,
    force_lever_loop_signal: bool,
) -> SkipDecision:
    """Pure decision function for the lever_loop Starting Point Gate.

    Args:
        thresholds_met: Resolved current-state ``thresholds_met``
            (notebook prefers post-enrichment over baseline_eval —
            this function consumes the already-resolved bool).
        accuracy_source: ``"enrichment.post_enrichment_accuracy"``
            or ``"baseline_eval"``. Drives the skip-reason string
            only.
        force_lever_loop_signal: Per-run override signal from the
            harness. Default False on normal runs.

    Returns:
        :class:`SkipDecision`. Byte-stable with pre-Trial-27 behaviour
        when ``force_lever_loop_signal=False`` (every normal replay)
        or when the deploy-time capability flag is OFF (emergency
        rollback).
    """
    if not thresholds_met:
        return SkipDecision(
            skip=False,
            reason="",
            override_engaged=False,
            would_have_skipped_reason=None,
        )

    would_have_skipped_reason = _skip_reason_from_source(accuracy_source)

    capability_enabled = trial27_force_lever_loop_override_enabled()
    if force_lever_loop_signal and capability_enabled:
        return SkipDecision(
            skip=False,
            reason="",
            override_engaged=True,
            would_have_skipped_reason=would_have_skipped_reason,
        )

    return SkipDecision(
        skip=True,
        reason=would_have_skipped_reason,
        override_engaged=False,
        would_have_skipped_reason=None,
    )


def starting_point_gate_force_marker(
    *,
    optimization_run_id: str,
    would_have_skipped_reason: str,
    accuracy_source: str,
    post_enrichment_accuracy: Optional[float],
    baseline_accuracy: Optional[float],
) -> str:
    """Build the ``GSO_TRIAL27_FORCE_LEVER_LOOP_V1`` marker line.

    Emitted only when the W27.3 override engaged (``skip=False``
    despite ``thresholds_met=True``). Captures the would-have-skipped
    reason and the accuracies at decision time so postmortems can
    measure: how far above threshold the override was applied, what
    the gate would have done without the override, and how often the
    override is engaged across the verification corpus.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "would_have_skipped_reason": str(would_have_skipped_reason),
        "accuracy_source": str(accuracy_source),
        "post_enrichment_accuracy": (
            None
            if post_enrichment_accuracy is None
            else float(post_enrichment_accuracy)
        ),
        "baseline_accuracy": (
            None
            if baseline_accuracy is None
            else float(baseline_accuracy)
        ),
    }
    return (
        "GSO_TRIAL27_FORCE_LEVER_LOOP_V1 "
        + json.dumps(payload, sort_keys=True)
    )


__all__ = [
    "SkipDecision",
    "should_skip_starting_point_gate",
    "starting_point_gate_force_marker",
]
