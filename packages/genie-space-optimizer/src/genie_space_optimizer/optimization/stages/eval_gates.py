"""RCO-4b — Pure helpers for the gate-stages inside
``harness._run_gate_checks``.

This module is the per-gate-stage extraction target named in RCO-4b's
phase roadmap (``docs/2026-05-12-rco-4b-phase-roadmap.md``). Each
helper is a pure function that:

1. Accepts already-resolved inputs (no ``apply_log`` dict, no
   ``WorkspaceClient``, no ``spark``).
2. Accepts side-effecting dependencies as injected callables (e.g.
   ``sleep_fn``, ``fetch_text_fn``, ``run_evaluation_fn``) so unit
   tests can drive the function deterministically.
3. Returns a typed ``*Outcome`` dataclass from
   ``stages.gate_types``.

The harness retains ownership of:

- The ``_audit_emit`` / ``_audit_persist`` closures.
- ``mlflow.end_run`` lifecycle calls.
- ``write_stage`` / ``write_iteration`` / ``update_provenance_gate`` /
  ``log_gate_feedback_on_traces`` spark writes.
- The early-return rollback control flow.

Helpers RETURN decisions; the harness ACTS on them.

Phase A ships ``run_propagation_wait_gate``. Phases B-E append
``run_slice_gate``, ``run_p0_gate``, ``run_asi_extraction``,
``run_baseline_drift_diagnostic``, ``run_full_eval_acceptance``.
"""

from __future__ import annotations

from typing import Callable

from genie_space_optimizer.optimization.stages.gate_types import (
    PropagationWaitInput,
    PropagationWaitOutcome,
)

__all__ = ["run_propagation_wait_gate"]


def run_propagation_wait_gate(
    inp: PropagationWaitInput,
    *,
    sleep_fn: Callable[[float], None],
    fetch_text_fn: Callable[[], str],
) -> PropagationWaitOutcome:
    """RCO-4b Phase A — pure propagation-wait gate.

    Mirrors the inline body at ``harness._run_gate_checks:12878-12946``
    line-by-line, with the two side-effecting dependencies
    (``time.sleep`` and ``fetch_space_config`` text extraction)
    injected as parameters.

    Behavior contract:

    - When ``expected_instruction_snippets`` is non-empty, poll
      ``fetch_text_fn`` every ``poll_interval_seconds`` until either a
      snippet is found (returns ``propagated=True``,
      ``audit_decision="confirmed"``, ``reason_code=None``) or the
      max-wait budget is exhausted (returns ``propagated=False``,
      ``audit_decision="waited_full_budget"``,
      ``reason_code="snippet_not_observed"``).
    - When ``expected_instruction_snippets`` is empty, sleep through
      the full budget without polling — there is no verifiable
      criterion. Returns ``audit_decision="waited_full_budget"``,
      ``reason_code="no_verifiable_snippet"``. Mirrors the legacy
      ``continue`` branch.
    - ``fetch_text_fn`` raising is treated as a transient failure and
      the next poll interval retries. Mirrors the legacy ``except
      Exception: continue``.
    - On confirmed-fast, the helper does NOT sleep the remaining
      budget. The harness's wiring may sleep further when emitting
      the audit row; the inline body does not (it falls through to
      the next gate immediately). ``elapsed_seconds`` is reported
      truthfully.
    """
    elapsed = 0.0
    expected = tuple(s for s in (inp.expected_instruction_snippets or ()) if s)
    propagated = False

    while elapsed < float(inp.max_wait_seconds):
        sleep_fn(float(inp.poll_interval_seconds))
        elapsed += float(inp.poll_interval_seconds)
        if not expected:
            # No verifiable snippet — fall through; just consume the
            # interval and re-check the budget. The legacy code uses
            # ``continue`` here.
            continue
        try:
            text = fetch_text_fn()
        except Exception:
            # Transient fetch failure; retry next interval.
            continue
        if isinstance(text, str) and text and any(s in text for s in expected):
            propagated = True
            break

    if expected and propagated:
        decision = "confirmed"
        reason_code: str | None = None
    else:
        decision = "waited_full_budget"
        reason_code = "no_verifiable_snippet" if not expected else "snippet_not_observed"

    return PropagationWaitOutcome(
        propagated=bool(propagated),
        elapsed_seconds=round(float(elapsed), 1),
        max_wait_seconds=int(inp.max_wait_seconds),
        applied_patches_count=int(inp.applied_patches_count),
        audit_decision=decision,
        reason_code=reason_code,
    )
