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

from typing import Any, Callable

from genie_space_optimizer.optimization.stages.gate_types import (
    PropagationWaitInput,
    PropagationWaitOutcome,
    SliceGateInput,
    SliceGateOutcome,
)

__all__ = [
    "run_propagation_wait_gate",
    "decide_slice_gate_should_run",
    "compute_slice_gate_effective_tolerance",
    "decide_slice_gate_post_eval",
]


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


def decide_slice_gate_should_run(
    inp: SliceGateInput,
    *,
    slice_min_reduction: float,
    broadness_small_corpus_rows: int = 30,
) -> SliceGateOutcome:
    """RCO-4b Phase B — pre-eval slice-gate gating decision.

    Mirrors the inline body at ``harness._run_gate_checks:13030-13099``.
    Pure function — no ``run_evaluation`` calls, no Spark, no prints.

    Returns a ``SliceGateOutcome`` with ``should_run`` populated; when
    ``should_run`` is False, ``skip_reason`` carries one of:

    - ``"legacy_gates_disabled"`` — ``ENABLE_LEGACY_SLICE_P0_GATES=False``
    - ``"slice_gate_disabled"`` — ``ENABLE_SLICE_GATE=False``
    - ``"slice_empty"`` — ``filter_benchmarks_by_scope`` returned 0 rows
    - ``"slice_too_broad"`` — broadness ratio exceeds the threshold

    The post-eval fields (``passed``, ``rollback_reason``,
    ``regression_judge``, ``effective_tolerance``) are left at their
    dataclass defaults (``None``). The harness owns those after
    ``run_evaluation`` and ``detect_regressions`` produce slice drops.

    ``broadness_small_corpus_rows`` defaults to 30 to match the legacy
    hardcoded threshold at ``harness:13076``. It is parameterized only
    so tests can probe other thresholds.
    """
    if not inp.legacy_gates_enabled:
        return SliceGateOutcome(
            should_run=False,
            skip_reason="legacy_gates_disabled",
        )
    if not inp.slice_gate_enabled:
        return SliceGateOutcome(
            should_run=False,
            skip_reason="slice_gate_disabled",
        )
    if inp.slice_benchmark_count <= 0:
        return SliceGateOutcome(
            should_run=False,
            skip_reason="slice_empty",
        )

    total = int(inp.full_benchmark_count)
    sliced = int(inp.slice_benchmark_count)
    broadness_ratio = (sliced / total) if total > 0 else 1.0
    is_small_corpus = total <= int(broadness_small_corpus_rows)
    threshold = 0.9 if is_small_corpus else (1.0 - float(slice_min_reduction))

    if broadness_ratio > threshold:
        return SliceGateOutcome(
            should_run=False,
            skip_reason="slice_too_broad",
            broadness_ratio=broadness_ratio,
        )

    return SliceGateOutcome(
        should_run=True,
        skip_reason=None,
        broadness_ratio=broadness_ratio,
    )
