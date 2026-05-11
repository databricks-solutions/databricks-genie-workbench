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

import json
from typing import Any, Callable

from genie_space_optimizer.optimization.stages.gate_types import (
    AsiExtractionInput,
    AsiExtractionOutcome,
    BaselineDriftDiagnosticInput,
    BaselineDriftDiagnosticOutcome,
    P0GateInput,
    P0GateOutcome,
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
    "decide_p0_gate_should_run",
    "decide_p0_gate_post_eval",
    "forward_asi_extraction_audit",
    "build_baseline_drift_diagnostic",
    "decide_full_eval_acceptance",
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


def compute_slice_gate_effective_tolerance(
    inp: SliceGateInput,
    *,
    base_tol_standard: float,
    base_tol_small_corpus: float,
    small_corpus_threshold_rows: int,
) -> float:
    """RCO-4b Phase B — compute the effective slice-gate regression
    tolerance.

    Mirrors ``harness._run_gate_checks:13136-13144`` exactly:

        full_corpus = len(benchmarks)
        is_small_corpus = full_corpus < SLICE_GATE_SMALL_CORPUS_ROWS
        base = TOLERANCE_SMALL_CORPUS if is_small_corpus else TOLERANCE
        qw = 100.0 / max(full_corpus, 1)
        effective = max(base, noise_floor + 2.0, qw + 0.5)

    The harness consumes the return value as the ``threshold`` argument
    to ``detect_regressions``. Pure function; no I/O.
    """
    full_corpus = int(inp.full_benchmark_count)
    is_small_corpus = full_corpus < int(small_corpus_threshold_rows)
    base = (
        float(base_tol_small_corpus)
        if is_small_corpus
        else float(base_tol_standard)
    )
    qw = 100.0 / max(full_corpus, 1)
    return max(base, float(inp.noise_floor) + 2.0, qw + 0.5)


def decide_slice_gate_post_eval(
    inp: SliceGateInput,
    *,
    slice_drops: tuple[dict[str, Any], ...],
    effective_tolerance: float,
) -> SliceGateOutcome:
    """RCO-4b Phase B — post-eval slice-gate rollback decision.

    Mirrors the inline body at ``harness._run_gate_checks:13169-13222``.
    Pure function; the harness owns the ``run_evaluation`` /
    ``detect_regressions`` calls that produce ``slice_drops``, the
    ``print(...)`` audit-line render, and the spark-side writes
    (``update_provenance_gate``, ``log_gate_feedback_on_traces``).

    Returns ``passed=True`` when ``slice_drops`` is empty (the
    candidate clears the slice gate). Returns ``passed=False`` with
    ``rollback_reason="slice_gate: <judge>"`` and
    ``regression_judge=<judge>`` when any drop is present (the first
    drop wins, matching the legacy ``slice_drops[0]["judge"]`` read).
    """
    if not slice_drops:
        return SliceGateOutcome(
            should_run=False,  # not relevant post-eval; preserve default
            passed=True,
            rollback_reason=None,
            regression_judge=None,
            effective_tolerance=float(effective_tolerance),
        )
    first = dict(slice_drops[0])
    judge = str(first.get("judge", "") or "")
    return SliceGateOutcome(
        should_run=False,
        passed=False,
        rollback_reason=f"slice_gate: {judge}",
        regression_judge=judge,
        effective_tolerance=float(effective_tolerance),
    )


def decide_p0_gate_should_run(inp: P0GateInput) -> P0GateOutcome:
    """RCO-4b Phase C — pre-eval P0-gate gating decision.

    Mirrors the inline body at ``harness._run_gate_checks:13276-13290``.
    Pure function — no ``run_evaluation`` calls, no Spark, no prints.

    Returns a ``P0GateOutcome`` with ``should_run`` populated; when
    ``should_run`` is False, ``skip_reason`` carries one of:

    - ``"legacy_gates_disabled"`` — ``ENABLE_LEGACY_SLICE_P0_GATES=False``
    - ``"p0_empty"`` — ``filter_benchmarks_by_scope(benchmarks, "p0")``
      returned 0 rows

    The post-eval fields (``passed``, ``failure_count``, ``rollback_reason``)
    are left at their dataclass defaults. The harness owns those after
    ``run_evaluation`` produces the failure list.

    Note on banner parity: the legacy code prints a "SKIPPED (Task 2)"
    banner for ``legacy_gates_disabled`` but does NOT print a banner
    for ``p0_empty`` (it silently falls through because
    ``if p0_benchmarks:`` is False). The harness's wiring preserves
    this asymmetry — the helper just records the reason; the harness
    decides whether to render.
    """
    if not inp.legacy_gates_enabled:
        return P0GateOutcome(
            should_run=False,
            skip_reason="legacy_gates_disabled",
        )
    if int(inp.p0_benchmark_count) <= 0:
        return P0GateOutcome(
            should_run=False,
            skip_reason="p0_empty",
        )
    return P0GateOutcome(
        should_run=True,
        skip_reason=None,
    )


def decide_p0_gate_post_eval(
    inp: P0GateInput,
    *,
    p0_failures_count: int,
) -> P0GateOutcome:
    """RCO-4b Phase C — post-eval P0-gate rollback decision.

    Mirrors the inline body at ``harness._run_gate_checks:13321-13343``.
    Pure function; the harness owns the ``run_evaluation`` call that
    produces ``p0_result``, the ``write_iteration`` spark write, the
    ``print(...)`` audit-line render, and the early-return control
    flow.

    Returns ``passed=True`` when ``p0_failures_count <= 0`` (the
    candidate clears the P0 gate). Returns ``passed=False`` with
    ``rollback_reason="p0_gate: N failures"`` and ``failure_count=N``
    when N > 0, matching the legacy ``return {"passed": False,
    "rollback_reason": f"p0_gate: {len(p0_failures)} failures", ...}``
    line.
    """
    count = int(p0_failures_count)
    if count <= 0:
        return P0GateOutcome(
            should_run=False,  # post-eval doesn't drive should_run; preserve default
            passed=True,
            failure_count=0,
            rollback_reason=None,
        )
    return P0GateOutcome(
        should_run=False,
        passed=False,
        failure_count=count,
        rollback_reason=f"p0_gate: {count} failures",
    )


def forward_asi_extraction_audit(
    inp: AsiExtractionInput,
) -> AsiExtractionOutcome:
    """RCO-4b Phase D — forward the ASI-extraction audit row that
    ``run_evaluation`` stamped on its result.

    Mirrors the inline body at ``harness._run_gate_checks:~13731``.
    Pure function — does not call ``_audit_emit``; returns the audit
    payload as a typed outcome and lets the harness decide whether
    and how to render it.

    Returns ``should_emit=False`` when ``inp.raw_audit`` is None or
    not a dict (matches the legacy ``isinstance(_asi_audit_1, dict)``
    guard). Otherwise returns ``should_emit=True`` with all five
    audit fields populated using the same ``or``-fallback defaults as
    the legacy code:

      - ``stage_letter`` defaults to ``"C"``.
      - ``gate_name`` defaults to ``"asi_extraction"``.
      - ``decision`` defaults to ``"ok"``.
      - ``reason_code`` defaults to ``None``.
      - ``metrics`` is the parsed ``metrics_json`` payload:
        - If it's a JSON string, it's ``json.loads``-parsed; parse
          failures set ``metrics=None``.
        - If it's already a dict, it passes through.
        - If it's any other type (list, int, etc.), it's set to None.
    """
    raw = inp.raw_audit
    if not isinstance(raw, dict):
        return AsiExtractionOutcome(should_emit=False)

    metrics_value: Any = raw.get("metrics_json")
    if isinstance(metrics_value, str):
        try:
            metrics_value = json.loads(metrics_value)
        except (TypeError, ValueError):
            metrics_value = None
    metrics_out = metrics_value if isinstance(metrics_value, dict) else None

    return AsiExtractionOutcome(
        should_emit=True,
        stage_letter=raw.get("stage_letter") or "C",
        gate_name=raw.get("gate_name") or "asi_extraction",
        decision=raw.get("decision") or "ok",
        reason_code=raw.get("reason_code"),
        metrics=metrics_out,
    )


# Import at module level so the default keyword arg captures the production
# function. Tests inject a stub via the keyword-only ``decide_drift_fn`` arg.
from genie_space_optimizer.optimization.acceptance_policy import (
    decide_baseline_drift as _default_decide_baseline_drift,
)


def build_baseline_drift_diagnostic(
    inp: BaselineDriftDiagnosticInput,
    *,
    decide_drift_fn: Callable[..., Any] = _default_decide_baseline_drift,
) -> BaselineDriftDiagnosticOutcome:
    """RCO-4b Phase D — wrap ``decide_baseline_drift`` and package the
    audit-row + log-line payload.

    Mirrors the inline body at ``harness._run_gate_checks:~13830``.
    Pure function — does not call logger, does not call ``_audit_emit``.
    Returns a typed outcome the harness then emits.

    The ``decide_drift_fn`` parameter is keyword-only and defaults to
    the production ``acceptance_policy.decide_baseline_drift``. Tests
    inject a stub to exercise both branches deterministically.

    When the underlying decision is not triggered, the outcome carries
    ``triggered=False`` and all emission fields at their defaults.
    When triggered, the outcome carries the formatted ``log_line``
    (matching the legacy ``logger.info(...)`` format byte-for-byte)
    and the four-key ``audit_metrics`` dict that the legacy
    ``_audit_emit(metrics=...)`` payload used.
    """
    drift = decide_drift_fn(
        post_arbiter_current=float(inp.current_post_arbiter_accuracy),
        prev_iter_pre_accept_baseline=inp.prev_iter_pre_accept_baseline,
        threshold_pp=float(inp.diagnostic_threshold_pp),
    )
    if not drift.triggered:
        return BaselineDriftDiagnosticOutcome(triggered=False)

    prev_baseline_for_log = float(drift.prev_iter_pre_accept_baseline or 0.0)
    log_line = (
        "BASELINE DRIFT [%s]: iter %d post-arbiter %.1f%% is %.1fpp "
        "below the previous iteration's pre-acceptance baseline "
        "(%.1f%%). Logging suspected_stale_baseline diagnostic; "
        "iteration continues normally."
    ) % (
        inp.ag_id,
        int(inp.iteration),
        float(drift.post_arbiter_current),
        float(drift.delta_pp),
        prev_baseline_for_log,
    )

    audit_metrics: dict[str, float] = {
        "post_arbiter_candidate": float(drift.post_arbiter_current),
        "prev_iter_pre_accept_baseline": (
            float(drift.prev_iter_pre_accept_baseline)
            if drift.prev_iter_pre_accept_baseline is not None
            else 0.0
        ),
        "delta_pp": float(drift.delta_pp),
        "threshold_pp": float(drift.threshold_pp),
    }

    return BaselineDriftDiagnosticOutcome(
        triggered=True,
        delta_pp=float(drift.delta_pp),
        audit_metrics=audit_metrics,
        reason_code=drift.reason_code,
        log_line=log_line,
    )


# Import the new types at module level for the helper below.
from genie_space_optimizer.optimization.stages.gate_types import (
    FullEvalAcceptanceInput as _FullEvalAcceptanceInput,
    FullEvalAcceptanceOutcome as _FullEvalAcceptanceOutcome,
)


_CONTROL_PLANE_REASON_TO_BRANCH = {
    "accepted_with_attribution_drift": "accept_with_drift",
    "accepted_with_regression_debt": "accept_with_debt",
}


def decide_full_eval_acceptance(
    inp: _FullEvalAcceptanceInput,
) -> _FullEvalAcceptanceOutcome:
    """RCO-4b Phase E — consolidate upstream decisions into the
    full-eval acceptance verdict.

    Mirrors the verdict-consolidation logic at
    ``harness._run_gate_checks:13987-14524 + 14657-14673``. Pure
    function — no logger calls, no ``_audit_emit`` calls, no Spark,
    no prints.

    The harness pre-computes the strict decision (from
    ``acceptance_policy.decide_acceptance``), the Task 4 per-question
    transition verdict, and the control-plane decision, then
    populates ``inp.regressions`` from the three sources. The helper
    decides verdict purely on ``len(inp.regressions)`` and packages
    the three audit-metrics payloads (verdict, rollback, accept) for
    the harness to emit.

    Verdict rules:
      - ``regressions`` is empty → accepted, branch determined by
        ``control_plane_reason_code``.
      - ``regressions`` is non-empty → rollback, branch="rollback",
        rollback_reason="full_eval: <first regression's judge>".

    Branch vocabulary:
      - ``"rollback"`` — regressions non-empty.
      - ``"accept_with_drift"`` — ``control_plane_reason_code ==
        "accepted_with_attribution_drift"`` and regressions empty.
      - ``"accept_with_debt"`` — ``control_plane_reason_code ==
        "accepted_with_regression_debt"`` and regressions empty.
      - ``"accept"`` — fallback (accepted/default reason codes).
    """
    # Verdict-time audit metrics fire regardless of pass/fail.
    # Mirrors harness.py:13987-14000.
    verdict_metrics = {
        "delta_pp": float(inp.strict_decision_delta_pp),
        "min_gain_pp": float(inp.strict_decision_min_gain_pp),
        "post_arbiter_candidate": float(inp.strict_decision_post_arbiter_candidate),
        "post_arbiter_baseline": float(inp.strict_decision_post_arbiter_baseline),
        "previous_pre_arbiter": float(inp.pre_arbiter_baseline),
        "previous_post_arbiter": float(inp.strict_decision_post_arbiter_baseline),
    }

    if inp.regressions:
        first = inp.regressions[0]
        try:
            first_judge = str(first.get("judge", "") or "")
        except AttributeError:
            first_judge = ""
        rollback_reason = f"full_eval: {first_judge}"
        # Rollback-time audit metrics fire only on the rollback branch.
        # Mirrors harness.py:14506-14524.
        rollback_metrics: dict[str, Any] = {
            "regression_count": len(inp.regressions),
            "post_arbiter_candidate": float(inp.strict_decision_post_arbiter_candidate),
            "post_arbiter_baseline": float(inp.strict_decision_post_arbiter_baseline),
            "delta_pp": float(inp.strict_decision_delta_pp),
            "min_gain_pp": float(inp.strict_decision_min_gain_pp),
            "pre_arbiter_candidate": float(inp.pre_arbiter_candidate),
            "pre_arbiter_baseline": float(inp.pre_arbiter_baseline),
            "diagnostic_regressions": list(inp.diagnostic_regression_judges),
        }
        return _FullEvalAcceptanceOutcome(
            accepted=False,
            branch="rollback",
            reason_code=str(inp.strict_decision_reason_code),
            rollback_reason=rollback_reason,
            regression_count=len(inp.regressions),
            verdict_audit_metrics=verdict_metrics,
            rollback_audit_metrics=rollback_metrics,
            accept_audit_metrics=None,
        )

    # Accept branch — derive label from control_plane_reason_code.
    branch = _CONTROL_PLANE_REASON_TO_BRANCH.get(
        inp.control_plane_reason_code,
        "accept",
    )
    # Accept-time audit metrics fire only on the accept branch.
    # Mirrors harness.py:14657-14673.
    accept_metrics: dict[str, Any] = {
        "post_arbiter_candidate": float(inp.strict_decision_post_arbiter_candidate),
        "post_arbiter_baseline": float(inp.strict_decision_post_arbiter_baseline),
        "delta_pp": float(inp.strict_decision_delta_pp),
        "min_gain_pp": float(inp.strict_decision_min_gain_pp),
        "pre_arbiter_candidate": float(inp.pre_arbiter_candidate),
        "pre_arbiter_baseline": float(inp.pre_arbiter_baseline),
        "diagnostic_regressions": list(inp.diagnostic_regression_judges),
    }
    return _FullEvalAcceptanceOutcome(
        accepted=True,
        branch=branch,
        reason_code=str(inp.strict_decision_reason_code),
        rollback_reason=None,
        regression_count=0,
        verdict_audit_metrics=verdict_metrics,
        rollback_audit_metrics=None,
        accept_audit_metrics=accept_metrics,
    )
