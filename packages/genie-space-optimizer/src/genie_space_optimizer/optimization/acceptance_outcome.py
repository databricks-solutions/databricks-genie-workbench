"""Phase 1 (2026-05-16) — Acceptance Unification.

Wraps the strict-gate ``GainGateDecision`` + the canonical
``ControlPlaneAcceptance`` into a single ``AcceptanceOutcome`` that
every full-eval downstream surface reads from. Closes the three
independent computations cataloged in the audit (regressions list at
``harness.py:16638-16669``, marker payload at ``harness.py:16740``,
rollback-path ``acceptance_decision`` dict at ``harness.py:16846``).

Design contract:

* ``AcceptanceOutcome.accepted`` mirrors ``ControlPlaneAcceptance.accepted``
  exactly — control-plane is the canonical owner per the RCO-5 policy.
  The upstream gain-gate is a feeder; its accept/reject NEVER overrides
  the canonical decision. The Run A iter 1 anchor (gain-gate=False,
  control-plane=True) is the precise scenario this contract resolves.
* ``accepted_label`` is a pure function of ``reason_code`` — ``PASS`` /
  ``PASS WITH DEBT`` / ``FAIL (REGRESSION)``. Render-only — never an
  input to subsequent acceptance logic.
* ``regression_attribution`` is the list of ``{judge, previous, current,
  drop|delta, ...}`` dicts the harness used to build inline from
  ``_strict_decision.accepted`` and ``_control_plane_decision.accepted``.

This module is pure (no I/O, no globals, no env reads). The harness
constructs one ``AcceptanceOutcome`` per AG per iteration, immediately
after the canonical decisions are available and BEFORE the
accept/rollback fork.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.acceptance_policy import (
    GainGateDecision,
)
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    format_control_plane_acceptance_detail,
)


# Reason-code → operator label vocabulary. Closed set — every canonical
# reason code emitted by ``decide_control_plane_acceptance`` and
# ``decide_acceptance`` MUST map here.
_LABEL_PASS = "PASS"
_LABEL_PASS_WITH_DEBT = "PASS WITH DEBT"
_LABEL_FAIL = "FAIL (REGRESSION)"

_ACCEPTED_LABELS: dict[str, str] = {
    # Pure accept.
    "accepted": _LABEL_PASS,
    "accepted_with_attribution_drift": _LABEL_PASS,
    # Accept-with-debt branches (Cycle 14B / Phase 1 acceptance redesign).
    "accepted_with_regression_debt": _LABEL_PASS_WITH_DEBT,
    "accepted_with_partial_harvest_debt": _LABEL_PASS_WITH_DEBT,
    "accepted_with_attribution_drift_and_debt": _LABEL_PASS_WITH_DEBT,
    # Reject set (covered for completeness; harness rollback path also
    # produces these via the canonical decision).
    "rejected_insufficient_gain": _LABEL_FAIL,
    "rejected_regression": _LABEL_FAIL,
    "rejected_no_gain": _LABEL_FAIL,
    "target_fixed_offset_by_regression": _LABEL_FAIL,
    "target_fixed_with_unresolved_other_hard": _LABEL_FAIL,
    "post_arbiter_not_improved": _LABEL_FAIL,
    "missing_target_qids": _LABEL_FAIL,
    "rejected_missing_causal_target": _LABEL_FAIL,
    "missing_pre_rows": _LABEL_FAIL,
    "stale_or_candidate_pre_rows": _LABEL_FAIL,
}


def derive_accepted_label(reason_code: str) -> str:
    """Render-only mapping from canonical reason code → operator label.

    Unknown codes fall back to ``FAIL (REGRESSION)`` (the conservative
    label — historically the hardcode at ``harness.py:16740``).
    """
    return _ACCEPTED_LABELS.get(str(reason_code or ""), _LABEL_FAIL)


@dataclass(frozen=True, slots=True)
class AcceptanceOutcome:
    """Canonical typed view of one AG's full-eval acceptance verdict.

    Built once at the full-eval site (``harness.py``, immediately
    after ``_control_plane_decision`` is in scope and BEFORE the
    accept/rollback fork). Every downstream surface consumes this
    object — never the raw ``GainGateDecision`` / ``ControlPlaneAcceptance``
    fields independently.
    """

    accepted: bool
    reason_code: str
    accepted_label: str
    gain_gate_failed: bool
    control_plane_failed: bool
    target_fixed_qids: tuple[str, ...]
    regression_attribution: tuple[dict, ...] = field(default_factory=tuple)
    strict_decision: GainGateDecision | None = None
    control_plane_decision: ControlPlaneAcceptance | None = None


def _strict_regression_entry(strict: GainGateDecision) -> dict:
    """Mirror the harness's historical entry at ``harness.py:16642-16647``."""
    return {
        "judge": f"acceptance_gate ({strict.reason_code})",
        "previous": strict.post_arbiter_baseline,
        "current": strict.post_arbiter_candidate,
        "drop": -strict.delta_pp,
    }


def _control_plane_regression_entry(decision: ControlPlaneAcceptance) -> dict:
    """Mirror the harness's historical entry at ``harness.py:16653-16669``."""
    return {
        "judge": "control_plane_acceptance",
        "previous": decision.baseline_accuracy,
        "current": decision.candidate_accuracy,
        "delta": decision.delta_pp,
        "severity": "critical",
        "reason": decision.reason_code,
        "detail": format_control_plane_acceptance_detail(decision),
        "target_qids": list(decision.target_qids),
        "target_fixed_qids": list(decision.target_fixed_qids),
        "target_still_hard_qids": list(decision.target_still_hard_qids),
        "out_of_target_regressed_qids": list(
            decision.out_of_target_regressed_qids
        ),
    }


def build_acceptance_outcome(
    *,
    strict_decision: GainGateDecision,
    control_plane_decision: ControlPlaneAcceptance,
    enable_control_plane_acceptance: bool,
) -> AcceptanceOutcome:
    """Compose the typed outcome from the two upstream decisions.

    Pure. The canonical ``accepted`` flag follows the control-plane
    decision — gain-gate rejection alone does NOT roll back the
    iteration when the canonical accepts.
    """
    attribution: list[dict] = []
    if not strict_decision.accepted:
        attribution.append(_strict_regression_entry(strict_decision))
    if enable_control_plane_acceptance and not control_plane_decision.accepted:
        attribution.append(_control_plane_regression_entry(control_plane_decision))

    accepted = bool(control_plane_decision.accepted)
    reason_code = str(control_plane_decision.reason_code)
    label = derive_accepted_label(reason_code)

    return AcceptanceOutcome(
        accepted=accepted,
        reason_code=reason_code,
        accepted_label=label,
        gain_gate_failed=not bool(strict_decision.accepted),
        control_plane_failed=not bool(control_plane_decision.accepted),
        target_fixed_qids=tuple(
            str(q) for q in (control_plane_decision.target_fixed_qids or ())
        ),
        regression_attribution=tuple(attribution),
        strict_decision=strict_decision,
        control_plane_decision=control_plane_decision,
    )


def acceptance_decision_dict(outcome: AcceptanceOutcome) -> dict[str, Any]:
    """Render the per-AG ``acceptance_decision`` dict.

    Replaces BOTH historical literals — the pass path AND the rollback
    path. The rollback path previously omitted ``_canonical``; this
    serialiser unconditionally carries it.
    """
    decision = outcome.control_plane_decision
    if decision is None:
        raise ValueError(
            "acceptance_decision_dict requires an AcceptanceOutcome built "
            "via build_acceptance_outcome (control_plane_decision is unset)."
        )

    return {
        "accepted": bool(outcome.accepted),
        "reason": str(outcome.reason_code),
        "target_qids": list(decision.target_qids),
        "target_fixed_qids": list(decision.target_fixed_qids),
        "target_still_hard_qids": list(decision.target_still_hard_qids),
        "out_of_target_regressed_qids": list(
            decision.out_of_target_regressed_qids
        ),
        "regression_debt_qids": list(decision.regression_debt_qids),
        "soft_to_hard_regressed_qids": list(decision.soft_to_hard_regressed_qids),
        "passing_to_hard_regressed_qids": list(
            decision.passing_to_hard_regressed_qids
        ),
        "unresolved_target_debt_qids": list(
            getattr(decision, "unresolved_target_debt_qids", ()) or ()
        ),
        "_canonical": decision,
    }
