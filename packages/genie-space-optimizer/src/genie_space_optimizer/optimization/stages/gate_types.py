"""RCO-4 — typed input/output dataclasses for the three Stage-6 pure
helpers extracted out of harness.py.

Three pairs:
  * BlastRadiusProductionInput / BlastRadiusProductionOutcome
  * NarrowReplacementInput / NarrowReplacementOutcome
  * ApplyabilityGateInput / ApplyabilityGateOutcome

All six dataclasses are frozen and JSON-roundtrippable so fixture
pairs can serialize them cleanly. Lives in its own module to keep
``stages/gates.py`` under the hold-in-context threshold.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True)
class BlastRadiusProductionInput(JsonRoundTrip):
    ag_id: str
    ag_target_qids: tuple[str, ...]
    live_hard_qids: tuple[str, ...]
    max_outside_target: int
    patches: tuple[dict[str, Any], ...]

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "ag_id": self.ag_id,
            "ag_target_qids": list(self.ag_target_qids),
            "live_hard_qids": list(self.live_hard_qids),
            "max_outside_target": int(self.max_outside_target),
            "patches": [dict(p) for p in self.patches],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "BlastRadiusProductionInput":  # type: ignore[override]
        return cls(
            ag_id=str(payload.get("ag_id") or ""),
            ag_target_qids=tuple(str(q) for q in (payload.get("ag_target_qids") or [])),
            live_hard_qids=tuple(str(q) for q in (payload.get("live_hard_qids") or [])),
            max_outside_target=int(payload.get("max_outside_target") or 0),
            patches=tuple(dict(p) for p in (payload.get("patches") or [])),
        )


@dataclass(frozen=True)
class BlastRadiusProductionOutcome(JsonRoundTrip):
    kept: tuple[dict[str, Any], ...] = ()
    dropped: tuple[dict[str, Any], ...] = ()

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "kept": [dict(p) for p in self.kept],
            "dropped": [dict(p) for p in self.dropped],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "BlastRadiusProductionOutcome":  # type: ignore[override]
        return cls(
            kept=tuple(dict(p) for p in (payload.get("kept") or [])),
            dropped=tuple(dict(p) for p in (payload.get("dropped") or [])),
        )


@dataclass(frozen=True)
class NarrowReplacementInput(JsonRoundTrip):
    ag_id: str
    ag_rca_id: str
    ag_target_qids: tuple[str, ...]
    ag_root_cause: str
    blast_dropped: tuple[dict[str, Any], ...]
    qid_to_question_text: dict[str, str] = field(default_factory=dict)
    qid_to_reference_sql: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "ag_id": self.ag_id,
            "ag_rca_id": self.ag_rca_id,
            "ag_target_qids": list(self.ag_target_qids),
            "ag_root_cause": self.ag_root_cause,
            "blast_dropped": [dict(d) for d in self.blast_dropped],
            "qid_to_question_text": dict(self.qid_to_question_text),
            "qid_to_reference_sql": dict(self.qid_to_reference_sql),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "NarrowReplacementInput":  # type: ignore[override]
        return cls(
            ag_id=str(payload.get("ag_id") or ""),
            ag_rca_id=str(payload.get("ag_rca_id") or ""),
            ag_target_qids=tuple(
                str(q) for q in (payload.get("ag_target_qids") or [])
            ),
            ag_root_cause=str(payload.get("ag_root_cause") or ""),
            blast_dropped=tuple(
                dict(d) for d in (payload.get("blast_dropped") or [])
            ),
            qid_to_question_text=dict(payload.get("qid_to_question_text") or {}),
            qid_to_reference_sql=dict(payload.get("qid_to_reference_sql") or {}),
        )


@dataclass(frozen=True)
class NarrowReplacementOutcome(JsonRoundTrip):
    narrow_survivors: tuple[dict[str, Any], ...] = ()
    structural_causal_dropped: tuple[dict[str, Any], ...] = ()
    halt_no_structural_alternative: bool = False

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "narrow_survivors": [dict(s) for s in self.narrow_survivors],
            "structural_causal_dropped": [
                dict(d) for d in self.structural_causal_dropped
            ],
            "halt_no_structural_alternative": bool(self.halt_no_structural_alternative),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "NarrowReplacementOutcome":  # type: ignore[override]
        return cls(
            narrow_survivors=tuple(
                dict(s) for s in (payload.get("narrow_survivors") or [])
            ),
            structural_causal_dropped=tuple(
                dict(d) for d in (payload.get("structural_causal_dropped") or [])
            ),
            halt_no_structural_alternative=bool(
                payload.get("halt_no_structural_alternative") or False
            ),
        )


@dataclass(frozen=True)
class ApplyabilityGateInput(JsonRoundTrip):
    candidates: tuple[dict[str, Any], ...]
    metadata_snapshot: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "candidates": [dict(c) for c in self.candidates],
            "metadata_snapshot": dict(self.metadata_snapshot),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ApplyabilityGateInput":  # type: ignore[override]
        return cls(
            candidates=tuple(dict(c) for c in (payload.get("candidates") or [])),
            metadata_snapshot=dict(payload.get("metadata_snapshot") or {}),
        )


@dataclass(frozen=True)
class ApplyabilityGateOutcome(JsonRoundTrip):
    applyable: tuple[dict[str, Any], ...] = ()
    rejected: tuple[dict[str, Any], ...] = ()

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "applyable": [dict(c) for c in self.applyable],
            "rejected": [dict(d) for d in self.rejected],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ApplyabilityGateOutcome":  # type: ignore[override]
        return cls(
            applyable=tuple(dict(c) for c in (payload.get("applyable") or [])),
            rejected=tuple(dict(d) for d in (payload.get("rejected") or [])),
        )


# ---------------------------------------------------------------------------
# RCO-4b — Typed input/output contracts for ``_run_gate_checks`` stages
# ---------------------------------------------------------------------------
#
# Phase A defines all six stage contracts up-front so Phases B/C/D/E can
# implement the corresponding helpers independently. Only PropagationWait*
# is consumed in Phase A.
#
# Cross-references:
#   - Inventory: docs/2026-05-12-rco-4b-gate-stage-inventory.md
#   - Phase roadmap: docs/2026-05-12-rco-4b-phase-roadmap.md


@dataclass(frozen=True)
class PropagationWaitInput(JsonRoundTrip):
    """Input for ``run_propagation_wait_gate``.

    The pure helper consumes already-resolved values; the harness
    extracts ``applied_patches_count`` / ``expected_instruction_snippets``
    / ``has_dictionary_changes`` from ``apply_log`` before calling.
    """
    ag_id: str
    max_wait_seconds: int
    poll_interval_seconds: float
    applied_patches_count: int
    patched_objects: tuple[str, ...]
    expected_instruction_snippets: tuple[str, ...]
    has_dictionary_changes: bool


@dataclass(frozen=True)
class PropagationWaitOutcome(JsonRoundTrip):
    """Outcome of ``run_propagation_wait_gate``.

    The helper does not perform I/O. The harness reads ``elapsed_seconds``
    and ``audit_decision`` to emit the audit row and, if applicable,
    sleep the remainder of the max-wait budget.

    Field shape mirrors the real harness ``_audit_emit`` calls at
    ``harness._run_gate_checks:12915-12946``:
      * Confirmed branch: ``audit_decision="confirmed"``, ``reason_code=None``.
      * Full-budget branch: ``audit_decision="waited_full_budget"``,
        ``reason_code`` is either ``"no_verifiable_snippet"`` (when
        ``expected_instruction_snippets`` was empty) or
        ``"snippet_not_observed"`` (when polling timed out without
        finding any expected snippet).
    """
    propagated: bool
    elapsed_seconds: float
    max_wait_seconds: int
    applied_patches_count: int
    audit_decision: str  # "confirmed" or "waited_full_budget"
    reason_code: str | None = None


@dataclass(frozen=True)
class SliceGateInput(JsonRoundTrip):
    """Input for ``run_slice_gate`` (Phase B).

    The two-step decision (pre-eval / post-eval) is handled by a
    helper that returns a ``should_run`` flag and, on the second
    invocation, the rollback decision. Phase B will split this into
    two helpers if needed; the contract here is the union of both.
    """
    ag_id: str
    run_id: str
    iteration: int
    all_benchmark_qids: tuple[str, ...]
    prev_failure_qids: tuple[str, ...]
    affected_question_ids: tuple[str, ...]
    baseline_passing_qids_known: bool
    slice_benchmark_count: int
    full_benchmark_count: int
    best_accuracy: float
    noise_floor: float
    legacy_gates_enabled: bool
    slice_gate_enabled: bool


@dataclass(frozen=True)
class SliceGateOutcome(JsonRoundTrip):
    """Outcome of ``run_slice_gate`` (Phase B)."""
    should_run: bool
    skip_reason: str | None = None
    effective_tolerance: float | None = None
    broadness_ratio: float | None = None
    passed: bool | None = None
    rollback_reason: str | None = None
    regression_judge: str | None = None


@dataclass(frozen=True)
class P0GateInput(JsonRoundTrip):
    """Input for ``run_p0_gate`` (Phase C)."""
    ag_id: str
    run_id: str
    iteration: int
    p0_benchmark_count: int
    legacy_gates_enabled: bool


@dataclass(frozen=True)
class P0GateOutcome(JsonRoundTrip):
    """Outcome of ``run_p0_gate`` (Phase C)."""
    should_run: bool
    skip_reason: str | None = None
    passed: bool | None = None
    failure_count: int = 0
    rollback_reason: str | None = None


@dataclass(frozen=True)
class AsiExtractionInput(JsonRoundTrip):
    """Input for ``run_asi_extraction`` (Phase D)."""
    ag_id: str
    applied_instruction_texts: tuple[str, ...]
    post_eval_pre_arbiter_accuracy: float
    post_eval_post_arbiter_accuracy: float
    baseline_post_arbiter_accuracy: float


@dataclass(frozen=True)
class AsiExtractionOutcome(JsonRoundTrip):
    """Outcome of ``run_asi_extraction`` (Phase D)."""
    triggered: bool
    gate_name: str
    audit_metrics: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class BaselineDriftDiagnosticInput(JsonRoundTrip):
    """Input for ``run_baseline_drift_diagnostic`` (Phase D)."""
    ag_id: str
    iteration: int
    prev_iter_pre_accept_baseline: float
    current_post_arbiter_accuracy: float
    diagnostic_threshold_pp: float


@dataclass(frozen=True)
class BaselineDriftDiagnosticOutcome(JsonRoundTrip):
    """Outcome of ``run_baseline_drift_diagnostic`` (Phase D)."""
    triggered: bool
    delta_pp: float
    audit_metrics: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class FullEvalAcceptanceInput(JsonRoundTrip):
    """Input for ``run_full_eval_acceptance`` (Phase E).

    Phase E may split this into Part-1 (eval-run) and Part-2 (decide)
    inputs; this is the union shape for the typed-contract guard.
    """
    ag_id: str
    iteration: int
    full_eval_post_arbiter_accuracy: float
    baseline_post_arbiter_accuracy: float
    min_gain_pp: float
    target_qids: tuple[str, ...]
    cumulative_regression_debt: int


@dataclass(frozen=True)
class FullEvalAcceptanceOutcome(JsonRoundTrip):
    """Outcome of ``run_full_eval_acceptance`` (Phase E).

    The full canonical ``ControlPlaneAcceptance`` instance is constructed
    by the helper and returned alongside this outcome via a sibling
    field (Phase E adds it). For Phase A's typed-contract guard we only
    need the accept/reject + branch tag here.
    """
    accepted: bool
    branch: str
    rollback_reason: str | None = None
