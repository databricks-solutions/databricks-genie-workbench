"""GateVerdict and TransformerContext."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Union

from genie_space_optimizer.optimization.state_machine.proposal_store import (
    ProposalStore,
)
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
    TerminalRecord,
)


RejectionOutcome = Union[ProposalAttempt, TerminalRecord]


@dataclass(frozen=True, slots=True)
class GateVerdict:
    passed: bool
    success_record: Any | None = None
    rejection_outcome: RejectionOutcome | None = None

    def __post_init__(self):
        if self.passed and self.rejection_outcome is not None:
            raise ValueError("GateVerdict cannot be both passed and rejected")
        if not self.passed and self.rejection_outcome is None:
            raise ValueError("non-passing GateVerdict requires a rejection_outcome")

    @classmethod
    def success(cls, *, record: Any | None = None) -> "GateVerdict":
        return cls(passed=True, success_record=record, rejection_outcome=None)

    @classmethod
    def reject_terminal(cls, terminal: TerminalRecord) -> "GateVerdict":
        return cls(passed=False, rejection_outcome=terminal)

    @classmethod
    def reject_proposal(cls, attempt: ProposalAttempt) -> "GateVerdict":
        return cls(passed=False, rejection_outcome=attempt)


@dataclass(frozen=True, slots=True)
class ValidationContext:
    """Per-iteration context passed to ValidationGate predicates."""
    iteration: int
    run_id: str
    extras: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TransformerContext:
    """Per-iteration context passed to every transformer.

    Phase 2+3-followup adds typed-but-optional fields used by the
    production-wired transformer seams (see
    ``docs/llmdrivenarchitecture/v3/2026-05-22-production-seam-wire-in-plan.md``).
    All new fields carry safe defaults so the long tail of existing
    tests that construct ``TransformerContext`` with only the first
    three positionals continues to work unchanged.
    """
    iteration: int
    run_id: str
    validation_context: ValidationContext
    forbidden_signatures: tuple[str, ...] = ()
    # Trial 18 Step 3 — sibling channel to ``forbidden_signatures``.
    # Carries typed ``insufficient_repair_signature`` strings harvested
    # from prior iterations' ``AcceptanceDecisionRecord`` for the
    # ``kept_insufficient`` lane. The Stage 3 strategist consumes this
    # channel to avoid re-proposing the same (lever, patch_type, qid,
    # rca_kind, behavior) shape as a *sole* primary repair — but may
    # include it as part of a reinforcement bundle.
    insufficient_repair_signatures: tuple[str, ...] = ()
    # Trial 29 W29.1 — typed lever-loop feedback channel for
    # kit-forced inert patches. Populated by
    # :func:`inert_mechanism_history.harvest_sm_inert_mechanism_history`
    # at iteration end and consumed by Stage 3 synthesis so the LLM
    # picks from ``_structural_fix_mechanisms(rca) - rejected``.
    # Typed as ``tuple[Any, ...]`` here to avoid a circular import
    # (the typed model lives in
    # ``optimization.inert_mechanism_history``); callers always pass
    # ``tuple[InertMechanismHistory, ...]``.
    inert_mechanism_history: tuple[Any, ...] = ()
    extras: Mapping[str, Any] = field(default_factory=dict)
    # Bridge from ``ProposalAttempt.intent_id`` to the typed
    # ``RepairProposal`` body. Stage 3 writes; gates and the escalation
    # ladder read. Per-iteration scoped via default_factory.
    proposal_store: ProposalStore = field(default_factory=ProposalStore)

    # --- diagnose / cluster / synthesize lane -------------------------------
    schema_columns: tuple[str, ...] = ()
    # Trial 13i — provenance label for ``schema_columns``. Populated by
    # the SM + workbench seam via ``_derive_schema_columns``; consumed by
    # ``plan11_stage1_input_quality_marker`` so postmortems can tell
    # ``"empty"`` (deploy-block canary) from ``"typed_evidence_union"``
    # (healthy default path) without re-running the derivation.
    # Member of ``SCHEMA_COLUMNS_SOURCE_LABELS``.
    schema_columns_source: str = ""
    w: Any | None = None
    recent_diagnoses: tuple[Mapping[str, Any], ...] = ()
    schema_slice: Mapping[str, Any] = field(default_factory=dict)
    history: tuple[Mapping[str, Any], ...] = ()
    # Plan 12 typed RCA evidence keyed by QID. Threaded into the SM
    # canonical lane so ``diagnose_llm._invoke_stage1_llm`` can hand it
    # to ``build_stage1_evidence_card`` — symmetric to the Plan 11
    # batch path at ``optimizer.py:_build_plan11_failing_qids_from_typed_evidence``.
    # Without this field the SM lane silently dropped Plan 12's typed
    # evidence at the Stage 1 boundary (see Trial 12 / 13 postmortems:
    # ``evidence_card_empty:blame_set_empty,rca_evidence_empty``).
    rca_evidence_typed: Mapping[str, Any] = field(default_factory=dict)

    # --- gates lane ---------------------------------------------------------
    live_hard_qids: tuple[str, ...] = ()
    # Trial 20 Workstream E1 — counterfactual scanner output keyed by
    # ``intent_id``. The harness-direct path stamps ``passing_dependents``
    # on each proposal dict via ``_t24_counterfactual_scan``; the SM
    # path was previously a no-op because the proposals reach the SM
    # via ``RepairProposal.from_llm_output`` and the field never made
    # it onto ``patch_body``. E1 closes that gap by carrying the scan
    # output through the ctx itself. ``blast_radius_batch._assess_blast_
    # radius`` consults this mapping before reading ``patch_body`` so
    # missing-on-patch-body absences are now diagnosed by the
    # Trial 20 E2 ``passing_dependents_missing`` reject path instead
    # of silently absorbed by the safe-by-default fallback.
    passing_dependents_by_intent: Mapping[str, tuple[str, ...]] = field(
        default_factory=dict
    )
    high_collateral_risk_by_intent: Mapping[str, bool] = field(
        default_factory=dict
    )
    # Benchmarks (with ``required_tables`` / ``required_columns`` and
    # SQL-text fallback fields) the synthesize_llm transformer needs in
    # order to stamp ``passing_dependents`` on each typed proposal's
    # ``patch_body`` at creation time. Mirrors the legacy harness lane's
    # ``_t24_counterfactual_scan`` inputs (``benchmarks`` + ``ag`` +
    # ``prev_failure_qids``). Defaulting to an empty tuple keeps every
    # pre-Trial-20 SM unit test that constructs
    # ``TransformerContext`` with only the first three positionals
    # byte-stable; with the field populated, synthesize_llm stamps
    # ``passing_dependents`` so blast_radius_batch reads it from
    # ``patch_body`` (no ctx-side fallback required).
    benchmarks: tuple[Mapping[str, Any], ...] = ()
    ag_target_qids: tuple[str, ...] = ()
    prev_failure_qids: tuple[str, ...] = ()

    # --- apply lane ---------------------------------------------------------
    space_id: str = ""
    metadata_snapshot: Mapping[str, Any] = field(default_factory=dict)

    # --- evaluated / acceptance lane ---------------------------------------
    eval_qids: tuple[str, ...] = ()
    baseline_eval: Any | None = None
    eval_kwargs: Any | None = None
    stage_ctx: Any | None = None
    post_apply_arbiter_accuracy: float = 0.0
    baseline_arbiter_accuracy: float = 0.0
    min_gain_pp: float = 0.0
    baseline_eval_rows: tuple[Mapping[str, Any], ...] = ()
    post_apply_eval_rows: tuple[Mapping[str, Any], ...] = ()
