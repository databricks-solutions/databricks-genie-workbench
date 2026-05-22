"""Per-stage typed records attached to QuestionStateInIteration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage


@dataclass(frozen=True, slots=True)
class HardQidSeenRecord(JsonRoundTrip):
    eval_row_id: str
    predicate: Literal["row_is_hard_failure"]
    score: float
    baseline_sql: str
    expected_shape: str
    iteration_first_seen: int


@dataclass(frozen=True, slots=True)
class DiagnosisRecord(JsonRoundTrip):
    source: Literal["plan11_stage1", "legacy_classifier"]
    rca_kind_label: str
    evidence_summary: str
    observed_failure: str
    expected_sql_shape: str
    confidence: Literal["high", "medium", "low"]
    rca_card_id: str


@dataclass(frozen=True, slots=True)
class ClusterMembershipRecord(JsonRoundTrip):
    cluster_id: str
    ag_id: str
    co_member_qids: tuple[str, ...]
    effective_target_lever: int
    routing_evidence_kind: str  # mandatory; "" forbidden by emit-time validator


ProposalAttemptOutcome = Literal[
    "applied", "accepted", "rolled_back",
    "contract_failed", "validator_rejected",
    "blast_radius_rejected", "applyability_rejected",
    "structural_repair_rejected", "escalated",
]


@dataclass(frozen=True, slots=True)
class ProposalAttempt(JsonRoundTrip):
    attempt_index: int
    intent_id: str
    patch_type: str
    deepest_stage_in_attempt: FunnelStage
    outcome: ProposalAttemptOutcome
    outcome_reason: str
    escalated_to_attempt_index: int | None = None
    patch_outcome_id: str | None = None

    @classmethod
    def from_json(cls, payload: dict) -> "ProposalAttempt":  # type: ignore[override]
        return cls(
            attempt_index=int(payload["attempt_index"]),
            intent_id=str(payload["intent_id"]),
            patch_type=str(payload["patch_type"]),
            deepest_stage_in_attempt=FunnelStage(payload["deepest_stage_in_attempt"]),
            outcome=payload["outcome"],
            outcome_reason=str(payload["outcome_reason"]),
            escalated_to_attempt_index=payload.get("escalated_to_attempt_index"),
            patch_outcome_id=payload.get("patch_outcome_id"),
        )


@dataclass(frozen=True, slots=True)
class AppliedRecord(JsonRoundTrip):
    applied_at_ms: int
    apply_call_id: str
    proposal_attempt_index: int
    applied_intent_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvaluatedRecord(JsonRoundTrip):
    pre_apply_score: float
    post_apply_score: float
    pre_apply_sql: str
    post_apply_sql: str
    eval_row_id_post: str


@dataclass(frozen=True, slots=True)
class AcceptanceDecisionRecord(JsonRoundTrip):
    decision: Literal["accepted", "rolled_back"]
    arbiter_reason: str
    target_fixed: bool
    collateral_regressions: tuple[str, ...]


TerminalKind = Literal[
    "OPTIMIZER_IMPROVED",
    "OPTIMIZER_TRIED_NO_GAIN",
    "OPTIMIZER_STALLED_NO_APPLIED_PATCHES",
    "OPTIMIZER_NO_CANDIDATES",
    "OPTIMIZER_SKIPPED_INPUT_GAP",
    "OPTIMIZER_STALLED_SAFE_NOOP",
]


@dataclass(frozen=True, slots=True)
class TerminalRecord(JsonRoundTrip):
    kind: TerminalKind
    reason: str
    deepest_stage_reached: FunnelStage
    forbidden_signature: str

    @classmethod
    def from_json(cls, payload: dict) -> "TerminalRecord":  # type: ignore[override]
        return cls(
            kind=payload["kind"],
            reason=str(payload["reason"]),
            deepest_stage_reached=FunnelStage(payload["deepest_stage_reached"]),
            forbidden_signature=str(payload["forbidden_signature"]),
        )


TransitionKind = Literal["llm", "validation_gate", "batch"]


@dataclass(frozen=True, slots=True)
class StageTransition(JsonRoundTrip):
    from_stage: FunnelStage
    to_stage: FunnelStage
    at_ms: int
    transformer_name: str
    transition_kind: TransitionKind
    proposal_attempt_index: int | None = None
    reason: str = ""

    @classmethod
    def from_json(cls, payload: dict) -> "StageTransition":  # type: ignore[override]
        return cls(
            from_stage=FunnelStage(payload["from_stage"]),
            to_stage=FunnelStage(payload["to_stage"]),
            at_ms=int(payload["at_ms"]),
            transformer_name=str(payload["transformer_name"]),
            transition_kind=payload["transition_kind"],
            proposal_attempt_index=payload.get("proposal_attempt_index"),
            reason=str(payload.get("reason") or ""),
        )
