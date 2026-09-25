"""Internal Delta row representation; no credentials or process-local authority."""
from dataclasses import dataclass, field
from datetime import datetime
from backend.services.version_control.contracts import (
    BindingRef, CoordinationState, Digest, Heads, JsonObject, NonnegativeInt,
    PatchStage, UUIDString, WireValue,
)


@dataclass(frozen=True)
class CoordinationRow(WireValue):
    binding: BindingRef
    updated_at: datetime
    generation: NonnegativeInt = 0
    row_version: NonnegativeInt = 0
    state: CoordinationState = CoordinationState.IDLE
    holder: str | None = None
    attempt_id: UUIDString | None = None
    executor_kind: str | None = None
    executor_ref: str | None = None
    lease_expires_at: datetime | None = None
    active_operation_id: UUIDString | None = None
    idempotency_key: str | None = None
    request_digest: Digest | None = None
    approval_id: UUIDString | None = None
    approval_digest: Digest | None = None
    approval_consumption_published: bool = False
    pre_version_id: UUIDString | None = None
    preimage_digest: Digest | None = None
    create_intent_event_id: UUIDString | None = None
    expected_base_fingerprint: Digest | None = None
    admitted_at: datetime | None = None
    mutation_stage: PatchStage | None = None
    checkpoint: JsonObject | None = None
    unresolved: bool = False
    quarantine_reason: str | None = None
    termination_evidence: JsonObject | None = None
    heads: Heads = field(default_factory=lambda: Heads(None, None, None))
    observed_sequence: NonnegativeInt = 0

