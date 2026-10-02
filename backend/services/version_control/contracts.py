"""VC/1.0 immutable values and synchronous, dependency-free domain ports.

Internal VC identifiers are canonical UUID strings. Platform identifiers (workspace,
physical space, principal and Job run) remain opaque strings, as on their native APIs.
UTC timestamps use ISO 8601 with Z on the wire; nullable fields stay explicit except
the optional ApiError fields. Policy-owned JSON is deeply frozen, not interpreted.

The spec names some port values without field schemas. Those values carry explicit
binding, request and evidence references here, without implementing authorization.
HeadUpdate None means unchanged, not permission to clear a head. ExecutorContext
and AuthorizationGrant are internal capabilities and are never wire inputs/outputs.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from hashlib import sha256
import json
import math
import re
from types import MappingProxyType, UnionType
from typing import Annotated, Generic, Literal, Protocol, TypeVar, Union, get_args, get_origin, get_type_hints
from uuid import NAMESPACE_URL, UUID, uuid5


UUIDString = Annotated[str, "uuid"]
Digest = Annotated[str, "sha256"]
PositiveInt = Annotated[int, "positive"]
NonnegativeInt = Annotated[int, "nonnegative"]
JsonObject = Mapping[str, object]
ValueType = TypeVar("ValueType")


class DriftState(str, Enum):
    CLEAN = "clean"
    EXTERNAL_AHEAD = "external_ahead"
    DESIRED_AHEAD = "desired_ahead"
    DIVERGED = "diverged"
    UNKNOWN = "unknown"
    UNREACHABLE = "unreachable"
    APPLIED_UNVERIFIED = "applied_unverified"
    CONFLICTED = "conflicted"


class OperationStatus(str, Enum):
    REQUESTED = "requested"
    PREIMAGE_CAPTURED = "preimage_captured"
    APPLY_ATTEMPTED = "apply_attempted"
    APPLIED_UNVERIFIED = "applied_unverified"
    APPLIED_PARTIAL = "applied_partial"
    CONFIRMED = "confirmed"
    CONFLICTED = "conflicted"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    COMPENSATION_ATTEMPTED = "compensation_attempted"
    COMPENSATED = "compensated"
    NOOP = "noop"


class FactStatus(str, Enum):
    REQUESTED = "requested"
    PREIMAGE_CAPTURED = "preimage_captured"
    APPLY_ATTEMPTED = "apply_attempted"
    APPLIED_UNVERIFIED = "applied_unverified"
    APPLIED_PARTIAL = "applied_partial"
    CONFIRMED = "confirmed"
    CONFLICTED = "conflicted"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    COMPENSATION_ATTEMPTED = "compensation_attempted"
    COMPENSATED = "compensated"
    NOOP = "noop"
    APPROVED = "approved"
    CONSUMED = "consumed"
    INVALIDATED = "invalidated"
    ACKNOWLEDGED = "acknowledged"


class Origin(str, Enum):
    WORKBENCH = "workbench"
    EXTERNAL = "external"
    OPTIMIZER = "optimizer"
    RESTORE = "restore"
    PROMOTION = "promotion"
    UNKNOWN = "unknown"
    CREATE = "create"


class Comparison(str, Enum):
    EQUAL = "equal"
    DIFFERENT = "different"
    UNKNOWN = "unknown"


class PatchStage(str, Enum):
    CONFIG_PENDING = "config_pending"
    CONFIG_IN_FLIGHT = "config_in_flight"
    CONFIG_OBSERVED = "config_observed"
    DESCRIPTION_PENDING = "description_pending"
    DESCRIPTION_IN_FLIGHT = "description_in_flight"
    DESCRIPTION_OBSERVED = "description_observed"


class CoordinationState(str, Enum):
    IDLE = "idle"
    OBSERVING = "observing"
    RESERVED = "reserved"
    ADMITTED = "admitted"
    QUARANTINED = "quarantined"


class FactKind(str, Enum):
    OPERATION = "operation"
    CREATE_INTENT = "create_intent"
    APPROVAL_REQUEST = "approval_request"
    APPROVAL_VOTE = "approval_vote"
    APPROVAL_GRANTED = "approval_granted"
    APPROVAL_CONSUMED = "approval_consumed"
    APPROVAL_INVALIDATED = "approval_invalidated"
    RELEASE = "release"
    RECEIPT = "receipt"
    ACKNOWLEDGEMENT = "acknowledgement"
    BREAK_GLASS = "break_glass"
    RECOVERY = "recovery"


class DiffCategory(str, Enum):
    SOURCES = "sources"
    COLUMNS = "columns"
    INSTRUCTIONS = "instructions"
    JOINS = "joins"
    FILTERS = "filters"
    PARAMETERS = "parameters"
    SQL = "sql"
    BENCHMARKS = "benchmarks"
    QUESTIONS = "questions"
    METADATA = "metadata"
    BINDINGS = "bindings"


class DiffChange(str, Enum):
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("JSON object keys must be strings")
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    raise TypeError("Expected a finite JSON value")


def _convert(annotation: object, value: object) -> object:
    origin, arguments = get_origin(annotation), get_args(annotation)
    if origin is Annotated:
        converted = _convert(arguments[0], value)
        for constraint in arguments[1:]:
            if constraint == "uuid" and str(UUID(converted)) != converted:
                raise ValueError("Expected a canonical UUID string")
            if constraint == "sha256" and not re.fullmatch(r"[0-9a-f]{64}", converted):
                raise ValueError("Expected lowercase SHA-256 hex")
            if constraint == "positive" and converted <= 0:
                raise ValueError("Expected a positive integer")
            if constraint == "nonnegative" and converted < 0:
                raise ValueError("Expected a nonnegative integer")
        return converted
    if origin in (Union, UnionType):
        for candidate in arguments:
            try:
                return _convert(candidate, value)
            except (TypeError, ValueError):
                pass
        raise ValueError(f"Value does not match {annotation}")
    if annotation is type(None):
        if value is not None:
            raise TypeError("Expected null")
        return None
    if origin is Literal:
        if value not in arguments:
            raise ValueError(f"Expected one of {arguments}")
        return value
    if origin in (tuple, frozenset):
        if not isinstance(value, (tuple, list, frozenset)):
            raise TypeError("Expected a sequence")
        return origin(_convert(arguments[0], item) for item in value)
    if origin is Mapping:
        if not isinstance(value, Mapping):
            raise TypeError("Expected an object")
        return MappingProxyType({_convert(arguments[0], key): _convert(arguments[1], item)
                                 for key, item in value.items()})
    if annotation is object or isinstance(annotation, TypeVar):
        return _freeze_json(value) if not isinstance(value, WireValue) else value
    if annotation is datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
            raise ValueError("Expected a timezone-aware UTC timestamp")
        return value.astimezone(timezone.utc)
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation(value)
    if isinstance(annotation, type) and issubclass(annotation, WireValue):
        return value if isinstance(value, annotation) else from_wire(annotation, value)
    if type(value) is not annotation:
        raise TypeError(f"Expected {annotation}, received {type(value)}")
    return value


@dataclass(frozen=True)
class WireValue:
    def __post_init__(self) -> None:
        annotations = get_type_hints(type(self), include_extras=True)
        for definition in fields(self):
            object.__setattr__(self, definition.name,
                               _convert(annotations[definition.name], getattr(self, definition.name)))


def from_wire(value_type: type[ValueType], payload: Mapping[str, object]) -> ValueType:
    if value_type in (ExecutorContext, AuthorizationGrant):
        raise TypeError("Internal credential/capability values cannot be deserialized")
    if not isinstance(payload, Mapping):
        raise TypeError("Expected a JSON object")
    return value_type(**payload)


def to_wire(value: object) -> object:
    if isinstance(value, (ExecutorContext, AuthorizationGrant)):
        raise TypeError("Internal credential/capability values cannot be serialized")
    if isinstance(value, WireValue):
        return {definition.name: to_wire(getattr(value, definition.name))
                for definition in fields(value)
                if not (definition.metadata.get("omit_none") and getattr(value, definition.name) is None)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _convert(datetime, value).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("JSON object keys must be strings")
        return {key: to_wire(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [to_wire(item) for item in value]
    if isinstance(value, frozenset):
        return sorted(to_wire(item) for item in value)
    return _freeze_json(value)


def canonical_json_hash(domain: str, payload: Mapping[str, object], *,
                        digest_field: str | None = None) -> str:
    """Hash compact, sorted-key UTF-8 JSON with an explicit versioned domain.

    Only the named top-level self-digest is excluded; nested evidence digests
    remain bound. Arrays retain order. This is VC's JSON encoding, not a claim
    of RFC 8785 number normalization; consumers must use these exact bytes.
    """
    if not isinstance(domain, str) or not re.fullmatch(r"[a-z][a-z0-9-]*/[1-9][0-9]*", domain):
        raise ValueError("Hash domain must be explicitly versioned")
    if not isinstance(payload, Mapping):
        raise TypeError("Hash payload must be a structured object")
    if "domain" in payload or digest_field == "domain":
        raise ValueError("Hash domain is reserved")
    content = {key: value for key, value in payload.items() if key != digest_field}
    encoded = json.dumps(to_wire({"domain": domain, **content}), ensure_ascii=False,
                         sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return sha256(encoded).hexdigest()


@dataclass(frozen=True)
class BindingRef(WireValue):
    binding_id: UUIDString
    binding_revision: PositiveInt
    space_key: str
    workspace_id: str
    space_id: str | None
    environment: str


@dataclass(frozen=True)
class Fingerprints(WireValue):
    config: Digest
    benchmark: Digest
    metadata: Digest
    canonicalizer_version: str

    @property
    def state_digest(self) -> str:
        return canonical_json_hash("vc-state/1", to_wire(self))


@dataclass(frozen=True)
class Snapshot(WireValue):
    response_envelope: JsonObject
    serialized_space: JsonObject
    restorable_metadata: JsonObject
    response_envelope_digest: Digest
    raw_state_digest: Digest
    canonical_state: JsonObject
    fingerprints: Fingerprints
    state_digest: Digest


@dataclass(frozen=True)
class ObservationRef(WireValue):
    version_id: UUIDString
    binding_id: UUIDString
    binding_revision: PositiveInt
    state_digest: Digest
    envelope_digest: Digest


@dataclass(frozen=True)
class CreateIntentRef(WireValue):
    event_id: UUIDString
    operation_id: UUIDString
    binding_id: UUIDString
    binding_revision: PositiveInt
    request_digest: Digest


@dataclass(frozen=True)
class ActorContext(WireValue):
    subject_id: str
    workspace_id: str
    actor_kind: str


@dataclass(frozen=True)
class ExecutorContext:
    workspace_id: str
    host: str
    principal_id: str
    actor_kind: str
    credential_handle: object = field(repr=False, compare=False)
    execution_ref: str


@dataclass(frozen=True)
class RequestIdentity(WireValue):
    operation_id: UUIDString
    idempotency_key: str
    request_digest: Digest


@dataclass(frozen=True)
class FenceToken(WireValue):
    binding_id: UUIDString
    binding_revision: PositiveInt
    attempt_id: UUIDString
    generation: NonnegativeInt
    row_version: NonnegativeInt


@dataclass(frozen=True)
class Reservation(WireValue):
    fence: FenceToken
    request: RequestIdentity
    executor: ExecutorContext
    lease_expires_at: datetime


@dataclass(frozen=True)
class AdmissionClaim(FenceToken):
    request: RequestIdentity
    preimage: ObservationRef | CreateIntentRef
    approval_id: UUIDString | None
    approval_digest: Digest | None


@dataclass(frozen=True)
class Heads(WireValue):
    observed: UUIDString | None
    approved: UUIDString | None
    deployed: UUIDString | None


@dataclass(frozen=True)
class HeadUpdate(WireValue):
    observed: UUIDString | None
    approved: UUIDString | None
    deployed: UUIDString | None
    authorization_reference: str | None


@dataclass(frozen=True)
class OperationHandle(WireValue):
    operation_id: UUIDString
    status: OperationStatus
    job_run_id: str | None


@dataclass(frozen=True)
class DiffItem(WireValue):
    category: DiffCategory
    path: str
    change: DiffChange
    before: object
    after: object
    review_required: bool


@dataclass(frozen=True)
class SemanticDiff(WireValue):
    comparison: Comparison
    items: tuple[DiffItem, ...]


@dataclass(frozen=True)
class BindingStatus(WireValue):
    binding_id: UUIDString
    binding_revision: PositiveInt
    heads: Heads
    drift: DriftState
    quarantined: bool
    unresolved_operation_id: UUIDString | None
    observed_at: datetime | None
    projection_as_of: datetime
    stale: bool
    allowed_actions: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class VersionSummary(WireValue):
    version_id: UUIDString
    binding_id: UUIDString
    observed_at: datetime
    origin: Origin
    observed_by: str
    parent_version_id: UUIDString | None
    restored_from_version_id: UUIDString | None
    fingerprints: Fingerprints
    optimizer_run_id: str | None
    champion_id: str | None


@dataclass(frozen=True)
class CaptureContext(WireValue):
    binding: BindingRef
    observation_key: str
    observed_at: datetime
    observation_reason: str
    actor: ActorContext
    origin: Origin
    parent_version_id: UUIDString | None = None
    restored_from_version_id: UUIDString | None = None
    operation_id: UUIDString | None = None
    attempt_id: UUIDString | None = None
    generation: NonnegativeInt | None = None
    api_update_time: datetime | None = None
    optimizer_run_id: str | None = None
    champion_id: str | None = None
    release_id: UUIDString | None = None


@dataclass(frozen=True)
class Version(WireValue):
    version_id: UUIDString
    snapshot: Snapshot
    context: CaptureContext


@dataclass(frozen=True)
class VersionDetail(WireValue):
    version: Version


@dataclass(frozen=True)
class Page(WireValue, Generic[ValueType]):
    items: tuple[ValueType, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class VersionPage(Page[VersionSummary]):
    items: tuple[VersionSummary, ...]


@dataclass(frozen=True)
class OverviewPage(Page[BindingStatus]):
    items: tuple[BindingStatus, ...]


@dataclass(frozen=True)
class ObservationResult(WireValue):
    status: BindingStatus
    captured_version: VersionSummary | None
    busy: bool


@dataclass(frozen=True, kw_only=True)
class ApiError(WireValue):
    code: str
    message: str
    operation_id: UUIDString | None = field(default=None, metadata={"omit_none": True})
    retryable: bool
    stale: bool
    details: JsonObject | None = field(default=None, metadata={"omit_none": True})


@dataclass(frozen=True)
class EnrollmentRequest(WireValue):
    space_key: str
    workspace_id: str
    space_id: str | None
    environment: str
    binding_reason: str


@dataclass(frozen=True)
class EnrollmentProof(WireValue):
    registry_event_id: UUIDString
    binding_id: UUIDString
    binding_revision: PositiveInt
    authority_reference: str


@dataclass(frozen=True)
class IdentityEvidence(WireValue):
    schema_version: Literal["VC/1.0"]
    workspace_id: str
    space_id: str
    evidence_digest: Digest
    recorded_at: datetime
    actor: ActorContext


@dataclass(frozen=True)
class RebindAuthorization(WireValue):
    binding: BindingRef
    approval_id: UUIDString
    approval_digest: Digest
    actor: ActorContext
    reason: str
    expires_at: datetime


@dataclass(frozen=True)
class StageEvidence(WireValue):
    schema_version: Literal["VC/1.0"]
    stage: PatchStage
    recorded_at: datetime
    evidence_digest: Digest
    observation: ObservationRef | None


@dataclass(frozen=True)
class ExecutionTerminalEvidence(WireValue):
    execution_ref: str
    terminal_at: datetime
    evidence_digest: Digest


@dataclass(frozen=True)
class TerminationEvidence(WireValue):
    attempt_id: UUIDString
    execution_ref: str
    source: str
    terminated_at: datetime
    executions: tuple[ExecutionTerminalEvidence, ...]
    app_worker_proof_digest: Digest | None


@dataclass(frozen=True)
class RecoverySample(WireValue):
    observed_at: datetime
    state_digest: Digest
    envelope_digest: Digest


@dataclass(frozen=True)
class RecoveryEvidence(WireValue):
    schema_version: Literal["VC/1.0"]
    fence: FenceToken
    termination: TerminationEvidence
    samples: tuple[RecoverySample, ...]
    maximum_request_lifetime_seconds: float | None
    lifetime_bound_reference: str | None
    checkpoint_classification: str
    human_authorization_reference: str | None
    human_reason: str | None
    residual_risk_acknowledged: bool


@dataclass(frozen=True)
class AuthorizationGrant(WireValue):
    binding: BindingRef
    request: RequestIdentity
    authorization_reference: str
    expected_base_fingerprints: Fingerprints
    rendered_target_digest: Digest
    expires_at: datetime
    approval_id: UUIDString | None
    approval_digest: Digest | None


@dataclass(frozen=True)
class OperationResult(WireValue):
    operation_id: UUIDString
    status: OperationStatus
    preimage: ObservationRef | CreateIntentRef | None
    postimage: ObservationRef | None
    unresolved: bool
    evidence_references: tuple[str, ...]


@dataclass(frozen=True)
class RecoveryResult(WireValue):
    binding: BindingRef
    status: OperationStatus
    fence: FenceToken
    unresolved: bool
    evidence_references: tuple[str, ...]


@dataclass(frozen=True)
class ObservationLease(WireValue):
    fence: FenceToken
    observed_sequence: NonnegativeInt
    lease_expires_at: datetime


@dataclass(frozen=True)
class CreatePayload(WireValue):
    title: str
    serialized_space: JsonObject
    description: str | None


@dataclass(frozen=True)
class CreateResponse(WireValue):
    space_id: str
    response_envelope: JsonObject


@dataclass(frozen=True)
class MutationRequest(WireValue):
    identity: RequestIdentity
    binding: BindingRef
    operation_type: str
    source_version_id: UUIDString
    expected_base: Digest
    serialized_space: JsonObject
    description: str | None
    approval_id: UUIDString | None


@dataclass(frozen=True)
class CreateRequest(WireValue):
    identity: RequestIdentity
    binding: BindingRef
    payload: CreatePayload
    approval_id: UUIDString | None


@dataclass(frozen=True)
class DriftResult(WireValue):
    state: DriftState
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ReconcileRequest(WireValue):
    identity: RequestIdentity
    action: Literal["adopt", "reapply", "acknowledge"]
    binding_revision: PositiveInt
    expected_base: Digest
    approval_id: UUIDString | None
    policy_inputs: JsonObject


@dataclass(frozen=True)
class ReconcileBatch(Page[BindingStatus]):
    items: tuple[BindingStatus, ...]


@dataclass(frozen=True)
class ApprovalInputs(WireValue):
    schema_version: Literal["VC/1.0"]
    operation_id: UUIDString
    operation_type: str
    source_version_id: UUIDString
    raw_source_digest: Digest
    source_fingerprints: Fingerprints
    artifact_digest: Digest | None
    mapping_digest: Digest | None
    rendered_target_digest: Digest
    transformer_version: str | None
    canonicalizer_version: str
    target_binding: BindingRef
    expected_base_fingerprints: Fingerprints
    permission_policy_digest: Digest | None
    validation_policy_digest: Digest
    benchmark_policy_digest: Digest
    preflight_evidence_digest: Digest
    thresholds: JsonObject
    requester_id: str
    recovery_policy: JsonObject
    expires_at: datetime


@dataclass(frozen=True)
class ApprovalRequest(WireValue):
    approval_id: UUIDString
    inputs: ApprovalInputs
    requested_at: datetime


@dataclass(frozen=True)
class ApprovalVote(WireValue):
    approver_id: str
    decision: str
    approved_at: datetime


@dataclass(frozen=True)
class ApprovalRecord(WireValue):
    request: ApprovalRequest
    votes: tuple[ApprovalVote, ...]
    status: FactStatus
    approval_digest: Digest | None


@dataclass(frozen=True)
class BreakGlassRequest(WireValue):
    identity: RequestIdentity
    binding: BindingRef
    reason: str
    expires_at: datetime
    evidence_digest: Digest
    residual_risk_acknowledged: bool


@dataclass(frozen=True)
class MappingSpec(WireValue):
    schema_version: Literal["VC/1.0"]
    target_binding: BindingRef
    mappings: Mapping[str, str]
    mapping_digest: Digest
    transformer_version: str


@dataclass(frozen=True)
class TestPolicy(WireValue):
    schema_version: Literal["VC/1.0"]
    validation_policy_digest: Digest
    benchmark_policy_digest: Digest
    thresholds: JsonObject


@dataclass(frozen=True)
class PackageManifest(WireValue):
    schema_version: Literal["VC/1.0"]
    space_key: str
    source_version_id: UUIDString
    raw_source_digest: Digest
    source_fingerprints: Fingerprints
    artifact_digest: Digest
    mapping_digest: Digest
    transformer_version: str
    required_sources: tuple[str, ...]
    validation_policy_digest: Digest
    benchmark_policy_digest: Digest
    ownership: JsonObject
    file_digests: Mapping[str, Digest]
    package_digest: Digest


@dataclass(frozen=True)
class PackageRef(WireValue):
    package_digest: Digest
    manifest_uri: str


@dataclass(frozen=True)
class PreflightEvidence(WireValue):
    schema_version: Literal["VC/1.0"]
    operation_id: UUIDString
    target_binding: BindingRef
    expected_base_fingerprints: Fingerprints
    rendered_fingerprints: Fingerprints
    permission_policy_digest: Digest | None
    validation_evidence_digest: Digest
    benchmark_evidence_digest: Digest
    preflight_evidence_digest: Digest
    recorded_at: datetime


@dataclass(frozen=True)
class DispatchPage(Page[OperationHandle]):
    items: tuple[OperationHandle, ...]


@dataclass(frozen=True, kw_only=True)
class DeploymentReceipt(WireValue):
    schema_version: Literal["VC/1.0"]
    release_id: UUIDString
    operation_id: UUIDString
    target_binding: BindingRef
    attempt_id: UUIDString
    generation: NonnegativeInt
    coordination_backend: Literal["delta"] = "delta"
    approval_id: UUIDString
    approval_digest: Digest
    source_version_id: UUIDString
    package_digest: Digest
    mapping_digest: Digest
    intended_fingerprints: Fingerprints
    rendered_fingerprints: Fingerprints
    observed_fingerprints: Fingerprints
    pre_version_id: UUIDString
    post_version_id: UUIDString | None
    transformer_version: str
    canonicalizer_version: str
    executor_id: str
    job_run_id: str
    validation_evidence_digest: Digest
    benchmark_evidence_digest: Digest
    status: OperationStatus
    compensation_operation_id: UUIDString | None
    recorded_at: datetime


@dataclass(frozen=True)
class AuthenticatedRequest(WireValue):
    authentication_reference: str
    workspace_id: str


@dataclass(frozen=True)
class ExplicitExecutorSelection(WireValue):
    workspace_id: str
    host: str
    principal_id: str
    execution_ref: str
    profile: str | None


@dataclass(frozen=True)
class FactRef(WireValue):
    event_id: UUIDString
    event_key: str
    evidence_digest: Digest


@dataclass(frozen=True)
class OperationFact(WireValue):
    event_id: UUIDString
    fact_kind: FactKind
    operation_id: UUIDString
    transition_sequence: NonnegativeInt
    event_key: str
    binding: BindingRef
    request: RequestIdentity
    operation_type: str
    requester_id: str
    actor: ActorContext
    status: FactStatus
    evidence: ApprovalInputs | ApprovalRecord | PackageManifest | DeploymentReceipt | StageEvidence | RecoveryEvidence | CreateIntentRef | BreakGlassRequest
    recorded_at: datetime
    coordination_backend: Literal["delta"] = "delta"
    attempt_id: UUIDString | None = None
    generation: NonnegativeInt | None = None
    expected_base_fingerprint: Digest | None = None
    desired_artifact_fingerprint: Digest | None = None
    mapping_fingerprint: Digest | None = None
    transformer_version: str | None = None
    test_policy_fingerprint: Digest | None = None
    pre_version_id: UUIDString | None = None
    post_version_id: UUIDString | None = None
    approval_id: UUIDString | None = None
    approval_digest: Digest | None = None
    approval_reference: str | None = None
    release_id: UUIDString | None = None
    job_run_id: str | None = None
    drift_override: bool = False
    evidence_uri: str | None = None
    evidence_digest: Digest | None = None


@dataclass(frozen=True)
class RequestHistory(WireValue):
    facts: tuple[OperationFact, ...]
    ambiguous: bool


def fact_key(fact: "OperationFact") -> str:
    """The deterministic logical identity of an operation fact.

    Shared by every fact writer (M03 coordination, M04 gate, M06 governance,
    M07 receipts) so the durable-lookup-before-append dedup contract holds and no
    writer needs to import another module's fact implementation (M03 §4 forbids
    M03 → M06). The key intentionally excludes `event_id`, `evidence`,
    `event_key`, timestamps and `status`: it is the *slot* a fact occupies, keyed
    by binding, operation, kind and the transition-sequence category, plus the
    attempt/generation fence. Two facts that share a key must be byte-identical
    (`DurableOperationFacts._append` fails closed otherwise).
    """
    payload = {
        "binding": to_wire(fact.binding), "operation_id": fact.operation_id,
        "kind": fact.fact_kind.value, "sequence": fact.transition_sequence,
        "attempt_id": fact.attempt_id, "generation": fact.generation,
    }
    if fact.fact_kind == FactKind.APPROVAL_VOTE:
        payload["voter"] = fact.actor.subject_id
    if fact.fact_kind in (FactKind.ACKNOWLEDGEMENT, FactKind.RECOVERY) and fact.approval_reference is not None:
        payload["approval_reference"] = fact.approval_reference
    return canonical_json_hash("vc-fact-key/1", payload)


def deterministic_event_id(event_key: str) -> str:
    """The event UUID a durable fact must carry, derived from its logical key."""
    return str(uuid5(NAMESPACE_URL, event_key))


def seal_fact(fact: "OperationFact") -> "OperationFact":
    """Stamp the deterministic `event_key`/`event_id` onto an assembled fact.

    Writers construct a fact with the correct binding/kind/sequence/fence and
    provisional identity, then seal it so `event_key == fact_key(fact)` and
    `event_id == deterministic_event_id(event_key)` — exactly what
    `DurableOperationFacts._append` verifies. Facts whose evidence embeds the
    event_id (CREATE_INTENT) compute the key first and build evidence around it.
    """
    key = fact_key(fact)
    return replace(fact, event_key=key, event_id=deterministic_event_id(key))


@dataclass(frozen=True)
class ApprovalUse(WireValue):
    binding: BindingRef
    approval_id: UUIDString
    consumptions: tuple[FactRef, ...]
    operation_ids: tuple[UUIDString, ...]
    ambiguous: bool


@dataclass(frozen=True)
class ApprovedOperation(WireValue):
    request: MutationRequest | CreateRequest
    approval: ApprovalRecord | None


class VersionLookup(Protocol):
    def get(self, version_id: str) -> Snapshot: ...


class EnrollmentInitializer(Protocol):
    def initialize(self, binding: BindingRef, enrollment: EnrollmentProof) -> None: ...


class TerminationEvidenceProvider(Protocol):
    def for_attempt(self, execution_ref: str, attempt_id: str) -> TerminationEvidence | None: ...


class Canonicalizer(Protocol):
    def observe(self, envelope: dict, version: str = "vc-c14n/1") -> Snapshot: ...
    def compare(self, left: Snapshot, right: Snapshot) -> Comparison: ...
    def semantic_diff(self, left: Snapshot, right: Snapshot) -> list[DiffItem]: ...

class VersionLedger(Protocol):
    def append_observation(self, snapshot: Snapshot, context: CaptureContext) -> ObservationRef: ...
    def get_version(self, binding: BindingRef, version_id: str) -> Version: ...
    def history(self, binding: BindingRef, cursor: str | None, limit: int) -> VersionPage: ...
    def verify_committed(self, observation: ObservationRef) -> bool: ...

class Registry(Protocol):
    def enroll(self, request: EnrollmentRequest, actor: ActorContext) -> BindingRef: ...
    def resolve(self, binding_id: str) -> BindingRef: ...
    def bind_created(self, intent: CreateIntentRef, space_id: str, evidence: IdentityEvidence) -> BindingRef: ...
    def tombstone(self, binding: BindingRef, approval: RebindAuthorization) -> BindingRef: ...
    def rebind(self, binding: BindingRef, new_space_id: str, approval: RebindAuthorization) -> BindingRef: ...

class OperationFacts(Protocol):
    def append(self, fact: OperationFact) -> FactRef: ...
    def lookup_request(self, binding: BindingRef, idempotency_key: str) -> RequestHistory: ...
    def approval_use(self, binding: BindingRef, approval_id: str) -> ApprovalUse: ...
    def get_request(self, operation_id: str) -> ApprovedOperation: ...
    def publish_consumption(self, claim: AdmissionClaim) -> FactRef: ...
    def verify_flush(self, claim: AdmissionClaim) -> bool: ...

class Coordination(Protocol):
    def initialize(self, binding: BindingRef, enrollment: EnrollmentProof) -> None: ...
    def reserve(self, binding: BindingRef, operation: RequestIdentity, executor: ExecutorContext) -> Reservation: ...
    def admit(self, reservation: Reservation, preimage: ObservationRef | CreateIntentRef,
              authorization: AuthorizationGrant) -> AdmissionClaim: ...
    def renew(self, claim: FenceToken) -> FenceToken: ...
    def assert_owner(self, claim: FenceToken) -> None: ...
    def checkpoint(self, claim: AdmissionClaim, stage: PatchStage, evidence: StageEvidence) -> None: ...
    def finish(self, claim: AdmissionClaim, result: OperationResult) -> None: ...
    def reject_reservation(self, reservation: Reservation, message: str) -> None:
        """Publish terminal CONFLICTED and release a never-admitted RESERVED row to IDLE.

        Return only after durable rejection and release. Fail closed unless this
        reservation owns a proven pre-send row; never bypass possible-send quarantine.
        """
        ...
    def quarantine(self, claim: FenceToken, reason: str) -> None: ...
    def recover(self, binding: BindingRef, evidence: RecoveryEvidence) -> RecoveryResult: ...
    def observe_exclusively(self, binding: BindingRef, executor: ExecutorContext) -> ObservationLease: ...
    def advance_heads(self, fence: FenceToken, update: HeadUpdate) -> Heads: ...
    def release_observation(self, fence: FenceToken) -> None: ...

class GenieTransport(Protocol):
    def get(self, binding: BindingRef, executor: ExecutorContext) -> dict: ...
    def create_once(self, payload: CreatePayload, claim: AdmissionClaim) -> CreateResponse: ...
    def patch_config_once(self, binding: BindingRef, serialized_space: dict, claim: AdmissionClaim) -> None: ...
    def patch_description_once(self, binding: BindingRef, description: str, claim: AdmissionClaim) -> None: ...

class MutationGate(Protocol):
    def execute(self, request: MutationRequest, executor: ExecutorContext) -> OperationResult: ...
    def create(self, request: CreateRequest, executor: ExecutorContext) -> OperationResult: ...
    def verify_only(self, operation_id: str, executor: ExecutorContext) -> OperationResult: ...

class Observer(Protocol):
    def capture_on_open(self, binding: BindingRef, viewer: ActorContext) -> ObservationResult: ...
    def capture(self, binding: BindingRef, reason: str, executor: ExecutorContext) -> ObservationResult: ...

class DriftService(Protocol):
    def classify(self, heads: Heads, versions: VersionLookup, reachability: str) -> DriftResult: ...
    def reconcile(self, binding: BindingRef, action: ReconcileRequest, actor: ActorContext) -> OperationHandle: ...
    def scan(self, workspace_id: str, cursor: str | None) -> ReconcileBatch: ...

class ApprovalService(Protocol):
    def request(self, inputs: ApprovalInputs, actor: ActorContext) -> ApprovalRequest: ...
    def vote(self, approval_id: str, decision: str, actor: ActorContext) -> ApprovalRecord: ...
    def authorize(self, request: MutationRequest, executor: ExecutorContext) -> AuthorizationGrant: ...
    def break_glass(self, request: BreakGlassRequest, actor: ActorContext) -> AuthorizationGrant: ...

class PromotionService(Protocol):
    def package(self, version_id: str, mapping: MappingSpec, policy: TestPolicy) -> PackageRef: ...
    def preflight(self, operation_id: str, executor: ExecutorContext) -> PreflightEvidence: ...
    def dispatch_pending(self, target_workspace_id: str, cursor: str | None) -> DispatchPage: ...
    def execute(self, operation_id: str, executor: ExecutorContext) -> DeploymentReceipt: ...

class IdentityProvider(Protocol):
    def actor(self, request: AuthenticatedRequest) -> ActorContext: ...
    def groups(self, subject_id: str, workspace_id: str) -> frozenset[str]: ...
    def can_edit(self, subject_id: str, binding: BindingRef) -> bool: ...
    def executor(self, selection: ExplicitExecutorSelection) -> ExecutorContext: ...
    def verify_run_as(self, execution_ref: str, expected_principal_id: str) -> None: ...

class JobDispatcher(Protocol):
    def submit_local(self, operation_id: str, job_kind: str) -> OperationHandle: ...
