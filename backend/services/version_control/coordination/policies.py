"""Production coordination-authorization policy callbacks.

M03 owns the coordination *mechanism* (`CoordinationService`); it deliberately
takes its authorization decisions as injected callbacks so it never imports M06
policy or M02 identity (the module doc forbids that cycle). Those callbacks
existed only as test mocks — this module supplies the real bindings that M08's
composition (`build_vc_runtime`) wires into the live service.

Each policy is a thin, fail-closed adapter over an already-built durable
service (registry, approvals, facts, coordination store):

* `resolve_binding` / `verify_enrollment` / `insert_enrolled` — enrollment and
  current-binding identity via the M02 registry and the M03 store.
* `validate_authorization` — re-derives the AuthorizationGrant through
  `ApprovalService.authorize` and accepts the candidate only if it is byte-equal,
  so a revoked approval, lapsed membership or expired grant fails closed.
* `verify_create_intent` — confirms the durable request digest matches the
  committed create intent.
* `validate_stage_evidence` — confirms the checkpoint's recorded digest matches
  its committed observation for the same binding revision.

`authorize_heads` and `authorize_human_recovery` gate the reconciliation
(M05) and human-recovery (M03) flows, which are **not** part of the promotion
write path and are not yet platform-integrated; they fail closed so those flows
stay disabled until their owning milestones supply and validate a positive
contract. The promotion gate never advances approved/deployed heads or performs
human recovery, so this does not restrict it.
"""

from collections.abc import Callable
from typing import Any

from backend.services.version_control import contracts as vc
from backend.services.version_control.registry import RegistryConflict


def resolve_binding(registry) -> Callable[[str], Any]:
    def resolve(binding_id: str):
        try:
            return registry.resolve(binding_id)
        except (KeyError, RegistryConflict):
            return None

    return resolve


def verify_enrollment(registry) -> Callable[[Any, Any], bool]:
    # The serialized enrollment lock is held by the registry's per-space-key
    # serialization while `enroll` calls the initializer; this callback confirms
    # the just-recorded provisional binding is the unique current head, so a
    # forked or superseded chain cannot bootstrap a coordination row.
    def verify(binding, enrollment) -> bool:
        try:
            return registry.resolve(binding.binding_id) == binding
        except (KeyError, RegistryConflict):
            return False

    return verify


def insert_enrolled(store) -> Callable[[Any], Any]:
    # The service hands a fully built IDLE CoordinationRow; the store's
    # EnrollmentInitializer inserts exactly that one row (it rebuilds the row
    # from the binding and its own clock, ignoring the proof argument).
    def insert(row) -> Any:
        return store.initialize(row.binding, None)

    return insert


def validate_authorization(approvals, facts) -> Callable[[Any, Any], bool]:
    def validate(authorization, executor) -> bool:
        try:
            # The grant carries only the RequestIdentity; re-derive the durable
            # MutationRequest from facts so ApprovalService re-runs the full policy
            # against the immutable approved request (it keys on request.approval_id).
            request = facts.get_request(authorization.request.operation_id).request
            return approvals.authorize(request, executor) == authorization
        except (KeyError, PermissionError, ValueError):
            return False

    return validate


def verify_create_intent(facts) -> Callable[[Any], bool]:
    def verify(preimage) -> bool:
        try:
            stored = facts.get_request(preimage.operation_id).request
        except (KeyError, PermissionError, ValueError):
            return False
        return stored.identity.request_digest == preimage.request_digest

    return verify


def validate_stage_evidence(claim, row, evidence) -> bool:
    observation = evidence.observation
    return (isinstance(observation, vc.ObservationRef)
            and evidence.evidence_digest == observation.state_digest
            and (observation.binding_id, observation.binding_revision)
            == (row.binding.binding_id, row.binding.binding_revision))


def authorize_heads(binding, fence, update) -> bool:
    # Approved/deployed head advancement belongs to M05 reconciliation and is not
    # integrated; fail closed. Promotion never advances these heads via the gate.
    return False


def authorize_human_recovery(binding, evidence) -> bool:
    # Human break-glass recovery (M03/M06) is not integrated; fail closed.
    return False
