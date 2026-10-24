"""Synchronous, fail-closed coordination. Never sends a Genie mutation."""
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from math import isfinite
from uuid import uuid4

from backend.services.version_control import contracts as c
from .row import CoordinationRow


def _floor_ms(value: datetime) -> datetime:
    """Truncate a datetime to millisecond precision (the resolution Delta TIMESTAMPs
    round-trip through the REST Statements API), so CAS read-back verification of a
    server-authoritative timestamp is not a false mismatch on sub-ms micros."""
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


@dataclass(frozen=True)
class AttemptSubject:
    """Names the attempt a fact is ABOUT; not a VC/1.0 contract type (M03-private)."""
    attempt_id: str | None
    generation: int | None
    holder: str | None
    executor_kind: str | None
    pre_version_id: str | None
    approval_id: str | None
    approval_digest: str | None


def _rejected_subject(executor):
    """A CAS loser never acquired an attempt; attribute the fact to who tried."""
    return AttemptSubject(None, None, executor.principal_id, executor.actor_kind,
                          None, None, None)


# Deterministic transition-sequence categories for coordination-authored facts.
# `fact_key` includes (kind, sequence), so these keep the facts that can coexist for
# one (operation, attempt, generation) in distinct durable slots. All are < the gate's
# terminal OPERATION sequence (100), so M07's `max(by transition_sequence)` still selects
# the gate's lineage-bearing terminal fact when minting a DeploymentReceipt.
#
# The terminal *closure* fact replaces the coordination RECEIPT of earlier drafts:
# `FactKind.RECEIPT` is reserved for the target-written `DeploymentReceipt` (contracts §6;
# `promotion/receipts.existing` rejects any RECEIPT whose evidence is not a DeploymentReceipt).
# Coordination's completion/closure marker is therefore an OPERATION fact authored by
# `operation_type == 'coordination'` at `_SEQ_CLOSURE`, and `reserve` keys idempotency and
# terminal-conflict detection off exactly that marker.
_SEQ_REJECT = 1      # pre-send / CAS-loss rejection (CONFLICTED / FAILED)
_SEQ_QUARANTINE = 2  # unresolved quarantine marker (QUARANTINED)
_SEQ_CLOSURE = 3     # terminal closure receipt-equivalent (finish / recover)
_SEQ_RECOVERY = 4    # recovery audit (RECOVERY kind)


class CoordinationError(RuntimeError):
    """No write authority may be inferred from a failed coordination call."""


class OwnershipError(CoordinationError):
    pass


class AuthorityUnavailable(CoordinationError):
    pass


class _RejectionReleased(CoordinationError):
    """A definite rejection was durably published and the binding released."""


class ExistingReceipt(CoordinationError):
    """Non-admitting result channel; preserves the frozen reserve() return type."""

    def __init__(self, receipt: c.OperationFact):
        super().__init__('Request already completed; return original durable receipt')
        self.receipt = receipt


class CoordinationService:
    # Lease TTL for observation/reservation/admission windows. Sized for live-platform
    # latency: a governed mutation renews this lease at admit and at every checkpoint
    # heartbeat, so it need only cover a single phase (SCIM group resolution, a Genie
    # GET/PATCH round-trip, or a ledger commit) rather than the whole operation.
    _LEASE_TTL = timedelta(seconds=300)

    def __init__(self, *, store, facts: c.OperationFacts, ledger: c.VersionLedger,
                 clock, resolve_binding, verify_enrollment, insert_enrolled,
                 validate_authorization, verify_create_intent,
                 termination: c.TerminationEvidenceProvider,
                 authorize_human_recovery, authorize_heads,
                 verified_request_lifetime: tuple[float, str] | None = None,
                 validate_stage_evidence=None):
        self.store = store
        self.facts = facts
        self.ledger = ledger
        self.clock = clock
        self.resolve_binding = resolve_binding
        self.verify_enrollment = verify_enrollment
        self.insert_enrolled = insert_enrolled
        self.validate_authorization = validate_authorization
        self.verify_create_intent = verify_create_intent
        self.termination = termination
        self.authorize_human_recovery = authorize_human_recovery
        self.authorize_heads = authorize_heads
        self.validate_stage_evidence = validate_stage_evidence
        # Deployment-owned verified capability, NEVER copied from RecoveryEvidence.
        self.verified_request_lifetime = verified_request_lifetime

    @staticmethod
    def _io(call, *args, **kwargs):
        try:
            return call(*args, **kwargs)
        except CoordinationError:
            raise
        except Exception as exc:
            raise AuthorityUnavailable('Durable authority unavailable or ambiguous') from exc

    @staticmethod
    def _binding_eq(observed, current):
        """Coordination binding equality that treats `space_id` as registry-authoritative.

        A space enrolled provisionally (space_id=None) and later BOUND fills in its
        physical `space_id` at the SAME `binding_revision` (`bind_created` bumps no
        revision). The coordination row and any versions/grants minted before the bind
        therefore freeze space_id=None while the registry -- the sole authority, checked
        by `_current` -- reports the bound space_id. Every other identity field
        (binding_id/revision/space_key/workspace_id/environment) still pins the binding;
        only space_id is normalized away so a bound binding matches its own
        provisionally-initialized row/lineage.
        """
        return replace(observed, space_id=current.space_id) == current

    def _current(self, binding):
        if self._io(self.resolve_binding, binding.binding_id) != binding:
            raise OwnershipError('Not the unique current binding/revision')

    def _row(self, binding):
        self._current(binding)
        row = self._io(self.store.read, binding.binding_id)
        if row is None or not self._binding_eq(row.binding, binding):
            raise OwnershipError('Missing ownership row or revision mismatch')
        # `binding` is the registry-current binding (`_current` just proved it). Heal the
        # row's frozen enroll-time space_id to it, so every downstream consumer -- the
        # `_cas` post-verify (`_row(row.binding)`), observe/reserve acquisition, head
        # lineage -- operates on the BOUND identity rather than the provisional one. The
        # store's space_id column stays as enrolled (space_id is registry-authoritative,
        # never coordination-owned); `_row` re-heals on each read.
        return row if row.binding == binding else replace(row, binding=binding)

    def initialize(self, binding: c.BindingRef, enrollment: c.EnrollmentProof) -> None:
        # The callback verifies a held serialized enrollment authority, including
        # unique physical ownership. A proof string alone is not a lock.
        if ((enrollment.binding_id, enrollment.binding_revision) !=
                (binding.binding_id, binding.binding_revision)
                or not self._io(self.verify_enrollment, binding, enrollment)):
            raise OwnershipError('Serialized enrollment proof required')
        self._current(binding)
        row = self._io(self.store.read, binding.binding_id)
        if row is None:
            self._io(self.insert_enrolled, CoordinationRow(binding, self.clock.now()))
        self._row(binding)

    def _cas(self, row, predicate, **changes):
        result = self._io(self.store.compare_and_swap, row.binding.binding_id,
                          row.binding.binding_revision, row.row_version,
                          row.generation, predicate, **changes)
        if result is None:
            raise OwnershipError('Serializable CAS lost; do not retry acquisition')
        observed = self._row(row.binding)
        def _mismatch(key, value):
            got = getattr(observed, key)
            if isinstance(got, datetime) and isinstance(value, datetime):
                # Server-authoritative TIMESTAMPs (lease_expires_at/admitted_at) round-trip
                # through the REST Statements API at millisecond precision, so the locally
                # computed micros value never byte-equals the read-back. Compare at ms; the
                # store already proved the exact-attempt win via row_version/generation/
                # attempt_id, and the timestamp is written from this same CAS.
                return _floor_ms(got) != _floor_ms(value)
            if got == value:
                return False
            # JSON columns re-read as frozen mappings/tuples; compare canonical wire
            # form so a stored dict/list equals its read-back proxy/tuple.
            try:
                return c.to_wire(got) != c.to_wire(value)
            except TypeError:
                return True
        if (observed.row_version != row.row_version + 1
                or observed.holder != changes.get('holder', row.holder)
                or observed.attempt_id != changes.get('attempt_id', row.attempt_id)
                or observed.generation != changes.get('generation', row.generation)
                or any(_mismatch(key, value) for key, value in changes.items())):
            raise AuthorityUnavailable('CAS read-back did not prove exact ownership/claim')
        return observed

    @staticmethod
    def _fence(row):
        return c.FenceToken(row.binding.binding_id, row.binding.binding_revision,
                            row.attempt_id, row.generation, row.row_version)

    def _fact(self, row, request, status, *, evidence=None, kind=c.FactKind.OPERATION,
              post_version_id=None, subject=None, sequence=_SEQ_REJECT):
        """`subject` names the attempt this fact is ABOUT; None means row's own attempt.

        `sequence` selects the deterministic transition-sequence category (`_SEQ_*`) so
        the sealed `event_key` slots facts that can coexist for one attempt (e.g. a
        quarantine marker and its later recovery closure) without colliding.
        """
        stage = row.mutation_stage or c.PatchStage.CONFIG_PENDING
        subject = subject or AttemptSubject(row.attempt_id, row.generation, row.holder,
                                            row.executor_kind, row.pre_version_id,
                                            row.approval_id, row.approval_digest)
        if evidence is None:
            evidence = c.StageEvidence('VC/1.0', stage, self.clock.now(),
                c.canonical_json_hash('vc-coordination/1', {
                    'status': status.value, 'kind': kind.value,
                    'attempt': subject.attempt_id, 'row_version': row.row_version}), None)
        fact = c.seal_fact(c.OperationFact(request.operation_id, kind, request.operation_id,
            sequence, '', row.binding, request, 'coordination',
            subject.holder or 'coordination',
            c.ActorContext(subject.holder or 'coordination', row.binding.workspace_id,
                           subject.executor_kind or 'service'), status, evidence,
            self.clock.now(), attempt_id=subject.attempt_id, generation=subject.generation,
            pre_version_id=subject.pre_version_id, post_version_id=post_version_id,
            approval_id=subject.approval_id, approval_digest=subject.approval_digest))
        # Idempotent replay: a coordination fact carries a wall-clock recorded_at, so a
        # second write of the same logical slot (same deterministic key) would not be
        # byte-identical and the strict durable validator fails such conflicts closed.
        # Query the key first and return the existing durable reference (contracts §"Logical
        # uniqueness ... retrying an ambiguous append first queries its key").
        prior = next((f for f in self._history_raw(row, request.idempotency_key).facts
                      if f.event_key == fact.event_key), None)
        if prior is not None:
            return c.FactRef(prior.event_id, prior.event_key,
                             c.canonical_json_hash('vc-fact-evidence/1', c.to_wire(prior.evidence)))
        return self._io(self.facts.append, fact)

    def reserve(self, binding: c.BindingRef, operation: c.RequestIdentity,
                executor: c.ExecutorContext) -> c.Reservation:
        row = self._expire(self._row(binding))
        if executor.workspace_id != binding.workspace_id or not executor.execution_ref:
            raise OwnershipError('Explicit target-local executor required')
        try:
            if row.state != c.CoordinationState.IDLE or row.unresolved:
                raise OwnershipError('An unresolved attempt already owns this binding')
            row = self._cas(row,
                lambda r: r.state == c.CoordinationState.IDLE and not r.unresolved,
                state=c.CoordinationState.RESERVED, unresolved=True,
                generation=row.generation + 1, attempt_id=str(uuid4()),
                holder=executor.principal_id, executor_kind=executor.actor_kind,
                executor_ref=executor.execution_ref,
                lease_expires_at=self.clock.now() + self._LEASE_TTL,
                active_operation_id=operation.operation_id,
                idempotency_key=operation.idempotency_key, request_digest=operation.request_digest)
        except OwnershipError:
            self._fact(row, operation, c.FactStatus.CONFLICTED,
                       subject=_rejected_subject(executor))
            raise
        history = self._history(row, operation)
        # Terminal closure markers authored by coordination (finish/recover), keyed by the
        # OPERATION/_SEQ_CLOSURE slot. FactKind.RECEIPT (DeploymentReceipt) is not a
        # coordination completion signal and is intentionally not scanned here.
        closures = [f for f in history.facts
                    if f.fact_kind == c.FactKind.OPERATION
                    and f.operation_type == 'coordination'
                    and f.transition_sequence == _SEQ_CLOSURE]
        completed = [f for f in closures
                     if f.status in {c.FactStatus.CONFIRMED, c.FactStatus.NOOP}]
        if completed:
            identities = {(f.operation_id, f.status, f.post_version_id) for f in completed}
            if len(identities) != 1:
                raise CoordinationError('Ambiguous completed receipts')
            self._release(row)  # This reservation never acquired write authority.
            raise ExistingReceipt(completed[0])
        if closures:
            # Terminal non-success: a conflict for the request, not a completion.
            self._reject(row, operation,
                         'Request already terminated non-successfully; file a new governed request')
        return c.Reservation(self._fence(row), operation, executor, row.lease_expires_at)

    def _owned(self, fence, *, allow_quarantine=False):
        binding = self._io(self.resolve_binding, fence.binding_id)
        row = self._row(binding)
        if not allow_quarantine:
            row = self._expire(row)
        if ((row.binding.binding_revision, row.attempt_id, row.generation) !=
                (fence.binding_revision, fence.attempt_id, fence.generation)
                or fence.row_version > row.row_version or not row.unresolved
                or row.state == c.CoordinationState.IDLE
                or (row.state == c.CoordinationState.QUARANTINED and not allow_quarantine)):
            raise OwnershipError('Stale attempt/revision/generation or quarantined claim')
        if not allow_quarantine and (row.lease_expires_at is None or
                                    row.lease_expires_at <= self.clock.now()):
            raise OwnershipError('Expired lease; no takeover or renewal')
        return row

    def renew(self, claim: c.FenceToken) -> c.FenceToken:
        row = self._owned(claim)
        updated = self._cas(row, lambda r: r.attempt_id == claim.attempt_id,
                           lease_expires_at=self.clock.now() + self._LEASE_TTL)
        return self._fence(updated)

    def assert_owner(self, claim: c.FenceToken) -> None:
        row = self._owned(claim)
        if not isinstance(claim, c.AdmissionClaim) or row.state != c.CoordinationState.ADMITTED:
            raise OwnershipError('Reservation/observation is not write admission')
        if (claim.request != self._request(row)
                or (claim.approval_id, claim.approval_digest) != (row.approval_id, row.approval_digest)
                or (isinstance(claim.preimage, c.ObservationRef) and
                    (claim.preimage.version_id, claim.preimage.state_digest) !=
                    (row.pre_version_id, row.preimage_digest))
                or (isinstance(claim.preimage, c.CreateIntentRef) and
                    claim.preimage.event_id != row.create_intent_event_id)):
            raise OwnershipError('Admission claim differs from durable row')

    @staticmethod
    def _request(row):
        return c.RequestIdentity(row.active_operation_id, row.idempotency_key, row.request_digest)

    def admit(self, reservation: c.Reservation,
              preimage: c.ObservationRef | c.CreateIntentRef,
              authorization: c.AuthorizationGrant) -> c.AdmissionClaim:
        row = self._owned(reservation.fence)
        if row.state != c.CoordinationState.RESERVED:
            raise OwnershipError('Only a reservation can be admitted; no replay')
        if (reservation.request != self._request(row)
                or reservation.executor.principal_id != row.holder
                or reservation.executor.execution_ref != row.executor_ref
                or not self._binding_eq(authorization.binding, row.binding)
                or authorization.request != reservation.request
                or authorization.expires_at <= self.clock.now()
                or bool(authorization.approval_id) != bool(authorization.approval_digest)
                or not self._io(self.validate_authorization, authorization, reservation.executor)):
            self._reject(row, reservation.request, 'Authorization is stale, mismatched or revoked')
        if (preimage.binding_id, preimage.binding_revision) != (
                row.binding.binding_id, row.binding.binding_revision):
            self._reject(row, reservation.request,
                         'Preimage belongs to another binding/revision')
        if isinstance(preimage, c.ObservationRef):
            if not self._io(self.ledger.verify_committed, preimage):
                self._reject(row, reservation.request, 'Preimage not committed')
            if preimage.state_digest != authorization.expected_base_fingerprints.state_digest:
                self._reject(row, reservation.request,
                             'Committed preimage differs from reviewed base')
            evidence = dict(pre_version_id=preimage.version_id,
                            preimage_digest=preimage.state_digest, create_intent_event_id=None)
        elif isinstance(preimage, c.CreateIntentRef):
            if (row.binding.space_id is not None
                    or preimage.operation_id != reservation.request.operation_id
                    or preimage.request_digest != reservation.request.request_digest
                    or not self._io(self.verify_create_intent, preimage)):
                self._reject(row, reservation.request,
                             'Committed provisional create intent required')
            evidence = dict(pre_version_id=None, preimage_digest=None,
                            create_intent_event_id=preimage.event_id)
        else:
            raise CoordinationError('Unsupported committed evidence')
        self._history(row, reservation.request)
        if authorization.approval_id is not None:
            use = self._io(self.facts.approval_use, row.binding, authorization.approval_id)
            if use.ambiguous or use.consumptions or use.operation_ids:
                self._reject(row, reservation.request,
                             'Consumed or ambiguous approval cannot authorize another attempt')
        row = self._cas(row, lambda r: (r.state == c.CoordinationState.RESERVED
            and r.lease_expires_at > self.clock.now()
            and authorization.expires_at > self.clock.now()),
            state=c.CoordinationState.ADMITTED,
            active_operation_id=reservation.request.operation_id,
            idempotency_key=reservation.request.idempotency_key,
            request_digest=reservation.request.request_digest,
            approval_id=authorization.approval_id, approval_digest=authorization.approval_digest,
            expected_base_fingerprint=authorization.expected_base_fingerprints.state_digest,
            admitted_at=self.clock.now(),
            # Renew the lease for the mutation phase. Admission itself (authorization
            # re-validation, SCIM group resolution, ledger verification) can consume most of
            # the reserve-time lease on a live platform, so the mutation must start with a
            # fresh window rather than the residue of the reservation lease.
            lease_expires_at=self.clock.now() + self._LEASE_TTL,
            mutation_stage=c.PatchStage.CONFIG_PENDING,
            checkpoint={'schema_version': 'VC/1.0', 'preimage': c.to_wire(preimage),
                        'stages': [s.value for s in self._planned_stages(authorization)]},
            **evidence)
        claim = c.AdmissionClaim(row.binding.binding_id, row.binding.binding_revision,
            row.attempt_id, row.generation, row.row_version, reservation.request,
            preimage, row.approval_id, row.approval_digest)
        try:
            row = self._publish_consumption(claim, row)
        except CoordinationError:
            self._cas(row, lambda r: r.attempt_id == claim.attempt_id,
                      state=c.CoordinationState.QUARANTINED,
                      quarantine_reason='Consumption publication failed/ambiguous')
            raise
        return claim

    def _publish_consumption(self, claim, row=None):
        self._io(self.facts.publish_consumption, claim)
        if not self._io(self.facts.verify_flush, claim):
            raise AuthorityUnavailable('Consumption facts not durably verified')
        # Truthful column: a durable consumption fact exists for the approval bound
        # to THIS row. No approval bound means nothing was consumed.
        if (row is not None and claim.approval_id is not None
                and not row.approval_consumption_published):
            return self._cas(row, lambda r: r.attempt_id == claim.attempt_id,
                             approval_consumption_published=True)
        return row

    @staticmethod
    def _presend(row):
        """Durable proof that no Genie mutation can have been issued for this attempt."""
        return (row.mutation_stage in {None, c.PatchStage.CONFIG_PENDING}
                and 'resume_classification' not in (row.checkpoint or {}))

    @staticmethod
    def _classification(row):
        """The only authority on what a quarantined attempt may have sent."""
        recorded = (row.checkpoint or {}).get('resume_classification')
        if recorded is not None:
            return recorded
        if row.mutation_stage in {c.PatchStage.CONFIG_IN_FLIGHT,
                                  c.PatchStage.DESCRIPTION_IN_FLIGHT}:
            # In-flight without a recorded classification: fail closed (C5.7 default).
            return 'read-only-possible-send'
        if row.admitted_at is None:
            return 'read-only-never-admitted'
        return 'read-only-presend'

    def _history_raw(self, row, idempotency_key):
        """lookup_request + ambiguity check ONLY. No digest predicate, so _reject can call it."""
        history = self._io(self.facts.lookup_request, row.binding, idempotency_key)
        if history.ambiguous:
            raise CoordinationError('Ambiguous durable request history')
        return history

    def _reject(self, row, request, message, *, status=c.FactStatus.CONFLICTED):
        """Definite pre-send rejection: publish the terminal fact, then free the binding."""
        if not self._presend(row):
            raise AuthorityUnavailable('Possible send cannot be closed as a definite rejection')
        ref = self._fact(row, request, status)
        if not any(f.event_id == ref.event_id
                   for f in self._history_raw(row, request.idempotency_key).facts):
            raise AuthorityUnavailable('Rejection fact not durably verified; row stays unresolved')
        self._release(row)
        raise _RejectionReleased(message)

    def _history(self, row, request):
        history = self._history_raw(row, request.idempotency_key)
        if any(f.request.request_digest != request.request_digest for f in history.facts):
            self._reject(row, request,
                         'Idempotency key already bound to a different request digest')
        return history

    def _release(self, row, *, generation=None):
        # Never erase heads or the monotonically increasing fence/observer sequence.
        return self._cas(row, lambda r: r.attempt_id == row.attempt_id,
            state=c.CoordinationState.IDLE, unresolved=False,
            generation=row.generation if generation is None else generation,
            holder=None, attempt_id=None, executor_kind=None, executor_ref=None,
            lease_expires_at=None, active_operation_id=None, idempotency_key=None,
            request_digest=None, approval_id=None, approval_digest=None,
            approval_consumption_published=False, pre_version_id=None, preimage_digest=None,
            create_intent_event_id=None, expected_base_fingerprint=None, admitted_at=None,
            mutation_stage=None, checkpoint=None, quarantine_reason=None, termination_evidence=None)

    def reject_reservation(self, reservation: c.Reservation, message: str) -> None:
        row = self._owned(reservation.fence)
        if row.state != c.CoordinationState.RESERVED:
            raise OwnershipError('Only a reserved pre-send attempt can be reject-released')
        if (reservation.request != self._request(row)
                or reservation.executor.principal_id != row.holder
                or reservation.executor.execution_ref != row.executor_ref):
            raise OwnershipError('Reservation identity differs from durable row')
        try:
            self._reject(row, reservation.request, message)
        except _RejectionReleased:
            return None

    def finish(self, claim: c.AdmissionClaim, result: c.OperationResult) -> None:
        self.assert_owner(claim)
        row = self._owned(claim)
        # Argument checks that cannot depend on mutation state run first and reject
        # a caller mistake without touching row state (409, binding intact).
        if (result.operation_id != row.active_operation_id
                or result.preimage != claim.preimage):
            raise CoordinationError('Result does not identify this admitted operation')
        # Possible-send: assume-possible-send obligation (C5.7); quarantine.
        if (row.mutation_stage in {c.PatchStage.CONFIG_IN_FLIGHT, c.PatchStage.DESCRIPTION_IN_FLIGHT}
                or result.unresolved or result.status in {
                    c.OperationStatus.APPLIED_UNVERIFIED, c.OperationStatus.APPLIED_PARTIAL}):
            self._quarantine_row(row, 'Possible send remains unresolved; GET cannot prove termination')
            raise CoordinationError('In-flight or ambiguous result cannot release the attempt')
        if result.status not in {c.OperationStatus.CONFIRMED, c.OperationStatus.NOOP,
                                 c.OperationStatus.CONFLICTED, c.OperationStatus.FAILED}:
            if self._presend(row):
                return self._reject(row, claim.request,
                    'Non-terminal result on a proven pre-send attempt',
                    status=c.FactStatus.FAILED)
            raise CoordinationError('Not a resolved terminal result')
        if result.postimage is not None:
            if ((result.postimage.binding_id, result.postimage.binding_revision) !=
                    (row.binding.binding_id, row.binding.binding_revision)
                    or not self._io(self.ledger.verify_committed, result.postimage)):
                raise CoordinationError('Terminal observation not committed to this binding')
        elif result.status in {c.OperationStatus.CONFIRMED, c.OperationStatus.NOOP}:
            if self._presend(row):
                return self._reject(row, claim.request,
                    'Successful completion requires committed postimage',
                    status=c.FactStatus.FAILED)
            raise CoordinationError('Successful completion requires committed postimage')
        row = self._publish_consumption(claim, row)
        # Terminal closure marker (receipt-equivalent). Not FactKind.RECEIPT: that kind is
        # reserved for the target-written DeploymentReceipt. This is a coordination-authored
        # OPERATION fact at _SEQ_CLOSURE, which reserve() keys idempotency/terminal detection on.
        ref = self._fact(row, claim.request, c.FactStatus(result.status.value),
                         kind=c.FactKind.OPERATION, sequence=_SEQ_CLOSURE,
                         post_version_id=result.postimage.version_id if result.postimage else None)
        history = self._history(row, claim.request)
        if not any(f.event_id == ref.event_id for f in history.facts):
            raise AuthorityUnavailable('Terminal receipt publication not verified')
        self._release(row)

    def _expire(self, row):
        if row.state in {c.CoordinationState.IDLE, c.CoordinationState.QUARANTINED}:
            return row
        if row.lease_expires_at is not None and row.lease_expires_at > self.clock.now():
            return row
        if row.state == c.CoordinationState.OBSERVING:
            return self._reclaim_observer(row)
        return self._quarantine_row(row, 'Lease expired; termination is unproven')

    def _reclaim_observer(self, row):
        # An observation lease never carried write authority; reclaim to IDLE.
        # Fail closed if the row somehow carries mutation authority.
        if (row.active_operation_id is not None or row.admitted_at is not None
                or row.approval_id is not None or not self._presend(row)):
            raise AuthorityUnavailable('Observation row carries mutation authority; refusing reclaim')
        return self._release(row, generation=row.generation + 1)

    def _quarantine_row(self, row, reason):
        if not reason.strip():
            raise CoordinationError('Quarantine reason is mandatory')
        row = self._cas(row, lambda r: r.attempt_id == row.attempt_id,
                        state=c.CoordinationState.QUARANTINED, unresolved=True,
                        quarantine_reason=reason)
        if row.active_operation_id is not None:
            self._fact(row, self._request(row), c.FactStatus.QUARANTINED, sequence=_SEQ_QUARANTINE)
        return row

    def quarantine(self, claim: c.FenceToken, reason: str) -> None:
        row = self._owned(claim, allow_quarantine=True)
        self._quarantine_row(row, reason)

    def recover(self, binding: c.BindingRef, evidence: c.RecoveryEvidence) -> c.RecoveryResult:
        self._current(binding)
        if evidence.fence.binding_id != binding.binding_id:
            raise OwnershipError('Recovery fence belongs to another binding')
        row = self._owned(evidence.fence, allow_quarantine=True)
        if not self._binding_eq(row.binding, binding) or row.state != c.CoordinationState.QUARANTINED:
            raise OwnershipError('Only the quarantined exact attempt can be recovered')
        trusted = self._io(self.termination.for_attempt, row.executor_ref, row.attempt_id)
        if (trusted is None or trusted != evidence.termination
                or trusted.attempt_id != row.attempt_id or trusted.execution_ref != row.executor_ref):
            raise CoordinationError('Trusted termination of the exact prior attempt is required')
        self._validate_termination(trusted)
        # Persist validated evidence BEFORE any fact append, so a crash between the
        # audit append and release leaves durable proof for the next worker.
        row = self._cas(row, lambda r: r.attempt_id == evidence.fence.attempt_id,
                        termination_evidence=c.to_wire(trusted))
        human = bool(evidence.human_authorization_reference and evidence.human_reason
                     and evidence.human_reason.strip() and evidence.residual_risk_acknowledged
                     and self._io(self.authorize_human_recovery, binding, evidence))
        self._validate_samples(evidence, human=human)
        derived = self._classification(row)
        if evidence.checkpoint_classification.strip() != derived:
            raise CoordinationError(
                f'Recovery classification must equal the durable checkpoint classification ({derived})')
        evidence = replace(evidence, checkpoint_classification=derived)
        if row.admitted_at is not None:
            row = self._publish_consumption(self._claim_from_row(row), row)
        request = self._request(row)
        # Strip AdmissionClaim's extra fields at the frozen FenceToken wire seam.
        evidence = replace(evidence, fence=self._fence(row))
        # Recovery CLOSES the operation: append the audit AND a terminal receipt so
        # the same idempotency key cannot be re-admitted (C5.5, C5.7). CONFLICTED,
        # never CONFIRMED/NOOP — recovery proves the executor is dead, not success.
        ref = self._fact(row, request, c.FactStatus.CONFLICTED,
                         evidence=evidence, kind=c.FactKind.RECOVERY, sequence=_SEQ_RECOVERY)
        receipt = self._fact(row, request, c.FactStatus.CONFLICTED,
                             evidence=evidence, kind=c.FactKind.OPERATION, sequence=_SEQ_CLOSURE)
        history = self._history_raw(row, request.idempotency_key)
        if not {ref.event_id, receipt.event_id} <= {f.event_id for f in history.facts}:
            raise AuthorityUnavailable(
                'Recovery audit/receipt publication not verified; row stays unresolved')
        released = self._release(row, generation=row.generation + 1)
        # A recovery result is a historical fence, not a new write capability.
        fence = c.FenceToken(binding.binding_id, binding.binding_revision,
                            row.attempt_id, released.generation, released.row_version)
        return c.RecoveryResult(binding, c.OperationStatus.CONFLICTED, fence, False,
                                (ref.event_id, receipt.event_id))

    def _claim_from_row(self, row):
        if not row.checkpoint or 'preimage' not in row.checkpoint:
            raise CoordinationError('Missing durable admission checkpoint')
        value_type = c.CreateIntentRef if row.create_intent_event_id else c.ObservationRef
        preimage = c.from_wire(value_type, row.checkpoint['preimage'])
        return c.AdmissionClaim(row.binding.binding_id, row.binding.binding_revision,
            row.attempt_id, row.generation, row.row_version, self._request(row),
            preimage, row.approval_id, row.approval_digest)

    def _validate_termination(self, trusted):
        if trusted.terminated_at > self.clock.now():
            raise CoordinationError('Future termination evidence')
        if trusted.source == 'job':
            # The trusted provider must enumerate the COMPLETE retry/child closure,
            # not merely a caller-provided list or cancellation acknowledgement.
            refs = [e.execution_ref for e in trusted.executions]
            if (trusted.execution_ref not in refs or len(refs) != len(set(refs))
                    or any(e.terminal_at > trusted.terminated_at for e in trusted.executions)):
                raise CoordinationError('Incomplete Job retry/child termination evidence')
        elif trusted.source == 'app-worker':
            if not trusted.app_worker_proof_digest:
                raise CoordinationError('Positive app-worker termination proof required')
        else:
            raise CoordinationError('Untrusted termination source')

    def _validate_samples(self, evidence, *, human=False):
        capability = self.verified_request_lifetime
        if not human and (capability is None or not isfinite(capability[0]) or capability[0] <= 0
                or not capability[1] or capability != (
                    evidence.maximum_request_lifetime_seconds, evidence.lifetime_bound_reference)):
            raise CoordinationError('Automatic recovery disabled: request lifetime is unverified')
        samples = evidence.samples
        if (len(samples) < 3 or len({s.state_digest for s in samples}) != 1
                or any(s.observed_at <= evidence.termination.terminated_at
                       or s.observed_at > self.clock.now() for s in samples)
                or any(a.observed_at >= b.observed_at for a, b in zip(samples, samples[1:]))
                or (not human and (samples[-1].observed_at - samples[0].observed_at).total_seconds() <= capability[0])):
            raise CoordinationError('Need three stable post-termination GETs spanning beyond verified lifetime')

    @staticmethod
    def _planned_stages(authorization):
        # M03 does not decide which fields change; M04's rendering does. Derive the
        # planned sequence from the validated grant, defaulting conservatively to
        # the full config+description march when no plan is declared (NB-5).
        planned = getattr(authorization, 'planned_stages', None)
        if not planned:
            return tuple(c.PatchStage)
        return tuple(planned)

    def checkpoint(self, claim: c.AdmissionClaim, stage: c.PatchStage,
                   evidence: c.StageEvidence) -> None:
        self.assert_owner(claim)
        row = self._owned(claim)
        if (evidence.stage != stage or evidence.recorded_at > self.clock.now()
                or evidence.recorded_at < row.admitted_at):
            raise CoordinationError('Checkpoint evidence has a stale stage or recorded_at')
        planned = [c.PatchStage(v) for v in ((row.checkpoint or {}).get('stages')
                                             or [s.value for s in c.PatchStage])]
        if row.mutation_stage not in planned or stage not in planned:
            raise CoordinationError('Stage is not part of this admitted operation')
        if planned.index(stage) != planned.index(row.mutation_stage) + 1:
            raise CoordinationError('Checkpoint stage must advance exactly once, never replay or skip')
        observed = stage in {c.PatchStage.CONFIG_OBSERVED, c.PatchStage.DESCRIPTION_OBSERVED}
        if observed:
            if (evidence.observation is None
                    or (evidence.observation.binding_id, evidence.observation.binding_revision) !=
                        (row.binding.binding_id, row.binding.binding_revision)
                    or not self._io(self.ledger.verify_committed, evidence.observation)
                    or self.validate_stage_evidence is None
                    or not self._io(self.validate_stage_evidence, claim, row, evidence)):
                self._quarantine_row(row, 'GET does not prove synchronous send completion')
                raise CoordinationError('Trusted send completion and committed observation required')
        row = self._publish_consumption(claim, row)
        checkpoint = dict(row.checkpoint or {})
        checkpoint.update(evidence=c.to_wire(evidence),
            resume_classification=('read-only-possible-send' if stage in {
                c.PatchStage.CONFIG_IN_FLIGHT, c.PatchStage.DESCRIPTION_IN_FLIGHT}
                else 'read-only-unless-next-stage-proven-unsent'))
        # Heartbeat: each proven stage advance renews the mutation lease, so a multi-stage
        # mutation making forward progress never expires mid-flight. The lease then only has
        # to cover the gap between two consecutive checkpoints, not the whole operation.
        self._cas(row, lambda r: (r.attempt_id == claim.attempt_id
                                  and r.mutation_stage == row.mutation_stage),
                  mutation_stage=stage, checkpoint=checkpoint,
                  lease_expires_at=self.clock.now() + self._LEASE_TTL)

    def observe_exclusively(self, binding: c.BindingRef,
                            executor: c.ExecutorContext) -> c.ObservationLease:
        row = self._expire(self._row(binding))
        if (row.unresolved or row.state != c.CoordinationState.IDLE
                or executor.workspace_id != binding.workspace_id or not executor.execution_ref):
            raise OwnershipError('Binding busy or executor not target-local; return stale/checkpoint')
        row = self._cas(row, lambda r: not r.unresolved and r.state == c.CoordinationState.IDLE,
            state=c.CoordinationState.OBSERVING, unresolved=True,
            generation=row.generation + 1, attempt_id=str(uuid4()), holder=executor.principal_id,
            executor_kind=executor.actor_kind, executor_ref=executor.execution_ref,
            lease_expires_at=self.clock.now() + self._LEASE_TTL,
            observed_sequence=row.observed_sequence + 1)
        return c.ObservationLease(self._fence(row), row.observed_sequence, row.lease_expires_at)

    def release_observation(self, fence: c.FenceToken) -> None:
        """Finalize an OBSERVING lease with no head change (back to IDLE).

        The unchanged-capture path (M04 Observer) takes an observation lease, finds the
        live target still equal to the committed observed head, and appends no version --
        so there is no head to advance. `advance_heads` deliberately rejects an empty
        update (BLOCK-3: an all-None update must never silently consume a lease), so the
        no-op observation is released here instead, preserving heads/observed sequence
        exactly like `advance_heads`'s observing-lease finalization. Only the lease owner
        holding an OBSERVING row may release; anything else fails closed.
        """
        row = self._owned(fence)
        if row.state != c.CoordinationState.OBSERVING:
            raise OwnershipError('Only an active observation lease can be released')
        self._release(row)

    def _advances(self, binding, current, candidate):
        """True iff candidate is current, or a descendant of current, by committed lineage."""
        if current is None:
            return True
        seen, node = set(), candidate
        while node is not None and node not in seen:
            if node == current:
                return True
            seen.add(node)
            version = self._io(self.ledger.get_version, binding, node)
            if version is None or not self._binding_eq(version.context.binding, binding):
                raise CoordinationError('Head lineage is not committed to this binding')
            node = version.context.parent_version_id
        return False

    def _committed_head(self, row, version_id, name):
        """Prove `version_id` is a version committed to this exact binding/revision."""
        version = self._io(self.ledger.get_version, row.binding, version_id)
        if version is None or version.version_id != version_id:
            raise CoordinationError(f'{name} head does not resolve to a committed version')
        if not self._binding_eq(version.context.binding, row.binding):
            raise CoordinationError(f'{name} head belongs to another binding or revision')
        ref = c.ObservationRef(version.version_id, row.binding.binding_id,
                               row.binding.binding_revision,
                               version.snapshot.state_digest,
                               version.snapshot.response_envelope_digest)
        if not self._io(self.ledger.verify_committed, ref):
            raise CoordinationError(f'{name} head is not committed evidence')
        return version

    def advance_heads(self, fence: c.FenceToken, update: c.HeadUpdate) -> c.Heads:
        row = self._owned(fence)
        if update.observed is None and update.approved is None and update.deployed is None:
            raise CoordinationError('Empty head update')
        observing = row.state == c.CoordinationState.OBSERVING
        if not observing:
            # Any non-observer head write requires a verified admission. assert_owner
            # re-checks AdmissionClaim + ADMITTED + request/approval/preimage vs row.
            if not isinstance(fence, c.AdmissionClaim):
                raise OwnershipError('Head updates require an observation lease or an admission claim')
            self.assert_owner(fence)
        # Evidence before policy: resolve every governed head to a committed version
        # of THIS binding before asking the policy oracle to authorize it.
        for name in ('approved', 'deployed'):
            value = getattr(update, name)
            if value is not None:
                self._committed_head(row, value, name)
        if update.approved is not None or update.deployed is not None:
            if (observing or not update.authorization_reference
                    or not self._io(self.authorize_heads, row.binding, fence, update)):
                raise CoordinationError('Approved/deployed heads require verified policy/deployment evidence')
        if update.observed is not None:
            version = self._io(self.ledger.get_version, row.binding, update.observed)
            context = version.context
            ref = c.ObservationRef(version.version_id, row.binding.binding_id,
                row.binding.binding_revision, version.snapshot.state_digest,
                version.snapshot.response_envelope_digest)
            if (version.version_id != update.observed or not self._binding_eq(context.binding, row.binding)
                    or context.attempt_id != row.attempt_id or context.generation != row.generation
                    or context.parent_version_id != row.heads.observed
                    or not self._io(self.ledger.verify_committed, ref)):
                raise CoordinationError('Observation is stale, uncommitted or regresses the serialized head')
        # Governed heads may only advance along committed lineage, never regress.
        for name in ('approved', 'deployed'):
            value = getattr(update, name)
            if value is not None and not self._advances(row.binding, getattr(row.heads, name), value):
                raise CoordinationError(f'{name} head cannot regress or leave the committed lineage')
        final_approved = row.heads.approved if update.approved is None else update.approved
        final_deployed = row.heads.deployed if update.deployed is None else update.deployed
        if final_deployed is not None and not self._advances(row.binding, final_deployed, final_approved):
            raise CoordinationError('Deployed head must not lead the approved head')
        heads = c.Heads(row.heads.observed if update.observed is None else update.observed,
                        final_approved, final_deployed)
        row = self._cas(row, lambda r: r.attempt_id == fence.attempt_id, heads=heads)
        if observing:
            self._release(row)  # advance_heads is observation lease finalization.
        return heads
