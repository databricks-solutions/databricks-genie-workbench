"""M03 contract tests: durable fake stores, explicit clock, no Genie calls."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from backend.services.version_control import contracts as c
from backend.services.version_control.coordination import CoordinationService, CoordinationError
from backend.services.version_control.coordination.service import (
    AuthorityUnavailable, OwnershipError, _SEQ_CLOSURE)
from backend.tests.vc_fakes.fixtures import binding_fixture, executor_fixture
from backend.tests.vc_fakes.stores import (
    CoordinationRow, FakeCoordinationStore, FakeFactStore, ManualClock,
)


def uid():
    return str(uuid4())


@pytest.fixture
def h():
    clock = ManualClock(datetime(2026, 9, 7, tzinfo=timezone.utc))
    binding = binding_fixture()
    store = FakeCoordinationStore(clock)
    durable = FakeFactStore(clock)
    facts = Mock(spec=c.OperationFacts)
    facts.lookup_request.return_value = c.RequestHistory((), False)
    facts.approval_use.side_effect = lambda b, a: c.ApprovalUse(b, a, (), (), False)
    facts.verify_flush.return_value = True
    ledger = Mock(spec=c.VersionLedger)
    ledger.verify_committed.return_value = True
    termination = Mock(spec=c.TerminationEvidenceProvider)
    termination.for_attempt.return_value = None
    ports = dict(store=store, facts=facts, ledger=ledger, clock=clock,
                 resolve_binding=Mock(return_value=binding),
                 verify_enrollment=Mock(return_value=True),
                 insert_enrolled=store.seed, validate_authorization=Mock(return_value=True),
                 verify_create_intent=Mock(return_value=True), termination=termination,
                 authorize_human_recovery=Mock(return_value=False),
                 authorize_heads=Mock(return_value=False),
                 validate_stage_evidence=Mock(return_value=True))
    service = CoordinationService(**ports)
    proof = c.EnrollmentProof(uid(), binding.binding_id, 1, 'serialized-enrollment')
    request = c.RequestIdentity(uid(), 'key-1', '1' * 64)
    fp = c.Fingerprints('2' * 64, '3' * 64, '4' * 64, 'vc-c14n/1')
    preimage = c.ObservationRef(uid(), binding.binding_id, 1, fp.state_digest, '5' * 64)
    grant = c.AuthorizationGrant(binding, request, 'validated-policy', fp, '6' * 64,
                                 clock.now() + timedelta(hours=1), uid(), '7' * 64)
    return SimpleNamespace(**ports, service=service, binding=binding, proof=proof,
                           request=request, preimage=preimage, grant=grant,
                           executor=executor_fixture(), durable=durable)


def enroll(h):
    h.service.initialize(h.binding, h.proof)


def reserve(h):
    return h.service.reserve(h.binding, h.request, h.executor)


def admit(h):
    return h.service.admit(reserve(h), h.preimage, h.grant)


def test_reject_reservation_releases_presend_with_durable_conflict(h):
    durable_facts(h)
    enroll(h)
    row = h.store.read(h.binding.binding_id)
    h.store.compare_and_swap(h.binding.binding_id, 1, row.row_version, row.generation,
                             lambda current: current == row,
                             heads=c.Heads(uid(), uid(), uid()), observed_sequence=7)
    reservation = reserve(h)
    before = h.store.read(h.binding.binding_id)

    assert h.service.reject_reservation(reservation, 'Reviewed base changed') is None

    released = h.store.read(h.binding.binding_id)
    assert released.state == c.CoordinationState.IDLE
    assert not released.unresolved
    assert released.heads == before.heads
    assert released.observed_sequence == before.observed_sequence
    assert released.generation == before.generation
    assert released.row_version == before.row_version + 1
    history = h.facts.lookup_request(h.binding, h.request.idempotency_key)
    assert len(history.facts) == 1
    fact = history.facts[0]
    assert fact.status == c.FactStatus.CONFLICTED
    assert fact.request == reservation.request
    assert fact.attempt_id == reservation.fence.attempt_id
    assert fact.generation == reservation.fence.generation
    corrected = c.RequestIdentity(uid(), 'corrected-key', 'b' * 64)
    assert h.service.reserve(h.binding, corrected, h.executor).request == corrected


@pytest.mark.parametrize('in_flight', [False, True])
def test_reject_reservation_refuses_admitted_attempt(h, in_flight):
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    claim = h.service.admit(reservation, h.preimage, h.grant)
    if in_flight:
        h.service.checkpoint(claim, c.PatchStage.CONFIG_IN_FLIGHT,
            c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(),
                            h.request.request_digest, None))
    before = h.store.read(h.binding.binding_id)
    facts_before = tuple(h.durable.state.rows)

    with pytest.raises(OwnershipError):
        h.service.reject_reservation(reservation, 'Cannot bypass quarantine')

    assert h.store.read(h.binding.binding_id) == before
    assert tuple(h.durable.state.rows) == facts_before


@pytest.mark.parametrize('foreign', ['fence', 'request', 'principal', 'executor_ref'])
def test_reject_reservation_refuses_foreign_reservation(h, foreign):
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    if foreign == 'fence':
        reservation = replace(reservation, fence=replace(reservation.fence, attempt_id=uid()))
    elif foreign == 'request':
        reservation = replace(reservation, request=replace(h.request, request_digest='a' * 64))
    elif foreign == 'principal':
        reservation = replace(reservation, executor=replace(h.executor, principal_id='foreign'))
    else:
        reservation = replace(reservation, executor=replace(h.executor, execution_ref='foreign/run'))
    before = h.store.read(h.binding.binding_id)

    with pytest.raises(OwnershipError):
        h.service.reject_reservation(reservation, 'Foreign reservation')

    assert h.store.read(h.binding.binding_id) == before
    assert not h.durable.state.rows


@pytest.mark.parametrize('failure', ['append', 'unverified', 'ambiguous', 'cas', 'possible_send'])
def test_reject_reservation_fails_closed_without_proven_release(h, failure):
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    expected_error = AuthorityUnavailable
    if failure == 'append':
        h.facts.append.side_effect = RuntimeError('Unavailable')
    elif failure in {'unverified', 'ambiguous'}:
        h.facts.lookup_request.side_effect = None
        h.facts.lookup_request.return_value = c.RequestHistory((), failure == 'ambiguous')
        if failure == 'ambiguous':
            expected_error = CoordinationError
    elif failure == 'cas':
        h.store.compare_and_swap = Mock(return_value=None)
        expected_error = OwnershipError
    else:
        row = h.store.read(h.binding.binding_id)
        h.store.compare_and_swap(h.binding.binding_id, 1, row.row_version, row.generation,
                                 lambda current: current == row,
                                 checkpoint={'resume_classification': 'read-only-possible-send'})
    before = h.store.read(h.binding.binding_id)

    with pytest.raises(expected_error):
        h.service.reject_reservation(reservation, 'Ambiguous durable request history')

    assert h.store.read(h.binding.binding_id) == before
    assert before.unresolved


def test_missing_or_duplicate_ownership_row_blocks_admission(h):
    with pytest.raises(CoordinationError):
        reserve(h)
    assert not h.store.state.rows
    h.verify_enrollment.return_value = False
    with pytest.raises(CoordinationError):
        enroll(h)
    h.verify_enrollment.return_value = True
    enroll(h)
    enroll(h)  # idempotent enrollment does not INSERT twice
    assert len(h.store.state.rows) == 1
    h.resolve_binding.return_value = replace(h.binding, binding_revision=2)
    with pytest.raises(CoordinationError):
        reserve(h)
    h.resolve_binding.return_value = h.binding
    h.store.seed(CoordinationRow(h.binding, h.clock.now()))
    with pytest.raises(CoordinationError):
        reserve(h)


def test_approved_and_deployed_heads_cannot_regress(h):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    snapshot = FakeCanonicalizer().observe({'serialized_space': {}, 'description': ''})
    v1, v2, v3 = uid(), uid(), uid()
    parents = {v1: None, v2: v1, v3: v2}

    def version(vid, parent, binding=None):
        return c.Version(vid, snapshot, c.CaptureContext(binding or h.binding, 'obs',
            h.clock.now(), 'open', c.ActorContext('a', h.binding.workspace_id, 'service'),
            c.Origin.EXTERNAL, attempt_id=uid(), generation=0, parent_version_id=parent))

    chain = dict(parents)
    extra = {}

    def get_version(binding, version_id):
        if version_id in extra:
            return extra[version_id]
        if version_id in chain:
            return version(version_id, chain[version_id])
        return None

    h.ledger.get_version.side_effect = get_version
    h.authorize_heads.return_value = True

    # 1. baseline: advance both heads to v2.
    heads = h.service.advance_heads(claim, c.HeadUpdate(None, v2, v2, 'ref'))
    assert heads == c.Heads(None, v2, v2)

    original_cas = h.store.compare_and_swap
    h.store.compare_and_swap = Mock(wraps=original_cas)
    before = h.store.read(h.binding.binding_id).heads

    # 2/3. regress approved to an ancestor -> raises, no CAS.
    with pytest.raises(CoordinationError):
        h.service.advance_heads(claim, c.HeadUpdate(None, v1, None, 'ref'))
    assert h.store.read(h.binding.binding_id).heads == before
    assert h.store.compare_and_swap.call_count == 0

    # 4. deployed leading approved raises (approved still v2).
    with pytest.raises(CoordinationError):
        h.service.advance_heads(claim, c.HeadUpdate(None, None, v3, 'ref'))
    assert h.store.read(h.binding.binding_id).heads == before

    # 5. an unrelated chain (parent never reaches v2) raises.
    unrelated = uid()
    extra[unrelated] = version(unrelated, uid())  # parent is an unknown id
    with pytest.raises(CoordinationError):
        h.service.advance_heads(claim, c.HeadUpdate(None, unrelated, None, 'ref'))

    # 6. a lineage cycle terminates with a CoordinationError.
    va, vb = uid(), uid()
    extra[va] = version(va, vb)
    extra[vb] = version(vb, va)
    with pytest.raises(CoordinationError):
        h.service.advance_heads(claim, c.HeadUpdate(None, va, None, 'ref'))

    # 7. genuine forward advance v2 -> v3 for both heads succeeds.
    h.store.compare_and_swap = original_cas
    heads = h.service.advance_heads(claim, c.HeadUpdate(None, v3, v3, 'ref'))
    assert heads == c.Heads(None, v3, v3)


@pytest.mark.parametrize('head', ['approved', 'deployed'])
@pytest.mark.parametrize('defect', ['unknown-id', 'foreign-binding', 'old-revision', 'uncommitted'])
def test_approved_and_deployed_heads_require_committed_versions_of_this_binding(h, head, defect):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer
    durable_facts(h)
    if defect == 'old-revision':
        h.binding = replace(h.binding, binding_revision=2)
        h.resolve_binding.return_value = h.binding
        h.grant = replace(h.grant, binding=h.binding)
        h.preimage = replace(h.preimage, binding_revision=2)
        h.proof = c.EnrollmentProof(uid(), h.binding.binding_id, 2, 'serialized-enrollment')
    enroll(h)
    claim = admit(h)
    snapshot = FakeCanonicalizer().observe({'serialized_space': {}, 'description': ''})
    value = uid()

    def context_for(binding):
        return c.CaptureContext(binding, 'obs', h.clock.now(), 'open',
            c.ActorContext('a', h.binding.workspace_id, 'service'), c.Origin.EXTERNAL,
            attempt_id=uid(), generation=0)

    def get_version(binding, version_id):
        if defect == 'unknown-id' or version_id != value:
            return None
        if defect == 'foreign-binding':
            return c.Version(value, snapshot, context_for(replace(h.binding, binding_id=uid())))
        if defect == 'old-revision':
            return c.Version(value, snapshot, context_for(replace(h.binding, binding_revision=1)))
        return c.Version(value, snapshot, context_for(h.binding))

    h.ledger.get_version.side_effect = get_version
    if defect == 'uncommitted':
        h.ledger.verify_committed.side_effect = lambda ref: getattr(ref, 'version_id', None) != value
    h.authorize_heads.return_value = True

    original_cas = h.store.compare_and_swap
    h.store.compare_and_swap = Mock(wraps=original_cas)
    before = h.store.read(h.binding.binding_id).heads
    update = c.HeadUpdate(None, value, None, 'ref') if head == 'approved' \
        else c.HeadUpdate(None, None, value, 'ref')
    with pytest.raises(CoordinationError):
        h.service.advance_heads(claim, update)
    # 2. no partial write.
    assert h.store.read(h.binding.binding_id).heads == before
    assert h.store.compare_and_swap.call_count == 0
    # 3. evidence resolution precedes policy.
    h.authorize_heads.assert_not_called()
    # 5. resolution was on the path.
    h.ledger.get_version.assert_any_call(h.binding, value)


def test_approved_head_positive_control_and_earlier_attempt_allowed(h):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    snapshot = FakeCanonicalizer().observe({'serialized_space': {}, 'description': ''})
    value = uid()

    def get_version(binding, version_id):
        if version_id != value:
            return None
        # captured under an EARLIER attempt/generation than the admitted claim.
        return c.Version(value, snapshot, c.CaptureContext(h.binding, 'obs', h.clock.now(), 'open',
            c.ActorContext('a', h.binding.workspace_id, 'service'), c.Origin.EXTERNAL,
            attempt_id=uid(), generation=0))

    h.ledger.get_version.side_effect = get_version
    h.authorize_heads.return_value = True
    # 6 & 7: an approved head captured under an earlier attempt is accepted.
    heads = h.service.advance_heads(claim, c.HeadUpdate(None, value, None, 'ref'))
    assert heads.approved == value


@pytest.mark.parametrize('head', ['observed', 'approved', 'deployed'])
def test_reservation_fence_cannot_advance_any_head(h, head):
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    v = uid()
    h.authorize_heads.return_value = True  # policy is NOT the missing gate.
    update = {
        'observed': c.HeadUpdate(v, None, None, 'ref'),
        'approved': c.HeadUpdate(None, v, None, 'ref'),
        'deployed': c.HeadUpdate(None, None, v, 'ref'),
    }[head]
    with pytest.raises(CoordinationError):
        h.service.advance_heads(reservation.fence, update)
    # 2. no partial write.
    assert h.store.read(h.binding.binding_id).heads == c.Heads(None, None, None)
    # 3. authority precedes the policy callback.
    if head in {'approved', 'deployed'}:
        h.authorize_heads.assert_not_called()


def test_empty_head_update_rejected_and_lease_retained(h):
    enroll(h)
    lease = h.service.observe_exclusively(h.binding, h.executor)
    with pytest.raises(CoordinationError):
        h.service.advance_heads(lease.fence, c.HeadUpdate(None, None, None, None))
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.OBSERVING and row.unresolved


def test_release_observation_finalizes_unchanged_lease_to_idle(h):
    """The M04 unchanged-capture path appends no version, so it finalizes its observation
    lease via release_observation rather than advance_heads (which rejects the all-None
    update). The row returns to IDLE with heads/observed_sequence preserved; only the
    OBSERVING lease owner may release, and a stale fence fails closed."""
    enroll(h)
    lease = h.service.observe_exclusively(h.binding, h.executor)
    before = h.store.read(h.binding.binding_id)
    h.service.release_observation(lease.fence)
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.IDLE and not row.unresolved
    assert row.holder is None and row.attempt_id is None
    assert row.heads == before.heads and row.observed_sequence == before.observed_sequence
    with pytest.raises(OwnershipError):  # no active lease left to release
        h.service.release_observation(lease.fence)


def test_bound_binding_matches_its_provisionally_enrolled_row(h):
    """A space enrolled provisionally (space_id=None) and later BOUND fills its space_id at
    the SAME binding_revision (bind_created bumps no revision), so the coordination row
    freezes space_id=None. `_binding_eq` treats space_id as registry-authoritative (already
    pinned by `_current`), so post-bind ownership ops -- observe_exclusively/reserve --
    match the provisional row instead of failing OwnershipError. A genuine revision
    mismatch still fails closed."""
    provisional = replace(h.binding, space_id=None)
    h.proof = replace(h.proof, binding_id=provisional.binding_id, binding_revision=1)
    h.resolve_binding.return_value = provisional
    h.service.initialize(provisional, h.proof)
    assert h.store.read(provisional.binding_id).binding.space_id is None
    # Registry now BOUND at the same revision; the coordination row still says space_id=None.
    bound = h.binding  # space_id set
    h.resolve_binding.return_value = bound
    lease = h.service.observe_exclusively(bound, h.executor)  # OwnershipError before the fix
    assert lease.fence.binding_id == bound.binding_id
    h.service.release_observation(lease.fence)
    reservation = h.service.reserve(bound, h.request, h.executor)
    assert reservation.fence.binding_id == bound.binding_id
    # A real binding_revision mismatch is still rejected.
    stale = replace(bound, binding_revision=2)
    h.resolve_binding.return_value = stale
    with pytest.raises(OwnershipError):
        h.service.reserve(stale, h.request, h.executor)


def test_admission_claim_can_advance_heads_positive_control(h):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer
    durable_facts(h)
    enroll(h)
    # Establish an observed head first via an observer lease.
    lease = h.service.observe_exclusively(h.binding, h.executor)
    snapshot = FakeCanonicalizer().observe({'serialized_space': {}, 'description': ''})
    v = uid()
    context = c.CaptureContext(h.binding, 'observer-1', h.clock.now(), 'open',
        c.ActorContext('observer', h.binding.workspace_id, 'service'), c.Origin.EXTERNAL,
        attempt_id=lease.fence.attempt_id, generation=lease.fence.generation)
    h.ledger.get_version.return_value = c.Version(v, snapshot, context)
    h.service.advance_heads(lease.fence, c.HeadUpdate(v, None, None, None))
    h.service.quarantine(h.service.observe_exclusively(h.binding, h.executor).fence, 'reset')
    h.store.state.rows[:] = [replace(h.store.state.rows[0],
        state=c.CoordinationState.IDLE, unresolved=False, holder=None, attempt_id=None,
        active_operation_id=None, idempotency_key=None, request_digest=None,
        lease_expires_at=None, mutation_stage=None, checkpoint=None)]
    claim = admit(h)
    # 5. positive control: admitted claim + authorized policy succeeds.
    h.authorize_heads.return_value = True
    heads = h.service.advance_heads(claim, c.HeadUpdate(None, v, v, 'verified-deployment'))
    assert heads == c.Heads(v, v, v)
    # 6. assert_owner durable-row comparison is on the path.
    with pytest.raises(CoordinationError):
        h.service.advance_heads(replace(claim, request=replace(h.request, request_digest='f' * 64)),
                                c.HeadUpdate(None, v, v, 'verified-deployment'))


@pytest.mark.parametrize('shape', ['config-only', 'description-only', 'config-and-description'])
def test_checkpoint_stage_machine_is_scoped_to_the_admitted_operation(h, shape):
    durable_facts(h)
    enroll(h)
    plans = {
        'config-only': (c.PatchStage.CONFIG_PENDING, c.PatchStage.CONFIG_IN_FLIGHT,
                        c.PatchStage.CONFIG_OBSERVED),
        'description-only': (c.PatchStage.CONFIG_PENDING, c.PatchStage.DESCRIPTION_PENDING,
                             c.PatchStage.DESCRIPTION_IN_FLIGHT, c.PatchStage.DESCRIPTION_OBSERVED),
        'config-and-description': tuple(c.PatchStage),
    }
    h.service._planned_stages = staticmethod(lambda auth: plans[shape])
    claim = admit(h)

    def cp(stage, observation=None):
        h.service.checkpoint(claim, stage,
            c.StageEvidence('VC/1.0', stage, h.clock.now(), 'a' * 64, observation))

    planned = plans[shape]
    if shape == 'config-only':
        cp(c.PatchStage.CONFIG_IN_FLIGHT)
        cp(c.PatchStage.CONFIG_OBSERVED, h.preimage)
        # 2. a description stage is not part of this operation.
        with pytest.raises(CoordinationError, match='not part of this admitted operation'):
            cp(c.PatchStage.DESCRIPTION_PENDING)
        assert h.store.read(h.binding.binding_id).mutation_stage == c.PatchStage.CONFIG_OBSERVED
        # 1. terminate from CONFIG_OBSERVED.
        complete(h, claim)
        assert not h.store.read(h.binding.binding_id).unresolved
        return

    if shape == 'description-only':
        # 3. first checkpoint is DESCRIPTION_PENDING and succeeds from CONFIG_PENDING.
        cp(c.PatchStage.DESCRIPTION_PENDING)
        assert h.store.read(h.binding.binding_id).mutation_stage == c.PatchStage.DESCRIPTION_PENDING
        # 4. a config stage is not part of this operation.
        with pytest.raises(CoordinationError):
            cp(c.PatchStage.CONFIG_IN_FLIGHT)
        return

    # config-and-description: walk all planned transitions.
    for stage in planned[1:]:
        obs = h.preimage if stage in {c.PatchStage.CONFIG_OBSERVED,
                                      c.PatchStage.DESCRIPTION_OBSERVED} else None
        cp(stage, obs)
    assert h.store.read(h.binding.binding_id).mutation_stage == planned[-1]


def test_checkpoint_replay_skip_and_attempt_fencing(h, monkeypatch):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    send = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(), 'a' * 64, None)
    h.service.checkpoint(claim, send.stage, send)
    # 5. replay current stage raises.
    with pytest.raises(CoordinationError):
        h.service.checkpoint(claim, send.stage, send)
    # skipping a planned stage raises.
    with pytest.raises(CoordinationError):
        h.service.checkpoint(claim, c.PatchStage.DESCRIPTION_PENDING,
            c.StageEvidence('VC/1.0', c.PatchStage.DESCRIPTION_PENDING, h.clock.now(), 'a' * 64, None))

    # 6. attempt fencing: a foreign attempt_id row is rejected without change.
    row = h.store.read(h.binding.binding_id)
    foreign = replace(row, attempt_id=uid())
    original_cas = h.store.compare_and_swap

    def takeover_inside_cas(*args, **changes):
        assert h.store.read(h.binding.binding_id) == row
        assert changes['mutation_stage'] == c.PatchStage.CONFIG_OBSERVED
        h.store.state.rows[:] = [foreign]
        return original_cas(*args, **changes)

    takeover_cas = Mock(side_effect=takeover_inside_cas)
    observed = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_OBSERVED, h.clock.now(), 'a' * 64, h.preimage)
    with monkeypatch.context() as patch:
        patch.setattr(h.store, 'compare_and_swap', takeover_cas)
        with pytest.raises(CoordinationError):
            h.service.checkpoint(claim, observed.stage, observed)
    takeover_cas.assert_called_once()
    after = h.store.read(h.binding.binding_id)
    assert after.attempt_id == foreign.attempt_id
    assert after.mutation_stage == c.PatchStage.CONFIG_IN_FLIGHT
    assert after == foreign

    # 7. recorded_at bounds raise with a distinguishable message.
    h.store.state.rows[:] = [row]
    with pytest.raises(CoordinationError, match='recorded'):
        h.service.checkpoint(claim, c.PatchStage.CONFIG_OBSERVED,
            c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_OBSERVED,
                            h.clock.now() + timedelta(seconds=1), 'a' * 64, h.preimage))
    with pytest.raises(CoordinationError, match='recorded'):
        h.service.checkpoint(claim, c.PatchStage.CONFIG_OBSERVED,
            c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_OBSERVED,
                            row.admitted_at - timedelta(seconds=1), 'a' * 64, h.preimage))


@pytest.mark.parametrize('case', ['confirmed-no-postimage', 'wrong-preimage',
    'wrong-operation-id', 'compensation-attempted', 'noop-no-postimage'])
def test_finish_releases_only_on_proven_presend_failure(h, case):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    row = h.store.read(h.binding.binding_id)
    # _presend holds: CONFIG_PENDING and no resume_classification.
    assert row.mutation_stage == c.PatchStage.CONFIG_PENDING
    assert 'resume_classification' not in (row.checkpoint or {})

    if case == 'confirmed-no-postimage':
        result = c.OperationResult(claim.request.operation_id, c.OperationStatus.CONFIRMED,
                                   claim.preimage, None, False, ('x',))
    elif case == 'wrong-preimage':
        result = c.OperationResult(claim.request.operation_id, c.OperationStatus.CONFIRMED,
            c.ObservationRef(uid(), h.binding.binding_id, 1, '2' * 64, '5' * 64), h.preimage, False, ('x',))
    elif case == 'wrong-operation-id':
        result = c.OperationResult(uid(), c.OperationStatus.CONFIRMED,
                                   claim.preimage, h.preimage, False, ('x',))
    elif case == 'compensation-attempted':
        result = c.OperationResult(claim.request.operation_id,
            c.OperationStatus.COMPENSATION_ATTEMPTED, claim.preimage, None, False, ('x',))
    elif case == 'noop-no-postimage':
        result = c.OperationResult(claim.request.operation_id, c.OperationStatus.NOOP,
                                   claim.preimage, None, False, ('x',))

    with pytest.raises(CoordinationError):
        h.service.finish(claim, result)

    # wrong-preimage / wrong-operation-id are argument errors: rejected before any
    # state change (row still ADMITTED+unresolved). The other three are proven
    # pre-send failures and must release.
    row = h.store.read(h.binding.binding_id)
    if case in {'wrong-preimage', 'wrong-operation-id'}:
        assert row.state == c.CoordinationState.ADMITTED and row.unresolved
        return

    assert row.state == c.CoordinationState.IDLE and not row.unresolved
    assert row.attempt_id is None and row.quarantine_reason is None
    assert row.heads == c.Heads(None, None, None)
    assert row.observed_sequence == 0
    facts = [c.from_wire(c.OperationFact, r.payload) for r in h.durable.state.rows
             if c.from_wire(c.OperationFact, r.payload).request.idempotency_key == claim.request.idempotency_key
             and c.from_wire(c.OperationFact, r.payload).status == c.FactStatus.FAILED]
    assert len(facts) == 1
    assert facts[0].fact_kind == c.FactKind.OPERATION
    assert facts[0].attempt_id == claim.attempt_id
    # binding reusable.
    h.request = c.RequestIdentity(uid(), 'key-after', 'a' * 64)
    fresh = h.service.reserve(h.binding, h.request, h.executor)
    assert isinstance(fresh, c.Reservation)


def test_finish_possible_send_and_publication_failures_stay_unresolved(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    send = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(), 'a' * 64, None)
    h.service.checkpoint(claim, send.stage, send)
    # 7. caller error on a possible-send row is not released (quarantined).
    bad = c.OperationResult(claim.request.operation_id, c.OperationStatus.CONFIRMED,
        c.ObservationRef(uid(), h.binding.binding_id, 1, '2' * 64, '5' * 64), h.preimage, False, ('x',))
    with pytest.raises(CoordinationError):
        h.service.finish(claim, bad)
    assert h.store.read(h.binding.binding_id).unresolved

    # 8. publication failure still strands a fully valid result.
    h.service = CoordinationService(**{k: getattr(h, k) for k in (
        'store', 'facts', 'ledger', 'clock', 'resolve_binding', 'verify_enrollment',
        'insert_enrolled', 'validate_authorization', 'verify_create_intent',
        'termination', 'authorize_human_recovery', 'authorize_heads', 'validate_stage_evidence')})
    # reset to a fresh admitted claim on a clean binding
    h.store.state.rows[:] = [replace(h.store.state.rows[0],
        state=c.CoordinationState.IDLE, unresolved=False, holder=None, attempt_id=None,
        active_operation_id=None, idempotency_key=None, request_digest=None, approval_id=None,
        approval_consumption_published=False, lease_expires_at=None, mutation_stage=None,
        checkpoint=None, admitted_at=None, pre_version_id=None, preimage_digest=None,
        expected_base_fingerprint=None, quarantine_reason=None)]
    h.request = c.RequestIdentity(uid(), 'key-flush', 'a' * 64)
    h.grant = replace(h.grant, request=h.request, approval_id=uid(), approval_digest='7' * 64)
    claim2 = admit(h)
    h.facts.verify_flush.side_effect = lambda _: False
    with pytest.raises(CoordinationError):
        complete(h, claim2)
    assert h.store.read(h.binding.binding_id).unresolved


@pytest.mark.parametrize('terminal', ['failed', 'conflicted'])
def test_terminal_non_success_receipt_is_a_conflict_not_a_completion(h, terminal):
    from backend.services.version_control.coordination import ExistingReceipt
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    status = {'failed': c.OperationStatus.FAILED,
              'conflicted': c.OperationStatus.CONFLICTED}[terminal]
    h.service.finish(claim, c.OperationResult(claim.request.operation_id, status,
        claim.preimage, None, False, ('pre-send-rejected',)))
    row = h.store.read(h.binding.binding_id)
    assert not row.unresolved

    # Retry with a new operation_id but the SAME idempotency key + digest.
    second = replace(h.request, operation_id=uid())
    h.request = second
    with pytest.raises(CoordinationError) as caught:
        reserve(h)
    # 1. not an ExistingReceipt "completion".
    assert not isinstance(caught.value, ExistingReceipt)
    # 2. a new OPERATION/CONFLICTED fact for the SECOND request.
    facts = [c.from_wire(c.OperationFact, r.payload) for r in h.durable.state.rows]
    new = [f for f in facts if f.request.operation_id == second.operation_id
           and f.fact_kind == c.FactKind.OPERATION and f.status == c.FactStatus.CONFLICTED]
    assert len(new) == 1
    # 3. row released.
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.IDLE and not row.unresolved
    assert row.attempt_id is None
    # 4. no quarantine on expiry.
    h.request = c.RequestIdentity(uid(), 'key-later', 'a' * 64)
    fresh = h.service.reserve(h.binding, h.request, h.executor)
    h.clock.advance(h.service._LEASE_TTL + timedelta(seconds=1))
    with pytest.raises(CoordinationError):
        h.service.renew(fresh.fence)
    # the fresh reserve genuinely expired -> quarantined; but the terminal path
    # itself did not brick the binding (proved by 3).


def test_confirmed_and_noop_receipts_still_return_existing_receipt(h):
    from backend.services.version_control.coordination import ExistingReceipt
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    complete(h, claim)  # CONFIRMED with committed postimage
    original_id = claim.request.operation_id
    h.request = replace(h.request, operation_id=uid())
    with pytest.raises(ExistingReceipt) as caught:
        reserve(h)
    assert caught.value.receipt.status == c.FactStatus.CONFIRMED
    assert caught.value.receipt.operation_id == original_id


def test_mixed_history_prefers_confirmed_over_failed_receipt(h):
    from backend.services.version_control.coordination import ExistingReceipt
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    row = h.store.read(h.binding.binding_id)
    # Seed a FAILED receipt for the key alongside the eventual CONFIRMED one.
    h.service._fact(row, claim.request, c.FactStatus.FAILED, kind=c.FactKind.RECEIPT)
    complete(h, claim)  # CONFIRMED receipt
    h.request = replace(h.request, operation_id=uid())
    with pytest.raises(ExistingReceipt) as caught:
        reserve(h)
    assert caught.value.receipt.status == c.FactStatus.CONFIRMED


@pytest.mark.parametrize('cause', ['reviewed-base', 'different-digest', 'revoked-grant',
                                    'uncommitted-preimage', 'consumed-approval'])
def test_definite_presend_rejection_releases_binding_and_publishes_conflict(h, cause):
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    before = h.store.read(h.binding.binding_id)

    if cause == 'reviewed-base':
        h.grant = replace(h.grant, expected_base_fingerprints=replace(
            h.grant.expected_base_fingerprints, config='9' * 64))
    elif cause == 'different-digest':
        # A prior durable fact bound this idempotency key to a different digest.
        other = c.RequestIdentity(uid(), h.request.idempotency_key, 'e' * 64)
        h.service._fact(before, other, c.FactStatus.CONFIRMED)
    elif cause == 'revoked-grant':
        h.validate_authorization.return_value = False
    elif cause == 'uncommitted-preimage':
        h.ledger.verify_committed.return_value = False
    elif cause == 'consumed-approval':
        h.facts.approval_use.side_effect = lambda b, a: c.ApprovalUse(
            b, a, (c.FactRef(uid(), 'k', 'd' * 64),), (uid(),), False)

    with pytest.raises(CoordinationError):
        h.service.admit(reservation, h.preimage, h.grant)

    # 2. row released to IDLE, all attempt identity cleared.
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.IDLE and not row.unresolved
    assert row.attempt_id is None and row.holder is None
    assert row.approval_id is None and row.pre_version_id is None
    # 3. heads / observed_sequence preserved.
    assert row.heads == c.Heads(None, None, None)
    assert row.observed_sequence == before.observed_sequence

    # 4. exactly one durable fact for this key names the rejected request.
    facts = [c.from_wire(c.OperationFact, r.payload) for r in h.durable.state.rows
             if c.from_wire(c.OperationFact, r.payload).binding == h.binding
             and c.from_wire(c.OperationFact, r.payload).request.idempotency_key == h.request.idempotency_key
             and c.from_wire(c.OperationFact, r.payload).request.request_digest == h.request.request_digest]
    assert len(facts) == 1
    f = facts[0]
    assert f.fact_kind == c.FactKind.OPERATION
    assert f.status == c.FactStatus.CONFLICTED
    assert f.attempt_id == reservation.fence.attempt_id
    assert f.request == h.request

    # 5. the rejected fence is dead.
    with pytest.raises(CoordinationError):
        h.service.renew(reservation.fence)
    with pytest.raises(CoordinationError):
        h.service.assert_owner(reservation.fence)

    # 6. binding is not bricked: a fresh reserve succeeds.
    h.request = c.RequestIdentity(uid(), 'key-fresh', 'a' * 64)
    fresh = h.service.reserve(h.binding, h.request, h.executor)
    assert isinstance(fresh, c.Reservation)

    # 7. after expiry there is nothing to quarantine.
    h.service.renew(fresh.fence)  # keep alive
    row = h.store.read(h.binding.binding_id)
    assert row.state != c.CoordinationState.QUARANTINED


def test_drift_conflict_leaves_binding_observable_and_reusable(h):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    # Reviewed-base drift: committed preimage differs from the reviewed base.
    h.grant = replace(h.grant, expected_base_fingerprints=replace(
        h.grant.expected_base_fingerprints, config='9' * 64))
    with pytest.raises(CoordinationError):
        h.service.admit(reservation, h.preimage, h.grant)

    # observe_exclusively succeeds and yields a lease.
    lease = h.service.observe_exclusively(h.binding, h.executor)
    assert lease.observed_sequence == 1

    # advance_heads records the external observation.
    snapshot = FakeCanonicalizer().observe({'serialized_space': {}, 'description': ''})
    version_id = uid()
    context = c.CaptureContext(h.binding, 'observer-1', h.clock.now(), 'open',
        c.ActorContext('observer', h.binding.workspace_id, 'service'), c.Origin.EXTERNAL,
        attempt_id=lease.fence.attempt_id, generation=lease.fence.generation)
    h.ledger.get_version.return_value = c.Version(version_id, snapshot, context)
    heads = h.service.advance_heads(lease.fence, c.HeadUpdate(version_id, None, None, None))
    assert heads == c.Heads(version_id, None, None)

    # A new reserve + admit against the newly captured base succeeds.
    h.request = c.RequestIdentity(uid(), 'key-reapply', 'a' * 64)
    newfp = h.grant.expected_base_fingerprints
    h.preimage = c.ObservationRef(version_id, h.binding.binding_id, 1,
                                  newfp.state_digest, '5' * 64)
    h.grant = replace(h.grant, request=h.request)
    reservation2 = h.service.reserve(h.binding, h.request, h.executor)
    claim = h.service.admit(reservation2, h.preimage, h.grant)
    assert isinstance(claim, c.AdmissionClaim)


def test_conflict_facts_carry_the_rejected_requests_own_identity(h):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    durable_facts(h)
    enroll(h)
    reqs = [c.RequestIdentity(uid(), f'key-{i}', f'{i}' * 64) for i in (1, 2)]
    execs = [replace(h.executor, principal_id=f'principal-{i}',
                     execution_ref=f'job/{i}') for i in (1, 2)]
    barrier = Barrier(2)
    original = h.store.compare_and_swap

    def race(*args, **kwargs):
        barrier.wait(timeout=5)
        return original(*args, **kwargs)

    h.store.compare_and_swap = race

    def run(i):
        try:
            return i, h.service.reserve(h.binding, reqs[i], execs[i])
        except CoordinationError:
            return i, None

    with ThreadPoolExecutor(2) as pool:
        results = dict(pool.map(lambda i: run(i), range(2)))
    h.store.compare_and_swap = original  # race barrier is done; single-threaded now
    winners = [(i, r) for i, r in results.items() if r is not None]
    assert len(winners) == 1
    win_i, winner = winners[0]
    lose_i = 1 - win_i
    winner_executor, loser_executor = execs[win_i], execs[lose_i]
    loser_request = reqs[lose_i]
    row = h.store.read(h.binding.binding_id)
    assert row.attempt_id == winner.fence.attempt_id
    assert row.holder == winner_executor.principal_id

    all_facts = [c.from_wire(c.OperationFact, r.payload) for r in h.durable.state.rows]
    loser_facts = [f for f in all_facts if f.request == loser_request]
    assert len(loser_facts) == 1
    lf = loser_facts[0]
    assert lf.actor.subject_id == loser_executor.principal_id
    assert lf.requester_id == loser_executor.principal_id
    assert lf.attempt_id != winner.fence.attempt_id
    assert lf.approval_id is None and lf.pre_version_id is None
    # No key collision; the loser's key is not ambiguous.
    assert len({f.event_key for f in all_facts}) == len(all_facts)
    assert h.facts.lookup_request(h.binding, loser_request.idempotency_key).ambiguous is False

    # fact_kind participates in the key.
    op_ref = h.service._fact(row, loser_request, c.FactStatus.CONFLICTED,
                             kind=c.FactKind.OPERATION)
    rc_ref = h.service._fact(row, loser_request, c.FactStatus.CONFLICTED,
                             kind=c.FactKind.RECEIPT)
    op_key = next(r.event_key for r in h.durable.state.rows
                  if r.payload['event_id'] == op_ref.event_id)
    rc_key = next(r.event_key for r in h.durable.state.rows
                  if r.payload['event_id'] == rc_ref.event_id)
    assert op_key != rc_key

    # Key is stable across an unrelated CAS (renew bumps row_version).
    reservation = c.Reservation(winner.fence, reqs[win_i], winner_executor, winner.lease_expires_at)
    stable_req = c.RequestIdentity(uid(), 'key-stable', 'a' * 64)
    r1 = h.service._fact(h.store.read(h.binding.binding_id), stable_req, c.FactStatus.CONFLICTED)
    key1 = next(r.event_key for r in h.durable.state.rows if r.payload['event_id'] == r1.event_id)
    h.service.renew(reservation.fence)
    r2 = h.service._fact(h.store.read(h.binding.binding_id), stable_req, c.FactStatus.CONFLICTED)
    key2 = next(r.event_key for r in h.durable.state.rows if r.payload['event_id'] == r2.event_id)
    assert key1 == key2
    # The winner still holds the row.
    assert h.store.read(h.binding.binding_id).unresolved


def test_two_concurrent_reservations_have_exactly_one_winner(h):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    enroll(h)
    barrier = Barrier(2)
    original = h.store.compare_and_swap
    trace = []

    def race(*args, **kwargs):
        barrier.wait(timeout=5)
        trace.append(kwargs)
        return original(*args, **kwargs)

    h.store.compare_and_swap = race

    def run():
        try:
            return reserve(h)
        except CoordinationError:
            return None

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda _: run(), range(2)))
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    row = h.store.read(h.binding.binding_id)
    assert row.attempt_id == winners[0].fence.attempt_id
    assert row.holder == h.executor.principal_id
    assert row.generation == winners[0].fence.generation == 1
    assert row.unresolved and row.state == c.CoordinationState.RESERVED
    assert len(trace) == 2
    h.facts.append.assert_called_once()
    assert h.facts.append.call_args.args[0].status == c.FactStatus.CONFLICTED

    # Successful statement return is NOT proof: a non-committing adapter lies.
    other = replace(h.binding, binding_id=uid())
    h.resolve_binding.return_value = other
    h.store.seed(CoordinationRow(other, h.clock.now()))
    h.store.compare_and_swap = Mock(return_value=True)
    with pytest.raises(CoordinationError):
        h.service.reserve(other, h.request, h.executor)


@pytest.mark.parametrize('change', ['attempt', 'generation', 'revision'])
def test_same_sp_stale_attempt_cannot_renew_or_patch(h, change):
    enroll(h)
    reservation = reserve(h)
    renewed = h.service.renew(reservation.fence)
    assert renewed.row_version > reservation.fence.row_version
    row = h.store.read(h.binding.binding_id)
    changes = {'attempt_id': uid()} if change == 'attempt' else {'generation': row.generation + 1}
    if change == 'revision':
        changes = {'binding': replace(h.binding, binding_revision=2)}
        h.resolve_binding.return_value = changes['binding']
    h.store.state.rows[:] = [replace(row, **changes)]
    assert h.store.state.rows[0].holder == h.executor.principal_id
    with pytest.raises(CoordinationError):
        h.service.renew(renewed)
    with pytest.raises(CoordinationError):
        h.service.assert_owner(renewed)


@pytest.mark.parametrize('kind', ['observation', 'create_intent'])
def test_reservation_is_not_admission_and_requires_committed_preimage(h, kind):
    if kind == 'create_intent':
        h.binding = replace(h.binding, space_id=None)
        h.resolve_binding.return_value = h.binding
        h.grant = replace(h.grant, binding=h.binding)
        h.preimage = c.CreateIntentRef(uid(), h.request.operation_id, h.binding.binding_id,
                                       1, h.request.request_digest)
    enroll(h)
    reservation = reserve(h)
    assert not isinstance(reservation, c.AdmissionClaim)
    with pytest.raises(CoordinationError):
        h.service.assert_owner(reservation.fence)
    verifier = h.ledger.verify_committed if kind == 'observation' else h.verify_create_intent
    verifier.return_value = False
    with pytest.raises(CoordinationError):
        h.service.admit(reservation, h.preimage, h.grant)
    assert h.store.read(h.binding.binding_id).state == c.CoordinationState.RESERVED
    verifier.return_value = True
    claim = h.service.admit(reservation, h.preimage, h.grant)
    assert isinstance(claim, c.AdmissionClaim)
    assert claim.preimage == h.preimage
    h.service.assert_owner(claim)


def test_admission_atomically_binds_key_digest_approval_and_preimage(h):
    enroll(h)
    reservation = reserve(h)
    original = h.store.compare_and_swap
    h.store.compare_and_swap = Mock(wraps=original)
    h.validate_authorization.return_value = False
    with pytest.raises(CoordinationError):
        h.service.admit(reservation, h.preimage, h.grant)
    h.validate_authorization.return_value = True
    for bad in (replace(h.grant, request=replace(h.request, idempotency_key='other')),
                replace(h.grant, expires_at=h.clock.now()),
                replace(h.grant, binding=replace(h.binding, binding_revision=2)),
                replace(h.grant, approval_digest=None)):
        with pytest.raises(CoordinationError):
            h.service.admit(reservation, h.preimage, bad)
    assert h.store.compare_and_swap.call_count == 0
    claim = h.service.admit(reservation, h.preimage, h.grant)
    # The key/digest/approval/preimage binding is exactly ONE CAS (state=ADMITTED).
    # BLOCK-10 adds a distinct post-admission CAS that only records the truthful
    # approval_consumption_published column; it binds no admission fields.
    admit_cas = [ca for ca in h.store.compare_and_swap.call_args_list
                 if ca.kwargs.get('state') == c.CoordinationState.ADMITTED]
    assert len(admit_cas) == 1
    consumption_cas = [ca for ca in h.store.compare_and_swap.call_args_list
                       if set(ca.kwargs) == {'approval_consumption_published'}]
    assert h.store.compare_and_swap.call_count == len(admit_cas) + len(consumption_cas)
    row = h.store.read(h.binding.binding_id)
    assert (row.active_operation_id, row.idempotency_key, row.request_digest) == (
        h.request.operation_id, h.request.idempotency_key, h.request.request_digest)
    assert (row.approval_id, row.approval_digest) == (h.grant.approval_id, h.grant.approval_digest)
    assert (row.pre_version_id, row.preimage_digest, row.expected_base_fingerprint) == (
        h.preimage.version_id, h.preimage.state_digest, h.grant.expected_base_fingerprints.state_digest)
    assert row.generation == claim.generation == reservation.fence.generation
    assert row.attempt_id == claim.attempt_id
    with pytest.raises(CoordinationError):
        h.service.assert_owner(replace(claim, request=replace(h.request, request_digest='a' * 64)))


def durable_facts(h):
    def append(fact):
        h.durable.append(fact.event_key, c.to_wire(fact))
        return c.FactRef(fact.event_id, fact.event_key, 'd' * 64)

    def lookup(binding, key):
        rows = [c.from_wire(c.OperationFact, r.payload) for r in h.durable.state.rows]
        return c.RequestHistory(tuple(f for f in rows if f.binding == binding
                                      and f.request.idempotency_key == key), False)

    h.facts.append.side_effect = append
    h.facts.lookup_request.side_effect = lookup

    def publish(claim):
        evidence = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_PENDING, h.clock.now(),
                                   'd' * 64, claim.preimage if isinstance(claim.preimage, c.ObservationRef) else None)
        fact = c.OperationFact(uid(), c.FactKind.APPROVAL_CONSUMED,
            claim.request.operation_id, claim.row_version,
            f'consumption:{claim.attempt_id}', h.binding, claim.request, 'coordination',
            'target-service', c.ActorContext('target-service', h.binding.workspace_id, 'service'),
            c.FactStatus.CONSUMED, evidence, h.clock.now(), attempt_id=claim.attempt_id,
            generation=claim.generation, approval_id=claim.approval_id,
            approval_digest=claim.approval_digest)
        existing = h.durable.lookup(fact.event_key)
        if existing:
            old = c.from_wire(c.OperationFact, existing[0].payload)
            return c.FactRef(old.event_id, old.event_key, 'd' * 64)
        return append(fact)

    def use(binding, approval_id):
        rows = [c.from_wire(c.OperationFact, r.payload) for r in h.durable.state.rows]
        used = [f for f in rows if f.binding == binding and f.approval_id == approval_id
                and f.fact_kind == c.FactKind.APPROVAL_CONSUMED]
        return c.ApprovalUse(binding, approval_id,
            tuple(c.FactRef(f.event_id, f.event_key, 'd' * 64) for f in used),
            tuple(f.operation_id for f in used), False)

    h.facts.publish_consumption.side_effect = publish
    h.facts.approval_use.side_effect = use
    h.facts.verify_flush.side_effect = lambda claim: (claim.approval_id is None or
        claim.request.operation_id in use(h.binding, claim.approval_id).operation_ids)


def complete(h, claim):
    result = c.OperationResult(claim.request.operation_id, c.OperationStatus.CONFIRMED,
                               claim.preimage, h.preimage, False, ('verified',))
    h.service.finish(claim, result)
    return result


def test_same_key_different_digest_rejected_after_row_reuse(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    complete(h, claim)
    assert not h.store.read(h.binding.binding_id).unresolved
    # Reconstruct the service: all remembered authority must be in the stores.
    h.service = CoordinationService(**{k: getattr(h, k) for k in (
        'store', 'facts', 'ledger', 'clock', 'resolve_binding', 'verify_enrollment',
        'insert_enrolled', 'validate_authorization', 'verify_create_intent',
        'termination', 'authorize_human_recovery', 'authorize_heads')})
    h.request = replace(h.request, operation_id=uid(), request_digest='a' * 64)
    with pytest.raises(CoordinationError, match='digest'):
        reserve(h)
    assert h.store.read(h.binding.binding_id).state != c.CoordinationState.ADMITTED


def test_duplicate_completed_request_returns_receipt_without_admission(h):
    from backend.services.version_control.coordination import ExistingReceipt
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    complete(h, claim)
    original_id = h.request.operation_id
    h.request = replace(h.request, operation_id=uid())
    before = h.store.read(h.binding.binding_id).generation
    with pytest.raises(ExistingReceipt) as caught:
        reserve(h)
    assert caught.value.receipt.operation_id == original_id
    assert caught.value.receipt.status == c.FactStatus.CONFIRMED
    row = h.store.read(h.binding.binding_id)
    assert row.generation == before + 1
    assert not row.unresolved and row.state == c.CoordinationState.IDLE


@pytest.mark.parametrize('crash', [None, 'before-commit', 'after-commit-before-response'])
def test_consumed_approval_survives_row_reuse_and_crash(h, crash):
    from backend.tests.vc_fakes.stores import CrashBoundary
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    if crash:
        h.durable.failures.inject(CrashBoundary(crash))
        with pytest.raises(CoordinationError):
            h.service.admit(reservation, h.preimage, h.grant)
        row = h.store.read(h.binding.binding_id)
        assert row.unresolved and row.approval_id == h.grant.approval_id
        assert row.state == c.CoordinationState.QUARANTINED
        with pytest.raises(CoordinationError):
            reserve(h)
    else:
        claim = h.service.admit(reservation, h.preimage, h.grant)
        h.facts.publish_consumption.assert_called_once_with(claim)
        complete(h, claim)
        h.request = c.RequestIdentity(uid(), 'key-2', 'a' * 64)
        h.grant = replace(h.grant, request=h.request)
        with pytest.raises(CoordinationError, match='approval'):
            admit(h)
        assert h.facts.approval_use(h.binding, h.grant.approval_id).operation_ids == (claim.request.operation_id,)


@pytest.mark.parametrize('point', ['evidence', 'admission', 'consumption', 'intent', 'receipt', 'release'])
@pytest.mark.parametrize('boundary', ['before-commit', 'after-commit-before-response', 'during-follow-up-read'])
def test_crashes_between_evidence_cas_consumption_patch_and_release_remain_safe(h, point, boundary):
    from backend.tests.vc_fakes.stores import CrashBoundary
    durable_facts(h)
    enroll(h)
    reservation = reserve(h)
    if point == 'evidence':
        h.ledger.verify_committed.side_effect = OSError('Delta evidence unavailable')
        action = lambda: h.service.admit(reservation, h.preimage, h.grant)
    elif point in {'admission', 'consumption'}:
        failures = h.store.failures if point == 'admission' else h.durable.failures
        failures.inject(CrashBoundary(boundary))
        action = lambda: h.service.admit(reservation, h.preimage, h.grant)
    else:
        claim = h.service.admit(reservation, h.preimage, h.grant)
        if point == 'intent':
            h.store.failures.inject(CrashBoundary(boundary))
            action = lambda: h.service.checkpoint(claim, c.PatchStage.CONFIG_IN_FLIGHT,
                c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(), 'a' * 64, None))
        else:
            if point == 'receipt':
                if boundary == 'during-follow-up-read':
                    h.facts.lookup_request.side_effect = OSError('receipt read-back unavailable')
                else:
                    # Consumption already durable; inject only the next append (receipt).
                    h.durable.failures.inject(CrashBoundary(boundary))
            else:
                h.store.failures.inject(CrashBoundary(boundary))
            action = lambda: complete(h, claim)
    with pytest.raises(CoordinationError):
        action()
    row = h.store.read(h.binding.binding_id)
    if row.unresolved:
        assert row.attempt_id == reservation.fence.attempt_id
        with pytest.raises(CoordinationError):
            reserve(h)
    else:
        assert point == 'release'
        # The terminal closure marker (receipt-equivalent) is a coordination OPERATION
        # fact at _SEQ_CLOSURE; FactKind.RECEIPT is reserved for the DeploymentReceipt.
        assert any(r.payload['fact_kind'] == 'operation'
                   and r.payload.get('operation_type') == 'coordination'
                   and r.payload['transition_sequence'] == _SEQ_CLOSURE
                   for r in h.durable.state.rows)


def test_consumption_and_termination_columns_are_truthful(h):
    from backend.tests.vc_fakes.stores import CrashBoundary
    durable_facts(h)
    enroll(h)
    # 1. approval-bearing admit: column True and fact table agrees.
    claim = admit(h)
    row = h.store.read(h.binding.binding_id)
    assert row.approval_consumption_published is True
    assert h.facts.approval_use(h.binding, h.grant.approval_id).operation_ids == (
        claim.request.operation_id,)

    # 5. _release resets it.
    complete(h, claim)
    row = h.store.read(h.binding.binding_id)
    assert row.approval_consumption_published is False and row.approval_id is None

    # 2. approval-less case: never claims a consumption.
    h.request = c.RequestIdentity(uid(), 'key-noapproval', 'a' * 64)
    h.grant = replace(h.grant, request=h.request, approval_id=None, approval_digest=None)
    reservation = reserve(h)
    claim2 = h.service.admit(reservation, h.preimage, h.grant)
    assert h.store.read(h.binding.binding_id).approval_consumption_published is False
    send = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(), 'a' * 64, None)
    # 4. checkpoint does not flip the column on its own.
    original = h.store.compare_and_swap
    seen = []
    h.store.compare_and_swap = lambda *a, **k: (seen.append(k), original(*a, **k))[1]
    h.service.checkpoint(claim2, send.stage, send)
    h.store.compare_and_swap = original
    assert h.store.read(h.binding.binding_id).approval_consumption_published is False
    assert not any(k.get('approval_consumption_published') is True for k in seen)
    # Release claim2 so the binding is free for case 3.
    h.service.quarantine(claim2, 'test teardown')
    h.store.state.rows[:] = [replace(h.store.state.rows[0],
        state=c.CoordinationState.IDLE, unresolved=False, holder=None, attempt_id=None,
        approval_id=None, approval_consumption_published=False, mutation_stage=None,
        checkpoint=None, active_operation_id=None, idempotency_key=None, request_digest=None,
        lease_expires_at=None, admitted_at=None, pre_version_id=None, preimage_digest=None)]

    # 3. crash-before-publish is distinguishable: (approval bound, not published).
    h.request = c.RequestIdentity(uid(), 'key-crash', 'b' * 64)
    h.grant = replace(h.grant, request=h.request, approval_id=uid(), approval_digest='7' * 64)
    reservation = reserve(h)
    h.durable.failures.inject(CrashBoundary('before-commit'))
    with pytest.raises(CoordinationError):
        h.service.admit(reservation, h.preimage, h.grant)
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.QUARANTINED
    assert row.approval_consumption_published is False
    assert row.approval_id == h.grant.approval_id


def test_checkpoint_is_monotonic_and_matching_get_cannot_finish_in_flight(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    send = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(), 'a' * 64, None)
    h.service.checkpoint(claim, send.stage, send)
    row = h.store.read(h.binding.binding_id)
    assert row.checkpoint['resume_classification'] == 'read-only-possible-send'
    with pytest.raises(CoordinationError):
        h.service.checkpoint(claim, send.stage, send)
    with pytest.raises(CoordinationError):
        complete(h, claim)
    assert h.store.read(h.binding.binding_id).state == c.CoordinationState.QUARANTINED


def test_checkpoint_observation_requires_trusted_send_completion(h):
    enroll(h)
    claim = admit(h)
    send = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(), 'a' * 64, None)
    h.service.checkpoint(claim, send.stage, send)
    observed = c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_OBSERVED, h.clock.now(), 'b' * 64, h.preimage)
    h.validate_stage_evidence.return_value = False  # GET equality alone
    with pytest.raises(CoordinationError):
        h.service.checkpoint(claim, observed.stage, observed)
    with pytest.raises(CoordinationError):
        h.service.checkpoint(claim, c.PatchStage.DESCRIPTION_PENDING,
            replace(observed, stage=c.PatchStage.DESCRIPTION_PENDING))


def test_checkpoint_normal_sequence_and_flush_failure_retains_claim(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    for stage in list(c.PatchStage)[1:]:
        observation = h.preimage if stage in {c.PatchStage.CONFIG_OBSERVED, c.PatchStage.DESCRIPTION_OBSERVED} else None
        h.service.checkpoint(claim, stage,
            c.StageEvidence('VC/1.0', stage, h.clock.now(), 'a' * 64, observation))
    h.facts.verify_flush.side_effect = lambda _: False
    with pytest.raises(CoordinationError):
        complete(h, claim)
    assert h.store.read(h.binding.binding_id).unresolved


def test_observer_cannot_regress_or_approve_heads(h):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer
    enroll(h)
    lease = h.service.observe_exclusively(h.binding, h.executor)
    with pytest.raises(CoordinationError):
        reserve(h)
    with pytest.raises(CoordinationError):
        h.service.assert_owner(lease.fence)
    version_id = uid()
    for update in (c.HeadUpdate(version_id, version_id, None, 'caller-claim'),
                   c.HeadUpdate(version_id, None, version_id, 'caller-claim')):
        with pytest.raises(CoordinationError):
            h.service.advance_heads(lease.fence, update)
    snapshot = FakeCanonicalizer().observe({'serialized_space': {}, 'description': ''})
    context = c.CaptureContext(h.binding, 'observer-1', h.clock.now(), 'open',
        c.ActorContext('observer', h.binding.workspace_id, 'service'), c.Origin.EXTERNAL,
        attempt_id=lease.fence.attempt_id, generation=lease.fence.generation)
    h.ledger.get_version.return_value = c.Version(version_id, snapshot, context)
    heads = h.service.advance_heads(lease.fence, c.HeadUpdate(version_id, None, None, None))
    assert heads == c.Heads(version_id, None, None)
    assert not h.store.read(h.binding.binding_id).unresolved
    newer = h.service.observe_exclusively(h.binding, h.executor)
    assert newer.observed_sequence > lease.observed_sequence
    with pytest.raises(CoordinationError):
        h.service.advance_heads(lease.fence, c.HeadUpdate(uid(), None, None, None))
    # Even the current holder cannot attach an old observation from another lease.
    with pytest.raises(CoordinationError):
        h.service.advance_heads(newer.fence, c.HeadUpdate(version_id, None, None, None))
    final_id = uid()
    h.ledger.get_version.return_value = c.Version(final_id, snapshot,
        replace(context, observation_key='observer-2', parent_version_id=version_id,
                attempt_id=newer.fence.attempt_id, generation=newer.fence.generation))
    h.service.advance_heads(newer.fence, c.HeadUpdate(final_id, None, None, None))
    claim = admit(h)
    with pytest.raises(CoordinationError):
        h.service.advance_heads(claim, c.HeadUpdate(None, final_id, final_id, 'unvalidated'))
    h.authorize_heads.return_value = True
    heads = h.service.advance_heads(claim, c.HeadUpdate(None, final_id, final_id, 'verified-deployment-policy'))
    assert heads == c.Heads(final_id, final_id, final_id)
