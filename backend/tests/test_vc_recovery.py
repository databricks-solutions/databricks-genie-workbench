"""Recovery never infers executor termination from a matching Genie GET."""
from dataclasses import replace
from datetime import timedelta

import pytest

from backend.services.version_control import contracts as c
from backend.services.version_control.coordination import CoordinationError
from backend.services.version_control.coordination.service import _SEQ_CLOSURE
from backend.tests.test_vc_coordination import h, enroll, reserve, admit, durable_facts, uid


def _closures(facts):
    """Coordination-authored terminal closure markers (the receipt-equivalent that
    replaced FactKind.RECEIPT: OPERATION facts at _SEQ_CLOSURE from finish/recover)."""
    return [f for f in facts if f.fact_kind == c.FactKind.OPERATION
            and f.operation_type == 'coordination' and f.transition_sequence == _SEQ_CLOSURE]


@pytest.mark.parametrize('entry', ['reserve', 'renew', 'assert_owner'])
def test_expired_lease_quarantines_without_takeover(h, entry):
    enroll(h)
    claim = admit(h)
    before = h.store.read(h.binding.binding_id)
    h.clock.advance(h.service._LEASE_TTL + timedelta(seconds=1))
    with pytest.raises(CoordinationError):
        if entry == 'reserve':
            reserve(h)
        else:
            getattr(h.service, entry)(claim)
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.QUARANTINED
    assert row.unresolved
    assert (row.attempt_id, row.generation, row.approval_id, row.pre_version_id) == (
        before.attempt_id, before.generation, before.approval_id, before.pre_version_id)
    assert any(call.args[0].status == c.FactStatus.QUARANTINED for call in h.facts.append.call_args_list)
    with pytest.raises(CoordinationError):
        reserve(h)


@pytest.mark.parametrize('entry', ['reserve', 'observe_exclusively', 'advance_heads'])
def test_expired_observation_lease_is_reclaimed_and_never_quarantines_binding(h, entry):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer
    enroll(h)
    lease = h.service.observe_exclusively(h.binding, h.executor)
    h.clock.advance(h.service._LEASE_TTL + timedelta(seconds=1))
    if entry == 'reserve':
        h.service.reserve(h.binding, c.RequestIdentity(uid(), 'k', 'a' * 64), h.executor)
        # reserve succeeded on the reclaimed binding; re-read after.
    elif entry == 'observe_exclusively':
        h.service.observe_exclusively(h.binding, h.executor)
    else:  # advance_heads on the stale fence triggers _owned -> _expire
        with pytest.raises(CoordinationError):
            h.service.advance_heads(lease.fence, c.HeadUpdate(uid(), None, None, None))
    row = h.store.read(h.binding.binding_id)
    # The reclaim never quarantines; the entry point may have re-acquired the row.
    assert row.quarantine_reason is None
    assert row.state != c.CoordinationState.QUARANTINED
    # 6. no QUARANTINED fact appended.
    assert not any(call.args[0].status == c.FactStatus.QUARANTINED
                   for call in h.facts.append.call_args_list)
    # 5. the stale lease is dead.
    with pytest.raises(CoordinationError):
        h.service.assert_owner(lease.fence)


def test_expired_observation_lease_full_reclaim_semantics(h):
    enroll(h)
    lease = h.service.observe_exclusively(h.binding, h.executor)
    h.clock.advance(h.service._LEASE_TTL + timedelta(seconds=1))
    # Trigger reclaim via reserve, then inspect.
    fresh = h.service.reserve(h.binding, c.RequestIdentity(uid(), 'k', 'a' * 64), h.executor)
    row = h.store.read(h.binding.binding_id)
    # observed_sequence preserved through the reclaim (was 1 from the lease).
    assert row.observed_sequence == 1
    assert row.heads == c.Heads(None, None, None)
    # write admission survived; a reservation was granted.
    assert isinstance(fresh, c.Reservation)
    # a fresh observation lease bumps the sequence beyond the old one.
    h.service.quarantine(fresh.fence, 'test teardown')
    h.store.state.rows[:] = [replace(h.store.state.rows[0],
        state=c.CoordinationState.IDLE, unresolved=False, holder=None, attempt_id=None,
        active_operation_id=None, idempotency_key=None, request_digest=None,
        lease_expires_at=None, mutation_stage=None, checkpoint=None)]
    newer = h.service.observe_exclusively(h.binding, h.executor)
    assert newer.observed_sequence > lease.observed_sequence


def test_reclaim_refuses_observing_row_that_carries_mutation_authority(h):
    enroll(h)
    lease = h.service.observe_exclusively(h.binding, h.executor)
    row = h.store.read(h.binding.binding_id)
    # Corrupt the row: OBSERVING but carrying an approval (mutation authority).
    h.store.state.rows[:] = [replace(row, approval_id=uid())]
    h.clock.advance(h.service._LEASE_TTL + timedelta(seconds=1))
    with pytest.raises(CoordinationError):
        h.service.reserve(h.binding, c.RequestIdentity(uid(), 'k', 'a' * 64), h.executor)
    after = h.store.read(h.binding.binding_id)
    assert after.state == c.CoordinationState.OBSERVING and after.approval_id is not None


def test_expired_admitted_lease_still_quarantines(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.clock.advance(h.service._LEASE_TTL + timedelta(seconds=1))
    with pytest.raises(CoordinationError):
        reserve(h)
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.QUARANTINED
    assert any(call.args[0].status == c.FactStatus.QUARANTINED
               for call in h.facts.append.call_args_list)


def recovery_evidence(h, claim, classification='read-only-presend'):
    # BLOCK-11: recovery classification must equal the derived durable checkpoint
    # classification. An admit+quarantine (no checkpoint) path derives 'read-only-presend'.
    terminal = h.clock.now()
    termination = c.TerminationEvidence(claim.attempt_id, h.executor.execution_ref,
                                        'app-worker', terminal, (), 'a' * 64)
    samples = tuple(c.RecoverySample(terminal + timedelta(seconds=i),
                                     h.preimage.state_digest, 'b' * 64) for i in (1, 12, 23))
    h.clock.advance(timedelta(seconds=30))
    return c.RecoveryEvidence('VC/1.0', claim, termination, samples, 20.0,
                              'verified-lifetime-reference', classification, None, None, False)


def test_matching_get_after_timeout_cannot_resolve_attempt(h):
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'PATCH timeout; matching GET is observation only')
    evidence = recovery_evidence(h, claim)
    with pytest.raises(CoordinationError, match='termination'):
        h.service.recover(h.binding, evidence)
    with pytest.raises(CoordinationError):
        h.service.assert_owner(claim)
    with pytest.raises(CoordinationError):
        h.service.finish(claim, c.OperationResult(claim.request.operation_id,
            c.OperationStatus.CONFIRMED, claim.preimage, h.preimage, False, ('GET-matched',)))
    assert h.store.read(h.binding.binding_id).unresolved


@pytest.mark.parametrize('fault', [None, 'job-ok', 'wrong-attempt', 'missing-child',
    'no-worker-proof', 'two-reads', 'pre-termination', 'unstable', 'short-span', 'future'])
def test_recovery_needs_exact_attempt_termination_and_three_post_termination_reads(h, fault):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'unknown send outcome')
    evidence = recovery_evidence(h, claim)
    # Test-only verified capability. Production defaults to unavailable.
    h.service.verified_request_lifetime = (20.0, 'verified-lifetime-reference')
    trusted = evidence.termination
    if fault in {'job-ok', 'missing-child'}:
        executions = tuple(c.ExecutionTerminalEvidence(ref, trusted.terminated_at, 'c' * 64)
            for ref in (h.executor.execution_ref, 'job/retry', 'job/child'))
        trusted = replace(trusted, source='job', executions=executions, app_worker_proof_digest=None)
        evidence = replace(evidence, termination=trusted)
    h.termination.for_attempt.return_value = trusted
    if fault == 'wrong-attempt':
        evidence = replace(evidence, termination=replace(trusted, attempt_id=uid()))
    elif fault == 'missing-child':
        evidence = replace(evidence, termination=replace(trusted, executions=trusted.executions[:-1]))
    elif fault == 'no-worker-proof':
        evidence = replace(evidence, termination=replace(trusted, app_worker_proof_digest=None))
        h.termination.for_attempt.return_value = evidence.termination
    elif fault == 'two-reads':
        evidence = replace(evidence, samples=evidence.samples[:2])
    elif fault == 'pre-termination':
        evidence = replace(evidence, samples=(replace(evidence.samples[0], observed_at=trusted.terminated_at), *evidence.samples[1:]))
    elif fault == 'unstable':
        evidence = replace(evidence, samples=(*evidence.samples[:2], replace(evidence.samples[2], state_digest='c' * 64)))
    elif fault == 'short-span':
        evidence = replace(evidence, samples=(*evidence.samples[:2], replace(evidence.samples[2], observed_at=evidence.samples[0].observed_at + timedelta(seconds=20))))
    elif fault == 'future':
        evidence = replace(evidence, samples=(*evidence.samples[:2], replace(evidence.samples[2], observed_at=h.clock.now() + timedelta(seconds=1))))
    if fault in {None, 'job-ok'}:
        result = h.service.recover(h.binding, evidence)
        assert not result.unresolved
        assert result.fence.generation == claim.generation + 1
        assert h.store.read(h.binding.binding_id).state == c.CoordinationState.IDLE
        with pytest.raises(CoordinationError):
            h.service.assert_owner(claim)
        assert any(r.payload['fact_kind'] == 'recovery' for r in h.durable.state.rows)
    else:
        with pytest.raises(CoordinationError):
            h.service.recover(h.binding, evidence)
        assert h.store.read(h.binding.binding_id).unresolved


@pytest.mark.parametrize('stage', ['never-admitted', 'admitted-presend',
                                    'config-in-flight', 'config-observed'])
def test_recovery_classification_is_derived_from_durable_state(h, stage):
    durable_facts(h)
    enroll(h)
    expected = {
        'never-admitted': 'read-only-never-admitted',
        'admitted-presend': 'read-only-presend',
        'config-in-flight': 'read-only-possible-send',
        'config-observed': 'read-only-unless-next-stage-proven-unsent',
    }[stage]
    if stage == 'never-admitted':
        reservation = reserve(h)
        fence = reservation.fence
        h.service.quarantine(reservation.fence, 'worker lost')
    else:
        claim = admit(h)
        if stage in {'config-in-flight', 'config-observed'}:
            h.service.checkpoint(claim, c.PatchStage.CONFIG_IN_FLIGHT,
                c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_IN_FLIGHT, h.clock.now(), 'a' * 64, None))
        if stage == 'config-observed':
            h.service.checkpoint(claim, c.PatchStage.CONFIG_OBSERVED,
                c.StageEvidence('VC/1.0', c.PatchStage.CONFIG_OBSERVED, h.clock.now(), 'a' * 64, h.preimage))
        fence = claim
        h.service.quarantine(claim, 'worker lost')

    h.service.verified_request_lifetime = (20.0, 'verified-lifetime-reference')

    def evidence_with(classification):
        ev = recovery_evidence(h, fence, classification)
        h.termination.for_attempt.return_value = ev.termination
        return ev

    # 1/2. a wrong (mismatched) classification is rejected; row stays quarantined.
    wrong = 'read-only-possible-send' if expected != 'read-only-possible-send' \
        else 'read-only-presend'
    with pytest.raises(CoordinationError, match='classification'):
        h.service.recover(h.binding, evidence_with(wrong))
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.QUARANTINED and row.unresolved
    assert not any(r.payload['fact_kind'] == 'recovery' for r in h.durable.state.rows)

    # 4/5. the diff's free-form value and blanks are rejected.
    for bad in ('observed-preimage', '', '   '):
        with pytest.raises(CoordinationError):
            h.service.recover(h.binding, evidence_with(bad))

    # 3/6. the correct derived value (even with whitespace) succeeds and the fact
    # records the DERIVED string.
    result = h.service.recover(h.binding, evidence_with('  ' + expected + '  '))
    assert not result.unresolved
    recov = [c.from_wire(c.OperationFact, r.payload) for r in h.durable.state.rows
             if r.payload['fact_kind'] == 'recovery']
    assert recov and recov[-1].evidence.checkpoint_classification == expected


@pytest.mark.parametrize('path', ['automatic', 'human'])
def test_recovery_closes_the_operation_and_forbids_a_second_admission(h, path):
    from backend.services.version_control.coordination import ExistingReceipt
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'unknown send outcome')
    evidence = recovery_evidence(h, claim)
    h.termination.for_attempt.return_value = evidence.termination
    if path == 'automatic':
        h.service.verified_request_lifetime = (20.0, 'verified-lifetime-reference')
    else:
        evidence = replace(evidence, maximum_request_lifetime_seconds=None,
            lifetime_bound_reference=None, human_authorization_reference='audited-operator/123',
            human_reason='Orphan inspection complete; reconcile under new governed request',
            residual_risk_acknowledged=True)
        h.authorize_human_recovery.return_value = True

    result = h.service.recover(h.binding, evidence)
    # 1. baseline.
    assert result.status == c.OperationStatus.CONFLICTED and not result.unresolved

    # 2/3. both a RECOVERY audit and a terminal closure marker exist for the key, CONFLICTED.
    hist = h.facts.lookup_request(h.binding, claim.request.idempotency_key)
    assert not hist.ambiguous
    recov = [f for f in hist.facts if f.fact_kind == c.FactKind.RECOVERY]
    receipts = _closures(hist.facts)
    assert len(recov) == 1 and len(receipts) == 1
    for f in (recov[0], receipts[0]):
        assert f.status == c.FactStatus.CONFLICTED
        assert f.attempt_id == claim.attempt_id
        assert f.post_version_id is None
    assert recov[0].event_key != receipts[0].event_key

    # 4/5. the operation cannot be admitted again; reserve must not return a
    # Reservation, and after BLOCK-8 a CONFLICTED receipt is a conflict (not a
    # completion), so reserve raises CoordinationError and releases the row.
    second = replace(h.request, operation_id=uid())
    with pytest.raises(CoordinationError) as caught:
        h.service.reserve(h.binding, second, h.executor)
    assert not isinstance(caught.value, ExistingReceipt)
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.IDLE and not row.unresolved

    # 6. genuinely new work (different idempotency key) still admits.
    h.request = c.RequestIdentity(uid(), 'key-new', 'a' * 64)
    h.grant = replace(h.grant, request=h.request, approval_id=uid(), approval_digest='7' * 64)
    fresh_claim = admit(h)
    assert isinstance(fresh_claim, c.AdmissionClaim)


def test_recovery_publication_failure_keeps_row_quarantined(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'unknown send outcome')
    evidence = recovery_evidence(h, claim)
    h.termination.for_attempt.return_value = evidence.termination
    h.service.verified_request_lifetime = (20.0, 'verified-lifetime-reference')
    # 7. read-back after the appends fails -> stays QUARANTINED.
    real_append = h.facts.append.side_effect
    count = {'receipt_seen': False}

    def append_then_break(fact):
        ref = real_append(fact)
        if (fact.fact_kind == c.FactKind.OPERATION and fact.operation_type == 'coordination'
                and fact.transition_sequence == _SEQ_CLOSURE):
            h.facts.lookup_request.side_effect = OSError('receipt read-back unavailable')
        return ref

    h.facts.append.side_effect = append_then_break
    with pytest.raises(CoordinationError):
        h.service.recover(h.binding, evidence)
    assert h.store.read(h.binding.binding_id).state == c.CoordinationState.QUARANTINED


def test_recovery_persists_validated_termination_evidence_before_releasing(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'unknown send outcome')
    evidence = recovery_evidence(h, claim)
    h.service.verified_request_lifetime = (20.0, 'verified-lifetime-reference')
    h.termination.for_attempt.return_value = evidence.termination

    # 6. evidence is durable at the moment the recovery fact is appended.
    seen = {}
    real_append = h.facts.append.side_effect

    def capture(fact):
        if fact.fact_kind == c.FactKind.RECOVERY:
            seen['at_append'] = h.store.read(h.binding.binding_id).termination_evidence
        return real_append(fact)

    h.facts.append.side_effect = capture
    result = h.service.recover(h.binding, evidence)
    assert not result.unresolved
    assert c.to_wire(seen['at_append']) == c.to_wire(evidence.termination)
    # 9. released row clears the column; the durable RECOVERY fact persists.
    assert h.store.read(h.binding.binding_id).termination_evidence is None
    assert any(r.payload['fact_kind'] == 'recovery' for r in h.durable.state.rows)


def test_recovery_does_not_persist_invalid_termination(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'unknown send outcome')
    evidence = recovery_evidence(h, claim)
    h.service.verified_request_lifetime = (20.0, 'verified-lifetime-reference')
    h.termination.for_attempt.return_value = evidence.termination
    # 7. invalid termination never reaches the column.
    bad = replace(evidence, termination=replace(evidence.termination, attempt_id=uid()))
    with pytest.raises(CoordinationError):
        h.service.recover(h.binding, bad)
    assert h.store.read(h.binding.binding_id).termination_evidence is None


def test_recovery_crash_between_audit_and_release_keeps_termination_durable(h):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'unknown send outcome')
    evidence = recovery_evidence(h, claim)
    h.service.verified_request_lifetime = (20.0, 'verified-lifetime-reference')
    h.termination.for_attempt.return_value = evidence.termination
    # 8. crash between recovery append and release leaves evidence durable.
    real_append = h.facts.append.side_effect

    def append_then_break(fact):
        ref = real_append(fact)
        if fact.fact_kind == c.FactKind.RECOVERY:
            h.facts.lookup_request.side_effect = OSError('audit read-back unavailable')
        return ref

    h.facts.append.side_effect = append_then_break
    with pytest.raises(CoordinationError):
        h.service.recover(h.binding, evidence)
    row = h.store.read(h.binding.binding_id)
    assert row.state == c.CoordinationState.QUARANTINED
    assert c.to_wire(row.termination_evidence) == c.to_wire(evidence.termination)


@pytest.mark.parametrize('missing', [None, 'authorization', 'reason', 'risk', 'trusted-policy'])
def test_unknown_request_lifetime_disables_auto_clear(h, missing):
    durable_facts(h)
    enroll(h)
    claim = admit(h)
    h.service.quarantine(claim, 'worker disconnected')
    evidence = recovery_evidence(h, claim)
    h.termination.for_attempt.return_value = evidence.termination
    # Caller-supplied lifetime/reference cannot turn on automatic recovery.
    with pytest.raises(CoordinationError, match='unverified'):
        h.service.recover(h.binding, evidence)
    evidence = replace(evidence, maximum_request_lifetime_seconds=None,
        lifetime_bound_reference=None, human_authorization_reference='audited-operator/123',
        human_reason='Orphan inspection complete; reconcile under new governed request',
        residual_risk_acknowledged=True)
    h.authorize_human_recovery.return_value = True
    if missing == 'authorization':
        evidence = replace(evidence, human_authorization_reference=None)
    elif missing == 'reason':
        evidence = replace(evidence, human_reason='')
    elif missing == 'risk':
        evidence = replace(evidence, residual_risk_acknowledged=False)
    elif missing == 'trusted-policy':
        h.authorize_human_recovery.return_value = False
    if missing is None:
        result = h.service.recover(h.binding, evidence)
        assert not result.unresolved
        audits = [r for r in h.durable.state.rows if r.payload['fact_kind'] == 'recovery']
        assert audits[-1].payload['evidence']['residual_risk_acknowledged'] is True
        assert audits[-1].payload['evidence']['human_authorization_reference'] == 'audited-operator/123'
    else:
        with pytest.raises(CoordinationError):
            h.service.recover(h.binding, evidence)
        assert h.store.read(h.binding.binding_id).unresolved
