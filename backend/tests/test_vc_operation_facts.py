from dataclasses import replace
from datetime import datetime, timezone
from uuid import UUID, uuid5, NAMESPACE_URL

import pytest

from backend.services.version_control.contracts import (
    CreateIntentRef, FactKind, FactStatus, MutationRequest,
    OperationFact, PatchStage, RequestIdentity, StageEvidence,
    to_wire,
)
from backend.tests.vc_fakes.fixtures import actor_fixture, binding_fixture
from backend.tests.vc_fakes.stores import (
    CrashBoundary, FakeFactStore, InjectedCrash, ManualClock,
)


NOW = datetime(2026, 9, 7, tzinfo=timezone.utc)
DIGEST = 'a' * 64


def uid(number):
    return str(UUID(int=number))


def fact(**changes):
    from backend.services.version_control.governance.facts import fact_key

    values = dict(
        event_id=uid(1), fact_kind=FactKind.OPERATION, operation_id=uid(2),
        transition_sequence=0, event_key='', binding=binding_fixture(),
        request=RequestIdentity(uid(2), 'client-key', DIGEST), operation_type='edit',
        requester_id=actor_fixture().subject_id, actor=actor_fixture(),
        status=FactStatus.REQUESTED,
        evidence=StageEvidence('VC/1.0', PatchStage.CONFIG_PENDING, NOW, DIGEST, None),
        recorded_at=NOW,
    )
    values.update(changes)
    provisional = OperationFact(**values)
    key = fact_key(provisional)
    return replace(provisional, event_key=key, event_id=str(uuid5(NAMESPACE_URL, key)))


def durable(store=None, enabled=True):
    from backend.services.version_control.governance.facts import DurableOperationFacts

    store = store or FakeFactStore(ManualClock(NOW))

    def read():
        store.failures.hit(CrashBoundary.DURING_FOLLOW_UP_READ)
        return tuple(row.payload for row in store.state.rows)

    return DurableOperationFacts(read, store.append, writes_enabled=enabled), store


def test_operation_fact_schema_and_idempotent_append_survive_response_loss():
    ledger, store = durable()
    original = fact()
    store.failures.inject(CrashBoundary.AFTER_COMMIT_BEFORE_RESPONSE)
    with pytest.raises(InjectedCrash):
        ledger.append(original)
    restarted, _ = durable(store)
    reference = restarted.append(original)
    assert reference.event_key == original.event_key
    assert len(store.state.rows) == 1
    with pytest.raises(ValueError, match='event key'):
        restarted.append(replace(original, event_key='caller-controlled'))
    with pytest.raises(ValueError, match='identity'):
        restarted.append(fact(operation_id=uid(99)))
    with pytest.raises(ValueError, match='conflict'):
        restarted.append(replace(original, status=FactStatus.CONFIRMED))
    store.failures.inject(CrashBoundary.DURING_FOLLOW_UP_READ)
    with pytest.raises(InjectedCrash):
        restarted.append(original)
    assert len(store.state.rows) == 1


def test_fact_writes_default_off():
    from backend.services.version_control.governance.facts import DurableOperationFacts

    ledger = DurableOperationFacts(lambda: (), lambda *args: pytest.fail('write'))
    with pytest.raises(PermissionError, match='disabled'):
        ledger.append(fact())


def test_completed_request_and_consumed_approval_remain_after_coordination_reuse():
    ledger, store = durable()
    initial = fact()
    request = MutationRequest(initial.request, initial.binding, 'edit', uid(3),
                              DIGEST, {'instructions': []}, None, uid(4))
    ledger.record_request(request, initial)
    consumed = fact(fact_kind=FactKind.APPROVAL_CONSUMED, status=FactStatus.CONSUMED,
                    approval_id=uid(4), approval_digest=DIGEST,
                    attempt_id=uid(5), generation=1)
    ledger.append(consumed)
    ledger.append(fact(transition_sequence=1, status=FactStatus.CONFIRMED))
    ledger.append(fact(operation_id=uid(6), request=RequestIdentity(uid(6), 'later', DIGEST)))
    restarted, _ = durable(store)
    history = restarted.lookup_request(initial.binding, 'client-key')
    assert not history.ambiguous
    assert any(row.status == FactStatus.CONFIRMED for row in history.facts)
    assert restarted.get_request(uid(2)).request == request
    use = restarted.approval_use(initial.binding, uid(4))
    assert use.operation_ids == (uid(2),)
    assert len(use.consumptions) == 1
    with pytest.raises(ValueError, match='idempotency'):
        restarted.append(fact(operation_id=uid(7), request=RequestIdentity(uid(7), 'client-key', 'b' * 64)))
    store.seed(replace(store.state.rows[0], payload=to_wire(fact(
        operation_id=uid(8), request=RequestIdentity(uid(8), 'client-key', 'b' * 64)))))
    assert restarted.lookup_request(initial.binding, 'client-key').ambiguous


def test_create_intent_is_durable_without_fake_pre_version():
    ledger, store = durable()
    binding = replace(binding_fixture(), space_id=None)
    provisional = fact(binding=binding, fact_kind=FactKind.CREATE_INTENT, operation_type='create')
    intent = CreateIntentRef(provisional.event_id, uid(2), binding.binding_id, 1, DIGEST)
    created = replace(provisional, evidence=intent)
    ledger.append(created)
    restarted, _ = durable(store)
    assert restarted.lookup_request(binding, 'client-key').facts == (created,)
    assert created.pre_version_id is None
    with pytest.raises(ValueError, match='create intent'):
        durable()[0].append(replace(created, pre_version_id=uid(10)))
    with pytest.raises(ValueError, match='create intent'):
        durable()[0].append(replace(created, evidence=fact().evidence))
    with pytest.raises(ValueError, match='create intent'):
        durable()[0].append(replace(created, evidence=replace(intent, binding_revision=2)))
def test_target_delta_adapter_and_owned_ddl_are_append_only_and_default_off():
    import json
    from pathlib import Path
    from backend.services.version_control.governance.delta import DeltaFactStore

    ddl = Path('backend/version_control_ddl/06-operations.sql').read_text()
    volume = Path('backend/version_control_ddl/07-approval-evidence-volume.sql').read_text()
    assert ddl.count('CREATE TABLE') == 1
    assert "'delta.appendOnly'='true'" in ddl
    for column in ('event_key', 'transition_sequence', 'target_workspace', 'idempotency_key',
                   'request_digest', 'approval_digest', 'evidence_json', 'generation'):
        assert column in ddl
    assert 'vc_approval_evidence' in volume and volume.count('CREATE VOLUME') == 1
    calls = []

    def sql(statement, parameters):
        calls.append((statement, parameters))
        if statement.startswith('SELECT'):
            return [{'evidence_json': calls[0][1]['evidence_json']}]
        return []

    store = DeltaFactStore(sql, 'catalog', 'control', '123')
    with pytest.raises(PermissionError, match='disabled'):
        store.append(fact().event_key, to_wire(fact()))
    assert calls == []
    store = DeltaFactStore(sql, 'catalog', 'control', '123', writes_enabled=True)
    store.append(fact().event_key, to_wire(fact()))
    assert calls[0][0].startswith('INSERT INTO `catalog`.`control`.`genie_space_operations`')
    assert json.loads(calls[0][1]['evidence_json'])['schema_version'] == 'VC/1.0'
    assert store.read() == (to_wire(fact()),)
    assert all(not statement.startswith(('UPDATE', 'DELETE', 'MERGE')) for statement, _ in calls)
    with pytest.raises(PermissionError, match='target'):
        store.append(fact().event_key, to_wire(fact(binding=replace(binding_fixture(), workspace_id='source'))))
    with pytest.raises(ValueError):
        DeltaFactStore(sql, 'catalog; DROP TABLE x', 'control', '123')
@pytest.mark.parametrize('kind', [FactKind.APPROVAL_REQUEST, FactKind.APPROVAL_VOTE,
                                 FactKind.APPROVAL_GRANTED, FactKind.APPROVAL_INVALIDATED,
                                 FactKind.RELEASE, FactKind.RECEIPT, FactKind.BREAK_GLASS,
                                 FactKind.RECOVERY])
def test_fact_kind_rejects_unrelated_typed_evidence(kind):
    ledger, store = durable()
    with pytest.raises(ValueError, match='evidence'):
        ledger.append(fact(fact_kind=kind))
    assert store.state.rows == []


@pytest.mark.parametrize('fault', ['missing-reader', 'corrupted', 'outage', 'foreign-volume', 'traversal'])
def test_private_evidence_loss_fails_closed_at_durable_read(fault):
    import json
    from hashlib import sha256
    from backend.services.version_control.governance.delta import DeltaFactStore

    content = b'private-preflight-evidence'
    uri = '/Volumes/catalog/control/vc_approval_evidence/preflight.json'
    evidence = fact(evidence_uri=uri, evidence_digest=sha256(content).hexdigest())
    payload = to_wire(evidence)
    sql = lambda *args: [{'evidence_json': json.dumps({'schema_version': 'VC/1.0', 'fact': payload})}]
    seen = []

    def read(uri):
        seen.append(uri)
        return content

    adapter = DeltaFactStore(sql, 'catalog', 'control', '123', read_evidence=read)
    assert adapter.read() == (payload,)
    assert seen == [uri]
    if fault == 'missing-reader':
        adapter = DeltaFactStore(sql, 'catalog', 'control', '123')
    elif fault == 'corrupted':
        content = b'changed'
    elif fault == 'outage':
        def read(uri):
            raise OSError('Volume unavailable')
        adapter = DeltaFactStore(sql, 'catalog', 'control', '123', read_evidence=read)
    elif fault == 'foreign-volume':
        payload['evidence_uri'] = '/Volumes/catalog/control/export/preflight.json'
    else:
        payload['evidence_uri'] = uri + '/../other.json'
    with pytest.raises((PermissionError, ValueError, OSError)):
        adapter.read()
