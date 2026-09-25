"""Offline tests for the Delta-backed identity registry (M02)."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from backend.services.version_control.contracts import (
    ActorContext,
    BindingRef,
    CreateIntentRef,
    EnrollmentRequest,
    IdentityEvidence,
    RebindAuthorization,
)
from backend.services.version_control.registry import DeltaRegistry, RegistryConflict

NOW = datetime(2026, 9, 7, tzinfo=UTC)
DIGEST = "a" * 64
WORKSPACE = "123"


def uid(number):
    return str(UUID(int=number))


class FakeRegistrySql:
    def __init__(self):
        self.rows = []
        self.calls = []

    def seed(self, **row):
        self.rows.append(row)

    def __call__(self, statement, parameters):
        self.calls.append((statement, parameters))
        text = statement.strip()
        if text.startswith("INSERT INTO"):
            self.rows.append(dict(parameters))
            return []
        if text.startswith("SELECT * FROM") and "space_key = :space_key" in text:
            matched = [row for row in self.rows if row["space_key"] == parameters["space_key"]]
            matched.sort(key=lambda row: (row["binding_id"], row["binding_revision"], row["event_sequence"]))
            return [dict(row) for row in matched]
        if text.startswith("SELECT * FROM") and "binding_id = :binding_id" in text:
            matched = [row for row in self.rows if row["binding_id"] == parameters["binding_id"]]
            matched.sort(key=lambda row: (row["binding_revision"], row["event_sequence"]))
            return [dict(row) for row in matched]
        raise AssertionError(f"Unexpected statement: {text}")


class RecordingInitializer:
    def __init__(self):
        self.calls = []

    def __call__(self, binding, proof):
        self.calls.append((binding, proof))


_UNSET = object()


def make_registry(sql=None, *, enabled=True, initialize=_UNSET, binding_ids=None, event_ids=None):
    sql = sql or FakeRegistrySql()
    initialize = RecordingInitializer() if initialize is _UNSET else initialize
    bindings = iter(binding_ids) if binding_ids else None
    events = iter(event_ids) if event_ids else None
    registry = DeltaRegistry(
        sql, "cat", "control", WORKSPACE, clock=lambda: NOW, writes_enabled=enabled,
        initialize=initialize,
        new_binding_id=(lambda: next(bindings)) if bindings else None,
        new_event_id=(lambda: next(events)) if events else None)
    return registry, sql, initialize


def enrollment(space_key="sales"):
    return EnrollmentRequest(space_key, WORKSPACE, None, "dev", "operator enrolled")


def actor():
    return ActorContext("operator@example.invalid", WORKSPACE, "human")


def test_writes_default_off_touches_no_sql():
    sql = FakeRegistrySql()
    registry, _, _ = make_registry(sql, enabled=False)
    with pytest.raises(PermissionError, match="disabled"):
        registry.enroll(enrollment(), actor())
    assert sql.calls == []


def test_enroll_creates_provisional_binding_and_initializes_coordination():
    registry, _, initialize = make_registry(binding_ids=[uid(1)], event_ids=[uid(100)])
    binding = registry.enroll(enrollment(), actor())
    assert binding == BindingRef(uid(1), 1, "sales", WORKSPACE, None, "dev")
    assert registry.resolve(uid(1)) == binding
    assert len(initialize.calls) == 1
    initialized_binding, proof = initialize.calls[0]
    assert initialized_binding == binding
    assert proof.registry_event_id == uid(100) and proof.binding_id == uid(1)


def test_enroll_requires_initializer_to_be_write_ready():
    registry, _, _ = make_registry(initialize=None, binding_ids=[uid(1)], event_ids=[uid(100)])
    with pytest.raises(PermissionError, match="not write-ready"):
        registry.enroll(enrollment(), actor())


def test_duplicate_enrollment_for_space_key_is_refused():
    registry, _, _ = make_registry(binding_ids=[uid(1), uid(2)], event_ids=[uid(100), uid(101)])
    registry.enroll(enrollment(), actor())
    with pytest.raises(RegistryConflict, match="already has a live binding"):
        registry.enroll(enrollment(), actor())


def test_bind_created_attaches_audited_identity_to_provisional():
    registry, _, _ = make_registry(binding_ids=[uid(1)], event_ids=[uid(100), uid(101)])
    registry.enroll(enrollment(), actor())
    intent = CreateIntentRef(uid(100), uid(9), uid(1), 1, DIGEST)
    evidence = IdentityEvidence("VC/1.0", WORKSPACE, "physical-1", DIGEST, NOW, actor())
    bound = registry.bind_created(intent, "physical-1", evidence)
    assert bound == BindingRef(uid(1), 1, "sales", WORKSPACE, "physical-1", "dev")
    assert registry.resolve(uid(1)) == bound


def test_bind_created_rejects_unaudited_or_already_bound():
    registry, _, _ = make_registry(binding_ids=[uid(1)], event_ids=[uid(100), uid(101)])
    registry.enroll(enrollment(), actor())
    intent = CreateIntentRef(uid(100), uid(9), uid(1), 1, DIGEST)
    mismatched = IdentityEvidence("VC/1.0", WORKSPACE, "other-space", DIGEST, NOW, actor())
    with pytest.raises(PermissionError, match="does not match"):
        registry.bind_created(intent, "physical-1", mismatched)
    good = IdentityEvidence("VC/1.0", WORKSPACE, "physical-1", DIGEST, NOW, actor())
    registry.bind_created(intent, "physical-1", good)
    with pytest.raises(RegistryConflict, match="provisional"):
        registry.bind_created(intent, "physical-1", good)


def test_resolve_rejects_forked_chain():
    sql = FakeRegistrySql()
    for event_id in (uid(100), uid(101)):
        sql.seed(registry_event_id=event_id, space_key="sales", binding_id=uid(1),
                 binding_revision=1, event_sequence=1, event_type="provisional",
                 workspace_id=WORKSPACE, space_id=None, environment="dev")
    registry, _, _ = make_registry(sql)
    with pytest.raises(RegistryConflict, match="Forked"):
        registry.resolve(uid(1))


def test_tombstone_supersedes_and_rejects_old_token():
    registry, _, _ = make_registry(binding_ids=[uid(1)],
                                   event_ids=[uid(100), uid(101), uid(102)])
    registry.enroll(enrollment(), actor())
    intent = CreateIntentRef(uid(100), uid(9), uid(1), 1, DIGEST)
    evidence = IdentityEvidence("VC/1.0", WORKSPACE, "physical-1", DIGEST, NOW, actor())
    bound = registry.bind_created(intent, "physical-1", evidence)
    approval = RebindAuthorization(bound, uid(50), DIGEST, actor(), "decommission",
                                   NOW + timedelta(hours=1))
    superseded = registry.tombstone(bound, approval)
    assert superseded.binding_revision == 2
    assert registry.resolve(uid(1)) == superseded
    assert registry.resolve(uid(1)) != bound  # old token no longer resolves


def test_rebind_requires_current_unexpired_authorization():
    registry, _, _ = make_registry(binding_ids=[uid(1)],
                                   event_ids=[uid(100), uid(101), uid(102)])
    registry.enroll(enrollment(), actor())
    intent = CreateIntentRef(uid(100), uid(9), uid(1), 1, DIGEST)
    evidence = IdentityEvidence("VC/1.0", WORKSPACE, "physical-1", DIGEST, NOW, actor())
    bound = registry.bind_created(intent, "physical-1", evidence)
    expired = RebindAuthorization(bound, uid(50), DIGEST, actor(), "swap", NOW - timedelta(seconds=1))
    with pytest.raises(PermissionError, match="expired"):
        registry.rebind(bound, "physical-2", expired)
    valid = RebindAuthorization(bound, uid(50), DIGEST, actor(), "swap", NOW + timedelta(hours=1))
    rebound = registry.rebind(bound, "physical-2", valid)
    assert rebound == BindingRef(uid(1), 2, "sales", WORKSPACE, "physical-2", "dev")
    assert registry.resolve(uid(1)) == rebound


def test_workspace_guard_rejects_foreign_enrollment():
    registry, sql, _ = make_registry(binding_ids=[uid(1)], event_ids=[uid(100)])
    foreign = EnrollmentRequest("sales", "999", None, "dev", "operator enrolled")
    with pytest.raises(PermissionError, match="trusted workspace"):
        registry.enroll(foreign, actor())
    assert sql.rows == []
