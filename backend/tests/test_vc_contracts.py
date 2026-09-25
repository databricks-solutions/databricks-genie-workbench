"""Language-neutral VC/1.0 seam and deterministic test-double contracts."""

import ast
from dataclasses import FrozenInstanceError, is_dataclass, replace
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import importlib
import inspect
from hashlib import sha256
import json
from pathlib import Path
from threading import Barrier

import pytest


FIXTURES = Path(__file__).parent / "fixtures" / "vc_contracts"
SPEC = Path(__file__).parents[2] / "docs/design/version_control/implementation_plan/contracts.md"


def contracts():
    return importlib.import_module("backend.services.version_control.contracts")


def test_vc_wire_contract_golden_roundtrip():
    module = contracts()
    fixtures = sorted(FIXTURES.glob("*.json"))
    assert len(fixtures) >= 10
    for path in fixtures:
        golden = json.loads(path.read_text())
        if path.stem == "enums":
            for name, values in golden.items():
                assert [member.value for member in getattr(module, name)] == values
            continue
        value_type = getattr(module, golden["type"])
        for payload in golden["examples"]:
            value = module.from_wire(value_type, payload)
            assert is_dataclass(value)
            assert value.__dataclass_params__.frozen
            assert module.to_wire(value) == payload, path.name
            assert module.from_wire(value_type, json.loads(json.dumps(module.to_wire(value)))) == value


def test_all_spec_ports_are_synchronous_and_types_resolve():
    module = contracts()
    import typing

    source = SPEC.read_text().split("```python\n", 1)[1].split("```", 1)[0]
    for port in ast.parse(source).body:
        protocol = getattr(module, port.name)
        assert protocol._is_protocol
        for method in port.body:
            implementation = getattr(protocol, method.name)
            assert not inspect.iscoroutinefunction(implementation)
            assert list(inspect.signature(implementation).parameters) == [
                argument.arg for argument in method.args.args
            ]
            annotations = typing.get_type_hints(implementation)
            assert "return" in annotations
            for annotation in ast.walk(method):
                if isinstance(annotation, ast.Name) and annotation.id[0].isupper():
                    assert hasattr(module, annotation.id), annotation.id


def test_wire_values_reject_invalid_ids_digests_timestamps_and_mutability():
    module = contracts()
    payload = json.loads((FIXTURES / "version_summary.json").read_text())["examples"][0]
    for field, invalid in (("version_id", "short-id"), ("observed_at", "2026-09-06T00:00:00")):
        with pytest.raises((TypeError, ValueError)):
            module.from_wire(module.VersionSummary, {**payload, field: invalid})
    value = module.from_wire(module.VersionSummary, payload)
    assert value.observed_at.utcoffset().total_seconds() == 0
    with pytest.raises(FrozenInstanceError):
        value.origin = module.Origin.EXTERNAL
    with pytest.raises(ValueError):
        replace(value.fingerprints, config="A" * 64)
    with pytest.raises((TypeError, ValueError)):
        replace(value, observed_at=datetime(2026, 9, 6))
    with pytest.raises((TypeError, ValueError)):
        module.from_wire(module.VersionSummary, {**payload, "unrecognized": True})
    with pytest.raises((TypeError, ValueError)):
        module.BindingRef("00000000-0000-4000-8000-000000000001", 0, "sales", "123", None, "dev")


def test_credentials_are_never_wire_values():
    module = contracts()
    executor = module.ExecutorContext("123", "https://example.invalid", "principal", "service", object(), "job/1")
    with pytest.raises(TypeError, match="credential|ExecutorContext"):
        module.to_wire(executor)
    assert "credential_handle" not in repr(executor)


def test_canonicalizer_hash_is_domain_separated_and_deterministic():
    module = contracts()
    hash_json = getattr(module, "canonical_json_hash", None)
    assert callable(hash_json), "VC/1.0 needs a domain-separated canonical JSON hash"
    payload = {"left": "ab", "right": "c", "digest": "old", "nested": {"z": 2, "a": "é"}}
    expected = sha256('{"domain":"vc-test/1","left":"ab","nested":{"a":"é","z":2},"right":"c"}'.encode()).hexdigest()
    assert hash_json("vc-test/1", payload, digest_field="digest") == expected
    assert hash_json("vc-test/1", dict(reversed(list(payload.items()))), digest_field="digest") == expected
    assert hash_json("vc-test/1", {**payload, "digest": "new"}, digest_field="digest") == expected
    assert payload["digest"] == "old"
    assert hash_json("vc-test/2", payload, digest_field="digest") != expected
    assert hash_json("vc-other/1", payload, digest_field="digest") != expected
    assert hash_json("vc-test/1", {**payload, "left": "a", "right": "bc"}, digest_field="digest") != expected
    for invalid in ({"domain": "spoofed"}, {"value": float("nan")}, {"value": float("inf")}, {1: "key"}):
        with pytest.raises((ValueError, TypeError)):
            hash_json("vc-test/1", invalid)
    with pytest.raises(ValueError):
        hash_json("unversioned", {})
    with pytest.raises(ValueError):
        hash_json("vc-test/1", {}, digest_field="domain")
    fingerprints = module.Fingerprints("a" * 64, "b" * 64, "c" * 64, "vc-c14n/1")
    expected_state = sha256(json.dumps({"domain": "vc-state/1", **module.to_wire(fingerprints)},
                                      sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert fingerprints.state_digest == expected_state
    for field in ("config", "benchmark", "metadata"):
        assert replace(fingerprints, **{field: "d" * 64}).state_digest != expected_state
    assert replace(fingerprints, canonicalizer_version="vc-c14n/2").state_digest != expected_state


def fakes():
    return importlib.import_module("backend.tests.vc_fakes")


def test_fact_store_crash_boundaries_duplicates_and_separate_commits():
    fake = fakes()
    clock = fake.ManualClock(datetime(2026, 9, 6, tzinfo=timezone.utc))
    versions = fake.FakeFactStore(clock)
    operations = fake.FakeFactStore(clock)
    versions.failures.inject(fake.CrashBoundary.BEFORE_COMMIT)
    with pytest.raises(fake.InjectedCrash):
        versions.append("observation/1", {"state": "A"})
    assert versions.lookup("observation/1") == ()
    versions.failures.inject(fake.CrashBoundary.AFTER_COMMIT_BEFORE_RESPONSE)
    with pytest.raises(fake.InjectedCrash):
        versions.append("observation/1", {"state": "A"})
    restarted = fake.FakeFactStore(clock, state=versions.state)
    assert len(restarted.lookup("observation/1")) == 1
    operations.failures.inject(fake.CrashBoundary.BEFORE_COMMIT)
    with pytest.raises(fake.InjectedCrash):
        operations.append("consumed/old", {"approval_id": "old", "operation_id": "original"})
    assert len(restarted.lookup("observation/1")) == 1
    assert operations.lookup("consumed/old") == ()
    operations.append("consumed/old", {"approval_id": "old", "operation_id": "original"})
    operations.append("consumed/new", {"approval_id": "new", "operation_id": "later"})
    assert operations.lookup("consumed/old")[0].payload["operation_id"] == "original"
    versions.failures.inject(fake.CrashBoundary.DURING_FOLLOW_UP_READ)
    with pytest.raises(fake.InjectedCrash):
        versions.lookup("observation/1")
    committed = versions.lookup("observation/1")[0]
    versions.seed(committed, replace(committed, payload={"state": "conflicting"}))
    assert len(versions.lookup("observation/1")) == 3
    with pytest.raises(TypeError):
        committed.payload["state"] = "rewritten"
    for sequence, state in enumerate(("A", "B", "A")):
        versions.append(f"sequence/{sequence}", {"state": state})
    assert versions.lookup("sequence/2")[0].payload["state"] == "A"


def test_coordination_store_single_row_cas_and_crash_boundaries():
    fake = fakes()
    module = contracts()
    clock = fake.ManualClock(datetime(2026, 9, 6, tzinfo=timezone.utc))
    binding = fake.binding_fixture()
    row = fake.CoordinationRow(binding=binding, updated_at=clock.now())
    store = fake.FakeCoordinationStore(clock)
    store.seed(row)

    def cas(target, expected_version=0, expected_generation=0, **changes):
        return target.compare_and_swap(binding.binding_id, binding.binding_revision,
                                       expected_version, expected_generation,
                                       lambda current: current.state == module.CoordinationState.IDLE,
                                       **changes)

    store.failures.inject(fake.CrashBoundary.BEFORE_COMMIT)
    with pytest.raises(fake.InjectedCrash):
        cas(store, state=module.CoordinationState.RESERVED)
    assert store.read(binding.binding_id) == row
    store.failures.inject(fake.CrashBoundary.AFTER_COMMIT_BEFORE_RESPONSE)
    with pytest.raises(fake.InjectedCrash):
        cas(store, state=module.CoordinationState.RESERVED, generation=1)
    restarted = fake.FakeCoordinationStore(clock, state=store.state)
    assert restarted.read(binding.binding_id).row_version == 1
    assert restarted.read(binding.binding_id).generation == 1
    assert cas(restarted) is None
    assert cas(restarted, 1, 1) is None
    store.failures.inject(fake.CrashBoundary.DURING_FOLLOW_UP_READ)
    with pytest.raises(fake.InjectedCrash):
        store.read(binding.binding_id)
    store.seed(row)
    with pytest.raises(fake.AmbiguousRows):
        store.read(binding.binding_id)
    with pytest.raises(fake.AmbiguousRows):
        cas(store)
    empty = fake.FakeCoordinationStore(clock)
    assert cas(empty) is None
    assert empty.read(binding.binding_id) is None


def test_coordination_cas_race_and_old_consumption_survive_row_reuse():
    fake = fakes()
    module = contracts()
    clock = fake.ManualClock(datetime(2026, 9, 6, tzinfo=timezone.utc))
    binding = fake.binding_fixture()
    store = fake.FakeCoordinationStore(clock)
    store.seed(fake.CoordinationRow(binding=binding, updated_at=clock.now()))
    barrier = Barrier(2)

    def race(attempt):
        client = fake.FakeCoordinationStore(clock, state=store.state)
        barrier.wait()
        return client.compare_and_swap(binding.binding_id, 1, 0, 0, lambda row: not row.unresolved,
                                       attempt_id=attempt, generation=1, unresolved=True)

    attempts = ["00000000-0000-4000-8000-000000000011", "00000000-0000-4000-8000-000000000012"]
    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(race, attempts))
    assert sum(result is not None for result in results) == 1
    operations = fake.FakeFactStore(clock)
    operations.append("consumed/old", {"approval_id": "old"})
    updated = store.compare_and_swap(binding.binding_id, 1, 1, 1, lambda row: row.unresolved,
                                     unresolved=False, generation=2, state=module.CoordinationState.IDLE)
    assert updated.row_version == 2
    assert updated.generation == 2
    assert len(operations.lookup("consumed/old")) == 1
    assert store.compare_and_swap(binding.binding_id, 1, 2, 1, lambda row: True) is None
    assert store.compare_and_swap(binding.binding_id, 2, 2, 2, lambda row: True) is None


def test_clock_canonicalizer_and_identity_fixtures_are_explicit():
    fake = fakes()
    module = contracts()
    clock = fake.ManualClock(datetime(2026, 9, 6, tzinfo=timezone.utc))
    before = clock.now()
    assert clock.now() == before
    clock.advance(timedelta(seconds=5))
    assert clock.now() == before + timedelta(seconds=5)
    with pytest.raises(ValueError):
        fake.ManualClock(datetime(2026, 9, 6))
    canonicalizer = fake.FakeCanonicalizer()
    envelope = {"serialized_space": '{"instructions":{"text_instructions":[]}}', "description": "before", "title": "Sales"}
    snapshot = canonicalizer.observe(envelope)
    assert snapshot.response_envelope == envelope
    assert module.to_wire(snapshot.serialized_space) == json.loads(envelope["serialized_space"])
    envelope["description"] = "after"
    assert snapshot.restorable_metadata["description"] == "before"
    changed = canonicalizer.observe(envelope)
    assert changed.fingerprints.config == snapshot.fingerprints.config
    assert changed.fingerprints.metadata != snapshot.fingerprints.metadata
    assert canonicalizer.compare(snapshot, changed) == module.Comparison.DIFFERENT
    assert canonicalizer.compare(snapshot, snapshot) == module.Comparison.EQUAL
    unknown = replace(snapshot, fingerprints=replace(snapshot.fingerprints, canonicalizer_version="future/2"))
    assert canonicalizer.compare(unknown, unknown) == module.Comparison.UNKNOWN
    assert canonicalizer.semantic_diff(snapshot, changed)[0].category == module.DiffCategory.METADATA
    with pytest.raises(ValueError):
        canonicalizer.observe({"desired_payload": {}})
    provider = fake.FakeIdentityProvider()
    binding = fake.binding_fixture()
    actor = fake.actor_fixture()
    request = module.AuthenticatedRequest("verified-session", binding.workspace_id)
    provider.actors[request.authentication_reference] = actor
    assert provider.actor(request) == actor
    assert provider.can_edit(actor.subject_id, binding) is False
    provider.edit_rights.add((actor.subject_id, binding.workspace_id, binding.binding_id, binding.binding_revision))
    assert provider.can_edit(actor.subject_id, binding) is True
    assert provider.can_edit(actor.subject_id, replace(binding, binding_revision=2)) is False
    provider.memberships[(actor.subject_id, binding.workspace_id)] = frozenset({"target-approvers"})
    assert provider.groups(actor.subject_id, binding.workspace_id) == frozenset({"target-approvers"})
    provider.memberships.clear()
    assert provider.groups(actor.subject_id, binding.workspace_id) == frozenset()
    executor = fake.executor_fixture()
    selection = module.ExplicitExecutorSelection(executor.workspace_id, executor.host, executor.principal_id, executor.execution_ref, None)
    with pytest.raises(PermissionError):
        provider.executor(selection)
    provider.executors[selection] = executor
    assert provider.executor(selection) == executor
    provider.run_as[executor.execution_ref] = executor.principal_id
    provider.verify_run_as(executor.execution_ref, executor.principal_id)
    with pytest.raises(PermissionError):
        provider.verify_run_as(executor.execution_ref, "other-principal")
