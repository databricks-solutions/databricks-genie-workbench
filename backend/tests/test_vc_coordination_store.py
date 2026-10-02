"""Offline tests for the production Delta coordination CAS store (M03).

The live CAS behavior (real optimistic concurrency, one winner) is proved by the
deployment gate test_vc_delta_cas.py. These offline tests pin the row<->column
mapping and the CAS decision/guard logic against a stateful fake `execute` seam
that interprets SELECT/INSERT/MERGE by parameters (never by SQL text).
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from backend.services.version_control.contracts import (
    CoordinationState,
    Heads,
    PatchStage,
)
from backend.services.version_control.coordination.row import CoordinationRow
from backend.services.version_control.coordination.service import OwnershipError
from backend.services.version_control.coordination.store import (
    DeltaCoordinationStore,
    row_from_columns,
    row_to_columns,
)
from backend.tests.vc_fakes.fixtures import binding_fixture

NOW = datetime(2026, 9, 7, tzinfo=UTC)
DIGEST = "a" * 64


def uid(number):
    return str(UUID(int=number))


class FakeCoordSql:
    def __init__(self):
        self.stored = None
        self.duplicate = False
        self.raise_on_merge = None
        self.truncate_ms = False  # simulate REST Statements API TIMESTAMP ms-truncation
        self.calls = []

    def _maybe_truncate(self):
        if not self.truncate_ms or self.stored is None:
            return
        for column in ("updated_at", "lease_expires_at", "admitted_at"):
            value = self.stored.get(column)
            if isinstance(value, datetime):
                self.stored[column] = value.replace(microsecond=(value.microsecond // 1000) * 1000)

    def __call__(self, statement, parameters=None):
        parameters = parameters or {}
        self.calls.append((statement.strip(), parameters))
        text = statement.strip()
        if text.startswith("SELECT"):
            if self.stored is None:
                return []
            rows = [dict(self.stored)]
            return rows * 2 if self.duplicate else rows
        if text.startswith("INSERT"):
            self.stored = dict(parameters)
            self._maybe_truncate()
            return []
        if text.startswith("MERGE"):
            if self.raise_on_merge is not None:
                raise self.raise_on_merge
            if self.stored is not None and (
                    self.stored["binding_revision"] == parameters["revision"]
                    and self.stored["row_version"] == parameters["version"]
                    and self.stored["generation"] == parameters["generation"]
                    and self.stored["state"] == parameters["state"]
                    and bool(self.stored["unresolved"]) == bool(parameters["unresolved"])
                    and self.stored["attempt_id"] == parameters["attempt"]):
                for key, value in parameters.items():
                    if key.startswith("new_"):
                        self.stored[key[len("new_"):]] = value
                self._maybe_truncate()
            return []
        raise AssertionError(f"Unexpected statement: {text}")

    def merges(self):
        return [call for call in self.calls if call[0].startswith("MERGE")]


def make_store(sql=None, binding=None):
    sql = sql or FakeCoordSql()
    binding = binding or binding_fixture()
    store = DeltaCoordinationStore(sql, "`cat`.`control`.`genie_ops_coordination`",
                                   resolve_binding=lambda _id: binding, clock=lambda: NOW)
    return store, sql, binding


def test_initialize_and_read_roundtrip_idle_row():
    store, _, binding = make_store()
    store.initialize(binding, None)
    assert store.read(binding.binding_id) == CoordinationRow(binding, NOW)


def test_read_none_when_empty_and_duplicate_fails_closed():
    store, sql, binding = make_store()
    assert store.read(binding.binding_id) is None
    store.initialize(binding, None)
    sql.duplicate = True
    with pytest.raises(OwnershipError, match="Duplicate"):
        store.read(binding.binding_id)


def test_cas_success_updates_and_returns_new_row():
    store, sql, binding = make_store()
    store.initialize(binding, None)
    result = store.compare_and_swap(
        binding.binding_id, 1, 0, 0, lambda row: row.state == CoordinationState.IDLE,
        state=CoordinationState.RESERVED, holder="principal", attempt_id=uid(5),
        active_operation_id=uid(6), generation=1, unresolved=True)
    assert result is not None
    assert result.state == CoordinationState.RESERVED
    assert result.row_version == 1 and result.generation == 1
    assert result.holder == "principal" and result.attempt_id == uid(5)
    assert store.read(binding.binding_id) == result
    assert len(sql.merges()) == 1


def test_cas_win_detected_despite_server_timestamp_truncation():
    """The REST Statements API returns Delta TIMESTAMP columns truncated to millisecond
    precision, so the read-back row never byte-equals the locally computed `after`
    (updated_at/lease_expires_at). The CAS must still detect the win by the optimistic
    token (row_version/generation/attempt_id) and return the durable, server-truth row --
    otherwise a MERGE that already committed would false-negative and orphan the write
    (the live promotion-gate 'Serializable CAS lost' bug)."""
    micros = datetime(2026, 9, 7, 12, 0, 0, 123456, tzinfo=UTC)
    sql = FakeCoordSql()
    sql.truncate_ms = True
    binding = binding_fixture()
    store = DeltaCoordinationStore(sql, "`cat`.`control`.`genie_ops_coordination`",
                                   resolve_binding=lambda _id: binding, clock=lambda: micros)
    store.initialize(binding, None)
    result = store.compare_and_swap(
        binding.binding_id, 1, 0, 0, lambda row: row.state == CoordinationState.IDLE,
        state=CoordinationState.RESERVED, holder="principal", attempt_id=uid(5),
        active_operation_id=uid(6), generation=1, unresolved=True,
        lease_expires_at=micros + timedelta(seconds=60))
    assert result is not None  # committed win, not a false negative on truncated ts
    assert result.state == CoordinationState.RESERVED and result.row_version == 1
    # The returned row carries the server-truncated timestamp, not the local micros.
    assert result.updated_at == micros.replace(microsecond=123000)
    assert result.lease_expires_at == (micros + timedelta(seconds=60)).replace(microsecond=123000)


def test_cas_precondition_miss_returns_none_without_merge():
    store, sql, binding = make_store()
    store.initialize(binding, None)
    result = store.compare_and_swap(
        binding.binding_id, 1, 5, 0, lambda row: True, state=CoordinationState.RESERVED)
    assert result is None
    assert sql.merges() == []


def test_cas_failed_predicate_returns_none_without_merge():
    store, sql, binding = make_store()
    store.initialize(binding, None)
    result = store.compare_and_swap(
        binding.binding_id, 1, 0, 0, lambda row: False, state=CoordinationState.RESERVED)
    assert result is None
    assert sql.merges() == []


def test_cas_delta_conflict_is_loss_but_other_errors_fail_closed():
    store, sql, binding = make_store()
    store.initialize(binding, None)
    sql.raise_on_merge = RuntimeError("[DELTA_CONCURRENT_WRITE] concurrent update")
    assert store.compare_and_swap(
        binding.binding_id, 1, 0, 0, lambda row: True, state=CoordinationState.RESERVED) is None
    sql.raise_on_merge = RuntimeError("Query timed out")
    with pytest.raises(RuntimeError, match="timed out"):
        store.compare_and_swap(
            binding.binding_id, 1, 0, 0, lambda row: True, state=CoordinationState.RESERVED)


def test_cas_rejects_managed_metadata_and_generation_regression():
    store, _, binding = make_store()
    store.initialize(binding, None)
    for managed in ("binding", "row_version", "updated_at"):
        with pytest.raises(ValueError, match="managed row metadata"):
            store.compare_and_swap(binding.binding_id, 1, 0, 0, lambda row: True, **{managed: 1})
    with pytest.raises(ValueError, match="regress"):
        store.compare_and_swap(binding.binding_id, 1, 0, 2, lambda row: True, generation=1)


def test_row_column_mapping_roundtrips_every_field():
    binding = binding_fixture()
    row = CoordinationRow(
        binding=binding, updated_at=NOW, generation=3, row_version=7,
        state=CoordinationState.ADMITTED, holder="p", attempt_id=uid(1), executor_kind="service",
        executor_ref="job/1", lease_expires_at=NOW + timedelta(minutes=5),
        active_operation_id=uid(2), idempotency_key="key", request_digest=DIGEST,
        approval_id=uid(3), approval_digest=DIGEST, approval_consumption_published=True,
        pre_version_id=uid(4), preimage_digest=DIGEST, create_intent_event_id=uid(5),
        expected_base_fingerprint=DIGEST, admitted_at=NOW + timedelta(seconds=1),
        mutation_stage=PatchStage.CONFIG_IN_FLIGHT, checkpoint={"stage": "x"}, unresolved=True,
        quarantine_reason="fenced", termination_evidence={"source": "supervisor"},
        heads=Heads(uid(6), uid(7), uid(8)), observed_sequence=9)
    assert row_from_columns(row_to_columns(row), binding.environment) == row
