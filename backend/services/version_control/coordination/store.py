"""Delta-backed coordination CAS store (M03), promoted from the M03 release gate.

Single-row Serializable UPDATE that never INSERTs on a CAS miss, over
`genie_ops_coordination`. The MERGE mirrors the statement the live M03 CAS gate
(`backend/tests/integration/test_vc_delta_cas.py`) validated against real
serverless Delta: it re-checks the fence (`binding_revision`, `row_version`,
`generation`), the occupied state and the attempt in SQL, and treats *only* a
confirmed Delta optimistic-concurrency abort as a CAS loss — timeouts,
permission and ambiguous errors fail closed by re-raising.

`environment` is not a coordination column; it is recovered from the injected
binding resolver (the registry is the identity authority, coordination stores
operational state). The same-shape `FakeCoordinationStore` in `vc_fakes/stores`
is the offline behavioral contract, so this class enforces the same guards:
identity/managed-metadata cannot be swapped and the fence generation cannot
regress.
"""

import json
import re
from dataclasses import replace
from datetime import UTC

from backend.services.version_control.contracts import (
    BindingRef,
    CoordinationState,
    Heads,
    PatchStage,
    to_wire,
)
from backend.services.version_control.coordination.row import CoordinationRow
from backend.services.version_control.coordination.service import OwnershipError

_DELTA_CONFLICT = re.compile(
    r"\[DELTA_CONCURRENT_(?:APPEND|DELETE_READ|DELETE_DELETE|WRITE)(?:\.[A-Z_]+)?\]")
_MANAGED_CHANGES = ("binding", "row_version", "updated_at")
_HEAD_COLUMNS = (("observed", "observed_head_version_id"), ("approved", "approved_head_version_id"),
                 ("deployed", "deployed_head_version_id"))


def _to_naive_utc(value):
    return value.astimezone(UTC).replace(tzinfo=None) if value is not None else None


def _from_naive_utc(value):
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _dumps(value):
    if value is None:
        return None
    return json.dumps(to_wire(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def row_to_columns(row: CoordinationRow) -> dict:
    """Flatten a CoordinationRow into the owned coordination DDL columns."""
    binding = row.binding
    columns = {
        "binding_id": binding.binding_id, "binding_revision": binding.binding_revision,
        "space_key": binding.space_key, "workspace_id": binding.workspace_id,
        "space_id": binding.space_id, "generation": row.generation, "row_version": row.row_version,
        "state": row.state.value, "holder": row.holder, "attempt_id": row.attempt_id,
        "executor_kind": row.executor_kind, "executor_ref": row.executor_ref,
        "lease_expires_at": _to_naive_utc(row.lease_expires_at),
        "active_operation_id": row.active_operation_id, "idempotency_key": row.idempotency_key,
        "request_digest": row.request_digest, "approval_id": row.approval_id,
        "approval_digest": row.approval_digest,
        "approval_consumption_published": row.approval_consumption_published,
        "pre_version_id": row.pre_version_id, "preimage_digest": row.preimage_digest,
        "create_intent_event_id": row.create_intent_event_id,
        "expected_base_fingerprint": row.expected_base_fingerprint,
        "admitted_at": _to_naive_utc(row.admitted_at),
        "mutation_stage": row.mutation_stage.value if row.mutation_stage is not None else None,
        "checkpoint_json": _dumps(row.checkpoint), "unresolved": row.unresolved,
        "quarantine_reason": row.quarantine_reason,
        "termination_evidence_json": _dumps(row.termination_evidence),
        "observed_sequence": row.observed_sequence,
        "updated_at": _to_naive_utc(row.updated_at),
    }
    for attribute, column in _HEAD_COLUMNS:
        columns[column] = getattr(row.heads, attribute)
    return columns


def row_from_columns(values: dict, environment: str) -> CoordinationRow:
    binding = BindingRef(values["binding_id"], values["binding_revision"], values["space_key"],
                         values["workspace_id"], values["space_id"], environment)
    heads = Heads(*(values[column] for _, column in _HEAD_COLUMNS))
    checkpoint = json.loads(values["checkpoint_json"]) if values["checkpoint_json"] is not None else None
    termination = (json.loads(values["termination_evidence_json"])
                   if values["termination_evidence_json"] is not None else None)
    mutation_stage = PatchStage(values["mutation_stage"]) if values["mutation_stage"] is not None else None
    return CoordinationRow(
        binding=binding, updated_at=_from_naive_utc(values["updated_at"]),
        generation=values["generation"], row_version=values["row_version"],
        state=CoordinationState(values["state"]), holder=values["holder"],
        attempt_id=values["attempt_id"], executor_kind=values["executor_kind"],
        executor_ref=values["executor_ref"], lease_expires_at=_from_naive_utc(values["lease_expires_at"]),
        active_operation_id=values["active_operation_id"], idempotency_key=values["idempotency_key"],
        request_digest=values["request_digest"], approval_id=values["approval_id"],
        approval_digest=values["approval_digest"],
        approval_consumption_published=bool(values["approval_consumption_published"]),
        pre_version_id=values["pre_version_id"], preimage_digest=values["preimage_digest"],
        create_intent_event_id=values["create_intent_event_id"],
        expected_base_fingerprint=values["expected_base_fingerprint"],
        admitted_at=_from_naive_utc(values["admitted_at"]), mutation_stage=mutation_stage,
        checkpoint=checkpoint, unresolved=bool(values["unresolved"]),
        quarantine_reason=values["quarantine_reason"], termination_evidence=termination,
        heads=heads, observed_sequence=values["observed_sequence"])


class DeltaCoordinationStore:
    def __init__(self, execute, table, *, resolve_binding, clock):
        self.execute = execute
        self.table = table
        self.resolve_binding = resolve_binding
        self.clock = clock

    def read(self, binding_id: str) -> CoordinationRow | None:
        rows = self.execute(
            f"SELECT * FROM {self.table} WHERE binding_id = :binding_id",
            {"binding_id": binding_id})
        if len(rows) > 1:
            raise OwnershipError("Duplicate real Delta coordination rows")
        if not rows:
            return None
        values = dict(rows[0])
        if values["binding_id"] != binding_id:
            raise OwnershipError("Coordination row identity mismatch")
        binding = self.resolve_binding(binding_id)
        if binding is None:
            raise OwnershipError("No binding for coordination row")
        return row_from_columns(values, binding.environment)

    def initialize(self, binding: BindingRef, enrollment) -> None:
        """M03 EnrollmentInitializer: insert the one IDLE row for a new binding.

        Enrollment serialization and write-enablement are enforced upstream by
        M02's registry; a duplicate row would surface at read time as a
        fail-closed OwnershipError rather than being silently repaired.
        """
        columns = row_to_columns(CoordinationRow(binding, self.clock()))
        names = ", ".join(columns)
        placeholders = ", ".join(f":{name}" for name in columns)
        self.execute(f"INSERT INTO {self.table} ({names}) VALUES ({placeholders})", columns)

    def compare_and_swap(self, binding_id, binding_revision, expected_row_version,
                         expected_generation, predicate, **changes) -> CoordinationRow | None:
        if any(key in changes for key in _MANAGED_CHANGES):
            raise ValueError("Runtime CAS cannot replace identity or managed row metadata")
        if changes.get("generation", expected_generation) < expected_generation:
            raise ValueError("Generation cannot regress")
        before = self.read(binding_id)
        if (before is None
                or before.binding.binding_revision != binding_revision
                or before.row_version != expected_row_version
                or before.generation != expected_generation
                or not predicate(before)):
            return None
        after = replace(before, **changes, row_version=expected_row_version + 1,
                        updated_at=self.clock())
        columns = row_to_columns(after)
        parameters = {f"new_{key}": value for key, value in columns.items()}
        parameters.update(id=binding_id, revision=binding_revision, version=expected_row_version,
                          generation=expected_generation, state=before.state.value,
                          unresolved=before.unresolved, attempt=before.attempt_id)
        assignments = ", ".join(f"t.{key} = :new_{key}" for key in columns)
        statement = f"""MERGE INTO {self.table} AS t
            USING (SELECT :id AS binding_id) AS s ON t.binding_id = s.binding_id
            WHEN MATCHED AND t.binding_revision = :revision
              AND t.row_version = :version AND t.generation = :generation
              AND t.state = :state AND t.unresolved = :unresolved
              AND t.attempt_id <=> :attempt
            THEN UPDATE SET {assignments}"""
        try:
            self.execute(statement, parameters)
        except Exception as error:
            # Only a confirmed Delta optimistic-concurrency abort is a CAS loss;
            # timeouts, permission and ambiguous errors must fail closed.
            if _DELTA_CONFLICT.search(str(error)):
                return None
            raise
        observed = self.read(binding_id)
        # Detect the win by the optimistic-concurrency token advancing to exactly this
        # attempt's (row_version, generation, attempt_id) -- NOT byte-exact full-row
        # equality. Server-authoritative timestamps (`updated_at`/`lease_expires_at`)
        # are stored as Delta TIMESTAMP and round-trip through the REST Statements API
        # truncated to millisecond precision, so `observed == after` can never hold
        # against a real table and would false-negative *after the MERGE already
        # committed*, orphaning the write. Delta serializes concurrent MERGEs (the
        # loser raises DELTA_CONCURRENT, caught above), so a row_version match uniquely
        # identifies our commit; return the durable (server-truth) row.
        if (observed is None or observed.row_version != after.row_version
                or observed.generation != after.generation
                or observed.attempt_id != after.attempt_id):
            return None
        return observed
