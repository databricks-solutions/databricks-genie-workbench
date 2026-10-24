"""Test-only commit stores, not Delta adapters or authorization implementations.

Each durable state is one logical table and one lock. There is intentionally no
transaction interface spanning stores. Raw seeding and append retain duplicate
keys and forks so consumer tests must address Delta's unenforced uniqueness.
"""

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
from threading import RLock

from backend.services.version_control.contracts import (
    BindingRef, CoordinationState, Digest, Heads, JsonObject, NonnegativeInt,
    PatchStage, UUIDString, WireValue,
)


class CrashBoundary(str, Enum):
    BEFORE_COMMIT = "before-commit"
    AFTER_COMMIT_BEFORE_RESPONSE = "after-commit-before-response"
    DURING_FOLLOW_UP_READ = "during-follow-up-read"


class InjectedCrash(RuntimeError):
    pass


class AmbiguousRows(RuntimeError):
    pass


class FailureInjector:
    def __init__(self) -> None:
        self._remaining: Counter[CrashBoundary] = Counter()
        self._lock = RLock()

    def inject(self, boundary: CrashBoundary) -> None:
        with self._lock:
            self._remaining[CrashBoundary(boundary)] += 1

    def hit(self, boundary: CrashBoundary) -> None:
        with self._lock:
            if self._remaining[boundary]:
                self._remaining[boundary] -= 1
                raise InjectedCrash(boundary.value)


@dataclass(frozen=True)
class _ClockValue(WireValue):
    instant: datetime


class ManualClock:
    def __init__(self, initial: datetime) -> None:
        self._value = _ClockValue(initial)
        self._lock = RLock()

    def now(self) -> datetime:
        with self._lock:
            return self._value.instant

    def advance(self, duration: timedelta) -> None:
        if duration < timedelta(0):
            raise ValueError("The explicit test clock cannot move backwards")
        with self._lock:
            self._value = _ClockValue(self._value.instant + duration)


@dataclass(frozen=True)
class FactRecord(WireValue):
    event_key: str
    payload: JsonObject
    committed_at: datetime


@dataclass
class FactState:
    rows: list[FactRecord] = field(default_factory=list)
    lock: RLock = field(default_factory=RLock, repr=False)


class FakeFactStore:
    def __init__(self, clock: ManualClock, *, state: FactState | None = None) -> None:
        self.clock = clock
        self.state = state if state is not None else FactState()
        self.failures = FailureInjector()

    def seed(self, *rows: FactRecord) -> None:
        """Bypass logical key checks, including conflicting chains and old facts."""
        with self.state.lock:
            self.state.rows.extend(rows)

    def append(self, event_key: str, payload: JsonObject) -> FactRecord:
        with self.state.lock:
            record = FactRecord(event_key, payload, self.clock.now())
            self.failures.hit(CrashBoundary.BEFORE_COMMIT)
            self.state.rows.append(record)
            self.failures.hit(CrashBoundary.AFTER_COMMIT_BEFORE_RESPONSE)
            return record

    def lookup(self, event_key: str) -> tuple[FactRecord, ...]:
        with self.state.lock:
            self.failures.hit(CrashBoundary.DURING_FOLLOW_UP_READ)
            return tuple(row for row in self.state.rows if row.event_key == event_key)


@dataclass(frozen=True)
class CoordinationRow(WireValue):
    binding: BindingRef
    updated_at: datetime
    generation: NonnegativeInt = 0
    row_version: NonnegativeInt = 0
    state: CoordinationState = CoordinationState.IDLE
    holder: str | None = None
    attempt_id: UUIDString | None = None
    executor_kind: str | None = None
    executor_ref: str | None = None
    lease_expires_at: datetime | None = None
    active_operation_id: UUIDString | None = None
    idempotency_key: str | None = None
    request_digest: Digest | None = None
    approval_id: UUIDString | None = None
    approval_digest: Digest | None = None
    approval_consumption_published: bool = False
    pre_version_id: UUIDString | None = None
    preimage_digest: Digest | None = None
    create_intent_event_id: UUIDString | None = None
    expected_base_fingerprint: Digest | None = None
    admitted_at: datetime | None = None
    mutation_stage: PatchStage | None = None
    checkpoint: JsonObject | None = None
    unresolved: bool = False
    quarantine_reason: str | None = None
    termination_evidence: JsonObject | None = None
    heads: Heads = field(default_factory=lambda: Heads(None, None, None))
    observed_sequence: NonnegativeInt = 0


@dataclass
class CoordinationStateStore:
    rows: list[CoordinationRow] = field(default_factory=list)
    lock: RLock = field(default_factory=RLock, repr=False)


class FakeCoordinationStore:
    def __init__(self, clock: ManualClock, *, state: CoordinationStateStore | None = None) -> None:
        self.clock = clock
        self.state = state if state is not None else CoordinationStateStore()
        self.failures = FailureInjector()

    def seed(self, *rows: CoordinationRow) -> None:
        """Enrollment/testing-only INSERT, deliberately permitting duplicate rows."""
        with self.state.lock:
            self.state.rows.extend(rows)

    def _index(self, binding_id: str) -> int | None:
        indexes = [index for index, row in enumerate(self.state.rows)
                   if row.binding.binding_id == binding_id]
        if len(indexes) > 1:
            raise AmbiguousRows(f"Duplicate coordination rows for {binding_id}")
        return indexes[0] if indexes else None

    def read(self, binding_id: str) -> CoordinationRow | None:
        with self.state.lock:
            self.failures.hit(CrashBoundary.DURING_FOLLOW_UP_READ)
            index = self._index(binding_id)
            return self.state.rows[index] if index is not None else None

    def compare_and_swap(self, binding_id: str, binding_revision: int,
                         expected_row_version: int, expected_generation: int,
                         predicate: Callable[[CoordinationRow], bool],
                         **changes: object) -> CoordinationRow | None:
        """Single-row Serializable UPDATE only; a miss never INSERTs a row."""
        with self.state.lock:
            index = self._index(binding_id)
            if index is None:
                return None
            current = self.state.rows[index]
            if (current.binding.binding_revision != binding_revision
                    or current.row_version != expected_row_version
                    or current.generation != expected_generation
                    or not predicate(current)):
                return None
            if "binding" in changes or "row_version" in changes or "updated_at" in changes:
                raise ValueError("Runtime CAS cannot replace identity or managed row metadata")
            if changes.get("generation", expected_generation) < expected_generation:
                raise ValueError("Generation cannot regress")
            updated = replace(current, **changes, row_version=expected_row_version + 1, updated_at=self.clock.now())
            self.failures.hit(CrashBoundary.BEFORE_COMMIT)
            self.state.rows[index] = updated
            self.failures.hit(CrashBoundary.AFTER_COMMIT_BEFORE_RESPONSE)
            return updated
