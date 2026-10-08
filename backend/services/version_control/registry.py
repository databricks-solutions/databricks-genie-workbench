"""Delta-backed logical identity registry (M02).

Append-only events over `genie_space_registry` project a *current binding* view
that refuses to pick a timestamp winner: any fork (two live events at one
binding's head, or duplicate rows) fails closed so a caller can quarantine
rather than silently bind to one of two physical spaces.

* `enroll` records a provisional binding (stable `binding_id`/revision, no
  physical `space_id`) under per-space-key serialization, then hands the
  enrollment to the injected M03 initializer to create the single coordination
  row. Two concurrent enrollments for one space_key cannot both win.
* `bind_created` attaches the physical id to the provisional binding using
  *audited* create-intent + identity evidence, never a display-name heuristic.
* `tombstone`/`rebind` supersede the binding with a higher revision, so any
  previously issued binding token is rejected by `resolve`.

Delta enforces neither uniqueness nor immutability; this adapter validates the
event chain itself and hands least-privilege append-only grants to M08.
"""

import json
import re
from contextlib import nullcontext
from uuid import uuid4

from backend.services.version_control.contracts import (
    ActorContext,
    BindingRef,
    CreateIntentRef,
    EnrollmentProof,
    EnrollmentRequest,
    IdentityEvidence,
    RebindAuthorization,
    to_wire,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")
_PROVISIONAL, _BOUND, _TOMBSTONED, _REBOUND = "provisional", "bound", "tombstoned", "rebound"


class RegistryConflict(RuntimeError):
    """The registry chain forked or duplicated; a caller must quarantine."""


class DeltaRegistry:
    def __init__(self, sql, catalog, control_schema, workspace_id, *, clock,
                 writes_enabled=False, initialize=None, serialize_enrollment=None,
                 new_binding_id=None, new_event_id=None):
        if any(not _IDENTIFIER.fullmatch(name) for name in (catalog, control_schema)):
            raise ValueError("Invalid control catalog or schema identifier")
        self.sql = sql
        self.table = f"`{catalog}`.`{control_schema}`.`genie_space_registry`"
        self.workspace_id = workspace_id
        self.clock = clock
        self.writes_enabled = writes_enabled
        self.initialize = initialize
        self.serialize_enrollment = serialize_enrollment or (lambda _key: nullcontext())
        self.new_binding_id = new_binding_id or (lambda: str(uuid4()))
        self.new_event_id = new_event_id or (lambda: str(uuid4()))

    # -- reads ------------------------------------------------------------

    def resolve(self, binding_id: str) -> BindingRef:
        head = self._head(self._events_for_binding(binding_id), binding_id)
        return self._binding_of(head)

    def _events_for_binding(self, binding_id):
        return self.sql(
            f"SELECT * FROM {self.table} WHERE binding_id = :binding_id "
            "ORDER BY binding_revision, event_sequence",
            {"binding_id": binding_id})

    @staticmethod
    def _head(rows, binding_id):
        if not rows:
            raise KeyError(f"No registry events for binding {binding_id}")
        # A duplicated (revision, sequence) coordinate is a fork; refuse it.
        coordinates = {}
        for row in rows:
            key = (row["binding_revision"], row["event_sequence"])
            if coordinates.setdefault(key, row["registry_event_id"]) != row["registry_event_id"]:
                raise RegistryConflict(f"Forked registry chain for binding {binding_id}")
        return rows[-1]

    @staticmethod
    def _binding_of(row) -> BindingRef:
        return BindingRef(row["binding_id"], row["binding_revision"], row["space_key"],
                          row["workspace_id"], row["space_id"], row["environment"])

    def find_active_by_space_key(self, space_key) -> BindingRef | None:
        """Public read: the current non-tombstoned binding for a space_key, or None.

        Used by the observe-only auto-enroll path to resolve an already-enrolled space
        before enrolling a new provisional binding (enroll refuses a duplicate live key).
        """
        return self._active_binding_for_space_key(space_key)

    def _active_binding_for_space_key(self, space_key):
        rows = self.sql(
            f"SELECT * FROM {self.table} WHERE space_key = :space_key "
            "ORDER BY binding_id, binding_revision, event_sequence",
            {"space_key": space_key})
        by_binding = {}
        for row in rows:
            by_binding.setdefault(row["binding_id"], []).append(row)
        for binding_id, chain in by_binding.items():
            head = self._head(chain, binding_id)
            if head["event_type"] != _TOMBSTONED:
                return self._binding_of(head)
        return None

    # -- writes -----------------------------------------------------------

    def enroll(self, request: EnrollmentRequest, actor: ActorContext) -> BindingRef:
        self._guard_write(request.workspace_id)
        with self.serialize_enrollment(request.space_key):
            if self._active_binding_for_space_key(request.space_key) is not None:
                raise RegistryConflict(f"space_key {request.space_key!r} already has a live binding")
            binding = BindingRef(self.new_binding_id(), 1, request.space_key,
                                 request.workspace_id, None, request.environment)
            event_id = self.new_event_id()
            self._append(event_id, binding, 1, _PROVISIONAL, None,
                         request.binding_reason, actor.subject_id, None, None)
            if self.initialize is None:
                raise PermissionError("Enrollment initializer unavailable; enrollment is not write-ready")
            self.initialize(binding, EnrollmentProof(event_id, binding.binding_id, 1, event_id))
            return binding

    def bind_created(self, intent: CreateIntentRef, space_id: str,
                     evidence: IdentityEvidence) -> BindingRef:
        self._guard_write(evidence.workspace_id)
        if evidence.space_id != space_id:
            raise PermissionError("Create identity evidence does not match the observed space")
        chain = self._events_for_binding(intent.binding_id)
        head = self._head(chain, intent.binding_id)
        if head["event_type"] != _PROVISIONAL or head["space_id"] is not None:
            raise RegistryConflict("bind_created requires a provisional, unbound head")
        if head["binding_revision"] != intent.binding_revision:
            raise RegistryConflict("Create intent revision does not match the provisional binding")
        binding = BindingRef(intent.binding_id, head["binding_revision"], head["space_key"],
                             head["workspace_id"], space_id, head["environment"])
        self._append(self.new_event_id(), binding, head["event_sequence"] + 1, _BOUND,
                     head["registry_event_id"], head["binding_reason"], evidence.actor.subject_id,
                     intent.operation_id, evidence)
        return binding

    def tombstone(self, binding: BindingRef, approval: RebindAuthorization) -> BindingRef:
        return self._supersede(binding, approval, _TOMBSTONED, None)

    def rebind(self, binding: BindingRef, new_space_id: str,
               approval: RebindAuthorization) -> BindingRef:
        return self._supersede(binding, approval, _REBOUND, new_space_id)

    def _supersede(self, binding, approval, event_type, new_space_id):
        self._guard_write(binding.workspace_id)
        if approval.binding != binding:
            raise PermissionError("Rebind authorization does not match the binding")
        if approval.expires_at <= self.clock():
            raise PermissionError("Rebind authorization has expired")
        head = self._head(self._events_for_binding(binding.binding_id), binding.binding_id)
        if self._binding_of(head) != binding:
            raise RegistryConflict("Rebind targets a superseded binding revision")
        superseded = BindingRef(binding.binding_id, binding.binding_revision + 1, binding.space_key,
                                binding.workspace_id, new_space_id, binding.environment)
        self._append(self.new_event_id(), superseded, head["event_sequence"] + 1, event_type,
                     head["registry_event_id"], approval.reason, approval.actor.subject_id, None, None)
        return superseded

    # -- helpers ----------------------------------------------------------

    def _guard_write(self, workspace_id):
        if not self.writes_enabled:
            raise PermissionError("Delta registry writes disabled")
        if workspace_id != self.workspace_id:
            raise PermissionError("Registry events must belong to the trusted workspace")

    def _append(self, event_id, binding: BindingRef, event_sequence, event_type,
                supersedes, binding_reason, bound_by, create_operation_id, evidence):
        parameters = {
            "registry_event_id": event_id,
            "space_key": binding.space_key,
            "binding_id": binding.binding_id,
            "binding_revision": binding.binding_revision,
            "event_sequence": event_sequence,
            "event_type": event_type,
            "supersedes_event_id": supersedes,
            "workspace_id": binding.workspace_id,
            "space_id": binding.space_id,
            "environment": binding.environment,
            "governed_by": "workbench",
            "binding_reason": binding_reason,
            "bound_by": bound_by,
            "bound_at": to_wire(self.clock()),
            "create_operation_id": create_operation_id,
            "evidence_json": json.dumps(to_wire(evidence), ensure_ascii=False, sort_keys=True,
                                        separators=(",", ":")) if evidence is not None else None,
        }
        columns = ", ".join(parameters)
        values = ", ".join("CAST(:bound_at AS TIMESTAMP)" if column == "bound_at" else f":{column}"
                           for column in parameters)
        self.sql(f"INSERT INTO {self.table} ({columns}) VALUES ({values})", parameters)
