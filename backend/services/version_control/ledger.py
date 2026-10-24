"""Delta-backed immutable version ledger (M02).

Parameter-bound SQL over the target-owned `genie_space_versions` table; M08
supplies the explicitly target-authenticated SQL adapter. A read-only artifact
adapter is retained solely to resolve legacy envelope pointers. This adapter is
fail-closed and append-only:

* `append_observation` never treats a submitted desired payload as an
  observation — only a `Snapshot` produced by the canonicalizer from a real GET
  envelope is durable.
* The response envelope is stored inline in Delta (CUJ-1 mimics the optimizer);
  the write path never forks to a Volume. An envelope over the inline guard is a
  loud, explicit failure, never a silent divert. See
  OBSERVE_ONLY_PIVOT_PLAN.md §16.
* Idempotency is keyed on the caller's `observation_key`, not a content hash: an
  A→B→A history retains the final A, an exact adjacent re-read reuses the prior
  version, and a lost INSERT response resolves by re-reading the same key.
* Delta enforces neither uniqueness nor immutability; an inconsistent duplicate
  for one observation_key fails closed rather than picking a winner.
"""

import base64
import json
import re
from uuid import uuid4

from backend.services.version_control.contracts import (
    ActorContext,
    BindingRef,
    CaptureContext,
    Fingerprints,
    ObservationRef,
    Origin,
    Snapshot,
    Version,
    VersionPage,
    VersionSummary,
    canonical_json_hash,
    to_wire,
)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")
_ENVELOPE_DOMAIN = "vc-envelope/1"

# CUJ-1 stores the whole config inline in Delta. The binding constraint is the
# SQL Statement Execution API request budget (~16 MiB max query text), not Delta
# storage; one INSERT carries ~3 large JSON copies (envelope + canonical_state +
# serialized_space), so we guard the dominant payload (the envelope) at ~1/3 of
# the budget with headroom. This is a defensive tripwire — a real Genie config is
# 5–300 KB, orders of magnitude below it. See OBSERVE_ONLY_PIVOT_PLAN.md §16.
_INLINE_ENVELOPE_GUARD_BYTES = 4 * 1024 * 1024

# Timestamp columns are bound as ISO-8601 strings and cast target-side so the
# adapter never depends on a particular driver's datetime binding.
_TIMESTAMP_COLUMNS = ("observed_at", "api_update_time")

_SUMMARY_COLUMNS = (
    "version_id, binding_id, observed_at, origin, observed_by, parent_version_id, "
    "restored_from_version_id, config_fingerprint, benchmark_fingerprint, "
    "metadata_fingerprint, canonicalizer_version, optimizer_run_id, champion_id"
)


class AmbiguousVersions(RuntimeError):
    """Two durable rows share one observation_key with different evidence."""


def _compact(value):
    return json.dumps(to_wire(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _encode_cursor(observed_at, version_id):
    raw = json.dumps({"observed_at": observed_at, "version_id": version_id},
                     separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(cursor):
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")))
        return str(payload["observed_at"]), str(payload["version_id"])
    except (ValueError, KeyError, TypeError) as error:
        raise ValueError("Malformed history cursor") from error


def _iso(value):
    return value if isinstance(value, str) else to_wire(value)


class DeltaVersionLedger:
    def __init__(self, sql, catalog, control_schema, target_workspace_id, *,
                 write_artifact=None, read_artifact=None, writes_enabled=False,
                 new_version_id=None, inline_max_bytes=_INLINE_ENVELOPE_GUARD_BYTES):
        if any(not _IDENTIFIER.fullmatch(name) for name in (catalog, control_schema)):
            raise ValueError("Invalid control catalog or schema identifier")
        self.sql = sql
        self.catalog = catalog
        self.control_schema = control_schema
        self.table = f"`{catalog}`.`{control_schema}`.`genie_space_versions`"
        self.target_workspace_id = target_workspace_id
        self.write_artifact = write_artifact
        self.read_artifact = read_artifact
        self.writes_enabled = writes_enabled
        self.new_version_id = new_version_id or (lambda: str(uuid4()))
        self.inline_max_bytes = inline_max_bytes
        self.volume_prefix = f"/Volumes/{catalog}/{control_schema}/vc_snapshots/"

    # -- append -----------------------------------------------------------

    def append_observation(self, snapshot: Snapshot, context: CaptureContext) -> ObservationRef:
        if not self.writes_enabled:
            raise PermissionError("Delta version writes disabled")
        binding = context.binding
        if binding.workspace_id != self.target_workspace_id:
            raise PermissionError("Versions must belong to the trusted target workspace")
        if context.actor.workspace_id != self.target_workspace_id:
            raise PermissionError("Observer identity must belong to the trusted target workspace")

        existing = self._lookup_observation(binding, context.observation_key)
        if existing is not None:
            if (existing["state_digest"] != snapshot.state_digest
                    or existing["response_envelope_digest"] != snapshot.response_envelope_digest):
                raise AmbiguousVersions(
                    "Inconsistent durable version for observation key "
                    f"{context.observation_key!r}")
            return self._reference(existing["version_id"], binding, snapshot)

        version_id = self.new_version_id()
        envelope_json, envelope_uri = self._publish_envelope(snapshot)
        parameters = self._row_parameters(version_id, snapshot, context, envelope_json, envelope_uri)
        columns = ", ".join(parameters)
        values = ", ".join(self._placeholder(column) for column in parameters)
        self.sql(f"INSERT INTO {self.table} ({columns}) VALUES ({values})", parameters)
        return self._reference(version_id, binding, snapshot)

    def _publish_envelope(self, snapshot: Snapshot):
        # CUJ-1 stores the envelope inline in Delta and never forks to a Volume.
        # Oversize is a loud, explicit failure — never a silent divert. The read
        # path still resolves legacy `_uri` pointers; nothing here writes one.
        encoded = _compact(snapshot.response_envelope).encode("utf-8")
        if len(encoded) > self.inline_max_bytes:
            raise ValueError(
                "Genie space config too large to version inline: response envelope is "
                f"{len(encoded)} bytes, over the {self.inline_max_bytes}-byte inline guard "
                f"(digest {snapshot.response_envelope_digest}). This guard is a defensive "
                "tripwire; a real config is orders of magnitude smaller, so investigate the "
                "space rather than raising the guard.")
        return encoded.decode("utf-8"), None

    @staticmethod
    def _envelope_digest(content: bytes) -> str:
        return canonical_json_hash(_ENVELOPE_DOMAIN, {"envelope": json.loads(content)})

    def _row_parameters(self, version_id, snapshot, context, envelope_json, envelope_uri):
        binding = context.binding
        fingerprints = snapshot.fingerprints
        return {
            "version_id": version_id,
            "binding_id": binding.binding_id,
            "binding_revision": binding.binding_revision,
            "space_key": binding.space_key,
            "workspace_id": binding.workspace_id,
            "space_id": binding.space_id,
            "environment": binding.environment,
            "observation_key": context.observation_key,
            "observed_at": _iso(context.observed_at),
            "observation_reason": context.observation_reason,
            "observed_by": context.actor.subject_id,
            "actor_kind": context.actor.actor_kind,
            "origin": context.origin.value,
            "parent_version_id": context.parent_version_id,
            "restored_from_version_id": context.restored_from_version_id,
            "operation_id": context.operation_id,
            "attempt_id": context.attempt_id,
            "generation": context.generation,
            "api_update_time": _iso(context.api_update_time) if context.api_update_time else None,
            "response_envelope_json": envelope_json,
            "response_envelope_uri": envelope_uri,
            "response_envelope_digest": snapshot.response_envelope_digest,
            "serialized_space_json": _compact(snapshot.serialized_space),
            "restorable_metadata_json": _compact(snapshot.restorable_metadata),
            "raw_state_digest": snapshot.raw_state_digest,
            "canonical_state_json": _compact(snapshot.canonical_state),
            "config_fingerprint": fingerprints.config,
            "benchmark_fingerprint": fingerprints.benchmark,
            "metadata_fingerprint": fingerprints.metadata,
            "state_digest": snapshot.state_digest,
            "canonicalizer_version": fingerprints.canonicalizer_version,
            "optimizer_run_id": context.optimizer_run_id,
            "champion_id": context.champion_id,
            "release_id": context.release_id,
        }

    @staticmethod
    def _placeholder(column):
        if column in _TIMESTAMP_COLUMNS:
            return f"CAST(:{column} AS TIMESTAMP)"
        return f":{column}"

    def _lookup_observation(self, binding: BindingRef, observation_key: str):
        rows = self.sql(
            f"SELECT version_id, state_digest, response_envelope_digest FROM {self.table} "
            "WHERE binding_id = :binding_id AND binding_revision = :binding_revision "
            "AND observation_key = :observation_key",
            {"binding_id": binding.binding_id, "binding_revision": binding.binding_revision,
             "observation_key": observation_key})
        if not rows:
            return None
        if len({(row["version_id"], row["state_digest"], row["response_envelope_digest"])
                for row in rows}) > 1:
            raise AmbiguousVersions(
                f"Duplicate durable versions for observation key {observation_key!r}")
        return rows[0]

    @staticmethod
    def _reference(version_id, binding: BindingRef, snapshot: Snapshot) -> ObservationRef:
        return ObservationRef(version_id, binding.binding_id, binding.binding_revision,
                              snapshot.state_digest, snapshot.response_envelope_digest)

    # -- read -------------------------------------------------------------

    def verify_committed(self, observation: ObservationRef) -> bool:
        rows = self.sql(
            f"SELECT version_id FROM {self.table} WHERE version_id = :version_id "
            "AND binding_id = :binding_id AND binding_revision = :binding_revision "
            "AND state_digest = :state_digest "
            "AND response_envelope_digest = :response_envelope_digest",
            {"version_id": observation.version_id, "binding_id": observation.binding_id,
             "binding_revision": observation.binding_revision,
             "state_digest": observation.state_digest,
             "response_envelope_digest": observation.envelope_digest})
        return len(rows) == 1

    def get_version(self, binding: BindingRef, version_id: str) -> Version:
        rows = self.sql(
            f"SELECT * FROM {self.table} WHERE binding_id = :binding_id AND version_id = :version_id",
            {"binding_id": binding.binding_id, "version_id": version_id})
        if not rows:
            raise KeyError(f"No version {version_id} for binding {binding.binding_id}")
        if len(rows) > 1:
            raise AmbiguousVersions(f"Duplicate durable rows for version {version_id}")
        return self._to_version(rows[0])

    def history(self, binding: BindingRef, cursor: str | None, limit: int) -> VersionPage:
        if limit <= 0:
            raise ValueError("history limit must be positive")
        parameters = {"binding_id": binding.binding_id, "limit": limit + 1}
        predicate = ""
        if cursor is not None:
            observed_at, cursor_version = _decode_cursor(cursor)
            predicate = ("AND (observed_at < CAST(:cursor_at AS TIMESTAMP) OR "
                         "(observed_at = CAST(:cursor_at AS TIMESTAMP) AND version_id < :cursor_version)) ")
            parameters.update({"cursor_at": observed_at, "cursor_version": cursor_version})
        rows = self.sql(
            f"SELECT {_SUMMARY_COLUMNS} FROM {self.table} WHERE binding_id = :binding_id "
            # LIMIT must be INT in Databricks SQL; the Statements API binds Python ints as
            # BIGINT (INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE), so cast the bound parameter.
            f"{predicate}ORDER BY observed_at DESC, version_id DESC LIMIT CAST(:limit AS INT)",
            parameters)
        page, next_cursor = rows[:limit], None
        if len(rows) > limit:
            last = page[-1]
            next_cursor = _encode_cursor(_iso(last["observed_at"]), last["version_id"])
        return VersionPage(tuple(self._to_summary(row) for row in page), next_cursor)

    def _to_summary(self, row) -> VersionSummary:
        return VersionSummary(
            version_id=row["version_id"], binding_id=row["binding_id"],
            observed_at=_iso(row["observed_at"]), origin=Origin(row["origin"]),
            observed_by=row["observed_by"], parent_version_id=row["parent_version_id"],
            restored_from_version_id=row["restored_from_version_id"],
            fingerprints=self._fingerprints(row),
            optimizer_run_id=row["optimizer_run_id"], champion_id=row["champion_id"])

    def _to_version(self, row) -> Version:
        snapshot = Snapshot(
            response_envelope=self._response_envelope(row),
            serialized_space=json.loads(row["serialized_space_json"]),
            restorable_metadata=json.loads(row["restorable_metadata_json"]),
            response_envelope_digest=row["response_envelope_digest"],
            raw_state_digest=row["raw_state_digest"],
            canonical_state=json.loads(row["canonical_state_json"]),
            fingerprints=self._fingerprints(row),
            state_digest=row["state_digest"])
        context = CaptureContext(
            binding=BindingRef(row["binding_id"], row["binding_revision"], row["space_key"],
                               row["workspace_id"], row["space_id"], row["environment"]),
            observation_key=row["observation_key"], observed_at=_iso(row["observed_at"]),
            observation_reason=row["observation_reason"],
            actor=ActorContext(row["observed_by"], row["workspace_id"], row["actor_kind"]),
            origin=Origin(row["origin"]), parent_version_id=row["parent_version_id"],
            restored_from_version_id=row["restored_from_version_id"],
            operation_id=row["operation_id"], attempt_id=row["attempt_id"],
            generation=row["generation"],
            api_update_time=_iso(row["api_update_time"]) if row["api_update_time"] else None,
            optimizer_run_id=row["optimizer_run_id"], champion_id=row["champion_id"],
            release_id=row["release_id"])
        return Version(row["version_id"], snapshot, context)

    @staticmethod
    def _fingerprints(row) -> Fingerprints:
        return Fingerprints(config=row["config_fingerprint"], benchmark=row["benchmark_fingerprint"],
                            metadata=row["metadata_fingerprint"],
                            canonicalizer_version=row["canonicalizer_version"])

    def _response_envelope(self, row):
        if row["response_envelope_json"] is not None:
            return json.loads(row["response_envelope_json"])
        uri = row["response_envelope_uri"]
        if not uri or not uri.startswith(self.volume_prefix) or self.read_artifact is None:
            raise PermissionError("Snapshot envelope pointer is unreadable or foreign")
        content = self.read_artifact(uri[len(self.volume_prefix):])
        if not isinstance(content, bytes) or self._envelope_digest(content) != row["response_envelope_digest"]:
            raise ValueError("Snapshot envelope integrity failure")
        return json.loads(content)
