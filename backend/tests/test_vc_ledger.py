"""Offline tests for the Delta-backed immutable version ledger (M02).

The `sql` seam is faked in-memory (the house pattern from
test_vc_operation_facts.py); snapshots come from the real Canonicalizer so
digests, fingerprints and envelope integrity are exercised for real.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from backend.services.config_fingerprint import Canonicalizer
from backend.services.version_control.contracts import (
    ActorContext,
    BindingRef,
    CaptureContext,
    ObservationRef,
    Origin,
)
from backend.services.version_control.ledger import (
    _INLINE_ENVELOPE_GUARD_BYTES,
    AmbiguousVersions,
    DeltaVersionLedger,
)
from backend.tests.vc_fakes.fixtures import actor_fixture, binding_fixture

NOW = datetime(2026, 9, 7, tzinfo=UTC)
CANON = Canonicalizer()


def uid(number):
    return str(UUID(int=number))


def envelope(description, extra=None):
    space = {"instructions": {"text_instructions": []}}
    if extra:
        space.update(extra)
    return {"serialized_space": space, "description": description}


SNAP_A = CANON.observe(envelope("A"))
SNAP_B = CANON.observe(envelope("B"))


def ctx(observation_key, *, reason="poll", when=NOW, origin=Origin.EXTERNAL, **changes):
    return CaptureContext(binding=binding_fixture(), observation_key=observation_key,
                          observed_at=when, observation_reason=reason,
                          actor=actor_fixture(), origin=origin, **changes)


class FakeVersionsSql:
    """Interprets exactly the statements DeltaVersionLedger issues."""

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
        if text.startswith("SELECT version_id, state_digest, response_envelope_digest"):
            return [{key: row[key] for key in ("version_id", "state_digest", "response_envelope_digest")}
                    for row in self.rows
                    if row["binding_id"] == parameters["binding_id"]
                    and row["binding_revision"] == parameters["binding_revision"]
                    and row["observation_key"] == parameters["observation_key"]]
        if text.startswith("SELECT version_id FROM"):
            return [{"version_id": row["version_id"]} for row in self.rows
                    if row["version_id"] == parameters["version_id"]
                    and row["binding_id"] == parameters["binding_id"]
                    and row["binding_revision"] == parameters["binding_revision"]
                    and row["state_digest"] == parameters["state_digest"]
                    and row["response_envelope_digest"] == parameters["response_envelope_digest"]]
        if text.startswith("SELECT * FROM"):
            return [dict(row) for row in self.rows
                    if row["binding_id"] == parameters["binding_id"]
                    and row["version_id"] == parameters["version_id"]]
        if text.startswith("SELECT version_id, binding_id, observed_at"):
            matched = [row for row in self.rows if row["binding_id"] == parameters["binding_id"]]
            if "cursor_at" in parameters:
                key = (parameters["cursor_at"], parameters["cursor_version"])
                matched = [row for row in matched if (row["observed_at"], row["version_id"]) < key]
            matched.sort(key=lambda row: (row["observed_at"], row["version_id"]), reverse=True)
            return [dict(row) for row in matched[:parameters["limit"]]]
        raise AssertionError(f"Unexpected statement: {text}")


class FakeVolume:
    def __init__(self):
        self.files = {}

    def write(self, relative, content):
        self.files[relative] = content

    def read(self, relative):
        return self.files[relative]


def make_ledger(sql=None, *, enabled=True, volume=None,
                inline_max_bytes=_INLINE_ENVELOPE_GUARD_BYTES, ids=None):
    sql = sql or FakeVersionsSql()
    counter = iter(ids) if ids is not None else None
    ledger = DeltaVersionLedger(
        sql, "cat", "control", "123", writes_enabled=enabled,
        write_artifact=volume.write if volume else None,
        read_artifact=volume.read if volume else None,
        inline_max_bytes=inline_max_bytes,
        new_version_id=(lambda: next(counter)) if counter else None)
    return ledger, sql


def test_writes_default_off_touches_no_sql():
    sql = FakeVersionsSql()
    ledger, _ = make_ledger(sql, enabled=False)
    with pytest.raises(PermissionError, match="disabled"):
        ledger.append_observation(SNAP_A, ctx("k1"))
    assert sql.calls == []


def test_append_is_idempotent_by_observation_key():
    ledger, sql = make_ledger(ids=[uid(10), uid(11)])
    first = ledger.append_observation(SNAP_A, ctx("k1"))
    again = ledger.append_observation(SNAP_A, ctx("k1"))
    assert again == first
    assert first.version_id == uid(10)
    assert len([c for c in sql.calls if c[0].startswith("INSERT")]) == 1
    assert ledger.verify_committed(first) is True


def test_a_b_a_history_retains_final_a():
    ledger, sql = make_ledger(ids=[uid(1), uid(2), uid(3)])
    ledger.append_observation(SNAP_A, ctx("k1", when=NOW))
    ledger.append_observation(SNAP_B, ctx("k2", when=NOW + timedelta(seconds=1)))
    final = ledger.append_observation(SNAP_A, ctx("k3", when=NOW + timedelta(seconds=2)))
    assert len(sql.rows) == 3
    page = ledger.history(binding_fixture(), None, 10)
    assert [item.version_id for item in page.items] == [uid(3), uid(2), uid(1)]
    # The final A is a distinct version but shares first A's state digest.
    assert final.state_digest == ledger.get_version(binding_fixture(), uid(1)).snapshot.state_digest
    assert final.version_id == uid(3)


def test_inconsistent_duplicate_for_key_fails_closed():
    ledger, _ = make_ledger(ids=[uid(1)])
    ledger.append_observation(SNAP_A, ctx("k1"))
    with pytest.raises(AmbiguousVersions):
        ledger.append_observation(SNAP_B, ctx("k1"))


def test_ambiguous_durable_rows_for_key_fail_closed():
    sql = FakeVersionsSql()
    binding = binding_fixture()
    for version_id, snap in ((uid(1), SNAP_A), (uid(2), SNAP_B)):
        sql.seed(version_id=version_id, binding_id=binding.binding_id,
                 binding_revision=binding.binding_revision, observation_key="dup",
                 state_digest=snap.state_digest,
                 response_envelope_digest=snap.response_envelope_digest)
    ledger, _ = make_ledger(sql, ids=[uid(9)])
    with pytest.raises(AmbiguousVersions):
        ledger.append_observation(SNAP_A, ctx("dup"))


def test_workspace_guard_rejects_foreign_binding_and_actor():
    ledger, sql = make_ledger(ids=[uid(1)])
    foreign_binding = BindingRef(uid(5), 1, "sales", "999", "space", "dev")
    with pytest.raises(PermissionError, match="target workspace"):
        ledger.append_observation(SNAP_A, CaptureContext(
            binding=foreign_binding, observation_key="k", observed_at=NOW,
            observation_reason="poll", actor=actor_fixture(), origin=Origin.EXTERNAL))
    with pytest.raises(PermissionError, match="Observer identity"):
        ledger.append_observation(SNAP_A, CaptureContext(
            binding=binding_fixture(), observation_key="k", observed_at=NOW,
            observation_reason="poll", actor=ActorContext("who", "999", "human"),
            origin=Origin.EXTERNAL))
    assert sql.rows == []


def test_large_envelope_inlines_without_volume():
    # A config well over the old 32 KB cap (the exact nonprod regression) now
    # writes inline, never diverting to a Volume — CUJ-1 mimics the optimizer.
    volume = FakeVolume()
    big = CANON.observe(envelope("x" * 40000))
    ledger, sql = make_ledger(volume=volume, ids=[uid(1)])
    ref = ledger.append_observation(big, ctx("k1"))
    row = sql.rows[0]
    assert row["response_envelope_uri"] is None
    assert row["response_envelope_json"] is not None
    assert len(row["response_envelope_json"].encode("utf-8")) > 32768  # over the old cap
    assert volume.files == {}  # no Volume was touched
    restored = ledger.get_version(binding_fixture(), ref.version_id)
    assert restored.snapshot.response_envelope == big.response_envelope


def test_envelope_over_inline_guard_raises_loud_error_without_volume():
    # Over the guard is a loud, explicit failure — never a silent Volume divert.
    volume = FakeVolume()
    big = CANON.observe(envelope("x" * 40000))
    ledger, sql = make_ledger(volume=volume, inline_max_bytes=1024, ids=[uid(1)])
    with pytest.raises(ValueError, match="too large to version inline"):
        ledger.append_observation(big, ctx("k1"))
    assert sql.rows == []
    assert volume.files == {}


def test_history_pagination_is_stable_across_cursor():
    ledger, _ = make_ledger(ids=[uid(1), uid(2), uid(3)])
    for index in range(3):
        ledger.append_observation(CANON.observe(envelope(f"v{index}")),
                                  ctx(f"k{index}", when=NOW + timedelta(seconds=index)))
    first = ledger.history(binding_fixture(), None, 2)
    assert [item.version_id for item in first.items] == [uid(3), uid(2)]
    assert first.next_cursor is not None
    second = ledger.history(binding_fixture(), first.next_cursor, 2)
    assert [item.version_id for item in second.items] == [uid(1)]
    assert second.next_cursor is None


def test_history_casts_limit_to_int_for_databricks_sql():
    """The Statements API binds Python ints as BIGINT, but Databricks SQL requires LIMIT to
    be INT (INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE). The bound limit must be cast to INT."""
    ledger, sql = make_ledger(ids=[uid(1)])
    ledger.history(binding_fixture(), None, 25)
    statement, parameters = sql.calls[-1]
    assert "LIMIT CAST(:limit AS INT)" in statement
    assert parameters["limit"] == 26  # limit + 1 for the next-cursor probe


def test_get_version_roundtrips_snapshot_and_full_context():
    ledger, _ = make_ledger(ids=[uid(42)])
    context = ctx("k1", origin=Origin.OPTIMIZER, parent_version_id=uid(7),
                  restored_from_version_id=uid(8), operation_id=uid(9), attempt_id=uid(10),
                  generation=3, api_update_time=NOW + timedelta(minutes=1),
                  optimizer_run_id="run-1", champion_id="champ-1", release_id=uid(11))
    ref = ledger.append_observation(SNAP_A, context)
    version = ledger.get_version(binding_fixture(), ref.version_id)
    assert version.version_id == uid(42)
    assert version.snapshot == SNAP_A
    assert version.context == context


def test_verify_committed_is_false_for_unknown_reference():
    ledger, _ = make_ledger(ids=[uid(1)])
    ledger.append_observation(SNAP_A, ctx("k1"))
    unknown = ObservationRef(uid(99), binding_fixture().binding_id, 1,
                             SNAP_B.state_digest, SNAP_B.response_envelope_digest)
    assert ledger.verify_committed(unknown) is False
