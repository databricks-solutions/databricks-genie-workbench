"""M04 serialized observer port tests (no platform claims)."""

from datetime import datetime, timedelta, timezone
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

import pytest

from backend.services.version_control import contracts as vc
from backend.services.version_control.observer import Observer
from backend.tests._vc_rig import rig, uid
from backend.tests.vc_fakes.fixtures import actor_fixture


@pytest.fixture
def observer_rig(rig):
    viewer = actor_fixture()
    status = vc.BindingStatus(rig.binding.binding_id, 1, vc.Heads(None, uid(), uid()),
                              vc.DriftState.EXTERNAL_AHEAD, False, None, None,
                              datetime.now(timezone.utc), False, (), ())
    identity = Mock(spec=vc.IdentityProvider)
    identity.executor.return_value = rig.executor
    selection = vc.ExplicitExecutorSelection(rig.executor.workspace_id, rig.executor.host,
        rig.executor.principal_id, rig.executor.execution_ref, None)
    rig.coordination.observe_exclusively.side_effect = lambda *args: rig.trace.append("lease") or vc.ObservationLease(
        rig.fence, 1, datetime.now(timezone.utc) + timedelta(minutes=1))
    rig.coordination.advance_heads.side_effect = lambda fence, update: rig.trace.append("advance") or vc.Heads(
        update.observed, status.heads.approved, status.heads.deployed)
    rig.coordination.release_observation.side_effect = lambda fence: rig.trace.append("release")
    observer = Observer(coordination=rig.coordination, ledger=rig.ledger, canonicalizer=rig.canonicalizer,
        transport=rig.transport, identity=identity, reader_selection=selection,
        status_reader=lambda binding, actor: status)
    return rig, observer, viewer, status, identity


def test_capture_on_open_commits_before_display_and_advances_only_observed(observer_rig):
    rig, observer, viewer, status, identity = observer_rig
    result = observer.capture_on_open(rig.binding, viewer)
    assert result is not None, "Capture must return committed display evidence"
    assert rig.trace == ["lease", "get", "commit:open", "verify_commit", "advance"]
    assert result.captured_version.version_id == result.status.heads.observed
    assert result.status.heads.approved == status.heads.approved
    assert result.status.heads.deployed == status.heads.deployed
    assert result.status.drift != vc.DriftState.CLEAN
    update = rig.coordination.advance_heads.call_args.args[1]
    assert update.approved is update.deployed is None
    assert rig.transport.get.call_args.args[1] is rig.executor
    identity.executor.assert_called_once()


def test_concurrent_opens_deduplicate_without_regressing_or_mislabeling_managed_checkpoint(observer_rig):
    rig, observer, viewer, status, identity = observer_rig
    entered, release = Event(), Event()
    lease = rig.coordination.observe_exclusively.side_effect
    def acquire(*args):
        if entered.is_set():
            raise RuntimeError("active observation or managed checkpoint")
        return lease(*args)
    def slow_get(*args):
        entered.set()
        assert release.wait(5)
        return rig.get(*args)
    rig.coordination.observe_exclusively.side_effect = acquire
    rig.transport.get.side_effect = slow_get
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(observer.capture_on_open, rig.binding, viewer)
        assert entered.wait(5)
        try:
            second = observer.capture_on_open(rig.binding, viewer)
            assert second.busy and second.status.stale
            assert second.captured_version is None
            assert second.status.heads == status.heads
        finally:
            release.set()
        assert first.result().captured_version is not None
    rig.transport.get.assert_called_once()
    rig.ledger.append_observation.assert_called_once()


def test_unchanged_open_deduplicates_committed_observation(observer_rig):
    rig, observer, viewer, status, identity = observer_rig
    first = observer.capture_on_open(rig.binding, viewer)
    observer.status_reader = lambda *args: first.status
    second = observer.capture_on_open(rig.binding, viewer)
    assert second.status.heads.observed == first.status.heads.observed
    rig.ledger.append_observation.assert_called_once()


def test_unchanged_capture_returns_observed_head_as_base_preserving_drift(observer_rig):
    """M07 preflight/compensation need the committed base on an unchanged target: the
    unchanged branch must return the observed-head version as captured_version and leave
    drift exactly as the status reader reports (CLEAN in the promotion path), never None
    and never forced to UNKNOWN like a fresh capture."""
    rig, observer, viewer, status, identity = observer_rig
    first = observer.capture_on_open(rig.binding, viewer)
    head = first.captured_version.version_id
    # Simulate the promotion path's CLEAN status reader over the now-committed head.
    clean = replace(first.status, drift=vc.DriftState.CLEAN, stale=False)
    observer.status_reader = lambda *args: clean
    rig.trace.clear()
    second = observer.capture(rig.binding, "promotion_base", rig.executor)
    assert second.captured_version is not None
    assert second.captured_version.version_id == head
    assert second.status.drift == vc.DriftState.CLEAN
    assert not second.busy and not second.status.stale
    # No head to advance on an unchanged target: the lease is finalized via
    # release_observation (advance_heads rejects an all-None update), not advance.
    assert "release" in rig.trace and "advance" not in rig.trace
    rig.ledger.append_observation.assert_called_once()  # no second append on unchanged


def test_capture_stamps_optimizer_run_id_and_champion(observer_rig):
    """An after-run capture carries optimizer provenance on both the appended
    CaptureContext and the returned VersionSummary (origin stays EXTERNAL — the
    optimizer authored the change outside VC governance)."""
    rig, observer, viewer, status, identity = observer_rig
    result = observer.capture(rig.binding, "optimizer_after", rig.executor,
                              optimizer_run_id="run-123", champion_id="champ-9")
    assert result.captured_version is not None
    assert result.captured_version.optimizer_run_id == "run-123"
    assert result.captured_version.champion_id == "champ-9"
    context = rig.ledger.append_observation.call_args.args[1]
    assert context.optimizer_run_id == "run-123"
    assert context.champion_id == "champ-9"
    assert context.origin == vc.Origin.EXTERNAL


def test_capture_on_open_leaves_optimizer_provenance_empty(observer_rig):
    rig, observer, viewer, status, identity = observer_rig
    result = observer.capture_on_open(rig.binding, viewer)
    assert result.captured_version.optimizer_run_id is None
    assert result.captured_version.champion_id is None
    context = rig.ledger.append_observation.call_args.args[1]
    assert context.optimizer_run_id is None and context.champion_id is None


def test_capture_on_open_records_workbench_origin(observer_rig):
    """CUJ-1 §4.2: a user-initiated capture surfaces as `workbench`, not `external`."""
    rig, observer, viewer, status, identity = observer_rig
    result = observer.capture_on_open(rig.binding, viewer)
    assert result.captured_version.origin == vc.Origin.WORKBENCH
    context = rig.ledger.append_observation.call_args.args[1]
    assert context.origin == vc.Origin.WORKBENCH


def test_capture_threads_restore_origin_and_lineage(observer_rig):
    """A restore record carries origin=restore and links restored_from_version_id on both
    the appended CaptureContext and the returned VersionSummary."""
    rig, observer, viewer, status, identity = observer_rig
    source = uid()
    result = observer.capture(rig.binding, "restore", rig.executor,
                              origin=vc.Origin.RESTORE, restored_from_version_id=source)
    assert result.captured_version is not None
    assert result.captured_version.origin == vc.Origin.RESTORE
    assert result.captured_version.restored_from_version_id == source
    context = rig.ledger.append_observation.call_args.args[1]
    assert context.origin == vc.Origin.RESTORE
    assert context.restored_from_version_id == source


def test_capture_records_actor_override_while_reading_as_the_executor(observer_rig):
    """A deliberate write (restore) records the human as the ledger actor, but the GET and
    status read still run under the SP executor identity."""
    rig, observer, viewer, status, identity = observer_rig
    human = vc.ActorContext("user@x", rig.executor.workspace_id, "human")
    reads = []
    observer.status_reader = lambda binding, actor: reads.append(actor) or status
    result = observer.capture(rig.binding, "restore", rig.executor, origin=vc.Origin.RESTORE,
                              actor_override=human)
    context = rig.ledger.append_observation.call_args.args[1]
    assert context.actor == human
    assert result.captured_version.observed_by == "user@x"
    # The status reader saw the SP executor, not the human override.
    assert all(actor.subject_id == rig.executor.principal_id for actor in reads)


def test_capture_on_open_records_actor_override_as_the_human(observer_rig):
    """CUJ-1 §C: a workbench capture records the clicking human as the ledger actor while
    the GET/lease still run under the SP executor. Omitting the override records the SP."""
    rig, observer, viewer, status, identity = observer_rig
    human = vc.ActorContext("user@x", rig.executor.workspace_id, "human")
    result = observer.capture_on_open(rig.binding, viewer, actor_override=human)
    context = rig.ledger.append_observation.call_args.args[1]
    assert context.actor == human
    assert result.captured_version.observed_by == "user@x"
    assert context.origin == vc.Origin.WORKBENCH
    # GET still runs as the SP executor (authorship override never changes the reader).
    assert rig.transport.get.call_args.args[1] is rig.executor


def test_capture_on_open_reads_live_state_via_injected_obo_reader(observer_rig):
    """The space-tab path injects an OBO `live_reader` so the capture reads the serialized
    space under the caller's identity, never through the SP-pinned transport (the SP often
    has no grant on a user-owned space). The lease + ledger append still run as the SP."""
    rig, observer, viewer, status, identity = observer_rig
    envelope = {"serialized_space": {"instructions": "live-obo"}, "description": "d"}
    calls = []
    result = observer.capture_on_open(
        rig.binding, viewer, live_reader=lambda: calls.append("obo") or envelope)
    assert calls == ["obo"]                        # the injected reader was used
    rig.transport.get.assert_not_called()          # ...and the SP transport was not
    assert "get" not in rig.trace and "commit:open" in rig.trace
    assert result.captured_version is not None
    rig.ledger.append_observation.assert_called_once()


def test_capture_on_open_without_reader_still_reads_via_sp_transport(observer_rig):
    """Omitting `live_reader` keeps the default SP-pinned transport read (optimizer/system
    callers), so their behavior is unchanged by the OBO seam."""
    rig, observer, viewer, status, identity = observer_rig
    result = observer.capture_on_open(rig.binding, viewer)
    assert "get" in rig.trace
    assert rig.transport.get.call_args.args[1] is rig.executor
    assert result.captured_version is not None


def test_capture_on_open_without_override_records_the_service_principal(observer_rig):
    """System/optimizer callers omit the override and stay recorded as the SP executor."""
    rig, observer, viewer, status, identity = observer_rig
    result = observer.capture_on_open(rig.binding, viewer)
    context = rig.ledger.append_observation.call_args.args[1]
    assert context.actor.subject_id == rig.executor.principal_id
    assert result.captured_version.observed_by == rig.executor.principal_id


@pytest.mark.parametrize("failure", ["append_observation", "verify_committed", "advance_heads"])
def test_capture_persistence_failure_returns_stale_and_disables_actions(observer_rig, failure):
    rig, observer, viewer, status, identity = observer_rig
    owner = rig.coordination if failure == "advance_heads" else rig.ledger
    getattr(owner, failure).side_effect = RuntimeError("persistence unavailable")
    result = observer.capture_on_open(rig.binding, viewer)
    assert result.status.stale
    assert result.status.allowed_actions == ()
    assert result.status.heads == status.heads
    assert result.captured_version is None
    assert result.status.drift != vc.DriftState.CLEAN
