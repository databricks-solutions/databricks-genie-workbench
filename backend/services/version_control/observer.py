"""Serialized full-state observation, independent of policy head authority."""

import logging
from dataclasses import replace
from datetime import datetime, timezone

from backend.services.version_control import contracts as vc

logger = logging.getLogger(__name__)


class Observer:
    def __init__(self, *, coordination, ledger, canonicalizer, transport, identity,
                 reader_selection, status_reader):
        self.coordination = coordination
        self.ledger = ledger
        self.canonicalizer = canonicalizer
        self.transport = transport
        self.identity = identity
        self.reader_selection = reader_selection
        self.status_reader = status_reader

    def capture_on_open(self, binding, viewer, *, origin=vc.Origin.WORKBENCH,
                        actor_override=None, live_reader=None):
        # A user opening the tab / clicking "Capture current state" is a workbench-initiated
        # observation (CUJ-1 §4.2), not `external`. Callers may override for other surfaces.
        # `actor_override` records the human who initiated the capture as the ledger actor
        # (mirrors `capture`); the lease, status read, and ledger append still run as the SP
        # executor. When omitted (e.g. system/optimizer callers) the SP executor identity is
        # recorded. `live_reader`, when supplied, reads the live serialized space under the
        # CALLER's identity (OBO) instead of the SP-pinned transport -- the space-tab path
        # injects it so a capture works on a user-owned space the SP has no grant on (an
        # SP-pinned read 403s there; see restore's OBO live GET). Omitted -> SP transport.
        status = self.status_reader(binding, viewer)
        executor = self.identity.executor(self.reader_selection)
        return self._capture(binding, "open", executor, status, origin=origin,
                             actor_override=actor_override, live_reader=live_reader)

    def capture(self, binding, reason, executor, *, origin=vc.Origin.EXTERNAL,
                restored_from_version_id=None, actor_override=None,
                optimizer_run_id=None, champion_id=None, live_reader=None):
        # `actor_override` records the human who initiated a deliberate write (e.g. a
        # restore clicked in the workbench) as the ledger actor, while the GET/lease still
        # run as the SP executor. Defaults to the executor identity (SP-observed captures).
        # `live_reader`, when supplied, reads the live serialized space under the CALLER's
        # identity (OBO) instead of the SP-pinned transport -- the restore path injects it so
        # the post-write capture reflects the user's own view of a space the SP has no grant
        # on (an SP-pinned read 403s there). Omitted -> SP transport (optimizer/system).
        actor = vc.ActorContext(executor.principal_id, executor.workspace_id, executor.actor_kind)
        return self._capture(binding, reason, executor, self.status_reader(binding, actor),
                             origin=origin, restored_from_version_id=restored_from_version_id,
                             actor_override=actor_override,
                             optimizer_run_id=optimizer_run_id, champion_id=champion_id,
                             live_reader=live_reader)

    @staticmethod
    def _summary_of(version) -> vc.VersionSummary:
        """Project a committed ledger Version into the VersionSummary the unchanged-capture
        branch hands back (mirrors DeltaVersionLedger._to_summary field-for-field)."""
        context = version.context
        return vc.VersionSummary(
            version.version_id, context.binding.binding_id, context.observed_at,
            context.origin, context.actor.subject_id, context.parent_version_id,
            context.restored_from_version_id, version.snapshot.fingerprints,
            context.optimizer_run_id, context.champion_id)

    def _capture(self, binding, reason, executor, status, *, origin=vc.Origin.EXTERNAL,
                 restored_from_version_id=None, actor_override=None,
                 optimizer_run_id=None, champion_id=None, live_reader=None):
        try:
            return self._capture_committed(binding, reason, executor, status, origin=origin,
                                           restored_from_version_id=restored_from_version_id,
                                           actor_override=actor_override,
                                           optimizer_run_id=optimizer_run_id, champion_id=champion_id,
                                           live_reader=live_reader)
        except Exception:
            logger.exception("VC observation capture failed for binding %s", binding.binding_id)
            return vc.ObservationResult(replace(status, stale=True, allowed_actions=(),
                reasons=(*status.reasons, "Observation evidence unavailable")), None, True)

    def _capture_committed(self, binding, reason, executor, status, *, origin=vc.Origin.EXTERNAL,
                           restored_from_version_id=None, actor_override=None,
                           optimizer_run_id=None, champion_id=None, live_reader=None):
        try:
            lease = self.coordination.observe_exclusively(binding, executor)
        except Exception:
            return vc.ObservationResult(replace(status, stale=True, allowed_actions=(),
                reasons=(*status.reasons, "Observation busy or unavailable")), None, True)
        actor = vc.ActorContext(executor.principal_id, executor.workspace_id, executor.actor_kind)
        status = self.status_reader(binding, actor)
        # The live read runs under the injected OBO reader when supplied (space-tab path, so
        # the capture reflects the user's own view of a space the SP cannot read); otherwise
        # through the SP-pinned transport (optimizer/system callers). It happens inside the
        # observation lease either way, so concurrent opens still serialize + dedup.
        raw = live_reader() if live_reader is not None else self.transport.get(binding, executor)
        snapshot = self.canonicalizer.observe(raw)
        if status.heads.observed is not None:
            previous = self.ledger.get_version(binding, status.heads.observed)
            if self.canonicalizer.compare(previous.snapshot, snapshot) == vc.Comparison.EQUAL:
                self.coordination.release_observation(lease.fence)
                # Unchanged: the live target still equals the committed observed head, so no
                # new version is appended. The observed head *is* the current base, so return
                # it as `captured_version` (drift stays exactly as the status reader reported)
                # instead of a spurious "no observation" None. Base-needing callers -- M07
                # preflight/compensation -- require the committed base here; a first promotion
                # to a freshly base-observed target only ever traverses this unchanged branch.
                # Busy/failure paths above still return None.
                return vc.ObservationResult(replace(status, stale=False),
                                            self._summary_of(previous), False)
        record_actor = actor_override or vc.ActorContext(
            executor.principal_id, executor.workspace_id, executor.actor_kind)
        context = vc.CaptureContext(binding, f"{lease.fence.attempt_id}:observe",
            datetime.now(timezone.utc), reason, record_actor,
            origin, parent_version_id=status.heads.observed,
            restored_from_version_id=restored_from_version_id,
            attempt_id=lease.fence.attempt_id, generation=lease.fence.generation,
            optimizer_run_id=optimizer_run_id, champion_id=champion_id)
        observation = self.ledger.append_observation(snapshot, context)
        if not self.ledger.verify_committed(observation):
            raise RuntimeError("Observation persistence unavailable")
        heads = self.coordination.advance_heads(lease.fence, vc.HeadUpdate(observation.version_id, None, None, None))
        summary = vc.VersionSummary(observation.version_id, binding.binding_id, context.observed_at,
            context.origin, context.actor.subject_id, context.parent_version_id,
            restored_from_version_id, snapshot.fingerprints, optimizer_run_id, champion_id)
        return vc.ObservationResult(replace(status, heads=heads, observed_at=context.observed_at,
            projection_as_of=context.observed_at, drift=vc.DriftState.UNKNOWN,
            stale=False, allowed_actions=()), summary, False)
