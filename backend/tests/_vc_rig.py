"""Shared governed-free test rig for the observe-surface tests.

Builds the mocked VC/1.0 port surface (coordination / ledger / facts / transport /
canonicalizer / binding / executor) reused by the observer and history/mutations
router tests. It intentionally does NOT construct a ``MutationGate`` — the governed
mutation graph was removed with the observe-only pivot — so the rig assembles from
contracts + fakes only.
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from backend.services.version_control import contracts as vc
from backend.tests.vc_fakes.fixtures import FakeCanonicalizer, binding_fixture, executor_fixture


def uid():
    return str(uuid4())


@pytest.fixture
def rig():
    trace = []
    canonicalizer = FakeCanonicalizer()
    binding = binding_fixture()
    executor = executor_fixture()
    state = {"serialized_space": {"instructions": "old"}, "description": "old"}
    before = canonicalizer.observe(state)
    identity = vc.RequestIdentity(uid(), "edit-key", "a" * 64)
    request = vc.MutationRequest(identity, binding, "edit", uid(), before.state_digest,
                                 {"instructions": "new"}, "new", None)
    fence = vc.FenceToken(binding.binding_id, 1, uid(), 1, 1)
    coordination = Mock(spec=vc.Coordination)
    ledger = Mock(spec=vc.VersionLedger)
    facts = Mock(spec=vc.OperationFacts)
    approvals = Mock(spec=vc.ApprovalService)
    transport = Mock(spec=vc.GenieTransport)
    flags = Mock()
    flags.enabled.return_value = True
    versions = {}

    def append(snapshot, context):
        trace.append("commit:" + context.observation_reason)
        version = vc.Version(uid(), snapshot, context)
        versions[version.version_id] = version
        return vc.ObservationRef(version.version_id, binding.binding_id, 1,
                                 snapshot.state_digest, snapshot.response_envelope_digest)

    def admit(reservation, preimage, authorization):
        trace.append("admit")
        return vc.AdmissionClaim(**vc.to_wire(fence), request=reservation.request,
                                 preimage=preimage, approval_id=None, approval_digest=None)

    def get(*args):
        trace.append("get")
        return deepcopy(state)

    def config(bound, serialized, claim):
        trace.append("patch_config")
        state["serialized_space"] = deepcopy(serialized)

    def description(bound, text, claim):
        trace.append("patch_description")
        state["description"] = text

    coordination.reserve.side_effect = lambda *args: (trace.append("reserve") or vc.Reservation(
        fence, args[1], executor, datetime.now(timezone.utc) + timedelta(minutes=5)))
    coordination.admit.side_effect = admit
    coordination.assert_owner.side_effect = lambda *args: trace.append("assert_owner")
    coordination.checkpoint.side_effect = lambda claim, stage, evidence: trace.append(stage.value)
    coordination.finish.side_effect = lambda *args: trace.append("finish")
    coordination.reject_reservation.side_effect = lambda *args: trace.append("reject_reservation")
    coordination.quarantine.side_effect = lambda *args: trace.append("quarantine")
    ledger.append_observation.side_effect = append
    ledger.verify_committed.side_effect = lambda ref: trace.append("verify_commit") or True
    ledger.get_version.side_effect = lambda bound, version_id: versions[version_id]
    facts.lookup_request.return_value = vc.RequestHistory((), False)
    facts.verify_flush.return_value = True
    facts.append.side_effect = lambda fact: trace.append("fact:" + fact.status.value) or vc.FactRef(
        fact.event_id, fact.event_key, "a" * 64)
    desired = canonicalizer.observe({"serialized_space": dict(request.serialized_space), "description": request.description})
    approvals.authorize.side_effect = lambda req, exe: trace.append("authorize") or vc.AuthorizationGrant(
        req.binding, req.identity, "grant", before.fingerprints, desired.state_digest,
        datetime.now(timezone.utc) + timedelta(minutes=5), None, None)
    transport.get.side_effect = get
    transport.patch_config_once.side_effect = config
    transport.patch_description_once.side_effect = description
    return SimpleNamespace(**locals())
