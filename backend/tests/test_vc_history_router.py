"""VC version-history read router: flag-gated, scoped, delegates to the ledger.

History is a read surface: it is gated on ``vc_history_enabled`` only and must never
require the write switches, so an operator can inspect what the optimizer changed even
while governed writes stay off.
"""

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.version_control import contracts as vc
from backend.tests._vc_rig import rig, uid
from backend.tests.vc_fakes.fixtures import actor_fixture


@pytest.fixture
def history_rig(rig):
    from backend.routers.vc_history import build_router

    now = datetime.now(timezone.utc)
    fingerprints = vc.Fingerprints("a" * 64, "b" * 64, "c" * 64, "vc-c14n/1")
    summary = vc.VersionSummary(uid(), rig.binding.binding_id, now, vc.Origin.EXTERNAL,
        "user@example.com", None, None, fingerprints, "run-1", None)
    page = vc.VersionPage((summary,), "cursor-2")
    ledger = Mock(spec=vc.VersionLedger)
    ledger.history.return_value = page
    actor = actor_fixture()
    identity = Mock(spec=vc.IdentityProvider)
    identity.actor.return_value = actor
    registry = Mock(spec=vc.Registry)
    registry.resolve.return_value = rig.binding
    authorize_history = Mock(return_value=True)
    authentication = vc.AuthenticatedRequest("verified-session", actor.workspace_id)

    def make_client(flags=rig.flags, authenticated=True):
        app = FastAPI()

        @app.middleware("http")
        async def authenticate(request, call_next):
            if authenticated:
                request.state.vc_auth = authentication
            return await call_next(request)

        router = build_router(ledger=ledger, registry=registry, identity=identity,
            authorize_history=authorize_history, flags=flags)
        app.include_router(router)
        return TestClient(app), router

    client, router = make_client()
    prefix = f"/api/version-control/bindings/{rig.binding.binding_id}"
    return SimpleNamespace(**locals())


def test_versions_delegates_to_ledger_history(history_rig):
    setup = history_rig
    response = setup.client.get(setup.prefix + "/versions?limit=5")
    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["version_id"] == setup.summary.version_id
    assert body["items"][0]["optimizer_run_id"] == "run-1"
    assert body["next_cursor"] == "cursor-2"
    setup.ledger.history.assert_called_once_with(setup.rig.binding, None, 5)
    setup.authorize_history.assert_called_once_with(setup.actor, setup.rig.binding)
    setup.identity.actor.assert_called_once_with(setup.authentication)


def test_versions_forwards_cursor_and_default_limit(history_rig):
    setup = history_rig
    assert setup.client.get(setup.prefix + "/versions?cursor=abc").status_code == 200
    setup.ledger.history.assert_called_once_with(setup.rig.binding, "abc", 25)


@pytest.mark.parametrize("flags", ["default", "disabled"])
def test_history_is_fail_closed_when_flag_off(history_rig, flags):
    setup = history_rig
    if flags == "disabled":
        setup.rig.flags.enabled.side_effect = lambda switch: switch != "vc_history_enabled"
    client, _ = setup.make_client(flags=None if flags == "default" else setup.rig.flags)
    response = client.get(setup.prefix + "/versions")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "vc_history_disabled"
    setup.ledger.history.assert_not_called()


@pytest.mark.parametrize("denial,code", [("authentication", 401), ("history", 403), ("workspace", 403)])
def test_history_requires_server_identity_scope(history_rig, denial, code):
    setup = history_rig
    client, _ = setup.make_client(authenticated=denial != "authentication")
    if denial == "history":
        setup.authorize_history.return_value = False
    if denial == "workspace":
        setup.identity.actor.return_value = replace(setup.actor, workspace_id="other")
    response = client.get(setup.prefix + "/versions")
    assert response.status_code == code
    setup.ledger.history.assert_not_called()


@pytest.mark.parametrize("limit", [0, 101])
def test_history_rejects_out_of_range_limit(history_rig, limit):
    setup = history_rig
    assert setup.client.get(setup.prefix + f"/versions?limit={limit}").status_code == 422
    setup.ledger.history.assert_not_called()


def test_history_router_exposes_exact_contract_route(history_rig):
    assert {(route.path, tuple(sorted(route.methods))) for route in history_rig.router.routes} == {
        ("/api/version-control/bindings/{binding_id}/versions", ("GET",)),
        ("/api/version-control/bindings/{binding_id}/versions/{version_id}", ("GET",)),
        ("/api/version-control/bindings/{binding_id}/diff", ("GET",)),
    }


def _version(rig, canon, *, version_id, serialized, description="d"):
    from datetime import datetime, timezone

    snapshot = canon.observe({"serialized_space": serialized, "description": description})
    context = vc.CaptureContext(
        binding=rig.binding, observation_key=f"key/{version_id}",
        observed_at=datetime.now(timezone.utc), observation_reason="open",
        actor=vc.ActorContext("reader@example.com", rig.binding.workspace_id, "human"),
        origin=vc.Origin.EXTERNAL)
    return vc.Version(version_id, snapshot, context)


def test_version_detail_returns_summary_plus_snapshot(history_rig):
    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer

    setup = history_rig
    version = _version(setup.rig, FakeCanonicalizer(),
                       version_id=uid(), serialized={"a": 1, "benchmarks": {}})
    setup.ledger.get_version.return_value = version
    response = setup.client.get(setup.prefix + f"/versions/{version.version_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["version_id"] == version.version_id
    assert body["origin"] == "external"
    assert body["fingerprints"]["config"] == version.snapshot.fingerprints.config
    assert body["snapshot"] == {"a": 1, "benchmarks": {}}
    setup.ledger.get_version.assert_called_once_with(setup.rig.binding, version.version_id)


def test_version_detail_404_when_missing(history_rig):
    setup = history_rig
    setup.ledger.get_version.side_effect = KeyError("nope")
    response = setup.client.get(setup.prefix + f"/versions/{uid()}")
    assert response.status_code == 404


def test_version_detail_fail_closed_when_history_disabled(history_rig):
    setup = history_rig
    setup.rig.flags.enabled.side_effect = lambda switch: switch != "vc_history_enabled"
    client, _ = setup.make_client(flags=setup.rig.flags)
    response = client.get(setup.prefix + f"/versions/{uid()}")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "vc_history_disabled"
    setup.ledger.get_version.assert_not_called()


def test_diff_reports_different_equal_and_unknown(history_rig):
    from dataclasses import replace

    from backend.tests.vc_fakes.fixtures import FakeCanonicalizer

    setup = history_rig
    canon = FakeCanonicalizer()
    left = _version(setup.rig, canon, version_id=uid(), serialized={"a": 1, "benchmarks": {}})
    right = _version(setup.rig, canon, version_id=uid(), serialized={"a": 2, "benchmarks": {}})
    versions = {left.version_id: left, right.version_id: right}
    setup.ledger.get_version.side_effect = lambda _binding, vid: versions[vid]

    different = setup.client.get(setup.prefix + f"/diff?left={left.version_id}&right={right.version_id}")
    assert different.status_code == 200
    assert different.json()["comparison"] == "different"
    assert different.json()["items"]

    equal = setup.client.get(setup.prefix + f"/diff?left={left.version_id}&right={left.version_id}")
    assert equal.status_code == 200
    assert equal.json() == {"comparison": "equal", "items": []}

    # A canonicalizer-version mismatch must report UNKNOWN, never a false "equal".
    mismatched = replace(right.snapshot,
                         fingerprints=replace(right.snapshot.fingerprints, canonicalizer_version="vc-c14n/2"))
    versions[right.version_id] = replace(right, snapshot=mismatched)
    unknown = setup.client.get(setup.prefix + f"/diff?left={left.version_id}&right={right.version_id}")
    assert unknown.status_code == 200
    assert unknown.json() == {"comparison": "unknown", "items": []}


def test_diff_404_when_version_missing(history_rig):
    setup = history_rig
    setup.ledger.get_version.side_effect = KeyError("nope")
    response = setup.client.get(setup.prefix + f"/diff?left={uid()}&right={uid()}")
    assert response.status_code == 404
