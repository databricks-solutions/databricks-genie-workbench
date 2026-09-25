"""M04 HTTP adapters: authenticated observation and operation-id-only restore Jobs."""

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.version_control import contracts as vc
from backend.tests._vc_rig import rig, uid
from backend.tests.test_vc_observer import observer_rig


@pytest.fixture
def router_rig(rig, observer_rig):
    from backend.routers.vc_mutations import build_router

    # Observe-only surface: restore is wired fail-closed in the deployed app, so the
    # mounted router's restore path never reaches a real restore service. The router
    # itself enforces auth/history scope and immutable-request validation before it
    # would ever call restore.submit, so a mock restore/dispatcher suffices here.
    _, observer, actor, status, _ = observer_rig
    restore = Mock()
    dispatcher = Mock(spec=vc.JobDispatcher)
    identity = Mock(spec=vc.IdentityProvider)
    request = replace(rig.request, operation_type="restore", approval_id=uid())
    rig.facts.get_request.return_value = vc.ApprovedOperation(request, None)
    now = datetime.now(timezone.utc)
    fact = vc.OperationFact(uid(), vc.FactKind.OPERATION, request.identity.operation_id, 0,
        "restore-request", request.binding, request.identity, "restore", actor.subject_id,
        actor, vc.FactStatus.REQUESTED,
        vc.StageEvidence("VC/1.0", vc.PatchStage.CONFIG_OBSERVED, now, request.expected_base, None), now)
    rig.facts.lookup_request.return_value = vc.RequestHistory((fact,), False)
    identity.actor.return_value = actor
    identity.executor.return_value = rig.executor
    registry = Mock(spec=vc.Registry)
    registry.resolve.return_value = rig.binding
    authorize_history = Mock(return_value=True)
    observer = Mock(spec=vc.Observer, wraps=observer)
    selection = vc.ExplicitExecutorSelection(rig.executor.workspace_id, rig.executor.host,
        rig.executor.principal_id, rig.executor.execution_ref, None)
    authentication = vc.AuthenticatedRequest("verified-session", actor.workspace_id)

    def make_client(flags=rig.flags, authenticated=True):
        app = FastAPI()

        @app.middleware("http")
        async def authenticate(request, call_next):
            if authenticated:
                request.state.vc_auth = authentication
            return await call_next(request)

        router = build_router(observer=observer, restore=restore, identity=identity,
            registry=registry, facts=rig.facts, authorize_history=authorize_history,
            reader_selection=selection, flags=flags)
        app.include_router(router)
        return TestClient(app), router

    client, router = make_client()
    prefix = f"/api/version-control/bindings/{rig.binding.binding_id}"
    headers = {"Idempotency-Key": request.identity.idempotency_key}
    body = {"version_id": request.source_version_id, "binding_revision": 1,
            "expected_base": request.expected_base, "approval_id": request.approval_id}
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("reason", ["open", "history", "refresh", "return"])
def test_observe_delegates_to_authorized_observer(router_rig, reason):
    setup = router_rig
    response = setup.client.post(setup.prefix + "/observe", json={"reason": reason}, headers=setup.headers)
    assert response.status_code == 200
    result = response.json()
    assert result["captured_version"]["version_id"] == result["status"]["heads"]["observed"]
    assert setup.rig.ledger.append_observation.call_args.args[1].observation_reason == reason
    if reason == "open":
        setup.observer.capture_on_open.assert_called_once_with(setup.rig.binding, setup.actor)
    else:
        setup.observer.capture.assert_called_once_with(setup.rig.binding, reason, setup.rig.executor)
    setup.authorize_history.assert_called_once_with(setup.actor, setup.rig.binding)
    setup.identity.actor.assert_called_once_with(setup.authentication)
    setup.dispatcher.submit_local.assert_not_called()


@pytest.mark.parametrize("disabled", ["default", "vc_writes_enabled", "vc_restore_enabled"])
def test_restore_is_fail_closed_and_observation_stays_stale_safe(router_rig, disabled):
    setup = router_rig
    setup.rig.flags.enabled.side_effect = lambda switch: switch != disabled
    client, _ = setup.make_client(flags=None if disabled == "default" else setup.rig.flags)
    response = client.post(setup.prefix + "/restore", json=setup.body, headers=setup.headers)
    assert response.status_code == 503
    assert response.json()["detail"]["retryable"] is False
    setup.dispatcher.submit_local.assert_not_called()
    setup.rig.transport.patch_config_once.assert_not_called()
    setup.rig.ledger.verify_committed.side_effect = lambda observation: False
    response = client.post(setup.prefix + "/observe", json={"reason": "open"}, headers=setup.headers)
    assert response.status_code == 200
    assert response.json()["status"]["stale"] is True
    assert response.json()["status"]["allowed_actions"] == []
    assert response.json()["captured_version"] is None


@pytest.mark.parametrize("route", ["observe", "restore"])
@pytest.mark.parametrize("denial", ["authentication", "history", "workspace", "key"])
def test_routes_require_server_identity_scope_and_command_key(router_rig, route, denial):
    setup = router_rig
    client, _ = setup.make_client(authenticated=denial != "authentication")
    if denial == "history":
        setup.authorize_history.return_value = False
    if denial == "workspace":
        setup.identity.actor.return_value = replace(setup.actor, workspace_id="other")
    response = client.post(setup.prefix + "/" + route,
        json=setup.body if route == "restore" else {"reason": "open"},
        headers={} if denial == "key" else setup.headers)
    assert response.status_code == {"authentication": 401, "history": 403, "workspace": 403, "key": 422}[denial]
    setup.dispatcher.submit_local.assert_not_called()
    setup.rig.transport.get.assert_not_called()


@pytest.mark.parametrize("field,value", [
    ("version_id", "00000000-0000-4000-8000-000000000099"),
    ("binding_revision", 2), ("expected_base", "f" * 64),
    ("approval_id", "00000000-0000-4000-8000-000000000099"),
])
def test_restore_body_must_match_immutable_request(router_rig, field, value):
    setup = router_rig
    response = setup.client.post(setup.prefix + "/restore",
        json={**setup.body, field: value}, headers=setup.headers)
    assert response.status_code == 409
    setup.dispatcher.submit_local.assert_not_called()


@pytest.mark.parametrize("history", ["missing", "ambiguous", "wrong_key"])
def test_restore_requires_unique_durable_request(router_rig, history):
    setup = router_rig
    fact = setup.fact
    if history == "wrong_key":
        fact = replace(fact, request=replace(fact.request, idempotency_key="other"))
    setup.rig.facts.lookup_request.return_value = vc.RequestHistory(
        () if history == "missing" else (fact,), history == "ambiguous")
    response = setup.client.post(setup.prefix + "/restore", json=setup.body, headers=setup.headers)
    assert response.status_code == 409
    setup.dispatcher.submit_local.assert_not_called()


def test_mutations_router_exposes_exact_contract_routes(router_rig):
    assert {(route.path, tuple(sorted(route.methods))) for route in router_rig.router.routes} == {
        ("/api/version-control/bindings/{binding_id}/observe", ("POST",)),
        ("/api/version-control/bindings/{binding_id}/restore", ("POST",)),
    }
