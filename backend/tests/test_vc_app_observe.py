"""Fail-closed app wiring for the observe surface: config, vc_auth, mounting."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.version_control import contracts as vc
from backend.services.version_control.platform.app_observe import (
    FailClosedRestore,
    mount_observe_routers,
    observe_config_from_env,
    vc_auth_from_request,
)
from backend.services.version_control.platform.observe_seams import build_observe_runtime
from backend.tests._vc_fakes import FakeAdapters, TARGET_HOST

_FULL_ENV = {
    "VC_OBSERVE_WORKSPACE_ID": "target",
    "VC_OBSERVE_CATALOG": "sandbox_cat",
    "VC_OBSERVE_CONTROL_SCHEMA": "vc_ctl",
    "VC_OBSERVE_WAREHOUSE_ID": "wh-123",
    "VC_OBSERVE_HOST": TARGET_HOST,
    "VC_OBSERVE_SP_PRINCIPAL_ID": "target-sp",
}


@pytest.mark.parametrize("drop", list(_FULL_ENV.keys()))
def test_config_is_none_when_a_required_var_is_missing(drop):
    env = {k: v for k, v in _FULL_ENV.items() if k != drop}
    assert observe_config_from_env(env) is None


def test_config_builds_full_shape_from_env():
    config = observe_config_from_env(_FULL_ENV)
    assert config["workspace_id"] == "target"
    assert config["catalog"] == "sandbox_cat" and config["control_schema"] == "vc_ctl"
    assert config["target_selection"]["principal_id"] == "target-sp"
    assert config["target_selection"]["execution_ref"] == "app/genie-workbench"
    assert config["roles"]["executor"]["warehouse_id"] == "wh-123"
    # CUJ-1 (history/capture/restore) is native: the flags are hardwired on whenever the
    # observe surface is wired, independent of any env switch.
    assert config["flags"] == {"vc_history_enabled": True, "vc_writes_enabled": True,
                               "vc_restore_enabled": True}


def test_config_binds_app_sp_m2m_executor_when_no_profile():
    """The deployed-app path: with no operator profile, the app SP is bound as an m2m
    executor from the ambient DATABRICKS_CLIENT_ID/SECRET env, with a routing profile key
    (into the identity provider's _profiles) equal to target_selection.profile so the
    identity provider takes the oauth-m2m executor path, never the bare-ambient client."""
    executor = observe_config_from_env(_FULL_ENV)["roles"]["executor"]
    assert executor["profile"] == "vc-observe-app-sp"
    assert executor["auth"] == {"mode": "m2m", "host": TARGET_HOST,
                                "client_id_env": "DATABRICKS_CLIENT_ID",
                                "client_secret_env": "DATABRICKS_CLIENT_SECRET"}
    config = observe_config_from_env(_FULL_ENV)
    assert config["target_selection"]["profile"] == executor["profile"]


def test_config_m2m_credential_env_names_are_overridable():
    config = observe_config_from_env({**_FULL_ENV,
                                      "VC_OBSERVE_CLIENT_ID_ENV": "APP_ID",
                                      "VC_OBSERVE_CLIENT_SECRET_ENV": "APP_SECRET"})
    auth = config["roles"]["executor"]["auth"]
    assert auth["client_id_env"] == "APP_ID" and auth["client_secret_env"] == "APP_SECRET"


def test_config_uses_operator_profile_when_provided_without_m2m_auth():
    """The operator/local path: an explicit VC_OBSERVE_PROFILE is a real named profile,
    so it routes through _profiles directly with no m2m auth block."""
    config = observe_config_from_env({**_FULL_ENV, "VC_OBSERVE_PROFILE": "vc-operator"})
    executor = config["roles"]["executor"]
    assert executor["profile"] == "vc-operator" and "auth" not in executor
    assert config["target_selection"]["profile"] == "vc-operator"


def test_config_empty_env_is_none():
    assert observe_config_from_env({}) is None


def test_vc_auth_prefers_email_then_username_then_user_id_then_dev():
    # The human email (X-Forwarded-Email) wins over the numeric X-Forwarded-User id so the
    # recorded author is a readable person, not a cryptic id.
    assert vc_auth_from_request(
        {"X-Forwarded-Email": "b@x", "X-Forwarded-User": "5999868213926204"}, "ws"
    ) == vc.AuthenticatedRequest("b@x", "ws")
    # Preferred username is the next-best readable identity.
    assert vc_auth_from_request(
        {"X-Forwarded-Preferred-Username": "user@x", "X-Forwarded-User": "5999868213926204"}, "ws"
    ) == vc.AuthenticatedRequest("user@x", "ws")
    # The numeric id is only a last resort when no readable identity is forwarded.
    assert vc_auth_from_request({"X-Forwarded-User": "5999868213926204"}, "ws") == vc.AuthenticatedRequest("5999868213926204", "ws")
    assert vc_auth_from_request({}, "ws", dev_email="d@x") == vc.AuthenticatedRequest("d@x", "ws")
    assert vc_auth_from_request({}, "ws") is None


def test_fail_closed_restore_never_executes():
    with pytest.raises(PermissionError, match="not wired"):
        FailClosedRestore().submit("op-1", object())


def _app_with_observe(*, authenticated: bool):
    runtime = build_observe_runtime(observe_config_from_env(_FULL_ENV), adapters=FakeAdapters())
    app = FastAPI()

    @app.middleware("http")
    async def authenticate(request, call_next):
        if authenticated:
            request.state.vc_auth = vc.AuthenticatedRequest("user@x", runtime.workspace_id)
        return await call_next(request)

    mount_observe_routers(app, runtime)
    return app, runtime


def test_mount_exposes_history_and_observe_and_restore_routes():
    app, _ = _app_with_observe(authenticated=True)
    paths = {(route.path, tuple(sorted(route.methods))) for route in app.routes
             if getattr(route, "path", "").startswith("/api/version-control")}
    assert ("/api/version-control/bindings/{binding_id}/versions", ("GET",)) in paths
    assert ("/api/version-control/bindings/{binding_id}/observe", ("POST",)) in paths
    assert ("/api/version-control/bindings/{binding_id}/restore", ("POST",)) in paths


def test_history_requires_vc_auth_when_mounted():
    app, _ = _app_with_observe(authenticated=False)
    binding_id = "00000000-0000-4000-8000-000000000001"
    response = TestClient(app).get(f"/api/version-control/bindings/{binding_id}/versions")
    assert response.status_code == 401


def test_obo_middleware_sets_vc_auth_on_api_version_control_path():
    """Regression: the real OBOAuthMiddleware must derive request.state.vc_auth on the
    /api/ path (every VC route is under /api/version-control). A prior bug placed the
    derivation in the non-/api else branch, so VC requests always 401'd in production."""
    from types import SimpleNamespace

    from fastapi import Request

    from backend.main import OBOAuthMiddleware

    app = FastAPI()
    app.add_middleware(OBOAuthMiddleware)
    app.state.vc_observe = SimpleNamespace(workspace_id="ws-1")

    @app.get("/api/version-control/probe")
    def probe(request: Request):
        auth = getattr(request.state, "vc_auth", None)
        return {"ref": None if auth is None else auth.authentication_reference,
                "ws": None if auth is None else auth.workspace_id}

    client = TestClient(app)
    hit = client.get("/api/version-control/probe", headers={"X-Forwarded-Email": "u@x"})
    assert hit.status_code == 200
    assert hit.json() == {"ref": "u@x", "ws": "ws-1"}
    # No forwarded identity -> no vc_auth (routers then fail closed with 401).
    anon = client.get("/api/version-control/probe")
    assert anon.json() == {"ref": None, "ws": None}
