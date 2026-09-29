"""GET /api/spaces/{space_id}/access: the level Genie grants the signed-in user (MV-D109)."""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.routers import spaces
from backend.services.space_access import SpaceAccessLevel as L

pytestmark = pytest.mark.real_space_access

_SPACE = "01f19f413ccc1ea3a42055a66e886302"


def _client(monkeypatch, answer):
    def resolve(space_id):
        assert space_id == _SPACE
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(spaces, "resolve_space_access_level", resolve)
    app = FastAPI()
    app.include_router(spaces.router)
    return TestClient(app)


@pytest.mark.parametrize("held,level", [(L.MANAGE, "manage"), (L.EDIT, "edit"), (L.VIEW, "view"), (None, None)])
def test_the_route_reports_the_level_genie_grants(monkeypatch, held, level):
    response = _client(monkeypatch, held).get(f"/api/spaces/{_SPACE}/access")
    assert response.status_code == 200
    assert response.json() == {"space_id": _SPACE, "level": level}


def test_a_refusal_other_than_denial_reaches_the_client(monkeypatch):
    refusal = HTTPException(503, detail={
        "code": "space_access_unavailable", "required": "edit",
        "message": "Could not verify your access to this Genie Agent. Try again shortly.",
        "platform_message": ""})
    response = _client(monkeypatch, refusal).get(f"/api/spaces/{_SPACE}/access")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "space_access_unavailable"


def test_space_detail_scope_error_does_not_call_the_service_principal(monkeypatch):
    """M1c-D3: a scope error from the user's client must not retry as the SP.

    The handler's catch-all returns 500; Task 6's VIEW gate answers before it
    for a denial. This test opts out of the default grant (``real_space_access``),
    so the gate is stubbed open to reach the OBO read under test.
    """
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    user_calls: list[str] = []

    def user_do(method, path, query=None):
        user_calls.append(path)
        raise RuntimeError("insufficient_scope: genie")

    async def allow(_space_id, _level):
        return None

    user = SimpleNamespace(api_client=SimpleNamespace(do=user_do))
    sp_client = MagicMock(name="sp")

    monkeypatch.setattr(spaces, "require_space_access", allow)
    monkeypatch.setattr(spaces, "require_obo_workspace_client", lambda: user)
    # SP must not be consulted for this read (spaces.py no longer imports it;
    # pin auth so a regression that reintroduces the call is visible).
    monkeypatch.setattr(
        "backend.services.auth.get_service_principal_client", lambda: sp_client
    )

    app = FastAPI()
    app.include_router(spaces.router)
    resp = TestClient(app).get(f"/api/spaces/{_SPACE}")
    assert resp.status_code == 500
    assert user_calls
    sp_client.api_client.do.assert_not_called()
