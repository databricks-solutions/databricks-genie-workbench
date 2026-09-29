"""GET /api/spaces/{space_id}/access: the level Genie grants the signed-in user (MV-D109)."""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.routers import spaces
from backend.services.space_access import SpaceAccessLevel as L

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
